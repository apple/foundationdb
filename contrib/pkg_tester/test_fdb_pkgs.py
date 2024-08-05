# test_fdb_pkgs.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import pathlib
import pytest
import shlex
import subprocess
import sys
import uuid

from typing import Iterator, List, Optional, Union


def run(args: List[str]) -> str:
    print("$ {}".format(" ".join(map(shlex.quote, args))))
    result = []
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while proc.poll() is None:
        text = proc.stdout.readline().decode("utf-8")
        result.append(text)
        sys.stdout.write(text)
    assert proc.returncode == 0
    return "".join(result)


class Image:
    def __init__(self, uid: str):
        self.uid = uid

    def dispose(self):
        run(["docker", "image", "rm", self.uid])


class Container:
    def __init__(self, image: Union[str, Image], initd=False):
        if isinstance(image, Image):
            image_name = image.uid
        else:
            assert isinstance(image, str)
            image_name = image

        # minimal extra args required to run systemd
        # https://developers.redhat.com/blog/2016/09/13/running-systemd-in-a-non-privileged-container#the_quest
        extra_initd_args = []
        if initd:
            extra_initd_args = (
                "--tmpfs /tmp --tmpfs /run -v /sys/fs/cgroup:/sys/fs/cgroup:ro".split()
            )

        self.uid = str(uuid.uuid4())

        run(
            ["docker", "run"]
            + ["-t", "-d", "--name", self.uid]
            + extra_initd_args
            + [image_name]
            + ["/usr/sbin/init" for _ in range(1) if initd]
        ).rstrip()

    def run(self, args: List[str]) -> str:
        return run(["docker", "exec", self.uid] + args)

    def copy_to(self, src_path: str, dst_path: str) -> None:
        run(["docker", "cp", src_path, "{}:{}".format(self.uid, dst_path)])

    def commit(self) -> Image:
        output = run(["docker", "commit", self.uid])
        uid = output.split(":")[1].rstrip()
        return Image(uid)

    def dispose(self):
        run(["docker", "rm", "-f", self.uid])


def ubuntu_image_with_fdb_helper(versioned: bool) -> Iterator[Optional[Image]]:
    """
    Return an image which has just the fdb deb packages installed.
    """
    builddir = os.environ.get("BUILDDIR")
    if builddir is None:
        assert False, "BUILDDIR environment variable not set"
    debs = [
        deb
        for deb in glob.glob(os.path.join(builddir, "packages", "*.deb"))
        if ("versioned" in deb) == versioned
    ]
    if not debs:
        yield None
        return

    container = None
    image = None
    try:
        container = Container("ubuntu")
        for deb in debs:
            container.copy_to(deb, "/opt")
        container.run(["bash", "-c", "apt-get update"])
        container.run(
            ["bash", "-c", "apt-get install --yes execstack"]
        )  # this is for testing libfdb_c execstack permissions
        container.run(
            [
                "bash",
                "-c",
                "DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true apt-get install --yes gcc pkg-config cmake",
            ]
        )  # this is for testing building client apps
        container.run(["bash", "-c", "dpkg -i /opt/*.deb"])
        container.run(["bash", "-c", "rm /opt/*.deb"])
        image = container.commit()
        yield image
    finally:
        if container is not None:
            container.dispose()
        if image is not None:
            image.dispose()


@pytest.fixture(scope="session")
def ubuntu_image_with_fdb() -> Iterator[Optional[Image]]:
    yield from ubuntu_image_with_fdb_helper(versioned=False)


@pytest.fixture(scope="session")
def ubuntu_image_with_fdb_versioned() -> Iterator[Optional[Image]]:
    yield from ubuntu_image_with_fdb_helper(versioned=True)


def centos_image_with_fdb_helper(versioned: bool) -> Iterator[Optional[Image]]:
    """
    Return an image which has just the fdb rpm packages installed.
    """
    builddir = os.environ.get("BUILDDIR")
    if builddir is None:
        assert False, "BUILDDIR environment variable not set"
    rpms = [
        rpm
        for rpm in glob.glob(os.path.join(builddir, "packages", "*.rpm"))
        if ("versioned" in rpm) == versioned
    ]
    if not rpms:
        yield None
        return

    container = None
    image = None
    try:
        container = Container("centos:7", initd=True)
        for rpm in rpms:
            container.copy_to(rpm, "/opt")
        container.run(
            ["bash", "-c", "yum install -y prelink"]
        )  # this is for testing libfdb_c execstack permissions
        container.run(
            ["bash", "-c", "yum install -y gcc pkg-config cmake make"]
        )  # this is for testing building client apps
        container.run(["bash", "-c", "yum install -y /opt/*.rpm"])
        container.run(["bash", "-c", "rm /opt/*.rpm"])
        image = container.commit()
        yield image
    finally:
        if container is not None:
            container.dispose()
        if image is not None:
            image.dispose()


@pytest.fixture(scope="session")
def centos_image_with_fdb() -> Iterator[Optional[Image]]:
    yield from centos_image_with_fdb_helper(versioned=False)


@pytest.fixture(scope="session")
def centos_image_with_fdb_versioned() -> Iterator[Optional[Image]]:
    yield from centos_image_with_fdb_helper(versioned=True)


def pytest_generate_tests(metafunc):
    if "linux_container" in metafunc.fixturenames:
        metafunc.parametrize(
            "linux_container",
            ["ubuntu", "centos", "ubuntu-versioned", "centos-versioned"],
            indirect=True,
        )


@pytest.fixture()
def linux_container(
    request,
    ubuntu_image_with_fdb,
    centos_image_with_fdb,
    ubuntu_image_with_fdb_versioned,
    centos_image_with_fdb_versioned,
) -> Iterator[Container]:
    """
    Tests which accept this fixture will be run once for each supported platform, for each type of package (versioned or unversioned).
    """
    container: Optional[Container] = None
    try:
        if request.param == "ubuntu":
            if ubuntu_image_with_fdb is None:
                pytest.skip("No debian packages available to test")
            container = Container(ubuntu_image_with_fdb)
            container.run(
                ["/etc/init.d/foundationdb", "start"]
            )  # outside docker this shouldn't be necessary
        elif request.param == "centos":
            if centos_image_with_fdb is None:
                pytest.skip("No rpm packages available to test")
            container = Container(centos_image_with_fdb, initd=True)
        elif request.param == "ubuntu-versioned":
            if ubuntu_image_with_fdb is None:
                pytest.skip("No versioned debian packages available to test")
            container = Container(ubuntu_image_with_fdb_versioned)
            container.run(
                ["/etc/init.d/foundationdb", "start"]
            )  # outside docker this shouldn't be necessary
        elif request.param == "centos-versioned":
            if centos_image_with_fdb is None:
                pytest.skip("No versioned rpm packages available to test")
            container = Container(centos_image_with_fdb_versioned, initd=True)
        else:
            assert False
        yield container
    finally:
        if container is not None:
            container.dispose()


#################### BEGIN ACTUAL TESTS ####################


def test_db_available(linux_container: Container):
    linux_container.run(["fdbcli", "--exec", "get x"])


def test_client_app(linux_container: Container):
    test_client_app_script = r"""#!/bin/bash

set -euxo pipefail

cat > app.c << EOF
// FDB_API_VERSION doesn't necessarily need to be kept up to date here
#define FDB_API_VERSION 700
#include <foundationdb/fdb_c.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

static void check(fdb_error_t e) {
    if (e) {
        fprintf(stderr, "%s\n", fdb_get_error(e));
        fflush(NULL);
        abort();
    }
}

int result = 0;

static void callback(FDBFuture* f, void* _ignored) {
    check(fdb_stop_network());
}

int main() {
    check(fdb_select_api_version(700));
    check(fdb_setup_network());
    FDBDatabase* db;
    check(fdb_create_database(NULL, &db));
    FDBTransaction* tr;
    check(fdb_database_create_transaction(db, &tr));
    FDBFuture* f = fdb_transaction_get_read_version(tr);
    check(fdb_future_set_callback(f, callback, NULL));
    check(fdb_run_network());
    fdb_future_destroy(f);
    fdb_transaction_destroy(tr);
    fdb_database_destroy(db);
    return 0;
}
EOF

cc app.c `pkg-config foundationdb-client --cflags --libs`
./a.out

cat > CMakeLists.txt << EOF
project(app C)
find_package(FoundationDB-Client REQUIRED)
add_executable(app app.c)
target_link_libraries(app PRIVATE fdb_c)
EOF

mkdir build
cd build
cmake ..
make
./app

"""
    linux_container.run(["bash", "-c", test_client_app_script])


def test_write(linux_container: Container, snapshot):
    linux_container.run(["fdbcli", "--exec", "writemode on; set x y"])
    assert snapshot == linux_container.run(["fdbcli", "--exec", "get x"])


def test_execstack_permissions_libfdb_c(linux_container: Container, snapshot):
    linux_container.run(["ldconfig"])
    assert snapshot == linux_container.run(
        [
            "bash",
            "-c",
            "execstack -q $(ldconfig -p | grep libfdb_c.so | awk '{print $(NF)}')",
        ]
    )


def test_etc_owner(linux_container: Container, snapshot):
    # /etc/foundationdb should be owned by the foundationdb user
    linux_container.run(
        [
            "bash",
            "-c",
            "stat /etc/foundationdb/ | grep 'Access.*foundationdb'",
        ]
    )


def test_backup_restore(linux_container: Container, snapshot, tmp_path: pathlib.Path):
    linux_container.run(["fdbcli", "--exec", "writemode on; set x y"])
    assert snapshot == linux_container.run(
        ["fdbbackup", "start", "-d", "file:///tmp/fdb_backup", "-w"]
    )
    linux_container.run(["fdbcli", "--exec", "writemode on; clear x"])
    linux_container.run(
        [
            "bash",
            "-c",
            "fdbrestore start -r file://$(echo /tmp/fdb_backup/*) -w --dest-cluster-file /etc/foundationdb/fdb.cluster",
        ]
    )
    assert snapshot == linux_container.run(["fdbcli", "--exec", "get x"])
