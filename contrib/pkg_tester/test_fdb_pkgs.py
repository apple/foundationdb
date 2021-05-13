# test_fdb_pkgs.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
import uuid

from typing import List, Optional, Union


def run(args: List[str]) -> str:
    print("$ {}".format(" ".join(map(shlex.quote, args))))
    result = subprocess.check_output(args).decode("utf-8")
    print(result, end="")
    return result


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

        # minimal privilege required to run systemd
        # https://github.com/docker/for-linux/issues/106#issuecomment-330518243
        extra_privilege = []
        if initd:
            extra_privilege = "--cap-add=SYS_ADMIN -e container=docker -v /sys/fs/cgroup:/sys/fs/cgroup".split()

        self.uid = str(uuid.uuid4())

        run(
            ["docker", "run"]
            + ["-t", "-d", "--name", self.uid]
            + extra_privilege
            + [image_name]
            + ["/usr/sbin/init" for _ in range(1) if initd]
        ).rstrip()

    def run(self, args: List[str]) -> bytes:
        return run(["docker", "exec", self.uid] + args)

    def copy_to(self, src_path: str, dst_path: str) -> None:
        run(["docker", "cp", src_path, "{}:{}".format(self.uid, dst_path)])

    def commit(self) -> Image:
        output = run(["docker", "commit", self.uid])
        uid = output.split(":")[1].rstrip()
        return Image(uid)

    def dispose(self):
        run(["docker", "rm", "-f", self.uid])


@pytest.fixture(scope="session")
def ubuntu_image_with_fdb() -> Image:
    """
    Return an image which has just the fdb deb packages installed.
    """
    debs = [
        deb
        for deb in glob.glob(
            os.path.join(os.environ.get("BUILDDIR"), "packages", "*.deb")
        )
        if "versioned" not in deb
    ]
    if not debs:
        yield None
        return

    container = None
    image = None
    try:
        container = Container("ubuntu")
        for deb in debs:
            container.copy_to(deb, "/tmp")
        container.run(["bash", "-c", "dpkg -i /tmp/*.deb"])
        container.run(["bash", "-c", "rm /tmp/*.deb"])
        image = container.commit()
        yield image
    finally:
        if container is not None:
            container.dispose()
        if image is not None:
            image.dispose()


@pytest.fixture(scope="session")
def centos_image_with_fdb() -> Image:
    """
    Return an image which has just the fdb rpm packages installed.
    """
    rpms = [
        rpm
        for rpm in glob.glob(
            os.path.join(os.environ.get("BUILDDIR"), "packages", "*.rpm")
        )
        if "versioned" not in rpm
    ]
    if not rpms:
        yield None
        return

    container = None
    image = None
    try:
        container = Container("centos", initd=True)
        for rpm in rpms:
            container.copy_to(rpm, "/tmp")
        container.run(["bash", "-c", "yum install -y /tmp/*.rpm"])
        container.run(["bash", "-c", "rm /tmp/*.rpm"])
        image = container.commit()
        yield image
    finally:
        if container is not None:
            container.dispose()
        if image is not None:
            image.dispose()


def pytest_generate_tests(metafunc):
    if "linux_container" in metafunc.fixturenames:
        metafunc.parametrize("linux_container", ["ubuntu", "centos"], indirect=True)


@pytest.fixture()
def linux_container(request, ubuntu_image_with_fdb, centos_image_with_fdb) -> Container:
    container: Optional[Container] = None
    try:
        if request.param == "ubuntu":
            if ubuntu_image_with_fdb is None:
                pytest.skip("No debian packages available to test")
            container = Container(ubuntu_image_with_fdb)
            container.run(
                ["service", "foundationdb", "start"]
            )  # outside docker this shouldn't be necessary
        elif request.param == "centos":
            if centos_image_with_fdb is None:
                pytest.skip("No rpm packages available to test")
            container = Container(centos_image_with_fdb, initd=True)
        else:
            assert False
        yield container
    finally:
        if container is not None:
            container.dispose()


#################### BEGIN ACTUAL TESTS ####################


def test_db_available(linux_container: Container):
    linux_container.run(["fdbcli", "--exec", "status"])


def test_write(linux_container: Container, snapshot):
    linux_container.run(["fdbcli", "--exec", "writemode on; set x y"])
    assert snapshot == linux_container.run(["fdbcli", "--exec", "get x"])


def test_fdbcli_help_text(linux_container: Container, snapshot):
    assert snapshot == linux_container.run(["fdbcli", "--help"])


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
            "fdbrestore start -r file://$(echo /tmp/fdb_backup/*) -w --dest_cluster_file /etc/foundationdb/fdb.cluster",
        ]
    )
    assert snapshot == linux_container.run(["fdbcli", "--exec", "get x"])
