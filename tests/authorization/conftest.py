#!/usr/bin/python
#
# conftest.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#
import fdb
import pytest
import subprocess
import admin_server
import base64
import glob
import time
from local_cluster import TLSConfig
from tmp_cluster import TempCluster
from typing import Union
from util import random_alphanum_str, random_alphanum_bytes, to_str, to_bytes
import xml.etree.ElementTree as ET

fdb.api_version(720)

cluster_scope = "module"

def pytest_addoption(parser):
    parser.addoption(
            "--build-dir", action="store", dest="build_dir", help="FDB build directory", required=True)
    parser.addoption(
            "--kty", action="store", choices=["EC", "RSA"], default="EC", dest="kty", help="Token signature algorithm")
    parser.addoption(
            "--trusted-client",
            action="store_true",
            default=False,
            dest="trusted_client",
            help="Whether client shall be configured trusted, i.e. mTLS-ready")
    parser.addoption(
            "--public-key-refresh-interval",
            action="store",
            default=1,
            dest="public_key_refresh_interval",
            help="How frequently server refreshes authorization public key file")
    parser.addoption(
            "--force-multi-version-client",
            action="store_true",
            default=False,
            dest="force_multi_version_client",
            help="Whether to force multi-version client mode")
    parser.addoption(
            "--use-grv-cache",
            action="store_true",
            default=False,
            dest="use_grv_cache",
            help="Whether to make client use cached GRV from database context")

@pytest.fixture(scope="session")
def build_dir(request):
    return request.config.option.build_dir

@pytest.fixture(scope="session")
def kty(request):
    return request.config.option.kty

@pytest.fixture(scope="session")
def trusted_client(request):
    return request.config.option.trusted_client

@pytest.fixture(scope="session")
def public_key_refresh_interval(request):
    return request.config.option.public_key_refresh_interval

@pytest.fixture(scope="session")
def force_multi_version_client(request):
    return request.config.option.force_multi_version_client

@pytest.fixture(scope="session")
def use_grv_cache(request):
    return request.config.option.use_grv_cache

@pytest.fixture(scope="session")
def kid():
    return random_alphanum_str(12)

@pytest.fixture(scope=cluster_scope)
def admin_ipc():
    server = admin_server.Server()
    server.start()
    yield server
    server.join()

@pytest.fixture(autouse=True, scope=cluster_scope)
def cluster(admin_ipc, build_dir, public_key_refresh_interval, trusted_client, force_multi_version_client, use_grv_cache):
    with TempCluster(
            build_dir=build_dir,
            tls_config=TLSConfig(server_chain_len=3, client_chain_len=2),
            authorization_kty="EC",
            authorization_keypair_id="authz-key",
            remove_at_exit=True,
            custom_config={
                "knob-public-key-file-refresh-interval-seconds": public_key_refresh_interval,
            }) as cluster:
        keyfile = str(cluster.client_key_file)
        certfile = str(cluster.client_cert_file)
        cafile = str(cluster.server_ca_file)
        logdir = str(cluster.log)
        fdb.options.set_tls_key_path(keyfile if trusted_client else "")
        fdb.options.set_tls_cert_path(certfile if trusted_client else "")
        fdb.options.set_tls_ca_path(cafile)
        fdb.options.set_trace_enable(logdir)
        fdb.options.set_trace_file_identifier("testclient")
        if force_multi_version_client:
            fdb.options.set_disable_client_bypass()
        admin_ipc.request("configure_client", [force_multi_version_client, use_grv_cache, logdir])
        admin_ipc.request("configure_tls", [keyfile, certfile, cafile])
        admin_ipc.request("connect", [str(cluster.cluster_file)])
        yield cluster
        err_count = {}
        for file in glob.glob(str(cluster.log.joinpath("*.xml"))):
            lineno = 1
            for line in open(file):
                try:
                    doc = ET.fromstring(line)
                except:
                    continue
                if doc.attrib.get("Severity", "") == "40":
                    ev_type = doc.attrib.get("Type", "[unset]")
                    err = doc.attrib.get("Error", "[unset]")
                    tup = (file, ev_type, err)
                    err_count[tup] = err_count.get(tup, 0) + 1
                lineno += 1
        print("Sev40 Summary:")
        if len(err_count) == 0:
            print("  No errors")
        else:
            for tup, count in err_count.items():
                print("  {}: {}".format(tup, count))

@pytest.fixture
def db(cluster, admin_ipc):
    db = fdb.open(str(cluster.cluster_file))
    db.options.set_transaction_timeout(2000) # 2 seconds
    db.options.set_transaction_retry_limit(3)
    yield db
    admin_ipc.request("cleanup_database")
    db = None

@pytest.fixture
def tenant_gen(db, admin_ipc):
    def fn(tenant):
        tenant = to_bytes(tenant)
        admin_ipc.request("create_tenant", [tenant])
    return fn

@pytest.fixture
def tenant_del(db, admin_ipc):
    def fn(tenant):
        tenant = to_str(tenant)
        admin_ipc.request("delete_tenant", [tenant])
    return fn

@pytest.fixture
def default_tenant(tenant_gen, tenant_del):
    tenant = random_alphanum_bytes(8)
    tenant_gen(tenant)
    yield tenant
    tenant_del(tenant)

@pytest.fixture
def tenant_tr_gen(db, use_grv_cache):
    def fn(tenant):
        tenant = db.open_tenant(to_bytes(tenant))
        tr = tenant.create_transaction()
        if use_grv_cache:
            tr.options.set_use_grv_cache()
        return tr
    return fn

@pytest.fixture
def token_claim_1h(db):
    # JWT claim that is valid for 1 hour since time of invocation
    def fn(tenant_name: Union[bytes, str]):
        tenant = db.open_tenant(to_bytes(tenant_name))
        tenant_id = tenant.get_id().wait()
        now = time.time()
        return {
            "iss": "fdb-authz-tester",
            "sub": "authz-test",
            "aud": ["tmp-cluster"],
            "iat": now,
            "nbf": now - 1,
            "exp": now + 60 * 60,
            "jti": random_alphanum_str(10),
            "tenants": [to_str(base64.b64encode(tenant_id.to_bytes(8, "big")))],
        }
    return fn
