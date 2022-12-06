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
from local_cluster import TLSConfig
from tmp_cluster import TempCluster
from authz_util import private_key_gen, public_keyset_from_keys
from typing import Union
from util import random_alphanum_str, random_alphanum_bytes, to_str, to_bytes

fdb.api_version(730)

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
def kid():
    return random_alphanum_str(12)

@pytest.fixture(scope=cluster_scope)
def admin_ipc():
    server = admin_server.Server()
    server.start()
    yield server
    server.join()

@pytest.fixture(autouse=True, scope=cluster_scope)
def cluster(admin_ipc, build_dir, public_key_refresh_interval, trusted_client):
    with TempCluster(
            build_dir=build_dir,
            tls_config=TLSConfig(server_chain_len=3, client_chain_len=2),
            authorization_kty="EC",
            remove_at_exit=True,
            custom_config={
                "knob-public-key-file-refresh-interval-seconds": public_key_refresh_interval,
            }) as cluster:
        keyfile = str(cluster.client_key_file)
        certfile = str(cluster.client_cert_file)
        cafile = str(cluster.server_ca_file)
        fdb.options.set_tls_key_path(keyfile if trusted_client else "")
        fdb.options.set_tls_cert_path(certfile if trusted_client else "")
        fdb.options.set_tls_ca_path(cafile)
        fdb.options.set_trace_enable()
        admin_ipc.request("configure_tls", [keyfile, certfile, cafile])
        admin_ipc.request("connect", [str(cluster.cluster_file)])
        yield cluster

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
def tenant_tr_gen(db):
    def fn(tenant):
        tenant = db.open_tenant(to_bytes(tenant))
        return tenant.create_transaction()
    return fn
