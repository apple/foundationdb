#!/usr/bin/python
#
# tenant_tests.py
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
import sys
import json
import base64
from fdb.tuple import pack

if __name__ == "__main__":
    fdb.api_version(fdb.LATEST_API_VERSION)


def cleanup_tenant(db, tenant_name):
    try:
        tenant = db.open_tenant(tenant_name)
        del tenant[:]
        fdb.tenant_management.delete_tenant(db, tenant_name)
    except fdb.FDBError as e:
        if e.code == 2131:  # tenant not found
            pass
        else:
            raise


def test_tenant_tuple_name(db):
    tuplename = (b"test", b"level", b"hierarchy", 3, 1.24, "str")
    cleanup_tenant(db, tuplename)

    fdb.tenant_management.create_tenant(db, tuplename)

    tenant = db.open_tenant(tuplename)
    tenant[b"foo"] = b"bar"

    assert tenant[b"foo"] == b"bar"

    del tenant[b"foo"]
    fdb.tenant_management.delete_tenant(db, tuplename)


def test_tenant_operations(db):
    cleanup_tenant(db, b"tenant1")
    cleanup_tenant(db, b"tenant2")

    fdb.tenant_management.create_tenant(db, b"tenant1")
    fdb.tenant_management.create_tenant(db, b"tenant2")

    tenant_list = fdb.tenant_management.list_tenants(db, b"a", b"z", 10).to_list()
    assert tenant_list[0].key == b"tenant1"
    assert tenant_list[1].key == b"tenant2"

    t1_entry = tenant_list[0].value
    t1_json = json.loads(t1_entry)
    p1 = base64.b64decode(t1_json["prefix"]["base64"])

    t2_entry = tenant_list[1].value
    t2_json = json.loads(t2_entry)
    p2 = base64.b64decode(t2_json["prefix"]["base64"])

    tenant1 = db.open_tenant(b"tenant1")
    tenant2 = db.open_tenant(b"tenant2")

    db[b"tenant_test_key"] = b"no_tenant"
    tenant1[b"tenant_test_key"] = b"tenant1"
    tenant2[b"tenant_test_key"] = b"tenant2"

    tenant1_entry = db[b"\xff\xff/management/tenant/map/tenant1"]
    tenant1_json = json.loads(tenant1_entry)
    prefix1 = base64.b64decode(tenant1_json["prefix"]["base64"])
    assert prefix1 == p1

    tenant2_entry = db[b"\xff\xff/management/tenant/map/tenant2"]
    tenant2_json = json.loads(tenant2_entry)
    prefix2 = base64.b64decode(tenant2_json["prefix"]["base64"])
    assert prefix2 == p2

    assert tenant1[b"tenant_test_key"] == b"tenant1"
    assert db[prefix1 + b"tenant_test_key"] == b"tenant1"
    assert tenant2[b"tenant_test_key"] == b"tenant2"
    assert db[prefix2 + b"tenant_test_key"] == b"tenant2"
    assert db[b"tenant_test_key"] == b"no_tenant"

    tr1 = tenant1.create_transaction()
    try:
        del tr1[:]
        tr1.commit().wait()
    except fdb.FDBError as e:
        tr1.on_error(e).wait()

    assert tenant1[b"tenant_test_key"] == None
    assert db[prefix1 + b"tenant_test_key"] == None
    assert tenant2[b"tenant_test_key"] == b"tenant2"
    assert db[prefix2 + b"tenant_test_key"] == b"tenant2"
    assert db[b"tenant_test_key"] == b"no_tenant"

    fdb.tenant_management.delete_tenant(db, b"tenant1")
    try:
        tenant1[b"tenant_test_key"]
        assert False
    except fdb.FDBError as e:
        assert e.code == 2131  # tenant not found

    del tenant2[:]
    fdb.tenant_management.delete_tenant(db, b"tenant2")

    assert db[prefix1 + b"tenant_test_key"] == None
    assert db[prefix2 + b"tenant_test_key"] == None
    assert db[b"tenant_test_key"] == b"no_tenant"

    del db[b"tenant_test_key"]

    assert db[b"tenant_test_key"] == None


def test_tenant_operation_retries(db):
    cleanup_tenant(db, b"tenant1")
    cleanup_tenant(db, b"tenant2")

    # Test that the tenant creation only performs the existence check once
    fdb.tenant_management._create_tenant_impl(
        db, b"tenant1", [], force_existence_check_maybe_committed=True
    )

    # An attempt to create the tenant again should fail
    try:
        fdb.tenant_management.create_tenant(db, b"tenant1")
        assert False
    except fdb.FDBError as e:
        assert e.code == 2132  # tenant already exists

    # Using a transaction skips the existence check
    tr = db.create_transaction()
    fdb.tenant_management.create_tenant(tr, b"tenant1")

    # Test that a concurrent tenant creation doesn't interfere with the existence check logic
    tr = db.create_transaction()
    existence_check_marker = []
    fdb.tenant_management._create_tenant_impl(tr, b"tenant2", existence_check_marker)

    fdb.tenant_management.create_tenant(db, b"tenant2")

    tr = db.create_transaction()
    try:
        fdb.tenant_management._create_tenant_impl(
            tr, b"tenant2", existence_check_marker
        )
        tr.commit().wait()
    except fdb.FDBError as e:
        tr.on_error(e).wait()

    # Test that tenant deletion only performs the existence check once
    fdb.tenant_management._delete_tenant_impl(
        db, b"tenant1", [], force_existence_check_maybe_committed=True
    )

    # An attempt to delete the tenant again should fail
    try:
        fdb.tenant_management.delete_tenant(db, b"tenant1")
        assert False
    except fdb.FDBError as e:
        assert e.code == 2131  # tenant not found

    # Using a transaction skips the existence check
    tr = db.create_transaction()
    fdb.tenant_management.delete_tenant(tr, b"tenant1")

    # Test that a concurrent tenant deletion doesn't interfere with the existence check logic
    tr = db.create_transaction()
    existence_check_marker = []
    fdb.tenant_management._delete_tenant_impl(tr, b"tenant2", existence_check_marker)

    fdb.tenant_management.delete_tenant(db, b"tenant2")

    tr = db.create_transaction()
    try:
        fdb.tenant_management._delete_tenant_impl(
            tr, b"tenant2", existence_check_marker
        )
        tr.commit().wait()
    except fdb.FDBError as e:
        tr.on_error(e).wait()


def test_tenants(db):
    test_tenant_tuple_name(db)
    test_tenant_operations(db)
    test_tenant_operation_retries(db)


# Expect a cluster file as input. This test will write to the FDB cluster, so
# be aware of potential side effects.
if __name__ == "__main__":
    clusterFile = sys.argv[1]
    db = fdb.open(clusterFile)
    db.options.set_transaction_timeout(2000)  # 2 seconds
    db.options.set_transaction_retry_limit(3)

    test_tenants(db)
