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
from fdb.tuple import pack

if __name__ == '__main__':
    fdb.api_version(710)

def test_tenant_tuple_name(db):
    tuplename=(b'test', b'level', b'hierarchy', 3, 1.24, 'str')
    db.allocate_tenant(tuplename)

    tenant=db.open_tenant(tuplename)
    tenant[b'foo'] = b'bar'

    assert tenant[b'foo'] == b'bar'

    del tenant[b'foo']
    db.delete_tenant(tuplename)

def cleanup_tenant(db, tenant_name):
    try:
        tenant = db.open_tenant(tenant_name)
        del tenant[:]
        db.delete_tenant(tenant_name)
    except fdb.FDBError as e:
        if e.code == 2131: # tenant not found
            pass
        else:
            raise

def test_tenant_operations(db):
    cleanup_tenant(db, b'tenant1')
    cleanup_tenant(db, b'tenant2')

    db.allocate_tenant(b'tenant1')
    db.allocate_tenant(b'tenant2')

    tenant1 = db.open_tenant(b'tenant1')
    tenant2 = db.open_tenant(b'tenant2')

    db[b'tenant_test_key'] = b'no_tenant'
    tenant1[b'tenant_test_key'] = b'tenant1'
    tenant2[b'tenant_test_key'] = b'tenant2'

    tenant1_entry = db[b'\xff\xff/management/tenant_map/tenant1']
    tenant1_json = json.loads(tenant1_entry)
    prefix1 = tenant1_json['prefix'].encode('utf8')

    tenant2_entry = db[b'\xff\xff/management/tenant_map/tenant2']
    tenant2_json = json.loads(tenant2_entry)
    prefix2 = tenant2_json['prefix'].encode('utf8')

    assert tenant1[b'tenant_test_key'] == b'tenant1'
    assert db[prefix1 + b'tenant_test_key'] == b'tenant1'
    assert tenant2[b'tenant_test_key'] == b'tenant2'
    assert db[prefix2 + b'tenant_test_key'] == b'tenant2'
    assert db[b'tenant_test_key'] == b'no_tenant'

    tr1 = tenant1.create_transaction()
    try:
        del tr1[:]
        tr1.commit().wait()
    except fdb.FDBError as e:
        tr.on_error(e).wait()

    assert tenant1[b'tenant_test_key'] == None
    assert db[prefix1 + b'tenant_test_key'] == None
    assert tenant2[b'tenant_test_key'] == b'tenant2'
    assert db[prefix2 + b'tenant_test_key'] == b'tenant2'
    assert db[b'tenant_test_key'] == b'no_tenant'

    db.delete_tenant(b'tenant1')
    try:
        tenant1[b'tenant_test_key']
        assert False
    except fdb.FDBError as e:
        assert e.code == 2131 # tenant not found

    del tenant2[:]
    db.delete_tenant(b'tenant2')

    assert db[prefix1 + b'tenant_test_key'] == None
    assert db[prefix2 + b'tenant_test_key'] == None
    assert db[b'tenant_test_key'] == b'no_tenant'

    del db[b'tenant_test_key']

    assert db[b'tenant_test_key'] == None

def test_tenants(db):
    test_tenant_tuple_name(db)
    test_tenant_operations(db)

# Expect a cluster file as input. This test will write to the FDB cluster, so
# be aware of potential side effects.
if __name__ == '__main__':
    clusterFile = sys.argv[1]
    db = fdb.open(clusterFile)
    db.options.set_transaction_timeout(2000)  # 2 seconds
    db.options.set_transaction_retry_limit(3)

    test_tenants(db)
