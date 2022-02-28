#!/usr/bin/python
#
# tenant_tuple_name_tests.py
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
from fdb.tuple import pack

if __name__ == '__main__':
    fdb.api_version(710)

def test_tenant_tuple_name(db):
    tuplename=(b'test', b'level', b'hierarchy', 3, 1.24, 'str')
    db.allocate_tenant(tuplename).wait()

    tenant=db.open_tenant(tuplename)
    tenant[b'foo'] = b'bar'

    assert tenant[b'foo'] == b'bar'

    del tenant[b'foo']
    db.delete_tenant(tuplename).wait()

# Expect a cluster file as input. This test will write to the FDB cluster, so
# be aware of potential side effects.
if __name__ == '__main__':
    clusterFile = sys.argv[1]
    db = fdb.open(clusterFile)
    db.options.set_transaction_timeout(2000)  # 2 seconds
    db.options.set_transaction_retry_limit(3)
    test_tenant_tuple_name(db)
