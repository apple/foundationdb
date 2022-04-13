#!/usr/bin/python
#
# size_limit_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

if __name__ == '__main__':
    fdb.api_version(720)

@fdb.transactional
def setValue(tr, key, value):
    tr[key] = value

@fdb.transactional
def setValueWithLimit(tr, key, value, limit):
    tr.options.set_size_limit(limit)
    tr[key] = value

def test_size_limit_option(db):
    value = b'a' * 1024

    setValue(db, b't1', value)
    assert(value == db[b't1'])

    try:
        db.options.set_transaction_size_limit(1000)
        setValue(db, b't2', value)
        assert(False)  # not reached
    except fdb.FDBError as e:
        assert(e.code == 2101)  # Transaction exceeds byte limit (2101)

    # Per transaction option overrides database option
    db.options.set_transaction_size_limit(1000000)
    try:
        setValueWithLimit(db, b't3', value, 1000)
        assert(False)  # not reached
    except fdb.FDBError as e:
        assert(e.code == 2101)  # Transaction exceeds byte limit (2101)

    # DB default survives on_error reset
    db.options.set_transaction_size_limit(1000)
    tr = db.create_transaction()
    try:
        tr[b't4'] = b'bar'
        tr.on_error(fdb.FDBError(1007)).wait()
        setValue(tr, b't4', value)
        tr.commit().wait()
        assert(False)  # not reached
    except fdb.FDBError as e:
        assert(e.code == 2101)  # Transaction exceeds byte limit (2101)

@fdb.transactional
def test_get_approximate_size(tr):
    tr[b'key1'] = b'value1'
    s1 = tr.get_approximate_size().wait()

    tr[b'key2'] = b'value2'
    s2 = tr.get_approximate_size().wait()
    assert(s1 < s2)

    tr.clear(b'key3')
    s3 = tr.get_approximate_size().wait()
    assert(s2 < s3)

    tr.add_read_conflict_key(b'key3+')
    s4 = tr.get_approximate_size().wait()
    assert(s3 < s4)

    tr.add_write_conflict_key(b'key4')
    s5 = tr.get_approximate_size().wait()
    assert(s4 < s5)

# Expect a cluster file as input. This test will write to the FDB cluster, so
# be aware of potential side effects.
if __name__ == '__main__':
    clusterFile = sys.argv[1]
    db = fdb.open(clusterFile)
    db.options.set_transaction_timeout(2000)  # 2 seconds
    db.options.set_transaction_retry_limit(3)
    test_size_limit_option(db)
    test_get_approximate_size(db)
