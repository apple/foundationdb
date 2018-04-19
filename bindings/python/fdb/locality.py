#
# locality.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

# FoundationDB Python API

"""Documentation for this API can be found at
https://apple.github.io/foundationdb/api-python.html"""

from fdb import impl as _impl


def _get_boundary_keys(db_or_tr, begin, end):
    if isinstance(db_or_tr, _impl.Transaction):
        tr = db_or_tr.db.create_transaction()
        # This does not guarantee transactionality because of the exception handling below,
        # but it does hide latency for the new transaction's start
        tr.set_read_version(db_or_tr.get_read_version().wait())
    else:
        tr = db_or_tr.create_transaction()
    first_time = True
    while begin < end:
        try:
            lastbegin = begin
            tr.options.set_read_system_keys()
            tr.options.set_lock_aware()
            kvs = tr.snapshot.get_range(b'\xff' + b'/keyServers/' + begin, b'\xff' + b'/keyServers/' + end)
            if first_time:
                first_time = False
                yield None  # trick to get the above get_range to be asynchronously dispatched before get_boundary_keys() returns.
            for kv in kvs:
                yield kv.key[13:]
                begin = kv.key[13:] + b'\x00'
            begin = end
        except _impl.FDBError as e:
            # if we get a transaction_too_old and *something* has happened, then we are no longer transactional
            if e.code == 1007 and begin != lastbegin:
                tr = tr.db.create_transaction()
            else:
                tr.on_error(e).wait()


def get_boundary_keys(db_or_tr, begin, end):
    begin = _impl.keyToBytes(begin)
    end = _impl.keyToBytes(end)

    gen = _get_boundary_keys(db_or_tr, begin, end)
    try:
        next(gen)
    except StopIteration:  # if _get_boundary_keys() never yields a value, e.g. begin > end
        return (x for x in list())
    return gen


@_impl.transactional
def get_addresses_for_key(tr, key):
    keyBytes = _impl.keyToBytes(key)
    return _impl.FutureStringArray(tr.capi.fdb_transaction_get_addresses_for_key(tr.tpointer, keyBytes, len(keyBytes)))
