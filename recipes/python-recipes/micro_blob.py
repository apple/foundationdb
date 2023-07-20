#
# micro_blob.py
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

import fdb
fdb.api_version(300)
db = fdb.open()


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


CHUNK_LARGE = 5

blob = fdb.Subspace(('B',))


@fdb.transactional
def write_blob(tr, data):
    if not len(data):
        return
    num_chunks = (len(data) + CHUNK_LARGE - 1) / CHUNK_LARGE
    chunk_size = (len(data) + num_chunks) / num_chunks
    chunks = [(n, n + chunk_size) for n in range(0, len(data), chunk_size)]
    for start, end in chunks:
        tr[blob[start]] = data[start:end]


@fdb.transactional
def read_blob(tr):
    value = ''
    for k, v in tr[blob.range()]:
        value += v
    return value


clear_subspace(db, blob)
write_blob(db, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
print read_blob(db)
