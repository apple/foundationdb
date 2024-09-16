#
# micro_table.py
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
#

import fdb
fdb.api_version(300)
db = fdb.open()

table = fdb.Subspace(('T',))
row_index = table['R']
col_index = table['C']


def _pack(value):
    return fdb.tuple.pack((value,))


def _unpack(value):
    return fdb.tuple.unpack(value)[0]


@fdb.transactional
def table_set_cell(tr, row, column, value):
    tr[row_index[row][column]] = _pack(value)
    tr[col_index[column][row]] = _pack(value)


@fdb.transactional
def table_get_cell(tr, row, column):
    return tr[row_index[row][column]]


@fdb.transactional
def table_set_row(tr, row, cols):
    del tr[row_index[row].range()]
    for c, v in cols.iteritems():
        table_set_cell(tr, row, c, v)


@fdb.transactional
def table_get_row(tr, row):
    cols = {}
    for k, v in tr[row_index[row].range()]:
        r, c = row_index.unpack(k)
        cols[c] = _unpack(v)
    return cols


@fdb.transactional
def table_get_col(tr, col):
    rows = {}
    for k, v in tr[col_index[col].range()]:
        c, r = col_index.unpack(k)
        rows[r] = _unpack(v)
    return rows


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())

# clear_subspace(db, table)
