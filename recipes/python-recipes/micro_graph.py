#
# micro_graph.py
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

graph = fdb.Subspace(('G',))
edge = graph['E']
inverse = graph['I']


@fdb.transactional
def set_edge(tr, node, neighbor):
    tr[edge[node][neighbor]] = ''
    tr[inverse[neighbor][node]] = ''


@fdb.transactional
def del_edge(tr, node, neighbor):
    del tr[edge[node][neighbor]]
    del tr[inverse[neighbor][node]]


@fdb.transactional
def get_out_neighbors(tr, node):
    return [edge.unpack(k)[1] for k, _ in tr[edge[node].range()]]


@fdb.transactional
def get_in_neighbors(tr, node):
    return [inverse.unpack(k)[1] for k, _ in tr[inverse[node].range()]]


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


clear_subspace(db, graph)
