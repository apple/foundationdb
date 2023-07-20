#
# micro_doc.py
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

import itertools
import json
import random

doc_space = fdb.Subspace(('D',))

EMPTY_OBJECT = -2
EMPTY_ARRAY = -1


def to_tuples(item):
    if item == {}:
        return [(EMPTY_OBJECT, None)]
    elif item == []:
        return [(EMPTY_ARRAY, None)]
    elif type(item) == dict:
        return [(k,) + sub for k, v in item.iteritems() for sub in to_tuples(v)]
    elif type(item) == list:
        return [(k,) + sub for k, v in enumerate(item) for sub in to_tuples(v)]
    else:
        return [(item,)]


def from_tuples(tuples):
    if not tuples:
        return {}
    first = tuples[0]  # Determine kind of object from first tuple
    if len(first) == 1:
        return first[0]  # Primitive value
    if first == (EMPTY_OBJECT, None):
        return {}
    if first == (EMPTY_ARRAY, None):
        return []
    # For an object or array, we need to group the tuples by their first element
    groups = [list(g) for k, g in itertools.groupby(tuples, lambda t:t[0])]
    if first[0] == 0:   # array
        return [from_tuples([t[1:] for t in g]) for g in groups]
    else:    # object
        return dict((g[0][0], from_tuples([t[1:] for t in g])) for g in groups)


@fdb.transactional
def insert_doc(tr, doc):
    if type(doc) == str:
        doc = json.loads(doc)
    if 'doc_id' not in doc:
        new_id = _get_new_id(tr)
        doc['doc_id'] = new_id
    for tup in to_tuples(doc):
        tr[doc_space.pack((doc['doc_id'],) + tup[:-1])] = fdb.tuple.pack((tup[-1],))
    return doc['doc_id']


@fdb.transactional
def _get_new_id(tr):
    found = False
    while (not found):
        new_id = random.randint(0, 100000000)
        found = True
        for _ in tr[doc_space[new_id].range()]:
            found = False
            break
    return new_id


@fdb.transactional
def get_doc(tr, doc_id, prefix=()):
    v = tr[doc_space.pack((doc_id,) + prefix)]
    if v.present():
        return from_tuples([prefix + fdb.tuple.unpack(v)])
    else:
        return from_tuples([doc_space.unpack(k)[1:] + fdb.tuple.unpack(v)
                            for k, v in tr[doc_space.range((doc_id,) + prefix)]])


@fdb.transactional
def print_subspace(tr, subspace):
    for k, v in tr[subspace.range()]:
        print subspace.unpack(k), fdb.tuple.unpack(v)[0]


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


clear_subspace(db, doc_space)


def smoke_test():
    h1 = {'user': {'jones': {'friend_of': 'smith', 'group': ['sales', 'service']}, 'smith': {'friend_of': 'jones', 'group': ['dev', 'research']}}}
    id = insert_doc(db, h1)
    print get_doc(db, id, ('user', 'smith', 'group'))


if __name__ == "__main__":
    smoke_test()
