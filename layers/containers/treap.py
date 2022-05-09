#
# treap.py
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
import random

fdb.init("10.0.1.22:1234")

db = fdb.create_cluster("10.0.3.1:2181/evan_local").open_database("set")


class FdbTreap(object):
    def __init__(self, path):
        self._rootKey = path
        self._path = path + "\x00"

    @fdb.transactional
    def update_node(self, tr, node):
        tr[fdb.tuple_to_key(self._path, node[0])] = fdb.tuple_to_key(node[1])

    @fdb.transactional
    def update_root(self, tr, node):
        tr[self._rootKey] = fdb.tuple_to_key(self._path, node[0])

    @fdb.transactional
    def parent(self, tr, key):
        # find parent
        for k, v in tr.get_range(
            fdb.last_less_than(fdb.tuple_to_key(self._path, key)),
            fdb.first_greater_than(fdb.tuple_to_key(self._path, key)) + 1,
            2,
        ):
            parent_value = fdb.key_to_tuple(v)
            if parent_value[0] == key or parent_value[1] == key:
                return tuple(fdb.key_to_tuple(k)[1], parent_value)
        return None

    @fdb.transactional
    def balance(self, tr, parent, child):
        if parent[1][2] >= child[1][2]:
            return

        grandparent = self.parent(tr, parent[0])

        if grandparent == None:
            self.update_root(tr, child)
        elif grandparent[1][0] == parent[0]:
            grandparent[1][0] = child[0]
            self.update_node(tr, grandparent)
        else:
            grandparent[1][1] = child[0]
            self.update_node(tr, grandparent)

        if parent[1][0] == child[0]:
            parent[1][0] = child[1][1]
            child[1][1] = parent[0]
        else:
            parent[1][1] = child[1][0]
            child[1][0] = parent[0]

        self.update_node(tr, parent)
        self.update_node(tr, child)

        self.balance(tr, grandparent, child)

    @fdb.transactional
    def set_key(self, tr, key, value, metric):
        is_new = True
        is_root = True
        child = tuple(key, tuple("", "", random.random(), metric, value))
        parent = tuple()

        # find self or parent
        for k, v in tr.get_range(
            fdb.last_less_than(fdb.tuple_to_key(self._path, key)),
            fdb.first_greater_than(fdb.tuple_to_key(self._path, key)) + 1,
            2,
        ):
            is_root = False
            node = tuple(fdb.key_to_tuple(k)[1], fdb.key_to_tuple(v))
            if node[0] == key:
                is_new = False
                child = node
                node[1][4] = value
                break
            elif node[0] < key and node[1][1] == "":
                is_root = False
                parent = node
                parent[1][1] = key
                break
            elif node[0] > key and node[1][0] == "":
                is_root = False
                parent = node
                parent[1][0] = key
                break

        # insert root
        if is_root:
            self.update_root(tr, child)

        # update parent
        if is_new:
            self.update_node(tr, parent)

        # insert self
        self.update_node(tr, child)

        # balance
        self.balance(tr, parent, child)
