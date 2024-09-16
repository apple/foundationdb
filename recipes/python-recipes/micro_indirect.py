#
# micro_indirect.py
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


class Workspace(object):

    def __init__(self, directory, db):
        self.dir = directory
        self.db = db

    def __enter__(self):
        return self.dir.create_or_open(self.db, (u'new',))

    def __exit__(self, *exc):
        self._update(self.db)

    @fdb.transactional
    def _update(self, tr):
        self.dir.remove(tr, (u'current'))
        self.dir.move(tr, (u'new'), (u'current'))

    @property
    def current(self):
        return self.dir.create_or_open(self.db, (u'current',))


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


@fdb.transactional
def print_subspace(tr, subspace):
    for k, v in tr[subspace.range()]:
        print subspace.unpack(k), v


def smoke_test():
    db = fdb.open()
    working_dir = fdb.directory.create_or_open(db, (u'working',))
    workspace = Workspace(working_dir, db)
    current = workspace.current
    clear_subspace(db, current)
    db[current[1]] = 'a'
    db[current[2]] = 'b'
    print "contents:"
    print_subspace(db, current)
    with workspace as newspace:
        clear_subspace(db, newspace)
        db[newspace[3]] = 'c'
        db[newspace[4]] = 'd'
    print "contents:"
    print_subspace(db, workspace.current)


if __name__ == "__main__":
    smoke_test()
