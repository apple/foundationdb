#
# dirtest2.py
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

import fdb, fdb.tuple
fdb.api_version(23)

from subspace import Subspace
from directory import directory, DirectoryLayer

def is_error(f):
    try:
        f()
        return False
    except:
        return True

db = fdb.open()
del db[:]

print directory.create( db, 'evil', prefix="\x14" )
directory = DirectoryLayer( content_subspace = Subspace(rawPrefix="\x01") )

# Make a new directory
stuff = directory.create( db, ('stuff',) )
print 'stuff is in', stuff
print 'stuff[0] is', fdb.tuple.unpack( stuff[0].key() )
#assert stuff.key() == "\x01\x14"

# Open it again
stuff2 = directory.open( db, ('stuff',) )
assert stuff2.key() == stuff.key()

# Make another directory
items = directory.create_or_open( db, ('items',) )
print 'items are in', items
#assert items.key() == "\x01\x15\x01"

# List the root directory
assert directory.list(db, ()) == ['evil','items','stuff']

# Move everything into an 'app' directory
app = directory.create( db, ('app',) )
directory.move( db, ('stuff',), ('app','stuff') )
directory.move( db, ('items',), ('app','items') )

# Make a directory in a hard-coded place
special = directory.create_or_open( db, ('app', 'special'), prefix="\x00" )
assert special.key() == "\x00"

assert directory.list(db, ()) == ['app','evil']
assert directory.list(db, ("app",)) == ['items', 'special', 'stuff']

assert directory.open( db, ('app', 'stuff') ).key() == stuff.key()

# Destroy the stuff directory
directory.remove( db, ('app', 'stuff') )
assert is_error( lambda: directory.open( db, ('app','stuff')) )
assert directory.list(db, ("app",)) == ['items', 'special']

# Test that items is still OK
items2 = directory.create_or_open( db, ('app','items') )
assert items.key() == items.key()
