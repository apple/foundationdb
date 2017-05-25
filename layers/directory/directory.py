#
# directory.py
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

from subspace import Subspace
import fdb, fdb.tuple
import random, struct

fdb.api_version(100)

#TODO: Error class

class HighContentionAllocator (object):
    def __init__(self, subspace):
        self.counters = subspace[0]
        self.recent = subspace[1]

    @fdb.transactional
    def allocate( self, tr ):
        """Returns a byte string which
            (1) has never and will never be returned by another call to HighContentionAllocator.allocate() on the same subspace
            (2) is nearly as short as possible given the above"""

        [(start, count)] = [ (self.counters.unpack(k)[0],struct.unpack("<q",v)[0]) for k,v in tr.snapshot.get_range( self.counters.range().start, self.counters.range().stop, limit=1, reverse=True ) ] or [ (0,0) ]

        window = self._window_size(start)
        if (count+1)*2 >= window:
            # Advance the window
            del tr[ self.counters : self.counters[start].key()+chr(0) ]
            start += window
            del tr[ self.recent : self.recent[start] ]
            window = self._window_size(start)

        # Increment the allocation count for the current window
        tr.add( self.counters[start], struct.pack("<q", 1) )

        while True:
            # As of the snapshot we are reading from, the window is less than half full, so
            # this should be expected 2 tries.  Under high contention (and when the window advances),
            # there is an additional subsequent risk of conflict for this transaction.
            candidate = random.randint( start, start+window )
            if tr[ self.recent[candidate] ] == None:
                tr[ self.recent[candidate] ] = ""
                return fdb.tuple.pack( (candidate,) )

    def _window_size(self, start):
        # Larger window sizes are better for high contention, smaller for keeping the keys small.  But if
        # there are lots of allocations the keys can't be too small.  So start small and scale up.  We don't
        # want this to ever get *too* big because we have to store about window_size/2 recent items.
        if start < 255: return 64
        if start < 65535: return 1024
        return 8192

class DirectoryLayer (object):
    def __init__(self, node_subspace = Subspace( rawPrefix="\xfe" ), content_subspace = Subspace() ):
        self.content_subspace = content_subspace
        self.node_subspace = node_subspace
        # The root node is the one whose contents are the node subspace
        self.root_node = self.node_subspace[ self.node_subspace.key() ]
        self.allocator = HighContentionAllocator( self.root_node['hca'] )

    @fdb.transactional
    def create_or_open( self, tr, path, layer=None, prefix=None, allow_create=True, allow_open=True ):
        """Opens the directory with the given path.
        If the directory does not exist, it is created (creating parent directories if necessary).
        If prefix is specified, the directory is created with the given physical prefix; otherwise a prefix is allocated automatically.
        If layer is specified, it is checked against the layer of an existing directory or set as the layer of a new directory."""
        if isinstance(path, str): path=(path,)
        if not path: raise ValueError( "The root directory may not be opened." )  # Because it contains node metadata!
        existing_node = self._find(tr, path)
        if existing_node:
            if not allow_open: raise ValueError("The directory already exists.")
            existing_layer = tr[ existing_node['layer'].key() ]
            if layer and existing_layer and existing_layer != layer:
                raise ValueError( "The directory exists but was created with an incompatible layer." )
            return self._contents_of_node(existing_node, path, existing_layer)
        if not allow_create: raise ValueError("The directory does not exist.")

        if prefix==None:
            prefix = self.allocator.allocate(tr)

        if not self._is_prefix_free(tr, prefix):
            raise ValueError("The given prefix is already in use.")

        if path[:-1]:
            parent_node = self._node_with_prefix( self.create_or_open(tr, path[:-1], layer=None).key() )
        else:
            parent_node = self.root_node
        #parent_node = self._find(tr, path[:-1])
        if not parent_node:
            print repr(path[:-1])
            raise ValueError("The parent directory doesn't exist.")

        node = self._node_with_prefix(prefix)
        tr[ parent_node[self.SUBDIRS][ path[-1] ].key() ] = prefix
        if layer: tr[ node['layer'].key() ] = layer

        return self._contents_of_node(node, path, layer)

    def open( self, db_or_tr, path, layer=None ):
        """Opens the directory with the given path.
        If the directory does not exist, an error is raised.
        If layer is specified, and a different layer was specified when the directory was created, an error is raised."""
        return self.create_or_open(db_or_tr, path, layer, allow_create=False)
    def create( self, db_or_tr, path, layer=None, prefix=None ):
        """Creates a directory with the given path (creating parent directories if necessary).
        If the given directory already exists, an error is raised.
        If prefix is specified, the directory is created with the given physical prefix; otherwise a prefix is allocated automatically.
        If layer is specified, it is recorded with the directory and will be checked by future calls to open."""
        return self.create_or_open(db_or_tr, path, layer, prefix, allow_open=False)

    @fdb.transactional
    def move( self, tr, old_path, new_path ):
        """Moves the directory found at `old_path` to `new_path`.
        There is no effect on the physical prefix of the given directory, or on clients that already have the directory open.
        If the old directory does not exist, a directory already exists at `new_path`, or the parent directory of `new_path`
        does not exist, an error is raised."""
        if isinstance(old_path, str): old_path=(old_path,)
        if isinstance(new_path, str): new_path=(new_path,)
        if self._find(tr, new_path): raise ValueError( "The destination directory already exists.  Remove it first." )
        old_node = self._find(tr, old_path)
        if not old_node: raise ValueError("The source directory does not exist.")
        parent_node = self._find(tr, new_path[:-1] )
        if not parent_node: raise ValueError( "The parent of the destination directory does not exist.  Create it first." )
        tr[ parent_node[self.SUBDIRS][ new_path[-1] ].key() ] = self._contents_of_node( old_node, None ).key()
        self._remove_from_parent( tr, old_path )
        return self._contents_of_node( old_node, new_path, tr[ old_node['layer'].key() ] )

    @fdb.transactional
    def remove( self, tr, path ):
        """Removes the directory, its contents and all subdirectories transactionally.
        Warning: Clients which have already opened the directory might still insert data into its contents after it is removed."""
        if isinstance(path, str): path=(path,)
        n = self._find(tr, path)
        if not n: raise ValueError( "The directory doesn't exist." )
        self._remove_recursive(tr, n)
        self._remove_from_parent(tr, path)

    @fdb.transactional
    def list( self, tr, path=() ):
        if isinstance(path, str): path=(path,)
        node = self._find( tr, path)
        if not node:
            raise ValueError("The given directory does not exist.")
        return [name for name, cnode in self._subdir_names_and_nodes(tr, node)]

    ### IMPLEMENTATION ###
    SUBDIRS=0

    def _node_containing_key(self, tr, key):
        # Right now this is only used for _is_prefix_free(), but if we add parent pointers to directory nodes,
        # it could also be used to find a path based on a key
        if key.startswith(self.node_subspace.key()):
            return self.root_node
        for k,v in tr.get_range( self.node_subspace.range( () ).start,
                                 self.node_subspace.pack( (key,) )+"\x00",
                                 reverse=True,
                                 limit=1 ):
            prev_prefix = self.node_subspace.unpack( k )[0]
            if key.startswith(prev_prefix):
                return Subspace( rawPrefix=k ) # self.node_subspace[prev_prefix]
        return None

    def _node_with_prefix( self, prefix ):
        if prefix==None: return None
        return self.node_subspace[prefix]

    def _contents_of_node( self, node, path, layer=None ):
        prefix = self.node_subspace.unpack( node.key() )[0]
        return DirectorySubspace( path, prefix, self, layer )

    def _find( self, tr, path ):
        n = self.root_node
        for name in path:
            n = self._node_with_prefix( tr[ n[self.SUBDIRS][name].key() ] )
            if n == None:
                return None
        return n

    def _subdir_names_and_nodes( self, tr, node ):
        sd = node[self.SUBDIRS]
        for k,v in tr[sd.range(())]:
            yield sd.unpack(k)[0], self._node_with_prefix( v )

    def _remove_from_parent( self, tr, path ):
        parent = self._find( tr, path[:-1] )
        del tr[ parent[self.SUBDIRS][ path[-1] ].key() ]

    def _remove_recursive( self, tr, node):
        for name, sn in self._subdir_names_and_nodes(tr, node):
            self._remove_recursive(tr, sn)
        tr.clear_range_startswith( self._contents_of_node(node,None).key() )
        del tr[ node.range(()) ]

    def _is_prefix_free( self, tr, prefix ):
        # Returns true if the given prefix does not intersect any currently allocated prefix
        # (including the root node).  This means that it neither contains any other prefix nor
        # is contained by any other prefix.
        return prefix and not self._node_containing_key( tr, prefix ) and not len(list(tr.get_range( self.node_subspace.pack( (prefix,) ), self.node_subspace.pack( (strinc(prefix),) ), limit=1 )))

directory = DirectoryLayer()

class DirectorySubspace (Subspace):
    # A DirectorySubspace represents the *contents* of a directory, but it also remembers
    # the path it was opened with and offers convenience methods to operate on the directory
    # at that path.
    def __init__(self, path, prefix, directoryLayer=directory, layer=None):
        Subspace.__init__(self, rawPrefix=prefix)
        self.path = path
        self.directoryLayer = directoryLayer
        self.layer = layer

    def __repr__(self):
        return 'DirectorySubspace(' + repr(self.path) + ',' + repr(self.rawPrefix) + ')'

    def check_layer(self, layer):
        if layer and self.layer and layer!=self.layer:
            raise ValueError("The directory was created with an incompatible layer.")

    def create_or_open( self, db_or_tr, name_or_path, layer=None, prefix=None ):
        if not isinstance( name_or_path, tuple ): name_or_path = (name_or_path,)
        return self.directoryLayer.create_or_open( db_or_tr, self.path + name_or_path, layer, prefix )
    def open( self, db_or_tr, name_or_path, layer=None ):
        if not isinstance( name_or_path, tuple ): name_or_path = (name_or_path,)
        return self.directoryLayer.open( db_or_tr, self.path + name_or_path, layer )
    def create( self, db_or_tr, name_or_path, layer=None ):
        if not isinstance( name_or_path, tuple ): name_or_path = (name_or_path,)
        return self.directoryLayer.create( db_or_tr, self.path + name_or_path, layer )
    def move_to( self, db_or_tr, new_path ):
        return self.directoryLayer.moveTo( db_or_tr, self.path, new_path )
    def remove( self, db_or_tr ):
        return self.directoryLayer.remove( db_or_tr, self.path )
    def list( self, db_or_tr ):
        return self.directoryLayer.list( db_or_tr, self.path )

def random_key():
    return uuid.uuid4().bytes

def strinc(key):
    lastc = (ord(key[-1:]) + 1) % 256
    if lastc:
        return key[:-1] + chr(lastc)
    else:
        return strinc(key[:-1]) + chr(lastc)
