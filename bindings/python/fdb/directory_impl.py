#
# directory_impl.py
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

import random
import struct
import threading

from fdb import impl as _impl
from fdb import six
import fdb.tuple
from .subspace_impl import Subspace


class AllocatorTransactionState:
    def __init__(self):
        self.lock = threading.Lock()


class HighContentionAllocator(object):
    def __init__(self, subspace):
        self.counters = subspace[0]
        self.recent = subspace[1]
        self.lock = threading.Lock()

    @_impl.transactional
    def allocate(self, tr):
        """Returns a byte string that
        1) has never and will never be returned by another call to this
           method on the same subspace
        2) is nearly as short as possible given the above
        """

        # Get transaction-local state
        if not hasattr(tr, "__fdb_directory_layer_hca_state__"):
            with self.lock:
                if not hasattr(tr, "__fdb_directory_layer_hca_state__"):
                    tr.__fdb_directory_layer_hca_state__ = AllocatorTransactionState()

        tr_state = tr.__fdb_directory_layer_hca_state__

        while True:
            [start] = [
                self.counters.unpack(k)[0]
                for k, _ in tr.snapshot.get_range(
                    self.counters.range().start,
                    self.counters.range().stop,
                    limit=1,
                    reverse=True,
                )
            ] or [0]

            window_advanced = False
            while True:
                with tr_state.lock:
                    if window_advanced:
                        del tr[self.counters : self.counters[start]]
                        tr.options.set_next_write_no_write_conflict_range()
                        del tr[self.recent : self.recent[start]]

                    # Increment the allocation count for the current window
                    tr.add(self.counters[start], struct.pack("<q", 1))
                    count = tr.snapshot[self.counters[start]]

                if count != None:
                    count = struct.unpack("<q", bytes(count))[0]
                else:
                    count = 0

                window = self._window_size(start)
                if count * 2 < window:
                    break

                start += window
                window_advanced = True

            while True:
                # As of the snapshot being read from, the window is less than half
                # full, so this should be expected to take 2 tries.  Under high
                # contention (and when the window advances), there is an additional
                # subsequent risk of conflict for this transaction.
                candidate = random.randrange(start, start + window)

                with tr_state.lock:
                    latest_counter = tr.snapshot.get_range(
                        self.counters.range().start,
                        self.counters.range().stop,
                        limit=1,
                        reverse=True,
                    )
                    candidate_value = tr[self.recent[candidate]]
                    tr.options.set_next_write_no_write_conflict_range()
                    tr[self.recent[candidate]] = b""

                latest_counter = [self.counters.unpack(k)[0] for k, _ in latest_counter]
                if len(latest_counter) > 0 and latest_counter[0] > start:
                    break

                if candidate_value == None:
                    tr.add_write_conflict_key(self.recent[candidate])
                    return fdb.tuple.pack((candidate,))

    def _window_size(self, start):
        # Larger window sizes are better for high contention, smaller sizes for
        # keeping the keys small.  But if there are many allocations, the keys
        # can't be too small.  So start small and scale up.  We don't want this
        # to ever get *too* big because we have to store about window_size/2
        # recent items.
        if start < 255:
            return 64
        if start < 65535:
            return 1024
        return 8192


class Directory(object):
    def __init__(self, directory_layer, path=(), layer=b""):
        self._directory_layer = directory_layer
        self._path = path
        self._layer = layer

    @_impl.transactional
    def create_or_open(self, tr, path, layer=None):
        path = self._tuplify_path(path)
        return self._directory_layer.create_or_open(
            tr, self._partition_subpath(path), layer
        )

    @_impl.transactional
    def open(self, tr, path, layer=None):
        path = self._tuplify_path(path)
        return self._directory_layer.open(tr, self._partition_subpath(path), layer)

    @_impl.transactional
    def create(self, tr, path, layer=None, prefix=None):
        path = self._tuplify_path(path)
        return self._directory_layer.create(
            tr, self._partition_subpath(path), layer, prefix
        )

    @_impl.transactional
    def list(self, tr, path=()):
        path = self._tuplify_path(path)
        return self._directory_layer.list(tr, self._partition_subpath(path))

    @_impl.transactional
    def move(self, tr, old_path, new_path):
        old_path = self._tuplify_path(old_path)
        new_path = self._tuplify_path(new_path)
        return self._directory_layer.move(
            tr, self._partition_subpath(old_path), self._partition_subpath(new_path)
        )

    @_impl.transactional
    def move_to(self, tr, new_absolute_path):
        directory_layer = self._get_layer_for_path(())
        new_absolute_path = _to_unicode_path(new_absolute_path)
        partition_len = len(directory_layer._path)
        partition_path = new_absolute_path[:partition_len]
        if partition_path != directory_layer._path:
            raise ValueError("Cannot move between partitions.")

        return directory_layer.move(
            tr, self._path[partition_len:], new_absolute_path[partition_len:]
        )

    @_impl.transactional
    def remove(self, tr, path=()):
        path = self._tuplify_path(path)
        directory_layer = self._get_layer_for_path(path)
        return directory_layer.remove(
            tr, self._partition_subpath(path, directory_layer)
        )

    @_impl.transactional
    def remove_if_exists(self, tr, path=()):
        path = self._tuplify_path(path)
        directory_layer = self._get_layer_for_path(path)
        return directory_layer.remove_if_exists(
            tr, self._partition_subpath(path, directory_layer)
        )

    @_impl.transactional
    def exists(self, tr, path=()):
        path = self._tuplify_path(path)
        directory_layer = self._get_layer_for_path(path)
        return directory_layer.exists(
            tr, self._partition_subpath(path, directory_layer)
        )

    def get_layer(self):
        return self._layer

    def get_path(self):
        return self._path

    def _tuplify_path(self, path):
        if not isinstance(path, tuple):
            path = (path,)
        return path

    def _partition_subpath(self, path, directory_layer=None):
        directory_layer = directory_layer or self._directory_layer
        return self._path[len(directory_layer._path) :] + path

    # Called by all functions that could operate on this subspace directly (move_to, remove, remove_if_exists, exists)
    # Subclasses can choose to return a different directory layer to use for the operation if path is in fact ()
    def _get_layer_for_path(self, path):
        return self._directory_layer


class DirectoryLayer(Directory):
    def __init__(
        self,
        node_subspace=Subspace(rawPrefix=b"\xfe"),
        content_subspace=Subspace(),
        allow_manual_prefixes=False,
    ):
        Directory.__init__(self, self)

        # If specified, new automatically allocated prefixes will all fall within content_subspace
        self._content_subspace = content_subspace
        self._node_subspace = node_subspace

        self._allow_manual_prefixes = allow_manual_prefixes

        # The root node is the one whose contents are the node subspace
        self._root_node = self._node_subspace[self._node_subspace.key()]
        self._allocator = HighContentionAllocator(self._root_node[b"hca"])

    @_impl.transactional
    def create_or_open(self, tr, path, layer=None):
        """Opens the directory with the given path.

        If the directory does not exist, it is created (creating parent
        directories if necessary).

        If layer is specified, it is checked against the layer of an existing
        directory or set as the layer of a new directory.
        """
        return self._create_or_open_internal(tr, path, layer)

    def _create_or_open_internal(
        self, tr, path, layer=None, prefix=None, allow_create=True, allow_open=True
    ):
        self._check_version(tr, write_access=False)

        if prefix is not None and not self._allow_manual_prefixes:
            if len(self._path) == 0:
                raise ValueError(
                    "Cannot specify a prefix unless manual prefixes are enabled."
                )
            else:
                raise ValueError("Cannot specify a prefix in a partition.")

        path = _to_unicode_path(path)

        if not path:
            # Root directory contains node metadata and so cannot be opened.
            raise ValueError("The root directory cannot be opened.")

        existing_node = self._find(tr, path).prefetch_metadata(tr)
        if existing_node.exists():
            if existing_node.is_in_partition():
                subpath = existing_node.get_partition_subpath()
                return existing_node.get_contents(
                    self
                )._directory_layer._create_or_open_internal(
                    tr, subpath, layer, prefix, allow_create, allow_open
                )

            if not allow_open:
                raise ValueError("The directory already exists.")

            if layer and existing_node.layer() != layer:
                raise ValueError(
                    "The directory was created with an incompatible layer."
                )

            return existing_node.get_contents(self)

        if not allow_create:
            raise ValueError("The directory does not exist.")

        self._check_version(tr)

        if prefix == None:
            prefix = self._content_subspace.key() + self._allocator.allocate(tr)

            if len(list(tr.get_range_startswith(prefix, limit=1))) > 0:
                raise Exception(
                    "The database has keys stored at the prefix chosen by the automatic prefix allocator: %r."
                    % prefix
                )

            if not self._is_prefix_free(tr.snapshot, prefix):
                raise Exception(
                    "The directory layer has manually allocated prefixes that conflict with the automatic prefix allocator."
                )

        elif not self._is_prefix_free(tr, prefix):
            raise ValueError("The given prefix is already in use.")

        if len(path) > 1:
            parent_node = self._node_with_prefix(
                self.create_or_open(tr, path[:-1]).key()
            )
        else:
            parent_node = self._root_node
        if not parent_node:
            # print repr(path[:-1])
            raise ValueError("The parent directory doesn't exist.")

        node = self._node_with_prefix(prefix)
        tr[parent_node[self.SUBDIRS][path[-1]]] = prefix
        if not layer:
            layer = b""

        tr[node[b"layer"]] = layer

        return self._contents_of_node(node, path, layer)

    @_impl.transactional
    def open(self, tr, path, layer=None):
        """Opens the directory with the given path.

        An error is raised if the directory does not exist, or if a layer is
        specified and a different layer was specified when the directory was
        created.
        """
        return self._create_or_open_internal(tr, path, layer, allow_create=False)

    @_impl.transactional
    def create(self, tr, path, layer=None, prefix=None):
        """Creates a directory with the given path (creating parent directories
           if necessary).

        An error is raised if the given directory already exists.

        If prefix is specified, the directory is created with the given physical
        prefix; otherwise a prefix is allocated automatically.

        If layer is specified, it is recorded with the directory and will be
        checked by future calls to open.
        """
        return self._create_or_open_internal(tr, path, layer, prefix, allow_open=False)

    @_impl.transactional
    def move_to(self, tr, new_absolute_path):
        raise Exception("The root directory cannot be moved.")

    @_impl.transactional
    def move(self, tr, old_path, new_path):
        """Moves the directory found at `old_path` to `new_path`.

        There is no effect on the physical prefix of the given directory, or on
        clients that already have the directory open.

        An error is raised if the old directory does not exist, a directory
        already exists at `new_path`, or the parent directory of `new_path` does
        not exist.
        """
        self._check_version(tr)

        old_path = _to_unicode_path(old_path)
        new_path = _to_unicode_path(new_path)

        if old_path == new_path[: len(old_path)]:
            raise ValueError(
                "The destination directory cannot be a subdirectory of the source directory."
            )

        old_node = self._find(tr, old_path).prefetch_metadata(tr)
        new_node = self._find(tr, new_path).prefetch_metadata(tr)

        if not old_node.exists():
            raise ValueError("The source directory does not exist.")

        if old_node.is_in_partition() or new_node.is_in_partition():
            if (
                not old_node.is_in_partition()
                or not new_node.is_in_partition()
                or old_node.path != new_node.path
            ):
                raise ValueError("Cannot move between partitions.")

            return new_node.get_contents(self).move(
                tr, old_node.get_partition_subpath(), new_node.get_partition_subpath()
            )

        if new_node.exists():
            raise ValueError(
                "The destination directory already exists. Remove it first."
            )

        parent_node = self._find(tr, new_path[:-1])
        if not parent_node.exists():
            raise ValueError(
                "The parent of the destination directory does not exist. Create it first."
            )
        tr[
            parent_node.subspace[self.SUBDIRS][new_path[-1]]
        ] = self._node_subspace.unpack(old_node.subspace.key())[0]
        self._remove_from_parent(tr, old_path)
        return self._contents_of_node(old_node.subspace, new_path, old_node.layer())

    @_impl.transactional
    def remove(self, tr, path=()):
        """Removes the directory, its contents, and all subdirectories.
        Throws an exception if the directory does not exist.

        Warning: Clients that have already opened the directory might still
        insert data into its contents after it is removed.
        """
        return self._remove_internal(tr, path, fail_on_nonexistent=True)

    @_impl.transactional
    def remove_if_exists(self, tr, path=()):
        """Removes the directory, its contents, and all subdirectories, if
        it exists. Returns true if the directory existed and false otherwise.

        Warning: Clients that have already opened the directory might still
        insert data into its contents after it is removed.
        """
        return self._remove_internal(tr, path, fail_on_nonexistent=False)

    def _remove_internal(self, tr, path, fail_on_nonexistent):
        self._check_version(tr)

        if not path:
            raise ValueError("The root directory cannot be removed.")

        path = _to_unicode_path(path)
        node = self._find(tr, path).prefetch_metadata(tr)

        if not node.exists():
            if fail_on_nonexistent:
                raise ValueError("The directory does not exist.")
            else:
                return False

        if node.is_in_partition():
            return node.get_contents(self)._directory_layer._remove_internal(
                tr, node.get_partition_subpath(), fail_on_nonexistent
            )

        self._remove_recursive(tr, node.subspace)
        self._remove_from_parent(tr, path)
        return True

    @_impl.transactional
    def list(self, tr, path=()):
        """Returns the names of the specified directory's subdirectories as a
        list of strings.
        """
        self._check_version(tr, write_access=False)

        path = _to_unicode_path(path)
        node = self._find(tr, path).prefetch_metadata(tr)
        if not node.exists():
            raise ValueError("The directory does not exist.")

        if node.is_in_partition(include_empty_subpath=True):
            return node.get_contents(self).list(tr, node.get_partition_subpath())

        return [name for name, cnode in self._subdir_names_and_nodes(tr, node.subspace)]

    @_impl.transactional
    def exists(self, tr, path=()):
        """Returns whether or not the specified directory exists."""
        self._check_version(tr, write_access=False)

        path = _to_unicode_path(path)
        node = self._find(tr, path).prefetch_metadata(tr)

        if not node.exists():
            return False

        if node.is_in_partition():
            return node.get_contents(self).exists(tr, node.get_partition_subpath())

        return True

    ########################################
    #  Private methods for implementation  #
    ########################################

    SUBDIRS = 0
    VERSION = (1, 0, 0)

    def _check_version(self, tr, write_access=True):
        version = tr[self._root_node[b"version"]]

        if not version.present():
            if write_access:
                self._initialize_directory(tr)

            return

        version = struct.unpack("<III", bytes(version))

        if version[0] > self.VERSION[0]:
            raise Exception(
                "Cannot load directory with version %d.%d.%d using directory layer %d.%d.%d"
                % (version + self.VERSION)
            )

        if version[1] > self.VERSION[1] and write_access:
            raise Exception(
                "Directory with version %d.%d.%d is read-only when opened using directory layer %d.%d.%d"
                % (version + self.VERSION)
            )

    def _initialize_directory(self, tr):
        tr[self._root_node[b"version"]] = struct.pack("<III", *self.VERSION)

    def _node_containing_key(self, tr, key):
        # Right now this is only used for _is_prefix_free(), but if we add
        # parent pointers to directory nodes, it could also be used to find a
        # path based on a key.
        if key.startswith(self._node_subspace.key()):
            return self._root_node
        for k, v in tr.get_range(
            self._node_subspace.range(()).start,
            self._node_subspace.pack((key,)) + b"\x00",
            reverse=True,
            limit=1,
        ):
            prev_prefix = self._node_subspace.unpack(k)[0]
            if key.startswith(prev_prefix):
                return self._node_with_prefix(prev_prefix)
        return None

    def _node_with_prefix(self, prefix):
        if prefix == None:
            return None
        return self._node_subspace[prefix]

    def _contents_of_node(self, node, path, layer=None):
        prefix = self._node_subspace.unpack(node.key())[0]

        if layer == b"partition":
            return DirectoryPartition(self._path + path, prefix, self)
        else:
            return DirectorySubspace(self._path + path, prefix, self, layer)

    def _find(self, tr, path):
        n = _Node(self._root_node, (), path)
        for i, name in enumerate(path):
            n = _Node(
                self._node_with_prefix(tr[n.subspace[self.SUBDIRS][name]]),
                path[: i + 1],
                path,
            )
            if not n.exists() or n.layer(tr) == b"partition":
                return n
        return n

    def _subdir_names_and_nodes(self, tr, node):
        sd = node[self.SUBDIRS]
        for k, v in tr[sd.range(())]:
            yield sd.unpack(k)[0], self._node_with_prefix(v)

    def _remove_from_parent(self, tr, path):
        parent = self._find(tr, path[:-1])
        del tr[parent.subspace[self.SUBDIRS][path[-1]]]

    def _remove_recursive(self, tr, node):
        for name, sn in self._subdir_names_and_nodes(tr, node):
            self._remove_recursive(tr, sn)
        tr.clear_range_startswith(self._node_subspace.unpack(node.key())[0])
        del tr[node.range(())]

    def _is_prefix_free(self, tr, prefix):
        # Returns true if the given prefix does not "intersect" any currently
        # allocated prefix (including the root node). This means that it neither
        # contains any other prefix nor is contained by any other prefix.
        return (
            prefix
            and not self._node_containing_key(tr, prefix)
            and not len(
                list(
                    tr.get_range(
                        self._node_subspace.pack((prefix,)),
                        self._node_subspace.pack((_impl.strinc(prefix),)),
                        limit=1,
                    )
                )
            )
        )

    def _is_prefix_empty(self, tr, prefix):
        return len(list(tr.get_range(prefix, _impl.strinc(prefix), limit=1))) == 0


def _to_unicode_path(path):
    if isinstance(path, bytes):
        path = six.text_type(path)

    if isinstance(path, six.text_type):
        return (path,)

    if isinstance(path, tuple):
        path = list(path)
        for i, name in enumerate(path):
            if isinstance(name, bytes):
                path[i] = six.text_type(path[i])
            elif not isinstance(name, six.text_type):
                raise ValueError(
                    "Invalid path: must be a unicode string or a tuple of unicode strings"
                )

        return tuple(path)

    raise ValueError(
        "Invalid path: must be a unicode string or a tuple of unicode strings"
    )


directory = DirectoryLayer()


class DirectorySubspace(Subspace, Directory):
    # A DirectorySubspace represents the *contents* of a directory, but it also
    # remembers the path with which it was opened and offers convenience methods
    # to operate on the directory at that path.

    def __init__(self, path, prefix, directory_layer=directory, layer=None):
        Subspace.__init__(self, rawPrefix=prefix)
        Directory.__init__(self, directory_layer, path, layer)

    def __repr__(self):
        return (
            "DirectorySubspace(path="
            + repr(self._path)
            + ", prefix="
            + repr(self.rawPrefix)
            + ")"
        )


class DirectoryPartition(DirectorySubspace):
    def __init__(self, path, prefix, parent_directory_layer):
        directory_layer = DirectoryLayer(
            Subspace(rawPrefix=prefix + b"\xfe"), Subspace(rawPrefix=prefix)
        )
        directory_layer._path = path
        DirectorySubspace.__init__(self, path, prefix, directory_layer, b"partition")

        self._parent_directory_layer = parent_directory_layer

    def __repr__(self):
        return (
            "DirectoryPartition(path="
            + repr(self._path)
            + ", prefix="
            + repr(self.rawPrefix)
            + ")"
        )

    def __getitem__(self, name):
        raise Exception("Cannot open subspace in the root of a directory partition.")

    def key(self):
        raise Exception("Cannot get key for the root of a directory partition.")

    def pack(self, t=tuple()):
        raise Exception("Cannot pack keys using the root of a directory partition.")

    def unpack(self, key):
        raise Exception("Cannot unpack keys using the root of a directory partition.")

    def range(self, t=tuple()):
        raise Exception("Cannot get range for the root of a directory partition.")

    def contains(self, key):
        raise Exception(
            "Cannot check whether a key belongs to the root of a directory partition."
        )

    def as_foundationdb_key(self):
        raise Exception("Cannot use the root of a directory partition as a key.")

    def subspace(self, tuple):
        raise Exception("Cannot open subspace in the root of a directory partition.")

    def _get_layer_for_path(self, path):
        if path == ():
            return self._parent_directory_layer
        else:
            return self._directory_layer


class _Node(object):
    def __init__(self, subspace, path, target_path):
        self.subspace = subspace
        self.path = path
        self.target_path = target_path
        self._layer = None

    def exists(self):
        return self.subspace is not None

    def prefetch_metadata(self, tr):
        if self.exists():
            self.layer(tr)

        return self

    def layer(self, tr=None):
        if tr:
            self._layer = tr[self.subspace[b"layer"]]
        elif self._layer is None:
            raise Exception("Layer has not been read")

        return self._layer

    def is_in_partition(self, tr=None, include_empty_subpath=False):
        return (
            self.exists()
            and self.layer(tr) == b"partition"
            and (include_empty_subpath or len(self.target_path) > len(self.path))
        )

    def get_partition_subpath(self):
        return self.target_path[len(self.path) :]

    def get_contents(self, directory_layer, tr=None):
        return directory_layer._contents_of_node(
            self.subspace, self.path, self.layer(tr)
        )
