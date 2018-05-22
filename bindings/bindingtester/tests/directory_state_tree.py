import sys

# Represents an element of the directory hierarchy, which could have multiple states 
class DirectoryStateTreeNode:
    # A cache of directory layers. We mustn't have multiple entries for the same layer
    layers = {}

    # This is the directory that gets used if the chosen directory is invalid
    # We must assume any operation could also have been applied to it
    default_directory = None

    # Used for debugging
    dir_id = 0  

    @classmethod
    def set_default_directory(cls, default_directory):
        cls.default_directory = default_directory

    @classmethod
    def get_layer(cls, node_subspace_prefix):
        if node_subspace_prefix not in DirectoryStateTreeNode.layers:
            DirectoryStateTreeNode.layers[node_subspace_prefix] = DirectoryStateTreeNode(True, False, has_known_prefix=False)

        return DirectoryStateTreeNode.layers[node_subspace_prefix]

    def __init__(self, is_directory, is_subspace, has_known_prefix=True, root=None, is_partition=False):
        self.root = root or self
        self.is_directory = is_directory
        self.is_subspace = is_subspace
        self.has_known_prefix = has_known_prefix
        self.children = {}
        self.deleted = False
        self.is_partition = is_partition

        self.dir_id = DirectoryStateTreeNode.dir_id + 1
        DirectoryStateTreeNode.dir_id += 1

    def __repr__(self):
        return '{DirEntry %d: %d}' % (self.dir_id, self.has_known_prefix)

    def _get_descendent(self, subpath, default):
        if not subpath:
            if default is not None:
                self._merge(default)
            return self

        default_child = None
        if default is not None:
            default_child = default.children.get(subpath[0])

        self_child = self.children.get(subpath[0]) 

        if self_child is None:
            if default_child is None:
                return None
            else:
                return default_child._get_descendent(subpath[1:], None)

        return self_child._get_descendent(subpath[1:], default_child)

    def get_descendent(self, subpath):
        return self._get_descendent(subpath, DirectoryStateTreeNode.default_directory)

    def add_child(self, subpath, child):
        child.root = self.root
        if DirectoryStateTreeNode.default_directory:
            # print('Adding child %r to default directory at %r' % (child, subpath))
            child = DirectoryStateTreeNode.default_directory._add_child_impl(subpath, child)
            # print('Added %r' % child)

        # print('Adding child %r to directory at %r' % (child, subpath))
        c = self._add_child_impl(subpath, child)

        # print('Added %r' % c)
        return c

    def _add_child_impl(self, subpath, child):
        # print('%d, %d. Adding child (recursive): %r' % (self.dir_id, child.dir_id, subpath))
        if len(subpath) == 0:
            # print('%d, %d. Setting child: %d, %d' % (self.dir_id, child.dir_id, self.has_known_prefix, child.has_known_prefix))
            self._merge(child)
            return self
        else:
            if not subpath[0] in self.children:
                # print('%d, %d. Path %r was absent from %r (%r)' % (self.dir_id, child.dir_id, subpath[0:1], self, self.children))
                subdir = DirectoryStateTreeNode(True, True, root=self.root)
                self.children[subpath[0]] = subdir
            else:
                subdir = self.children[subpath[0]]
                # print('%d, %d. Path was present' % (self.dir_id, child.dir_id))

            subdir.has_known_prefix = subdir.has_known_prefix and len(subpath) == 1 # For the last element in the path, the merge will take care of has_known_prefix
            return subdir._add_child_impl(subpath[1:], child)

    def _merge(self, other):
        if self == other:
            return

        self.is_directory = self.is_directory and other.is_directory
        self.is_subspace = self.is_subspace and other.is_subspace
        self.has_known_prefix = self.has_known_prefix and other.has_known_prefix
        self.deleted = self.deleted or other.deleted
        self.is_partition = self.is_partition or other.is_partition

        other.root = self.root
        other.is_directory = self.is_directory
        other.is_subspace = self.is_subspace
        other.has_known_prefix = self.has_known_prefix
        other.deleted = self.deleted
        other.is_partition = self.is_partition
        other.dir_id = self.dir_id

        other_children = other.children.copy()
        for c in other_children:
            if c not in self.children:
                self.children[c] = other_children[c]

        other.children = self.children

        for c in other_children:
            self.children[c]._merge(other_children[c])

    def _delete_impl(self):
        if not self.deleted:
            self.deleted = True
            for c in self.children.values():
                c._delete_impl()

    def delete(self, path):
        child = self.get_descendent(path)
        if child:
            child._delete_impl()
    
def validate_dir(dir, root):
    if dir.is_directory:
        assert dir.root == root
    else:
        assert dir.root == dir

def run_test():
    all_entries = []

    root = DirectoryStateTreeNode.get_layer('\xfe')
    all_entries.append(root)

    default_dir = root.add_child(('default',), DirectoryStateTreeNode(True, True, has_known_prefix=True))
    DirectoryStateTreeNode.set_default_directory(default_dir)
    all_entries.append(default_dir)

    all_entries.append(default_dir.add_child(('1',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    all_entries.append(default_dir.add_child(('1', '1'), DirectoryStateTreeNode(True, False, has_known_prefix=True)))
    all_entries.append(default_dir.add_child(('2',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    all_entries.append(default_dir.add_child(('3',), DirectoryStateTreeNode(True, True, has_known_prefix=False)))
    all_entries.append(default_dir.add_child(('5',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    all_entries.append(default_dir.add_child(('3', '1'), DirectoryStateTreeNode(True, True, has_known_prefix=False)))
    all_entries.append(default_dir.add_child(('1', '3'), DirectoryStateTreeNode(True, True, has_known_prefix=False)))

    entry = all_entries[-1]
    child_entries = []
    child_entries.append(entry.add_child(('1',), DirectoryStateTreeNode(True, False, has_known_prefix=True)))
    child_entries.append(entry.add_child(('2',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    child_entries.append(entry.add_child(('3',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    child_entries.append(entry.add_child(('4',), DirectoryStateTreeNode(True, False, has_known_prefix=False)))
    child_entries.append(entry.add_child(('5',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))

    all_entries.append(root.add_child(('1', '2'), DirectoryStateTreeNode(True, True, has_known_prefix=False)))
    all_entries.append(root.add_child(('2',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    all_entries.append(root.add_child(('3',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    all_entries.append(root.add_child(('1', '3',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))

    # This directory was merged with the default, but both have readable prefixes
    entry = root.get_descendent(('2',))
    assert entry.has_known_prefix

    entry = all_entries[-1]
    all_entries.append(entry.add_child(('1',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    all_entries.append(entry.add_child(('2',), DirectoryStateTreeNode(True, True, has_known_prefix=False)))
    all_entries.append(entry.add_child(('3',), DirectoryStateTreeNode(True, False, has_known_prefix=True)))

    entry_to_move = all_entries[-1]

    all_entries.append(entry.add_child(('5',), DirectoryStateTreeNode(True, False, has_known_prefix=True)))
    child_entries.append(entry.add_child(('6',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))

    all_entries.extend(child_entries)

    # This directory has an known prefix
    entry = root.get_descendent(('1', '2'))
    assert not entry.has_known_prefix

    # This directory was default created and should have an unknown prefix
    # It will merge with the default directory's child, which is not a subspace
    entry = root.get_descendent(('1',))
    assert not entry.has_known_prefix
    assert not entry.is_subspace

    # Multiple merges will have made this prefix unreadable
    entry = root.get_descendent(('2',))
    assert not entry.has_known_prefix

    # Merge with default directory's child that has an unknown prefix
    entry = root.get_descendent(('3',))
    assert not entry.has_known_prefix

    # Merge with default directory's child that has an unknown prefix and merged children
    entry = root.get_descendent(('1', '3'))
    assert set(entry.children.keys()) == {'1', '2', '3', '4', '5', '6'}

    # This child entry should be the combination of ['default', '3'], ['default', '1', '3'], and ['1', '3']
    entry = entry.get_descendent(('3',))
    assert not entry.has_known_prefix
    assert not entry.is_subspace

    # Verify the merge of the children
    assert not child_entries[0].has_known_prefix
    assert not child_entries[0].is_subspace

    assert not child_entries[1].has_known_prefix
    assert child_entries[1].is_subspace

    assert not child_entries[2].has_known_prefix
    assert not child_entries[2].is_subspace

    assert not child_entries[3].has_known_prefix
    assert not child_entries[3].is_subspace

    assert child_entries[4].has_known_prefix
    assert not child_entries[4].is_subspace

    assert child_entries[5].has_known_prefix
    assert child_entries[5].is_subspace

    entry = root.add_child(('3',), entry_to_move)
    all_entries.append(entry)

    # Test moving an entry
    assert not entry.has_known_prefix
    assert not entry.is_subspace
    assert entry.children.keys() == ['1']    

    for e in all_entries:
        validate_dir(e, root)

if __name__ == '__main__':
    sys.exit(run_test())

