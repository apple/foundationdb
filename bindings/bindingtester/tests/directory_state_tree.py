import sys

class TreeNodeState:
    def __init__(self, node, dir_id, is_directory, is_subspace, has_known_prefix, root, is_partition):
        self.dir_id = dir_id
        self.is_directory = is_directory
        self.is_subspace = is_subspace
        self.has_known_prefix = has_known_prefix
        self.root = root
        self.is_partition = is_partition

        self.parents = { node }
        self.children = {}
        self.deleted = False

# Represents an element of the directory hierarchy. As a result of various operations (e.g. moves) that
# may or may not have succeeded, a node can represent multiple possible states.
class DirectoryStateTreeNode:
    # A cache of directory layers. We mustn't have multiple entries for the same layer
    layers = {}

    # Because our operations may be applied to the default directory in the case that
    # the current directory failed to open/create, we compute the result of each operation
    # as if it was performed on the current directory and the default directory.
    default_directory = None

    # Used for debugging
    dir_id = 0  

    @classmethod
    def reset(cls):
        cls.dir_id = 0
        cls.layers = {}
        cls.default_directory = None

    @classmethod
    def set_default_directory(cls, default_directory):
        cls.default_directory = default_directory

    @classmethod
    def get_layer(cls, node_subspace_prefix):
        if node_subspace_prefix not in DirectoryStateTreeNode.layers:
            DirectoryStateTreeNode.layers[node_subspace_prefix] = DirectoryStateTreeNode(True, False, has_known_prefix=False)

        return DirectoryStateTreeNode.layers[node_subspace_prefix]

    def __init__(self, is_directory, is_subspace, has_known_prefix=True, root=None, is_partition=False):
        self.state = TreeNodeState(self, DirectoryStateTreeNode.dir_id + 1, is_directory, is_subspace, has_known_prefix,
                                   root or self, is_partition)
        DirectoryStateTreeNode.dir_id += 1

    def __repr__(self):
        return '{DirEntry %d: %d}' % (self.state.dir_id, self.state.has_known_prefix)

    def _get_descendent(self, subpath, default):
        if not subpath:
            if default is not None:
                self._merge(default)
            return self

        default_child = None
        if default is not None:
            default_child = default.state.children.get(subpath[0])

        self_child = self.state.children.get(subpath[0]) 

        if self_child is None:
            if default_child is None:
                return None
            else:
                return default_child._get_descendent(subpath[1:], None)

        return self_child._get_descendent(subpath[1:], default_child)

    def get_descendent(self, subpath):
        return self._get_descendent(subpath, DirectoryStateTreeNode.default_directory)

    def add_child(self, subpath, child):
        child.state.root = self.state.root
        if DirectoryStateTreeNode.default_directory:
            # print('Adding child %r to default directory at %r' % (child, subpath))
            child = DirectoryStateTreeNode.default_directory._add_child_impl(subpath, child)
            # print('Added %r' % child)

        # print('Adding child %r to directory at %r' % (child, subpath))
        c = self._add_child_impl(subpath, child)

        # print('Added %r' % c)
        return c

    def _add_child_impl(self, subpath, child):
        # print('%d, %d. Adding child %r (recursive): %r' % (self.state.dir_id, child.state.dir_id, child, subpath))
        if len(subpath) == 0:
            # print('%d, %d. Setting child: %d, %d' % (self.state.dir_id, child.state.dir_id, self.state.has_known_prefix, child.state.has_known_prefix))
            self._merge(child)
            return self
        else:
            if not subpath[0] in self.state.children:
                # print('%d, %d. Path %r was absent from %r (%r)' % (self.state.dir_id, child.state.dir_id, subpath[0:1], self, self.state.children))
                subdir = DirectoryStateTreeNode(True, True, root=self.state.root)
                self.state.children[subpath[0]] = subdir
            else:
                subdir = self.state.children[subpath[0]]
                # print('%d, %d. Path was present' % (self.state.dir_id, child.state.dir_id))

            if len(subpath) > 1:
                subdir.state.has_known_prefix = False

            return subdir._add_child_impl(subpath[1:], child)

    def _merge(self, other):
        if self.state.dir_id == other.state.dir_id:
            return

        self.dir_id = other.dir_id
        self.state.dir_id = min(other.state.dir_id, self.state.dir_id)
        self.state.is_directory = self.state.is_directory and other.state.is_directory
        self.state.is_subspace = self.state.is_subspace and other.state.is_subspace
        self.state.has_known_prefix = self.state.has_known_prefix and other.state.has_known_prefix
        self.state.deleted = self.state.deleted or other.state.deleted
        self.state.is_partition = self.state.is_partition or other.state.is_partition

        other_children = other.state.children.copy()
        other_parents = other.state.parents.copy()

        for node in other_parents:
            node.state = self.state
            self.state.parents.add(node)

        for c in other_children:
            if c not in self.state.children:
                self.state.children[c] = other_children[c]
            else:
                self.state.children[c]._merge(other_children[c])

    def _delete_impl(self):
        if not self.state.deleted:
            self.state.deleted = True
            for c in self.state.children.values():
                c._delete_impl()

    def delete(self, path):
        child = self.get_descendent(path)
        if child:
            child._delete_impl()
    
def validate_dir(dir, root):
    if dir.state.is_directory:
        assert dir.state.root == root
    else:
        assert dir.state.root == dir

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
    assert entry.state.has_known_prefix

    entry = all_entries[-1]
    all_entries.append(entry.add_child(('1',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))
    all_entries.append(entry.add_child(('2',), DirectoryStateTreeNode(True, True, has_known_prefix=False)))
    all_entries.append(entry.add_child(('3',), DirectoryStateTreeNode(True, False, has_known_prefix=True)))

    entry_to_move = all_entries[-1]

    all_entries.append(entry.add_child(('5',), DirectoryStateTreeNode(True, False, has_known_prefix=True)))
    child_entries.append(entry.add_child(('6',), DirectoryStateTreeNode(True, True, has_known_prefix=True)))

    all_entries.extend(child_entries)

    # This directory has an unknown prefix
    entry = root.get_descendent(('1', '2'))
    assert not entry.state.has_known_prefix

    # This directory was default created and should have an unknown prefix
    # It will merge with the default directory's child, which is not a subspace
    entry = root.get_descendent(('1',))
    assert not entry.state.has_known_prefix
    assert not entry.state.is_subspace

    # Multiple merges will have made this prefix unreadable
    entry = root.get_descendent(('2',))
    assert not entry.state.has_known_prefix

    # Merge with default directory's child that has an unknown prefix
    entry = root.get_descendent(('3',))
    assert not entry.state.has_known_prefix

    # Merge with default directory's child that has an unknown prefix and merged children
    entry = root.get_descendent(('1', '3'))
    assert set(entry.state.children.keys()) == {'1', '2', '3', '4', '5', '6'}

    # This child entry should be the combination of ['default', '3'], ['default', '1', '3'], and ['1', '3']
    entry = entry.get_descendent(('3',))
    assert not entry.state.has_known_prefix
    assert not entry.state.is_subspace

    # Verify the merge of the children
    assert not child_entries[0].state.has_known_prefix
    assert not child_entries[0].state.is_subspace

    assert not child_entries[1].state.has_known_prefix
    assert child_entries[1].state.is_subspace

    assert not child_entries[2].state.has_known_prefix
    assert not child_entries[2].state.is_subspace

    assert not child_entries[3].state.has_known_prefix
    assert not child_entries[3].state.is_subspace

    assert child_entries[4].state.has_known_prefix
    assert not child_entries[4].state.is_subspace

    assert child_entries[5].state.has_known_prefix
    assert child_entries[5].state.is_subspace

    entry = root.add_child(('3',), entry_to_move)
    all_entries.append(entry)

    # Test moving an entry
    assert not entry.state.has_known_prefix
    assert not entry.state.is_subspace
    assert list(entry.state.children.keys()) == ['1']    

    for e in all_entries:
        validate_dir(e, root)

if __name__ == '__main__':
    sys.exit(run_test())

