Overview
--------

The directory layer is tested by adding some additional instructions and state to
the existing stack tester. Each 'thread' of the stack tester should have its own
directory testing state.

Additional State and Initialization
-----------------------------------

Your tester should store three additional pieces of state.

* directory list - The items in this list should be accessible by index. The list
should support an append operation.  It will be required to store Subspaces,
DirectorySubspaces, and DirectoryLayers.

* directory list index - an index into the directory list of the currently active
directory.

* error index - the index to use when the directory at directory list index is not
present

At the beginning of the test, the list should contain just the default directory
layer. The directory index and error index should both be set to 0.

Popping Tuples
-------------

Some instructions will require you to pop N tuples. To do this, repeat the
following procedure N times:

Pop 1 item off the stack as M. Pop M items off the stack as
tuple = [item1, ..., itemM].

Errors
------

In the even that you encounter an error when performing a directory layer
operation, you should push the byte string: `"DIRECTORY_ERROR"` onto the stack. If
the operation being performed was supposed to append an item to the directory
list, then a null entry should be appended instead.

New Instructions
----------------

Below are the new instructions that must be implemented to test the directory
layer. Some instructions specify that the current directory should be used
for the operation. In that case, use the object in the directory list specified
by the current directory list index. Operations that are not defined for a
particular object will not be called (e.g. a DirectoryLayer will never be asked
to pack a key).

Directory/Subspace/Layer Creation
---------------------------------

#### DIRECTORY_CREATE_SUBSPACE

	Pop 1 tuple off the stack as [path]. Pop 1 additional item as [raw_prefix].  
	Create a subspace with path as the prefix tuple and the specified
	raw_prefix. Append it to the directory list.

#### DIRECTORY_CREATE_LAYER

	Pop 3 items off the stack as [index1, index2, allow_manual_prefixes]. Let
	node_subspace be the object in the directory list at index1 and
	content_subspace be the object in the directory list at index2. Create a new
	directory layer with the specified node_subspace and content_subspace. If
	allow_manual_prefixes is 1, then enable manual prefixes on the directory
	layer. Append the resulting directory layer to the directory list.

	If either of the two specified subspaces are null, then do not create a
	directory layer and instead push null onto the directory list.

#### DIRECTORY_CREATE_OR_OPEN[_DATABASE]

	Use the current directory for this operation.

	Pop 1 tuple off the stack as [path]. Pop 1 additional item as [layer].
	create_or_open a directory with the specified path and layer. If layer is
	null, use the default value for that parameter.

#### DIRECTORY_CREATE[_DATABASE]

	Pop 1 tuple off the stack as [path]. Pop 2 additional items as
	[layer, prefix]. create a directory with the specified path, layer,
	and prefix. If either of layer or prefix is null, use the default value for
	that parameter (layer='', prefix=null).

#### DIRECTORY_OPEN[_DATABASE|_SNAPSHOT]

	Use the current directory for this operation.

	Pop 1 tuple off the stack as [path]. Pop 1 additional item as [layer]. Open
	a directory with the specified path and layer. If layer is null, use the
	default value (layer='').

Directory Management
--------------------

#### DIRECTORY_CHANGE

	Pop the top item off the stack as [index]. Set the current directory list
	index to index. In the event that the directory at this new index is null
	(as the result of a previous error), set the directory list index to the
	error index.

#### DIRECTORY_SET_ERROR_INDEX

	Pop the top item off the stack as [error_index]. Set the current error index
	to error_index.

Directory Operations
--------------------

#### DIRECTORY_MOVE[_DATABASE]

	Use the current directory for this operation.

	Pop 2 tuples off the stack as [old_path, new_path]. Call move with the
	specified old_path and new_path. Append the result onto the directory list.

#### DIRECTORY_MOVE_TO[_DATABASE]

	Use the current directory for this operation.

	Pop 1 tuple off the stack as [new_absolute_path]. Call moveTo with the
	specified new_absolute_path. Append the result onto the directory list.

#### DIRECTORY_REMOVE[_DATABASE]

	Use the current directory for this operation.

	Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
	tuple off the stack as [path]. Call remove, passing it path if one was
	popped.

#### DIRECTORY_REMOVE_IF_EXISTS[_DATABASE]

	Use the current directory for this operation.

	Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
	tuple off the stack as [path]. Call remove_if_exits, passing it path if one
	was popped.

#### DIRECTORY_LIST[_DATABASE|_SNAPSHOT]

	Use the current directory for this operation.

	Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
	tuple off the stack as [path]. Call list, passing it path if one was popped.
	Pack the resulting list of directories using the tuple layer and push the
	packed string onto the stack.

#### DIRECTORY_EXISTS[_DATABASE|_SNAPSHOT]

	Use the current directory for this operation.

	Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
	tuple off the stack as [path]. Call exists, passing it path if one
	was popped. Push 1 onto the stack if the path exists and 0 if it does not.

Subspace Operations
-------------------

#### DIRECTORY_PACK_KEY

	Use the current directory for this operation.

	Pop 1 tuple off the stack as [key_tuple]. Pack key_tuple and push the result
	onto the stack.

#### DIRECTORY_UNPACK_KEY

	Use the current directory for this operation.

	Pop 1 item off the stack as [key]. Unpack key and push the resulting tuple
	onto the stack one item at a time.

#### DIRECTORY_RANGE

	Use the current directory for this operation.

	Pop 1 tuple off the stack as [tuple]. Create a range using tuple and push
	range.begin and range.end onto the stack.

#### DIRECTORY_CONTAINS

	Use the current directory for this operation.

	Pop 1 item off the stack as [key]. Check if the current directory contains
	the specified key. Push 1 if it does and 0 if it doesn't.

#### DIRECTORY_OPEN_SUBSPACE

	Use the current directory for this operation.

	Pop 1 tuple off the stack as [tuple]. Open the subspace of the current
	directory specified by tuple and push it onto the directory list.

Directory Logging
--------------------

#### DIRECTORY_LOG_SUBSPACE

	Use the current directory for this operation.

	Pop 1 item off the stack as [prefix]. Let key equal
	prefix + tuple.pack([dir_index]). Set key to be the result of calling
	directory.key() in the current transaction.

#### DIRECTORY_LOG_DIRECTORY

	Use the current directory for this operation.

	Pop 1 item off the stack as [raw_prefix]. Create a subspace log_subspace
	with path (dir_index) and the specified raw_prefix. Set:

	tr[log_subspace[u'path']] = the tuple packed path of the directory.

	tr[log_subspace[u'layer']] = the tuple packed layer of the directory.

	tr[log_subspace[u'exists']] = the packed tuple containing a 1 if the
	directory exists and 0 if it doesn't.

	tr[log_subspace[u'children']] the tuple packed list of children of the
	directory.

	Where log_subspace[u<str>] is the subspace packed tuple containing only the
	single specified unicode string <str>.

Other
-----

#### DIRECTORY_STRIP_PREFIX

	Use the current directory for this operation.

	Pop 1 item off the stack as [byte_array]. Call .key() on the current
	subspace and store the result as [prefix]. Throw an error if the popped
	array does not start with prefix. Otherwise, remove the prefix from the
	popped array and push the result onto the stack.
