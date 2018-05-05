Overview
--------

Your API test program must implement a simple stack machine that exercises the
FoundationDB API. The program is invoked with two or three arguments. The first
argument is a prefix that is the first element of a tuple, the second is the
API version, and the third argument is the path to a cluster file. If the
third argument is not specified, your program may assume that `fdb.open()` will
succeed with no arguments (an fdb.cluster file will exist in the current
directory). Otherwise, your program should connect to the cluster specified
by the given cluster file.

Your stack machine should begin reading the range returned by the tuple range
method of prefix and execute each instruction (stored in the value of the key)
until the range has been exhausted. When this stack machine (along with any
additional stack machines created as part of the test) have finished running,
your program should terminate.

Upon successful termination, your program should exit with code 0. If your
program or any of your stack machines failed to run correctly, then it should
exit with a nonzero exit code.

Instructions are also stored as packed tuples and should be expanded with the
tuple unpack method. The first element of the instruction tuple represents an
operation, and will always be returned as a unicode string. An operation may have
a second element which provides additional data, which may be of any tuple type.

Your stack machine must maintain a small amount of state while executing
instructions:

  - A global transaction map from byte string to Transactions. This map is
    shared by all tester 'threads'.

  - A stack of data items of mixed types and their associated metadata. At a
    minimum, each item should be stored with the 0-based instruction number
    which resulted in it being put onto the stack. Your stack must support push
    and pop operations. It may be helpful if it supports random access, clear
    and a peek operation. The stack is initialized to be empty.

  - A current FDB transaction name (stored as a byte string). The transaction
    name should be initialized to the prefix that instructions are being read
    from.

  - A last seen FDB version, which is a 64-bit integer.


Data Operations
---------------

#### PUSH &lt;item&gt;

    Pushes the provided item onto the stack.

#### DUP

    Duplicates the top item on the stack. The instruction number for the
    duplicate item should be the same as the original.

#### EMPTY_STACK

    Discards all items in the stack.

#### SWAP

    Pops the top item off of the stack as INDEX. Swaps the items in the stack at
    depth 0 and depth INDEX. Does not modify the instruction numbers of the
    swapped items.

#### POP

    Pops and discards the top item on the stack.

#### SUB

    Pops the top two items off of the stack as A and B and then pushes the
    difference (A-B) onto the stack. A and B may be assumed to be integers.

#### CONCAT

    Pops the top two items off the stack as A and B and then pushes the
    concatenation of A and B onto the stack. A and B can be assumed to
    be of the same type and will be either byte strings or unicode strings.

#### LOG_STACK

    Pops the top item off the stack as PREFIX. Using a new transaction with normal
    retry logic, inserts a key-value pair into the database for each item in the
    stack of the form:

        PREFIX + tuple.pack((stackIndex, instructionNumber)) = tuple.pack((item,))

    where stackIndex is the current index of the item in the stack. The oldest
    item in the stack should have stackIndex 0.

    If the byte string created by tuple packing the item exceeds 40000 bytes,
    then the value should be truncated to the first 40000 bytes of the packed
    tuple.

    When finished, the stack should be empty. Note that because the stack may be
    large, it may be necessary to commit the transaction every so often (e.g.
    after every 100 sets) to avoid past_version errors.

FoundationDB Operations
-----------------------

All of these operations map to a portion of the FoundationDB API. When an
operation applies to a transaction, it should use the transaction stored in
the global transaction map corresponding to the current transaction name. Certain
instructions will be followed by one or both of _SNAPSHOT and _DATABASE to
indicate that they may appear with these variations. _SNAPSHOT operations should
perform the operation as a snapshot read. _DATABASE operations should (if
possible) make use of the methods available directly on the FoundationDB
database object, rather than the currently open transaction.

If your binding does not support operations directly on a database object, you
should simulate it using an anonymous transaction. Remember that set and clear
operations must immediately commit (with appropriate retry behavior!).

Any error that bubbles out of these operations must be caught. In the event of
an error, you must push the packed tuple of the string `"ERROR"` and the error
code (as a string, not an integer).

Some operations may allow you to push future values onto the stack. When popping
objects from the stack, the future MUST BE waited on and errors caught before
any operations that use the result of the future.

Whether or not you choose to push a future, any operation that supports optional
futures must apply the following rules to the result:

  - If the result is an error, then its value is to be converted to an error
    string as defined above

  - If the result is void (i.e. the future was just a signal of
    completion), then its value should be the byte string
    `"RESULT_NOT_PRESENT"`

  - If the result is from a GET operation in which no result was
    returned, then its value is to be converted to the byte string
    `"RESULT_NOT_PRESENT"`

#### NEW_TRANSACTION

    Creates a new transaction and stores it in the global transaction map
    under the currently used transaction name.

#### USE_TRANSACTION

    Pop the top item off of the stack as TRANSACTION_NAME. Begin using the
    transaction stored at TRANSACTION_NAME in the transaction map for future
    operations. If no entry exists in the map for the given name, a new
    transaction should be inserted.

#### ON_ERROR

    Pops the top item off of the stack as ERROR_CODE. Passes ERROR_CODE in a
    language-appropriate way to the on_error method of current transaction
    object and blocks on the future. If on_error re-raises the error, bubbles
    the error out as indicated above. May optionally push a future onto the
    stack.

#### GET (_SNAPSHOT, _DATABASE)

    Pops the top item off of the stack as KEY and then looks up KEY in the
    database using the get() method. May optionally push a future onto the
    stack.

#### GET_KEY (_SNAPSHOT, _DATABASE)

    Pops the top four items off of the stack as KEY, OR_EQUAL, OFFSET, PREFIX
    and then constructs a key selector. This key selector is then resolved
    using the get_key() method to yield RESULT. If RESULT starts with PREFIX,
    then RESULT is pushed onto the stack. Otherwise, if RESULT < PREFIX, PREFIX
    is pushed onto the stack. If RESULT > PREFIX, then strinc(PREFIX) is pushed
    onto the stack. May optionally push a future onto the stack.

#### GET_RANGE (_SNAPSHOT, _DATABASE)

    Pops the top five items off of the stack as BEGIN_KEY, END_KEY, LIMIT,
    REVERSE and STREAMING_MODE. Performs a range read in a language-appropriate
    way using these parameters. The resulting range of n key-value pairs are
    packed into a tuple as [k1,v1,k2,v2,...,kn,vn], and this single packed value
    is pushed onto the stack.

#### GET_RANGE_STARTS_WITH (_SNAPSHOT, _DATABASE)

    Pops the top four items off of the stack as PREFIX, LIMIT, REVERSE and
    STREAMING_MODE. Performs a prefix range read in a language-appropriate way
    using these parameters. Output is pushed onto the stack as with GET_RANGE.

#### GET_RANGE_SELECTOR (_SNAPSHOT, _DATABASE)

    Pops the top ten items off of the stack as BEGIN_KEY, BEGIN_OR_EQUAL,
    BEGIN_OFFSET, END_KEY, END_OR_EQUAL, END_OFFSET, LIMIT, REVERSE,
    STREAMING_MODE, and PREFIX. Constructs key selectors BEGIN and END from
    the first six parameters, and then performs a range read in a language-
    appropriate way using BEGIN, END, LIMIT, REVERSE and STREAMING_MODE. Output
    is pushed onto the stack as with GET_RANGE, excluding any keys that do not
    begin with PREFIX.

#### GET_READ_VERSION (_SNAPSHOT)

    Gets the current read version and stores it in the internal stack machine
    state as the last seen version. Pushed the string "GOT_READ_VERSION" onto
    the stack.

#### GET_VERSIONSTAMP

    Calls get_versionstamp and pushes the resulting future onto the stack.

#### SET (_DATABASE)

    Pops the top two items off of the stack as KEY and VALUE. Sets KEY to have
    the value VALUE. A SET_DATABASE call may optionally push a future onto the
    stack.

#### SET_READ_VERSION

    Sets the current transaction read version to the internal state machine last
    seen version.

#### CLEAR (_DATABASE)

    Pops the top item off of the stack as KEY and then clears KEY from the
    database. A CLEAR_DATABASE call may optionally push a future onto the stack.

#### CLEAR_RANGE (_DATABASE)

    Pops the top two items off of the stack as BEGIN_KEY and END_KEY. Clears the
    range of keys from BEGIN_KEY to END_KEY in the database. A
    CLEAR_RANGE_DATABASE call may optionally push a future onto the stack.

#### CLEAR_RANGE_STARTS_WITH (_DATABASE)

    Pops the top item off of the stack as PREFIX and then clears all keys from
    the database that begin with PREFIX. A CLEAR_RANGE_STARTS_WITH_DATABASE call
    may optionally push a future onto the stack.

#### ATOMIC_OP (_DATABASE)

    Pops the top three items off of the stack as OPTYPE, KEY, and VALUE.
    Performs the atomic operation described by OPTYPE upon KEY with VALUE. An
    ATOMIC_OP_DATABASE call may optionally push a future onto the stack.

#### READ_CONFLICT_RANGE and WRITE_CONFLICT_RANGE

    Pops the top two items off of the stack as BEGIN_KEY and END_KEY. Adds a
    read conflict range or write conflict range from BEGIN_KEY to END_KEY.
    Pushes the byte string "SET_CONFLICT_RANGE" onto the stack.

#### READ_CONFLICT_KEY and WRITE_CONFLICT_KEY

    Pops the top item off of the stack as KEY. Adds KEY as a read conflict key
    or write conflict key. Pushes the byte string "SET_CONFLICT_KEY" onto the
    stack.

#### DISABLE_WRITE_CONFLICT

    Sets the NEXT_WRITE_NO_WRITE_CONFLICT_RANGE transaction option on the
    current transaction. Does not modify the stack.

#### COMMIT

    Commits the current transaction (with no retry behavior). May optionally
    push a future onto the stack.

#### RESET

    Resets the current transaction.

#### CANCEL

    Cancels the current transaction.

#### GET_COMMITTED_VERSION

    Gets the committed version from the current transaction and stores it in the
    internal stack machine state as the last seen version. Pushes the byte
    string "GOT_COMMITTED_VERSION" onto the stack.

#### WAIT_FUTURE

    Pops the top item off the stack and pushes it back on. If the top item on
    the stack is a future, this will have the side effect of waiting on the
    result of the future and pushing the result on the stack. Does not change
    the instruction number of the item.

Tuple Operations
----------------

#### TUPLE_PACK

    Pops the top item off of the stack as N. Pops the next N items off of the
    stack and packs them as the tuple [item0,item1,...,itemN], and then pushes
    this single packed value onto the stack.

#### TUPLE_PACK_WITH_VERSIONSTAMP

    Pops the top item off of the stack as a byte string prefix. Pops the next item
    off of the stack as N. Pops the next N items off of the stack and packs them
    as the tuple [item0,item1,...,itemN], with the provided prefix and tries to
    append the position of the first incomplete versionstamp as if the byte
    string were to be used as a key in a SET_VERSIONSTAMP_KEY atomic op. If there
    are no incomplete versionstamp instances, then this pushes the literal byte
    string 'ERROR: NONE' to the stack. If there is more than one, then this pushes
    the literal byte string 'ERROR: MULTIPLE'. If there is exactly one, then it pushes
    the literal byte string 'OK' and then pushes the packed tuple. (Languages that
    do not contain a 'Versionstamp' tuple-type do not have to implement this
    operation.)

#### TUPLE_UNPACK

    Pops the top item off of the stack as PACKED, and then unpacks PACKED into a
    tuple. For each element of the tuple, packs it as a new tuple and pushes it
    onto the stack.

#### TUPLE_RANGE

    Pops the top item off of the stack as N. Pops the next N items off of the
    stack, and passes these items as a tuple (or array, or language-appropriate
    structure) to the tuple range method. Pushes the begin and end elements of
    the returned range onto the stack.

#### TUPLE_SORT

    Pops the top item off of the stack as N. Pops the next N items off of the
    stack as packed tuples (i.e., byte strings), unpacks them, sorts the tuples,
    repacks them into byte strings, and then pushes these packed tuples onto
    the stack so that the final top of the stack now has the greatest
    element. If the binding has some kind of tuple comparison function, it should
    use that to sort. Otherwise, it should sort them lexicographically by
    their byte representation. The choice of function should not affect final sort order.

#### ENCODE_FLOAT

    Pops the top item off of the stack. This will be a byte-string of length 4
    containing the IEEE 754 encoding of a float in big-endian order.
    This is then converted into a float and pushed onto the stack.

#### ENCODE_DOUBLE

    Pops the top item off of the stack. This will be a byte-string of length 8
    containing the IEEE 754 encoding of a double in big-endian order.
    This is then converted into a double and pushed onto the stack.

#### DECODE_FLOAT

    Pops the top item off of the stack. This will be a single-precision float.
    This is converted into a (4 byte) byte-string of its IEEE 754 representation
    in big-endian order, and pushed onto the stack.

#### DECODE_DOUBLE

    Pops the top item off of the stack. This will be a double-precision float.
    This is converted into a (8 byte) byte-string its IEEE 754 representation
    in big-endian order, and pushed onto the stack.


Thread Operations
-----------------

#### START_THREAD

    Pops the top item off of the stack as PREFIX. Creates a new stack machine
    instance operating on the same database as the current stack machine, but
    operating on PREFIX. The new stack machine should have independent internal
    state. The new stack machine should begin executing instructions concurrent
    with the current stack machine through a language-appropriate mechanism.

#### WAIT_EMPTY

    Pops the top item off of the stack as PREFIX. Blocks execution until the
    range with prefix PREFIX is not present in the database. This should be
    implemented as a polling loop inside of a language- and binding-appropriate
    retryable construct which synthesizes FoundationDB error 1020 when the range
    is not empty. Pushes the string "WAITED_FOR_EMPTY" onto the stack when
    complete.

Miscellaneous
-------------

#### UNIT_TESTS

    This is called during the scripted test to allow bindings to test features
    which aren't supported by the stack tester. Things currently tested in the
    UNIT_TESTS section:

        Transaction options
        Watches
        Cancellation
        Retry limits
        Timeouts
