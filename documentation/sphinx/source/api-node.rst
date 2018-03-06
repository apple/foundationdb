.. default-domain:: js
.. highlight:: javascript

.. Required substitutions for api-common.rst.inc

.. |database-type| replace:: ``Database``
.. |database-class| replace:: :class:`Database`
.. |database-auto| replace:: :func:`Database.doTransaction`
.. |transaction-class| replace:: :class:`Transaction`
.. |get-key-func| replace:: :func:`Transaction.getKey`
.. |get-range-func| replace:: :func:`Transaction.getRange`
.. |commit-func| replace:: :func:`Transaction.commit`
.. |reset-func-name| replace:: :func:`reset <Transaction.reset>`
.. |reset-func| replace:: :func:`Transaction.reset`
.. |cancel-func| replace:: :func:`Transaction.cancel`
.. |init-func| replace:: :func:`fdb.init`
.. |open-func| replace:: :func:`fdb.open`
.. |on-error-func| replace:: :func:`Transaction.onError`
.. |null-type| replace:: ``null`` or ``undefined``
.. |error-type| replace:: error
.. |error-raise-type| replace:: respond with
.. |future-cancel| replace:: :func:`Future.cancel`
.. |max-watches-database-option| replace:: :func:`Database.options.setMaxWatches`
.. |future-type-string| replace:: a :ref:`future <nodeFutures>`
.. |read-your-writes-disable-option| replace:: :func:`Transaction.options.setReadYourWritesDisable`
.. |lazy-iterator-object| replace:: :class:`LazyIterator`
.. |key-meth| replace:: :func:`Subspace.key`
.. |directory-subspace| replace:: :ref:`DirectorySubspace <api-node-directory-subspace>`
.. |directory-layer| replace:: :class:`DirectoryLayer <fdb.DirectoryLayer>`
.. |subspace| replace:: :class:`Subspace <fdb.Subspace>`
.. |subspace-api| replace:: :ref:`subspaces <api-node-subspaces>`
.. |as-foundationdb-key| replace:: :func:`asFoundationDBKey`
.. |as-foundationdb-value| replace:: :func:`asFoundationDBValue`
.. |tuple-layer| replace:: :ref:`tuple layer <api-node-tuple-layer>`
.. |dir-path-type| replace:: an array
.. |node-subspace| replace:: ``nodeSubspace``
.. |content-subspace| replace:: ``contentSubspace``
.. |allow-manual-prefixes| replace:: ``allowManualPrefixes``

.. include:: api-common.rst.inc

###########
Node.js API
###########

.. |infrequent| replace:: *Infrequently used*. 
.. |asynchronous-function| replace:: This function is asynchronous
.. |callback| replace:: |asynchronous-function|. When complete, :ref:`callback <callbacks>` will be called 
.. |callback-when| replace:: |asynchronous-function|. :ref:`callback <callbacks>` will be called 
.. |if-no-callback| replace:: If the :ref:`callback <callbacks>` argument is omitted, then the function will return a :class:`Future` object that can be used to access the result. 
.. |if-no-callback-no-result| replace:: If the :ref:`callback <callbacks>` argument is omitted, then the function will return a :class:`Future` object that can be used to wait for completion. 

Installation
============

You can download the FoundationDB Node.js API directly from :doc:`downloads`.

.. note:: |separately-installed-bindings|
.. note:: |project-dependency|

API versioning
==============

When you require the ``fdb`` module, it exposes only one useful symbol:

.. function:: apiVersion(version)

    Returns the FoundationDB module corresponding with a particular API version. This allows future versions of FoundationDB to make API changes without breaking existing programs. The current version of the API is |api-version|.

    For example::

        var fdb = require('fdb').apiVersion(510)

    | **Throws** an error if the specified version is not supported.
    | **Throws** an error if ``apiVersion`` has been called previously with a different version.

.. note:: You must call ``apiVersion(...)`` to get access to the fdb module.

.. note:: |api-version-rationale|

.. warning:: |api-version-multi-version-warning|

For API changes between version 14 and |api-version| (for the purpose of porting older programs), see :doc:`release-notes`.

Opening a database
==================

After importing the ``fdb`` module and selecting an API version, you probably want to open a :class:`Database`. The simplest way of doing this is using |open-func| ::

    var fdb = require('fdb').apiVersion(510)
    var db = fdb.open();

.. function:: fdb.open( [clusterFile, dbName] )

    |fdb-open-blurb|

    Returns a :class:`Database` object.

    .. note:: In this release, dbName must be "DB".

    .. note:: ``fdb.open()`` combines the effect of :func:`fdb.init`, :func:`fdb.createCluster`, and :func:`Cluster.openDatabase`.

    **Throws** an error if the network was unable to be initialized, the cluster could not be created, or the database could not be opened. However, a database that is unavailable does *not* cause an error to be thrown.

.. function:: fdb.init()

    Initializes the FoundationDB API, creating a thread for the FoundationDB client and initializing the client's networking engine. |init-func|  can only be called once. If called subsequently or after |open-func| , it will raise a ``client_invalid_operation`` error.

    **Throws** an error if the network was unable to be initialized.

.. function:: fdb.createCluster( [clusterFile] )

    Connects to the cluster specified by :ref:`clusterFile <foundationdb-cluster-file>`, or by a :ref:`default cluster file <default-cluster-file>` if ``clusterFile`` is null or undefined.

    Returns a :class:`Cluster` object.

    **Throws** an error if the cluster could not be created.

.. attribute:: fdb.options

    |network-options-blurb|

    .. note:: |network-options-warning|

    .. function:: fdb.options.setTraceEnable(outputDirectory)

        |option-trace-enable-blurb|

        .. warning:: |option-trace-enable-warning|

    .. function:: fdb.options.setTraceMaxLogsSize(bytes)

        |option-trace-max-logs-size-blurb|

    .. function:: fdb.options.setTraceRollSize(bytes)

        |option-trace-roll-size-blurb|

    .. function :: fdb.options.setDisableMultiVersionClientApi()

       |option-disable-multi-version-client-api|

    .. function :: fdb.options.setCallbacksOnExternalThreads()

       |option-callbacks-on-external-threads|

    .. function :: fdb.options.setExternalClientLibrary(pathToLib)

       |option-external-client-library|

    .. function :: fdb.options.setExternalClientDirectory(pathToLibDirectory)

       |option-external-client-directory|

Cluster objects
===============

.. class:: Cluster

.. function:: Cluster.openDatabase( [name="DB"] )

    Opens a database with the given name.

    Returns a :class:`Database` object.

    .. note:: In this release, name **must** be "DB".

 **Throws** an error if the database could not be opened.

.. _api-node-keys:
.. _dataTypes:

Keys and values
===============

Keys and values in FoundationDB are byte arrays stored in `Node.js Buffers <http://nodejs.org/api/buffer.html>`_. Functions which take keys and values as arguments can also accept JavaScript strings, which will be encoded using the UTF-8 encoding, or ArrayBuffers. The ``fdb`` module also provides some convenience methods for converting to and from Buffers in :attr:`fdb.buffer`. 

|keys-values-other-types-blurb|

``asFoundationDBKey`` and ``asFoundationDBValue``
-------------------------------------------------

|as-foundationdb-blurb|

.. warning :: |as-foundationdb-warning|

Buffer conversion
=================

FoundationDB uses Node.js Buffers for all :ref:`keys and values <dataTypes>`. The ``fdb.buffer`` functions provide some utilities for converting other data types to and from Node.js Buffers.

.. function:: fdb.buffer(obj)

    Converts ``obj`` to a Node.js Buffer, assuming it is of an appropriate type. This is called automatically for any key or value passed to the :class:`Transaction` functions. The supported types are:

    * ``Buffer`` - ``obj`` is returned unchanged
    * ``ArrayBuffer`` - ``obj`` is copied to a new ``Buffer``
    * ``Uint8Array`` - ``obj`` is copied to a new ``Buffer``
    * ``string`` - ``obj`` is encoded using UTF-8 and returned as a ``Buffer``

.. function:: fdb.buffer.fromByteLiteral(str)

    Attempts to interpret the string ``str`` as a byte-literal string by converting each character to its one-byte character code using ``str.charCodeAt()``. Returns the result as a Node.js ``Buffer``.

    **Throws** an error if ``str`` contains any multi-byte characters.

    .. note :: This should not be used for encoding unicode strings. Instead, use ``new Buffer(str)`` or consider using the :attr:`fdb.tuple` layer.

.. function:: fdb.buffer.toByteLiteral(buffer)

    Converts a Node.js ``Buffer`` to a byte-literal string by converting each byte to a character using ``String.fromCharCode()``.

    .. note :: This should not be used for decoding unicode strings. Instead, use ``buffer.toString()`` or consider using the :attr:`fdb.tuple` layer.

.. _keyValuePairs:

Key-value pairs
===============

.. class:: KeyValuePair

A class that holds the key and value of a single entry in the database. Each of the :class:`Transaction` range query function returns a :class:`LazyIterator` of key-value pairs.

.. attribute:: key

    A :ref:`Buffer <dataTypes>` containing the key.

.. attribute:: value

    A :ref:`Buffer <dataTypes>` containing the value.

Key selectors
=============

|keysel-blurb1|

|keysel-blurb2|

.. class:: fdb.KeySelector(key, orEqual, offset)

    Creates a key selector with the given reference :ref:`key <dataTypes>`, equality flag, and offset. It is usually more convenient to obtain a key selector with one of the following methods:

    .. function:: fdb.KeySelector.lastLessThan(key)

        Returns a key selector referencing the last (greatest) key in the database less than the specified :ref:`key <dataTypes>`.

    .. function:: fdb.KeySelector.lastLessOrEqual(key)

        Returns a key selector referencing the last (greatest) key less than or equal to the specified :ref:`key <dataTypes>`.

    .. function:: fdb.KeySelector.firstGreaterThan(key)

        Returns a key selector referencing the first (least) key greater than the specified :ref:`key <dataTypes>`.

    .. function:: fdb.KeySelector.firstGreaterOrEqual(key)

        Returns a key selector referencing the first key greater than or equal to the specified :ref:`key <dataTypes>`.

``KeySelector`` objects have the following members:

.. function:: KeySelector.next()

    Returns a key selector referencing the key after the one referenced by this key selector.

.. function:: KeySelector.prev()

    Returns a key selector referencing the key before the one referenced by this key selector.

.. function:: KeySelector.add(addOffset)

    Returns a key selector referencing the key with offset ``addOffset`` relative to this one.

Database objects
================

.. class:: Database

A |database-blurb1| |database-blurb2|

.. function:: Database.doTransaction( func[, callback] )

    Executes the provided function ``func`` with a new transaction, commits the transaction, retries the function as necessary in response to retryable database errors, and calls ``callback`` with the result produced by ``func``. This is the recommended way to do operations transactionally.

    ``func`` must take a :class:`Transaction` as its first argument and an inner :ref:`callback <callbacks>` as its second argument.

    .. note:: Note that the ``callback`` passed to :func:`doTransaction` and the inner callback passed to ``func`` are distinct.

    ``Database.doTransaction`` is asynchronous. When the transaction has been committed, :ref:`callback <callbacks>` will be called with the result produced by ``func``. |if-no-callback| Without suppling a ``callback`` to :func:`doTransaction` or its resulting future, the calling code cannot determine if the transaction resulted in an error or even finished.

    The inner callback passed to ``func`` must be called for the transaction to commit or retry on errors. In particular, this inner callback requires the following:

    * Any FoundationDB API error encountered within ``func`` must be passed to the inner callback as its first argument. Calling the inner callback with any other error will cancel the transaction and pass the error to the outer level.
    * If no errors are encountered, then any result produced by ``func`` must be passed to the inner callback as its second argument. If ``func`` is not intended to produce a result, the inner callback must be called with no arguments.

    .. note:: |fdb-transactional-unknown-result-note|

    Here is an example of the use of :func:`doTransaction`, where ``db`` is a :class:`Database`::

        db.doTransaction(function(tr, innerCallback) {
            tr.get('a', function(err, val) {

                //Errors must be passed back to the callback for retry logic to work!
                if(err) return innerCallback(err);

                tr.set('b', val);
                innerCallback(null, val);
            });
        }, function(err, val) {
            if(err)
                console.log('transaction failed: ', err)
            else
                console.log('finished transaction: ', val);
        });

    The above will get the value at key ``'a'`` and set it at key ``'b'`` in the database. Once the transaction has committed, it will print 'finished transaction: <value>'.

    Functions can be made to automatically invoke :func:`doTransaction` by using the :func:`fdb.transactional` function.

.. function:: Database.createTransaction()

    Returns a new :class:`Transaction` object.  Consider using the :func:`Database.doTransaction` or :func:`fdb.transactional` functions to create transactions instead, since they will automatically provide you with appropriate retry behavior.

    **Throws** an error if the transaction could not be created.

.. function:: Database.get( key[, callback] )

    Gets a value associated with the specified :ref:`key <dataTypes>` in the database. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    |callback| with a Buffer containing the requested value or ``null`` if the key does not exist. |if-no-callback|

.. function:: Database.getKey( keySelector[, callback] )

    Gets the key referenced by the specified :class:`KeySelector() <fdb.KeySelector>`. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    |database-get-key-caching-blurb|

    |callback| with a Buffer containing the requested key. |if-no-callback|

.. function:: Database.getRange( begin, end[, options, callback] )

    Gets all keys ``k`` such that ``begin <= k < end`` and their associated values as an array of :ref:`key-value pairs <keyValuePairs>`. Note the exclusion of ``end`` from the range. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    Each of ``begin`` and ``end`` may be a :ref:`key <dataTypes>` or a :class:`KeySelector() <fdb.KeySelector>`. Note that in the case of a :class:`KeySelector`, the exclusion of ``end`` from the range still applies.

    If the ``options`` object is specified, it can have the following optional parameters:

        ``options.limit``
            Only the first ``limit`` keys (and their values) in the range will be returned. If unspecified, defaults to having no limit.

        ``options.reverse``
            If ``true``, then the keys in the range will be returned in reverse order. If unspecified, defaults to ``false``.

        ``options.streamingMode``
            A value from :attr:`fdb.streamingMode` which provides a hint to FoundationDB about how to retrieve the specified range. This option should generally not be specified, allowing FoundationDB to retrieve the full range very efficiently.

    |callback| with an array containing the requested :ref:`key-value pairs <keyValuePairs>`. |if-no-callback|

.. function:: Database.getRangeStartsWith( prefix[, options, callback] )

    Gets all keys ``k`` such that ``k`` begins with :ref:`keyPrefix <dataTypes>`, and their associated values, as an array of :ref:`key-value pairs <keyValuePairs>`. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    If the ``options`` object is specified, it can have the following optional parameters:

        ``options.limit``
            Only the first ``limit`` keys (and their values) in the range will be returned. If unspecified, defaults to having no limit.

        ``options.reverse``
            If ``true``, then the keys in the range will be returned in reverse order. If unspecified, defaults to ``false``.

        ``options.streamingMode``
            A value from :attr:`fdb.streamingMode` which provides a hint to FoundationDB about how to retrieve the specified range. This option should generally not be specified, allowing FoundationDB to retrieve the full range very efficiently.

    |callback| with an array containing the requested :ref:`key-value pairs <keyValuePairs>`. |if-no-callback|

.. function:: Database.set( key, value[, callback] )

    Associates the given :ref:`key <dataTypes>` and :ref:`value <dataTypes>` and commits the result.  Overwrites any prior value associated with ``key``. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    |callback-when| once the transaction has been committed. |if-no-callback-no-result|

.. function:: Database.clear( key[, callback] )

    Removes the specified :ref:`key <dataTypes>` (and any associated value) if it exists and commits the result. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    |callback-when| once the transaction has been committed. |if-no-callback-no-result|

.. function:: Database.clearRange( begin, end[, callback] )

    Removes all keys ``k`` such that :ref:`begin <dataTypes>` <= k < :ref:`end <dataTypes>`, and their associated values, and commits the result. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    .. note :: Unlike in the case of :func:`Database.getRange`, ``begin`` and ``end`` must be :ref:`keys <dataTypes>`, not :class:`KeySelector()s<fdb.KeySelector>`.

    |callback-when| once the transaction has been committed. |if-no-callback-no-result|

.. function:: Database.clearRangeStartsWith( prefix[, callback] )

    Removes all the keys ``k`` such that ``k`` begins with :ref:`keyPrefix <dataTypes>`, and their associated values, and commits the result. This operation will be executed in a :func:`Database.doTransaction` error retry loop.

    |callback-when| once the transaction has been committed. |if-no-callback-no-result|

.. function:: Database.getAndWatch( key[, callback] )

    Returns an object ``{value, watch}``, where ``value`` is the value associated with ``key`` or ``null`` if the key does not exist, and ``watch`` is a :ref:`future <nodeFutures>` that will become ready after ``value`` changes.

    See :func:`Transaction.watch` for a general description of watches and their limitations.

    |asynchronous-function|. When the watch has been committed, :ref:`callback <callbacks>` will be called with an object ``{value, watch}`` containing the value stored at ``key`` along with the watch :ref:`future <nodeFutures>`. |if-no-callback|

.. _api-node-setAndWatch:

.. function:: Database.setAndWatch( key, value[, callback] )

    Sets ``key`` to ``value`` and returns an object ``{watch}``, where ``watch`` is a :ref:`future <nodeFutures>` that will become ready after a subsequent change to ``value``.

    See :func:`Transaction.watch` for a general description of watches and their limitations.

    |asynchronous-function|. When the watch has been committed, :ref:`callback <callbacks>` will be called with an object ``{watch}`` containing the watch :ref:`future <nodeFutures>`. |if-no-callback|

.. function:: Database.clearAndWatch( key[, callback] )

    Removes ``key`` (and any associated value) if it exists and returns an object ``{watch}``, where ``watch`` is a :ref:`future <nodeFutures>` that will become ready after the value is subsequently set.

    See :func:`Transaction.watch` for a general description of watches and their limitations.

    |asynchronous-function|. When the watch has been committed, :ref:`callback <callbacks>` will be called with an object ``{watch}`` containing the watch :ref:`future <nodeFutures>`. |if-no-callback|

.. function:: Database.add(key, param, callback)
.. function:: Database.bit_and(key, param, callback)
.. function:: Database.bit_or(key, param, callback)
.. function:: Database.bit_xor(key, param, callback)

    Executes the :ref:`associated atomic operation <api-node-transaction-atomic-operations>` on a :class:`Transaction` and commits the result. These operations are each executed in a :func:`Database.doTransaction` error retry loop.

    |callback-when| once the transaction has been committed. |if-no-callback-no-result|

    .. note :: |database-atomic-ops-idempotency-note|

Database options
----------------

|database-options-blurb|

.. function:: Database.options.setLocationCacheSize( size )

    |option-location-cache-size-blurb|

.. function:: Database.options.setMaxWatches( maxWatches )

    |option-max-watches-blurb|

.. function:: Database.options.setMachineId( id )

    |option-machine-id-blurb|

.. function:: Database.options.setDatacenterId( id )

    |option-datacenter-id-blurb|

Transactional decoration
========================

.. function:: fdb.transactional( func )

    The ``fdb.transactional`` decorator wraps a function ``func`` with logic to automatically create a transaction and retry until success, producing a transactional function.

    Here is an example use::

        //Gets the value at key1 and sets it at key2. Returns the value that was set
        var transactionalFunction = fdb.transactional(
            function(tr, key1, key2, callback) {
                tr.get(key1, function(err, val) {

                    //Errors must be passed back to the callback for retry logic to work!
                    if(err) return callback(err);

                    tr.set(key2, val);
                    callback(null, val);
                });
            });

    Any function ``func`` decorated by ``fdb.transactional`` must take a :class:`Transaction` as its first argument and a :ref:`callback <callbacks>` as its last. The callback must be called for the transaction to commit or retry on errors. In particular, this callback requires that:

    * Any FoundationDB API error encountered within ``func`` must be passed to the callback as its first argument. Calling the callback with any other error will cancel the transaction and pass the error to the outer level.
    * If no errors are encountered, then any result produced by ``func`` must be passed to the callback as its second argument. If ``func`` is not intended to produce a result, the callback must be called with no arguments.

    There are several options for invoking the transactional function produced by ``fdb.transactional``.

    * The transactional function can be passed a :class:`Database` as its first argument. In this case, a :class:`Transaction` will be created, passed to ``func``, and be committed before returning to the caller. Any :func:`retryable error <Transaction.onError>` must be passed to the callback by ``func`` as described above, allowing ``func`` to be retried until the transaction successfully commits.
    * The transactional function can be passed a :class:`Transaction` as its first argument. In this case, the transactional function will not attempt to commit or retry the transaction on error. Rather, commit and retry are the responsibility of the caller that owns the transaction. This option allows transactional functions to be freely composed.
    * The transactional function can be invoked without a callback argument. In this case, it will return a :ref:`Future <nodeFutures>`.

    .. note:: |fdb-transactional-unknown-result-note|

Transaction objects
===================

.. class:: Transaction

A ``Transaction`` object represents a FoundationDB database transaction. All operations on FoundationDB take place, explicitly or implicitly, through a ``Transaction``.

|transaction-blurb1|

|transaction-blurb2|

|transaction-blurb3|

The most convenient way to use transactions is to use either the :func:`Database.doTransaction` function or the :func:`fdb.transactional` decorator function.

Attributes
----------

.. attribute:: Transaction.db

    |db-attribute-blurb|

Reading data
------------

.. function:: Transaction.get(key[, callback])

    Gets a value associated with the specified :ref:`key <dataTypes>` in the database.

    |callback| with a Buffer containing the requested value or ``null`` if the key does not exist. |if-no-callback|

.. function:: Transaction.getKey(keySelector[, callback])

    Gets the key referenced by the specified :class:`KeySelector() <fdb.KeySelector>`.

    |transaction-get-key-caching-blurb|

    |callback| with a Buffer containing the requested key. |if-no-callback|

.. function:: Transaction.getRange(begin, end[, options])

    Returns all keys ``k`` such that ``begin <= k < end`` and their associated values as a :class:`LazyIterator` of :ref:`key-value pairs <keyValuePairs>`. Note the exclusion of ``end`` from the range.

    Each of ``begin`` and ``end`` may be a :ref:`key <dataTypes>` or a :class:`KeySelector() <fdb.KeySelector>`. Note that in the case of a :class:`KeySelector`, the exclusion of ``end`` from the range still applies.

    If the ``options`` object is specified, it can have the following optional parameters:

        ``options.limit``
            Only the first ``limit`` keys (and their values) in the range will be returned. If unspecified, defaults to having no limit.

        ``options.reverse``
            If ``true``, then the keys in the range will be returned in reverse order. If unspecified, defaults to ``false``.

        ``options.streamingMode``
            A value from :attr:`fdb.streamingMode` which provides a hint to FoundationDB about how the returned iterator is likely to be used.  If unspecified, defaults to :attr:`fdb.streamingMode.iterator`.

.. function:: Transaction.getRangeStartsWith(keyPrefix[, options])

    Returns all keys ``k`` such that ``k`` begins with :ref:`keyPrefix <dataTypes>`, and their associated values, as a :class:`LazyIterator` of :ref:`key-value pairs <keyValuePairs>`.

    If the ``options`` object is specified, it can have the following optional parameters:

        ``options.limit``
            Only the first ``limit`` keys (and their values) in the range will be returned. If unspecified, defaults to having no limit.

        ``options.reverse``
            If ``true``, then the keys in the range will be returned in reverse order. If unspecified, defaults to ``false``.

        ``options.streamingMode``
            A value from :attr:`fdb.streamingMode` which provides a hint to FoundationDB about how the returned iterator is likely to be used.  If unspecified, defaults to :attr:`fdb.streamingMode.iterator`.

Snapshot reads
--------------

.. attribute:: Transaction.snapshot

    |snapshot-blurb1|

    |snapshot-blurb2|

    |snapshot-blurb3|

    |snapshot-blurb4|

.. attribute:: Transaction.snapshot.db

    |db-attribute-blurb|

.. function:: Transaction.snapshot.get(key[, callback])

    Like :func:`Transaction.get`, but as a snapshot read.

.. function:: Transaction.snapshot.getKey(keySelector[, callback])

    Like :func:`Transaction.getKey`, but as a snapshot read.

.. function:: Transaction.snapshot.getRange(begin, end[, options])

    Like :func:`Transaction.getRange`, but as a snapshot read.

.. function:: Transaction.snapshot.getRangeStartsWith(keyPrefix[, options])

    Like :func:`Transaction.getRangeStartsWith`, but as a snapshot read.

.. function:: Transaction.snapshot.getReadVersion([callback])

    Identical to :func:`Transaction.getReadVersion` (since snapshot and serializable reads use the same read version).

Writing Data
------------

.. function:: Transaction.set(key, value)

    Associates the given :ref:`key <dataTypes>` and :ref:`value <dataTypes>`.  Overwrites any prior value associated with ``key``.  Returns immediately, having modified the snapshot represented by this ``Transaction``.

.. function:: Transaction.clear(key)

    Removes the specified :ref:`key <dataTypes>` (and any associated value), if it exists.  Returns immediately, having modified the snapshot represented by this ``Transaction``.

.. function:: Transaction.clearRange(begin, end)

    Removes all keys ``k`` such that :ref:`begin <dataTypes>` <= k < :ref:`end <dataTypes>`, and their associated values.  Returns immediately, having modified the snapshot represented by this ``Transaction``.

    .. note :: Unlike in the case of :func:`Transaction.getRange`, ``begin`` and ``end`` must be :ref:`keys <dataTypes>`, not :class:`KeySelector()s<fdb.KeySelector>`. (Resolving arbitrary key selectors would prevent this method from returning immediately, introducing concurrency issues.)

.. function:: Transaction.clearRangeStartsWith(prefix)

    Removes all the keys ``k`` such that ``k`` begins with :ref:`keyPrefix <dataTypes>`, and their associated values.  Returns immediately, having modified the snapshot represented by this transaction.

.. _api-node-transaction-atomic-operations:

Atomic operations
-----------------

|atomic-ops-blurb1|

|atomic-ops-blurb2|

|atomic-ops-blurb3|

.. warning:: |atomic-ops-warning|

In each of the methods below, ``param`` should be a ``Buffer`` appropriately packed to represent the desired value. For example::

    # wrong
    tr.add('key', 1);

    # right
    var value = new Buffer(4);
    value.writeUInt32LE(1, 0);
    tr.add('key', value);

.. function:: Transaction.add(key, param)

    |atomic-add1|

    |atomic-add2|

.. function:: Transaction.bit_and(key, param)

    |atomic-and|

.. function:: Transaction.bit_or(key, param)

    |atomic-or|

.. function:: Transaction.bit_xor(key, param)

    |atomic-xor|

.. function:: Transaction.max(key, param)

    |atomic-max1|

    |atomic-max-min|

.. function:: Transaction.byte_max(key, param)

    |atomic-byte-max|

.. function:: Transaction.min(key, param)

    |atomic-min1|

    |atomic-max-min|
    
.. function:: Transaction.byte_min(key, param)

    |atomic-byte-min|

.. function:: Transaction.set_versionstamped_key(key, param)

    |atomic-set-versionstamped-key-1|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    |atomic-set-versionstamped-key-2|

    .. warning :: |atomic-versionstamps-tuple-warning-key|

.. function:: Transaction.set_versionstamped_value(key, param)

    |atomic-set-versionstamped-value|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    .. warning :: |atomic-versionstamps-tuple-warning-value|

Committing
----------

.. function:: fdb.transactional( func )

    The ``fdb.transactional`` decorator function makes it easy to write transactional functions which automatically commit. See :func:`fdb.transactional` for explanation and examples.

.. function :: Transaction.doTransaction( func[, callback] )

    Executes the provided function ``func`` with the transaction and calls ``callback`` with the result produced by ``func``. This is the recommended way to do operations transactionally.

    ``func`` must take a :class:`Transaction` as its first argument and an inner :ref:`callback <callbacks>` as its second argument.

    .. note:: Note that the ``callback`` passed to :func:`doTransaction` and the inner callback passed to ``func`` are distinct.

    This function, along with ``Database.doTransaction``, makes it easy to write transactional functions which automatically commit. See :func:`Database.doTransaction` for explanation of their interaction and examples.

.. function :: Transaction.commit([callback])

    Attempt to commit the changes made in the transaction to the database.

    |callback-when| once the transaction has committed. |if-no-callback-no-result|

    |commit-unknown-result-blurb|

    |commit-outstanding-reads-blurb|

    .. note :: Consider using either the :func:`fdb.transactional` decorator function or the :func:`Database.doTransaction` function, which not only call :func:`Database.createTransaction` and :func:`Transaction.commit()` for you but also implement the required error handling and retry logic for transactions.

    .. warning :: |used-during-commit-blurb|

.. function :: Transaction.onError(error[, callback])

    Determine whether an error returned by a method of ``Transaction`` is retryable. |asynchronous-function|. If the error is retryable, ``onError`` will delay, :func:`Transaction.reset` the transaction, and call :ref:`callback <callbacks>` with no result. Otherwise, the :ref:`callback <callbacks>` will be called immediately with the error. |if-no-callback|

    .. note :: Consider using either the :func:`fdb.transactional` decorator function or the :func:`Database.doTransaction` function, which call this method for you.

.. function:: Transaction.reset()

    |transaction-reset-blurb|

.. function :: Transaction.cancel()

    |transaction-cancel-blurb|
    
    .. warning :: |transaction-reset-cancel-warning|

    .. warning :: |transaction-commit-cancel-warning|

Watches
-------

.. function:: Transaction.watch(key)

    Returns a :class:`Future` that will be notified when the value stored at ``key`` changes.

    |transaction-watch-blurb|

    |transaction-watch-committed-blurb|

    |transaction-watch-error-blurb|

    |transaction-watch-limit-blurb|

    .. warning:: Unlike most asynchronous function in the FoundationDB API, watches do not take a callback. Instead, one must be passed to the :ref:`future <nodeFutures>` that the watch function returns. A watch will be automatically cancelled if the :ref:`future <nodeFutures>` returned by the watch function is garbage collected.


Conflict ranges
---------------

.. note:: |conflict-range-note|

|conflict-range-blurb|

.. function:: Transaction.addReadConflictRange(begin, end)

    |add-read-conflict-range-blurb|

.. function:: Transaction.addReadConflictKey(key)

    |add-read-conflict-key-blurb|

.. function:: Transaction.addWriteConflictRange(begin, end)

    |add-write-conflict-range-blurb|

.. function:: Transaction.addWriteConflictKey(key)

    |add-write-conflict-key-blurb|

Versions
--------

Most applications should use the read version that FoundationDB determines automatically during the transaction's first read and ignore all of these methods.

.. function:: Transaction.setReadVersion(version)

    |infrequent| Sets the database version that the transaction will read from the database.  The database cannot guarantee causal consistency if this method is used (the transaction's reads will be causally consistent only if the provided read version has that property).

.. function :: Transaction.getReadVersion([callback])

    |infrequent| |callback| with the transaction's read version. |if-no-callback|

.. function:: Transaction.getCommittedVersion()

    |infrequent| |transaction-get-committed-version-blurb|

.. function:: Transaction.getVersionstamp()

    |infrequent| |transaction-get-versionstamp-blurb|

Transaction options
-------------------

|transaction-options-blurb|

.. _api-node-snapshot-ryw:

.. function:: Transaction.options.setSnapshotRywDisable

    |option-snapshot-ryw-disable-blurb|

.. function:: Transaction.options.setSnapshotRywEnable

    |option-snapshot-ryw-enable-blurb|

.. function:: Transaction.options.setPriorityBatch()

    |option-priority-batch-blurb|

.. function:: Transaction.options.setPrioritySystemImmediate()

    |option-priority-system-immediate-blurb|

    .. warning:: |option-priority-system-immediate-warning|

.. function:: Transaction.options.setCausalReadRisky()

    |option-causal-read-risky-blurb|

.. function:: Transaction.options.setCausalWriteRisky()

    |option-causal-write-risky-blurb|

.. function:: Transaction.options.setNextWriteNoWriteConflictRange()

    |option-next-write-no-write-conflict-range-blurb|

.. function:: Transaction.options.setReadYourWritesDisable()

    |option-read-your-writes-disable-blurb|

    .. note:: |option-read-your-writes-disable-note|

.. function:: Transaction.options.setReadAheadDisable()

    |option-read-ahead-disable-blurb|

.. function:: Transaction.options.setAccessSystemKeys()

    |option-access-system-keys-blurb|

    .. warning:: |option-access-system-keys-warning|

.. function:: Transaction.options.setReadSystemKeys()

    |option-read-system-keys-blurb|

    .. warning:: |option-read-system-keys-warning|

.. function:: Transaction.options.setRetryLimit(numRetries)

    |option-set-retry-limit-blurb1|

    |option-set-retry-limit-blurb2|

.. function:: Transaction.options.setMaxRetryDelay(delay)

    |option-set-max-retry-delay-blurb|

.. function:: Transaction.options.setTimeout(timeoutInMilliseconds)

    |option-set-timeout-blurb1|
    
    |option-set-timeout-blurb2|

    |option-set-timeout-blurb3|

.. function:: Transaction.options.setDurabilityDevNullIsWebScale()

    |option-durability-dev-null-is-web-scale-blurb|

.. _callbacks:

Callbacks
=========

Many functions in the FoundationDB API are asynchronous and take as their final parameter an optional callback which is called when the function has completed. Unless otherwise stated, these callbacks will always have the following signature::

    callback = function(error, result) { }

If error is null or undefined, then the function succeeded, and the result it returned (if any) is provided in the second argument.

Functions taking callbacks also return :ref:`futures <nodeFutures>`. If such a function is called without passing it a callback, then one can be set later by passing the callback to the returned future.

.. _streamline:

Streamline
----------

The FoundationDB Node bindings also support use of the `streamline module <https://github.com/Sage/streamlinejs>`_ for simplified asynchronous development. If using the streamline module, any function which takes a callback may also take an ``_`` to execute the function in a blocking manner::

    var db = fdb.open();

    var x = db.get('x', _); //Blocks until finished
    var y = db.get('y', _); //Blocks until finished

.. _nodeFutures:

Futures
-------
 
.. class:: Future
 
Asynchronous functions in the FoundationDB API generally take a :ref:`callback <callbacks>` argument that will be called when the operation fails or completes. If no callback argument is supplied to an asynchronous FoundationDB function, then it will instead return a :class:`Future` object::

    // Database.get takes a callback
    db.get('x', function(err, val))
        console.log('got x', err, val);
    });

    // Database.get can instead return a future
    var future = db.get('x');

.. note:: If you pass a callback to an asynchronous function in the FoundationDB API, it will not return a Future.

The result of an operation can be retrieved by passing a callback to the future object::

    var future = db.get('x');
    future(function(err, val) {
        console.log('got x', err, val);
    });


Futures make it possible to start multiple operations before waiting for the result. For example::

    // Start both reads
    var x = db.get('x')
    var y = db.get('y')

    // Wait for both reads to complete
    x(function(xErr, xVal) {
        y(function(yErr, yVal) {
            if(xErr || yErr)
                console.log(xErr, yErr);
            else
                console.log('Finished parallel get:', xVal, yVal);
        });
    });

Futures can be combined with :ref:`streamline`::

    var x = db.get('x');
    var y = db.get('x');

    //This blocks until both gets have completed
    console.log(x(_) + y(_));

.. _api-node-promises:

Promises
^^^^^^^^

Futures also support the `Promises/A+ specification <http://promises-aplus.github.io/promises-spec/>`_, a popular standard for asynchronous operations in JavaScript. In the Promise/A+ interface, a promise is a future that exposes a ``then`` function, which registers callbacks to receive either a promise's eventual value or the reason why the promise cannot be fulfilled. A promise can be in one of three states: ``pending``, ``fulfilled``, or ``rejected``.

* When ``pending``, a promise will eventually transition to either the ``fulfilled`` or ``rejected`` state.
* When ``fulfilled``, a promise has a value, which will not change.
* When ``rejected``, a promise has a reason, which will not change.

.. function:: Future.cancel()

    Attempts to cancel the future and its associated asynchronous operation, if possible. If a :ref:`callback <callbacks>` was passed to the original function or its returned future, it may be called with an :ref:`operation_cancelled <developer-guide-error-codes>` error.

    .. note:: Currently, only :func:`watches <Transaction.watch>` support cancellation. All other asynchronous FoundationDB operations will continue running normally when cancelled.

.. function:: Future.then(onFulfilled, onRejected)

    Provides access to the promise's current or eventual value or the reason it was rejected. ``then`` may be called multiple times on the same promise.

    ``onFulfilled`` and ``onRejected`` are both optional arguments. If ``onFulfilled`` is a function, it will be called after promise is fulfilled, with promise's value as its first argument. If ``onRejected`` is a function, it will be called after promise is rejected, with promise's reason as its first argument. See the `specification <http://promises-aplus.github.io/promises-spec/>`_ for further details.

    Returns a :class:`Future`.

.. function:: Future.catch(onRejected)

    Provides access to the reason a promise is rejected. ``onRejected`` will be called after promise is rejected, with promise's reason as its first argument. ``catch`` may be called multiple times on the same promise. 
    
    Equivalent to calling :func:`Future.then(undefined, onRejected) <Future.then>`.

Promise utilities
-----------------

The ``fdb.future`` functions provide utilities for working with futures in a manner compatible with the promise interface. ``fdb.future`` also exports the prototype of created futures.

.. function:: fdb.future.create(func [, cb])

    Creates a :class:`Future` in the ``pending`` state, i.e. neither ``fulfilled`` nor ``rejected``, that will be satisfied once ``func`` has finished executing (either successfully or with an error). ``func`` will be passed a single :ref:`callback <callbacks>` argument ``futureCb`` that ``func`` should call when it is complete. 
    
    If the ``cb`` argument is supplied to ``fdb.future.create``, then no future will be returned; instead ``cb`` will be called when ``func`` has finished executing. Otherwise, a :class:`Future` will be returned. This allows an API to provide functions that can take Node-style callbacks or return :class:`Futures <Future>`. For example::

        function getAndClearFoo(tr, cb) {
            return fdb.future.create(function(futureCb) {
                tr.get('foo', function(err, val) {
                    if(err) return futureCb(err);
                    tr.clear('foo');
                    futureCb(undefined, val);
                });

                // Or we could use promises
                // tr.get('foo')
                // .then(function(val) {
                //    tr.clear('foo');
                //    return val;
                // })(futureCb);
            }, cb);
        }

        // Call getAndClearFoo without passing a callback
        var future = getAndClearFoo(tr);
        future.then(function(value) {
            console.log('got value', value);
        }, function(reason) {
            console.log('got error', reason);
        });

        // Call getAndClearFoo with a callback
        getAndClearFoo(tr, function(err, val) {
            if(err)
                console.log('got error', err);
            else
                console.log('got value', val);
        });

.. function:: fdb.future.resolve(value)

    Creates a :class:`Future` that is ``fulfilled`` with ``value``.

.. function:: fdb.future.reject(reason)

    Creates a :class:`Future` that is ``rejected`` with ``reason``.

.. function:: fdb.future.all(futureArray)

    Returns a :class:`Future` that is ``fulfilled`` if and when all futures in ``futureArray`` are ``fulfilled``, or is ``rejected`` if and when any future in ``futureArray`` is ``rejected``. If ``fulfilled``, its value will be an array containing the values that ``fulfilled`` each of the futures in ``futureArray``, in the same order. If ``rejected``, the reason will be the same as the first future that was ``rejected``.

.. function:: fdb.future.race(futureArray)

    Returns a :class:`Future` that is ``fulfilled`` if and when any future in ``futureArray`` is ``fulfilled``, or is ``rejected`` if and when any future in ``futureArray`` is ``rejected``. It's ``fullfilled`` value or ``rejected`` reason will be the same as the first future that was ``fulfilled`` or ``rejected``.

Lazy iterators
==============

.. class:: LazyIterator

:func:`Transaction.getRange` and similar interfaces all return ``LazyIterator`` objects. A ``LazyIterator`` efficiently manages fetching the results of a query in batches as you iterate so that you can avoid receiving more data than you need (see :attr:`fdb.streamingMode` for more information). The interface of a ``LazyIterator`` consists of functions that follow a common pattern.

Except for :func:`LazyIterator.next`, the ``LazyIterator`` functions always start iteration from the beginning of the results, even if the iterator has been used before or is being used concurrently.

.. function:: LazyIterator.forEach(func[, callback]) 

    Iterates over the requested results by calling ``func`` for each item being iterated over.

    ``func`` will be passed the next item in the iteration as its first argument and a :ref:`callback <callbacks>` as its second. ``func`` must call its callback upon completion. If its callback is called with any error or defined result, then iteration will terminate immediately.

    |callback-when| when either all results have been exhausted or ``func`` terminates prematurely. It will be called with the result returned through ``func``'s callback, if any. |if-no-callback|

    .. note:: Note that the ``callback`` passed to :func:`forEach` and the callback passed to ``func`` are distinct.

    For example, the following will print the first 10 keys and values in the range ['apple', 'cherry')::

        var numKeys = 0;
        var itr = tr.getRange('apple', 'cherry');
        itr.forEach(function(kv, cb) {
            console.log(kv.key, kv.value);

            if(++numKeys === 10)
                cb(null, null);
            else
                cb();
        }, function(err, res) {
            console.log('Finished forEach', err, res);
        }

.. function:: LazyIterator.forEachBatch(func[, callback])

    Iterates over the requested range by calling ``func`` for each batch of items fetched from the database. The size of batches is determined by which :attr:`fdb.streamingMode` is used. 

    ``func`` will be passed an array of items containing the current batch of results as its first argument and a :ref:`callback <callbacks>` as its second. ``func`` must call its callback upon completion. If its callback is called with any error or defined result, then iteration will terminate immediately. 

    |callback| when either all results have been exhausted or ``func`` terminates prematurely. It will be called with the result returned through ``func``'s callback, if any. |if-no-callback|

    .. note:: Note that the ``callback`` passed to :func:`forEachBatch` and the callback passed to ``func`` are distinct.

    For example, the following will print the first 10 keys and values in the range ['apple', 'cherry')::

        var numKeys = 0;
        var itr = tr.getRange('apple', 'cherry');
        itr.forEachBatch(function(arr, cb) {
            for(var i = 0; i < arr.length; ++i) {
                console.log(arr[i].key, arr[i].value);
                if(++numKeys === 10) {
                    cb(null, null);
                    return;
                }
            }

            cb();
        }, function(err, res) {
            console.log('Finished forEachBatch', err, res);
        }

.. function:: LazyIterator.toArray([callback])

    Converts the requested results to an array. Unless you intend to use the entire result, it may be more efficient to use one of :func:`LazyIterator.forEachBatch` or :func:`LazyIterator.forEach`.

    |callback| with an array containing all items in the iteration. |if-no-callback|

    For example, the following will print all keys and values in the range ['apple', 'cherry')::

        tr.getRange('apple', 'cherry').toArray(function(err, kvArr) {
            if(err)
                console.log('Error:', err);
            else
                for(var i = 0; i < arr.length; ++i)
                    console.log(kvArr[i].key, kvArr[i].value);
        };

.. function:: LazyIterator.next([callback])

    Gets the next item in the iteration. It is generally easier to use one of :func:`LazyIterator.forEachBatch`, :func:`LazyIterator.forEach`, or :func:`LazyIterator.toArray`.

    |callback| with the next item in the results. If there are no more results, then ``null`` is returned. |if-no-callback|

Streaming modes
===============

.. attribute:: fdb.streamingMode

|streaming-mode-blurb1|

|streaming-mode-blurb2|

The following streaming modes are available:

.. attribute:: fdb.streamingMode.iterator

    *The default.*  The client doesn't know how much of the range it is likely to use and wants different performance concerns to be balanced.

    Only a small portion of data is transferred to the client initially (in order to minimize costs if the client doesn't read the entire range), and as the caller iterates over more items in the range larger batches will be transferred in order to maximize throughput.

.. attribute:: fdb.streamingMode.want_all

    The client intends to consume the entire range and would like it all transferred as early as possible.

.. attribute:: fdb.streamingMode.small

    |infrequent| Transfer data in batches small enough to not be much more expensive than reading individual rows, to minimize cost if iteration stops early.

.. attribute:: fdb.streamingMode.medium

    |infrequent| Transfer data in batches sized in between small and large.

.. attribute:: fdb.streamingMode.large

    |infrequent| Transfer data in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible.  If the client stops iteration early, some disk and network bandwidth may be wasted.  The batch size may still be too small to allow a single client to get high throughput from the database, so if that is what you need consider :attr:`fdb.streamingMode.serial`.

.. attribute:: fdb.streamingMode.serial

    Transfer data in batches large enough that an individual client can get reasonable read bandwidth from the database.  If the client stops iteration early, considerable disk and network bandwidth may be wasted.

.. attribute:: fdb.streamingMode.exact

    |infrequent| The client has passed a specific row limit and wants that many rows delivered in a single batch.  This is not particularly useful in Node.js because iterator functionality makes batches of data transparent, so use :attr:`fdb.streamingMode.want_all` instead.

Errors
======

Errors in the FoundationDB API are raised as exceptions of type :class:`FDBError() <fdb.FDBError>`. These errors may be displayed for diagnostic purposes, but generally should be passed to :func:`Transaction.onError`. When using :func:`Database.doTransaction` or :func:`fdb.transactional`, appropriate errors will be retried automatically.

.. class:: fdb.FDBError

.. attribute:: fdb.FDBError.code

    An integer associated with the error type.

.. attribute:: fdb.FDBError.message

    A somewhat human-readable description of the error.

.. warning:: You should use only :attr:`FDBError.code <fdb.FDBError.code>` for programmatic comparisons, as the message of the error may change at any time. Whenever possible, use the :func:`Transaction.onError()` method to handle :class:`FDBError() <fdb.FDBError>` exceptions.

.. _api-node-tuple-layer:

Tuple layer
===========

.. attribute:: fdb.tuple

|tuple-layer-blurb|

.. note:: |tuple-layer-note|

.. _tupleDataTypes:

In the FoundationDB Node.js API, a tuple is a JavaScript array that contains elements of the following data types:

+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| Type                  | Legal Values                                                                      | Canonical Value           |
+=======================+===================================================================================+===========================+
| Null value            | ``null``                                                                          | ``null``                  |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| Byte string           | Any value of type ``Buffer``, ``ArrayBuffer``, or ``Uint8Array``                  | ``Buffer``                |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| Unicode string        | Any value of type ``String``                                                      | ``String``                |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| 54-bit signed integer | ``Number`` in the range [-2\ :sup:`53`, 2\ :sup:`53`) that has no fractional part | ``Number``                |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| 32-bit float          | ``fdb.tuple.Float`` with a value that fits within a 32-bit float                  | :class:`fdb.tuple.Float`  |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| 64-bit float          | ``fdb.tuple.Double`` with a value that fits within a 64-bit float                 | :class:`fdb.tuple.Double` |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| Boolean               | Any value of type ``Boolean``                                                     | ``Boolean``               |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| UUID                  | ``fdb.tuple.UUID`` with a 16-byte underlying UUID                                 | :class:`fdb.tuple.UUID`   |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+
| Array                 | An ``Array`` consisting of legal tuple-encodable values                           | ``Array``                 |
+-----------------------+-----------------------------------------------------------------------------------+---------------------------+


If ``T`` is a JavaScript array meeting these criteria, then conceptually::

    T == fdb.tuple.unpack(fdb.tuple.pack(T))

.. note:: Unpacking a tuple always returns an array of elements in a canonical representation, so packing and then unpacking a tuple may result in an equivalent but not identical representation.

.. warning:: In general, FoundationDB tuples can contain 64-bit integers. Node.js, however, lacks an appropriate data type to handle integers larger than 54 bits. If you intend to use the Node.js bindings with other language bindings, keep in mind that attempting to decode a tuple with signed integers larger than 54 bits will result in an error being thrown.

.. function:: fdb.tuple.pack(tuple)

    Returns a key (Buffer) encoding the specified array ``tuple``

    **Throws** an error if ``tuple`` contains any elements that are not one of the :ref:`encodable data types <tupleDataTypes>`.

.. function:: fdb.tuple.unpack(key)

    Returns the tuple encoded by the given key as an array

    | **Throws** an error if the unpacked tuple contains any integers outside the range of a 54-bit signed integer.
    | **Throws** an error if the encoded tuple contains unknown data types.

.. function:: fdb.tuple.range(tuple)

    Returns the range containing all keys that encode tuples strictly starting with the array ``tuple`` (that is, all tuples of greater length than ``tuple`` of which ``tuple`` is a prefix).

    The range will be an object of the form::

        var range = {
            begin: new Buffer(''), // A Buffer containing the beginning of the range, inclusive
            end: new Buffer('')    // A Buffer containing the end of the range, exclusive
        };

    This range can be used to call :func:`Transaction.getRange`.  For example::

        var r = fdb.tuple.range(['A', 2]);
        var itr = tr.getRange(r.begin, r.end);

    returns a :class:`LazyIterator` over all :ref:`key-value pairs <keyValuePairs>` in the database whose keys would unpack to tuples like ('A', 2, x), ('A', 2, x, y), etc.

    **Throws** an error if ``tuple`` contains any elements that are not one of the :ref:`encodable data types <tupleDataTypes>`.

.. function:: fdb.tuple.compare(tuple1, tuple2)

    Compares ``tuple1`` and ``tuple2`` and returns -1 if ``tuple1`` will sort before ``tuple2`` when serialized, 0 if they are equal, and 1 otherwise.

    **Throws** an error if either tuple contains any elements that are not one of the :ref:`encodeable data types <tupleDataTypes>`.

.. class:: fdb.tuple.Float(value)

    Wrapper class around a 32-bit floating point number. The constructor for this object takes a ``Number`` and uses it as the underlying
    value for the given object. If the item is out of the correct range, one may experience a loss of precision.
    The contract here is that for any value ``f`` in the correct range, we can conclude::

        f == fdb.tuple.Float.fromBytes(new fdb.tuple.Float(f).toBytes()).value

.. attribute:: fdb.tuple.Float.value

    Underlying value for the :class:`fdb.tuple.Float` instance. It should have the ``Number`` type.

.. attribute:: fdb.tuple.Float.rawData

    Big-endian byte-array representation of the :class:`fdb.tuple.Float` value. This value is only set to something other than ``undefined``
    if the float is a NaN value (as Javascript will collapse all NaN values to a single value otherwise).

.. function:: fdb.tuple.Float.toBytes()

    Retrieve the big-endian byte-array representation of the :class:`fdb.tuple.Float` instance.

.. function:: fdb.tuple.Float.fromBytes(buf)

    Static initializer of the :class:`fdb.tuple.Float` type that starts with a big-endian byte array representation of
    a 32-bit floating point number. The ``buf`` parameter must be a byte buffer of length 4. The contract
    here is that for any byte buffer ``buf``, we can conclude::

        buf == fdb.tuple.Float.fromBytes(buf).toBytes()

    **Throws** a ``RangeError`` if ``buf`` does not have length 4.

.. class:: fdb.tuple.Double(value)

    Wrapper class around a 64-bit floating point number. The constructor for this object takes a ``Number`` and uses it as the underlying
    value for the given object. The contract here is that for any value ``d`` in the correct range, we can conclude::

        d == fdb.tuple.Double.fromBytes(new fdb.tuple.Double(d).toBytes()).value

.. attribute:: fdb.tuple.Double.value

    Underlying value for the :class:`fdb.tuple.Double` instance. It should have the ``Number`` type.

.. attribute:: fdb.tuple.Double.rawData

    Big-endian byte-array representation of the :class:`fdb.tuple.Double` value. This value is only set to something other than ``undefined``
    if the double is a NaN value (as Javascript will collapse all NaN values to a single value otherwise).

.. function:: fdb.tuple.Double.toBytes()

    Retrieve the big-endian byte-array representation of the :class:`fdb.tuple.Double` instance.

.. function:: fdb.tuple.Double.fromBytes(buf)

    Static initializer of the :class:`fdb.tuple.Double` type that starts with a big-endian byte array representation of
    a 64-bit floating point number. The ``buf`` parameter must be a byte buffer of length 8. The contract
    here is that for any byte buffer ``buf``, we can conclude::

        buf == fdb.tuple.Double.fromBytes(buf).toBytes()

    **Throws** a ``RangeError`` if ``buf`` does not have length 8.

.. class:: fdb.tuple.UUID(data)

    Wrapper class around a 128-bit UUID. The constructor takes a byte buffer of length 16 that should represent
    the UUID in big-endian order.

    **Throws** an ``FDBError`` if ``buf`` does not have length 16.

.. attribute:: fdb.tuple.UUID.data 

    Data underlying the :class:`fdb.tuple.UUID` instance. This will be a buffer of length 16 just like the ``data``
    parameter of the :class:`fdb.tuple.UUID` constructor.

.. _api-node-subspaces:

Subspaces
=========

|subspace-blurb1|

|subspace-blurb2|

.. note:: For general guidance on subspace usage, see the discussion in the :ref:`Developer Guide <developer-guide-sub-keyspaces>`.

.. class:: fdb.Subspace([prefixArray=[], rawPrefix=new Buffer(0)])

    |subspace-blurb3|

.. function:: Subspace.key()

    |subspace-key-blurb|

.. function:: Subspace.pack(arr)

    |subspace-pack-blurb|

.. function:: Subspace.unpack(key)

    |subspace-unpack-blurb|

.. function:: Subspace.range(arr=[])

    |subspace-range-blurb|

    The range will be an object of the form::

        var range = {
            begin: new Buffer(''), // A Buffer containing the beginning of the range, inclusive
            end: new Buffer('')    // A Buffer containing the end of the range, exclusive
        };

    This range can be used to call :func:`Transaction.getRange`.  For example::

        var r = subspace.range(['A', 2]);
        var itr = tr.getRange(r.begin, r.end);

.. function:: Subspace.contains(key)

    |subspace-contains-blurb|

.. function:: Subspace.asFoundationDBKey()

    |subspace-as-foundationdb-key-blurb| This method serves to support the :ref:`asFoundationdbKey() <api-node-keys>` convenience interface.

.. function:: Subspace.subspace(arr)

    |subspace-subspace-blurb|

.. function:: Subspace.get(item)

    Shorthand for subspace.subspace([item]). This function can be combined with the :func:`Subspace.asFoundationDBKey()` convenience to turn this::

        var s = new fdb.Subspace(['x']);
        tr.set(s.pack(['foo']), '');
        tr.set(s.pack(['foo', 'bar']), '');

    into this::

        var s = new fdb.Subspace(['x']);
        tr.set(s.get('foo'), '');
        tr.set(s.get('foo').get('bar'), '');

.. _api-node-directories:

Directories
===========

|directory-blurb1|

.. note:: For general guidance on directory usage, see the discussion in the :ref:`Developer Guide <developer-guide-directories>`.

|directory-blurb2|

|directory-blurb3|

.. attribute:: fdb.directory

    The default instance of :class:`DirectoryLayer`.

.. class:: fdb.DirectoryLayer(nodeSubspace=new fdb.Subspace([], fdb.buffer.fromByteLiteral('\\xFE')), contentSubspace=new fdb.Subspace(), allowManualPrefixes=false)

    |directory-layer-blurb|

.. function:: DirectoryLayer.createOrOpen(databaseOrTransaction, path[, options, callback])

    |directory-create-or-open-blurb|

    If the :ref:`value <dataTypes>` ``options.layer`` is specified and the directory is new, it is recorded as the layer for this directory; if ``layer`` is specified and the directory already exists, it is compared against the layer specified when the directory was created, and an error is raised if they differ.

    |callback| with the existing or newly created |directory-subspace|. |if-no-callback|

.. function:: DirectoryLayer.open(databaseOrTransaction, path[, options, callback])

    |directory-open-blurb|

    If the :ref:`value <dataTypes>` ``options.layer`` is specified, it is compared against the layer specified when the directory was created, and an error is raised if they differ.

    |callback| with the opened |directory-subspace|. |if-no-callback|

.. function:: DirectoryLayer.create(databaseOrTransaction, path[, options, callback])

    |directory-create-blurb|

    If the :ref:`key <dataTypes>` ``options.prefix`` is specified, the directory is created with the given physical prefix; otherwise a prefix is allocated automatically.

    If the :ref:`value <dataTypes>` ``options.layer`` is specified, it is recorded with the directory and will be checked by future calls to open.

    |callback| with the newly created |directory-subspace|. |if-no-callback|

.. function:: DirectoryLayer.move(tr, oldPath, newPath[, callback])

    |directory-move-blurb|

    |callback| with the |directory-subspace| at its new location. |if-no-callback|

.. function:: DirectoryLayer.remove(tr[, path, callback])

    |directory-remove-blurb|

    .. warning:: |directory-remove-warning|

    |callback-when| when the directory is removed. |if-no-callback-no-result|

.. function:: DirectoryLayer.removeIfExists(tr[, path, callback])

    |directory-remove-if-exists-blurb|

    .. warning:: |directory-remove-warning|

    |callback| with ``true`` if the directory existed prior to this call and ``false`` otherwise. |if-no-callback|

.. function:: DirectoryLayer.list(tr[, path, callback])

    Returns an array of names of the immediate subdirectories of the directory at ``path``. Each name is a unicode string representing the last component of a subdirectory's path.

    |callback| with the array of subdirectory names. |if-no-callback|

.. function:: DirectoryLayer.exists(tr[, path, callback])

    |directory-exists-blurb|

    |callback| with ``true`` if the directory exists and ``false`` if not. |if-no-callback|
    
.. function:: DirectoryLayer.getLayer()

    |directory-get-layer-blurb|

.. function:: DirectoryLayer.getPath()

    |directory-get-path-blurb|

.. _api-node-directory-subspace:

DirectorySubspace
-----------------

|directory-subspace-blurb|

.. function:: DirectorySubspace.moveTo(databaseOrTransaction, newAbsoluteNameOrPath[, callback])

    |directory-move-to-blurb|

    |callback| with the |directory-subspace| at its new location. |if-no-callback|

Locality information
====================

.. attribute:: fdb.locality

|locality-api-blurb|

.. function:: fdb.locality.getBoundaryKeys(databaseOrTransaction, begin, end, callback)
   
    |callback| with a :class:`LazyIterator` of :ref:`keys <dataTypes>` ``k`` such that ``begin <= k < end`` and ``k`` is located at the start of a contiguous range stored on a single server. |if-no-callback|
    
    |locality-get-boundary-keys-db-or-tr|

    |locality-get-boundary-keys-warning-danger|
    
.. function:: fdb.locality.getAddressesForKey(transaction, key, callback)

    |locality-get-addresses-for-key-blurb|

    |callback| with an array of network address strings. |if-no-callback|
