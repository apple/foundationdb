.. default-domain:: rb
.. highlight:: ruby
.. module:: FDB

.. |database-type| replace:: ``Database``
.. |database-class| replace:: :class:`Database`
.. |database-auto| replace:: :meth:`Database.transact`
.. |tenant-type| replace:: FIXME
.. |transaction-class| replace:: :class:`Transaction`
.. |get-key-func| replace:: :meth:`Transaction.get_key`
.. |get-range-func| replace:: :meth:`Transaction.get_range`
.. |commit-func| replace:: :meth:`Transaction.commit`
.. |reset-func-name| replace:: :meth:`reset <Transaction.reset>`
.. |reset-func| replace:: :meth:`Transaction.reset`
.. |cancel-func| replace:: :meth:`Transaction.cancel`
.. |open-func| replace:: :func:`FDB.open`
.. |on-error-func| replace:: :meth:`Transaction.on_error`
.. |null-type| replace:: ``nil``
.. |error-type| replace:: exception
.. |error-raise-type| replace:: raise
.. |future-cancel| replace:: :meth:`Future.cancel`
.. |max-watches-database-option| replace:: :meth:`Database.options.set_max_watches`
.. |retry-limit-database-option| replace:: :meth:`Database.options.set_transaction_retry_limit`
.. |timeout-database-option| replace:: :meth:`Database.options.set_transaction_timeout`
.. |max-retry-delay-database-option| replace:: :meth:`Database.options.set_transaction_max_retry_delay`
.. |transaction-size-limit-database-option| replace:: :func:`Database.options.set_transaction_size_limit`
.. |causal-read-risky-database-option| replace:: :meth:`Database.options.set_transaction_causal_read_risky`
.. |snapshot-ryw-enable-database-option| replace:: :meth:`Database.options.set_snapshot_ryw_enable`
.. |snapshot-ryw-disable-database-option| replace:: :meth:`Database.options.set_snapshot_ryw_disable`
.. |transaction-logging-max-field-length-database-option| replace:: :meth:`Database.options.set_transaction_logging_max_field_length`
.. |future-type-string| replace:: a :class:`Future`
.. |read-your-writes-disable-option| replace:: :meth:`Transaction.options.set_read_your_writes_disable`
.. |retry-limit-transaction-option| replace:: :meth:`Transaction.options.set_retry_limit`
.. |timeout-transaction-option| replace:: :meth:`Transaction.options.set_timeout`
.. |max-retry-delay-transaction-option| replace:: :meth:`Transaction.options.set_max_retry_delay`
.. |size-limit-transaction-option| replace:: :meth:`Transaction.options.set_size_limit`
.. |snapshot-ryw-enable-transaction-option| replace:: :meth:`Transaction.options.set_snapshot_ryw_enable`
.. |snapshot-ryw-disable-transaction-option| replace:: :meth:`Transaction.options.set_snapshot_ryw_disable`
.. |causal-read-risky-transaction-option| replace:: :meth:`Transaction.options.set_causal_read_risky`
.. |transaction-logging-max-field-length-transaction-option| replace:: :meth:`Transaction.options.set_transaction_logging_max_field_length`
.. |lazy-iterator-object| replace:: :class:`Enumerator`
.. |key-meth| replace:: :meth:`Subspace.key`
.. |directory-subspace| replace:: :class:`DirectorySubspace`
.. |directory-layer| replace:: :class:`DirectoryLayer`
.. |subspace| replace:: :class:`Subspace`
.. |subspace-api| replace:: :ref:`subspaces <api-ruby-subspaces>`
.. |as-foundationdb-key| replace:: :meth:`as_foundationdb_key`
.. |as-foundationdb-value| replace:: :meth:`as_foundationdb_value`
.. |tuple-layer| replace:: :ref:`tuple layer <api-ruby-tuple-layer>`
.. |dir-path-type| replace:: an :class:`Enumerable`
.. |node-subspace| replace:: ``node_subspace``
.. |content-subspace| replace:: ``content_subspace``
.. |allow-manual-prefixes| replace:: ``allow_manual_prefixes``

.. include:: api-common.rst.inc

#####################
Ruby API
#####################

.. |future-type| replace:: (:ref:`future <api-ruby-future>`)
.. |future-object| replace:: :ref:`Future <api-ruby-future>` object
.. |infrequent| replace:: *Infrequently used*.
.. |streaming-mode| replace:: :ref:`streaming mode <ruby streaming mode>`

Installation
============

The FoundationDB Ruby API is distributed in our :doc:`downloads`.

.. note:: |separately-installed-bindings|
.. note:: |project-dependency|

API versioning
==============

When you require the ``FDB`` gem, it exposes only one useful method:

.. function:: api_version(version)

    Specifies the version of the API that the application uses. This allows future versions of FoundationDB to make API changes without breaking existing programs.  The current version of the API is |api-version|.

.. note:: You must call ``FDB.api_version(...)`` before using any other part of the API.  Once you have done so, the rest of the API will become available in the ``FDB`` module.

.. note:: |api-version-rationale|

.. warning:: |api-version-multi-version-warning|

For API changes between version 14 and |api-version| (for the purpose of porting older programs), see :ref:`release-notes` and :doc:`api-version-upgrade-guide`.

Opening a database
==================

After requiring the ``FDB`` gem and selecting an API version, you probably want to open a :class:`Database` using :func:`open`::

    require 'fdb'
    FDB.api_version 730
    db = FDB.open

.. function:: open( cluster_file=nil ) -> Database

    |fdb-open-blurb1|

    |fdb-open-blurb2|

.. global:: FDB.options

    |network-options-blurb|

    .. note:: |network-options-warning|

    .. method:: FDB.options.set_trace_enable(output_directory) -> nil

       |option-trace-enable-blurb|

       .. warning:: |option-trace-enable-warning|

    .. method:: FDB.options.set_trace_max_logs_size(bytes) -> nil

       |option-trace-max-logs-size-blurb|

    .. method:: FDB.options.set_trace_roll_size(bytes) -> nil

       |option-trace-roll-size-blurb|

    .. method:: FDB.options.set_trace_format(format) -> nil

       |option-trace-format-blurb|

    .. method:: FDB.options.set_trace_clock_source(source) -> nil

       |option-trace-clock-source-blurb|

    .. method:: FDB.options.set_disable_multi_version_client_api() -> nil

       |option-disable-multi-version-client-api|

    .. method :: FDB.options.set_callbacks_on_external_threads() -> nil

       |option-callbacks-on-external-threads|

    .. method :: FDB.options.set_external_client_library(path_to_lib) -> nil

       |option-external-client-library|

    .. method :: FDB.options.set_external_client_directory(path_to_lib_directory) -> nil

       |option-external-client-directory|

    .. note:: |tls-options-burb|

    .. method :: FDB.options.set_tls_plugin(plugin_path_or_name) -> nil

       |option-tls-plugin-blurb|

    .. method :: FDB.options.set_tls_cert_path(path_to_file) -> nil

       |option-tls-cert-path-blurb|

    .. method :: FDB.options.set_tls_key_path(path_to_file) -> nil

       |option-tls-key-path-blurb|

    .. method :: FDB.options.set_tls_verify_peers(criteria) -> nil

       |option-tls-verify-peers-blurb|

    .. method :: FDB.options.set_tls_cert_bytes(bytes) -> nil

       |option-tls-cert-bytes|

    .. method :: FDB.options.set_tls_key_bytes(bytes) -> nil

       |option-tls-key-bytes|

    .. method :: FDB.options.set_disable_multi_version_client_api() -> nil


.. _api-ruby-keys:

Keys and values
===============

|keys-values-blurb|

|keys-values-other-types-blurb|

``as_foundationdb_key`` and ``as_foundationdb_value``
-----------------------------------------------------

|as-foundationdb-blurb|

.. warning :: |as-foundationdb-warning|

KeyValue objects
================

.. class:: KeyValue

    Represents a single key-value pair in the database. This is a simple value type; mutating it won't affect your :class:`Transaction` or :class:`Database`.

    .. attr_reader:: key

    .. attr_reader:: value

Key selectors
=============

|keysel-blurb1|

|keysel-blurb2|

.. class:: KeySelector(key, or_equal, offset)

    Creates a key selector with the given reference key, equality flag, and offset. It is usually more convenient to obtain a key selector with one of the following methods:

    .. classmethod:: last_less_than(key) -> KeySelector

        Returns a key selector referencing the last (greatest) key in the database less than the specified key.

    .. classmethod:: KeySelector.last_less_or_equal(key) -> KeySelector

        Returns a key selector referencing the last (greatest) key less than, or equal to, the specified key.

    .. classmethod:: KeySelector.first_greater_than(key) -> KeySelector

        Returns a key selector referencing the first (least) key greater than the specified key.

    .. classmethod:: KeySelector.first_greater_or_equal(key) -> KeySelector

        Returns a key selector referencing the first key greater than, or equal to, the specified key.

.. method:: KeySelector.+(offset) -> KeySelector

    Adding an integer ``offset`` to a :class:`KeySelector` returns a new key selector referencing a key ``offset`` keys after the original :class:`KeySelector`.  FoundationDB does not efficiently resolve key selectors with large offsets, so :ref:`dont-use-key-selectors-for-paging`.

.. method:: KeySelector.-(offset) -> KeySelector

    Subtracting an integer ``offset`` from a :class:`KeySelector` returns a new key selector referencing a key ``offset`` keys before the original :class:`KeySelector`.  FoundationDB does not efficiently resolve key selectors with large offsets, so :ref:`dont-use-key-selectors-for-paging`.

Database objects
================

.. class:: Database

A |database-blurb1| |database-blurb2|

.. note:: |database-sync|

.. method:: Database.transact() {|tr| block }

    Executes the provided block with a new transaction, commits the transaction, and retries the block as necessary in response to retryable database errors such as transaction conflicts. This is the recommended way to do operations transactionally.

    This method, along with :meth:`Transaction.transact`, makes it easy to write a transactional functions which accept either a :class:`Database` or a :class:`Transaction` as a parameter. See :ref:`transact` for explanation and examples.

    .. note:: |fdb-transactional-unknown-result-note|

.. |sync-read| replace:: This read is fully synchronous.
.. |sync-write| replace:: This change will be committed immediately, and is fully synchronous.

.. method:: Database.create_transaction() -> Transaction

    Starts a new transaction on the database.  Consider using :meth:`Database.transact` instead, since it will automatically provide you with appropriate commit and retry behavior.

.. method:: Database.get(key) -> String or nil

    Returns the value associated with the specified key in the database (or ``nil`` if the key does not exist). |sync-read|

.. method:: Database.[](key) -> String or nil

    Alias of :meth:`Database.get`.

.. method:: Database.get_key(key_selector) -> String

    Returns the key referenced by the specified :class:`KeySelector`. |sync-read|

    |database-get-key-caching-blurb|

.. method:: Database.get_range(begin, end, options={}) -> Array

    Returns all keys ``k`` such that ``begin <= k < end`` and their associated values as an :class:`Array` of :class:`KeyValue` objects. Note the exclusion of ``end`` from the range. |sync-read|

    Each of ``begin`` and ``end`` may be a key (:class:`String` or :class:`Key`) or a :class:`KeySelector`. Note that in the case of a :class:`KeySelector`, the exclusion of ``end`` from the range still applies.

    The ``options`` hash accepts the following optional parameters:

    ``:limit``
        Only the first ``limit`` keys (and their values) in the range will be returned.

    ``:reverse``
        If ``true``, then the keys in the range will be returned in reverse order. Reading ranges in reverse is supported natively by the database and should have minimal extra cost.

        If ``:limit`` is also specified, the *last* ``limit`` keys in the range will be returned in reverse order.

    ``:streaming_mode``
        A valid |streaming-mode|, which provides a hint to FoundationDB about how to retrieve the specified range. This option should generally not be specified, allowing FoundationDB to retrieve the full range very efficiently.

.. method:: Database.get_range(begin, end, options={}) {|kv| block } -> nil

    If given a block, :meth:`Database.get_range` yields each :class:`KeyValue` in the range that would otherwise have been returned to ``block``.

.. method:: Database.get_range_start_with(prefix, options={}) -> Array

    Returns all keys ``k`` such that ``k.start_with? prefix``, and their associated values, as an :class:`Array` of :class:`KeyValue` objects. The ``options`` hash accepts the same values as :meth:`Database.get_range`. |sync-read|

.. method:: Database.get_range_start_with(prefix, options={}) {|kv| block } -> nil

    If given a block, :meth:`Database.get_range_start_with` yields each :class:`KeyValue` in the range that would otherwise have been returned to ``block``.

.. method:: Database.set(key, value) -> value

    Associates the given ``key`` and ``value``. Overwrites any prior value associated with ``key``. Returns the same value that was passed in. |sync-write|

.. method:: Database.[]=(key, value) -> value

    Alias of :meth:`Database.set`.

.. method:: Database.clear(key) -> nil

    Removes the specified ``key`` (and any associated value), if it exists. |sync-write|

.. method:: Database.clear_range(begin, end) -> nil

    Removes all keys ``k`` such that ``begin <= k < end``, and their associated values. |sync-write|

.. method:: Database.clear_range_start_with(prefix) -> nil

    Removes all keys ``k`` such that ``k.start_with? prefix``, and their associated values. |sync-write|

.. method:: Database.get_and_watch(key) -> [value, FutureNil]

    Returns an array ``[value, watch]``, where ``value`` is the value associated with ``key`` or ``nil`` if the key does not exist, and ``watch`` is a :class:`FutureNil` that will become ready after ``value`` changes.

    See :meth:`Transaction.watch` for a general description of watches and their limitations.

.. method:: Database.set_and_watch(key, value) -> FutureNil

    Sets ``key`` to ``value`` and returns a :class:`FutureNil` that will become ready after a subsequent change to ``value``.

    See :meth:`Transaction.watch` for a general description of watches and their limitations.

.. method:: Database.clear_and_watch(key) -> FutureNil

    Removes ``key`` (and any associated value) if it exists and returns a :class:`FutureNil` that will become ready after the value is subsequently set.

    See :meth:`Transaction.watch` for a general description of watches and their limitations.

.. method:: Database.add(key, param) -> nil
.. method:: Database.bit_and(key, param) -> nil
.. method:: Database.bit_or(key, param) -> nil
.. method:: Database.bit_xor(key, param) -> nil

    These atomic operations behave exactly like the :ref:`associated operations <api-ruby-transaction-atomic-operations>` on :class:`Transaction` objects except that the change will immediately be committed, and is fully synchronous.

    .. note :: |database-atomic-ops-idempotency-note|

Database options
----------------

|database-options-blurb|

.. method:: Database.options.set_location_cache_size(size) -> nil

    |option-location-cache-size-blurb|

.. method:: Database.options.set_max_watches(max_watches) -> nil

    |option-max-watches-blurb|

.. method:: Database.options.set_machine_id(id) -> nil

    |option-machine-id-blurb|

.. method:: Database.options.set_datacenter_id(id) -> nil

    |option-datacenter-id-blurb|

.. method:: Database.options.set_transaction_timeout(timeout) -> nil

    |option-db-tr-timeout-blurb|

.. method:: Database.options.set_transaction_retry_limit(retry_limit) -> nil

    |option-db-tr-retry-limit-blurb|

.. method:: Database.options.set_transaction_max_retry_delay(delay_limit) -> nil

    |option-db-tr-max-retry-delay-blurb|

.. method:: Database.options.set_transaction_size_limit(size_limit) -> nil

    |option-db-tr-size-limit-blurb|

.. method:: Database.options.set_transaction_causal_read_risky() -> nil

    |option-db-causal-read-risky-blurb|

.. method:: Database.options.set_transaction_logging_max_field_length(size_limit) -> nil

    |option-db-tr-transaction-logging-max-field-length-blurb|

.. method:: Database.options.set_snapshot_ryw_enable() -> nil

    |option-db-snapshot-ryw-enable-blurb|

.. method:: Database.options.set_snapshot_ryw_disable() -> nil

    |option-db-snapshot-ryw-disable-blurb|

Transaction objects
===================

.. class:: Transaction

A ``Transaction`` object represents a FoundationDB database transaction.  All operations on FoundationDB take place, explicitly or implicitly, through a ``Transaction``.

|transaction-blurb1|

|transaction-blurb2|

|transaction-blurb3|

The most convenient way to create and use transactions is using the :meth:`Database.transact` method.

Keys and values in FoundationDB are byte strings.  FoundationDB will accept Ruby strings with any encoding, but will always return strings with ``ASCII-8BIT`` encoding (also known as ``BINARY``).  To encode other data types, see the :mod:`FDB::Tuple` module and :ref:`encoding-data-types`.

Attributes
----------

.. global:: Transaction.db

    |db-attribute-blurb|

Reading data
------------

.. method:: Transaction.get(key) -> Value

    Returns the value associated with the specified key in the database (which may be ``nil`` if the key does not exist).

.. method:: Transaction.[](key)

    Alias of :meth:`Transaction.get`.

.. method:: Transaction.get_key(key_selector) -> Key

    Returns the key referenced by the specified :class:`KeySelector`.

    |transaction-get-key-caching-blurb|

.. method:: Transaction.get_range(begin, end, options={}) -> an_enumerable

    Returns all keys ``k`` such that ``begin <= k < end`` and their associated values as an enumerable of :class:`KeyValue` objects. Note the exclusion of ``end`` from the range.

    Like a |future-object|, the returned enumerable issues asynchronous read operations to fetch data in the range, and may block while enumerating its values if the read has not completed. Data will be fetched in one more more efficient batches (depending on the value of the ``:streaming_mode`` parameter).

    Each of ``begin`` and ``end`` may be a key (:class:`String` or :class:`Key`) or a :class:`KeySelector`. Note that in the case of a :class:`KeySelector`, the exclusion of ``end`` from the range still applies.

    The ``options`` hash accepts the following optional parameters:

    ``:limit``
        Only the first ``limit`` keys (and their values) in the range will be returned.

    ``:reverse``
        If ``true``, then the keys in the range will be returned in reverse order. Reading ranges in reverse is supported natively by the database and should have minimal extra cost.

        If ``:limit`` is also specified, the *last* ``limit`` keys in the range will be returned in reverse order.

    ``:streaming_mode``
        A valid |streaming-mode|, which provides a hint to FoundationDB about how the returned enumerable is likely to be used.  The default is ``:iterator``.

.. method:: Transaction.get_range(begin, end, options={}) {|kv| block } -> nil

    If given a block, :meth:`Transaction.get_range` yields each :class:`KeyValue` in the range that would otherwise have been returned to ``block``.

.. method:: Transaction.get_range_start_with(prefix, options={}) -> an_enumerable

    Returns all keys ``k`` such that ``k.start_with? prefix``, and their associated values, as an enumerable of :class:`KeyValue` objects (see :meth:`Transaction.get_range` for a description of the returned enumerable).

    The ``options`` hash accepts the same values as :meth:`Transaction.get_range`.

.. method:: Transaction.get_range_start_with(prefix, options={}) {|kv| block } -> nil

    If given a block, :meth:`Transaction.get_range_start_with` yields each :class:`KeyValue` in the range that would otherwise have been returned to ``block``.

Snapshot reads
--------------

.. global:: Transaction.snapshot

    |snapshot-blurb1|

    |snapshot-blurb2|

    |snapshot-blurb3|

    |snapshot-blurb4|

.. global:: Transaction.snapshot.db

    |db-attribute-blurb|

.. method:: Transaction.snapshot.get(key) -> Value

    Like :meth:`Transaction.get`, but as a snapshot read.

.. method:: Transaction.snapshot.[](key) -> Value

    Alias of :meth:`Transaction.snapshot.get`.

.. method:: Transaction.snapshot.get_key(key_selector) -> Key

    Like :meth:`Transaction.get_key`, but as a snapshot read.

.. method:: Transaction.snapshot.get_range(begin, end, options={}) -> an_enumerable

    Like :meth:`Transaction.get_range`, but as a snapshot read.

.. method:: Transaction.snapshot.get_range_start_with(prefix, options={}) -> an_enumerable

    Like :meth:`Transaction.get_range_start_with`, but as a snapshot read.

.. method:: Transaction.snapshot.get_read_version() -> Int64Future

    Identical to :meth:`Transaction.get_read_version` (since snapshot and strictly serializable reads use the same read version).

Writing data
------------

.. |immediate-return| replace:: Returns immediately, having modified the snapshot represented by this :class:`Transaction`.

.. method:: Transaction.set(key, value) -> nil

    Associates the given ``key`` and ``value``.  Overwrites any prior value associated with ``key``. |immediate-return|

.. method:: Transaction.[]=(key, value) -> nil

    Alias of :meth:`Transaction.set`.

    .. note:: Although the above method returns nil, assignments in Ruby evaluate to the value assigned, so the expression ``tr[key] = value`` will return ``value``.

.. method:: Transaction.clear(key) -> nil

    Removes the specified key (and any associated value), if it exists. |immediate-return|

.. method:: Transaction.clear_range(begin, end) -> nil

    Removes all keys ``k`` such that ``begin <= k < end``, and their associated values. |immediate-return|

    |transaction-clear-range-blurb|

    .. note:: Unlike in the case of :meth:`Transaction.get_range`, ``begin`` and ``end`` must be keys (:class:`String` or :class:`Key`), not :class:`KeySelector`\ s.  (Resolving arbitrary key selectors would prevent this method from returning immediately, introducing concurrency issues.)

.. method:: Transaction.clear_range_start_with(prefix) -> nil

    Removes all the keys ``k`` such that ``k.start_with? prefix``, and their associated values. |immediate-return|

    |transaction-clear-range-blurb|

.. _api-ruby-transaction-atomic-operations:

Atomic operations
-----------------

|atomic-ops-blurb1|

|atomic-ops-blurb2|

|atomic-ops-blurb3|

.. warning :: |atomic-ops-warning|

In each of the methods below, ``param`` should be a string appropriately packed to represent the desired value. For example::

    # wrong
    tr.add('key', 1)

    # right
    tr.add('key', [1].pack('q<'))

.. method:: Transaction.add(key, param) -> nil

    |atomic-add1|

    |atomic-add2|

.. method:: Transaction.bit_and(key, param) -> nil

    |atomic-and|

.. method:: Transaction.bit_or(key, param) -> nil

    |atomic-or|

.. method:: Transaction.bit_xor(key, param) -> nil

    |atomic-xor|

.. method:: Transaction.compare_and_clear(key, param) -> nil

    |atomic-compare-and-clear|

.. method:: Transaction.max(key, param) -> nil

    |atomic-max1|

    |atomic-max-min|

.. method:: Transaction.byte_max(key, param) -> nil

    |atomic-byte-max|

.. method:: Transaction.min(key, param) -> nil

    |atomic-min1|

    |atomic-max-min|

.. method:: Transaction.byte_min(key, param) -> nil

    |atomic-byte-min|

.. method:: Transaction.set_versionstamped_key(key, param) -> nil

    |atomic-set-versionstamped-key-1|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    .. warning :: |atomic-versionstamps-tuple-warning-key|

.. method:: Transaction.set_versionstamped_value(key, param) -> nil

    |atomic-set-versionstamped-value|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    .. warning :: |atomic-versionstamps-tuple-warning-value|

Committing
----------

.. method:: Transaction.transact() {|tr| block }

    Yields ``self`` to the given block.

    This method, along with :meth:`Database.transact`, makes it easy to write transactional functions which accept either a :class:`Database` or a :class:`Transaction` as a parameter. See :ref:`transact` for explanation and examples.

.. method:: Transaction.commit() -> FutureNil

    Attempt to commit the changes made in the transaction to the database. Returns a :class:`FutureNil`, representing the asynchronous result of the commit. You **must** call the :meth:`wait()` method on the returned :class:`FutureNil`, which will raise an exception if the commit failed.

    |commit-unknown-result-blurb|

    |commit-outstanding-reads-blurb|

    .. note:: Consider using :meth:`Database.transact`, which not only calls :meth:`Database.create_transaction` and :meth:`Transaction.commit` for you, but also implements the required error handling and retry logic for transactions.

    .. warning :: |used-during-commit-blurb|

.. method:: Transaction.on_error(exception) -> FutureNil

    Determine whether an exception raised by a :class:`Transaction` method is retryable. Returns a :class:`FutureNil`. You **must** call the :meth:`wait()` method on the :class:`FutureNil`, which will return after a delay if the exception was retryable, or re-raise the exception if it was not.

    .. note:: Consider using :meth:`Database.transact`, which calls this method for you.

.. method:: Transaction.reset() -> nil

    |transaction-reset-blurb|

.. method:: Transaction.cancel() -> nil

    |transaction-cancel-blurb|

    .. warning :: |transaction-reset-cancel-warning|

    .. warning :: |transaction-commit-cancel-warning|

Watches
-------

.. method:: Transaction.watch(key) -> FutureNil

    Creates a watch and returns a :class:`FutureNil` that will become ready when the watch reports a change to the value of the specified key.

    |transaction-watch-blurb|

    |transaction-watch-committed-blurb|

    |transaction-watch-error-blurb|

    |transaction-watch-limit-blurb|


Conflict ranges
---------------

.. note:: |conflict-range-note|

|conflict-range-blurb|

.. method:: Transaction.add_read_conflict_range(begin, end) -> nil

    |add-read-conflict-range-blurb|

.. method:: Transaction.add_read_conflict_key(key) -> nil

    |add-read-conflict-key-blurb|

.. method:: Transaction.add_write_conflict_range(begin, end) -> nil

    |add-write-conflict-range-blurb|

.. method:: Transaction.add_write_conflict_key(key) -> nil

    |add-write-conflict-key-blurb|

Versions
--------

Most applications should use the read version that FoundationDB determines automatically during the transaction's first read, and ignore all of these methods.

.. method:: Transaction.set_read_version(version) -> nil

    |infrequent| Sets the database version that the transaction will read from the database.  The database cannot guarantee causal consistency if this method is used (the transaction's reads will be causally consistent only if the provided read version has that property).

.. method:: Transaction.get_read_version() -> Int64Future

    |infrequent| Returns the transaction's read version.

.. method:: Transaction.get_committed_version() -> Integer

    |infrequent| |transaction-get-committed-version-blurb|

.. method:: Transaction.get_versionstamp() -> String

    |infrequent| |transaction-get-versionstamp-blurb|

Transaction misc functions
--------------------------

.. method:: Transaction.get_estimated_range_size_bytes(begin_key, end_key) -> Int64Future

    Gets the estimated byte size of the given key range. Returns a :class:`Int64Future`.

    .. note:: The estimated size is calculated based on the sampling done by FDB server. The sampling algorithm works roughly in this way: the larger the key-value pair is, the more likely it would be sampled and the more accurate its sampled size would be. And due to that reason it is recommended to use this API to query against large ranges for accuracy considerations. For a rough reference, if the returned size is larger than 3MB, one can consider the size to be accurate.

.. method:: Transaction.get_range_split_points(begin_key, end_key, chunk_size) -> FutureKeyArray

    Gets a list of keys that can split the given range into (roughly) equally sized chunks based on ``chunk_size``. Returns a :class:`FutureKeyArray`.
    .. note:: The returned split points contain the start key and end key of the given range

.. method:: Transaction.get_approximate_size() -> Int64Future

    |transaction-get-approximate-size-blurb| Returns a :class:`Int64Future`.

Transaction options
-------------------

|transaction-options-blurb|

.. _api-ruby-snapshot-ryw:

.. method:: Transaction.options.set_snapshot_ryw_disable() -> nil

    |option-snapshot-ryw-disable-blurb|

.. method:: Transaction.options.set_snapshot_ryw_enable() -> nil

    |option-snapshot-ryw-enable-blurb|

.. method:: Transaction.options.set_priority_batch() -> nil

    |option-priority-batch-blurb|

.. method:: Transaction.options.set_priority_system_immediate() -> nil

    |option-priority-system-immediate-blurb|

    .. warning:: |option-priority-system-immediate-warning|

.. method:: Transaction.options.set_causal_read_risky() -> nil

    |option-causal-read-risky-blurb|

.. method:: Transaction.options.set_causal_write_risky() -> nil

    |option-causal-write-risky-blurb|

.. method:: Transaction.options.set_next_write_no_write_conflict_range() -> nil

    |option-next-write-no-write-conflict-range-blurb|

    .. note:: |option-next-write-no-write-conflict-range-note|

.. method:: Transaction.options.set_read_your_writes_disable() -> nil

    |option-read-your-writes-disable-blurb|

    .. note:: |option-read-your-writes-disable-note|

.. method:: Transaction.options.set_read_ahead_disable() -> nil

    |option-read-ahead-disable-blurb|

.. method:: Transaction.options.set_access_system_keys() -> nil

    |option-access-system-keys-blurb|

    .. warning:: |option-access-system-keys-warning|

.. method:: Transaction.options.set_read_system_keys() -> nil

    |option-read-system-keys-blurb|

    .. warning:: |option-read-system-keys-warning|

.. method:: Transaction.options.set_retry_limit() -> nil

    |option-set-retry-limit-blurb1|

    |option-set-retry-limit-blurb2|

.. method:: Transaction.options.set_max_retry_delay() -> nil

    |option-set-max-retry-delay-blurb|

.. method:: Transaction.options.set_size_limit() -> nil

    |option-set-size-limit-blurb|

.. method:: Transaction.options.set_timeout() -> nil

    |option-set-timeout-blurb1|

    |option-set-timeout-blurb2|

    |option-set-timeout-blurb3|

.. method:: Transaction.options.set_transaction_logging_max_field_length(size_limit) -> nil

    |option-set-transaction-logging-max-field-length-blurb|

.. method:: Transaction.options.set_debug_transaction_identifier(id_string) -> nil

    |option-set-debug-transaction-identifier|

.. method:: Transaction.options.set_log_transaction() -> nil

    |option-set-log-transaction|

.. _transact:

The transact method
===================

When performing a database transaction, any read operation, as well as the commit itself, may fail with one of a number of errors. If the error is a retryable error, the transaction needs to be restarted from the beginning. Committing a transaction is also an asynchronous operation, and the returned :class:`FutureNil` object needs to be waited on to ensure that no errors occurred.

The methods :meth:`Database.transact` and :meth:`Transaction.transact` are convenient wrappers that allow much of this complexity to be handled automatically. A call like ::

    db.transact do |tr|
        tr['a'] = 'A'
        tr['b'] = 'B'
    end

is equivalent to ::

    tr = db.create_transaction
    committed = false
    while !committed
        begin
            tr['a'] = 'A'
            tr['b'] = 'B'
            tr.commit.wait
            committed = true
        rescue FDB::Error => e
            tr.on_error(e).wait
        end
    end

The first form is considerably easier to read, and ensures that the transaction is correctly committed (and retried, when necessary).

.. note:: Be careful when using control flow constructs within the block passed to :meth:`transact`. ``return`` or ``break`` will exit the retry loop *without committing* the transaction. Use ``next`` to exit the block and commit the transaction.

The :meth:`Transaction.transact` method, which logically does nothing, makes it easy to write functions that operate on either a :class:`Database` or :class:`Transaction`. Consider the following method::

    def increment(db_or_tr, key)
        db_or_tr.transact do |tr|
            tr[key] = (tr[key].to_i + 1).to_s
        end
    end

This method can be called with a :class:`Database`, and it will do its job atomically::

    increment(db, 'number')

It can also be called by another transactional method with a transaction::

    def increment_both(db_or_tr, key1, key2)
        db_or_tr.transact do |tr|
            increment(tr, key1)
            increment(tr, key2)
        end
    end

In the second case, ``increment`` will use provided transaction and will not commit it or retry errors, since that is the responsibility of its caller, ``increment_both``.

.. note:: |fdb-transactional-unknown-result-note|

.. _api-ruby-future:

Future objects
==============

|future-blurb1|

.. _api-ruby-block:

When a future object "blocks", the ruby thread is blocked, but the global interpreter lock is released.

When used in a conditional expression, a future object will evaluate to true, even if its value is nil. To test for nil, you must explicitly use the nil?() method::

    if tr['a'].nil?

All future objects are a subclass of the :class:`Future` type.

.. class:: Future

        .. method:: Future.ready?() -> bool

            Immediately returns true if the future object is ready, false otherwise.

        .. method:: Future.block_until_ready() -> nil

            Blocks until the future object is ready.

        .. method:: Future.on_ready() {|future| block } -> nil

            Yields ``self`` to the given block when the future object is ready. If the future object is ready at the time :meth:`on_ready` is called, the block may be called immediately in the current thread (although this behavior is not guaranteed). Otherwise, the call may be delayed and take place on the thread with which the client was initialized. Therefore, the block is responsible for any needed thread synchronization (and/or for posting work to your application's event loop, thread pool, etc., as may be required by your application's architecture).

            .. note:: This function guarantees the callback will be executed **at most once**.

            .. warning:: |fdb-careful-with-callbacks-blurb|

        .. method:: Future.cancel() -> nil

            |future-cancel-blurb|

        .. classmethod:: Future.wait_for_any(\*futures) -> Fixnum

            Does not return until at least one of the given future objects is ready. Returns the index in the parameter list of a ready future object.

Asynchronous methods return one of the following subclasses of :class:`Future`:

.. class:: Value

.. class:: Key

    Both types are future :class:`String` objects. Objects of these types respond to the same methods as objects of type :class:`String`, and may be passed to any method that expects a :class:`String`.

    An implementation quirk of :class:`Value` is that it will never evaluate to ``false``, even if its value is ``nil``. It is important to use ``if value.nil?`` rather than ``if ~value`` when checking to see if a key was not present in the database.

.. class:: Int64Future

    This type is a future :class:`Integer` object. Objects of this type respond to the same methods as objects of type :class:`Integer`, and may be passed to any method that expects a :class:`Integer`.

.. class:: FutureArray

    This type is a future :class:`Array` object. Objects of this type respond to the same methods as objects of type :class:`Array`, and may be passed to any method that expects a :class:`Array`.

.. class:: FutureNil

    This type is a future returned from asynchronous methods that logically have no return value.

    .. method:: FutureNil.wait() -> nil

        For a :class:`FutureNil` object returned by :meth:`Transaction.commit` or :meth:`Transaction.on_error`, you must call :meth:`FutureNil.wait`, which will return ``nil`` if the operation succeeds or raise an :exc:`FDB::Error` if an error occurred. Failure to call :meth:`FutureNil.wait` on a returned :class:`FutureNil` object means that any potential errors raised by the asynchronous operation that returned the object *will not be seen*, and represents a significant error in your code.


.. _ruby streaming mode:

Streaming modes
===============

|streaming-mode-blurb1|

|streaming-mode-blurb2|

The following streaming modes are available:

.. const:: :iterator

    *The default.*  The client doesn't know how much of the range it is likely to used and wants different performance concerns to be balanced.

    Only a small portion of data is transferred to the client initially (in order to minimize costs if the client doesn't read the entire range), and as the caller iterates over more items in the range larger batches will be transferred in order to maximize throughput.

.. const:: :want_all

    The client intends to consume the entire range and would like it all transferred as early as possible.

.. const:: :small

    |infrequent| Transfer data in batches small enough to not be much more expensive than reading individual rows, to minimize cost if iteration stops early.

.. const:: :medium

    |infrequent| Transfer data in batches sized in between ``:small`` and ``:large``.

.. const:: :large

    |infrequent| Transfer data in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible.  If the client stops iteration early, some disk and network bandwidth may be wasted.  The batch size may still be too small to allow a single client to get high throughput from the database, so if that is what you need consider ``:serial``.

.. const:: :serial

    Transfer data in batches large enough that an individual client can get reasonable read bandwidth from the database.  If the client stops iteration early, considerable disk and network bandwidth may be wasted.

.. const:: :exact

    |infrequent| The client has passed a specific row limit and wants that many rows delivered in a single batch.  This is not particularly useful in Ruby because enumerable functionality makes batches of data transparent, so use ``:want_all`` instead.

Errors
======

Errors in the FoundationDB API are raised as exceptions of type :class:`FDB::Error`. These errors may be displayed for diagnostic purposes, but generally should be passed to :meth:`Transaction.on_error`. When using :meth:`Database.transact`, appropriate errors will be retried automatically.

.. class:: Error

    .. attr_reader:: code

        A :class:`Fixnum` associated with the error type.

    .. method:: description() -> String

        Returns a somewhat human-readable description of the error.

.. warning:: You should only use the :attr:`code` attribute for programmatic comparisons, as the description of the error may change at any time. Whenever possible, use the :meth:`Transaction.on_error` method to handle :class:`FDB::Error` exceptions.

.. _api-ruby-tuple-layer:

Tuple layer
===========

.. module:: FDB::Tuple

|tuple-layer-blurb|

.. note:: |tuple-layer-note|

In the FoundationDB Ruby API, a tuple is an :class:`Enumerable` of elements of the following data types:

+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Type                 | Legal Values                                                                | Canonical Value                                                              |
+======================+=============================================================================+==============================================================================+
| Null value           | ``nil``                                                                     | ``nil``                                                                      |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Byte string          | Any value ``v`` where ``v.kind_of? String == true`` and ``v.encoding`` is   | ``String`` with encoding ``Encoding::ASCII_8BIT``                            |
|                      | either ``Encoding::ASCII_8BIT`` (aka ``Encoding::BINARY``) or               |                                                                              |
|                      | ``Encoding::US_ASCII`` (aka ``Encoding::ASCII``)                            |                                                                              |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Unicode string       | Any value ``v`` where ``v.kind_of? String == true`` and ``v.encoding`` is   | ``String`` with encoding ``Encoding::UTF_8``                                 |
|                      | ``Encoding::UTF_8``                                                         |                                                                              |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Integer              | Any value ``v`` where ``v.kind_of? Integer == true`` and                    | ``Integer``                                                                  |
|                      | ``-2**2040+1 <= v <= 2**2040-1``                                            |                                                                              |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Floating point number| Any value ``v`` where ``v.kind_of? FDB::Tuple::SingleFloat`` where          | :class:`FDB::Tuple::SingleFloat`                                             |
| (single-precision)   | ``v.value.kind_of? Float`` and ``v.value`` fits inside an IEEE 754 32-bit   |                                                                              |
|                      | floating-point number.                                                      |                                                                              |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Floating point number| Any value ``v`` where ``v.kind_of? Float``                                  | ``Float``                                                                    |
| (double-precision)   |                                                                             |                                                                              |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Boolean              | Any value ``v`` where ``v.kind_of? Boolean``                                | ``Boolean``                                                                  |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| UUID                 | Any value ``v`` where ``v.kind_of? FDB::Tuple::UUID`` where                 | :class:`FDB::Tuple::UUID`                                                    |
|                      | ``v.data.kind_of? String`` and ``v.data.encoding`` is ``Encoding::BINARY``  |                                                                              |
|                      | and ``v.data.length == 16``                                                 |                                                                              |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+
| Array                | Any value ``v`` such that ``v.kind_of? Array`` and each element within      | ``Array``                                                                    |
|                      | ``v`` is one of the supported types with a legal value.                     |                                                                              |
+----------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------------+

Note that as Ruby does not have native support for single-precision floating point values and UUIDs, tuple elements
of those types are returned instead as :class:`FDB::Tuple::SingleFloat` and :class:`FDB::Tuple::UUID` instances.
These are simple classes that just wrap their underlying values, and they are not intended to offer all of the methods
that a more fully-featured library for handling floating point values or UUIDs might offer. Most applications
should use their library of choice for handling these values and then convert to the appropriate tuple-type
when serializing for storage into the key-value store.

A single tuple element is ordered first by its type, and then by its value.

If ``T`` is an :class:`Enumerable` meeting these criteria, then conceptually::

    T == FDB::Tuple.unpack(FDB::Tuple.pack(T))

.. note:: Unpacking a tuple always returns an :class:`Array` of elements in a canonical representation, so packing and then unpacking a tuple may result in an equivalent but not identical representation.

.. function:: pack(tuple) -> String

    Returns a key encoding the given tuple.

.. function:: unpack(key) -> Array

    Returns the tuple encoded by the given key. Each element in the :class:`Array` will either be ``nil``, a :class:`String` (with encoding ``Encoding::ASCII_8BIT`` for byte strings or ``Encoding::UTF_8`` for unicode strings), or a :class:`Fixnum` or :class:`Bignum` for integers, depending on the magnitude.

.. function:: range(tuple) -> Array

    Returns the range containing all keys that encode tuples strictly starting with ``tuple`` (that is, all tuples of greater length than tuple of which tuple is a prefix).

    The range will be returned as an :class:`Array` of two elements, and may be used with any FoundationDB methods that require a range::

        r = FDB::Tuple.range(T)
        tr.get_range(r[0], r[1]) { |kv| ... }

.. class:: FDB::Tuple::SingleFloat(value)

    Wrapper around a single-precision floating point value. The ``value`` parameter should be a ``Float`` that
    can be encoded as an IEEE 754 floating point number. If the float does not fit within a IEEE 754 floating point
    integer, there may be a loss of precision.

.. attr_reader:: FDB::Tuple::SingleFloat.value

    The underlying value of the ``SingleFloat`` object. This should have type ``Float``.

.. function:: FDB::Tuple::SingleFloat.<=> -> Fixnum

    Comparison method for ``SingleFloat`` objects. This will compare the values based on their float value. This
    will sort the values in a manner consistent with the way items are sorted as keys in the database, which can
    be different from standard float comparison in that -0.0 is considered to be strictly less than 0.0 and NaN
    values are sorted based on their byte representation rather than being considered incomparable.

.. function:: FDB::Tuple::SingleFloat.to_s -> String

    Creates a string representation of the ``SingleFloat``. This will just return the string representation of the underlying
    value.

.. class:: FDB::Tuple::UUID(data)

    Wrapper around a 128-bit UUID. The ``data`` parameter should be a byte string of length 16 and is taken to
    be the big-endian byte representation of the UUID. If ``data`` is not of length 16, an |error-type| is thrown.

.. attr_reader:: FDB::Tuple::UUID.data

    The UUID data as a byte array of length 16. This is stored in big-endian order.

.. method:: FDB::Tuple::UUID.<=> -> Fixnum

    Comparison method for ``UUID`` objects. It will compare the UUID representations as unsigned byte arrays.
    This is the same order the database uses when comparing serialized UUIDs when they are used as part
    of a key.

.. method:: FDB::Tuple::UUID.to_s -> String

    Creates a string representation of the ``UUID``. This will be a string of length 32 containing the hex representation
    of the UUID bytes.

.. _api-ruby-subspaces:

Subspaces
=========

.. currentmodule:: FDB

|subspace-blurb1|

|subspace-blurb2|

.. note:: For general guidance on subspace usage, see the discussion in the :ref:`Developer Guide <developer-guide-sub-keyspaces>`.

.. class:: Subspace(prefix_tuple=[], raw_prefix='')

    |subspace-blurb3|

.. method:: Subspace.key() -> String

    |subspace-key-blurb|

.. method:: Subspace.pack(tuple) -> String

    |subspace-pack-blurb|

.. method:: Subspace.unpack(key) -> Array

    |subspace-unpack-blurb|

.. method:: Subspace.range(tuple=[]) -> Array

    |subspace-range-blurb|

    The range will be returned as an :class:`Array` of two elements, and may be used with any FoundationDB methods that require a range::

        r = subspace.range(['A', 2])
        tr.get_range(r[0], r[1]) { |kv| ... }

.. method:: Subspace.contains?(key) -> bool

    |subspace-contains-blurb|

.. method:: Subspace.as_foundationdb_key() -> String

    |subspace-as-foundationdb-key-blurb| This method serves to support the :ref:`as_foundationdb_key() <api-ruby-keys>` convenience interface.

.. method:: Subspace.subspace(tuple) -> Subspace

    |subspace-subspace-blurb|

.. method:: Subspace.[](item) -> Subspace

    Shorthand for Subspace.subspace([item]). This function can be combined with the :meth:`Subspace.as_foundationdb_key()` convenience to turn this::

        s = FDB::Subspace.new(['x'])
        tr[s.pack(['foo', 'bar', 1])] = ''

    into this::

        s = FDB::Subspace.new(['x'])
        tr[s['foo']['bar'][1]] = ''

.. _api-ruby-directories:

Directories
===========

|directory-blurb1|

.. note:: For general guidance on directory usage, see the discussion in the :ref:`Developer Guide <developer-guide-directories>`.

|directory-blurb2|

|directory-blurb3|

.. global:: FDB::directory

    The default instance of :class:`DirectoryLayer`.

.. class:: DirectoryLayer(node_subspace=Subspace.new([], "\\xFE"), content_subspace=Subspace.new, allow_manual_prefixes=false)

    |directory-layer-blurb|

.. method:: DirectoryLayer.create_or_open(db_or_tr, path, options={}) -> DirectorySubspace

    |directory-create-or-open-blurb|

    If the byte string ``:layer`` is specified in ``options`` and the directory is new, it is recorded as the layer; if ``:layer`` is specified and the directory already exists, it is compared against the layer specified when the directory was created, and the method will |error-raise-type| an |error-type| if they differ.

    |directory-create-or-open-return-blurb|

.. method:: DirectoryLayer.open(db_or_tr, path, options={}) -> DirectorySubspace

    |directory-open-blurb|

    If the byte string ``:layer`` is specified in ``options``, it is compared against the layer specified when the directory was created, and the method will |error-raise-type| an |error-type| if they differ.

    |directory-create-or-open-return-blurb|

.. method:: DirectoryLayer.create(db_or_tr, path, options={}) -> DirectorySubspace

    |directory-create-blurb|

    If the byte string ``:prefix`` is specified in ``options``, the directory is created with the given physical prefix; otherwise a prefix is allocated automatically.

    If the byte string ``:layer`` is specified in ``options``, it is recorded with the directory and will be checked by future calls to open.

    |directory-create-or-open-return-blurb|

.. method:: DirectoryLayer.move(db_or_tr, old_path, new_path) -> DirectorySubspace

    |directory-move-blurb|

    |directory-move-return-blurb|

.. method:: DirectoryLayer.remove(db_or_tr, path) -> bool

    |directory-remove-blurb|

    .. warning:: |directory-remove-warning|

.. method:: DirectoryLayer.remove_if_exists(db_or_tr, path) -> bool

    |directory-remove-if-exists-blurb|

    .. warning:: |directory-remove-warning|

.. method:: DirectoryLayer.list(db_or_tr, path=[]) -> Enumerable

    Returns an :class:`Enumerable` of names of the immediate subdirectories of the directory at ``path``. Each name is a unicode string representing the last component of a subdirectory's path.

.. method:: DirectoryLayer.exists?(db_or_tr, path) -> bool

    |directory-exists-blurb|

.. method:: DirectoryLayer.layer() -> String

    |directory-get-layer-blurb|

.. method:: DirectoryLayer.path() -> Enumerable

    |directory-get-path-blurb|

.. _api-ruby-directory-subspace:

DirectorySubspace
-----------------

.. class:: DirectorySubspace

    |directory-subspace-blurb|

.. method:: DirectorySubspace.move_to(db_or_tr, new_absolute_path) -> DirectorySubspace

    |directory-move-to-blurb|

    |directory-move-return-blurb|

Locality information
====================

.. module:: FDB::Locality

|locality-api-blurb|

.. function:: get_boundary_keys(db_or_tr, begin, end) -> Enumerator

    |locality-get-boundary-keys-blurb|

    |locality-get-boundary-keys-db-or-tr|

    Like a |future-object|, the returned :class:`Enumerator` issues asynchronous read operations to fetch data in the range, and may block while enumerating its values if the read has not completed.

    |locality-get-boundary-keys-warning-danger|

.. function:: get_addresses_for_key(tr, key) -> Array

    |locality-get-addresses-for-key-blurb|
