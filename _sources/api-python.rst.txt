.. default-domain:: py
.. highlight:: python
.. module:: fdb

.. Required substitutions for api-common.rst.inc

.. |database-type| replace:: ``Database``
.. |database-class| replace:: :class:`Database`
.. |database-auto| replace:: the :func:`@fdb.transactional <transactional>` decorator
.. |tenant-type| replace:: :class:`Tenant`
.. |transaction-class| replace:: :class:`Transaction`
.. |get-key-func| replace:: :func:`Transaction.get_key`
.. |get-range-func| replace:: :func:`Transaction.get_range`
.. |commit-func| replace:: :func:`Transaction.commit`
.. |reset-func-name| replace:: :func:`reset <Transaction.reset>`
.. |reset-func| replace:: :func:`Transaction.reset`
.. |cancel-func| replace:: :func:`Transaction.cancel`
.. |open-func| replace:: :func:`fdb.open`
.. |on-error-func| replace:: :meth:`Transaction.on_error`
.. |null-type| replace:: ``None``
.. |error-type| replace:: exception
.. |error-raise-type| replace:: raise
.. |future-cancel| replace:: :func:`Future.cancel`
.. |max-watches-database-option| replace:: :func:`Database.options.set_max_watches`
.. |retry-limit-database-option| replace:: :func:`Database.options.set_transaction_retry_limit`
.. |timeout-database-option| replace:: :func:`Database.options.set_transaction_timeout`
.. |max-retry-delay-database-option| replace:: :func:`Database.options.set_transaction_max_retry_delay`
.. |transaction-size-limit-database-option| replace:: :func:`Database.options.set_transaction_size_limit`
.. |causal-read-risky-database-option| replace:: :func:`Database.options.set_transaction_causal_read_risky`
.. |transaction-logging-max-field-length-database-option| replace:: :func:`Database.options.set_transaction_logging_max_field_length`
.. |snapshot-ryw-enable-database-option| replace:: :func:`Database.options.set_snapshot_ryw_enable`
.. |snapshot-ryw-disable-database-option| replace:: :func:`Database.options.set_snapshot_ryw_disable`
.. |future-type-string| replace:: a :ref:`future <api-python-future>`
.. |read-your-writes-disable-option| replace:: :func:`Transaction.options.set_read_your_writes_disable`
.. |retry-limit-transaction-option| replace:: :func:`Transaction.options.set_retry_limit`
.. |timeout-transaction-option| replace:: :func:`Transaction.options.set_timeout`
.. |max-retry-delay-transaction-option| replace:: :func:`Transaction.options.set_max_retry_delay`
.. |size-limit-transaction-option| replace:: :func:`Transaction.options.set_size_limit`
.. |snapshot-ryw-enable-transaction-option| replace:: :func:`Transaction.options.set_snapshot_ryw_enable`
.. |snapshot-ryw-disable-transaction-option| replace:: :func:`Transaction.options.set_snapshot_ryw_disable`
.. |causal-read-risky-transaction-option| replace:: :func:`Transaction.options.set_causal_read_risky`
.. |transaction-logging-max-field-length-transaction-option| replace:: :func:`Transaction.options.set_transaction_logging_max_field_length`
.. |lazy-iterator-object| replace:: generator
.. |key-meth| replace:: :meth:`Subspace.key`
.. |directory-subspace| replace:: :ref:`DirectorySubspace <api-python-directory-subspace>`
.. |directory-layer| replace:: :class:`DirectoryLayer`
.. |subspace| replace:: :class:`Subspace`
.. |subspace-api| replace:: :ref:`subspaces <api-python-subspaces>`
.. |as-foundationdb-key| replace:: :meth:`as_foundationdb_key`
.. |as-foundationdb-value| replace:: :meth:`as_foundationdb_value`
.. |tuple-layer| replace:: :ref:`tuple layer <api-python-tuple-layer>`
.. |dir-path-type| replace:: a tuple
.. |node-subspace| replace:: ``node_subspace``
.. |content-subspace| replace:: ``content_subspace``
.. |allow-manual-prefixes| replace:: ``allow_manual_prefixes``

.. include:: api-common.rst.inc

#######################
Python API
#######################

.. |block| replace:: :ref:`block <api-python-block>`
.. |future-type| replace:: (:ref:`future <api-python-future>`)
.. |future-object| replace:: :ref:`Future <api-python-future>` object
.. |infrequent| replace:: *Infrequently used*.
.. |slice-defaults| replace:: The default slice begin is ``''``; the default slice end is ``'\xFF'``.
.. |byte-string| replace:: In Python 2, a byte string is a string of type ``str``. In Python 3, a byte string has type ``bytes``.

Installation
============

The FoundationDB Python API is compatible with Python 2.7 - 3.7. You will need to have a Python version within this range on your system before the FoundationDB Python API can be installed. Also please note that Python 3.7 no longer bundles a full copy of libffi, which is used for building the _ctypes module on non-macOS UNIX platforms. Hence, if you are using Python 3.7, you should make sure libffi is already installed on your system.

On macOS, the FoundationDB Python API is installed as part of the FoundationDB installation (see :ref:`installing-client-binaries`). On Ubuntu or RHEL/CentOS, you will need to install the FoundationDB Python API manually via Python's package manager ``pip``:

.. code-block:: none

    user@host$ pip install foundationdb

You can also download the FoundationDB Python API source directly from :doc:`downloads`.

.. note:: The Python language binding is compatible with FoundationDB client binaries of version 2.0 or higher. When used with version 2.0.x client binaries, the API version must be set to 200 or lower.

After installation, the module ``fdb`` should be usable from your Python installation or path. (The system default python is always used by the client installer on macOS.)

.. _api-python-versioning:

API versioning
==============

When you import the ``fdb`` module, it exposes only one useful symbol:

.. function:: api_version(version)

    Specifies the version of the API that the application uses. This allows future versions of FoundationDB to make API changes without breaking existing programs.  The current version of the API is |api-version|.

.. note:: You must call ``fdb.api_version(...)`` before using any other part of the API.  Once you have done so, the rest of the API will become available in the ``fdb`` module. This requirement includes use of the ``@fdb.transactional`` decorator, which is called when your module is imported.

.. note:: |api-version-rationale|

.. warning:: |api-version-multi-version-warning|

For API changes between version 13 and |api-version| (for the purpose of porting older programs), see :ref:`release-notes` and :doc:`api-version-upgrade-guide`.

Opening a database
==================

After importing the ``fdb`` module and selecting an API version, you probably want to open a :class:`Database` using :func:`open`::

    import fdb
    fdb.api_version(730)
    db = fdb.open()

.. function:: open( cluster_file=None, event_model=None )

    |fdb-open-blurb1|

    |fdb-open-blurb2|

    .. param event_model:: Can be used to select alternate :ref:`api-python-event-models`

.. data:: options

    |network-options-blurb|

    .. note:: |network-options-warning|

    .. method :: fdb.options.set_knob(knob)
       
        |option-knob|

    .. method :: fdb.options.set_trace_enable( output_directory=None )

        |option-trace-enable-blurb|

        .. warning:: |option-trace-enable-warning|

    .. method :: fdb.options.set_trace_max_logs_size(bytes)

       |option-trace-max-logs-size-blurb|

    .. method :: fdb.options.set_trace_roll_size(bytes)

       |option-trace-roll-size-blurb|

    .. method :: fdb.options.set_trace_format(format)

       |option-trace-format-blurb|

    .. method :: fdb.options.set_trace_clock_source(source)

       |option-trace-clock-source-blurb|

    .. method :: fdb.options.set_disable_multi_version_client_api()

       |option-disable-multi-version-client-api|

    .. method :: fdb.options.set_callbacks_on_external_threads()

       |option-callbacks-on-external-threads|

    .. method :: fdb.options.set_external_client_library(path_to_lib)

       |option-external-client-library|

    .. method :: fdb.options.set_external_client_directory(path_to_lib_directory)

       |option-external-client-directory|

    .. note:: |tls-options-burb|

    .. method :: fdb.options.set_tls_plugin(plugin_path_or_name)

       |option-tls-plugin-blurb|

    .. method :: fdb.options.set_tls_cert_path(path_to_file)

       |option-tls-cert-path-blurb|

    .. method :: fdb.options.set_tls_key_path(path_to_file)

       |option-tls-key-path-blurb|

    .. method :: fdb.options.set_tls_verify_peers(criteria)

       |option-tls-verify-peers-blurb|

    .. method :: fdb.options.set_tls_cert_bytes(bytes)

       |option-tls-cert-bytes|

    .. method :: fdb.options.set_tls_key_bytes(bytes)

       |option-tls-key-bytes|
    
    .. method :: fdb.options.set_tls_ca_bytes(ca_bundle)

       |option-tls-ca-bytes|

    .. method :: fdb.options.set_tls_ca_path(path)

       |option-tls-ca-path|

    .. method :: fdb.options.set_tls_password(password)

       |option-tls-password|

    .. method :: fdb.options.set_disable_local_client()

      |option-set-disable-local-client|

    .. method :: fdb.options.set_client_threads_per_version(number)

       |option-set-client-threads-per-version|

    .. method :: fdb.options.set_disable_client_statistics_logging()

       |option-disable-client-statistics-logging|

    .. method :: fdb.options.set_enable_run_loop_profiling()
       
       |option-enable-run-loop-profiling|

    .. method :: fdb.options.set_distributed_client_tracer(tracer_type)
       
       |option-set-distributed-client-tracer|
    
    Please refer to fdboptions.py (generated) for a comprehensive list of options.

.. _api-python-keys:

Keys and values
===============

|keys-values-blurb| |byte-string|

|keys-values-other-types-blurb|

``as_foundationdb_key`` and ``as_foundationdb_value``
-----------------------------------------------------

|as-foundationdb-blurb|

.. warning :: |as-foundationdb-warning|

KeyValue objects
================

.. class :: KeyValue

    Represents a single key-value pair in the database.  This is a simple value type; mutating it won't affect your :class:`Transaction` or :class:`Database`.

    KeyValue supports the Python iterator protocol so that you can unpack a key and value directly into two variables::

        for key, value in tr[begin:end]:
            pass

Attributes
----------

.. attribute:: KeyValue.key
.. attribute:: KeyValue.value

Key selectors
=============

|keysel-blurb1|

|keysel-blurb2|

.. class :: KeySelector(key, or_equal, offset)

    Creates a key selector with the given reference key, equality flag, and offset. It is usually more convenient to obtain a key selector with one of the following methods:

.. classmethod:: KeySelector.last_less_than(key)

    Returns a key selector referencing the last (greatest) key in the database less than the specified key.

.. classmethod:: KeySelector.last_less_or_equal(key)

    Returns a key selector referencing the last (greatest) key less than, or equal to, the specified key.

.. classmethod:: KeySelector.first_greater_than(key)

    Returns a key selector referencing the first (least) key greater than the specified key.

.. classmethod:: KeySelector.first_greater_or_equal(key)

    Returns a key selector referencing the first key greater than, or equal to, the specified key.

``KeySelector + offset``
    Adding an integer ``offset`` to a ``KeySelector`` returns a key selector referencing a key ``offset`` keys after the original ``KeySelector``.  FoundationDB does not efficiently resolve key selectors with large offsets, so :ref:`dont-use-key-selectors-for-paging`.

``KeySelector - offset``
    Subtracting an integer ``offset`` from a ``KeySelector`` returns a key selector referencing a key ``offset`` keys before the original ``KeySelector``.  FoundationDB does not efficiently resolve key selectors with large offsets, so :ref:`dont-use-key-selectors-for-paging`.

Database objects
================

.. class:: Database

A |database-blurb1| |database-blurb2|

.. note:: |database-sync|

.. method:: Database.create_transaction()

    Returns a new :class:`Transaction` object.  Consider using the :func:`@fdb.transactional <transactional>` decorator to create transactions instead, since it will automatically provide you with appropriate retry behavior.

.. method:: Database.open_tenant(tenant_name)

    Opens an existing tenant to be used for running transactions and returns it as a :class`Tenant` object. 

    The tenant name can be either a byte string or a tuple. If a tuple is provided, the tuple will be packed using the tuple layer to generate the byte string tenant name.

    .. note :: Opening a tenant does not check its existence in the cluster. If the tenant does not exist, attempts to read or write data with it will fail.

.. |sync-read| replace:: This read is fully synchronous.
.. |sync-write| replace:: This change will be committed immediately, and is fully synchronous.

.. method:: Database.get(key)

    Returns the value associated with the specified key in the database (or ``None`` if the key does not exist). |sync-read|

``X = db[key]``
    Shorthand for ``X = db.get(key)``.

.. method:: Database.get_key(key_selector)

    Returns the key referenced by the specified :class:`KeySelector`. |sync-read|

    |database-get-key-caching-blurb|

.. method:: Database.get_range(begin, end[, limit, reverse, streaming_mode])

    Returns all keys ``k`` such that ``begin <= k < end`` and their associated values as a list of :class:`KeyValue` objects. Note the exclusion of ``end`` from the range. |sync-read|

    Each of ``begin`` and ``end`` may be a key or a :class:`KeySelector`. Note that in the case of a :class:`KeySelector`, the exclusion of ``end`` from the range still applies.

    If ``limit`` is specified, then only the first ``limit`` keys (and their values) in the range will be returned.

    If ``reverse`` is True, then the last ``limit`` keys in the range will be returned in reverse order. Reading ranges in reverse is supported natively by the database and should have minimal extra cost.

    If ``streaming_mode`` is specified, it must be a value from the :data:`StreamingMode` enumeration. It provides a hint to FoundationDB about how to retrieve the specified range. This option should generally not be specified, allowing FoundationDB to retrieve the full range very efficiently.

``X = db[begin:end]``
    Shorthand for ``X = db.get_range(begin, end)``. |slice-defaults|

``X = db[begin:end:-1]``
    Shorthand for ``X = db.get_range(begin, end, reverse=True)``. |slice-defaults|

.. method:: Database.get_range_startswith(prefix[, limit, reverse, streaming_mode])

    Returns all keys ``k`` such that ``k.startswith(prefix)``, and their associated values, as a list of :class:`KeyValue` objects. The ``limit``, ``reverse`` and ``streaming_mode`` parameters have the same meanings as in :meth:`Database.get_range()`.

.. method:: Database.set(key, value)

    Associates the given ``key`` and ``value``.  Overwrites any prior value associated with ``key``. |sync-write|

``db[key] = value``
    Shorthand for ``db.set(key, value)``.

.. method:: Database.clear(key)

    Removes the specified key (and any associated value), if it exists. |sync-write|

``del db[key]``
    Shorthand for ``db.clear(key)``.

.. method:: Database.clear_range(begin, end)

    Removes all keys ``k`` such that ``begin <= k < end``, and their associated values. |sync-write|

``del db[begin:end]``
    Shorthand for ``db.clear_range(begin, end)``. |slice-defaults|

.. method:: Database.clear_range_startswith(prefix)

    Removes all keys ``k`` such that ``k.startswith(prefix)``, and their associated values. |sync-write|

.. method:: Database.get_and_watch(key)

    Returns a tuple ``value, watch``, where ``value`` is the value associated with ``key`` or ``None`` if the key does not exist, and ``watch`` is a :class:`FutureVoid` that will become ready after ``value`` changes.

    See :meth:`Transaction.watch` for a general description of watches and their limitations.

.. method:: Database.set_and_watch(key, value)

    Sets ``key`` to ``value`` and returns a :class:`FutureVoid` that will become ready after a subsequent change to ``value``.

    See :meth:`Transaction.watch` for a general description of watches and their limitations.

.. method:: Database.clear_and_watch(key)

    Removes ``key`` (and any associated value) if it exists and returns a :class:`FutureVoid` that will become ready after the value is subsequently set.

    See :meth:`Transaction.watch` for a general description of watches and their limitations.

.. method:: Database.add(key, param)
.. method:: Database.bit_and(key, param)
.. method:: Database.bit_or(key, param)
.. method:: Database.bit_xor(key, param)

    These atomic operations behave exactly like the :ref:`associated operations <api-python-transaction-atomic-operations>` on :class:`Transaction` objects except that the change will immediately be committed, and is fully synchronous.

    .. note :: |database-atomic-ops-idempotency-note|

.. _api-python-database-options:

Database options
----------------

|database-options-blurb|

.. method:: Database.options.set_location_cache_size(size)

    |option-location-cache-size-blurb|

.. method:: Database.options.set_max_watches(max_watches)

    |option-max-watches-blurb|

.. method:: Database.options.set_machine_id(id)

    |option-machine-id-blurb|

.. method:: Database.options.set_datacenter_id(id)

    |option-datacenter-id-blurb|

.. method:: Database.options.set_transaction_timeout(timeout)

    |option-db-tr-timeout-blurb|

.. method:: Database.options.set_transaction_retry_limit(retry_limit)

    |option-db-tr-retry-limit-blurb|

.. method:: Database.options.set_transaction_max_retry_delay(delay_limit)

    |option-db-tr-max-retry-delay-blurb|

.. method:: Database.options.set_transaction_size_limit(size_limit)

    |option-db-tr-size-limit-blurb|

.. method:: Database.options.set_transaction_causal_read_risky()

    |option-db-causal-read-risky-blurb|

.. method:: Database.options.set_transaction_logging_max_field_length(size_limit)

    |option-db-tr-transaction-logging-max-field-length-blurb|

.. method:: Database.options.set_snapshot_ryw_enable()

    |option-db-snapshot-ryw-enable-blurb|

.. method:: Database.options.set_snapshot_ryw_disable()

    |option-db-snapshot-ryw-disable-blurb|
    
Tenant objects
==============

.. class:: Tenant

|tenant-blurb1|

.. method:: Tenant.create_transaction()

    Returns a new :class:`Transaction` object.  Consider using the :func:`@fdb.transactional <transactional>` decorator to create transactions instead, since it will automatically provide you with appropriate retry behavior.

.. _api-python-transactional-decorator:

Transactional decoration
========================

.. decorator:: transactional()

    The ``@fdb.transactional`` decorator is a convenience designed to concisely wrap a function with logic to automatically create a transaction and retry until success.

    For example::

        @fdb.transactional
        def simple_function(tr, x, y):
            tr[b'foo'] = x
            tr[b'bar'] = y

    The ``@fdb.transactional`` decorator makes ``simple_function`` a transactional function.  All functions using this decorator must have an argument **named** ``tr``.  This specially named argument is passed a transaction that the function can use to do reads and writes.

    A caller of a transactionally decorated function can pass a :class:`Database` or :class:`Tenant` instead of a transaction for the ``tr`` parameter.  Then a transaction will be created automatically, and automatically committed before returning to the caller.  The decorator will retry calling the decorated function until the transaction successfully commits.

    If ``db`` is a :class:`Database` or :class:`Tenant`, a call like ::

        simple_function(db, 'a', 'b')

    is equivalent to something like ::

        tr = db.create_transaction()
        while True:
            try:
                simple_function(tr, 'a', 'b')
                tr.commit().wait()
                break
            except fdb.FDBError as e:
                tr.on_error(e).wait()

    A caller may alternatively pass an actual transaction to the ``tr`` parameter.  In this case, the transactional function will not attempt to commit the transaction or to retry errors, since that is the responsibility of the caller who owns the transaction.  This design allows transactionally decorated functions to be composed freely into larger transactions.

    .. note :: |fdb-transactional-unknown-result-note|

Transaction objects
===================

.. class:: Transaction

A ``Transaction`` object represents a FoundationDB database transaction.  All operations on FoundationDB take place, explicitly or implicitly, through a ``Transaction``.

|transaction-blurb1|

|transaction-blurb2|

|transaction-blurb3|

The most convenient way to use Transactions is using the :func:`@fdb.transactional <transactional>` decorator.

Keys and values in FoundationDB are byte strings (``str`` in Python 2.x, ``bytes`` in 3.x).  To encode other data types, see the :mod:`fdb.tuple` module and :ref:`encoding-data-types`.

Attributes
----------

.. attribute:: Transaction.db

    |db-attribute-blurb|

Reading data
------------

.. method:: Transaction.get(key)

    Returns a |future-type| :class:`Value` associated with the specified key in the database.

    To check whether the specified key was present in the database, call :meth:`Value.present()` on the return value.

``X = tr[key]``
    Shorthand for ``X = tr.get(key)``.

.. method:: Transaction.get_key(key_selector)

    Returns the |future-type| :class:`Key` referenced by the specified :class:`KeySelector`.

    |transaction-get-key-caching-blurb|

.. method:: Transaction.get_range(begin, end[, limit, reverse, streaming_mode])

    Returns all keys ``k`` such that ``begin <= k < end`` and their associated values as an iterator yielding :class:`KeyValue` objects. Note the exclusion of ``end`` from the range.

    Like a |future-object|, the returned iterator issues asynchronous read operations. It fetches the data in one or more efficient batches (depending on the value of the ``streaming_mode`` parameter). However, the iterator will block if iteration reaches a value whose read has not yet completed.

    Each of ``begin`` and ``end`` may be a key or a :class:`KeySelector`. Note that in the case of a :class:`KeySelector`, the exclusion of ``end`` from the range still applies.

    If ``limit`` is specified, then only the first ``limit`` keys (and their values) in the range will be returned.

    If ``reverse`` is True, then the last ``limit`` keys in the range will be returned in reverse order. Reading ranges in reverse is supported natively by the database and should have minimal extra cost.

    If ``streaming_mode`` is specified, it must be a value from the :data:`StreamingMode` enumeration. It provides a hint to FoundationDB about how the returned container is likely to be used.  The default is :data:`StreamingMode.iterator`.

``X = tr[begin:end]``
    Shorthand for ``X = tr.get_range(begin, end)``. |slice-defaults|

``X = tr[begin:end:-1]``
    Shorthand for ``X = tr.get_range(begin, end, reverse=True)``. |slice-defaults|

.. method:: Transaction.get_range_startswith(prefix[, limit, reverse, streaming_mode])

    Returns all keys ``k`` such that ``k.startswith(prefix)``, and their associated values, as a container of :class:`KeyValue` objects (see :meth:`Transaction.get_range()` for a description of the returned container).

    The ``limit``, ``reverse`` and ``streaming_mode`` parameters have the same meanings as in :meth:`Transaction.get_range()`.

.. _api-python-snapshot-reads:

Snapshot reads
--------------

.. attribute:: Transaction.snapshot

    |snapshot-blurb1|

    |snapshot-blurb2|

    |snapshot-blurb3|

    |snapshot-blurb4|

.. attribute:: Transaction.snapshot.db

    |db-attribute-blurb|

.. method:: Transaction.snapshot.get(key)

    Like :meth:`Transaction.get`, but as a snapshot read.

``X = tr.snapshot[key]``
    Shorthand for ``X = tr.snapshot.get(key)``.

.. method:: Transaction.snapshot.get_key(key_selector) -> key

    Like :meth:`Transaction.get_key`, but as a snapshot read.

.. method:: Transaction.snapshot.get_range(begin, end[, limit, reverse, streaming_mode])

    Like :meth:`Transaction.get_range`, but as a snapshot read.

``X = tr.snapshot[begin:end]``
    Shorthand for ``X = tr.snapshot.get_range(begin, end)``. |slice-defaults|

``X = tr.snapshot[begin:end:-1]``
    Shorthand for ``X = tr.snapshot.get_range(begin, end, reverse=True)``. |slice-defaults|

.. method:: Transaction.snapshot.get_range_startswith(prefix[, limit, reverse, streaming_mode])

    Like :meth:`Transaction.get_range_startswith`, but as a snapshot read.

.. method:: Transaction.snapshot.get_read_version()

    Identical to :meth:`Transaction.get_read_version` (since snapshot and strictly serializable reads use the same read version).


Writing data
------------

.. |immediate-return| replace:: Returns immediately, having modified the snapshot represented by this :class:`Transaction`.

.. method:: Transaction.set(key,value)

    Associates the given ``key`` and ``value``.  Overwrites any prior value associated with ``key``. |immediate-return|

``tr[key] = value``
    Shorthand for ``tr.set(key,value)``.

.. method:: Transaction.clear(key)

    Removes the specified key (and any associated value), if it exists. |immediate-return|

``del tr[key]``
    Shorthand for ``tr.clear(key)``.

.. method:: Transaction.clear_range(begin, end)

    Removes all keys ``k`` such that ``begin <= k < end``, and their associated values. |immediate-return|

    |transaction-clear-range-blurb|

    .. note :: Unlike in the case of :meth:`get_range`, ``begin`` and ``end`` must be keys (byte strings), not :class:`KeySelector`\ s.  (Resolving arbitrary key selectors would prevent this method from returning immediately, introducing concurrency issues.)

``del tr[begin:end]``
    Shorthand for ``tr.clear_range(begin,end)``. |slice-defaults|

.. method:: Transaction.clear_range_startswith(prefix)

    Removes all the keys ``k`` such that ``k.startswith(prefix)``, and their associated values. |immediate-return|

    |transaction-clear-range-blurb|

.. _api-python-transaction-atomic-operations:

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
    import struct
    tr.add('key', struct.pack('<q', 1))

.. method:: Transaction.add(key, param)

    |atomic-add1|

    |atomic-add2|

.. method:: Transaction.bit_and(key, param)

    |atomic-and|

.. method:: Transaction.bit_or(key, param)

    |atomic-or|

.. method:: Transaction.bit_xor(key, param)

    |atomic-xor|

.. method:: Transaction.compare_and_clear(key, param)

    |atomic-compare-and-clear|

.. method:: Transaction.max(key, param)

    |atomic-max1|

    |atomic-max-min|

.. method:: Transaction.byte_max(key, param)

    |atomic-byte-max|

.. method:: Transaction.min(key, param)

    |atomic-min1|

    |atomic-max-min|

.. method:: Transaction.byte_min(key, param)

    |atomic-byte-min|

.. method:: Transaction.set_versionstamped_key(key, param)

    |atomic-set-versionstamped-key-1|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    .. warning :: |atomic-versionstamps-tuple-warning-key|

.. method:: Transaction.set_versionstamped_value(key, param)

    |atomic-set-versionstamped-value|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    .. warning :: |atomic-versionstamps-tuple-warning-value|

.. _api-python-committing:

Committing
----------

.. method :: Transaction.commit()

    Attempt to commit the changes made in the transaction to the database.  Returns a :class:`FutureVoid` representing the asynchronous result of the commit. You **must** call the :meth:`Future.wait()` method on the returned :class:`FutureVoid`, which will raise an exception if the commit failed.

    |commit-unknown-result-blurb|

    |commit-outstanding-reads-blurb|

    .. note :: Consider using the :func:`@fdb.transactional <transactional>` decorator, which not only calls :meth:`Database.create_transaction` or :meth`Tenant.create_transaction` and :meth:`Transaction.commit()` for you but also implements the required error handling and retry logic for transactions.

    .. warning :: |used-during-commit-blurb|

.. method :: Transaction.on_error(exception)

    Determine whether an exception raised by a :class:`Transaction` method is retryable. Returns a :class:`FutureVoid`. You **must** call the :meth:`Future.wait()` method on the :class:`FutureVoid`, which will return after a delay if the exception was retryable, or re-raise the exception if it was not.

    .. note :: Consider using the :func:`@fdb.transactional <transactional>` decorator, which calls this method for you.

.. method :: Transaction.reset()

    |transaction-reset-blurb|

.. _api-python-cancel:

.. method :: Transaction.cancel()

    |transaction-cancel-blurb|

    .. warning :: |transaction-reset-cancel-warning|

    .. warning :: |transaction-commit-cancel-warning|

.. _api-python-watches:

Watches
-------

.. method:: Transaction.watch(key)

    Creates a watch and returns a :class:`FutureVoid` that will become ready when the watch reports a change to the value of the specified key.

    |transaction-watch-blurb|

    |transaction-watch-committed-blurb|

    |transaction-watch-error-blurb|

    |transaction-watch-limit-blurb|

.. _api-python-conflict-ranges:

Conflict ranges
---------------

.. note:: |conflict-range-note|

|conflict-range-blurb|

.. method:: Transaction.add_read_conflict_range(begin, end)

    |add-read-conflict-range-blurb|

.. method:: Transaction.add_read_conflict_key(key)

    |add-read-conflict-key-blurb|

.. method:: Transaction.add_write_conflict_range(begin, end)

    |add-write-conflict-range-blurb|

.. method:: Transaction.add_write_conflict_key(key)

    |add-write-conflict-key-blurb|

Versions
--------

Most applications should use the read version that FoundationDB determines automatically during the transaction's first read, and ignore all of these methods.

.. method :: Transaction.set_read_version(version)

    |infrequent| Sets the database version that the transaction will read from the database.  The database cannot guarantee causal consistency if this method is used (the transaction's reads will be causally consistent only if the provided read version has that property).

.. method :: Transaction.get_read_version()

    |infrequent| Returns a :class:`FutureVersion` representing the transaction's |future-type| read version. You must call the :meth:`Future.wait()` method on the returned object to retrieve the version as an integer.

.. method :: Transaction.get_committed_version()

    |infrequent| |transaction-get-committed-version-blurb|

.. method :: Transaction.get_versionstamp()

    |infrequent| |transaction-get-versionstamp-blurb|

Transaction misc functions
--------------------------

.. method:: Transaction.get_estimated_range_size_bytes(begin_key, end_key)

    Gets the estimated byte size of the given key range. Returns a :class:`FutureInt64`.

    .. note:: The estimated size is calculated based on the sampling done by FDB server. The sampling algorithm works roughly in this way: the larger the key-value pair is, the more likely it would be sampled and the more accurate its sampled size would be. And due to that reason it is recommended to use this API to query against large ranges for accuracy considerations. For a rough reference, if the returned size is larger than 3MB, one can consider the size to be accurate.

.. method:: Transaction.get_range_split_points(self, begin_key, end_key, chunk_size)

    Gets a list of keys that can split the given range into (roughly) equally sized chunks based on ``chunk_size``. Returns a :class:`FutureKeyArray`.
    .. note:: The returned split points contain the start key and end key of the given range

.. method:: Transaction.get_approximate_size()

    |transaction-get-approximate-size-blurb| Returns a :class:`FutureInt64`.

.. _api-python-transaction-options:

Transaction options
-------------------

|transaction-options-blurb|

.. _api-python-snapshot-ryw:

.. method:: Transaction.options.set_snapshot_ryw_disable

    |option-snapshot-ryw-disable-blurb|

.. method:: Transaction.options.set_snapshot_ryw_enable

    |option-snapshot-ryw-enable-blurb|

.. method:: Transaction.options.set_priority_batch

    |option-priority-batch-blurb|

.. method:: Transaction.options.set_priority_system_immediate

    |option-priority-system-immediate-blurb|

    .. warning:: |option-priority-system-immediate-warning|

.. _api-python-option-set-causal-read-risky:

.. method:: Transaction.options.set_causal_read_risky

    |option-causal-read-risky-blurb|

.. method:: Transaction.options.set_causal_write_risky

    |option-causal-write-risky-blurb|

.. _api-python-no-write-conflict-range:

.. method:: Transaction.options.set_next_write_no_write_conflict_range

    |option-next-write-no-write-conflict-range-blurb|

    .. note:: |option-next-write-no-write-conflict-range-note|

.. method:: Transaction.options.set_read_your_writes_disable

    |option-read-your-writes-disable-blurb|

    .. note:: |option-read-your-writes-disable-note|

.. method:: Transaction.options.set_read_ahead_disable

    |option-read-ahead-disable-blurb|

.. method:: Transaction.options.set_access_system_keys

    |option-access-system-keys-blurb|

    .. warning:: |option-access-system-keys-warning|

.. method:: Transaction.options.set_read_system_keys

    |option-read-system-keys-blurb|

    .. warning:: |option-read-system-keys-warning|

.. method:: Transaction.options.set_retry_limit

    |option-set-retry-limit-blurb1|

    |option-set-retry-limit-blurb2|

.. method:: Transaction.options.set_max_retry_delay

    |option-set-max-retry-delay-blurb|

.. method:: Transaction.options.set_size_limit

    |option-set-size-limit-blurb|

.. _api-python-timeout:

.. method:: Transaction.options.set_timeout

    |option-set-timeout-blurb1|

    |option-set-timeout-blurb2|

    |option-set-timeout-blurb3|

.. method:: Transaction.options.set_transaction_logging_max_field_length(size_limit)

    |option-set-transaction-logging-max-field-length-blurb|

.. method:: Transaction.options.set_debug_transaction_identifier(id_string)

    |option-set-debug-transaction-identifier|

.. method:: Transaction.options.set_log_transaction()

    |option-set-log-transaction|

.. _api-python-future:

Future objects
==============

|future-blurb1|

.. _api-python-block:

When a future object "blocks", what actually happens is determined by the :ref:`event model <api-python-event-models>`.  A threaded program will block a thread, but a program using the gevent model will block a greenlet.

All future objects are a subclass of the :class:`Future` type.

.. class:: Future

.. method:: Future.wait()

    Blocks until the object is ready, and returns the object value (or raises an exception if the asynchronous function failed).

.. method:: Future.is_ready()

    Immediately returns true if the future object is ready, false otherwise.

.. method:: Future.block_until_ready()

    Blocks until the future object is ready.

.. method:: Future.on_ready(callback)

    Calls the specified callback function, passing itself as a single argument, when the future object is ready. If the future object is ready at the time :meth:`on_ready()` is called, the call may occur immediately in the current thread (although this behavior is not guaranteed). Otherwise, the call may be delayed and take place on the thread with which the client was initialized. Therefore, the callback is responsible for any needed thread synchronization (and/or for posting work to your application's event loop, thread pool, etc., as may be required by your application's architecture).

    .. note:: This function guarantees the callback will be executed **at most once**.

.. warning:: |fdb-careful-with-callbacks-blurb|

.. method:: Future.cancel()

    |future-cancel-blurb|

.. staticmethod:: Future.wait_for_any(*futures)

    Does not return until at least one of the given future objects is ready. Returns the index in the parameter list of a ready future object.

Asynchronous methods return one of the following subclasses of :class:`Future`:

.. class:: Value

    Represents a future string object and responds to the same methods as string in Python. They may be passed to FoundationDB methods that expect a string.

.. method:: Value.present()

    Returns ``False`` if the key used to request this value was not present in the database. For example::

        @fdb.transactional
        def foo(tr):
            val = tr[b'foo']
            if val.present():
                print 'Got value: %s' % val
            else:
                print 'foo was not present'

.. class:: Key

    Represents a future string object and responds to the same methods as string in Python. They may be passed to FoundationDB methods that expect a string.

.. class:: FutureInt64

    Represents a future integer. You must call the :meth:`Future.wait()` method on this object to retrieve the integer.

.. class:: FutureStringArray

    Represents a future list of strings. You must call the :meth:`Future.wait()` method on this object to retrieve the list of strings.

.. class:: FutureVoid

    Represents a future returned from asynchronous methods that logically have no return value.

    For a :class:`FutureVoid` object returned by :meth:`Transaction.commit()` or :meth:`Transaction.on_error()`, you must call the :meth:`Future.wait()` method, which will either raise an exception if an error occurred during the asynchronous call, or do nothing and return ``None``.

.. _streaming-mode-python:

Streaming modes
===============

.. data:: StreamingMode

|streaming-mode-blurb1|

|streaming-mode-blurb2|

The following streaming modes are available:

.. data:: StreamingMode.iterator

    *The default.*  The client doesn't know how much of the range it is likely to used and wants different performance concerns to be balanced.

    Only a small portion of data is transferred to the client initially (in order to minimize costs if the client doesn't read the entire range), and as the caller iterates over more items in the range larger batches will be transferred in order to maximize throughput.

.. data:: StreamingMode.want_all

    The client intends to consume the entire range and would like it all transferred as early as possible.

.. data:: StreamingMode.small

    |infrequent| Transfer data in batches small enough to not be much more expensive than reading individual rows, to minimize cost if iteration stops early.

.. data:: StreamingMode.medium

    |infrequent| Transfer data in batches sized in between small and large.

.. data:: StreamingMode.large

    |infrequent| Transfer data in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible.  If the client stops iteration early, some disk and network bandwidth may be wasted.  The batch size may still be too small to allow a single client to get high throughput from the database, so if that is what you need consider :data:`StreamingMode.serial`.

.. data:: StreamingMode.serial

    Transfer data in batches large enough that an individual client can get reasonable read bandwidth from the database.  If the client stops iteration early, considerable disk and network bandwidth may be wasted.

.. data:: StreamingMode.exact

    |infrequent| The client has passed a specific row limit and wants that many rows delivered in a single batch.  This is not particularly useful in Python because iterator functionality makes batches of data transparent, so use :data:`StreamingMode.want_all` instead.


.. _api-python-event-models:

Event models
============

By default, the FoundationDB Python API assumes that the calling program uses threads (as provided by the ``threading`` module) for concurrency.  This means that blocking operations will block the current Python thread.  This behavior can be changed by specifying the optional ``event_model`` parameter to the :func:`open` function.

The following event models are available:

``event_model=None``
    The default.  Blocking operations will block the current Python thread.  This is also fine for programs without any form of concurrency.

``event_model="gevent"``
    The calling program uses the `gevent <http://www.gevent.org/>`_ module for single-threaded concurrency. Blocking operations will block the current greenlet.

    The FoundationDB Python API has been tested with gevent versions 0.13.8 and 1.0rc2 and should work with all gevent 0.13 and 1.0 releases.

    .. note:: The ``gevent`` event model on Windows requires gevent 1.0 or newer.

``event_model="debug"``
    The calling program is threaded, but needs to be interruptible (by Ctrl-C).  Blocking operations will poll, effectively blocking the current thread but responding to keyboard interrupts.  This model is inefficient, but can be very useful for debugging.

.. _api-python-release-notes:

Errors
======

Errors in the FoundationDB API are raised as exceptions of type :class:`FDBError`. These errors may be displayed for diagnostic purposes, but generally should be passed to :meth:`Transaction.on_error`. When using :func:`@fdb.transactional <transactional>`, appropriate errors will be retried automatically.

.. class:: FDBError

.. attribute:: FDBError.code

    An integer associated with the error type.

.. attribute:: FDBError.description

    A somewhat human-readable description of the error.

.. warning:: You should use only :attr:`FDBError.code` for programmatic comparisons, as the description of the error may change at any time. Whenever possible, use the :meth:`Transaction.on_error()` method to handle :class:`FDBError` exceptions.

.. _api-python-tuple-layer:

Tuple layer
===========

.. module:: fdb.tuple

|tuple-layer-blurb|

.. note:: |tuple-layer-note|

The tuple layer in the FoundationDB Python API supports tuples that contain elements of the following data types:

+-----------------------+-------------------------------------------------------------------------------+
| Type                  | Legal Values                                                                  |
+=======================+===============================================================================+
| ``None``              | Any ``value`` such that ``value == None``                                     |
+-----------------------+-------------------------------------------------------------------------------+
| Byte string           | Any ``value`` such that ``isinstance(value, bytes)``                          |
+-----------------------+-------------------------------------------------------------------------------+
| Unicode string        | Any ``value`` such that ``isinstance(value, unicode)``                        |
+-----------------------+-------------------------------------------------------------------------------+
| Integer               | Python 2.7: Any ``value`` such that ``isinstance(value, (int,long))`` and     |
|                       | ``-2**2040+1 <= value <= 2**2040-1``. Python 3.x: Any ``value`` such that     |
|                       | ``isinstance(value, int)`` and ``-2**2040+1 <= value <= 2**2040-1``.          |
+-----------------------+-------------------------------------------------------------------------------+
| Floating point number | Any ``value`` such that ``isinstance(value, fdb.tuple.SingleFloat)`` or       |
| (single-precision)    | ``isinstance(value, ctypes.c_float)``                                         |
+-----------------------+-------------------------------------------------------------------------------+
| Floating point number | Any ``value`` such that ``isinstance(value, (ctypes.c_double, float))``       |
| (double-precision)    |                                                                               |
+-----------------------+-------------------------------------------------------------------------------+
| Boolean               | Any ``value`` such that ``isinstance(value, Boolean)``                        |
+-----------------------+-------------------------------------------------------------------------------+
| UUID                  | Any ``value`` such that ``isinstance(value, uuid.UUID)``                      |
+-----------------------+-------------------------------------------------------------------------------+
| Versionstamp          | Any ``value`` such that ``isinstance(value, fdb.tuple.Versionstamp)``         |
+-----------------------+-------------------------------------------------------------------------------+
| Tuple or List         | Any ``value`` such that ``isinstance(value, (tuple, list))`` and each element |
|                       | within ``value`` is one of the supported types with a legal value.            |
+-----------------------+-------------------------------------------------------------------------------+

If ``T`` is a Python tuple meeting these criteria, then::

    fdb.tuple.compare(T, fdb.tuple.unpack(fdb.tuple.pack(T))) == 0

That is, any tuple meeting these criteria will have the same semantic value if serialized and deserialized. For
the most part, this also implies that ``T == fdb.tuple.unpack(fdb.tuple.pack(T))`` with the following caveats:

- Any ``value`` of type ``ctypes.c_double`` is converted to the Python ``float`` type, but
  ``value.value == fdb.tuple.unpack(fdb.tuple.pack((value,)))[0]`` will be true (as long as ``value`` is not NaN).
- Any ``value`` of type ``ctypes.c_float`` is converted into a ``fdb.tuple.SingleFloat`` instance, but
  ``value.value == fdb.tuple.unpack(fdb.tuple.pack((value,)))[0].value`` will be true (as long as ``value.value`` is not NaN).
- Any ``value`` of type ``list`` or ``tuple`` is converted to a ``tuple`` type where the elements of the serialized and deserialized ``value``
  will be equal (subject to these caveats) to the elements of the original ``value``.

``import fdb.tuple``
    Imports the FoundationDB tuple layer.

.. method:: pack(tuple, prefix=b'')

    Returns a key (byte string) encoding the specified tuple. If ``prefix`` is set, it will prefix the serialized
    bytes with the prefix string. This throws an error if any of the tuple's items are incomplete :class:`Versionstamp`
    instances.

.. method:: pack_with_versionstamp(tuple, prefix=b'')

    Returns a key (byte string) encoding the specified tuple. This method will throw an error unless
    exactly one of the items of the tuple is an incomplete :class:`Versionstamp` instance. (It will
    recurse down nested tuples if there are any to find one.) If so, it will produce a byte string
    that can be fed into :meth:`fdb.Transaction.set_versionstamped_key` and correctly fill in the
    versionstamp information at commit time so that when the key is re-read and deserialized, the
    only difference is that the :class:`Versionstamp` instance is complete and has the transaction version
    filled in. This throws an error if there are no incomplete :class:`Versionstamp` instances in the tuple
    or if there is more than one.

.. method:: unpack(key)

    Returns the tuple encoded by the given key.

.. method:: has_incomplete_versionstamp(tuple)

    Returns ``True`` if there is at least one element contained within the tuple that is a
    :class:`Versionstamp` instance that is incomplete. If there are multiple incomplete
    :class:`Versionstamp` instances, this method will return ``True``, but trying to pack it into a
    byte string will result in an error.

.. method:: range(tuple)

    Returns a Python slice object representing all keys that encode tuples strictly starting with ``tuple`` (that is, all tuples of greater length than tuple of which tuple is a prefix).

    Can be used to directly index a Transaction object to retrieve a range.  For example::

        tr[ fdb.tuple.range(('A',2)) ]

    returns all key-value pairs in the database whose keys would unpack to tuples like ('A', 2, x), ('A', 2, x, y), etc.

.. method:: compare(tuple1, tuple2)

   Compares two tuples in a way that respects the natural ordering of the elements within the tuples. It will
   return -1 if ``tuple1`` would sort before ``tuple2`` when performing an element-wise comparison of the two
   tuples, it will return 1 if ``tuple1`` would sort after ``tuple2``, and it will return 0 if the two
   tuples are equivalent. If the function must compare two elements of different types while doing the comparison,
   it will sort the elements based on their internal type codes, so comparisons are consistent if not necessarily
   semantically meaningful. Strings are sorted on their byte representation when encoded into UTF-8 (which may
   differ from the default sort when non-ASCII characters are included within the string), and UUIDs are sorted
   based on their big-endian byte representation. Single-precision floating point numbers are sorted before all
   double-precision floating point numbers, and for floating point numbers, -NaN is sorted before -Infinity which
   is sorted before finite numbers which are sorted before Infinity which is sorted before NaN. Different representations
   of NaN are not treated as equal.

   Additionally, the tuple serialization contract is such that after they are serialized, the byte-string representations
   of ``tuple1`` and ``tuple2`` will sort in a manner that is consistent with this function. In particular, this function
   obeys the following contract::

       fdb.tuple.compare(tuple1, tuple2) == -1 if fdb.tuple.pack(tuple1) < fdb.tuple.pack(tuple2) else \
                                             0 if fdb.tuple.pack(tuple2) == fdb.tuple.pack(tuple2) else 1

   As byte order is the comparator used within the database, this comparator can be used to determine the order
   of keys within the database.

.. class:: SingleFloat(value)

   Wrapper around a single-precision floating point value. When constructed, the ``value`` parameter should either be
   an integral value, a ``float``, or a ``ctypes.c_float``. It will then properly store the value in its
   :attr:`SingleFloat.value` field (which should not be mutated). If the float does not fit within a IEEE 754 floating point
   integer, there may be a loss of precision.

.. attribute:: SingleFloat.value

   The underlying value of the ``SingleFloat`` object. This will have type ``float``.

.. method:: SingleFloat.__eq__(other)
.. method:: SingleFloat.__ne__(other)
.. method:: SingleFloat.__lt__(other)
.. method:: SingleFloat.__le__(other)
.. method:: SingleFloat.__gt__(other)
.. method:: SingleFloat.__ge__(other)

   Comparison functions for ``SingleFloat`` objects. This will sort according to the byte representation
   of the object rather than using standard float comparison. In particular, this means that ``-0.0 != 0.0``
   and that the ``NaN`` values will sort in a way that is consistent with the :meth:`compare` method between
   tuples rather than using standard floating-point comparison.

.. class:: Versionstamp(tr_version=None, user_version=0)

   Used to represent values written by versionstamp operations within the tuple layer. This
   wraps a single byte array of length 12 that can be used to represent some global order
   of items within the database. These versions are composed of two separate components:
   (1) the 10-byte ``tr_version`` and (2) the two-byte ``user_version``. The ``tr_version``
   is set by the database, and it is used to impose an order between different transactions.
   This order is guaranteed to be monotonically increasing over time for a given database.
   (In particular, it imposes an order that is consistent with a serialization order of
   the database's transactions.) If the client elects to leave the ``tr_version`` as its
   default value of ``None``, then the ``Versionstamp`` is considered "incomplete". This
   will cause the first 10 bytes of the serialized ``Versionstamp`` to be filled in with
   dummy bytes when serialized. When used with :meth:`fdb.Transaction.set_versionstamped_key`,
   an incomplete version can be used to ensure that a key gets written with the
   current transaction's version which can be useful for maintaining append-only data
   structures within the database. If the ``tr_version`` is set to something that is
   not ``None``, it should be set to a byte array of length 10. In this case, the
   ``Versionstamp`` is considered "complete". This is the usual case when one
   reads a serialized ``Versionstamp`` from the database.

   The ``user_version`` should be specified as an integer, but it must fit within a two-byte
   unsigned integer. It is set by the client, and it is used to impose an order between items
   serialized within a single transaction. If left unset, then final two bytes of the serialized
   ``Versionstamp`` are filled in with a default (constant) value.

   Sample usage of this class might be something like this::

        @fdb.transactional
        def write_versionstamp(tr, prefix):
            tr.set_versionstamped_key(fdb.tuple.pack_with_versionstamp((prefix, fdb.tuple.Versionstamp())), b'')
            return tr.get_versionstamp()

        @fdb.transactional
        def read_versionstamp(tr, prefix):
            subspace = fdb.Subspace((prefix,))
            for k, _ in tr.get_range(subspace.range().start, subspace.range().stop, 1):
                return subspace.unpack(k)[0]
            return None

        db = fdb.open()
        del db[fdb.tuple.range(('prefix',))]
        tr_version = write_versionstamp(db, 'prefix').wait()
        v = read_versionstamp(db, 'prefix')
        assert v == fdb.tuple.Versionstamp(tr_version=tr_version)

   Here, we serialize an incomplete ``Versionstamp`` and then write it using the ``set_versionstamped_key``
   mutation so that it picks up the transaction's version information. Then when we read it
   back, we get a complete ``Versionstamp`` with the committed transaction's version.

.. attribute:: Versionstamp.tr_version

   The inter-transaction component of the ``Versionstamp`` class. It should be either ``None`` (to
   indicate an incomplete ``Versionstamp`` that will set the version later) or to some 10 byte value
   indicating the commit version and batch version of some transaction.

.. attribute:: Versionstamp.user_version

   The intra-transaction component of the ``Versionstamp`` class. It should be some number that can
   fit within two bytes (i.e., between 0 and 65,535 inclusive). It can be used to impose an order
   between items that are committed together in the same transaction. If left unset, then
   the versionstamp is assigned a (constant) default user version value.

.. method:: Versionstamp.from_bytes(bytes)

   Static initializer for ``Versionstamp`` instances that takes a serialized ``Versionstamp`` and
   creates an instance of the class. The ``bytes`` parameter should be a byte string of length 12.
   This method will serialize the version as a "complete" ``Versionstamp`` unless the dummy bytes
   are equal to the default transaction version assigned to incomplete ``Versionstamps``.

.. method:: Versionstamp.is_complete()

   Returns whether this version has been given a (non-``None``) ``tr_version`` or not.

.. method:: Versionstamp.completed(tr_version)

   If this ``Versionstamp`` is incomplete, this returns a copy of this instance except that the
   ``tr_version`` is filled in with the passed parameter. If the ``Versionstamp`` is already
   complete, it will raise an error.

.. method:: Versionstamp.to_bytes()

   Produces a serialized byte string corresponding to this versionstamp. It will have length 12 and
   will combine the ``tr_version`` and ``user_version`` to produce a byte string that
   lexicographically sorts appropriately with other ``Versionstamp`` instances. If this instance is
   incomplete, then the ``tr_version`` component gets filled in with dummy bytes that will cause it
   to sort after every complete ``Verionstamp``'s serialized bytes.

.. method:: Versionstamp.__eq__(other)
.. method:: Versionstamp.__ne__(other)
.. method:: Versionstamp.__lt__(other)
.. method:: Versionstamp.__le__(other)
.. method:: Versionstamp.__gt__(other)
.. method:: Versionstamp.__ge__(other)

   Comparison functions for ``Versionstamp`` objects. For two complete ``Versionstamps``, the
   ordering is first lexicographically by ``tr_version`` and then by ``user_version``.
   Incomplete ``Versionstamps`` are defined to sort after all complete ``Versionstamps`` (the idea
   being that for a given transaction, if a ``Versionstamp`` has been created as the result of
   some prior transaction's work, then the incomplete ``Versionstamp``, when assigned a version,
   will be assigned a greater version than the existing one), and for two incomplete ``Versionstamps``,
   the order is by ``user_version`` only.

.. _api-python-subspaces:

Subspaces
=========

.. currentmodule:: fdb

|subspace-blurb1|

|subspace-blurb2|

.. note:: For general guidance on subspace usage, see the discussion in the :ref:`Developer Guide <developer-guide-sub-keyspaces>`.

.. class:: Subspace(prefixTuple=tuple(), rawPrefix="")

    |subspace-blurb3|

.. method:: Subspace.key()

    |subspace-key-blurb|

.. method:: Subspace.pack(tuple=tuple())

    |subspace-pack-blurb|

.. method:: Subspace.pack_with_versionstamp(tuple)

    Returns the key encoding the specified tuple in the subspace so that it may be used as the key in the
    :meth:`fdb.Transaction.set_versionstampe_key` method. The passed tuple must contain exactly one incomplete
    :class:`fdb.tuple.Versionstamp` instance or the method will raise an error. The behavior here is the same
    as if one used the :meth:`fdb.tuple.pack_with_versionstamp` method to appropriately pack together
    this subspace and the passed tuple.

.. method:: Subspace.unpack(key)

    |subspace-unpack-blurb|

.. method:: Subspace.range(tuple=tuple())

    |subspace-range-blurb|

    The range will be returned as a Python slice object, and may be used with any FoundationDB methods that require a range::

        r = subspace.range(('A', 2))
        rng_itr1 = tr[r]
        rng_itr2 = tr.get_range(r.start, r.stop, limit=1)

.. method:: Subspace.contains(key)

    |subspace-contains-blurb|

.. method:: Subspace.as_foundationdb_key()

    |subspace-as-foundationdb-key-blurb| This method serves to support the :ref:`as_foundationdb_key() <api-python-keys>` convenience interface.

.. method:: Subspace.subspace(tuple)

    |subspace-subspace-blurb|

    ``x = subspace[item]``

    Shorthand for ``x = subspace.subspace((item,))``. This function can be combined with the :meth:`Subspace.as_foundationdb_key()` convenience to turn this::

        s = fdb.Subspace(('x',))
        tr[s.pack(('foo', 'bar', 1))] = ''

    into this::

        s = fdb.Subspace(('x',))
        tr[s['foo']['bar'][1]] = ''

.. _api-python-directories:

Directories
===========

|directory-blurb1|

.. note:: For general guidance on directory usage, see the discussion in the :ref:`Developer Guide <developer-guide-directories>`.

|directory-blurb2|

|directory-blurb3|

.. data:: directory

    The default instance of :class:`DirectoryLayer`.

.. class:: DirectoryLayer(node_subspace=Subspace(rawPrefix="\xfe"), content_subspace=Subspace(), allow_manual_prefixes=False)

    |directory-layer-blurb|

.. method:: DirectoryLayer.create_or_open(tr, path, layer=None)

    |directory-create-or-open-blurb|

    If the byte string ``layer`` is specified and the directory is new, it is recorded as the layer; if ``layer`` is specified and the directory already exists, it is compared against the layer specified when the directory was created, and the method will |error-raise-type| an |error-type| if they differ.

    |directory-create-or-open-return-blurb|

.. method:: DirectoryLayer.open(tr, path, layer=None)

    |directory-open-blurb|

    If the byte string ``layer`` is specified, it is compared against the layer specified when the directory was created, and the method will |error-raise-type| an |error-type| if they differ.

    |directory-create-or-open-return-blurb|

.. method:: DirectoryLayer.create(tr, path, layer=None, prefix=None)

    |directory-create-blurb|

    If the byte string ``prefix`` is specified, the directory is created with the given physical prefix; otherwise a prefix is allocated automatically.

    If the byte string ``layer`` is specified, it is recorded with the directory and will be checked by future calls to open.

    |directory-create-or-open-return-blurb|

.. method:: DirectoryLayer.move(tr, old_path, new_path)

    |directory-move-blurb|

    |directory-move-return-blurb|

.. method:: DirectoryLayer.remove(tr, path)

    |directory-remove-blurb|

    .. warning:: |directory-remove-warning|

.. method:: DirectoryLayer.remove_if_exists(tr, path)

    |directory-remove-if-exists-blurb|

    .. warning:: |directory-remove-warning|

.. method:: DirectoryLayer.list(tr, path=())

    Returns a list of names of the immediate subdirectories of the directory at ``path``. Each name is a unicode string representing the last component of a subdirectory's path.

.. method:: DirectoryLayer.exists(tr, path)

    |directory-exists-blurb|

.. method:: DirectoryLayer.get_layer()

    |directory-get-layer-blurb|

.. method:: DirectoryLayer.get_path()

    |directory-get-path-blurb|

.. _api-python-directory-subspace:

DirectorySubspace
-----------------

|directory-subspace-blurb|

.. method:: DirectorySubspace.move_to(tr, new_path)

    |directory-move-to-blurb|

    |directory-move-return-blurb|

.. _api-python-locality:

Locality information
====================

.. module:: fdb.locality

|locality-api-blurb|

.. method:: fdb.locality.get_boundary_keys(db_or_tr, begin, end)

    |locality-get-boundary-keys-blurb|

    |locality-get-boundary-keys-db-or-tr|

    Like a |future-object|, the returned container issues asynchronous read operations to fetch the data in the range and may block while iterating over its values if the read has not completed.

    |locality-get-boundary-keys-warning-danger|

.. method:: fdb.locality.get_addresses_for_key(tr, key)

    Returns a :class:`fdb.FutureStringArray`. You must call the :meth:`fdb.Future.wait()` method on this object to retrieve a list of public network addresses as strings, one for each of the storage servers responsible for storing ``key`` and its associated value.

Tenant management
=================

.. module:: fdb.tenant_management

The FoundationDB API includes functions to manage the set of tenants in a cluster.

.. method:: fdb.tenant_management.create_tenant(db_or_tr, tenant_name)

    Creates a new tenant in the cluster.

    The tenant name can be either a byte string or a tuple and cannot start with the ``\xff`` byte. If a tuple is provided, the tuple will be packed using the tuple layer to generate the byte string tenant name.

    If a database is provided to this function for the ``db_or_tr`` parameter, then this function will first check if the tenant already exists. If it does, it will fail with a ``tenant_already_exists`` error. Otherwise, it will create a transaction and attempt to create the tenant in a retry loop. If the tenant is created concurrently by another transaction, this function may still return successfully.

    If a transaction is provided to this function for the ``db_or_tr`` parameter, then this function will not check if the tenant already exists. It is up to the user to perform that check if required. The user must also successfully commit the transaction in order for the creation to take effect.

.. method:: fdb.tenant_management.delete_tenant(db_or_tr, tenant_name)

    Delete a tenant from the cluster.

    The tenant name can be either a byte string or a tuple. If a tuple is provided, the tuple will be packed using the tuple layer to generate the byte string tenant name.

    It is an error to delete a tenant that still has data. To delete a non-empty tenant, first clear all of the keys in the tenant.

    If a database is provided to this function for the ``db_or_tr`` parameter, then this function will first check if the tenant already exists. If it does not, it will fail with a ``tenant_not_found`` error. Otherwise, it will create a transaction and attempt to delete the tenant in a retry loop. If the tenant is deleted concurrently by another transaction, this function may still return successfully.

    If a transaction is provided to this function for the ``db_or_tr`` parameter, then this function will not check if the tenant already exists. It is up to the user to perform that check if required. The user must also successfully commit the transaction in order for the deletion to take effect.
