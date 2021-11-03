.. _special-keys:

============
Special Keys
============

Keys starting with the bytes ``\xff\xff`` are called "special" keys, and they are materialized when read. :doc:`\\xff\\xff/status/json <mr-status>` is an example of a special key.
As of api version 630, additional features have been exposed as special keys and are available to read as ranges instead of just individual keys. Additionally, the special keys are now organized into "modules".

Modules
=======

A module is loosely defined as a key range in the special key space where a user can expect similar behavior from reading any key in that range.
By default, users will see a ``special_keys_no_module_found`` error if they read from a range not contained in a module.
The error indicates the read would always return an empty set of keys if it proceeded. This could be caused by typo in the keys to read.
Users will also (by default) see a ``special_keys_cross_module_read`` error if their read spans a module boundary.
The error is to save the user from the surprise of seeing the behavior of multiple modules in the same read.
Users may opt out of these restrictions by setting the ``special_key_space_relaxed`` transaction option.

Each special key that existed before api version 630 is its own module. These are

#. ``\xff\xff/cluster_file_path`` See :ref:`cluster file client access <cluster-file-client-access>`
#. ``\xff\xff/status/json`` See :doc:`Machine-readable status <mr-status>`

Prior to api version 630, it was also possible to read a range starting at
``\xff\xff/worker_interfaces``. This is mostly an implementation detail of fdbcli,
but it's available in api version 630 as a module with prefix ``\xff\xff/worker_interfaces/``.

Api version 630 includes two new modules with prefixes
``\xff\xff/transaction/`` (information about the current transaction), and
``\xff\xff/metrics/`` (various metrics, not transactional).

Transaction module
------------------

Reads from the transaction module generally do not require an rpc and only inspect in-memory state for the current transaction.

There are three sets of keys exposed by the transaction module, and each set uses the same encoding, so let's first describe that encoding.

Let's say we have a set of keys represented as intervals of the form ``begin1 <= k < end1 && begin2 <= k < end2 && ...``.
It could be the case that some of the intervals overlap, e.g. if ``begin1 <= begin2 < end1``, or are adjacent, e.g. if ``end1 == begin2``.
If we merge all overlapping/adjacent intervals then sort, we end up with a canonical representation of this set of keys.

We encode this canonical set as ordered key value pairs like this::

  <namespace><begin1> -> "1"
  <namespace><end1> -> "0"
  <namespace><begin2> -> "1"
  <namespace><end2> -> "0"
  ...

Python example::

  >>> tr = db.create_transaction()
  >>> tr.add_read_conflict_key('foo')
  >>> tr.add_read_conflict_range('bar/', 'bar0')
  >>> for k, v in tr.get_range_startswith('\xff\xff/transaction/read_conflict_range/'):
  ...     print(k, v)
  ...
  ('\xff\xff/transaction/read_conflict_range/bar/', '1')
  ('\xff\xff/transaction/read_conflict_range/bar0', '0')
  ('\xff\xff/transaction/read_conflict_range/foo', '1')
  ('\xff\xff/transaction/read_conflict_range/foo\x00', '0')

For read-your-writes transactions, this canonical encoding of conflict ranges
is already available in memory, and so requesting small ranges is
correspondingly cheaper than large ranges.

For transactions with read-your-writes disabled, this canonical encoding is computed on
every read, so you're paying the full cost in CPU time whether or not you
request a small range.

The namespaces for sets of keys are

#. ``\xff\xff/transaction/read_conflict_range/`` This is the set of keys that will be used for read conflict detection. If another transaction writes to any of these keys after this transaction's read version, then this transaction won't commit.
#. ``\xff\xff/transaction/write_conflict_range/`` This is the set of keys that will be used for write conflict detection. Keys in this range may cause other transactions which read these keys to abort if this transaction commits.
#. ``\xff\xff/transaction/conflicting_keys/`` If this transaction failed due to a conflict, it must be the case that some transaction attempted [#conflicting_keys]_ to commit with a write conflict range that intersects this transaction's read conflict range. This is the subset of your read conflict range that actually intersected a write conflict from another transaction.

Caveats
~~~~~~~

#. ``\xff\xff/transaction/read_conflict_range/`` The conflict range for a read is sometimes not known until that read completes (e.g. range reads with limits, key selectors). When you read from these special keys, the returned future first blocks until all pending reads are complete so it can give an accurate response.
#. ``\xff\xff/transaction/write_conflict_range/`` The conflict range range for a ``set_versionstamped_key`` atomic op is not known until commit time. You'll get an approximate range (the actual range will be a subset of the approximate range) until the precise range is known.
#. ``\xff\xff/transaction/conflicting_keys/`` Since using this feature costs server (i.e., proxy and resolver) resources, it's disabled by default. You must opt in by setting the ``report_conflicting_keys`` transaction option.

Metrics module
--------------

Reads in the metrics module are not transactional and may require rpcs to complete.

``\xff\xff/metrics/data_distribution_stats/<begin>`` represent stats about the shard that begins at ``<begin>``

  >>> for k, v in db.get_range_startswith('\xff\xff/metrics/data_distribution_stats/', limit=3):
  ...     print(k, v)
  ...
  ('\xff\xff/metrics/data_distribution_stats/', '{"shard_bytes":3828000}')
  ('\xff\xff/metrics/data_distribution_stats/mako00079', '{"shard_bytes":2013000}')
  ('\xff\xff/metrics/data_distribution_stats/mako00126', '{"shard_bytes":3201000}')

========================= ======== ===============
**Field**                 **Type** **Description**
------------------------- -------- ---------------
shard_bytes               number   An estimate of the sum of kv sizes for this shard.
========================= ======== ===============

Keys starting with ``\xff\xff/metrics/health/`` represent stats about the health of the cluster, suitable for application-level throttling.
Some of this information is also available in ``\xff\xff/status/json``, but these keys are significantly cheaper (in terms of server resources) to read.

  >>> for k, v in db.get_range_startswith('\xff\xff/metrics/health/'):
  ...     print(k, v)
  ...
  ('\xff\xff/metrics/health/aggregate', '{"batch_limited":false,"tps_limit":483988.66315011407,"worst_storage_durability_lag":5000001,"worst_storage_queue":2036,"worst_log_queue":300}')
  ('\xff\xff/metrics/health/log/e639a9ad0373367784cc550c615c469b', '{"log_queue":300}')
  ('\xff\xff/metrics/health/storage/ab2ce4caf743c9c1ae57063629c6678a', '{"cpu_usage":2.398696781487125,"disk_usage":0.059995917598039405,"storage_durability_lag":5000001,"storage_queue":2036}')

``\xff\xff/metrics/health/aggregate``

Aggregate stats about cluster health. Reading this key alone is slightly cheaper than reading any of the per-process keys.

============================ ======== ===============
**Field**                    **Type** **Description**
---------------------------- -------- ---------------
batch_limited                boolean  Whether or not the cluster is limiting batch priority transactions
tps_limit                    number   The rate at which normal priority transactions are allowed to start
worst_storage_durability_lag number   See the description for storage_durability_lag
worst_storage_queue          number   See the description for storage_queue
worst_log_queue              number   See the description for log_queue
============================ ======== ===============

``\xff\xff/metrics/health/log/<id>``

Stats about the health of a particular transaction log process

========================= ======== ===============
**Field**                 **Type** **Description**
------------------------- -------- ---------------
log_queue                 number   The number of bytes of mutations that need to be stored in memory on this transaction log process
========================= ======== ===============

``\xff\xff/metrics/health/storage/<id>``

Stats about the health of a particular storage process

========================== ======== ===============
**Field**                  **Type** **Description**
-------------------------- -------- ---------------
cpu_usage                  number   The cpu percentage used by this storage process
disk_usage                 number   The disk IO percentage used by this storage process
storage_durability_lag     number   The difference between the newest version and the durable version on this storage process. On a lightly loaded cluster this will stay just above 5000000 [#max_read_transaction_life_versions]_.
storage_queue              number   The number of bytes of mutations that need to be stored in memory on this storage process
========================== ======== ===============

Caveats
~~~~~~~

#. ``\xff\xff/metrics/health/`` These keys may return data that's several seconds old, and the data may not be available for a brief period during recovery. This will be indicated by the keys being absent.

.. [#conflicting_keys] In practice, the transaction probably committed successfully. However, if you're running multiple resolvers then it's possible for a transaction to cause another to abort even if it doesn't commit successfully.
.. [#max_read_transaction_life_versions] The number 5000000 comes from the server knob MAX_READ_TRANSACTION_LIFE_VERSIONS
