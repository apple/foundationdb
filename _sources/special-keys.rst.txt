.. _special-keys:

============
Special Keys
============

Keys starting with the bytes ``\xff\xff`` are called "special" keys, and they are materialized when read. :doc:`\\xff\\xff/status/json <mr-status>` is an example of a special key.
As of api version 630, additional features have been exposed as special keys and are available to read as ranges instead of just individual keys. Additionally, the special keys are now organized into "modules".

Read-only modules
=================

A module is loosely defined as a key range in the special key space where a user can expect similar behavior from reading any key in that range.
By default, users will see a ``special_keys_no_module_found`` error if they read from a range not contained in a module.
The error indicates the read would always return an empty set of keys if it proceeded. This could be caused by typo in the keys to read.
Users will also (by default) see a ``special_keys_cross_module_read`` error if their read spans a module boundary.
The error is to save the user from the surprise of seeing the behavior of multiple modules in the same read.
Users may opt out of these restrictions by setting the ``special_key_space_relaxed`` transaction option.

Each special key that existed before api version 630 is its own module. These are:

#. ``\xff\xff/cluster_file_path`` - See :ref:`cluster file client access <cluster-file-client-access>`
#. ``\xff\xff/status/json`` - See :doc:`Machine-readable status <mr-status>`

#. ``\xff\xff/worker_interfaces`` - key as the worker's network address and value as the serialized ClientWorkerInterface, not transactional

Prior to api version 630, it was also possible to read a range starting at ``\xff\xff/worker_interfaces``. This is mostly an implementation detail of fdbcli,
but it's available in api version 630 as a module with prefix ``\xff\xff/worker_interfaces/``.

Api version 630 includes three new modules:

#. ``\xff\xff/transaction/`` - information about the current transaction
#. ``\xff\xff/metrics/`` - various metrics, not transactional
#. ``\xff\xff/clusterId`` - returns an immutable unique ID for a cluster

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
#. ``\xff\xff/transaction/conflicting_keys/`` Since using this feature costs server (i.e., commit proxy and resolver) resources, it's disabled by default. You must opt in by setting the ``report_conflicting_keys`` transaction option.

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
  ('\xff\xff/metrics/health/aggregate', '{"batch_limited":false,"limiting_storage_durability_lag":5000000,"limiting_storage_queue":1000,"tps_limit":483988.66315011407,"worst_storage_durability_lag":5000001,"worst_storage_queue":2036,"worst_log_queue":300}')
  ('\xff\xff/metrics/health/log/e639a9ad0373367784cc550c615c469b', '{"log_queue":300}')
  ('\xff\xff/metrics/health/storage/ab2ce4caf743c9c1ae57063629c6678a', '{"cpu_usage":2.398696781487125,"disk_usage":0.059995917598039405,"storage_durability_lag":5000001,"storage_queue":2036}')

``\xff\xff/metrics/health/aggregate``

Aggregate stats about cluster health. Reading this key alone is slightly cheaper than reading any of the per-process keys.

=================================== ======== ===============
**Field**                           **Type** **Description**
----------------------------------- -------- ---------------
batch_limited                       boolean  Whether or not the cluster is limiting batch priority transactions
limiting_storage_durability_lag     number   storage_durability_lag that ratekeeper is using to determine throttling (see the description for storage_durability_lag)
limiting_storage_queue              number   storage_queue that ratekeeper is using to determine throttling (see the description for storage_queue)
tps_limit                           number   The rate at which normal priority transactions are allowed to start
worst_storage_durability_lag        number   See the description for storage_durability_lag
worst_storage_queue                 number   See the description for storage_queue
worst_log_queue                     number   See the description for log_queue
=================================== ======== ===============

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


Read/write modules
==================

As of api version 700, some modules in the special key space allow writes as
well as reads. In these modules, a user can expect that mutations (i.e. sets,
clears, etc) do not have side-effects outside of the current transaction
until commit is called (the same is true for writes to the normal key space).
A user can also expect the effects on commit to be atomic. Reads to
special keys may require reading system keys (whose format is an implementation
detail), and for those reads appropriate read conflict ranges are added on
the underlying system keys.

Writes to read/write modules in the special key space are disabled by
default. Use the ``special_key_space_enable_writes`` transaction option to
enable them [#special_key_space_enable_writes]_.


.. _special-key-space-management-module:

Management module
-----------------

The management module is for temporary cluster configuration changes. For
example, in order to safely remove a process from the cluster, one can add an
exclusion to the ``\xff\xff/management/excluded/`` key prefix that matches
that process, and wait for necessary data to be moved away.

#. ``\xff\xff/management/excluded/<exclusion>`` Read/write. Indicates that the cluster should move data away from processes matching ``<exclusion>``, so that they can be safely removed. See :ref:`removing machines from a cluster <removing-machines-from-a-cluster>` for documentation for the corresponding fdbcli command.
#. ``\xff\xff/management/failed/<exclusion>`` Read/write. Indicates that the cluster should consider matching processes as permanently failed. This allows the cluster to avoid maintaining extra state and doing extra work in the hope that these processes come back. See :ref:`removing machines from a cluster <removing-machines-from-a-cluster>` for documentation for the corresponding fdbcli command.
#. ``\xff\xff/management/in_progress_exclusion/<address>`` Read-only. Indicates that the process matching ``<address>`` matches an exclusion, but still has necessary data and can't yet be safely removed.
#. ``\xff\xff/management/options/excluded/force`` Read/write. Setting this key disables safety checks for writes to ``\xff\xff/management/excluded/<exclusion>``. Setting this key only has an effect in the current transaction and is not persisted on commit.
#. ``\xff\xff/management/options/failed/force`` Read/write. Setting this key disables safety checks for writes to ``\xff\xff/management/failed/<exclusion>``. Setting this key only has an effect in the current transaction and is not persisted on commit.
#. ``\xff\xff/management/min_required_commit_version`` Read/write. Changing this key will change the corresponding system key ``\xff/minRequiredCommitVersion = [[Version]]``. The value of this special key is the literal text of the underlying ``Version``, which is ``int64_t``. If you set the key with a value failed to be parsed as ``int64_t``, ``special_keys_api_failure`` will be thrown. In addition, the given ``Version`` should be larger than the current read version and smaller than the upper bound(``2**63-1-version_per_second*3600*24*365*1000``). Otherwise, ``special_keys_api_failure`` is thrown. For more details, see help text of ``fdbcli`` command ``advanceversion``.
#. ``\xff\xff/management/maintenance/<zone_id> := <seconds>`` Read/write. Set/clear a key in this range will change the corresponding system key ``\xff\x02/healthyZone``. The value is a literal text of a non-negative ``double`` which represents the remaining time for the zone to be in maintenance. Commiting with an invalid value will throw ``special_keys_api_failure``. Only one zone is allowed to be in maintenance at the same time. Setting a new key in the range will override the old one and the transaction will throw ``special_keys_api_failure`` error if more than one zone is given. For more details, see help text of ``fdbcli`` command ``maintenance``.
   In addition, a special key ``\xff\xff/management/maintenance/IgnoreSSFailures`` in the range, if set, will disable datadistribution for storage server failures.
   It is doing the same thing as the fdbcli command ``datadistribution disable ssfailure``.
   Maintenance mode will be unable to use until the key is cleared, which is the same as the fdbcli command ``datadistribution enable ssfailure``.
   While the key is set, any commit that tries to set a key in the range will fail with the ``special_keys_api_failure`` error.
#. ``\xff\xff/management/data_distribution/<mode|rebalance_ignored>`` Read/write. Changing these two keys will change the two corresponding system keys ``\xff/dataDistributionMode`` and ``\xff\x02/rebalanceDDIgnored``. Reading the value of ``\xff\xff/management/data_distribution/mode`` will return a literal text of ``-1`` if it has never been initialized; in this case it is considered enabled. Values of ``1`` (enable), ``2`` (security mode which disables data moves but allows auditStorage part),  or ``0`` (disable) can be set to explicitly enable/disable data distribution. Transactions committed with invalid values will throw ``special_keys_api_failure``. The key ``\xff\xff/management/data_distribution/rebalance_ignored`` is normally unset and it means that data distribution is enabled for rebalance; it will be disabled instead if the key is set to an empty value or an integer (since 7.1). Any transaction committed with an invalid value for this key will throw ``special_keys_api_failure``. For more details, see help text of ``fdbcli`` command ``datadistribution``.
#. ``\xff\xff/management/consistency_check_suspended`` Read/write. Set or read this key will set or read the underlying system key ``\xff\x02/ConsistencyCheck/Suspend``. The value of this special key is unused thus if present, will be empty. In particular, if the key exists, then consistency is suspended. For more details, see help text of ``fdbcli`` command ``consistencycheck``.
#. ``\xff\xff/management/db_locked`` Read/write. A single key that can be read and modified. Set the key with a 32 bytes hex string UID will lock the database and clear the key will unlock. Read the key will return the UID string as the value. If the database is already locked, then the commit will fail with the ``special_keys_api_failure`` error. For more details, see help text of ``fdbcli`` command ``lock`` and ``unlock``.
#. ``\xff\xff/management/auto_coordinators`` Read-only. A single key, if read, will return a set of processes which is able to satisfy the current redundency level and serve as new coordinators. The return value is formatted as a comma delimited string of network addresses of coordinators, i.e. ``<ip:port>,<ip:port>,...,<ip:port>``.
#. ``\xff\xff/management/excluded_locality/<locality>`` Read/write. Indicates that the cluster should move data away from processes matching ``<locality>``, so that they can be safely removed. See :ref:`removing machines from a cluster <removing-machines-from-a-cluster>` for documentation for the corresponding fdbcli command.
#. ``\xff\xff/management/failed_locality/<locality>`` Read/write. Indicates that the cluster should consider matching processes as permanently failed. This allows the cluster to avoid maintaining extra state and doing extra work in the hope that these processes come back. See :ref:`removing machines from a cluster <removing-machines-from-a-cluster>` for documentation for the corresponding fdbcli command.
#. ``\xff\xff/management/options/excluded_locality/force`` Read/write. Setting this key disables safety checks for writes to ``\xff\xff/management/excluded_locality/<locality>``. Setting this key only has an effect in the current transaction and is not persisted on commit.
#. ``\xff\xff/management/options/failed_locality/force`` Read/write. Setting this key disables safety checks for writes to ``\xff\xff/management/failed_locality/<locality>``. Setting this key only has an effect in the current transaction and is not persisted on commit.
#. ``\xff\xff/management/tenant/map/<tenant>`` Read/write. Setting a key in this range to any value will result in a tenant being created with name ``<tenant>``. Clearing a key in this range will delete the tenant with name ``<tenant>``. Reading all or a portion of this range will return the list of tenants currently present in the cluster, excluding any changes in this transaction. Values read in this range will be JSON objects containing the metadata for the associated tenants.
#. ``\xff\xff/management/tenant/rename/<tenant>`` Read/write. Setting a key in this range to an unused tenant name will result in the tenant with the name ``<tenant>`` to be renamed to the value provided. If the rename operation is a transaction retried in a loop, it is possible for the rename to be applied twice, in which case ``tenant_not_found`` or ``tenant_already_exists`` errors may be returned. This can be avoided by checking for the tenant's existence first.
#. ``\xff\xff/management/options/worker_interfaces/verify`` Read/write. Setting this key will add a verification phase in reading ``\xff\xff/worker_interfaces``. Setting this key only has an effect in the current transaction and is not persisted on commit. Try to establish connections with every worker from the list returned by Cluster Controller and only return those workers that the client can connect to. This option is now only used in fdbcli commands ``kill``, ``suspend`` and ``expensive_data_check`` to populate the worker list.

An exclusion is syntactically either an ip address (e.g. ``127.0.0.1``), or
an ip address and port (e.g. ``127.0.0.1:4500``) or any locality (e.g ``locality_dcid:primary-satellite`` or
``locality_zoneid:primary-satellite-log-2`` or ``locality_machineid:primary-stateless-1`` or ``locality_processid:223be2da244ca0182375364e4d122c30``).
If no port is specified, then all processes on that host match the exclusion.
For locality, all processes that match the given locality are excluded.

Configuration module
--------------------

The configuration module is for changing the cluster configuration.
For example, you can change a process type or update coordinators by manipulating related special keys through transactions.

#. ``\xff\xff/configuration/process/class_type/<address> := <class_type>`` Read/write. Reading keys in the range will retrieve processes' class types. Setting keys in the range will update processes' class types. The process matching ``<address>`` will be assigned to the given class type if the commit is successful. The valid class types are ``storage``, ``transaction``, ``resolution``, etc. A full list of class type can be found via ``fdbcli`` command ``help setclass``. Clearing keys is forbidden in the range. Instead, you can set the type as ``default``, which will clear the assigned class type if existing. For more details, see help text of ``fdbcli`` command ``setclass``.
#. ``\xff\xff/configuration/process/class_source/<address> := <class_source>`` Read-only. Reading keys in the range will retrieve processes' class source. The class source is one of ``command_line``, ``configure_auto``, ``set_class`` and ``invalid``, indicating the source that the process's class type comes from.
#. ``\xff\xff/configuration/coordinators/processes := <ip:port>,<ip:port>,...,<ip:port>`` Read/write. A single key, if read, will return a comma delimited string of coordinators' network addresses. Thus to provide a new set of cooridinators, set the key with a correct formatted string of new coordinators' network addresses. As there's always the need to have coordinators, clear on the key is forbidden and a transaction will fail with the ``special_keys_api_failure`` error if the clear is committed. For more details, see help text of ``fdbcli`` command ``coordinators``.
#. ``\xff\xff/configuration/coordinators/cluster_description := <new_description>`` Read/write. A single key, if read, will return the cluster description. Thus modifying the key will update the cluster decription. The new description needs to match ``[A-Za-z0-9_]+``, otherwise, the ``special_keys_api_failure`` error will be thrown. In addition, clear on the key is meaningless thus forbidden. For more details, see help text of ``fdbcli`` command ``coordinators``.

The ``<address>`` here is the network address of the corresponding process. Thus the general form is ``ip:port``.

Error message module
--------------------

Each module written to validates the transaction before committing, and this
validation failing is indicated by a ``special_keys_api_failure`` error.
More detailed information about why this validation failed can be accessed through the ``\xff\xff/error_message`` key, whose value is a json document with the following schema.

========================== ======== ===============
**Field**                  **Type** **Description**
-------------------------- -------- ---------------
retriable                  boolean  Whether or not this operation might succeed if retried
command                    string   The fdbcli command corresponding to this operation
message                    string   Help text explaining the reason this operation failed
========================== ======== ===============

Global configuration module
---------------------------

The global configuration module provides an interface to read and write values
to :doc:`global-configuration`. In general, clients should not read and write
the global configuration special key space keys directly, but should instead
use the global configuration functions.

#. ``\xff\xff/global_config/<key> := <value>`` Read/write. Reading keys in the range will return a tuple decoded string representation of the value for the given key. Writing a value will update all processes in the cluster with the new key-value pair. Values must be written using the :ref:`api-python-tuple-layer`.

.. _special-key-space-tracing-module:

Tracing module
--------------

The tracing module provides read and write access to a transactions' tracing
data. Every transaction contains a unique identifier which follows the
transaction through the system. By providing access to set this identifier,
clients can connect FoundationDB transactions to outside events.

#. ``\xff\xff/tracing/transaction_id := <transaction_id>`` Read/write. A 64-bit integer transaction ID which follows the transaction as it moves through FoundationDB. All transactions are assigned a random transaction ID on creation, and this key can be read to surface the randomly generated ID. Alternatively, set this key to provide a custom identifier. When setting this key, provide a string in the form of a 64-bit integer, which will be automatically converted to the appropriate type.
#. ``\xff\xff/tracing/token := <tracing_enabled>`` Read/write. Set to true/false to enable or disable tracing for the transaction, respectively. If read, returns a 64-bit integer set to 0 if tracing has been disabled, or a random 64-bit integer otherwise (this integers value has no meaning to the client other than to determine whether the transaction will be traced).

.. _special-key-space-deprecation:

Deprecated Keys
===============

Listed below are the special keys that have been deprecated. Special key(s) will no longer be accessible when the client specifies an API version equal to or larger than the version where they were deprecated. Clients specifying older API versions will be able to continue using the deprecated key(s).

#. ``\xff\xff/management/profiling/<client_txn_sample_rate|client_txn_size_limit>`` Deprecated as of API version 720. The corresponding functionalities are now covered by the global configuration module. For details, see :doc:`global-configuration`. Read/write. Changing these two keys will change the corresponding system keys ``\xff\x02/fdbClientInfo/<client_txn_sample_rate|client_txn_size_limit>``, respectively. The value of ``\xff\xff/management/client_txn_sample_rate`` is a literal text of ``double``, and the value of ``\xff\xff/management/client_txn_size_limit`` is a literal text of ``int64_t``. A special value ``default`` can be set to or read from these two keys, representing the client profiling is disabled. In addition, ``clear`` in this range is not allowed. For more details, see help text of ``fdbcli`` command ``profile client``.

Versioning
==========

For how FDB clients deal with versioning, see :ref:`api-versions`. The special key space deals with versioning by using the ``API_VERSION`` passed to initialize the client. Any module added at a version larger than the API version set by the client will be inaccessible. For example, if a module is added in version 7.0 and the client sets its API version to 630, then the module will not available. When removing or updating existing modules, module developers need to continue to provide the old behavior for clients that specify old API versions.

To remove the functionality of a certain special key(s), specify the API version where the function is being deprecated in the ``registerSpecialKeysImpl`` function. When a client specifies an API version greater than or equal to the deprecation version, the functionality will not be available. Move and update its documentation to :ref:`special-key-space-deprecation`.

To update the implementation of any special keys, add the new implementation and use ``API_VERSION`` to switch between different implementations.

Add notes in ``api-version-upgrade-guide.rst`` if you either remove or update a special key(s) implementation.

.. [#conflicting_keys] In practice, the transaction probably committed successfully. However, if you're running multiple resolvers then it's possible for a transaction to cause another to abort even if it doesn't commit successfully.
.. [#max_read_transaction_life_versions] The number 5000000 comes from the server knob MAX_READ_TRANSACTION_LIFE_VERSIONS
.. [#special_key_space_enable_writes] Enabling this option enables other transaction options, such as ``ACCESS_SYSTEM_KEYS``. This may change in the future.
