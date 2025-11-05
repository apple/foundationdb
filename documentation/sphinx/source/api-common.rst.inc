.. -*- mode: rst; -*-

.. |separately-installed-bindings| replace::
    For the language binding to function, FoundationDB client binaries whose version is at least as recent
    must be installed. If you upgrade a language binding to a new version, you may need to upgrade the FoundationDB client binaries as well. See :ref:`installing-client-binaries`.

.. |project-dependency| replace::
    If you have a project with automatic dependency installation and have expressed a dependency on foundationdb, it may automatically install the latest version of the language binding when you deploy your project to a new machine. If you have not also upgraded the Foundation client binary, an unplanned upgrade of the language binding may encounter an incompatibility. You should therefore configure any project dependency on foundationdb in coordination with your overall upgrade strategy.

.. |client-installed-bindings| replace::
    The language binding requires FoundationDB client binaries whose version is at least as recent. The binding installed with FoundationDB installation will automatically satisfy this requirement.

.. |api-version-rationale| replace::
    FoundationDB encapsulates multiple versions of its interface by requiring the client to explicitly specify the version of the API it uses. The purpose of this design is to allow you to upgrade the server, client libraries, or bindings without having to modify client code. The client libraries support all previous versions of the API. The API version specified by the client is used to control the behavior of the binding. You can therefore upgrade to more recent packages (and thus receive various improvements) without having to change your code.

.. |api-version-multi-version-warning| replace::
    When using the :ref:`multi-version client API <multi-version-client-api>`, setting an API version that is not supported by a particular client library will prevent that client from being used to connect to the cluster. In particular, you should not advance the API version of your application after upgrading your client until the cluster has also been upgraded.

.. |transaction-blurb1| replace::
    In FoundationDB, a transaction is a mutable snapshot of a database. All read and write operations on a transaction see and modify an otherwise-unchanging version of the database and only change the underlying database if and when the transaction is committed. Read operations do see the effects of previous write operations on the same transaction. Committing a transaction usually succeeds in the absence of :ref:`conflicts <conflict-ranges>`.

.. |transaction-blurb2| replace::
    Transactions group operations into a unit with the properties of *atomicity*, *isolation*, and *durability*. Transactions also provide the ability to maintain an application's invariants or integrity constraints, supporting the property of *consistency*. Together these properties are known as :ref:`ACID <ACID>`.

.. |transaction-blurb3| replace::
    Transactions are also causally consistent: once a transaction has been successfully committed, all subsequently created transactions will see the modifications made by it.

.. |used-during-commit-blurb| replace::
    If any operation is performed on a transaction after a commit has been issued but before it has returned, both the commit and the operation will |error-raise-type| a :ref:`used_during_commit <developer-guide-error-codes>` |error-type|. In this case, all subsequent operations on this transaction will |error-raise-type| this error until |reset-func-name| is called.

.. |unknown-result-blurb| replace::
    As with other client/server databases, in some failure scenarios a client may be unable to determine whether a transaction succeeded. In these cases, |commit-func| will |error-raise-type| a :ref:`commit_unknown_result <developer-guide-error-codes>` |error-type|. The |on-error-func| function treats this |error-type| as retryable, so retry loops that don't check for :ref:`commit_unknown_result <developer-guide-error-codes>` could execute the transaction twice. In these cases, you must consider the idempotence of the transaction.

.. |commit-unknown-result-blurb| replace::

    |unknown-result-blurb| For more information, see :ref:`developer-guide-unknown-results`.

.. |commit-outstanding-reads-blurb| replace::
    Normally, commit will wait for outstanding reads to return. However, if those reads were snapshot reads or the transaction option for disabling "read-your-writes" has been invoked, any outstanding reads will immediately return errors.

.. |transaction-cancel-blurb| replace::
    Cancels the transaction. All pending or future uses of the transaction will |error-raise-type| a :ref:`transaction_cancelled <developer-guide-error-codes>` |error-type|. The transaction can be used again after it is |reset-func-name|.

.. |snapshot-blurb1| replace::
    Snapshot reads selectively relax FoundationDB's isolation property, reducing :ref:`conflicts <developer-guide-transaction-conflicts>` but making it harder to reason about concurrency.

.. |snapshot-blurb2| replace::
    By default, FoundationDB transactions guarantee :ref:`strictly serializable isolation <ACID>`, resulting in a state that is *as if* transactions were executed one at a time, even if they were executed concurrently. Serializability has little performance cost when there are few :ref:`conflicts <developer-guide-transaction-conflicts>` but can be expensive when there are many. FoundationDB therefore also permits individual reads within a transaction to be done as snapshot reads.

.. |snapshot-blurb3| replace::
    Snapshot reads differ from ordinary (strictly serializable) reads by permitting the values they read to be modified by concurrent transactions, whereas strictly serializable reads cause conflicts in that case. Like strictly serializable reads, snapshot reads see the effects of prior writes in the same transaction. For more information on the use of snapshot reads, see :ref:`snapshot isolation`.

.. |snapshot-blurb4| replace::
    Snapshot reads also interact with transaction commit a little differently than normal reads. If a snapshot read is outstanding when transaction commit is called that read will immediately return an error. (Normally, transaction commit will wait until outstanding reads return before committing.)

.. |keys-values-blurb| replace::
    Keys and values in FoundationDB are simple byte strings.

.. |keys-values-other-types-blurb| replace::
    To encode other data types, see :ref:`encoding-data-types` and the |tuple-layer|.

.. |as-foundationdb-blurb| replace::
    In some cases, you may have objects that are used to *represent* specific keys or values (for example, see |subspace|). As a convenience, the language binding API can work seamlessly with such objects if they implement the |as-foundationdb-key| or |as-foundationdb-value| methods, respectively. API methods that accept a key will alternately accept an object that implements the |as-foundationdb-key| method. Likewise, API methods accepting a value will also accept an object that implements the |as-foundationdb-value| method.

.. |as-foundationdb-warning| replace::
    |as-foundationdb-key| and |as-foundationdb-value| are not intended to implement serialization protocols for object storage. Use these functions only when your object represents a specific key or value.

.. |database-blurb1| replace::
    |database-type| represents a FoundationDB database --- a mutable, lexicographically ordered mapping from binary keys to binary values.

.. |database-blurb2| replace::
    Although |database-type| provides convenience methods for reading and writing, modifications to a database are usually via transactions, which are usually created and committed automatically by |database-auto|.

.. |database-sync| replace::
    The convenience methods provided by |database-type| have the same signature as the corresponding methods of ``Transaction``. However, most of the |database-type| methods are fully synchronous. (An exception is the methods for watches.) As a result, the |database-type| methods do not support the use of :ref:`implicit parallelism with futures <developer-guide-programming-with-futures>`.

.. |tenant-blurb1| replace::
    |tenant-type| represents a FoundationDB tenant. Tenants are optional named transaction domains that can be used to provide multiple disjoint key-spaces to client applications. A transaction created in a tenant will be limited to the keys contained within that tenant, and transactions operating on different tenants can use the same key names without interfering with each other.

.. |keysel-blurb1| replace::
    FoundationDB's lexicographically ordered data model permits finding keys based on their order (for example, finding the first key in the database greater than a given key). Key selectors represent a description of a key in the database that could be resolved to an actual key by |get-key-func| or used directly as the beginning or end of a range in |get-range-func|.

.. |keysel-blurb2| replace::
    For more about how key selectors work, see :ref:`key selectors`.

.. |database-atomic-ops-idempotency-note| replace::

    Note that since some atomic operations are not idempotent, the implicit use of |database-auto| could interact with a :ref:`commit_unknown_result <developer-guide-error-codes>` |error-type| in unpredictable ways. For more information, see :ref:`developer-guide-unknown-results`.

.. |atomic-ops-blurb1| replace::
    An atomic operation is a single database command that carries out several logical steps: reading the value of a key, performing a transformation on that value, and writing the result. Different atomic operations perform different transformations. Like other database operations, an atomic operation is used within a transaction; however, its use within a transaction will not cause the transaction to conflict.

.. |atomic-ops-blurb2| replace::
    Atomic operations do not expose the current value of the key to the client but simply send the database the transformation to apply. In regard to conflict checking, an atomic operation is equivalent to a write without a read. It can only cause *other* transactions performing reads of the key to conflict.

.. |atomic-ops-blurb3| replace::
    By combining these logical steps into a single, read-free operation, FoundationDB can guarantee that the transaction will not conflict due to the operation. This makes atomic operations ideal for operating on keys that are frequently modified. A common example is the use of a key-value pair as a counter.

.. |atomic-ops-warning| replace::
    If a transaction uses both an atomic operation and a strictly serializable read on the same key, the benefits of using the atomic operation (for both conflict checking and performance) are lost.

.. |atomic-add1| replace::
    Performs an addition of little-endian integers. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``. In case of overflow, the result is truncated to the width of ``param``.

.. |atomic-add2| replace::
    The integers to be added must be stored in a little-endian representation. They can be signed in two's complement representation or unsigned. You can add to an integer at a known offset in the value by prepending the appropriate number of zero bytes to ``param`` and padding with zero bytes to match the length of the value. However, this offset technique requires that you know the addition will not cause the integer field within the value to overflow.

.. |atomic-and| replace::
    Performs a bitwise "and" operation. If the existing value in the database is not present, then ``param`` is stored in the database. If the existing value in the database is  shorter than ``param``, it is first extended to the length of ``param`` with zero bytes. If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.

.. |atomic-or| replace::
    Performs a bitwise "or" operation. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes. If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.

.. |atomic-xor| replace::
    Performs a bitwise "xor" operation. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes. If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.

.. |atomic-compare-and-clear| replace::
   Performs an atomic ``compare and clear`` operation. If the existing value in the database is equal to the given value, then given key is cleared.

.. |atomic-max1| replace::
    Sets the value in the database to the larger of the existing value and ``param``. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes. If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.

.. |atomic-max-min| replace::
    Both the existing value and ``param`` are treated as unsigned integers. (This differs from the behavior of atomic addition.)

.. |atomic-min1| replace::
    Sets the value in the database to the smaller of the existing value and ``param``. If the existing value in the database is not present, then ``param`` is stored in the database. If the existing value in the database is shorter than ``param``, it is first extended to the length of ``param`` with zero bytes. If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.

.. |atomic-byte-min| replace::
    Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored. Otherwise the smaller of the two values is then stored in the database.

.. |atomic-byte-max| replace::
    Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored. Otherwise the larger of the two values is then stored in the database.

.. |atomic-set-versionstamped-key-1| replace::
    Transforms ``key`` using a versionstamp for the transaction. This key must be at least 14 bytes long. The final 4 bytes will be interpreted as a 32-bit little-endian integer denoting an index into the key at which to perform the transformation, and then trimmed off the key. The 10 bytes in the key beginning at the index will be overwritten with the versionstamp. If the index plus 10 bytes points past the end of the key, the result will be an error. Sets the transformed key in the database to ``param``.

.. |atomic-set-versionstamped-value| replace::
    Transforms ``param`` using a versionstamp for the transaction. This parameter must be at least 14 bytes long. The final 4 bytes will be interpreted as a 32-bit little-endian integer denoting an index into the parameter at which to perform the transformation, and then trimmed off the key. The 10 bytes in the parameter beginning at the index will be overwritten with the versionstamp. If the index plus 10 bytes points past the end of the parameter, the result will be an error. Sets ``key`` in the database to the transformed parameter.

.. |atomic-versionstamps-1| replace::
    A versionstamp is a 10 byte, unique, monotonically (but not sequentially) increasing value for each committed transaction. The first 8 bytes are the committed version of the database (serialized in big-endian order). The last 2 bytes are monotonic in the serialization order for transactions (serialized in big-endian order).


.. |atomic-versionstamps-2| replace::
    A transaction is not permitted to read any transformed key or value previously set within that transaction, and an attempt to do so will result in an ``accessed_unreadable`` error.  The range of keys marked unreadable when setting a versionstamped key begins at the transactions's read version if it is known, otherwise a versionstamp of all ``0x00`` bytes is conservatively assumed.  The upper bound of the unreadable range is a versionstamp of all ``0xFF`` bytes.

.. |atomic-versionstamps-tuple-warning-key| replace::
    At this time, versionstamped keys are not compatible with the Tuple layer except in Java, Python, and Go. Note that this implies versionstamped keys may not be used with the Subspace and Directory layers except in those languages.

.. |atomic-versionstamps-tuple-warning-value| replace::
    At this time, versionstamped values are not compatible with the Tuple layer except in Java, Python, and Go. Note that this implies versionstamped values may not be used with the Subspace and Directory layers except in those languages.

.. |api-version| replace:: 800

.. |streaming-mode-blurb1| replace::
    When using |get-range-func| and similar interfaces, API clients can request large ranges of the database to iterate over.  Making such a request doesn't necessarily mean that the client will consume all of the data in the range - sometimes the client doesn't know how far it intends to iterate in advance.  FoundationDB tries to balance latency and bandwidth by requesting data for iteration in batches.

.. |streaming-mode-blurb2| replace::
    Streaming modes permit the API client to customize this performance tradeoff by providing extra information about how the iterator will be used.

.. |tuple-layer-blurb| replace::
    The FoundationDB API comes with a built-in layer for encoding tuples into keys usable by FoundationDB. The encoded key maintains the same sort order as the original tuple: sorted first by the first element, then by the second element, etc. This makes the tuple layer ideal for building a variety of higher-level data models.

.. |tuple-layer-note| replace::
    For general guidance on tuple usage, see the discussion in the document on :ref:`Data Modeling <data-modeling-tuples>`.

.. |transaction-reset-blurb| replace::
    Rollback a transaction, completely resetting it to its initial state. This is logically equivalent to destroying the transaction and creating a new one.

.. |transaction-reset-cancel-warning| replace::
    Be careful if you are using |reset-func| and |cancel-func| concurrently with the same transaction. Since they negate each other's effects, a race condition between these calls will leave the transaction in an unknown state.

.. |transaction-commit-cancel-warning| replace::
    If your program attempts to cancel a transaction after |commit-func| has been called but before it returns, unpredictable behavior will result. While it is guaranteed that the transaction will eventually end up in a cancelled state, the commit may or may not occur. Moreover, even if the call to |commit-func| appears to |error-raise-type| a :ref:`transaction_cancelled <developer-guide-error-codes>` |error-type|,  the commit may have occurred or may occur in the future. This can make it more difficult to reason about the order in which transactions occur.

.. |transaction-get-committed-version-blurb| replace::
    Gets the version number at which a successful commit modified the database. This must be called only after the successful (non-error) completion of a call to |commit-func| on this Transaction, or the behavior is undefined. Read-only transactions do not modify the database when committed and will have a committed version of -1. Keep in mind that a transaction which reads keys and then sets them to their current values may be optimized to a read-only transaction.

.. |transaction-get-approximate-size-blurb| replace::
    Gets the the approximate transaction size so far, which is the summation of the estimated size of mutations, read conflict ranges, and write conflict ranges.

.. |transaction-get-versionstamp-blurb| replace::
    Returns a future which will contain the versionstamp which was used by any versionstamp operations in this transaction. This function must be called before a call to |commit-func| on this Transaction. The future will be ready only after the successful completion of a call to |commit-func| on this Transaction. Read-only transactions do not modify the database when committed and will result in the future completing with an error. Keep in mind that a transaction which reads keys and then sets them to their current values may be optimized to a read-only transaction.

.. |transaction-watch-blurb| replace::

    A watch's behavior is relative to the transaction that created it. A watch will report a change in relation to the key's value as readable by that transaction. The initial value used for comparison is either that of the transaction's read version or the value as modified by the transaction itself prior to the creation of the watch. If the value changes and then changes back to its initial value, the watch might not report the change.

.. |transaction-watch-committed-blurb| replace::

    Until the transaction that created it has been committed, a watch will not report changes made by *other* transactions. In contrast, a watch will immediately report changes made by the transaction itself. Watches cannot be created if the transaction has set |read-your-writes-disable-option|, and an attempt to do so will |error-raise-type| an :ref:`watches_disabled <developer-guide-error-codes>` |error-type|.

.. |transaction-watch-error-blurb| replace::

    If the transaction used to create a watch encounters an |error-type| during commit, then the watch will be set with that |error-type|. A transaction whose :ref:`commit result is unknown <developer-guide-unknown-results>` will set all of its watches with the :ref:`commit_unknown_result <developer-guide-error-codes>` |error-type|. If an uncommitted transaction is reset or destroyed, then any watches it created will be set with the :ref:`transaction_cancelled <developer-guide-error-codes>` |error-type|.

.. |transaction-watch-limit-blurb| replace::

    By default, each database connection can have no more than 10,000 watches that have not yet reported a change. When this number is exceeded, an attempt to create a watch will |error-raise-type| a :ref:`too_many_watches <developer-guide-error-codes>` |error-type|. This limit can be changed using |max-watches-database-option|. Because a watch outlives the transaction that creates it, any watch that is no longer needed should be cancelled by calling |future-cancel| on its returned future.

.. |transaction-clear-range-blurb| replace::

    Range clears are efficient with FoundationDB -- clearing large amounts of data will be fast. However, this will not immediately free up disk - data for the deleted range is cleaned up in the background. For purposes of computing the transaction size, only the begin and end keys of a clear range are counted. The size of the data stored in the range does not count against the transaction size limit.

.. |conflict-range-note| replace::

    Most applications will use the strictly serializable isolation that transactions provide by default and will not need to manipulate conflict ranges.

.. |conflict-range-blurb| replace::

    The following make it possible to add :ref:`conflict ranges <conflict-ranges>` to a transaction.

.. |add-read-conflict-range-blurb| replace::

    Adds a range of keys to the transaction's read conflict ranges as if you had read the range. As a result, other transactions that write a key in this range could cause the transaction to fail with a conflict.

.. |add-read-conflict-key-blurb| replace::

    Adds a key to the transaction's read conflict ranges as if you had read the key. As a result, other transactions that concurrently write this key could cause the transaction to fail with a conflict.

.. |add-write-conflict-range-blurb| replace::

    Adds a range of keys to the transaction's write conflict ranges as if you had cleared the range. As a result, other transactions that concurrently read a key in this range could fail with a conflict.

.. |add-write-conflict-key-blurb| replace::

    Adds a key to the transaction's write conflict ranges as if you had written the key. As a result, other transactions that concurrently read this key could fail with a conflict.

.. |network-options-blurb| replace::

    A singleton providing options which affect the entire FoundationDB client. Note that network options can also be :ref:`set using environment variables<network-options-using-environment-variables>`.

.. |option-trace-enable-blurb| replace::
    Enables trace file generation on this FoundationDB client. Trace files will be generated in the specified output directory. If the directory is specified as |null-type|, then the output directory will be the current working directory.

.. |option-trace-enable-warning| replace::
    The specified output directory must be unique to this client. In the present release, trace logging does not allow two clients to share a directory.

.. |option-trace-max-logs-size-blurb| replace::
    Sets the maximum size in bytes for the sum of this FoundationDB client's trace output files in a single log directory.

.. |option-trace-roll-size-blurb| replace::
    Sets the maximum size in bytes of a single trace output file for this FoundationDB client.

.. |option-trace-format-blurb| replace::
    Select the format of the trace files for this FoundationDB client. xml (the default) and json are supported.

.. |option-trace-clock-source-blurb| replace::
    Select clock source for trace files. now (the default) or realtime are supported.

.. |network-options-warning| replace::

    It is an error to set these options after the first call to |open-func| anywhere in your application.

.. |tls-options-burb| replace::

    The following options are only used when connecting to a :doc:`TLS-enabled cluster <tls>`.

.. |option-tls-plugin-blurb| replace::

    Sets the :ref:`TLS plugin <configuring-tls>` to load. This option, if used, must be set before any other TLS options.

.. |option-tls-cert-path-blurb| replace::

    Sets the path for the file from which the :ref:`certificate chain <tls-certificate-file>` will be loaded.

.. |option-tls-key-path-blurb| replace::

    Sets the path for the file from which to load the :ref:`private key <tls-key-file>` corresponding to your own certificate.

.. |option-tls-verify-peers-blurb| replace::

    Sets the :ref:`peer certificate field verification criteria <tls-verify-peers>`.

.. |option-tls-cert-bytes| replace::

    Sets the certificate chain.

.. |option-tls-key-bytes| replace::

    Set the private key corresponding to your own certificate.

.. |option-disable-multi-version-client-api| replace::

    Disables the :ref:`multi-version client API <multi-version-client-api>` and instead uses the local client directly. Must be set before setting up the network.

.. |option-callbacks-on-external-threads| replace::

    If set, callbacks from :ref:`external client libraries <multi-version-client-api>` can be called from threads created by the FoundationDB client library. Otherwise, callbacks will be called from either the thread used to add the callback or the network thread. Setting this option can improve performance when connected using an external client, but may not be safe to use in all environments. Must be set before setting up the network. WARNING: This feature is considered experimental at this time.

.. |option-external-client-library| replace::

    Adds an external client library for use by the :ref:`multi-version client API <multi-version-client-api>`. Must be set before setting up the network.

.. |option-external-client-directory| replace::

    Searches the specified path for dynamic libraries and adds them to the list of client libraries for use by the :ref:`multi-version client API <multi-version-client-api>`. Must be set before setting up the network.

.. |database-options-blurb| replace::

    Database options alter the behavior of FoundationDB databases.

.. |option-location-cache-size-blurb| replace::
    Set the size of the client location cache. Raising this value can boost performance in very large databases where clients access data in a near-random pattern. This value must be an integer in the range [0, 2\ :sup:`31`-1]. Defaults to 100000.

.. |option-max-watches-blurb| replace::

    Set the maximum number of watches allowed to be outstanding on a database connection. Increasing this number could result in increased resource usage. Reducing this number will not cancel any outstanding watches. Defaults to 10000 and cannot be larger than 1000000.

.. |option-machine-id-blurb| replace::

    Specify the machine ID of a server to be preferentially used for database operations. ID must be a string of up to 16 hexadecimal digits that was used to configure :ref:`fdbserver processes <foundationdb-conf-fdbserver>`. Load balancing uses this option for location-awareness, attempting to send database operations first to servers on a specified machine, then a specified datacenter, then returning to its default algorithm.

.. |option-datacenter-id-blurb| replace::

    Specify the datacenter ID to be preferentially used for database operations. ID must be a string of up to 16 hexadecimal digits that was used to configure :ref:`fdbserver processes <foundationdb-conf-fdbserver>`. Load balancing uses this option for location-awareness, attempting to send database operations first to servers on a specified machine, then a specified datacenter, then returning to its default algorithm.

.. |option-db-tr-retry-limit-blurb| replace::

    Set the default maximum number of retries for each transaction after which additional calls to |on-error-func| will throw the most recently seen error code. This is equivalent to calling |retry-limit-transaction-option| on each transaction created by this database.

.. |option-db-tr-size-limit-blurb| replace::

    Set the default maximum transaction size in bytes. This is equivalent to calling |transaction-size-limit-database-option| on each transaction created by this database.

.. |option-db-tr-timeout-blurb| replace::

    Set the default timeout duration in milliseconds after which all transactions created by this database will automatically be cancelled. This is equivalent to calling |timeout-transaction-option| on each transaction created by this database. This option can only be called if the API version is at least 610.

.. |option-db-tr-max-retry-delay-blurb| replace::

    Set the default maximum backoff delay incurred by each transaction in the call to |on-error-func| if the error is retryable. This is equivalent to calling |max-retry-delay-transaction-option| on each transaction created by this database.

.. |option-db-causal-read-risky-blurb| replace::

    Transactions do not require the strict causal consistency guarantee that FoundationDB provides by default.  The read version will be committed, and usually will be the latest committed, but might not be the latest committed in the event of a simultaneous fault and misbehaving clock. Enabling this option is equivalent to calling |causal-read-risky-transaction-option| on each transaction created by this database.

.. |option-db-snapshot-ryw-enable-blurb| replace::

    If this option has been set an equal or more times with this database than the disable option, snapshot reads *will* see the effects of prior writes in the same transaction. Enabling this option is equivalent to calling |snapshot-ryw-enable-transaction-option| on each transaction created by this database.

.. |option-db-snapshot-ryw-disable-blurb| replace::

    If this option has been set more times with this database than the disable option, snapshot reads will *not* see the effects of prior writes in the same transaction. Disabling this option is equivalent to calling |snapshot-ryw-disable-transaction-option| on each transaction created by this database.

.. |option-db-tr-transaction-logging-max-field-length-blurb| replace::

    Sets the maximum escaped length of key and value fields to be logged to the trace file via the LOG_TRANSACTION option. This is equivalent to calling |transaction-logging-max-field-length-transaction-option| on each transaction created by this database.

.. |transaction-options-blurb| replace::

    Transaction options alter the behavior of FoundationDB transactions. FoundationDB defaults to extremely safe transaction behavior, and we have worked hard to make the performance excellent with the default setting, so you should not often need to use transaction options.

.. |option-snapshot-ryw-enable-blurb| replace::

    If this option is set an equal or more times in this transaction than the disable option, snapshot reads *will* see the effects of prior writes in the same transaction. This option can be enabled one or more times at the database-level by calling |snapshot-ryw-enable-database-option|.

.. |option-snapshot-ryw-disable-blurb| replace::

    If this option is set more times in this transaction than the enable option, snapshot reads will *not* see the effects of prior writes in the same transaction. Note that prior to API version 300, this was the default behavior. This option can be disabled one or more times at the database level by calling |snapshot-ryw-disable-database-option|.

.. |option-priority-batch-blurb| replace::
    This transaction should be treated as low priority (other transactions will be processed first). Batch priority transactions will also be throttled at load levels smaller than for other types of transactions and may be fully cut off in the event of machine failures. Useful for doing potentially saturating batch work without interfering with the latency of other operations.

.. |option-priority-system-immediate-blurb| replace::

    This transaction should be treated as extremely high priority, taking priority over other transactions and bypassing controls on transaction queuing.

.. |option-priority-system-immediate-warning| replace::

    This is intended for the use of internal database functions and low-level tools; use by applications may result in severe database performance or availability problems.

.. |option-causal-read-risky-blurb| replace::

    This transaction does not require the strict causal consistency guarantee that FoundationDB provides by default.  The read version will be committed, and usually will be the latest committed, but might not be the latest committed in the event of a simultaneous fault and misbehaving clock. One can set this for all transactions by calling |causal-read-risky-database-option|.

.. |option-causal-write-risky-blurb| replace::

    The application either knows that this transaction will be self-conflicting (at least one read overlaps at least one set or clear), or is willing to accept a small risk that the transaction could be committed a second time after its commit apparently succeeds.  This option provides a small performance benefit.

.. |option-read-your-writes-disable-blurb| replace::

    When this option is invoked, a read performed by a transaction will not see any prior mutations that occurred in that transaction, instead seeing the value which was in the database at the transaction's read version. This option may provide a small performance benefit for the client, but also disables a number of client-side optimizations which are beneficial for transactions which tend to read and write the same keys within a single transaction.

.. |option-read-your-writes-disable-note| replace::

    It is an error to set this option after performing any reads or writes on the transaction.

.. |option-read-ahead-disable-blurb| replace::

    Disables read-ahead caching for range reads. Under normal operation, a transaction will read extra rows from the database into cache if range reads are used to page through a series of data one row at a time (i.e. if a range read with a one row limit is followed by another one row range read starting immediately after the result of the first).

.. |option-access-system-keys-blurb| replace::

    Allows this transaction to read and modify system keys (those that start with the byte ``0xFF``).

.. |option-access-system-keys-warning| replace::

    Writing into system keys will likely break your database. Further, even for readers, the format of data in the system keys may change from version to version in FoundationDB.

.. |option-read-system-keys-blurb| replace::

    Allows this transaction to read system keys (those that start with the byte ``0xFF``).

.. |option-read-system-keys-warning| replace::

    The format of data in the system keys may change from version to version in FoundationDB.

..  |option-set-retry-limit-blurb1| replace::

    Set a maximum number of retries after which additional calls to |on-error-func| will throw the most recently seen error code. (By default, a transaction permits an unlimited number of retries.) Valid parameter values are [-1, INT_MAX]. If set to -1, the transaction returns to the default of unlimited retries.

..  |option-set-retry-limit-blurb2| replace::

    Prior to API version 610, Like all other transaction options, the retry limit must be reset after a call to |on-error-func|. If the API version is 610 or newer, then the retry limit is not reset. Note that at all API versions, it is safe and legal to call this option after each call to |on-error-func|, so most code written assuming the older behavior can be upgraded without requiring any modification. This also means there is no need to introduce logic to conditionally set this option within retry loops. One can also set the default retry limit for all transactions by calling |retry-limit-database-option|.

..  |option-set-max-retry-delay-blurb| replace::

    Set the maximum backoff delay incurred in the call to |on-error-func| if the error is retryable. Prior to API version 610, like all other transaction options, the maximum retry delay must be reset after a call to |on-error-func|. If the API version is 610 or newer, then the maximum retry delay is not reset. Note that at all API versions, it is safe and legal to call this option after each call to |on-error-func|, so most cade written assuming the older behavior can be upgraded without requiring any modification. This also means there is no need to introduce logic to conditionally set this option within retry loops. One can set the default retry limit for all transactions by calling |max-retry-delay-database-option|.

..  |option-set-size-limit-blurb| replace::

    Set the transaction size limit in bytes. The size is calculated by combining the sizes of all keys and values written or mutated, all key ranges cleared, and all read and write conflict ranges. (In other words, it includes the total size of all data included in the request to the cluster to commit the transaction.) Large transactions can cause performance problems on FoundationDB clusters, so setting this limit to a smaller value than the default can help prevent the client from accidentally degrading the cluster's performance. This value must be at least 32 and cannot be set to higher than 10,000,000, the default transaction size limit.

..  |option-set-timeout-blurb1| replace::

    Set a timeout duration in milliseconds after which the transaction automatically to be cancelled. The time is measured from transaction creation (or the most call to |reset-func-name|, if any). Valid parameter values are [0, INT_MAX]. If set to 0, all timeouts will be disabled. Once a transaction has timed out, all pending or future uses of the transaction will |error-raise-type| a :ref:`transaction_timed_out <developer-guide-error-codes>` |error-type|. The transaction can be used again after it is |reset-func-name|.

.. |option-set-timeout-blurb2| replace::

    Timeouts employ transaction cancellation, so you should note the issues raised by |cancel-func| when using timeouts.

.. |option-set-timeout-blurb3| replace::

    Prior to API version 610, like all other transaction options, a timeout must be reset after a call to |on-error-func|. Note that resetting this option resets only the timeout *duration*, not the starting point from which the time is measured. If the API version is 610 or newer, then the timeout is not reset. This allows the user to specify a timeout for specific transactions that is longer than the timeout specified by |timeout-database-option|. Note that at all API versions, it is safe and legal to call this option after each call to |on-error-func|, so most code written assuming the older behavior can be upgraded without requiring any modification. This also means that there is no need to introduce logic to conditionally set this option within retry loops. One can set the default timeout for all transactions by calling |timeout-database-option|.

.. |option-next-write-no-write-conflict-range-blurb| replace::

    The next write performed on this transaction will not generate a write conflict range. As a result, other transactions which read the key(s) being modified by the next write will not necessarily conflict with this transaction.

.. |option-next-write-no-write-conflict-range-note| replace::

    Care needs to be taken when using this option on a transaction that is shared between multiple threads. When setting this option, write conflict ranges will be disabled on the next write operation, regardless of what thread it is on.

..  |option-set-transaction-logging-max-field-length-blurb| replace::

    Sets the maximum escaped length of key and value fields to be logged to the trace file via the LOG_TRANSACTION option, after which the field will be truncated. A negative value disables truncation. One can set the default max field length for all transactions by calling |transaction-logging-max-field-length-database-option|.

..  |option-set-debug-transaction-identifier| replace::

    Sets a client provided string identifier for the transaction that will be used in scenarios like tracing or profiling. Client trace logging or transaction profiling must be separately enabled.

..  |option-set-log-transaction| replace::

    Enables tracing for this transaction and logs results to the client trace logs. The DEBUG_TRANSACTION_IDENTIFIER option must be set before using this option, and client trace logging must be enabled to get log output.

.. |future-blurb1| replace::
    Many FoundationDB API functions return "future" objects. A brief overview of futures is included in the :doc:`class scheduling tutorial <class-scheduling>`. Most future objects behave just like a normal object, but block when you use them for the first time if the asynchronous function which returned the future has not yet completed its action. A future object is considered ready when either a value is available, or when an error has occurred.

.. |future-cancel-blurb| replace::

    Cancels |future-type-string| and its associated asynchronous operation. If called before the future is ready, attempts to access its value will |error-raise-type| an :ref:`operation_cancelled <developer-guide-error-codes>` |error-type|. Cancelling a future which is already ready has no effect. Note that even if a future is not ready, its associated asynchronous operation may have successfully completed and be unable to be cancelled.

.. |fdb-open-blurb1| replace::
    Connects to the cluster specified by the :ref:`cluster file <foundationdb-cluster-file>`. This function is often called without any parameters, using only the defaults. If no cluster file is passed, FoundationDB automatically :ref:`determines a cluster file <specifying-a-cluster-file>` with which to connect to a cluster.

.. |fdb-open-blurb2| replace::
	A single client can use this function multiple times to connect to different clusters simultaneously, with each invocation requiring its own cluster file. To connect to multiple clusters running at different, incompatible versions, the :ref:`multi-version client API <multi-version-client-api>` must be used.

.. |fdb-transactional-unknown-result-note| replace::
    In some failure scenarios, it is possible that your transaction will be executed twice. See :ref:`developer-guide-unknown-results` for more information.

.. |db-attribute-blurb| replace::
    The |database-class| that this transaction is interacting with.

.. |database-get-key-caching-blurb| replace::
    The key is cached, providing a potential performance benefit. However, the
    value of the key is also retrieved, using network bandwidth.

.. |transaction-get-key-caching-blurb| replace::
    By default, the key is cached for the duration of the transaction, providing
    a potential performance benefit. However, the value of the key is also retrieved,
    using network bandwidth. Invoking |read-your-writes-disable-option| will avoid
    both the caching and the increased network bandwidth.

.. |network-cannot-be-restarted-blurb| replace::
    Once the network is stopped it cannot be restarted during the lifetime of the running program.

.. |fdb-careful-with-callbacks-blurb| replace::
    There are a number of requirements and constraints to be aware of when using callbacks with FoundationDB. Please read :ref:`developer-guide-programming-with-futures`.

.. |subspace-blurb1| replace::
    Subspaces provide a convenient way to use the |tuple-layer| to define namespaces for different categories of data. The namespace is specified by a prefix tuple which is prepended to all tuples packed by the subspace. When unpacking a key with the subspace, the prefix tuple will be removed from the result.

.. |subspace-blurb2| replace::
    As a best practice, API clients should use at least one subspace for application data.

.. |subspace-blurb3| replace::
    Creates a subspace with the specified prefix tuple. If the raw prefix byte string is specified, then it will be prepended to all packed keys. Likewise, the raw prefix will be removed from all unpacked keys.

.. |subspace-key-blurb| replace::
    Returns the key encoding the prefix used for the subspace. This is equivalent to packing the empty tuple.

.. |subspace-pack-blurb| replace::
    Returns the key encoding the specified tuple in the subspace. For example, if you have a subspace with prefix tuple ``('users')`` and you use it to pack the tuple ``('Smith')``, the result is the same as if you packed the tuple ``('users', 'Smith')`` with the |tuple-layer|.

.. |subspace-unpack-blurb| replace::
    Returns the tuple encoded by the given key, with the subspace's prefix tuple and raw prefix removed.

.. |subspace-range-blurb| replace::
    Returns a range representing all keys in the subspace that encode tuples strictly starting with the specified tuple.

.. |subspace-contains-blurb| replace::
    Returns true if ``key`` starts with |key-meth|, indicating that the subspace logically contains ``key``.

.. |subspace-as-foundationdb-key-blurb| replace::
    Returns the key encoding the prefix used for the subspace, like |key-meth|.

.. |subspace-subspace-blurb| replace::
    Returns a new subspace which is equivalent to this subspace with its prefix tuple extended by the specified tuple.

.. |directory-blurb1| replace::
    The FoundationDB API provides :ref:`directories <developer-guide-directories>` as a tool for managing related |subspace-api|. Directories are a recommended approach for administering applications. Each application should create or open at least one directory to manage its subspaces.

.. |directory-blurb2| replace::
    Directories are identified by hierarchical paths analogous to the paths in a Unix-like file system. A path is represented as |dir-path-type| of strings. Each directory has an associated subspace used to store its content. The directory layer maps each path to a short prefix used for the corresponding subspace. In effect, directories provide a level of indirection for access to subspaces.

.. |directory-blurb3| replace::
    Except where noted, directory methods interpret the provided path(s) relative to the path of the directory object. When opening a directory, a byte string ``layer`` option may be specified as a metadata identifier.

.. |directory-layer-blurb| replace::
    Each instance defines a new root directory. The subspaces |node-subspace| and |content-subspace| control where the directory metadata and contents, respectively, are stored. The default root directory has a |node-subspace| with raw prefix ``\xFE`` and a |content-subspace| with no prefix. Specifying more restrictive values for |node-subspace| and |content-subspace| will allow using the directory layer alongside other content in a database. If |allow-manual-prefixes| is false, attempts to create a directory with a manual prefix under the directory layer will |error-raise-type| an |error-type|. The default root directory does not allow manual prefixes.

.. |directory-create-or-open-blurb| replace::
    Opens the directory with ``path`` specified as |dir-path-type| of strings. ``path`` can also be a string, in which case it will be automatically wrapped in |dir-path-type|. All string values in a path will be converted to unicode. If the directory does not exist, it is created (creating parent directories if necessary).

.. |directory-create-or-open-return-blurb| replace::
    Returns the directory and its contents as a |directory-subspace|.

.. |directory-open-blurb| replace::
    Opens the directory with ``path`` specified as |dir-path-type| of strings. ``path`` can also be a string, in which case it will be automatically wrapped in |dir-path-type|. All string values in a path will be converted to unicode. The method will |error-raise-type| an |error-type| if the directory does not exist.

.. |directory-create-blurb| replace::
    Creates a directory with ``path`` specified as |dir-path-type| of strings. ``path`` can also be a string, in which case it will be automatically wrapped in |dir-path-type|. All string values in a path will be converted to unicode. Parent directories are created if necessary. The method will |error-raise-type| an |error-type| if the given directory already exists.

.. |directory-move-blurb| replace::
    Moves the directory at ``old_path`` to ``new_path``. There is no effect on the physical prefix of the given directory or on clients that already have the directory open. The method will |error-raise-type| an |error-type| if a directory does not exist at ``old_path``, a directory already exists at ``new_path``, or the parent directory of ``new_path`` does not exist.

.. |directory-move-return-blurb| replace::
    Returns the directory at its new location as a |directory-subspace|.

.. |directory-remove-blurb| replace::
    Removes the directory at ``path``, its contents, and all subdirectories. The method will |error-raise-type| an |error-type| if the directory does not exist.

.. |directory-remove-warning| replace::
    Clients that have already opened the directory might still insert data into its contents after removal.

.. |directory-remove-if-exists-blurb| replace::
    Checks if the directory at ``path`` exists and, if so, removes the directory, its contents, and all subdirectories. Returns ``true`` if the directory existed and ``false`` otherwise.

.. |directory-exists-blurb| replace::
    Returns ``true`` if the directory at ``path`` exists and ``false`` otherwise.

.. |directory-get-layer-blurb| replace::
    Returns the layer specified when the directory was created.

.. |directory-get-path-blurb| replace::
    Returns the path with which the directory was opened.

.. |directory-subspace-blurb| replace::
    A directory subspace represents a specific directory and its contents. It stores the ``path`` with which it was opened and supports all |directory-layer| methods for operating on itself and its subdirectories. It also implements all |subspace| methods for working with the contents of that directory.

.. |directory-move-to-blurb| replace::
    Moves this directory to ``new_path``, interpreting ``new_path`` absolutely. There is no effect on the physical prefix of the given directory or on clients that already have the directory open. The method will |error-raise-type| an |error-type| if a directory already exists at ``new_path`` or the parent directory of ``new_path`` does not exist.

.. |locality-api-blurb| replace::
    The FoundationDB API comes with a set of functions for discovering the storage locations of keys within your cluster. This information can be useful for advanced users who wish to take into account the location of keys in the design of applications or processes.

.. |locality-get-boundary-keys-db-or-tr| replace::
    The first parameter to this function may be either a |database-class| or a |transaction-class|. If it is passed a |transaction-class|, the transaction will not be committed, reset, or modified in any way, nor will its transaction options (such as retry limit) be applied within the function. However, if the database is unavailable prior to the function call, any timeout set on the transaction will still trigger.

.. |locality-get-boundary-keys-blurb| replace::

    Returns a |lazy-iterator-object| of keys ``k`` such that ``begin <= k < end`` and ``k`` is located at the start of a contiguous range stored on a single server.

.. |locality-get-boundary-keys-warning-danger| replace::

    This method is not transactional. It will return an answer no older than the Transaction or Database object it is passed, but the returned boundaries are an estimate and may not represent the exact boundary locations at any database version.

.. |locality-get-addresses-for-key-blurb| replace::

    Returns a list of public network addresses as strings, one for each of the storage servers responsible for storing ``key`` and its associated value.

.. |option-knob| replace::
   
   Sets internal tuning or debugging knobs. The argument to this function should be a string representing the knob name and the value, e.g. "transaction_size_limit=1000".

.. |option-tls-verify-peers| replace::

   Sets the peer certificate field verification criteria. 

.. |option-tls-ca-bytes| replace::
    
   Sets the certificate authority bundle.

.. |option-tls-ca-path| replace::

   Sets the file from which to load the certificate authority bundle.

.. |option-tls-password| replace::

    Sets the passphrase for encrypted private key. Password should be set before setting the key for the password to be used.

.. |option-tls-disable-plaintext-connection| replace::

    Disable non-TLS connections from the client, allowing only TLS connections. Plaintext connections will timeout.

.. |option-set-disable-local-client| replace::

    Prevents connections through the local client, allowing only connections through externally loaded client libraries.

.. |option-set-client-threads-per-version| replace::
   
    Spawns multiple worker threads for each version of the client that is loaded.  Setting this to a number greater than one implies disable_local_client.

.. |option-disable-client-statistics-logging| replace::

    Disables logging of client statistics, such as sampled transaction activity.
       
.. |option-enable-run-loop-profiling| replace::

    Enables debugging feature to perform run loop profiling. Requires trace logging to be enabled. WARNING: this feature is not recommended for use in production.

.. |option-set-distributed-client-tracer| replace::

    Sets a tracer to run on the client. Should be set to the same value as the tracer set on the server.
