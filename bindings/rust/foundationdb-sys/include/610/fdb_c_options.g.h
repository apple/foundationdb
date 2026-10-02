#ifndef FDB_C_OPTIONS_G_H
#define FDB_C_OPTIONS_G_H
#pragma once

/*
 * FoundationDB C API
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Do not include this file directly.
 */

typedef enum {
    // Deprecated
    // Parameter: (String) IP:PORT
    FDB_NET_OPTION_LOCAL_ADDRESS=10,

    // Deprecated
    // Parameter: (String) path to cluster file
    FDB_NET_OPTION_CLUSTER_FILE=20,

    // Enables trace output to a file in a directory of the clients choosing
    // Parameter: (String) path to output directory (or NULL for current working directory)
    FDB_NET_OPTION_TRACE_ENABLE=30,

    // Sets the maximum size in bytes of a single trace output file. This value should be in the range ``[0, INT64_MAX]``. If the value is set to 0, there is no limit on individual file size. The default is a maximum size of 10,485,760 bytes.
    // Parameter: (Int) max size of a single trace output file
    FDB_NET_OPTION_TRACE_ROLL_SIZE=31,

    // Sets the maximum size of all the trace output files put together. This value should be in the range ``[0, INT64_MAX]``. If the value is set to 0, there is no limit on the total size of the files. The default is a maximum size of 104,857,600 bytes. If the default roll size is used, this means that a maximum of 10 trace files will be written at a time.
    // Parameter: (Int) max total size of trace files
    FDB_NET_OPTION_TRACE_MAX_LOGS_SIZE=32,

    // Sets the 'LogGroup' attribute with the specified value for all events in the trace output files. The default log group is 'default'.
    // Parameter: (String) value of the LogGroup attribute
    FDB_NET_OPTION_TRACE_LOG_GROUP=33,

    // Select the format of the log files. xml (the default) and json are supported.
    // Parameter: (String) Format of trace files
    FDB_NET_OPTION_TRACE_FORMAT=34,

    // Set internal tuning or debugging knobs
    // Parameter: (String) knob_name=knob_value
    FDB_NET_OPTION_KNOB=40,

    // Deprecated
    // Parameter: (String) file path or linker-resolved name
    FDB_NET_OPTION_TLS_PLUGIN=41,

    // Set the certificate chain
    // Parameter: (Bytes) certificates
    FDB_NET_OPTION_TLS_CERT_BYTES=42,

    // Set the file from which to load the certificate chain
    // Parameter: (String) file path
    FDB_NET_OPTION_TLS_CERT_PATH=43,

    // Set the private key corresponding to your own certificate
    // Parameter: (Bytes) key
    FDB_NET_OPTION_TLS_KEY_BYTES=45,

    // Set the file from which to load the private key corresponding to your own certificate
    // Parameter: (String) file path
    FDB_NET_OPTION_TLS_KEY_PATH=46,

    // Set the peer certificate field verification criteria
    // Parameter: (Bytes) verification pattern
    FDB_NET_OPTION_TLS_VERIFY_PEERS=47,

    // 
    // Parameter: Option takes no parameter
    FDB_NET_OPTION_BUGGIFY_ENABLE=48,

    // 
    // Parameter: Option takes no parameter
    FDB_NET_OPTION_BUGGIFY_DISABLE=49,

    // Set the probability of a BUGGIFY section being active for the current execution.  Only applies to code paths first traversed AFTER this option is changed.
    // Parameter: (Int) probability expressed as a percentage between 0 and 100
    FDB_NET_OPTION_BUGGIFY_SECTION_ACTIVATED_PROBABILITY=50,

    // Set the probability of an active BUGGIFY section being fired
    // Parameter: (Int) probability expressed as a percentage between 0 and 100
    FDB_NET_OPTION_BUGGIFY_SECTION_FIRED_PROBABILITY=51,

    // Set the ca bundle
    // Parameter: (Bytes) ca bundle
    FDB_NET_OPTION_TLS_CA_BYTES=52,

    // Set the file from which to load the certificate authority bundle
    // Parameter: (String) file path
    FDB_NET_OPTION_TLS_CA_PATH=53,

    // Set the passphrase for encrypted private key. Password should be set before setting the key for the password to be used.
    // Parameter: (String) key passphrase
    FDB_NET_OPTION_TLS_PASSWORD=54,

    // Disables the multi-version client API and instead uses the local client directly. Must be set before setting up the network.
    // Parameter: Option takes no parameter
    FDB_NET_OPTION_DISABLE_MULTI_VERSION_CLIENT_API=60,

    // If set, callbacks from external client libraries can be called from threads created by the FoundationDB client library. Otherwise, callbacks will be called from either the thread used to add the callback or the network thread. Setting this option can improve performance when connected using an external client, but may not be safe to use in all environments. Must be set before setting up the network. WARNING: This feature is considered experimental at this time.
    // Parameter: Option takes no parameter
    FDB_NET_OPTION_CALLBACKS_ON_EXTERNAL_THREADS=61,

    // Adds an external client library for use by the multi-version client API. Must be set before setting up the network.
    // Parameter: (String) path to client library
    FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY=62,

    // Searches the specified path for dynamic libraries and adds them to the list of client libraries for use by the multi-version client API. Must be set before setting up the network.
    // Parameter: (String) path to directory containing client libraries
    FDB_NET_OPTION_EXTERNAL_CLIENT_DIRECTORY=63,

    // Prevents connections through the local client, allowing only connections through externally loaded client libraries. Intended primarily for testing.
    // Parameter: Option takes no parameter
    FDB_NET_OPTION_DISABLE_LOCAL_CLIENT=64,

    // Disables logging of client statistics, such as sampled transaction activity.
    // Parameter: Option takes no parameter
    FDB_NET_OPTION_DISABLE_CLIENT_STATISTICS_LOGGING=70,

    // Enables debugging feature to perform slow task profiling. Requires trace logging to be enabled. WARNING: this feature is not recommended for use in production.
    // Parameter: Option takes no parameter
    FDB_NET_OPTION_ENABLE_SLOW_TASK_PROFILING=71
} FDBNetworkOption;

typedef enum {
    // Set the size of the client location cache. Raising this value can boost performance in very large databases where clients access data in a near-random pattern. Defaults to 100000.
    // Parameter: (Int) Max location cache entries
    FDB_DB_OPTION_LOCATION_CACHE_SIZE=10,

    // Set the maximum number of watches allowed to be outstanding on a database connection. Increasing this number could result in increased resource usage. Reducing this number will not cancel any outstanding watches. Defaults to 10000 and cannot be larger than 1000000.
    // Parameter: (Int) Max outstanding watches
    FDB_DB_OPTION_MAX_WATCHES=20,

    // Specify the machine ID that was passed to fdbserver processes running on the same machine as this client, for better location-aware load balancing.
    // Parameter: (String) Hexadecimal ID
    FDB_DB_OPTION_MACHINE_ID=21,

    // Specify the datacenter ID that was passed to fdbserver processes running in the same datacenter as this client, for better location-aware load balancing.
    // Parameter: (String) Hexadecimal ID
    FDB_DB_OPTION_DATACENTER_ID=22,

    // Set a timeout in milliseconds which, when elapsed, will cause each transaction automatically to be cancelled. This sets the ``timeout`` option of each transaction created by this database. See the transaction option description for more information. Using this option requires that the API version is 610 or higher.
    // Parameter: (Int) value in milliseconds of timeout
    FDB_DB_OPTION_TRANSACTION_TIMEOUT=500,

    // Set a timeout in milliseconds which, when elapsed, will cause a transaction automatically to be cancelled. This sets the ``retry_limit`` option of each transaction created by this database. See the transaction option description for more information.
    // Parameter: (Int) number of times to retry
    FDB_DB_OPTION_TRANSACTION_RETRY_LIMIT=501,

    // Set the maximum amount of backoff delay incurred in the call to ``onError`` if the error is retryable. This sets the ``max_retry_delay`` option of each transaction created by this database. See the transaction option description for more information.
    // Parameter: (Int) value in milliseconds of maximum delay
    FDB_DB_OPTION_TRANSACTION_MAX_RETRY_DELAY=502,

    // Snapshot read operations will see the results of writes done in the same transaction. This is the default behavior.
    // Parameter: Option takes no parameter
    FDB_DB_OPTION_SNAPSHOT_RYW_ENABLE=26,

    // Snapshot read operations will not see the results of writes done in the same transaction. This was the default behavior prior to API version 300.
    // Parameter: Option takes no parameter
    FDB_DB_OPTION_SNAPSHOT_RYW_DISABLE=27
} FDBDatabaseOption;

typedef enum {
    // The transaction, if not self-conflicting, may be committed a second time after commit succeeds, in the event of a fault
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_CAUSAL_WRITE_RISKY=10,

    // The read version will be committed, and usually will be the latest committed, but might not be the latest committed in the event of a fault or partition
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_CAUSAL_READ_RISKY=20,

    // 
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_CAUSAL_READ_DISABLE=21,

    // The next write performed on this transaction will not generate a write conflict range. As a result, other transactions which read the key(s) being modified by the next write will not conflict with this transaction. Care needs to be taken when using this option on a transaction that is shared between multiple threads. When setting this option, write conflict ranges will be disabled on the next write operation, regardless of what thread it is on.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE=30,

    // Reads performed by a transaction will not see any prior mutations that occured in that transaction, instead seeing the value which was in the database at the transaction's read version. This option may provide a small performance benefit for the client, but also disables a number of client-side optimizations which are beneficial for transactions which tend to read and write the same keys within a single transaction.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE=51,

    // Deprecated
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_READ_AHEAD_DISABLE=52,

    // 
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_DURABILITY_DATACENTER=110,

    // 
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_DURABILITY_RISKY=120,

    // Deprecated
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_DURABILITY_DEV_NULL_IS_WEB_SCALE=130,

    // Specifies that this transaction should be treated as highest priority and that lower priority transactions should block behind this one. Use is discouraged outside of low-level tools
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_PRIORITY_SYSTEM_IMMEDIATE=200,

    // Specifies that this transaction should be treated as low priority and that default priority transactions will be processed first. Batch priority transactions will also be throttled at load levels smaller than for other types of transactions and may be fully cut off in the event of machine failures. Useful for doing batch work simultaneously with latency-sensitive work
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_PRIORITY_BATCH=201,

    // This is a write-only transaction which sets the initial configuration. This option is designed for use by database system tools only.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_INITIALIZE_NEW_DATABASE=300,

    // Allows this transaction to read and modify system keys (those that start with the byte 0xFF)
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_ACCESS_SYSTEM_KEYS=301,

    // Allows this transaction to read system keys (those that start with the byte 0xFF)
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_READ_SYSTEM_KEYS=302,

    // 
    // Parameter: (String) Optional transaction name
    FDB_TR_OPTION_DEBUG_RETRY_LOGGING=401,

    // Deprecated
    // Parameter: (String) String identifier to be used in the logs when tracing this transaction. The identifier must not exceed 100 characters.
    FDB_TR_OPTION_TRANSACTION_LOGGING_ENABLE=402,

    // Sets a client provided identifier for the transaction that will be used in scenarios like tracing or profiling. Client trace logging or transaction profiling must be separately enabled.
    // Parameter: (String) String identifier to be used when tracing or profiling this transaction. The identifier must not exceed 100 characters.
    FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER=403,

    // Enables tracing for this transaction and logs results to the client trace logs. The DEBUG_TRANSACTION_IDENTIFIER option must be set before using this option, and client trace logging must be enabled and to get log output.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_LOG_TRANSACTION=404,

    // Set a timeout in milliseconds which, when elapsed, will cause the transaction automatically to be cancelled. Valid parameter values are ``[0, INT_MAX]``. If set to 0, will disable all timeouts. All pending and any future uses of the transaction will throw an exception. The transaction can be used again after it is reset. Prior to API version 610, like all other transaction options, the timeout must be reset after a call to ``onError``. If the API version is 610 or greater, the timeout is not reset after an ``onError`` call. This allows the user to specify a longer timeout on specific transactions than the default timeout specified through the ``transaction_timeout`` database option without the shorter database timeout cancelling transactions that encounter a retryable error. Note that at all API versions, it is safe and legal to set the timeout each time the transaction begins, so most code written assuming the older behavior can be upgraded to the newer behavior without requiring any modification, and the caller is not required to implement special logic in retry loops to only conditionally set this option.
    // Parameter: (Int) value in milliseconds of timeout
    FDB_TR_OPTION_TIMEOUT=500,

    // Set a maximum number of retries after which additional calls to ``onError`` will throw the most recently seen error code. Valid parameter values are ``[-1, INT_MAX]``. If set to -1, will disable the retry limit. Prior to API version 610, like all other transaction options, the retry limit must be reset after a call to ``onError``. If the API version is 610 or greater, the retry limit is not reset after an ``onError`` call. Note that at all API versions, it is safe and legal to set the retry limit each time the transaction begins, so most code written assuming the older behavior can be upgraded to the newer behavior without requiring any modification, and the caller is not required to implement special logic in retry loops to only conditionally set this option.
    // Parameter: (Int) number of times to retry
    FDB_TR_OPTION_RETRY_LIMIT=501,

    // Set the maximum amount of backoff delay incurred in the call to ``onError`` if the error is retryable. Defaults to 1000 ms. Valid parameter values are ``[0, INT_MAX]``. If the maximum retry delay is less than the current retry delay of the transaction, then the current retry delay will be clamped to the maximum retry delay. Prior to API version 610, like all other transaction options, the maximum retry delay must be reset after a call to ``onError``. If the API version is 610 or greater, the retry limit is not reset after an ``onError`` call. Note that at all API versions, it is safe and legal to set the maximum retry delay each time the transaction begins, so most code written assuming the older behavior can be upgraded to the newer behavior without requiring any modification, and the caller is not required to implement special logic in retry loops to only conditionally set this option.
    // Parameter: (Int) value in milliseconds of maximum delay
    FDB_TR_OPTION_MAX_RETRY_DELAY=502,

    // Snapshot read operations will see the results of writes done in the same transaction. This is the default behavior.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE=600,

    // Snapshot read operations will not see the results of writes done in the same transaction. This was the default behavior prior to API version 300.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE=601,

    // The transaction can read and write to locked databases, and is resposible for checking that it took the lock.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_LOCK_AWARE=700,

    // By default, operations that are performed on a transaction while it is being committed will not only fail themselves, but they will attempt to fail other in-flight operations (such as the commit) as well. This behavior is intended to help developers discover situations where operations could be unintentionally executed after the transaction has been reset. Setting this option removes that protection, causing only the offending operation to fail.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_USED_DURING_COMMIT_PROTECTION_DISABLE=701,

    // The transaction can read from locked databases.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_READ_LOCK_AWARE=702,

    // This option should only be used by tools which change the database configuration.
    // Parameter: Option takes no parameter
    FDB_TR_OPTION_USE_PROVISIONAL_PROXIES=711
} FDBTransactionOption;

typedef enum {
    // Client intends to consume the entire range and would like it all transferred as early as possible.
    FDB_STREAMING_MODE_WANT_ALL=-2,

    // The default. The client doesn't know how much of the range it is likely to used and wants different performance concerns to be balanced. Only a small portion of data is transferred to the client initially (in order to minimize costs if the client doesn't read the entire range), and as the caller iterates over more items in the range larger batches will be transferred in order to minimize latency.
    FDB_STREAMING_MODE_ITERATOR=-1,

    // Infrequently used. The client has passed a specific row limit and wants that many rows delivered in a single batch. Because of iterator operation in client drivers make request batches transparent to the user, consider ``WANT_ALL`` StreamingMode instead. A row limit must be specified if this mode is used.
    FDB_STREAMING_MODE_EXACT=0,

    // Infrequently used. Transfer data in batches small enough to not be much more expensive than reading individual rows, to minimize cost if iteration stops early.
    FDB_STREAMING_MODE_SMALL=1,

    // Infrequently used. Transfer data in batches sized in between small and large.
    FDB_STREAMING_MODE_MEDIUM=2,

    // Infrequently used. Transfer data in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible. If the client stops iteration early, some disk and network bandwidth may be wasted. The batch size may still be too small to allow a single client to get high throughput from the database, so if that is what you need consider the SERIAL StreamingMode.
    FDB_STREAMING_MODE_LARGE=3,

    // Transfer data in batches large enough that an individual client can get reasonable read bandwidth from the database. If the client stops iteration early, considerable disk and network bandwidth may be wasted.
    FDB_STREAMING_MODE_SERIAL=4
} FDBStreamingMode;

typedef enum {
    // Performs an addition of little-endian integers. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``. The integers to be added must be stored in a little-endian representation.  They can be signed in two's complement representation or unsigned. You can add to an integer at a known offset in the value by prepending the appropriate number of zero bytes to ``param`` and padding with zero bytes to match the length of the value. However, this offset technique requires that you know the addition will not cause the integer field within the value to overflow.
    FDB_MUTATION_TYPE_ADD=2,

    // Deprecated
    FDB_MUTATION_TYPE_AND=6,

    // Performs a bitwise ``and`` operation.  If the existing value in the database is not present, then ``param`` is stored in the database. If the existing value in the database is shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.
    FDB_MUTATION_TYPE_BIT_AND=6,

    // Deprecated
    FDB_MUTATION_TYPE_OR=7,

    // Performs a bitwise ``or`` operation.  If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.
    FDB_MUTATION_TYPE_BIT_OR=7,

    // Deprecated
    FDB_MUTATION_TYPE_XOR=8,

    // Performs a bitwise ``xor`` operation.  If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.
    FDB_MUTATION_TYPE_BIT_XOR=8,

    // Appends ``param`` to the end of the existing value already in the database at the given key (or creates the key and sets the value to ``param`` if the key is empty). This will only append the value if the final concatenated value size is less than or equal to the maximum value size (i.e., if it fits). WARNING: No error is surfaced back to the user if the final value is too large because the mutation will not be applied until after the transaction has been committed. Therefore, it is only safe to use this mutation type if one can guarantee that one will keep the total value size under the maximum size.
    FDB_MUTATION_TYPE_APPEND_IF_FITS=9,

    // Performs a little-endian comparison of byte strings. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``. The larger of the two values is then stored in the database.
    FDB_MUTATION_TYPE_MAX=12,

    // Performs a little-endian comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored in the database. If the existing value in the database is shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``. The smaller of the two values is then stored in the database.
    FDB_MUTATION_TYPE_MIN=13,

    // Transforms ``key`` using a versionstamp for the transaction. Sets the transformed key in the database to ``param``. The key is transformed by removing the final four bytes from the key and reading those as a little-Endian 32-bit integer to get a position ``pos``. The 10 bytes of the key from ``pos`` to ``pos + 10`` are replaced with the versionstamp of the transaction used. The first byte of the key is position 0. A versionstamp is a 10 byte, unique, monotonically (but not sequentially) increasing value for each committed transaction. The first 8 bytes are the committed version of the database (serialized in big-Endian order). The last 2 bytes are monotonic in the serialization order for transactions. WARNING: At this time, versionstamps are compatible with the Tuple layer only in the Java, Python, and Go bindings. Also, note that prior to API version 520, the offset was computed from only the final two bytes rather than the final four bytes.
    FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY=14,

    // Transforms ``param`` using a versionstamp for the transaction. Sets the ``key`` given to the transformed ``param``. The parameter is transformed by removing the final four bytes from ``param`` and reading those as a little-Endian 32-bit integer to get a position ``pos``. The 10 bytes of the parameter from ``pos`` to ``pos + 10`` are replaced with the versionstamp of the transaction used. The first byte of the parameter is position 0. A versionstamp is a 10 byte, unique, monotonically (but not sequentially) increasing value for each committed transaction. The first 8 bytes are the committed version of the database (serialized in big-Endian order). The last 2 bytes are monotonic in the serialization order for transactions. WARNING: At this time, versionstamps are compatible with the Tuple layer only in the Java, Python, and Go bindings. Also, note that prior to API version 520, the versionstamp was always placed at the beginning of the parameter rather than computing an offset.
    FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE=15,

    // Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored. Otherwise the smaller of the two values is then stored in the database.
    FDB_MUTATION_TYPE_BYTE_MIN=16,

    // Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored. Otherwise the larger of the two values is then stored in the database.
    FDB_MUTATION_TYPE_BYTE_MAX=17,

    // Performs an atomic ``compare and clear`` operation. If the existing value in the database is equal to the given value, then given key is cleared.
    FDB_MUTATION_TYPE_COMPARE_AND_CLEAR=20
} FDBMutationType;

typedef enum {
    // Used to add a read conflict range
    FDB_CONFLICT_RANGE_TYPE_READ=0,

    // Used to add a write conflict range
    FDB_CONFLICT_RANGE_TYPE_WRITE=1
} FDBConflictRangeType;

typedef enum {
    // Returns ``true`` if the error indicates the operations in the transactions should be retried because of transient error.
    FDB_ERROR_PREDICATE_RETRYABLE=50000,

    // Returns ``true`` if the error indicates the transaction may have succeeded, though not in a way the system can verify.
    FDB_ERROR_PREDICATE_MAYBE_COMMITTED=50001,

    // Returns ``true`` if the error indicates the transaction has not committed, though in a way that can be retried.
    FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED=50002
} FDBErrorPredicate;

#endif
