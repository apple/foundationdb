.. default-domain:: c
.. highlight:: c

.. Required substitutions for api-common.rst.inc

.. |database-type| replace:: ``FDBDatabase``
.. |database-class| replace:: :type:`FDBDatabase`
.. |database-auto| replace:: FIXME
.. |tenant-type| replace:: ``FDBTenant``
.. |transaction-class| replace:: FIXME
.. |get-key-func| replace:: :func:`fdb_transaction_get_key()`
.. |get-range-func| replace:: :func:`fdb_transaction_get_range()`
.. |commit-func| replace:: :func:`fdb_transaction_commit()`
.. |reset-func-name| replace:: :func:`reset <fdb_transaction_reset()>`
.. |reset-func| replace:: :func:`fdb_transaction_reset()`
.. |cancel-func| replace:: :func:`fdb_transaction_cancel()`
.. |open-func| replace:: FIXME
.. |set-cluster-file-func| replace:: FIXME
.. |set-local-address-func| replace:: FIXME
.. |on-error-func| replace:: :func:`fdb_transaction_on_error()`
.. |null-type| replace:: FIXME
.. |error-type| replace:: error
.. |error-raise-type| replace:: return
.. |future-cancel| replace:: :func:`fdb_future_cancel()`
.. |max-watches-database-option| replace:: the MAX_WATCHES :func:`database option <fdb_database_set_option>`
.. |future-type-string| replace:: an :type:`FDBFuture` object
.. |read-your-writes-disable-option| replace:: the READ_YOUR_WRITES_DISABLE :func:`transaction option <fdb_transaction_set_option>`
.. |lazy-iterator-object| replace:: FIXME
.. |key-meth| replace:: FIXME
.. |directory-subspace| replace:: FIXME
.. |directory-layer| replace:: FIXME
.. |subspace| replace:: FIXME
.. |subspace-api| replace:: FIXME
.. |as-foundationdb-key| replace:: FIXME
.. |as-foundationdb-value| replace:: FIXME
.. |tuple-layer| replace:: FIXME
.. |dir-path-type| replace:: FIXME
.. |node-subspace| replace:: FIXME
.. |content-subspace| replace:: FIXME
.. |allow-manual-prefixes| replace:: FIXME
.. |retry-limit-transaction-option| replace:: FIXME
.. |timeout-transaction-option| replace:: FIXME
.. |max-retry-delay-transaction-option| replace:: FIXME
.. |size-limit-transaction-option| replace:: FIXME
.. |snapshot-ryw-enable-transaction-option| replace:: FIXME
.. |snapshot-ryw-disable-transaction-option| replace:: FIXME
.. |snapshot-ryw-enable-database-option| replace:: FIXME
.. |snapshot-ryw-disable-database-option| replace:: FIXME
.. |retry-limit-database-option| replace:: FIXME
.. |max-retry-delay-database-option| replace:: FIXME
.. |transaction-size-limit-database-option| replace:: FIXME
.. |timeout-database-option| replace:: FIXME
.. |causal-read-risky-transaction-option| replace:: FIXME
.. |causal-read-risky-database-option| replace:: FIXME
.. |transaction-logging-max-field-length-database-option| replace:: FIXME
.. |transaction-logging-max-field-length-transaction-option| replace:: FIXME

.. include:: api-common.rst.inc

.. |future-warning| replace:: ``future`` must represent a result of the appropriate type (i.e. must have been returned by a function documented as returning this type), or the results are undefined.

.. |future-get-return1| replace:: Returns zero if ``future`` is ready and not in an error state, and a non-zero :ref:`error code <developer-guide-error-codes>` otherwise

.. |future-get-return2| replace:: (in which case the value of any out parameter is undefined)

.. |future-memory-mine| replace:: The memory referenced by the result is owned by the :type:`FDBFuture` object and will be valid until either ``fdb_future_destroy(future)`` or ``fdb_future_release_memory(future)`` is called.

.. |future-memory-yours1| replace:: This function may only be called once on a given :type:`FDBFuture` object, as it transfers ownership of the

.. |future-memory-yours2| replace:: to the caller. The caller is responsible for calling

.. |future-memory-yours3| replace:: when finished with the result.

.. |future-return0| replace:: Returns an :type:`FDBFuture` which will be set to

.. |future-return1| replace:: You must first wait for the :type:`FDBFuture` to be ready, check for errors,

.. |future-return2| replace:: and then destroy the :type:`FDBFuture` with :func:`fdb_future_destroy()`.

.. |future-returnvoid0| replace:: Returns an :type:`FDBFuture` representing an empty value

.. |future-returnvoid| replace:: |future-returnvoid0|. |future-return1| |future-return2|

.. |option-doc| replace:: Please see ``fdb_c_options.g.h`` for a definition of this type, along with documentation of its allowed values.

.. |option-parameter| replace:: If the given option is documented as taking a parameter, you must also pass a pointer to the parameter value and the parameter value's length. If the option is documented as taking an ``Int`` parameter, ``value`` must point to a signed 64-bit integer (little-endian), and ``value_length`` must be 8. This memory only needs to be valid until

.. |no-null| replace:: The value does not need to be NULL-terminated.

.. |length-of| replace:: The length of the parameter specified by

.. |snapshot| replace:: Non-zero if this is a :ref:`snapshot read <snapshots>`.

.. |sets-and-clears1| replace:: Modify the database snapshot represented by ``transaction``

.. |sets-and-clears2| replace:: The modification affects the actual database only if ``transaction`` is later committed with :func:`fdb_transaction_commit()`.

=====
C API
=====

This API provides a very low-level interface to FoundationDB. It is primarily intended for use in implementing higher level APIs, rather than for direct use. If you are new to FoundationDB, you are probably better served by reading one of the other APIs first.

Installation
============

FoundationDB's C bindings are installed with the FoundationDB client binaries (see :ref:`installing-client-binaries`).

On Linux,
    | ``fdb_c.h``, ``fdb_c_types.h``, ``fdb_c_internal.h``, ``fdb_c_options.g.h``, ``fdb.options`` and ``fdb_c_shim.h`` are installed into ``/usr/include/foundationdb/``
    | ``libfdb_c.so`` and ``libfdb_c_shim.so`` is installed into ``/usr/lib/`` with the ``deb`` package and into  ``/usr/lib64/`` with the ``rpm`` package

On macOS,
    | ``fdb_c.h``, ``fdb_c_types.h``, ``fdb_c_internal.h``, ``fdb_c_options.g.h``, ``fdb.options`` and ``fdb_c_shim.h`` are installed into ``/usr/local/include/foundationdb/``
    | ``libfdb_c.dylib`` is installed into ``/usr/local/lib/``

Linking
=======

The FoundationDB C bindings are provided as a shared object which may be linked against at build time, or dynamically loaded at runtime. Any program that uses this API must be able to find a platform-appropriate shared library at runtime. Generally, this condition is best met by installing the FoundationDB client binaries (see :ref:`installing-client-binaries`) on any machine where the program will be run.

Linux
-----

When linking against ``libfdb_c.so``, you must also link against ``libm``, ``libpthread`` and ``librt``. These dependencies will be resolved by the dynamic linker when using this API via ``dlopen()`` or an FFI.

macOS
--------

When linking against ``libfdb_c.dylib``, no additional libraries are required.

API versioning
==============

Prior to including ``fdb_c.h``, you must define the ``FDB_API_VERSION`` macro. This, together with the :func:`fdb_select_api_version()` function, allows programs written against an older version of the API to compile and run with newer versions of the C library. The current version of the FoundationDB C API is |api-version|. ::

  #define FDB_API_VERSION 800
  #include <foundationdb/fdb_c.h>

.. function:: fdb_error_t fdb_select_api_version(int version)

   Must be called before any other API functions. ``version`` must be less than or equal to ``FDB_API_VERSION`` (and should almost always be equal).

   Language bindings implemented in C which themselves expose API versioning will usually pass the version requested by the application, instead of always passing ``FDB_API_VERSION``.

   Passing a version less than ``FDB_API_VERSION`` will cause the API to behave as it did in the older version.

   It is an error to call this function after it has returned successfully. It is not thread safe, and if called from more than one thread simultaneously its behavior is undefined.

   .. note:: This is actually implemented as a macro. If you are accessing this API via ``dlopen()`` or an FFI, you will need to use :func:`fdb_select_api_version_impl()`.

   .. warning:: |api-version-multi-version-warning|

.. function:: fdb_error_t fdb_select_api_version_impl(int runtime_version, int header_version)

   This is the actual entry point called by the :func:`fdb_select_api_version` macro. It should never be called directly from C, but if you are accessing this API via ``dlopen()`` or an FFI, you will need to use it. ``fdb_select_api_version(v)`` is equivalent to ``fdb_select_api_version_impl(v, FDB_API_VERSION)``.

   It is an error to call this function after it has returned successfully. It is not thread safe, and if called from more than one thread simultaneously its behavior is undefined.

   ``runtime_version``
      The version of run-time behavior the API is requested to provide. Must be less than or equal to ``header_version``, and should almost always be equal.

      Language bindings which themselves expose API versioning will usually pass the version requested by the application.

   ``header_version``
      The version of the ABI (application binary interface) that the calling code expects to find in the shared library. If you are using an FFI, this *must* correspond to the version of the API you are using as a reference (currently |api-version|). For example, the number of arguments that a function takes may be affected by this value, and an incorrect value is unlikely to yield success.

   .. warning:: |api-version-multi-version-warning|

.. function:: int fdb_get_max_api_version()

   Returns ``FDB_API_VERSION``, the current version of the FoundationDB C API.  This is the maximum version that may be passed to :func:`fdb_select_api_version()`.

Network
=======

The FoundationDB client library performs most tasks on a singleton thread (which usually will be a different thread than your application runs on). These functions are used to configure, start and stop the FoundationDB event loop on this thread.

.. function:: fdb_error_t fdb_network_set_option(FDBNetworkOption option, uint8_t const* value, int value_length)

   Called to set network options. |option-parameter| :func:`fdb_network_set_option()` returns.

.. type:: FDBNetworkOption

   |option-doc|

.. function:: fdb_error_t fdb_setup_network()

   Must be called after :func:`fdb_select_api_version()` (and zero or more calls to :func:`fdb_network_set_option()`) and before any other function in this API. :func:`fdb_setup_network()` can only be called once.

.. function:: fdb_error_t fdb_add_network_thread_completion_hook(void (*hook)(void*), void *hook_parameter)

	Must be called after :func:`fdb_setup_network()` and prior to :func:`fdb_run_network()` if called at all. This will register the given callback to run at the completion of the network thread. If there are multiple network threads running (which might occur if one is running multiple versions of the client, for example), then the callback is invoked once on each thread. When the supplied function is called, the supplied parameter is passed to it.

.. function:: fdb_error_t fdb_run_network()

   Must be called after :func:`fdb_setup_network()` before any asynchronous functions in this API can be expected to complete. Unless your program is entirely event-driven based on results of asynchronous functions in this API and has no event loop of its own, you will want to invoke this function on an auxiliary thread (which it is your responsibility to create).

   This function will not return until :func:`fdb_stop_network()` is called by you or a serious error occurs. It is not possible to run more than one network thread, and the network thread cannot be restarted once it has been stopped. This means that once ``fdb_run_network`` has been called, it is not legal to call it again for the lifetime of the running program.

.. function:: fdb_error_t fdb_stop_network()

   Signals the event loop invoked by :func:`fdb_run_network()` to terminate. You must call this function **and wait for** :func:`fdb_run_network()` **to return** before allowing your program to exit, or else the behavior is undefined. For example, when running :func:`fdb_run_network()` on a thread (using pthread), this will look like::

      pthread_t network_thread; /* handle for thread which invoked fdb_run_network() */
      int err;

      ...

      err = fdb_stop_network();
      if ( err ) {
          /* An error occurred (probably network not running) */
      }
      err = pthread_join( network_thread, NULL );
      if ( err ) {
          /* Unknown error */
      }
      exit(0);

   This function may be called from any thread. |network-cannot-be-restarted-blurb|

Future
======

Most functions in the FoundationDB API are asynchronous, meaning that they may return to the caller before actually delivering their result. These functions always return ``FDBFuture*``. An :type:`FDBFuture` object represents a result value or error to be delivered at some future time. You can wait for a Future to be "ready" -- to have a value or error delivered -- by setting a callback function, or by blocking a thread, or by polling. Once a Future is ready, you can extract either an error code or a value of the appropriate type (the documentation for the original function will tell you which ``fdb_future_get_()`` function you should call).

To use the API in a synchronous way, you would typically do something like this for each asynchronous call::

   // Call an API that returns FDBFuture*, documented as returning type foo in the future
   f = fdb_something();

   // Wait for the Future to be *ready*
   if ( (fdb_future_block_until_ready(f)) != 0 ) {
       // Exceptional error (e.g. out of memory)
   }

   if ( (err = fdb_future_get_foo(f, &result)) == 0 ) {
       // Use result
       // In some cases, you must be finished with result before calling
       // fdb_future_destroy() (see the documentation for the specific
       // fdb_future_get_*() method)
   } else {
       // Handle the error. If this is an error in a transaction, see
       // fdb_transaction_on_error()
   }

   fdb_future_destroy(f);

Futures make it easy to do multiple operations in parallel, by calling several asynchronous functions before waiting for any of the results. This can be important for reducing the latency of transactions.

See :ref:`developer-guide-programming-with-futures` for further (language-independent) discussion.

.. type:: FDBFuture

   An opaque type that represents a Future in the FoundationDB C API.

.. function:: void fdb_future_cancel(FDBFuture* future)

    |future-cancel-blurb|

.. function:: void fdb_future_destroy(FDBFuture* future)

   Destroys an :type:`FDBFuture` object. It must be called exactly once for each FDBFuture* returned by an API function. It may be called before or after the future is ready. It will also cancel the future (and its associated operation if the latter is still outstanding).

.. function:: fdb_error_t fdb_future_block_until_ready(FDBFuture* future)

   Blocks the calling thread until the given Future is ready. It will return success even if the Future is set to an error -- you must call :func:`fdb_future_get_error()` to determine that. :func:`fdb_future_block_until_ready()` will return an error only in exceptional conditions (e.g. deadlock detected, out of memory or other operating system resources).

   .. warning:: Never call this function from a callback passed to :func:`fdb_future_set_callback()`. This may block the thread on which :func:`fdb_run_network()` was invoked, resulting in a deadlock. In some cases the client can detect the deadlock and throw a ``blocked_from_network_thread`` error.

.. function:: fdb_bool_t fdb_future_is_ready(FDBFuture* future)

   Returns non-zero if the Future is ready. A Future is ready if it has been set to a value or an error.

.. function:: fdb_error_t fdb_future_set_callback(FDBFuture* future, FDBCallback callback, void* callback_parameter)

   Causes the :type:`FDBCallback` function to be invoked as ``callback(future, callback_parameter)`` when the given Future is ready. If the Future is already ready, the call may occur in the current thread before this function returns (but this behavior is not guaranteed). Alternatively, the call may be delayed indefinitely and take place on the thread on which :func:`fdb_run_network()` was invoked, and the callback is responsible for any necessary thread synchronization (and/or for posting work back to your application event loop, thread pool, etc. if your application's architecture calls for that).

   .. note:: This function guarantees the callback will be executed **at most once**.

   .. warning:: Never call :func:`fdb_future_block_until_ready()` from a callback passed to this function. This may block the thread on which :func:`fdb_run_network()` was invoked, resulting in a deadlock.

.. type:: FDBCallback

   A pointer to a function which takes ``FDBFuture*`` and ``void*`` and returns ``void``.

.. function:: void fdb_future_release_memory(FDBFuture* future)

   .. note:: This function provides no benefit to most application code. It is designed for use in writing generic, thread-safe language bindings. Applications should normally call :func:`fdb_future_destroy` only.

   This function may only be called after a successful (zero return value) call to :func:`fdb_future_get_key`, :func:`fdb_future_get_value`, or :func:`fdb_future_get_keyvalue_array`. It indicates that the memory returned by the prior get call is no longer needed by the application. After this function has been called the same number of times as ``fdb_future_get_*()``, further calls to ``fdb_future_get_*()`` will return a :ref:`future_released <developer-guide-error-codes>` error. It is still necessary to later destroy the future with :func:`fdb_future_destroy`.

   Calling this function is optional, since :func:`fdb_future_destroy` will also release the memory returned by get functions. However, :func:`fdb_future_release_memory` leaves the future object itself intact and provides a specific error code which can be used for coordination by multiple threads racing to do something with the results of a specific future. This has proven helpful in writing binding code.

.. function:: fdb_error_t fdb_future_get_error(FDBFuture* future)

   |future-get-return1|.

.. function:: fdb_error_t fdb_future_get_int64(FDBFuture* future, int64_t* out)

   Extracts a 64-bit integer from a pointer to :type:`FDBFuture` into a caller-provided variable of type ``int64_t``. |future-warning|

   |future-get-return1| |future-get-return2|.

.. function:: fdb_error_t fdb_future_get_double(FDBFuture* future, double* out)

   Extracts a double from a pointer to :type:`FDBFuture` into a caller-provided variable of type ``double``. |future-warning|

   |future-get-return1| |future-get-return2|.

.. function:: fdb_error_t fdb_future_get_key_array( FDBFuture* f, FDBKey const** out_key_array, int* out_count)

   Extracts an array of :type:`FDBKey` from an ``FDBFuture*`` into a caller-provided variable of type ``FDBKey*``. The size of the array will also be extracted and passed back by a caller-provided variable of type ``int`` |future-warning|

   |future-get-return1| |future-get-return2|.

.. function:: fdb_error_t fdb_future_get_key(FDBFuture* future, uint8_t const** out_key, int* out_key_length)

   Extracts a key from an :type:`FDBFuture` into caller-provided variables of type ``uint8_t*`` (a pointer to the beginning of the key) and ``int`` (the length of the key). |future-warning|

   |future-get-return1| |future-get-return2|.

   |future-memory-mine|

.. function:: fdb_error_t fdb_future_get_value(FDBFuture* future, fdb_bool_t* out_present, uint8_t const** out_value, int* out_value_length)

   Extracts a database value from an :type:`FDBFuture` into caller-provided variables. |future-warning|

   |future-get-return1| |future-get-return2|.

   ``*out_present``
      Set to non-zero if (and only if) the requested value was present in the database. (If zero, the other outputs are meaningless.)

   ``*out_value``
      Set to point to the first byte of the value.

   ``*out_value_length``
      Set to the length of the value (in bytes).

   |future-memory-mine|

.. function:: fdb_error_t fdb_future_get_string_array(FDBFuture* future, const char*** out_strings, int* out_count)

    Extracts an array of null-terminated C strings from an :type:`FDBFuture` into caller-provided variables. |future-warning|

    |future-get-return1| |future-get-return2|.

    ``*out_strings``
      Set to point to the first string in the array.

    ``*out_count``
      Set to the number of strings in the array.

    |future-memory-mine|

.. function:: fdb_error_t fdb_future_get_keyvalue_array(FDBFuture* future, FDBKeyValue const** out_kv, int* out_count, fdb_bool_t* out_more)

   Extracts an array of :type:`FDBKeyValue` objects from an :type:`FDBFuture` into caller-provided variables. |future-warning|

   |future-get-return1| |future-get-return2|.

   ``*out_kv``
      Set to point to the first :type:`FDBKeyValue` object in the array.

   ``*out_count``
      Set to the number of :type:`FDBKeyValue` objects in the array.

   ``*out_more``
      Set to true if (but not necessarily only if) values remain in the *key* range requested (possibly beyond the limits requested).

   |future-memory-mine|

.. type:: FDBKeyValue

   Represents a single key-value pair in the output of :func:`fdb_future_get_keyvalue_array`. ::

     typedef struct {
         const void* key;
         int         key_length;
         const void* value;
         int         value_length;
     } FDBKeyValue;

   ``key``
       A pointer to a key.

   ``key_length``
      The length of the key pointed to by ``key``.

   ``value``
      A pointer to a value.

   ``value_length``
      The length of the value pointed to by ``value``.

Database
========

An |database-blurb1| Modifications to a database are performed via transactions.

.. type:: FDBDatabase

   An opaque type that represents a database in the FoundationDB C API.

.. function:: fdb_error_t fdb_create_database(const char* cluster_file_path, FDBDatabase** out_database)

   Creates a new database connected the specified cluster. The caller assumes ownership of the :type:`FDBDatabase` object and must destroy it with :func:`fdb_database_destroy()`.

   |fdb-open-blurb2|

   ``cluster_file_path``
      A NULL-terminated string giving a local path of a :ref:`cluster file <foundationdb-cluster-file>` (often called 'fdb.cluster') which contains connection information for the FoundationDB cluster. If cluster_file_path is NULL or an empty string, then a :ref:`default cluster file <default-cluster-file>` will be used.

   ``*out_database``
      Set to point to the newly created :type:`FDBDatabase`.

.. function:: void fdb_database_destroy(FDBDatabase* database)

   Destroys an :type:`FDBDatabase` object. It must be called exactly once for each successful call to :func:`fdb_create_database()`. This function only destroys a handle to the database -- your database will be fine!

.. function:: fdb_error_t fdb_database_set_option(FDBDatabase* database, FDBDatabaseOption option, uint8_t const* value, int value_length)

   Called to set an option an on :type:`FDBDatabase`. |option-parameter| :func:`fdb_database_set_option()` returns.

.. type:: FDBDatabaseOption

   |option-doc|

.. function:: fdb_error_t fdb_database_open_tenant(FDBDatabase* database, uint8_t const* tenant_name, int tenant_name_length, FDBTenant** out_tenant)

   Opens a tenant on the given database. All transactions created by this tenant will operate on the tenant's key-space. The caller assumes ownership of the :type:`FDBTenant` object and must destroy it with :func:`fdb_tenant_destroy()`.

   ``tenant_name``
      The name of the tenant being accessed, as a byte string.
   ``tenant_name_length``
      The length of the tenant name byte string.
   ``*out_tenant``
      Set to point to the newly created :type:`FDBTenant`.

.. function:: fdb_error_t fdb_database_create_transaction(FDBDatabase* database, FDBTransaction** out_transaction)

   Creates a new transaction on the given database without using a tenant, meaning that it will operate on the entire database key-space. The caller assumes ownership of the :type:`FDBTransaction` object and must destroy it with :func:`fdb_transaction_destroy()`.

   ``*out_transaction``
      Set to point to the newly created :type:`FDBTransaction`.

.. function:: FDBFuture* fdb_database_reboot_worker(FDBDatabase* database, uint8_t const* address, int address_length, fdb_bool_t check, int duration)

   Reboot the specified process in the database.

   |future-return0| a :type:`int64_t` which represents whether the reboot request is sent or not. In particular, 1 means request sent and 0 means failure (e.g. the process with the specified address does not exist). |future-return1| call :func:`fdb_future_get_int64()` to extract the result, |future-return2|

   ``address``
        A pointer to the network address of the process.

   ``address_length``
        |length-of| ``address``.
   
   ``check``
        whether to perform a storage engine integrity check. In particular, the check-on-reboot is implemented by writing a check/validation file on disk as breadcrumb for the process to find after reboot, at which point it will eat the breadcrumb file and pass true to the integrityCheck parameter of the openKVStore() factory method.
   
   ``duration``
        If positive, the process will be first suspended for ``duration`` seconds before being rebooted.

.. function:: FDBFuture* fdb_database_force_recovery_with_data_loss(FDBDatabase* database, uint8_t const* dcId, int dcId_length)

   Force the database to recover into the given datacenter.

   This function is only useful in a fearless configuration where you want to recover your database even with losing recently committed mutations.
   
   In particular, the function will set usable_regions to 1 and the amount of mutations that will be lost depends on how far behind the remote datacenter is.
   
   The function will change the region configuration to have a positive priority for the chosen dcId, and a negative priority for all other dcIds.

   In particular, no error will be thrown if the given dcId does not exist. It will just not attempt to force a recovery.
   
   If the database has already recovered, the function does nothing. Thus it's safe to call it multiple times.

   |future-returnvoid|

.. function:: FDBFuture* fdb_database_create_snapshot(FDBDatabase* database, uint8_t const* snapshot_command, int snapshot_command_length)

   Create a snapshot of the database.

   ``uid``
         A UID used to create snapshot. A valid uid is a 32-length hex string, otherwise, it will fail with error_code_snap_invalid_uid_string.

         It is the user's responsibility to make sure the given ``uid`` is unique.
   
   ``uid_length``
         |length-of| ``uid``

   ``snapshot_command``
         A pointer to all the snapshot command arguments.

         In particular, if the original ``fdbcli`` command is ``snapshot <arg1> <arg2> <argN>``, then the string ``snapshot_command`` points to is ``<arg1> <arg2> <argN>``.
   
   ``snapshot_command_length``
         |length-of| ``snapshot_command``
   
   .. note:: The function is exposing the functionality of the fdbcli command ``snapshot``. Please take a look at the documentation before using (see :ref:`disk-snapshot-backups`).

.. function:: double fdb_database_get_main_thread_busyness(FDBDatabase* database)

   Returns a value where 0 indicates that the client is idle and 1 (or larger) indicates that the client is saturated. By default, this value is updated every second.

.. function:: FDBFuture* fdb_database_get_client_status(FDBDatabase* db)

   Returns a JSON string containing database client-side status information. If the Multi-version client API is disabled an empty string will be returned.
   At the top level the report describes the status of the Multi-Version Client database - its initialization state, the protocol version, the available client versions. The report schema is:

   .. code-block::

      {  "Healthy": <overall health status, true or false>,
         "InitializationState": <initializing|initialization_failed|created|incompatible|closed>,
         "InitializationError": <initialization error code, present if initialization failed>,
         "ProtocolVersion" : <determined protocol version of the cluster, present if determined>,
         "ConnectionRecord" : <connection file name or connection string>,
         "DatabaseStatus" : <Native Database status report, present if successfully retrieved>,
         "ErrorRetrievingDatabaseStatus" : <error code of retrieving status of the Native Database, present if failed>,
         "AvailableClients" : [
            { "ProtocolVersion" : <protocol version of the client>,
               "ReleaseVersion" : <release version of the client>,
               "ThreadIndex" : <the index of the client thread serving this database>
            },  ...
         ]
      }

   The status of the actual version-specific database is embedded within the ``DatabaseStatus`` attribute. It lists the addresses of various FDB 
   server roles the client is aware of and their connection status. The schema of the ``DatabaseStatus`` object is:
   
   .. code-block::

      {  "Healthy" : <overall health status: true or false>,
         "ClusterID" : <UUID>,
         "Coordinators" : [ <address>, ...  ],
         "CurrentCoordinator" : <address>
         "GrvProxies" : [ <address>, ...  ],
         "CommitProxies" : [ <address>, ... ],
         "StorageServers" : [ { "Address" : <address>, "SSID" : <Storage Server ID> }, ... ],
         "Connections" : [
         { "Address" : <address>,
            "Status" : <failed|connected|connecting|disconnected>,
            "Compatible" : <is protocol version compatible with the client>,
            "ConnectFailedCount" : <number of failed connection attempts>,
            "LastConnectTime" : <elapsed time in seconds since the last connection attempt>,
            "PingCount" : <total ping count>,
            "PingTimeoutCount" : <number of ping timeouts>,
            "BytesSampleTime" : <elapsed time of the reported the bytes received and sent values>,
            "BytesReceived" : <bytes received>,
            "BytesSent" : <bytes sent>,
            "ProtocolVersion" : <protocol version of the server, missing if unknown>
         },
         ...
         ]
      }

Tenant
======

|tenant-blurb1|

.. type:: FDBTenant

   An opaque type that represents a tenant in the FoundationDB C API.

.. function:: void fdb_tenant_destroy(FDBTenant* tenant)

   Destroys an :type:`FDBTenant` object. It must be called exactly once for each successful call to :func:`fdb_database_create_tenant()`. This function only destroys a handle to the tenant -- the tenant and its data will be fine!

.. function:: fdb_error_t fdb_tenant_create_transaction(FDBTenant* tenant, FDBTronsaction **out_transaction)

   Creates a new transaction on the given tenant. This transaction will operate within the tenant's key-space and cannot access data outside the tenant. The caller assumes ownership of the :type:`FDBTransaction` object and must destroy it with :func:`fdb_transaction_destroy()`.

   ``*out_transaction``
      Set to point to the newly created :type:`FDBTransaction`.

Transaction
===========

|transaction-blurb1|

Applications must provide error handling and an appropriate retry loop around the application code for a transaction. See the documentation for :func:`fdb_transaction_on_error()`.

|transaction-blurb2|

|transaction-blurb3|

.. type:: FDBTransaction

   An opaque type that represents a transaction in the FoundationDB C API.

.. function:: void fdb_transaction_destroy(FDBTransaction* transaction)

   Destroys an :type:`FDBTransaction` object. It must be called exactly once for each successful call to :func:`fdb_database_create_transaction()`. Destroying a transaction which has not had :func:`fdb_transaction_commit()` called implicitly "rolls back" the transaction (sets and clears do not take effect on the database).

.. function:: fdb_error_t fdb_transaction_set_option(FDBTransaction* transaction, FDBTransactionOption option, uint8_t const* value, int value_length)

   Called to set an option on an :type:`FDBTransaction`. |option-parameter| :func:`fdb_transaction_set_option()` returns.

.. type:: FDBTransactionOption

   |option-doc|

.. function:: void fdb_transaction_set_read_version(FDBTransaction* transaction, int64_t version)

   Sets the snapshot read version used by a transaction. This is not needed in simple cases. If the given version is too old, subsequent reads will fail with error_code_transaction_too_old; if it is too new, subsequent reads may be delayed indefinitely and/or fail with error_code_future_version. If any of ``fdb_transaction_get_*()`` have been called on this transaction already, the result is undefined.

.. function:: FDBFuture* fdb_transaction_get_read_version(FDBTransaction* transaction)

   |future-return0| the transaction snapshot read version. |future-return1| call :func:`fdb_future_get_int64()` to extract the version into an int64_t that you provide, |future-return2|

   The transaction obtains a snapshot read version automatically at the time of the first call to ``fdb_transaction_get_*()`` (including this one) and (unless causal consistency has been deliberately compromised by transaction options) is guaranteed to represent all transactions which were reported committed before that call.

.. function:: FDBFuture* fdb_transaction_get(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length, fdb_bool_t snapshot)

   Reads a value from the database snapshot represented by ``transaction``.

   |future-return0| the value of ``key_name`` in the database. |future-return1| call :func:`fdb_future_get_value()` to extract the value, |future-return2|

   See :func:`fdb_future_get_value()` to see exactly how results are unpacked. If ``key_name`` is not present in the database, the result is not an error, but a zero for ``*out_present`` returned from that function.

   ``key_name``
      A pointer to the name of the key to be looked up in the database. |no-null|

   ``key_name_length``
      |length-of| ``key_name``.

   ``snapshot``
      |snapshot|

.. function:: FDBFuture* fdb_transaction_get_estimated_range_size_bytes( FDBTransaction* tr, uint8_t const* begin_key_name, int begin_key_name_length, uint8_t const* end_key_name, int end_key_name_length)

   Returns an estimated byte size of the key range.

   .. note:: The estimated size is calculated based on the sampling done by FDB server. The sampling algorithm works roughly in this way: the larger the key-value pair is, the more likely it would be sampled and the more accurate its sampled size would be. And due to that reason it is recommended to use this API to query against large ranges for accuracy considerations. For a rough reference, if the returned size is larger than 3MB, one can consider the size to be accurate.

   |future-return0| the estimated size of the key range given. |future-return1| call :func:`fdb_future_get_int64()` to extract the size, |future-return2|

.. function:: FDBFuture* fdb_transaction_get_range_split_points( FDBTransaction* tr, uint8_t const* begin_key_name, int begin_key_name_length, uint8_t const* end_key_name, int end_key_name_length, int64_t chunk_size)

   Returns a list of keys that can split the given range into (roughly) equally sized chunks based on ``chunk_size``.

   .. note:: The returned split points contain the start key and end key of the given range

   |future-return0| the list of split points. |future-return1| call :func:`fdb_future_get_key_array()` to extract the array, |future-return2|

.. function:: FDBFuture* fdb_transaction_get_key(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length, fdb_bool_t or_equal, int offset, fdb_bool_t snapshot)

   Resolves a :ref:`key selector <key-selectors>` against the keys in the database snapshot represented by ``transaction``.

   |future-return0| the key in the database matching the :ref:`key selector <key-selectors>`. |future-return1| call :func:`fdb_future_get_key()` to extract the key, |future-return2|

   ``key_name``, ``key_name_length``, ``or_equal``, ``offset``
      The four components of a :ref:`key selector <key-selectors>`.

   ``snapshot``
      |snapshot|

.. function:: FDBFuture* fdb_transaction_get_addresses_for_key(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length)

    Returns a list of public network addresses as strings, one for each of the storage servers responsible for storing ``key_name`` and its associated value.

    |future-return0| an array of strings. |future-return1| call :func:`fdb_future_get_string_array()` to extract the string array, |future-return2|

    ``key_name``
        A pointer to the name of the key whose location is to be queried.

    ``key_name_length``
        |length-of| ``key_name``.

.. |range-limited-by| replace:: If this limit was reached before the end of the specified range, then the ``*more`` return of :func:`fdb_future_get_keyvalue_array()` will be set to a non-zero value.

.. function:: FDBFuture* fdb_transaction_get_range(FDBTransaction* transaction, uint8_t const* begin_key_name, int begin_key_name_length, fdb_bool_t begin_or_equal, int begin_offset, uint8_t const* end_key_name, int end_key_name_length, fdb_bool_t end_or_equal, int end_offset, int limit, int target_bytes, FDBStreamingMode mode, int iteration, fdb_bool_t snapshot, fdb_bool_t reverse)

   Reads all key-value pairs in the database snapshot represented by ``transaction`` (potentially limited by :data:`limit`, :data:`target_bytes`, or :data:`mode`) which have a key lexicographically greater than or equal to the key resolved by the begin :ref:`key selector <key-selectors>` and lexicographically less than the key resolved by the end :ref:`key selector <key-selectors>`.

   |future-return0| an :type:`FDBKeyValue` array. |future-return1| call :func:`fdb_future_get_keyvalue_array()` to extract the key-value array, |future-return2|

   ``begin_key_name``, :data:`begin_key_name_length`, :data:`begin_or_equal`, :data:`begin_offset`
      The four components of a :ref:`key selector <key-selectors>` describing the beginning of the range.

   ``end_key_name``, :data:`end_key_name_length`, :data:`end_or_equal`, :data:`end_offset`
      The four components of a :ref:`key selector <key-selectors>` describing the end of the range.

   ``limit``
      If non-zero, indicates the maximum number of key-value pairs to return. |range-limited-by|

   ``target_bytes``
      If non-zero, indicates a (soft) cap on the combined number of bytes of keys and values to return. |range-limited-by|

   ``mode``
      One of the :type:`FDBStreamingMode` values indicating how the caller would like the data in the range returned.

   ``iteration``
      If ``mode`` is :data:`FDB_STREAMING_MODE_ITERATOR`, this parameter should start at 1 and be incremented by 1 for each successive call while reading this range. In all other cases it is ignored.

   ``snapshot``
      |snapshot|

   ``reverse``
      If non-zero, key-value pairs will be returned in reverse lexicographical order beginning at the end of the range. Reading ranges in reverse is supported natively by the database and should have minimal extra cost.

.. type:: FDBStreamingMode

   An enumeration of available streaming modes to be passed to :func:`fdb_transaction_get_range()`.

   ``FDB_STREAMING_MODE_ITERATOR``

   The caller is implementing an iterator (most likely in a binding to a higher level language). The amount of data returned depends on the value of the ``iteration`` parameter to :func:`fdb_transaction_get_range()`.

   ``FDB_STREAMING_MODE_SMALL``

   Data is returned in small batches (not much more expensive than reading individual key-value pairs).

   ``FDB_STREAMING_MODE_MEDIUM``

   Data is returned in batches between _SMALL and _LARGE.

   ``FDB_STREAMING_MODE_LARGE``

   Data is returned in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible. If the caller does not need the entire range, some disk and network bandwidth may be wasted. The batch size may be still be too small to allow a single client to get high throughput from the database.

   ``FDB_STREAMING_MODE_SERIAL``

   Data is returned in batches large enough that an individual client can get reasonable read bandwidth from the database. If the caller does not need the entire range, considerable disk and network bandwidth may be wasted.

   ``FDB_STREAMING_MODE_WANT_ALL``

   The caller intends to consume the entire range and would like it all transferred as early as possible.

   ``FDB_STREAMING_MODE_EXACT``

   The caller has passed a specific row limit and wants that many rows delivered in a single batch.

.. function:: void fdb_transaction_set(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length, uint8_t const* value, int value_length)

   |sets-and-clears1| to change the given key to have the given value. If the given key was not previously present in the database it is inserted.

   |sets-and-clears2|

   ``key_name``
      A pointer to the name of the key to be inserted into the database. |no-null|

   ``key_name_length``
      |length-of| ``key_name``.

   ``value``
      A pointer to the value to be inserted into the database. |no-null|

   ``value_length``
      |length-of| ``value``.

.. function:: void fdb_transaction_clear(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length)

   |sets-and-clears1| to remove the given key from the database. If the key was not previously present in the database, there is no effect.

   |sets-and-clears2|

   ``key_name``
      A pointer to the name of the key to be removed from the database. |no-null|

   ``key_name_length``
      |length-of| ``key_name``.

.. function:: void fdb_transaction_clear_range(FDBTransaction* transaction, uint8_t const* begin_key_name, int begin_key_name_length, uint8_t const* end_key_name, int end_key_name_length)

   |sets-and-clears1| to remove all keys (if any) which are lexicographically greater than or equal to the given begin key and lexicographically less than the given end_key.

   |sets-and-clears2|

   |transaction-clear-range-blurb|

   ``begin_key_name``
      A pointer to the name of the key specifying the beginning of the range to clear. |no-null|

   ``begin_key_name_length``
      |length-of| ``begin_key_name``.

   ``end_key_name``
      A pointer to the name of the key specifying the end of the range to clear. |no-null|

   ``end_key_name_length``
      |length-of| ``end_key_name_length``.

.. function:: void fdb_transaction_atomic_op(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length, uint8_t const* param, int param_length, FDBMutationType operationType)

    |sets-and-clears1| to perform the operation indicated by ``operationType`` with operand ``param`` to the value stored by the given key.

    |atomic-ops-blurb1|

    |atomic-ops-blurb2|

    |atomic-ops-blurb3|

    .. warning :: |atomic-ops-warning|

    |sets-and-clears2|

    ``key_name``
        A pointer to the name of the key whose value is to be mutated.

    ``key_name_length``
        |length-of| ``key_name``.

    ``param``
        A pointer to the parameter with which the atomic operation will mutate the value associated with ``key_name``.

    ``param_length``
        |length-of| ``param``.

    ``operation_type``
        One of the :type:`FDBMutationType` values indicating which operation should be performed.

.. type:: FDBMutationType

    An enumeration of available opcodes to be passed to :func:`fdb_transaction_atomic_op()`

    ``FDB_MUTATION_TYPE_ADD``

    |atomic-add1|

    |atomic-add2|

    ``FDB_MUTATION_TYPE_AND``

    |atomic-and|

    ``FDB_MUTATION_TYPE_OR``

    |atomic-or|

    ``FDB_MUTATION_TYPE_XOR``

    |atomic-xor|

    ``FDB_MUTATION_TYPE_COMPARE_AND_CLEAR``

    |atomic-compare-and-clear|

    ``FDB_MUTATION_TYPE_MAX``

    |atomic-max1|

    |atomic-max-min|

    ``FDB_MUTATION_TYPE_BYTE_MAX``

    |atomic-byte-max|

    ``FDB_MUTATION_TYPE_MIN``

    |atomic-min1|

    |atomic-max-min|

    ``FDB_MUTATION_TYPE_BYTE_MIN``

    |atomic-byte-min|

    ``FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY``

    |atomic-set-versionstamped-key-1|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    .. warning :: |atomic-versionstamps-tuple-warning-key|

    ``FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE``

    |atomic-set-versionstamped-value|

    |atomic-versionstamps-1|

    |atomic-versionstamps-2|

    .. warning :: |atomic-versionstamps-tuple-warning-value|

.. function:: FDBFuture* fdb_transaction_commit(FDBTransaction* transaction)

   Attempts to commit the sets and clears previously applied to the database snapshot represented by ``transaction`` to the actual database. The commit may or may not succeed -- in particular, if a conflicting transaction previously committed, then the commit must fail in order to preserve transactional isolation. If the commit does succeed, the transaction is durably committed to the database and all subsequently started transactions will observe its effects.

   It is not necessary to commit a read-only transaction -- you can simply call :func:`fdb_transaction_destroy()`.

   |future-returnvoid|

   Callers will usually want to retry a transaction if the commit or a prior ``fdb_transaction_get_*()`` returns a retryable error (see :func:`fdb_transaction_on_error()`).

   |commit-unknown-result-blurb|

   |commit-outstanding-reads-blurb|

.. function:: fdb_error_t fdb_transaction_get_committed_version(FDBTransaction* transaction, int64_t* out_version)

   Retrieves the database version number at which a given transaction was committed. :func:`fdb_transaction_commit()` must have been called on ``transaction`` and the resulting future must be ready and not an error before this function is called, or the behavior is undefined. Read-only transactions do not modify the database when committed and will have a committed version of -1. Keep in mind that a transaction which reads keys and then sets them to their current values may be optimized to a read-only transaction.

   Note that database versions are not necessarily unique to a given transaction and so cannot be used to determine in what order two transactions completed. The only use for this function is to manually enforce causal consistency when calling :func:`fdb_transaction_set_read_version()` on another subsequent transaction.

   Most applications will not call this function.

.. function:: FDBFuture* fdb_transaction_get_tag_throttled_duration(FDBTransaction* transaction)

  |future-return0| the time (in seconds) that the transaction was throttled by the tag throttler in the returned future. |future-return1| call :func:`fdb_future_get_double()` to extract the duration, |future-return2|

.. function:: FDBFuture* fdb_transaction_get_total_cost(FDBTransaction* transaction)

  |future-return0| the cost of the transaction so far (in bytes) in the returned future, as computed by the tag throttler, and used for tag throttling if throughput quotas are specified. |future-return1| call :func:`fdb_future_get_int64()` to extract the cost, |future-return2|

.. function:: FDBFuture* fdb_transaction_get_approximate_size(FDBTransaction* transaction)

  |future-return0| the approximate transaction size so far in the returned future, which is the summation of the estimated size of mutations, read conflict ranges, and write conflict ranges. |future-return1| call :func:`fdb_future_get_int64()` to extract the size, |future-return2|

  This can be called multiple times before the transaction is committed.

.. function:: FDBFuture* fdb_transaction_get_versionstamp(FDBTransaction* transaction)

  |future-return0| the versionstamp which was used by any versionstamp operations in this transaction. |future-return1| call :func:`fdb_future_get_key()` to extract the key, |future-return2|

  The future will be ready only after the successful completion of a call to |commit-func| on this Transaction. Read-only transactions do not modify the database when committed and will result in the future completing with an error. Keep in mind that a transaction which reads keys and then sets them to their current values may be optimized to a read-only transaction.

  Most applications will not call this function.

.. function:: FDBFuture* fdb_transaction_watch(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length)

    |transaction-watch-blurb|

    |transaction-watch-committed-blurb|

    |transaction-watch-error-blurb|

    |future-returnvoid0| that will be set once the watch has detected a change to the value at the specified key. |future-return1| |future-return2|

    |transaction-watch-limit-blurb|

    ``key_name``
        A pointer to the name of the key to watch. |no-null|

    ``key_name_length``
        |length-of| ``key_name``.


.. function:: FDBFuture* fdb_transaction_on_error(FDBTransaction* transaction, fdb_error_t error)

   Implements the recommended retry and backoff behavior for a transaction. This function knows which of the error codes generated by other ``fdb_transaction_*()`` functions represent temporary error conditions and which represent application errors that should be handled by the application. It also implements an exponential backoff strategy to avoid swamping the database cluster with excessive retries when there is a high level of conflict between transactions.

   On receiving any type of error from an ``fdb_transaction_*()`` function, the application should:

   1. Call :func:`fdb_transaction_on_error()` with the returned :type:`fdb_error_t` code.

   2. Wait for the resulting future to be ready.

   3. If the resulting future is itself an error, destroy the future and FDBTransaction and report the error in an appropriate way.

   4. If the resulting future is not an error, destroy the future and restart the application code that performs the transaction. The transaction itself will have already been reset to its initial state, but should not be destroyed and re-created because state used by :func:`fdb_transaction_on_error()` to implement its backoff strategy and state related to timeouts and retry limits is stored there.

   |future-returnvoid|

.. function:: void fdb_transaction_reset(FDBTransaction* transaction)

   Reset ``transaction`` to its initial state. This is similar to calling :func:`fdb_transaction_destroy()` followed by :func:`fdb_database_create_transaction()`. It is not necessary to call :func:`fdb_transaction_reset()` when handling an error with :func:`fdb_transaction_on_error()` since the transaction has already been reset.

.. function:: void fdb_transaction_cancel(FDBTransaction* transaction)

   |transaction-cancel-blurb|

   .. warning :: |transaction-reset-cancel-warning|

   .. warning :: |transaction-commit-cancel-warning|

.. _conflictRanges:

.. function:: fdb_error_t fdb_transaction_add_conflict_range(FDBTransaction* transaction, uint8_t const* begin_key_name, int begin_key_name_length, uint8_t const* end_key_name, int end_key_name_length, FDBConflictRangeType type)

    Adds a :ref:`conflict range <conflict-ranges>` to a transaction without performing the associated read or write.

    .. note:: |conflict-range-note|

    ``begin_key_name``
        A pointer to the name of the key specifying the beginning of the conflict range. |no-null|

    ``begin_key_name_length``
        |length-of| ``begin_key_name``.

    ``end_key_name``
        A pointer to the name of the key specifying the end of the conflict range. |no-null|

    ``end_key_name_length``
        |length-of| ``end_key_name_length``.

    ``type``
        One of the :type:`FDBConflictRangeType` values indicating what type of conflict range is being set.

.. type:: FDBConflictRangeType

    An enumeration of available conflict range types to be passed to :func:`fdb_transaction_add_conflict_range()`.

    ``FDB_CONFLICT_RANGE_TYPE_READ``

    |add-read-conflict-range-blurb|

    ``FDB_CONFLICT_RANGE_TYPE_WRITE``

    |add-write-conflict-range-blurb|

.. _snapshots:

Snapshot reads
--------------

|snapshot-blurb1|

|snapshot-blurb2|

|snapshot-blurb3|

In the C API, snapshot reads are performed by passing a non-zero value to the ``snapshot`` parameter of any of ``fdb_transaction_get_*`` (see for example :func:`fdb_transaction_get()`).  |snapshot-blurb4|

.. _key-selectors:

Key selectors
=============

|keysel-blurb1|

|keysel-blurb2|

In the FoundationDB C API, key selectors are not represented by a structure of any kind, but are instead expressed as sequential parameters to |get-key-func| and |get-range-func|. For convenience, the most common key selectors are available as C macros that expand to the appropriate parameters.

.. type:: FDB_KEYSEL_LAST_LESS_THAN(key_name, key_name_length)

.. type:: FDB_KEYSEL_LAST_LESS_OR_EQUAL(key_name, key_name_length)

.. type:: FDB_KEYSEL_FIRST_GREATER_THAN(key_name, key_name_length)

.. type:: FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(key_name, key_name_length)

To use one of these macros, simply replace the four parameters in the function with one of ``FDB_KEYSEL_*``::

    future = fdb_transaction_get_key(transaction, "key", 3, 0, 2, 0);

could instead be written as::

    future = fdb_transaction_get_key(transaction, FDB_KEYSEL_FIRST_GREATER_THAN("key", 3)+1, 0);

Miscellaneous
=============

.. type:: fdb_bool_t

   An integer type representing a boolean. A value of 0 is false and non-zero is true.

.. type:: fdb_error_t

   An integer type representing an error. A value of 0 is success and non-zero is an error.

.. function:: const char* fdb_get_error(fdb_error_t code)

   Returns a (somewhat) human-readable English message from an error code. The return value is a statically allocated null-terminated string that *must not* be freed by the caller.

.. function:: fdb_bool_t fdb_error_predicate(int predicate_test, fdb_error_t code)

	Evaluates a predicate against an error code. The predicate to run should be one of the codes listed by the ``FDBErrorPredicate`` enum defined within ``fdb_c_options.g.h``. Sample predicates include ``FDB_ERROR_PREDICATE_RETRYABLE``, which can be used to determine whether the error with the given code is a retryable error or not.
