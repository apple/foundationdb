#############
Release Notes
#############

71.3.0
======

Features
--------
- Added system clock timing to CommitDebug trace events using MonotonicTime field.
- BlobGranule driven backup + restore.
- Encryption at-Rest, supports per-tenant and/or per-cluster encryption semantics.
- External Vault support for per-tenant blob granule storage location.
- Improved APIs for creating and managing Blob Granule ranges.
- Updated tenants to be ID oriented. Tenants cannot be upgraded from previous releases.
- Restored data clusters can join a metacluster. A management cluster can be recovered from the data clusters.
- Added support for locking tenants.
- Added a new function fdb_database_get_client_status providing client-side connection status json information.
- Added new database options to define defaults for transaction options report_conflicting_keys and used_during_commit_protection_disable.
- Added a new network option retain_client_library_copies to avoid deleting the temporary library copies after completion of the process. This may be useful in various debugging and profiling scenarios.
- Added a new network option trace_initialize_on_setup to enable client traces already on fdb_setup_network, so that traces do not get lost on client configuration issues.
- TraceEvents related to TLS handshakes, new connections, and tenant access by authorization token are no longer subject to suppression or throttling, using an internal “AuditedEvent” TraceEvent classification.
- Usage of authorization token is logged as part of AuditedEvent, with a 5-second suppression time window for duplicate entries. (suppression time window is controlled by AUDIT_TIME_WINDOW flow knob).
- Client-side arena blocks and packet buffers that held authorization tokens are zeroed out immediately before deallocation to minimize the likelihood of leaked tokens.
- Authorization token’s “aud” field may have singular string as value, instead of a singleton array (optional JWT standard).
- RPC response to tenant-scoped get-key-location requests now returns a key range clamped to the tenant keyspace to prevent leaking key range information of the normal keyspace to the other tenants.
- Updated tenant authorization to be ID oriented: instead of base64-encoded tenant names, a token’s “tenants” field shall contain one or more base64-encoded 8-byte big-endian tenant ID numbers. (which is also a tenant key prefix).


Performance
-----------
- Reduced overhead of creating transactions in the Java bindings by using the default option of used_during_commit_protection_disable, instead of setting it on every transaction.
- Remove duplicated transaction timeout management in the multi-version client. This significantly reduces the overall CPU usage related to setting timeouts.


Reliability
-----------
- Added quota driven workload isolation.
- Added commands for hot range detection and custom sharding + over-replication.
- Added a new network option fail_incompatible_client. If the option is set, transactions will fail with fail_incompatible_client in cases where they attempt to connect to a cluster without providing a compatible client library.
- Added a new network option trace_initialize_on_setup to enable client traces already on fdb_setup_network, so that traces do not get lost on client configuration issues.
- Starting with API version 710300, fdb_setup_network will fail if at least one of the external clients fails to initialize, e.g. because it does not support the selected API version. Added a network option ignore_external_client_failures to restore the previous behavior ignoring external client initialization errors.
- In case where the request to determine the cluster version fails because of a fatal error, avoid entering the busy loop repeating the request indefinitely.
- Fixed a run loop profiler deadlock in the multi-version client, the profiler can be now be enabled on the client-side using the network option enable_run_loop_profiling.
- Use different base trace file names for client threads in multi-threaded mode. This fixes the issue of one client thread deleting traces of another one. 


Fixes
-----

Status
------

Bindings
--------

Other Changes
-------------

Earlier release notes
---------------------
