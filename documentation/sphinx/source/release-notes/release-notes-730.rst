Release Notes
=============

7.3.0
-----

###Fixes \* Added back samples for (non)empty peeks stats.
``(PR #9072) <https://github.com/apple/foundationdb/pull/9072>``\ *\*
Fixed a consistency scan infinite looping without progress bug when a
storage server is removed.
``(PR #9154) <https://github.com/apple/foundationdb/pull/9154>``* \*
Fixed a backup worker assertion failure.
``(PR #8886) <https://github.com/apple/foundationdb/pull/8886>``\ *\*
Fix a log router race condition that blocks remote tlogs forever.
``(PR #8856) <https://github.com/apple/foundationdb/pull/8856>``* \*
Fixed a DD stuck issue when the remote data center is dead.
``(PR #9338) <https://github.com/apple/foundationdb/pull/9338>``\ \_ \*
Exclude command will not perform a write if the addresses being excluded
are already excluded.
``(PR #9873) <https://github.com/apple/foundationdb/pull/9873>`` \*
ConsistencyCheck should finish after complete scan than failing on first
mismatch.
``(PR #8539) <https://github.com/apple/foundationdb/pull/8539>``

###Bindings \* Allow Ruby bindings to run on arm64.
``(PR #9575) <https://github.com/apple/foundationdb/pull/9575>``\ \_

###Performance \* Improvements on Physical shard creation to reduce
shard count.
``(PR #9067) <https://github.com/apple/foundationdb/pull/9067>`` \*
Older TLog generations are garbage collected as long as they are no
longer needed.
``(PR #10289) <https://github.com/apple/foundationdb/pull/10289>`` \*

###Reliability \* Gray failure will monitor satellite TLog
disconnections. \* Storage progress is logged during the slow recovery.
``(PR #9041) <https://github.com/apple/foundationdb/pull/9041>`` \*
Added a new network option fail_incompatible_client. If the option is
set, transactions are failing with fail_incompatible_client in case of
an attempt to connect to a cluster without providing a compatible client
library

###Status

###Other Changes

-  Added MonotonicTime field, based on system clock, to CommitDebug
   trace events, for accurate timing.

-  Added a new function fdb_database_get_client_status providing a
   client-side connection status information in json format.

-  Added a new network option retain_client_library_copies to avoid
   deleting the temporary library copies after completion of the
   process. This may be useful in various debugging and profiling
   scenarios.

-  Added a new network option trace_initialize_on_setup to enable client
   traces already on fdb_setup_network, so that traces do not get lost
   on client configuration issues

-  TraceEvents related to TLS handshake, new connections, and tenant
   access by authorization token are no longer subject to suppression or
   throttling, using an internal “AuditedEvent” TraceEvent
   classification

-  Usage of authorization token is logged as part of AuditedEvent, with
   5-second suppression time window for duplicate entries (suppression
   time window is controlled by AUDIT_TIME_WINDOW flow knob)
