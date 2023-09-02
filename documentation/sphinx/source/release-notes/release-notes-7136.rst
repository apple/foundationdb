#############
Release Notes
#############

71.3.6
======

Features
--------

Performance
-----------

Reliability
-----------

* Ensure exponential backoff while refreshing KMS URLs.
* Update EncryptKeyProxy KMS health check timeouts.
* Avoid commit proxy and storage server commit timeout due to KMS failure.


Fixes
-----

* Fix quota throttler clear cost estimation.
* Fix the DataLossRecovery workload stuck issue.
* Wait file deleted in blob manifest cleanup.
* Fixed slowtask and OOM in large blob granule purges, and improved observability.
* Improve Blob Manager’s granule balancing algorithm in pathological cases.
* Allow granule purge to continue when the cluster is unhealthy.
* Restore original function names for old blob granule API.
* Fix memory leak in Status.actor.cpp by avoiding creating database context as part of lockedStatusFetcher operation.

Status
------

Bindings
--------

Other Changes
-------------

* Report how many logical ranges and shards have been over-replicated by custom replication.


Change Log
---------------------

* https://github.com/apple/foundationdb/pull/10664
* https://github.com/apple/foundationdb/pull/10811
* https://github.com/apple/foundationdb/pull/10808
* https://github.com/apple/foundationdb/pull/10792
* https://github.com/apple/foundationdb/pull/10803
* https://github.com/apple/foundationdb/pull/10804
* https://github.com/apple/foundationdb/pull/10824
* https://github.com/apple/foundationdb/pull/10830
* https://github.com/apple/foundationdb/pull/10832
* https://github.com/apple/foundationdb/pull/10797
* https://github.com/apple/foundationdb/pull/10789
* https://github.com/apple/foundationdb/pull/10847
* https://github.com/apple/foundationdb/pull/10860


Earlier release notes
---------------------

* :doc:`71.3.0 (API Version 700) </release-notes/release-notes-7130>`
* :doc:`71.3.1 (API Version 700) </release-notes/release-notes-7131>`
* :doc:`71.3.2 (API Version 700) </release-notes/release-notes-7132>`
* :doc:`71.3.4 (API Version 700) </release-notes/release-notes-7134>`