#############
Release Notes
#############

71.3.4
======

Features
--------

Performance
-----------

Reliability
-----------

* Support tracking write throughput for multiple tags on a single storage server

Fixes
-----

* Fix a bug in the change feed cache on the blob workers causing it to hang when enabled on an encrypted cluster.
* Fixed locality based exclusions to move tlog processes off excluded servers.
* Fix the go tuple tests for new go version.
* Fixes change feed head-of-line blocking if a client requests a whenAtLeast on a feed version significantly ahead of where the feed current location.
* Fix forward compatibility of blob granule APIs to allow upgrading to future major release with changed fields.
* Fix worker server reporting IoTimeoutError as injected fault.
* Fix fdbcli status json to report blob granules backup status after blob manager restarts.
* Fix blob manager to find the right key for blob granules split in edge cases.
* Fix a workload issue that was caused DataLossRecovery test to hang.
* Reduce the frequency of polling tenants' storage usage.
* Stagger storage quota estimation requests and observability improvements.
* Fix test storage server recruitment when cluster is unhealthy.
* Add a new flag to skip missing data tenants during blob granules restore. The flag is false as default. 
* Stop tracking a storage server after its removal to avoid infinite looping.
* Prevent Status actor from bubbling up timeout error leading to cluster recovery.
* Fixes bug and adds test for properly truncating keys in the case of large splits.

Status
------

Bindings
--------

Other Changes
-------------

* Improved observability for change feeds, blob worker, blob manager, and encrypt key proxy.

Change Log
---------------------

* https://github.com/apple/foundationdb/pull/10749
* https://github.com/apple/foundationdb/pull/10738
* https://github.com/apple/foundationdb/pull/10731
* https://github.com/apple/foundationdb/pull/10591
* https://github.com/apple/foundationdb/pull/10594	
* https://github.com/apple/foundationdb/pull/10679
* https://github.com/apple/foundationdb/pull/10585
* https://github.com/apple/foundationdb/pull/10617
* https://github.com/apple/foundationdb/pull/10760
* https://github.com/apple/foundationdb/pull/10734 
* https://github.com/apple/foundationdb/pull/10713
* https://github.com/apple/foundationdb/pull/10625
* https://github.com/apple/foundationdb/pull/10776
* https://github.com/apple/foundationdb/pull/10622
* https://github.com/apple/foundationdb/pull/10635
* https://github.com/apple/foundationdb/pull/10788 
* https://github.com/apple/foundationdb/pull/10794

Earlier release notes
---------------------

* :doc:`71.3.2 (API Version 710300) </release-notes/release-notes-7132>`
* :doc:`71.3.1 (API Version 710300) </release-notes/release-notes-7131>`
* :doc:`71.3.0 (API Version 710300) </release-notes/release-notes-7130>`
