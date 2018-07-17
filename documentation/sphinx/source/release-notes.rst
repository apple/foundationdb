#############
Release Notes
#############

6.0.2
=====

Features
--------

* Added support for asynchronous replication to a remote DC with processes in a single cluster. This improves on the asynchronous replication offered by fdbdr because servers can fetch data from the remote DC if all replicas have been lost in one DC.
* Added support for synchronous replication of the transaction log to a remote DC. This remote DC does not need to contain any storage servers, meaning you need much fewer servers in this remote DC.
* The TLS plugin is now statically linked into the client and server binaries and no longer requires a separate library. `(Issue #436) <https://github.com/apple/foundationdb/issues/436>`_
* TLS peer verification now supports verifiying on Subject Alternative Name. `(Issue #514) <https://github.com/apple/foundationdb/issues/514>`_
* TLS peer verification now supports suffix matching by field. `(Issue #515) <https://github.com/apple/foundationdb/issues/515>`_

Performance
-----------

* Transaction logs do not copy mutations from previous generations of transaction logs. `(PR #339) <https://github.com/apple/foundationdb/pull/339>`_
* Load balancing temporarily avoids communicating with storage servers that have fallen behind.
* Avoid assigning storage servers responsibility for keys they do not have.
* Clients optimistically assume the first leader reply from a coordinator is correct. `(PR #425) <https://github.com/apple/foundationdb/pull/425>`_
* Network connections are now closed after no interface needs the connection. [6.0.1] `(Issue #375) <https://github.com/apple/foundationdb/issues/375>`_
* Significantly improved the CPU efficiency of copy mutations to transaction logs during recovery. [6.0.2] `(PR #595) <https://github.com/apple/foundationdb/pull/595>`_

Fixes
-----

* Not all endpoint failures were reported to the failure monitor.
* Watches registered on a lagging storage server would take a long time to trigger.
* The cluster controller would not start a new generation until it recovered its files from disk.
* Under heavy write load, storage servers would occasionally pause for ~100ms. [6.0.2] `(PR #597) <https://github.com/apple/foundationdb/pull/597>`_
* Storage servers were not given time to rejoin the cluster before being marked as failed. [6.0.2] `(PR #592) <https://github.com/apple/foundationdb/pull/592>`_

Status
------

* The replication factor in status JSON is stored under "redundancy_mode" instead of "redundancy":"factor". `(PR #492) <https://github.com/apple/foundationdb/pull/492>`_
* Additional metrics for storage server lag as well as the number of watches and mutation count have been added and are exposed through status. `(PR #521) <https://github.com/apple/foundationdb/pull/521>`_


Bindings
--------

* API version updated to 600. There are no changes since API version 520.
* Several cases where functions in go might previously cause a panic now return a non-``nil`` error. `(PR #532) <https://github.com/apple/foundationdb/pull/532>`_
* C API calls made on the network thread could be reordered with calls made from other threads. [6.0.2] `(Issue #518) <https://github.com/apple/foundationdb/issues/518>`_

Other Changes
-------------

* Does not support upgrades from any version older than 5.0.
* Normalized the capitalization of trace event names and attributes. `(PR #455) <https://github.com/apple/foundationdb/pull/455>`_

Earlier release notes
---------------------
* :doc:`5.2 (API Version 520) </old-release-notes/release-notes-520>`
* :doc:`5.1 (API Version 510) </old-release-notes/release-notes-510>`
* :doc:`5.0 (API Version 500) </old-release-notes/release-notes-500>`
* :doc:`4.6 (API Version 460) </old-release-notes/release-notes-460>`
* :doc:`4.5 (API Version 450) </old-release-notes/release-notes-450>`
* :doc:`4.4 (API Version 440) </old-release-notes/release-notes-440>`
* :doc:`4.3 (API Version 430) </old-release-notes/release-notes-430>`
* :doc:`4.2 (API Version 420) </old-release-notes/release-notes-420>`
* :doc:`4.1 (API Version 410) </old-release-notes/release-notes-410>`
* :doc:`4.0 (API Version 400) </old-release-notes/release-notes-400>`
* :doc:`3.0 (API Version 300) </old-release-notes/release-notes-300>`
* :doc:`2.0 (API Version 200) </old-release-notes/release-notes-200>`
* :doc:`1.0 (API Version 100) </old-release-notes/release-notes-100>`
* :doc:`Beta 3 (API Version 23) </old-release-notes/release-notes-023>`
* :doc:`Beta 2 (API Version 22) </old-release-notes/release-notes-022>`
* :doc:`Beta 1 (API Version 21) </old-release-notes/release-notes-021>`
* :doc:`Alpha 6 (API Version 16) </old-release-notes/release-notes-016>`
* :doc:`Alpha 5 (API Version 14) </old-release-notes/release-notes-014>`
