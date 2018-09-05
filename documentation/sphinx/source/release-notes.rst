#############
Release Notes
#############

6.0.10
=====

Features
--------

* Added support for asynchronous replication to a remote DC with processes in a single cluster. This improves on the asynchronous replication offered by fdbdr because servers can fetch data from the remote DC if all replicas have been lost in one DC.
* Added support for synchronous replication of the transaction log to a remote DC. This remote DC does not need to contain any storage servers, meaning you need much fewer servers in this remote DC.
* The TLS plugin is now statically linked into the client and server binaries and no longer requires a separate library. `(Issue #436) <https://github.com/apple/foundationdb/issues/436>`_
* TLS peer verification now supports verifiying on Subject Alternative Name. `(Issue #514) <https://github.com/apple/foundationdb/issues/514>`_
* TLS peer verification now supports suffix matching by field. `(Issue #515) <https://github.com/apple/foundationdb/issues/515>`_
* TLS certificates are automatically reloaded after being updated. [6.0.5] `(Issue #505) <https://github.com/apple/foundationdb/issues/505>`_
* Added the ``fileconfigure`` command to fdbcli, which configures a database from a JSON document. [6.0.10] `(PR #713) <https://github.com/apple/foundationdb/pull/713>`_

Performance
-----------

* Transaction logs do not copy mutations from previous generations of transaction logs. `(PR #339) <https://github.com/apple/foundationdb/pull/339>`_
* Load balancing temporarily avoids communicating with storage servers that have fallen behind.
* Avoid assigning storage servers responsibility for keys they do not have.
* Clients optimistically assume the first leader reply from a coordinator is correct. `(PR #425) <https://github.com/apple/foundationdb/pull/425>`_
* Network connections are now closed after no interface needs the connection. [6.0.1] `(Issue #375) <https://github.com/apple/foundationdb/issues/375>`_
* Significantly improved the CPU efficiency of copy mutations to transaction logs during recovery. [6.0.2] `(PR #595) <https://github.com/apple/foundationdb/pull/595>`_
* A cluster configured with usable_regions=2 did not limit the rate at which it could copy data from the primary DC to the remote DC. This caused poor performance when recovering from a DC outage. [6.0.5] `(PR #673) <https://github.com/apple/foundationdb/pull/673>`_
* Significantly improved the CPU efficiency of generating status on the cluster controller. [6.0.10] `(PR #738) <https://github.com/apple/foundationdb/pull/738>`_

Fixes
-----

* Not all endpoint failures were reported to the failure monitor.
* Watches registered on a lagging storage server would take a long time to trigger.
* The cluster controller would not start a new generation until it recovered its files from disk.
* Under heavy write load, storage servers would occasionally pause for ~100ms. [6.0.2] `(PR #597) <https://github.com/apple/foundationdb/pull/597>`_
* Storage servers were not given time to rejoin the cluster before being marked as failed. [6.0.2] `(PR #592) <https://github.com/apple/foundationdb/pull/592>`_
* Incorrect accounting of incompatible connections led to occasional assertion failures. [6.0.3] `(PR #616) <https://github.com/apple/foundationdb/pull/616>`_
* A client could fail to connect to a cluster when the cluster was upgraded to a version compatible with the client. This affected upgrades that were using the multi-version client to maintain compatibility with both versions of the cluster. [6.0.4] `(PR #637) <https://github.com/apple/foundationdb/pull/637>`_
* A large number of concurrent read attempts could bring the database down after a cluster reboot. [6.0.4] `(PR #650) <https://github.com/apple/foundationdb/pull/650>`_
* Automatic suppression of trace events which occur too frequently was happening before trace events were suppressed by other mechanisms. [6.0.4] `(PR #656) <https://github.com/apple/foundationdb/pull/656>`_
* After a recovery, the rate at which transaction logs made mutations durable to disk was around 5 times slower than normal. [6.0.5] `(PR #666) <https://github.com/apple/foundationdb/pull/666>`_
* Clusters configured to use TLS could get stuck spending all of their CPU opening new connections. [6.0.5] `(PR #666) <https://github.com/apple/foundationdb/pull/666>`_
* Configuring usable_regions=2 on a cluster with a large amount of data caused commits to pause for a few seconds. [6.0.5] `(PR #687) <https://github.com/apple/foundationdb/pull/687>`_
* On clusters configured with usable_regions=2, status reported no replicas remaining when the primary DC was still healthy. [6.0.5] `(PR #687) <https://github.com/apple/foundationdb/pull/687>`_
* Clients could crash when passing in TLS options. [6.0.5] `(PR #649) <https://github.com/apple/foundationdb/pull/649>`_
* A mismatched TLS certificate and key set could cause the server to crash. [6.0.5] `(PR #689) <https://github.com/apple/foundationdb/pull/689>`_
* Databases with more than 10TB of data would pause for a few seconds after recovery. [6.0.6] `(PR #705) <https://github.com/apple/foundationdb/pull/705>`_
* Sometimes a minority of coordinators would fail to converge after a new leader was elected. [6.0.6] `(PR #700) <https://github.com/apple/foundationdb/pull/700>`_
* Calling status too many times in a 5 second interval caused the cluster controller to pause for a few seconds. [6.0.7] `(PR #711) <https://github.com/apple/foundationdb/pull/711>`_
* TLS certificate reloading could cause TLS connections to drop until process restart. [6.0.9] `(PR #717) <https://github.com/apple/foundationdb/pull/717>`_
* Configuring from usable_regions=2 to usable_regions=1 on a cluster with a large number of processes would prevent data distribution from completing. [6.0.10] `(PR #721) <https://github.com/apple/foundationdb/pull/721>`_ `(PR #739) <https://github.com/apple/foundationdb/pull/739>`_
* Watches polled the server much more frequently than intended. [6.0.10] `(PR #728) <https://github.com/apple/foundationdb/pull/728>`_
* Backup and DR didn't allow setting certain knobs. [6.0.10] `(Issue #715) <https://github.com/apple/foundationdb/issues/715>`_
* The failure monitor will become much less reactive after multiple successive failed recoveries. [6.0.10] `(PR #739) <https://github.com/apple/foundationdb/pull/739>`_
* Data distribution did not limit the number of source servers for a shard. [6.0.10] `(PR #739) <https://github.com/apple/foundationdb/pull/739>`_

Status
------

* The replication factor in status JSON is stored under ``redundancy_mode`` instead of ``redundancy.factor``. `(PR #492) <https://github.com/apple/foundationdb/pull/492>`_
* The metric ``data_version_lag`` has been replaced by ``data_lag.versions`` and ``data_lag.seconds``. `(PR #521) <https://github.com/apple/foundationdb/pull/521>`_
* Additional metrics for the number of watches and mutation count have been added and are exposed through status. `(PR #521) <https://github.com/apple/foundationdb/pull/521>`_


Bindings
--------

* API version updated to 600. There are no changes since API version 520.
* Several cases where functions in go might previously cause a panic now return a non-``nil`` error. `(PR #532) <https://github.com/apple/foundationdb/pull/532>`_
* C API calls made on the network thread could be reordered with calls made from other threads. [6.0.2] `(Issue #518) <https://github.com/apple/foundationdb/issues/518>`_
* The TLS_PLUGIN option is now a no-op and has been deprecated. [6.0.10] `(PR #710) <https://github.com/apple/foundationdb/pull/710>`_

Other Changes
-------------

* Does not support upgrades from any version older than 5.0.
* Normalized the capitalization of trace event names and attributes. `(PR #455) <https://github.com/apple/foundationdb/pull/455>`_
* Increased the memory requirements of the transaction log by 400MB. [6.0.5] `(PR #673) <https://github.com/apple/foundationdb/pull/673>`_

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
