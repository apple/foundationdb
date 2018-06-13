#############
Release Notes
#############


6.0.0
=====

5.2.4
=====

Features
--------

* tls_verify_peers splits input using the '|' character. `(PR #468) https://github.com/apple/foundationdb/pull/468`_

Fixes
-----

* fdbcli kill command did not work when TLS was enabled. `(PR #471) https://github.com/apple/foundationdb/pull/471`_

5.2.3
=====

Fixes
-----

* Backup would attempt to clear too many ranges in a single transaction when erasing log ranges. `(PR #440) https://github.com/apple/foundationdb/pull/440`_
* A read-only transaction using the ``READ_LOCK_AWARE`` option would fail if committed. `(PR #437) https://github.com/apple/foundationdb/pull/437`_

5.2.2
=====

Fixes
-----

* Build would fail on recent versions of Clang. `(PR #389) https://github.com/apple/foundationdb/pull/389/files`_
* Clusters running with TLS plugin would reject clients using non-server certificates. `(PR #396) https://github.com/apple/foundationdb/pull/396`_

5.2.1
=====

Fixes
-----

* Client input validation would handle inputs to versionstamp mutations incorrectly if the API version was less than 520. `(Issue #387) <https://github.com/apple/foundationdb/issues/387>`_

5.2.0
=====

Features
--------

* Added support for asynchronous replication to a remote DC with processes in a single cluster. This improves on the asynchronous replication offered by fdbdr because servers can fetch data from the remote DC if all replicas have been lost in one DC.
* Added support for synchronous replication of the transaction log to a remote DC. This remote DC does not need to contain any storage servers, meaning you need much fewer servers in this remote DC.

Performance
-----------

* Transaction logs do not copy mutations from previous generations of transaction logs. `(PR #339) <https://github.com/apple/foundationdb/pull/339>`_
* Load balancing temporarily avoids communicating with storage servers that have fallen behind.
* Avoid assigning storage servers responsibility for keys they do not have.

Fixes
-----

* Not all endpoint failures were reported to the failure monitor.
* Watches registered on a lagging storage server would take a long time to trigger.
* The cluster controller would not start a new generation until it recovered its files from disk.

Other Changes
-------------

* Does not support upgrades from any version older than 5.0.
* Renamed the trace log attribute ``logGroup`` to ``LogGroup``.

Earlier release notes
---------------------
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
