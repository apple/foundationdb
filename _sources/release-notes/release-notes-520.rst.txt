#############
Release Notes
#############

5.2.8
=====

Bindings
--------

* Java: ``FDBDatabase::run`` and ``FDBDatabase::read`` now use the ``Executor`` provided for executing asynchronous callbacks instead of the default one for the database. `(Issue #640) <https://github.com/apple/foundationdb/issues/640>`_

Fixes
-----

* A large number of concurrent read attempts could bring the database down after a cluster reboot. `(PR #650) <https://github.com/apple/foundationdb/pull/650>`_

5.2.7
=====

Bindings
--------

* The go bindings now caches database connections on a per-cluster basis. `(Issue #607) <https://github.com/apple/foundationdb/issues/607>`_

Fixes
-----

* A client could fail to connect to a cluster when the cluster was upgraded to a version compatible with the client. This affected upgrades that were using the multi-version client to maintain compatibility with both versions of the cluster. `(PR #637) <https://github.com/apple/foundationdb/pull/637>`_
* Incorrect accounting of incompatible connections led to occasional assertion failures. `(PR #637) <https://github.com/apple/foundationdb/pull/637>`_

5.2.6
=====

Features
--------

* Improved backup error specificity regarding timeouts and active connection failures. `(PR #581) <https://github.com/apple/foundationdb/pull/581>`_

Fixes
-----

* A memory leak was fixed in connection closing. `(PR #574) <https://github.com/apple/foundationdb/pull/574>`_
* A memory leak was fixed in the coordinator's handling of disconnected clients. `(PR #579) <https://github.com/apple/foundationdb/pull/579>`_
* Aligned memory allocation on MacOS was sometimes failing to allocate memory, causing a crash. `(PR #547) <https://github.com/apple/foundationdb/pull/547>`_

5.2.5
=====

Features
--------

* Backup and DR share a single mutation log when both are being used on the same cluster. Ongoing backups will be aborted when upgrading to 5.2. `(PR #3) <https://github.com/apple/foundationdb/pull/3>`_
* Added a TLS plugin implementation. `(PR #343) <https://github.com/apple/foundationdb/pull/343>`_
* Backup supports HTTPS for blobstore connections. `(PR #343) <https://github.com/apple/foundationdb/pull/343>`_
* Added the APPEND_IF_FITS atomic operation. `(PR #22) <https://github.com/apple/foundationdb/pull/22>`_
* Updated the SET_VERSIONSTAMPED_KEY atomic operation to take four bytes to specify the offset instead of two (if the API version is set to 520 or higher). `(Issue #148) <https://github.com/apple/foundationdb/issues/148>`_
* Updated the SET_VERSIONSTAMPED_VALUE atomic operation to place the versionstamp at a specified offset in a value (if the API version is set to 520 or higher). `(Issue #148) <https://github.com/apple/foundationdb/issues/148>`_
* tls_verify_peers splits input using the '|' character. [5.2.4] `(PR #468) <https://github.com/apple/foundationdb/pull/468>`_
* Added knobs and blob Backup URL parameters for operations/sec limits by operation type. [5.2.5] `(PR #513) <https://github.com/apple/foundationdb/pull/513>`_

Performance
-----------

* Improved backup task prioritization. `(PR #71) <https://github.com/apple/foundationdb/pull/71>`_

Fixes
-----

* The client did not clear the storage server interface cache on endpoint failure for all request types. This causes up to one second of additional latency on the first get range request to a rebooted storage server. `(Issue #351) <https://github.com/apple/foundationdb/issues/351>`_
* Client input validation would handle inputs to versionstamp mutations incorrectly if the API version was less than 520. [5.2.1] `(Issue #387) <https://github.com/apple/foundationdb/issues/387>`_
* Build would fail on recent versions of Clang. [5.2.2] `(PR #389) <https://github.com/apple/foundationdb/pull/389/files>`_
* Clusters running with TLS plugin would reject clients using non-server certificates. [5.2.2] `(PR #396) <https://github.com/apple/foundationdb/pull/396>`_
* Backup would attempt to clear too many ranges in a single transaction when erasing log ranges. [5.2.3] `(PR #440) <https://github.com/apple/foundationdb/pull/440>`_
* A read-only transaction using the ``READ_LOCK_AWARE`` option would fail if committed. [5.2.3] `(PR #437) <https://github.com/apple/foundationdb/pull/437>`_
* fdbcli kill command did not work when TLS was enabled. [5.2.4] `(PR #471) <https://github.com/apple/foundationdb/pull/471>`_
* Don't disable certificate checks by default. [5.2.5] `(PR #511) <https://github.com/apple/foundationdb/pull/511>`_

Status
------

* Available space metrics for the memory storage engine take into account both memory and disk. `(PR #41) <https://github.com/apple/foundationdb/pull/41>`_
* Added metrics for read bytes per second and read keys per second. `(PR #303) <https://github.com/apple/foundationdb/pull/303>`_

Bindings
--------

* API version updated to 520. See the :ref:`API version upgrade guide <api-version-upgrade-guide-520>` for upgrade details.
* Java and Python: Versionstamp packing methods within tuple class now add four bytes for the offset instead of two if the API version is set to 520 or higher. `(Issue #148) <https://github.com/apple/foundationdb/issues/148>`_
* Added convenience methods to determine if an API version has been set. `(PR #72) <https://github.com/apple/foundationdb/pull/72>`_
* Go: Reduce memory allocations when packing tuples. `(PR #278) <https://github.com/apple/foundationdb/pull/278>`_
* Python: Correctly thread the versionstamp offset when there are incomplete versionstamps within nested tuples. `(Issue #356) <https://github.com/apple/foundationdb/issues/356>`_
* Java: Length in ``Tuple.fromBytes`` is now honored if specified. `(Issue #362) <https://github.com/apple/foundationdb/issues/362>`_

Other Changes
-------------

* Deprecated the read_ahead_disable option. The commit_on_first_proxy, debug_dump, and check_writes_enable options are no longer exposed through the bindings. `(PR #134) <https://github.com/apple/foundationdb/pull/134>`_

Earlier release notes
---------------------
* :doc:`5.1 (API Version 510) </release-notes/release-notes-510>`
* :doc:`5.0 (API Version 500) </release-notes/release-notes-500>`
* :doc:`4.6 (API Version 460) </release-notes/release-notes-460>`
* :doc:`4.5 (API Version 450) </release-notes/release-notes-450>`
* :doc:`4.4 (API Version 440) </release-notes/release-notes-440>`
* :doc:`4.3 (API Version 430) </release-notes/release-notes-430>`
* :doc:`4.2 (API Version 420) </release-notes/release-notes-420>`
* :doc:`4.1 (API Version 410) </release-notes/release-notes-410>`
* :doc:`4.0 (API Version 400) </release-notes/release-notes-400>`
* :doc:`3.0 (API Version 300) </release-notes/release-notes-300>`
* :doc:`2.0 (API Version 200) </release-notes/release-notes-200>`
* :doc:`1.0 (API Version 100) </release-notes/release-notes-100>`
* :doc:`Beta 3 (API Version 23) </release-notes/release-notes-023>`
* :doc:`Beta 2 (API Version 22) </release-notes/release-notes-022>`
* :doc:`Beta 1 (API Version 21) </release-notes/release-notes-021>`
* :doc:`Alpha 6 (API Version 16) </release-notes/release-notes-016>`
* :doc:`Alpha 5 (API Version 14) </release-notes/release-notes-014>`
