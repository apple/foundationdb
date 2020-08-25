#############
Release Notes
#############

3.0.8
=====

Release 3.0.8 is protocol-compatible with all prior 3.0.x releases. All users should continue to employ the bindings released with 3.0.2, with the exception of the following:

* Node.js - updated to 3.0.6
* Ruby - updated to 3.0.7
* Java - updated to 3.0.8

Fixes
-----

* Backup: the backup agent could crash in some circumstances, preventing a backup from completing.
* Linux: On some systems, disk space usage tracking could be inaccurate.
* In rare cases, range reading could get stuck in an infinite past_version loop.
* Range reading with a begin key selector that resolved to the end of the database might not set the correct conflict range.

Java
----

* Fix: getBoundaryKeys could throw a NullPointerException.

3.0.7
=====

Release 3.0.7 is protocol-compatible with all prior 3.0.x releases. All users should continue to employ the bindings released with 3.0.2, with the exception of the following:

* Node.js - updated to 3.0.6
* Ruby - updated to 3.0.7

Fixes
-----

* ``fdbcli`` would segmentation fault if there was a semicolon after a quoted string.
* :ref:`Atomic operations <api-python-transaction-atomic-operations>` performed on keys that had been :ref:`snapshot read <api-python-snapshot-reads>` would be converted into a set operation.
* Reading a key to which an atomic operation had already been applied would cause the read to behave as a snapshot read.
* In rare scenarios, it was possible for the memory holding the result of a read to be released when a transaction was reset.
* If available RAM was negative, it was reported as a very large number in status.

Ruby
----

* Fix: ``FDB`` objects could not be garbage collected.

3.0.6
=====

Release 3.0.6 is protocol-compatible with all prior 3.0.x releases. All users should continue to employ the bindings released with 3.0.2, with the exception of the following:

* Node.js - updated to 3.0.6

Fixes
-----

* Read-latency probes for status incorrectly returned zero.
* Commit-latency probe for status included the time to acquire its read version.
* Client and server could crash when experiencing problems with network connections.

Node.js
-------

* Fix: npm source package did not compile on Mac OS X 10.9 or newer.

Windows
-------

* Added registry key during installation.

3.0.5
=====

Release 3.0.5 is protocol-compatible with all prior 3.0.x releases. This release contains only a bug fix for Windows packages; Linux and Mac OS X packages for 3.0.5 are identical to those for 3.0.4. All users should continue to employ the bindings released with 3.0.2, with the exception of the following:

* Node.js - updated to 3.0.3 if downloaded from ``npm``.

Fixes
-----

* Windows: fix Visual Studio 2013 code generation bug on older processors or versions of Windows that don't support the AVX instruction set (see https://connect.microsoft.com/VisualStudio/feedback/details/811093).

3.0.4
=====

Release 3.0.4 is protocol-compatible with all prior 3.0.x releases. Users should continue to employ the bindings released with 3.0.2, with the exception of the following:

* Node.js - updated to 3.0.3 if downloaded from ``npm``.

Fixes
-----

* Mac OS X: backup agent used 100% CPU even when idle.
* Backups were inoperative on databases with greater than 32-bit versions.
* Backup agents were not started on Windows.
* Restore required write permissions on files.
* The backup client did not report errors properly in all scenarios.
* ``fdbserver -v`` did not print the version.

Node.js
-------

* Fixed a compilation problem on Linux and Mac OS X as distributed on ``npm``. (Note: The corrected binding is distributed as version 3.0.3.)

3.0.2
=====

Upgrades
--------

* When upgrading from version 2.0.x to 3.0.x, you should consult :ref:`Upgrading to 3.0 <upgrading-from-older-versions>`.

Features
--------

* Status information provided in :doc:`machine-readable JSON </mr-status>` form.
* Differential backups and backup of selective keyspaces added to :ref:`backup tool <backup-wait>`.
* Clients may retrieve :ref:`machine-readable status <mr-status-key>`, :ref:`cluster filepath, and cluster file contents <cluster-file-client-access>` by reading designated system keys from the database.
* Two new :ref:`atomic operations <api-python-transaction-atomic-operations>`: max and min.

Performance
-----------

* Increased maximum writes per second from 200,000 to 11,000,000.
* Improved latencies, particularly on underutilized clusters.
* Improved performance of backup and restore.
* Improved client CPU usage.
* Better rate-limiting when committing very large transactions.
* Improved performance while servers rejoin the cluster.

Fixes
-----

* B-tree vacuuming could exhibit poor performance after large deletions of data.
* Computation of memory availability was not correct on newer Linux versions.
* Integers could overflow when setting range limits.
* With the memory storage engine, a key could be lost after multiple reboots in quick succession.

Client
------

* API version updated to 300. See the :ref:`API version upgrade guide <api-version-upgrade-guide-300>` for upgrade details.
* By default, :ref:`snapshot reads <snapshot isolation>` see writes within the same transaction. The previous behavior can be achieved using transaction options.
* The :ref:`transaction size limit <large-transactions>` includes conflict ranges.
* Explicitly added read or write :ref:`conflict ranges <api-python-conflict-ranges>` and :ref:`watches <api-python-watches>` for keys that begin with ``\xFF`` require one of the transaction options ``access_system_keys`` or ``read_system_keys`` to be set.
* New network options for ``trace_max_logs_size`` and ``trace_roll_size`` for an individual client's trace files.
* New transaction options: max_retry_delay, read_system_keys.
* All errors cause :ref:`watches <api-python-watches>` to trigger.
* All errors cause a transaction to reset (previously true only of some errors).

Java
----

* ``ReadTransactionContext`` added next to ``TransactionContext``, allowing ``read()`` and ``readAsync()`` composable read-only operations on transactions.
* The ``Future`` interface adds ``getInterruptibly()`` and ``blockInterruptibly()``, which propagate ``InterruptedExcetption`` to the calling code.
* Exception-handling logic is reworked in ``map()``, ``flatMap()``, and ``rescue()`` to propagate ``OutOfMemoryError`` and ``RejectedExecutionException`` instead of the spurious ``SettableAlreadySet`` exception.
* Performance is improved for applications that use many blocking-style ``get()`` calls.

Node.js
-------

* Fix: ``fdb.open``, ``fdb.createCluster``, and ``cluster.openDatabase`` didn't use the callback in API versions 22 or lower.
* Tuple performance is improved.

PHP
---

* Snapshot reads have a ``transact`` function.

Python
------

* Bindings work in Cygwin.
* The :ref:`transactional decorator <api-python-transactional-decorator>` no longer warns of a transaction approaching the 5 second limit.

Ruby
----

* Fix: ``db.get``, ``get_key``, and ``get_and_watch`` returned Futures instead of actual values.

Other changes
-------------

* Versions increase by 1 million per second instead of 1 thousand per second.
* Removed support for Ubuntu 11.10.
* Python binding has been removed from Linux packages.
* In ``fdbcli``, ``getrange`` does a prefix range read if no end key is specified.
* In ``fdbcli``, added an option to disable the initial status check.

Note on version numbers
-----------------------

Version 3.0.2 is the first publicly released version in the 3.0.x series. Versions 3.0.0-1 were limited-availability releases with the same feature set.

Earlier release notes
---------------------
* :doc:`2.0 (API Version 200) <release-notes-200>`
* :doc:`1.0 (API Version 100) <release-notes-100>`
* :doc:`Beta 3 (API Version 23) <release-notes-023>`
* :doc:`Beta 2 (API Version 22) <release-notes-022>`
* :doc:`Beta 1 (API Version 21) <release-notes-021>`
* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
