#############
Release Notes
#############

FoundationDB Alpha 6
====================

Platform support
----------------

* FoundationDB now supports both clients and development servers on :doc:`Mac OS X </getting-started-mac>`.

* FoundationDB now supports both clients and development servers on (64-bit) Windows.

* All language APIs are supported on Linux, Mac, and Windows (except for Ruby on Windows, because there is not a 64-bit Ruby for Windows.)

Features
--------

* The set of coordination servers can be safely :ref:`changed <configuration-changing-coordination-servers>` on-the-fly via the CLI.

* Unintentional deletion of the coordination state files is now ACID-safe and self-correcting when a majority of the state files still exist. 

* The :ref:`foundationdb.conf <foundationdb-conf>` file format has changed.

* A new more flexible and automatic system for :ref:`network configuration <foundationdb-conf-fdbserver>`. Common server setups will auto-configure using the cluster file. More advanced setups are supported via separate configurable listen and public addresses.

* The CLI now support tab-completion.

* The CLI now supports setting transaction options

* The CLI has a new command "getrangekeys" that returns the keys in a range and omits the values.

* The database size estimate shown in the CLI status is much more accurate.

Performance
-----------

* Improved latency performance for intense workloads with range-read operations.

* Improved performance and decreased memory usage for certain intense write workloads targeting a small set of keys (such as sequential insert).

Fixes
-----

* An incorrect result could be returned by a range read when: (1) The range start was specified using a non-default "less than" type key selector; and (2) the range read started at the beginning of the database; and (3) the transaction also included a prior write to a key less than the key of the begin key selector.

* In certain cases a FoundationDB cluster would not correctly re-configure itself to achieve a more optimal usage of servers of specific machine classes.

Changes to all APIs
-------------------

* The API version has been updated from 14 to 16. (Thanks to our API versioning technology, programs requesting API version 14 will work unmodified.)

* Calling the :py:meth:`reset <fdb.Transaction.reset>` method of a transaction now also resets transaction options.

* :ref:`System keys <system-keys>` (those beginning with the byte ``0xFF``) are now inaccessible by default.

* Simpler network setup: The network connection options are no longer necessary and have been deprecated.

* Three new transaction options (:py:meth:`READ_AHEAD_DISABLE <fdb.Transaction.options.set_read_ahead_disable>`, :py:meth:`READ_YOUR_WRITES_DISABLE <fdb.Transaction.options.set_read_your_writes_disable>`, and :py:meth:`ACCESS_SYSTEM_KEYS <fdb.Transaction.options.set_access_system_keys>`) enable more control for advanced applications.

Changes to the Java API
-----------------------

* A new construct `AsyncUtil.whileTrue() <../javadoc/com/apple/cie/foundationdb/async/AsyncUtil.html#whileTrue-com.apple.foundationdb.async.Function->`_ simplifies writing loops using the asynchronous version of the Java FDB client.

Earlier release notes
---------------------

For changes in alpha 5, see :doc:`Release Notes (Alpha 5) <release-notes-014>`.
