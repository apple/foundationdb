#############
Release Notes
#############

1.0.1
=====

* Fix segmentation fault in client when there are a very large number of dependent operations in a transaction and certain errors occur.

1.0.0
=====

After a year and a half of Alpha and Beta testing, FoundationDB is now commercially available. Thanks to the help of the thousands of Alpha and Beta testers in our community, we believe that this release is highly robust and capable.

You can now find pricing and order enterprise licenses online.

The new Community License now permits free-of-charge use for production systems with up to 6 server processes and for non-production systems with an unlimited number of processes.

There are only minor technical differences between this release and the 0.3.0 release of August 7, 2013:

Java
----

* ``clear(Range)`` replaces the now deprecated ``clearRangeStartsWith()``.

Python
------

* Windows installer supports Python 3.

Node and Ruby
-------------

* String option parameters are converted to UTF-8.
 
All
---

* API version updated to 100. See the :ref:`API version upgrade guide <api-version-upgrade-guide-100>` for upgrade details.
* Runs on Mac OS X 10.7.
* Improvements to installation packages, including package paths and directory modes.
* Eliminated cases of excessive resource usage in the locality API.
* Watches are disabled when read-your-writes functionality is disabled.
* Fatal error paths now call ``_exit()`` instead instead of ``exit()``.

Fixes
-----

* A few Python API entry points failed to respect the ``as_foundationdb_key()`` convenience interface.
* ``fdbcli`` could print commit version numbers incorrectly in Windows.
* Multiple watches set on the same key were not correctly triggered by a subsequent write in the same transaction.

Earlier release notes
---------------------

* :doc:`Beta 3 (API Version 23) <release-notes-023>`
* :doc:`Beta 2 (API Version 22) <release-notes-022>`
* :doc:`Beta 1 (API Version 21) <release-notes-021>`
* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
