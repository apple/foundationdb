#############
Release Notes
#############

Beta 2
======

Features
--------

* ``fdbcli`` history is stored between sessions; consecutive duplicate commands are stored as a single history entry
* The ``fdbcli`` tool prints a minimal cluster status message if an operation does not complete in 5 seconds.

Performance
-----------

* Support for databases up to 100TB (aggregate key-value size). We recommend you contact us for configuration suggestions for databases exceeding 10TB.
* Reduced client CPU usage when returning locally cached values.
* Clients do not write to the database if a value is set to its known current value.
* Improved transaction queuing behavior when a significant portion of transactions are "System Immediate" priority.
* Reduced downtime in certain server-rejoin situations.

Language APIs
-------------
	
* All

	* The API version has been updated from 21 to 22. (Thanks to our API versioning technology, programs requesting earlier API versions will work unmodified.) There are no changes required to migrate from version 21 to 22.
	* The ``open()`` call blocks until the client can communicate with the cluster.

* Node.js

	* Support for Node.js v0.10.x.
	* Functions throw errors of type ``FDBError``.
	* Removed some variables from the global scope.

* Java

	* Compiles class files with 1.6 source and target flags.
	* Single-jar packaging for all platforms. (In rare cases, setting the ``FDB_LIBRARY_PATH_FDB_JAVA`` environment variable will be requried if you previously relied on loading the library from a system path.)

* Ruby
   
	* Support for Ruby on Windows. Requires Ruby version at least 2.0.0 (x64).
	* Added implementation of ``on_ready()``.
	
Fixes
-----

* Coordinators could fail to respond if they were busy with other work.
* Fixed a rare segmentation fault on cluster shutdown.
* Fixed an issue where CLI status could sometimes fail.
* Status showed the wrong explanation when performance was limited by system write-to-read latency limit.
* Fixed a rare issue where a "stuck" process trying to participate in the database could run out of RAM.
* Increased robustness of FoundationDB server when loaded with large data sets.
* Eliminated certain cases where the data distribution algorithim could do unnecessary splitting and merging work.
* Several fixes for rare issues encountered by our fault simulation framework.
* Certain uncommon usage of on_ready() in Python could cause segmentation faults.

Earlier release notes
---------------------

* :doc:`Beta 1 (API Version 21) <release-notes-021>`
* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
