#############
Release Notes
#############

6.1.0
=====

Features
--------

Performance
-----------

Fixes
-----

Status
------

Bindings
--------

* The API to create a database has been simplified across the bindings. All changes are backward compatible with previous API versions, with one exception in Java noted below.
* C: `FDBCluster` objects and related methods (`fdb_create_cluster`, `fdb_cluster_create_database`, `fdb_cluster_set_option`, `fdb_cluster_destroy`, `fdb_future_get_cluster`) have been removed.
* C: Added `fdb_create_database` that creates a new `FDBDatabase` object synchronously and removed `fdb_future_get_database`.
* Python: Removed `fdb.init`, `fdb.create_cluster`, and `fdb.Cluster`. `fdb.open` no longer accepts a `database_name` parameter.
* Java: Deprecated `FDB.createCluster` and `Cluster`. The preferred way to get a `Database` is by using `FDB.open`, which should work in both new and old API versions.
* Java: Removed `Cluster(long cPtr, Executor executor)` constructor. This is API breaking for any code that has subclassed the `Cluster` class, and is not protected by API versioning.
* Ruby: Removed `FDB.init`, `FDB.create_cluster`, and `FDB.Cluster`. `FDB.open` no longer accepts a `database_name` parameter.
* Golang: Deprecated `fdb.StartNetwork`, `fdb.Open`, `fdb.MustOpen`, and `fdb.CreateCluster` and added `fdb.OpenDatabase` and `fdb.MustOpenDatabase`. The preferred way to start the network and get a `Database` is by using `FDB.OpenDatabase` or `FDB.OpenDefault`.
* Flow: Deprecated `API::createCluster` and `Cluster` and added `API::createDatabase`. The preferred way to get a `Database` is by using `API::createDatabase`.

Other Changes
-------------

Earlier release notes
---------------------
* :doc:`6.0 (API Version 600) </old-release-notes/release-notes-600>`
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
