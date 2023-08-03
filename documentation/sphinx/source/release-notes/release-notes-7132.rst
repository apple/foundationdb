#############
Release Notes
#############

71.3.2
======

Features
--------

Performance
-----------

Reliability
-----------

Fixes
-----

* Segfault in DDShardTracker where the tracker gets destroyed before ::run finished
* GRV queue leak
* The first time a GRV proxy hits its request limit it indefinitely rejects all future GRVs even after its request queue becomes empty

Status
------

Bindings
--------

Other Changes
-------------

Change Log
---------------------
* https://github.com/apple/foundationdb/pull/10690
* https://github.com/apple/foundationdb/pull/10663
* https://github.com/apple/foundationdb/pull/10721

Earlier release notes
---------------------
* :doc:`71.3.0 (API Version 700) </release-notes/release-notes-7131>`
* :doc:`71.3.1 (API Version 700) </release-notes/release-notes-7130>`