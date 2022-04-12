.. _release-notes:

#############
Release Notes
#############

7.1.0
=====

Features
--------
* Added ``USE_GRV_CACHE`` transaction option to allow read versions to be locally cached on the client side for latency optimizations. `(PR #5725) <https://github.com/apple/foundationdb/pull/5725>`_ `(PR #6664) <https://github.com/apple/foundationdb/pull/6664>`_
* Added "get range and flat map" feature with new APIs (see Bindings section). Storage servers are able to generate the keys in the queries based on another query. With this, upper layer can push some computations down to FDB, to improve latency and bandwidth when read. `(PR #5609) <https://github.com/apple/foundationdb/pull/5609>`_, `(PR #6181) <https://github.com/apple/foundationdb/pull/6181>`_, etc..

Performance
-----------

Reliability
-----------

Fixes
-----

Status
------
* Added ``cluster.storage_wiggler`` field report storage wiggle stats `(PR #6219) <https://github.com/apple/foundationdb/pull/6219>`_

Bindings
--------
* C: Added ``fdb_transaction_get_range_and_flat_map`` function to support running queries based on another query in one request. `(PR #5609) <https://github.com/apple/foundationdb/pull/5609>`_
* Java: Added ``Transaction.getRangeAndFlatMap`` function to support running queries based on another query in one request. `(PR #5609) <https://github.com/apple/foundationdb/pull/5609>`_

Other Changes
-------------
* OpenTracing support is now deprecated in favor of OpenTelemetry tracing, which will be enabled in a future release. `(PR #6478) <https://github.com/apple/foundationdb/pull/6478/files>`_
* Changed ``memory`` option to limit resident memory instead of virtual memory. Added a new ``memory_vsize`` option if limiting virtual memory is desired. `(PR #6719) <https://github.com/apple/foundationdb/pull/6719>`_
* Change ``perpetual storage wiggle`` to wiggle the storage servers based on their created time. `(PR #6219) <https://github.com/apple/foundationdb/pull/6219>`_

Earlier release notes
---------------------
* :doc:`7.0 (API Version 700) </release-notes/release-notes-700>`
* :doc:`6.3 (API Version 630) </release-notes/release-notes-630>`
* :doc:`6.2 (API Version 620) </release-notes/release-notes-620>`
* :doc:`6.1 (API Version 610) </release-notes/release-notes-610>`
* :doc:`6.0 (API Version 600) </release-notes/release-notes-600>`
* :doc:`5.2 (API Version 520) </release-notes/release-notes-520>`
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
