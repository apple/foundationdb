#############
Release Notes
#############

7.0.0
=====

Performance
-----------

Fixes
-----

Status
------
* Replaced ``cluster.database_locked`` status field with ``cluster.database_lock_state``, which contains two subfields: ``locked`` (boolean) and ``lock_uid`` (which contains the database lock uid if the database is locked). `(PR #2058) <https://github.com/apple/foundationdb/pull/2058>`_

Bindings
--------
* API version updated to 700. See the :ref:`API version upgrade guide <api-version-upgrade-guide-700>` for upgrade details.
* Java: Introduced ``keyAfter`` utility function that can be used to create the immediate next key for a given byte array. `(PR #2458) <https://github.com/apple/foundationdb/pull/2458>`_
* C: The ``FDBKeyValue`` struct's ``key`` and ``value`` members have changed type from ``void*`` to ``uint8_t*``. `(PR #2622) <https://github.com/apple/foundationdb/pull/2622>`_

Other Changes
-------------
* Double the number of shard locations that the client will cache locally. `(PR #2198) <https://github.com/apple/foundationdb/pull/2198>`_
* Add an option for transactions to report conflicting keys by calling getRange with the special key prefix \xff\xff/transaction/conflicting_keys/. `(PR 2257) <https://github.com/apple/foundationdb/pull/2257>`_

Earlier release notes
---------------------
* :doc:`6.2 (API Version 620) </old-release-notes/release-notes-620>`
* :doc:`6.1 (API Version 610) </old-release-notes/release-notes-610>`
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
