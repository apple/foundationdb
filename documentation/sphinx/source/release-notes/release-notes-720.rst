#############
Release Notes
#############

7.2.0
======

Features
--------
- Read-aware Data Distribution feature is developed for balance the read bytes bandwidth among storage servers.

Performance
-----------

Reliability
-----------

Fixes
-----

* In ``fdbcli``, integer options are now expressed as integers rather than byte strings (e.g. ``option on TIMEOUT 1000``). `(PR #7571) <https://github.com/apple/foundationdb/pull/7571>`_
* Fixed the bug in ``ConflictingKeysImpl::getRange`` which happens when an underlying conflicting range contains the read range. Added additional test coverage for validating random ``getRange`` results from special keys. `(PR #7597) <https://github.com/apple/foundationdb/pull/7597>`_
* Fixed the bug in ``SpecialKeyRangeAsyncImpl::getRange`` that the local cache is updated incorrectly after a cross-module read range if it touched more than one ``SpecialKeyRangeAsyncImpl`` in resolving key selectors. Extended the ``SpecialKeySpaceCorrectness`` workload to catch the bug. `(PR #7671) <https://github.com/apple/foundationdb/pull/7671>`_

Status
------

Bindings
--------

Other Changes
-------------

Earlier release notes
---------------------
* :doc:`7.1 (API Version 710) </release-notes/release-notes-710>`
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
