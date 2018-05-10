#############
Release Notes
#############

5.2.0
=====

Features
--------

* Backup and DR share a single mutation log when both are being used on the same cluster. Ongoing backups will be aborted when upgrading to 5.2. `(PR #3) <https://github.com/apple/foundationdb/pull/3>`_
* Added a TLS plugin implementation. `(PR #343) <https://github.com/apple/foundationdb/pull/343>`_
* Backup supports HTTPS for blobstore connections. `(PR #343) <https://github.com/apple/foundationdb/pull/343>`_
* Added the APPEND_IF_FITS atomic operation. `(PR #22) <https://github.com/apple/foundationdb/pull/22>`_
* Updated the SET_VERSIONSTAMPED_VALUE atomic operation to place the versionstamp at a specified offset in a value. `(Issue #148) <https://github.com/apple/foundationdb/issues/148>`_

Performance
-----------

* Improved backup task prioritization. `(PR #71) <https://github.com/apple/foundationdb/pull/71>`_

Status
------

* Available space metrics for the memory storage engine take into account both memory and disk. `(PR #41) <https://github.com/apple/foundationdb/pull/41>`_
* Added metrics for read bytes per second and read keys per second. `(PR #303) <https://github.com/apple/foundationdb/pull/303>`_

Bindings
--------

* API version updated to 520.
* Added convenience methods to determine if an API version has been set. `(PR #72) <https://github.com/apple/foundationdb/pull/72>`_
* Go: Reduce memory allocations when packing tuples. `(PR #278) <https://github.com/apple/foundationdb/pull/278>`_

Other Changes
-------------

* Deprecated the read_ahead_disable option. The commit_on_first_proxy, debug_dump, and check_writes_enable options are no longer exposed through the bindings. `(PR #134) <https://github.com/apple/foundationdb/pull/134>`_

Earlier release notes
---------------------
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
