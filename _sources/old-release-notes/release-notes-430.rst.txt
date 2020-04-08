#############
Release Notes
#############

4.3.0
=====
    
Features
--------

* Improved DR thoughput by having mutations copied into the DR database before applying them.
* Renamed db_agent to dr_agent.
* Added more detailed DR and backup active task detail into layer status.

Fixes
-----

* Backup seconds behind did not update in continuous mode.
* DR layer status did not report correctly.
* The Java bindings had an incorrectly named native extension on Linux.
* DR status would crash if called before a DR had been started.
* Changed the blob restore read pattern to work around blob store issues.
* External clients do not load environment variable options.

Bindings
--------

* API version updated to 430. There are no behavior changes in this API version. See the :ref:`API version upgrade guide <api-version-upgrade-guide-430>` for upgrade details.

Earlier release notes
---------------------
* :doc:`4.2 (API Version 420) <release-notes-420>`
* :doc:`4.1 (API Version 410) <release-notes-410>`
* :doc:`4.0 (API Version 400) <release-notes-400>`
* :doc:`3.0 (API Version 300) <release-notes-300>`
* :doc:`2.0 (API Version 200) <release-notes-200>`
* :doc:`1.0 (API Version 100) <release-notes-100>`
* :doc:`Beta 3 (API Version 23) <release-notes-023>`
* :doc:`Beta 2 (API Version 22) <release-notes-022>`
* :doc:`Beta 1 (API Version 21) <release-notes-021>`
* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
