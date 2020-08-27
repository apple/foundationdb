#############
Release Notes
#############

4.1.1
=====

Fixes
-----

* Many short-lived file access metrics were being created.
* A completed backup could be improperly marked as incomplete.
* In rare scenarios the resolvers could fail to make progress.

4.1.0
=====

Performance
-----------

* Significantly improved cluster performance in a wide variety of machine failure scenarios.
    
Features
--------

* Clients can now load multiple versions of the client library, and will gracefully switch to the appropriate version when the server is upgraded.
* A new operating mode for ``fdbbackup`` writes backup data files into the blob store.
* Transactions no longer automatically reset after a successful commit.
* Added ability to set network options with environment variables.
* Added a new API function for determining the value to which atomic versionstamp operations in a transaction were transformed or would have been transformed.
* Improved logic for integrating manually-assigned machine classes with other constraints on role locations.
* Added a new machine class ``stateless`` which is the top priority location for resolvers, proxies, and masters.
* Added a new machine class ``log`` which is the top priority location for transaction logs.
* Trace events are now event metrics that are exposed in Scope.

Fixes
-----

* A log could attempt to recover from a partially recovered set of logs when fast recovery was enabled.
* A rare scenario could cause a crash when a master is recovering metadata from the previous generation of logs.
* Streaming mode ``EXACT`` was ignoring the ``target_bytes`` parameter.

Bindings
--------

* API version updated to 410. See the :ref:`API version upgrade guide <api-version-upgrade-guide-410>` for upgrade details.

Earlier release notes
---------------------
* :doc:`4.0 (API Version 400) <release-notes-400>`
* :doc:`3.0 (API Version 300) <release-notes-300>`
* :doc:`2.0 (API Version 200) <release-notes-200>`
* :doc:`1.0 (API Version 100) <release-notes-100>`
* :doc:`Beta 3 (API Version 23) <release-notes-023>`
* :doc:`Beta 2 (API Version 22) <release-notes-022>`
* :doc:`Beta 1 (API Version 21) <release-notes-021>`
* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
