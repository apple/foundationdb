.. _release-notes:

#############
Release Notes
#############

7.0.0
=====

Features
--------
* Added a new API in all bindings that can be used to get a list of split points that will split the given range into (roughly) equally sized chunks. `(PR #3394) <https://github.com/apple/foundationdb/pull/3394>`_
* Added support for writing backup files directly to Azure blob storage. This is not yet performance tested on large-scale clusters. `(PR #3961) <https://github.com/apple/foundationdb/pull/3961>`_

Performance
-----------
* Improved Deque copy performance. `(PR #3197) <https://github.com/apple/foundationdb/pull/3197>`_
* Increased performance of dr_agent when copying the mutation log. The ``COPY_LOG_BLOCK_SIZE``, ``COPY_LOG_BLOCKS_PER_TASK``, ``COPY_LOG_PREFETCH_BLOCKS``, ``COPY_LOG_READ_AHEAD_BYTES`` and ``COPY_LOG_TASK_DURATION_NANOS`` knobs can be set. `(PR #3436) <https://github.com/apple/foundationdb/pull/3436>`_
* Added multiple new microbenchmarks for PromiseStream, Reference, IRandom, and timer, as well as support for benchmarking actors. `(PR #3590) <https://github.com/apple/foundationdb/pull/3590>`_
* Use xxhash3 for SQLite page checksums. `(PR #4075) <https://github.com/apple/foundationdb/pull/4075>`_
* Reduced the number of connections required by the multi-version client when loading external clients. When connecting to 7.0 clusters, only one connection with version 6.2 or larger will be used. With older clusters, at most two connections with version 6.2 or larger will be used. Clients older than version 6.2 will continue to create an additional connection each. `(PR #4667) <https://github.com/apple/foundationdb/pull/4667>`_
* Reduce CPU overhead of load balancing on client processes. `(PR #4561) <https://github.com/apple/foundationdb/pull/4561>`_

Reliability
-----------



Fixes
-----



Status
------
* Added limiting metrics (limiting_storage_durability_lag and limiting_storage_queue) to health metrics. `(PR #4067) <https://github.com/apple/foundationdb/pull/4067>`_
* Added ``commit_batching_window_size`` to the proxy roles section of status to record statistics about commit batching window size on each proxy. `(PR #4735) <https://github.com/apple/foundationdb/pull/4735>`_
* Added ``cluster.bounce_impact`` section to status to report if there will be any extra effects when bouncing the cluster, and if so, the reason for those effects. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added ``fetched_versions`` to the storage metrics section of status to report how fast a storage server is catching up in versions. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added ``fetches_from_logs`` to the storage metrics section of status to report how frequently a storage server fetches updates from transaction logs. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added ``seconds_since_last_recovered`` to the ``cluster.recovery_state`` section to report how long it has been since the cluster recovered to the point where it is able to accept requests. `(PR #3759) <https://github.com/apple/foundationdb/pull/3759>`_

Bindings
--------
* Python: The function ``get_estimated_range_size_bytes`` will now throw an error if the ``begin_key`` or ``end_key`` is ``None``. `(PR #3394) <https://github.com/apple/foundationdb/pull/3394>`_
* C: Added a function, ``fdb_database_reboot_worker``, to reboot or suspend the specified process. `(PR #4094) <https://github.com/apple/foundationdb/pull/4094>`_
* C: Added a function, ``fdb_database_force_recovery_with_data_loss``, to force the database to recover into the given datacenter. `(PR #4220) <https://github.com/apple/foundationdb/pull/4220>`_
* C: Added a function, ``fdb_database_create_snapshot``, to create a snapshot of the database. `(PR #4241) <https://github.com/apple/foundationdb/pull/4241/files>`_
* C: Added ``fdb_database_get_main_thread_busyness`` function to report how busy a client's main thread is. `(PR #4504) <https://github.com/apple/foundationdb/pull/4504>`_
* Java: Added ``Database.getMainThreadBusyness`` function to report how busy a client's main thread is. `(PR #4564) <https://github.com/apple/foundationdb/pull/4564>`_ 

Other Changes
-------------
* When ``fdbmonitor`` dies, all of its child processes are now killed. `(PR #3841) <https://github.com/apple/foundationdb/pull/3841>`_
* The ``foundationdb`` service installed by the RPM packages will now automatically restart ``fdbmonitor`` after 60 seconds when it fails. `(PR #3841) <https://github.com/apple/foundationdb/pull/3841>`_
* Capture output of forked snapshot processes in trace events. `(PR #4254) <https://github.com/apple/foundationdb/pull/4254/files>`_
* Add ErrorKind field to Severity 40 trace events. `(PR #4741) <https://github.com/apple/foundationdb/pull/4741/files>`_

Earlier release notes
---------------------
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
