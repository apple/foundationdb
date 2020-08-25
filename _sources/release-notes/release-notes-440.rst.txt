#############
Release Notes
#############

4.4.2
=====

Features
--------

* Backup's minimum unit of progress is now a single committed version, allowing progress to be made when the database is very unhealthy.

Fixes
--------

* Options being disabled in fdbcli required an unnecessary parameter.
* In rare situations, an incorrect backup index could be written.  Contact us if you need to restore data from a v4.4.1 or earlier backup.
* A crash could occur on startup in fdbbackup and fdbcli.
* A data corruption bug observed on OS X was fixed.  The issue has never been observed on other platforms.

4.4.1
=====
    
Features
--------

* Added support for streaming writes. This allows a client to load an ordered list of mutations into the database in parallel, and once they are all loaded, the mutations will be applied to the database in order.
* DR uses streaming writes to significantly improve throughput.
* Restore was rewritten so that many clients can partipate in restoring data, significantly improving restore speed. The command line restore tool interface has been updated to support this new capability.
* Cluster files now support comments (using the '#' character).
* A wide variety of new client-side statistics are logged in client trace files every 5 seconds.
* Status reports the generation of the system. The generation is incremented every time there is a failure (and recovery) in the transaction subsystem.
* Added a new machine-wide identification token. This token is used in place of the user-supplied "machine ID" in instances where true physical machine is the unit of interest. This change will allow for reporting tools to output the actual number of physical machines present in a cluster.
* Added per-process metrics for total disk capacity and free space to status json output that allow for more repeatable and expected reporting of host disk usage. These metrics are based on the "data-dir" parameter to fdbserver and will be reported without regard to whether the process is using the disk or not.
* Added backup size estimates to status json output.
* Added process uptime seconds to status json output.
* Added a flag indicating whether the database is locked to status json output.

Fixes
-----

* Only processes which can become logs are counted towards fault tolerance.
* A long running process would have a local estimate of time which differed greatly from system clock of the machine the process was running on.
* DR errors were not being reported properly in DR status.
* Backup and DR layer status expiration and cleanup now use database read version instead of time. <rdar://problem/24805824>

Bindings
--------

* API version updated to 440. There are no behavior changes in this API version. See the :ref:`API version upgrade guide <api-version-upgrade-guide-440>` for upgrade details.

Java
----

* The ``ReadTransaction`` interface supports the ability to set transaction options.

Other Changes
-------------

* Removed support for the old log system (pre 3.0). To upgrade to 4.4+ from a version before 3.0, first upgrade to a version between 3.0 and 4.3.
* Removed trace event spam in backup and DR.
* Backup and DR only report the most recent error, rather than a list of errors.
* Updated language binding 'API version not supported' error message to include the version requested and supported. <rdar://problem/23769929>

Earlier release notes
---------------------
* :doc:`4.3 (API Version 430) <release-notes-430>`
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
