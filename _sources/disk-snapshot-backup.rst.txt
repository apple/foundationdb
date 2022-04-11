
.. _disk-snapshot-backups:

#################################
Disk snapshot backup and Restore
#################################

This document covers disk snapshot based backup and restoration of a FoundationDB database. This tool leverages disk level snapshots and gets a point-in-time consistent copy of the database. The disk snapshot backup can be used for test and development purposes, for compliance reasons or to provide an additional level of protection in case of hardware or software failures.

.. _disk-snapshot-backup-introduction:

Introduction
============

FoundationDB's disk snapshot backup tool makes a consistent, point-in-time backup of FoundationDB database without downtime by taking crash consistent snapshot of all the disk stores that have persistent data.

The prerequisite of this feature is to have crash consistent snapshot support on the filesystem (or the disks) on which FoundationDB is running.

The disk snapshot backup tool orchestrates the snapshotting of all the disk images and ensures that they are restorable to a consistent point in time.

Restore is achieved by copying or attaching the disk snapshot images to FoundationDB compute instances. Restore behaves as if the cluster were powered down and restarted.

Backup vs Disk snapshot backup
==============================
Backup feature already exists in FoundationDB and is detailed here :ref:`backups`, any use of fdbbackup will refer to this feature.

Both fdbbackup and Disk snapshot backup tools provide a point-in-time consistent backup of FoundationDB database, but they operate at different levels and there are differences in terms of performance, features and external dependency.

fdbbackup operates at the key-value level. Backup involves copying of all the key-value pairs from the source cluster and restore involves applying all the key-value pairs to the destination database. Performance depends on the amount of data and the throughput with which the data can be read and written. This approach has no external dependency, there is no requirement for any snapshotting feature from the disk system. Additionally, it has an option for continuous backup with the flexibility to pick a restore point.

Disk snapshot backup and restore are generally high performance because it operates at disk level and data is not read or written through the FoundationDB stack. In environments where disk snapshot and restore are highly performant this approach can be very fast. Frequent backups can be done as a substitute to continuous backup if the backups are performant.

Limitations
===========

* No support for continuous backup
* Feature is not supported on Windows operating system
* Data encryption is dependent on the disk system
* Backup and restore involves tooling which are deployment and environment specific to be developed by operators
* ``snapshot`` command is a hidden fdbcli command in the current release and will be unhidden in a future patch release.

Disk snapshot backup steps
==========================

``snapshot``
    This command line tool is used to create the snapshot. It takes a full path to a ``snapshot create binary`` and reports the status. Optionally, it can take additional arguments to be passed down to the ``snapshot create binary``. It returns a unique identifier which can be used to identify all the disk snapshots of a backup. Even in case of failures the unique identifier is returned to identify and clear any partially create disk snapshots.

In response to the snapshot request from the user, FoundationDB will run the user specified ``snapshot create binary`` on all processes which have persistent data, binary should call filesystem/disk system specific snapshot create API.

Before using the ``snapshot`` command the following setup needs to be done

* Write a program that will snapshot the local disk store when invoked by the ``fdbserver`` with the following arguments:

  - UID - 32 byte alpha-numeric unique identifier, the same identifier will be passed to all the nodes in the cluster, can be used to identify the set of disk snapshots associated with this backup
  - Version - version string of the FoundationDB binary
  - Path - path of the FoundationDB ``datadir`` to be snapshotted, ``datadir`` specified in :ref:`foundationdb-conf-fdbserver`
  - Role - ``tlog``/``storage``/``coordinator``, identifies the role of the node on which the snapshot is being invoked

* Install ``snapshot create binary`` on the FoundationDB instance in a secure path that can be invoked by the ``fdbserver``
* Set a new config parameter ``whitelist_binpath`` in :ref:`foundationdb-conf-fdbserver`, whose value is the ``snapshot create binary`` absolute path. Running any ``snapshot`` command will validate that it is in the ``whitelist_binpath``. This is a security mechanism to stop running a random/insecure command on the cluster by a client using the ``snapshot`` command. Example configuration entry will look like::

    whitelist_binpath = "/bin/snap_create.sh"

* ``snapshot create binary`` should capture any additional data needed to restore the cluster. Additional data can be stored as tags in cloud environments or it can be stored in an additional file/directory in the ``datadir`` and then snapshotted. The section :ref:`disk-snapshot-backup-specification` describes the recommended specification of the list of things that can be gathered by the binary.
* Program should return a non-zero status for any failures and zero for success
* If the ``snapshot create binary`` process takes longer than 5 minutes to return a status then it will be killed and ``snapshot`` command will fail. Timeout of 5 minutes is configurable and can be set with ``SNAP_CREATE_MAX_TIMEOUT`` config parameter in :ref:`foundationdb-conf-fdbserver`. Since the default value is large enough, there should not be a need to modify this configuration.

``snapshot`` is a synchronous command and when it returns successfully backup is considered complete and restorable. The time it takes to finish a backup is a function of the time it takes to snapshot the disk store. For example, if disk snapshot takes 1 second, time to finish backup should be less than < 10 seconds, this is general guidance and in some cases it may take longer. If the command is aborted by the user then the disk snapshots should not be used for restore, because the state of backup is undefined. If the command fails or aborts, operator can retry by issuing another ``snapshot`` command.

Example ``snapshot`` command usage::

    fdb> snapshot /bin/snap_create.sh --param1 param1-value --param2 param2-value
    Snapshot command succeeded with UID c50263df28be44ebb596f5c2a849adbb

will invoke the ``snapshot create binary`` on ``tlog`` role with the following arguments::

    --param1 param1-value --param2 param2-value --path /mnt/circus/data/4502 --version 6.2.6 --role tlog --uid c50263df28be44ebb596f5c2a849adbb


.. _disk-snapshot-backup-specification:

Disk snapshot backup specification
----------------------------------

Details the list of artifacts the ``snapshot create binary`` should gather to aid the restore.

================================  ========================================================   ========================================================
Field Name                        Description                                                Source of information
================================  ========================================================   ========================================================
``UID``                           unique identifier passed with all the                      ``snapshot`` CLI command output contains the UID
                                  snapshot create binary invocations associated with
                                  a backup. Disk snapshots could be tagged with this UID.
``FoundationDB Server Version``   software version of the ``fdbserver``                      command line argument to snap create binary
``CreationTime``                  current system date and time                               time obtained by calling the system time
``FoundationDB Cluster File``     cluster file which has cluster-name, magic and             read from the location of the cluster file location
                                  the list of coordinators, cluster file is detailed         mentioned in the command line arguments. Command
                                  here :ref:`foundationdb-cluster-file`                      line arguments of ``fdbserver`` can be accessed from
                                                                                             /proc/$PPID/cmdline
``Config Knobs``                  command line arguments passed to ``fdbserver``             available from command line arguments of ``fdbserver``
                                                                                             or from foundationdb.conf
``IP Address + Port``             host address and port information of the ``fdbserver``     available from command line arguments of ``fdbserver``
                                  that is invoking the snapshot
``LocalityData``                  machine id, zone id or any other locality information      available from command line arguments of ``fdbserver``
``Name for the snapshot file``    recommended name for the disk snapshot                     cluster-name:ip-addr:port:UID
================================  ========================================================   ========================================================

``snapshot create binary`` will not be invoked on processes which does not have any persistent data (for example, Cluster Controller or Master or CommitProxy). Since these processes are stateless, there is no need for a snapshot. Any specialized configuration knobs used for one of these stateless processes need to be copied and restored externally.

Management of disk snapshots
----------------------------

Unused disk snapshots or disk snapshots that are part of failed backups have to deleted by the operator externally.

Error codes
-----------

Error codes returned by ``snapshot`` command

======================================= ============ ============================= =============================================================
Name                                    Code         Description                    Comments
======================================= ============ ============================= =============================================================
snap_path_not_whitelisted               2505         Snapshot create binary path   Whitelist the ``snap create binary`` path and retry the
                                                     not whitelisted               operation.
snap_not_fully_recovered_unsupported    2506         Unsupported when the cluster  Wait for the cluster to finish recovery and then retry the
                                                     is not fully recovered        operation
snap_log_anti_quorum_unsupported        2507         Unsupported when log anti     Feature is not supported when log anti quorum is configured
                                                     quorum is configured
snap_with_recovery_unsupported          2508         Cluster recovery during       Recovery happened while snapshot operation was in progress,
                                                     snapshot operation not        retry the operation.
                                                     supported
snap_storage_failed                     2501         Failed to snapshot storage    Verify that the ``snap create binary`` is installed and
                                                     nodes                         can be executed by the user running ``fdbserver``
snap_tlog_failed                        2502         Failed to snapshot TLog            ,,
                                                     nodes
snap_coord_failed                       2503         Failed to snapshot                 ,,
                                                     coordinator nodes
unknown_error                           4000         An unknown error occurred          ,,
snap_disable_tlog_pop_failed            2500         Disk Snapshot error           No operator action is needed, retry the operation
snap_enable_tlog_pop_failed             2504         Disk Snapshot error                ,,
======================================= ============ ============================= =============================================================


Disk snapshot restore steps
===========================

Restore is the process of building up the cluster from the snapshotted disk images. There is no option to specify a restore version because there is no support for continuous backup. Here is the list of steps for the restore process:

* Identify the snapshot disk images associated with the backup to be restored with the help of UID or creation time
* Group disk images of a backup by IP address and/or locality information
* Bring up a new cluster similar to the source cluster with FoundationDB services stopped and either attach the snapshot disk images or copy the snapshot disk images to the cluster in the following manner:

  * Map the old IP address to new IP address in a one to one fashion and use that mapping to guide the restoration of disk images
* Compute the new fdb.cluster file based on where the new ``coordinators`` disk stores are placed and push it to the all the instances in the new cluster
* Start the FoundationDB service on all the instances
* NOTE: Process can have multiple roles with persistent data which share the same ``datadir``. ``snapshot create binary`` will create multiple snapshots, one per role. In such case, snapshot disk images needs to go through additional processing before restore, if a snapshot image of a role has files that belongs to other roles then they need to be deleted.

Cluster will start and get to healthy state indicating the completion of restore. Applications can optionally do any additional validations and use the cluster.


Example backup and restore steps 
================================

Here are the backup and restore steps on an over simplified setup with a single node cluster and ``cp`` command to create snapshots and restore. This is purely for illustration, real world backup and restore scripts needs to follow all the steps detailed above.


* Create a single node cluster by following the steps here :ref:`building-cluster`

* Check the status of the cluster and write a few sample keys::
  
    fdb> status

    Using cluster file `/mnt/source/fdb.cluster'.

    Configuration:
      Redundancy mode        - single
      Storage engine         - ssd-2
      Coordinators           - 1

    Cluster:
      FoundationDB processes - 1
      Zones                  - 1
      Machines               - 1
      Memory availability    - 30.6 GB per process on machine with least available
      Fault Tolerance        - 0 machines
      Server time            - 12/11/19 04:02:57

    Data:
      Replication health     - Healthy
      Moving data            - 0.000 GB
      Sum of key-value sizes - 0 MB
      Disk space used        - 210 MB

    Operating space:
      Storage server         - 72.6 GB free on most full server
      Log server             - 72.6 GB free on most full server

    Workload:
      Read rate              - 9 Hz
      Write rate             - 0 Hz
      Transactions started   - 5 Hz
      Transactions committed - 0 Hz
      Conflict rate          - 0 Hz

    Backup and DR:
      Running backups        - 0
      Running DRs            - 0

    Client time: 12/11/19 04:02:57

    fdb> writemode on
    fdb> set key1 value1
    Committed (76339236)
    fdb> set key2 value2
    Committed (80235963)

* Write a ``snap create binary`` which copies the ``datadir`` to a user passed destination directory location::

    #!/bin/sh

    while (( "$#" )); do
        case "$1" in
            --uid)
                SNAPUID=$2
                shift 2
                ;;
            --path)
                DATADIR=$2
                shift 2
                ;;
            --role)
                ROLE=$2
                shift 2
                ;;
            --destdir)
                DESTDIR=$2
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done

    mkdir -p "$DESTDIR/$SNAPUID/$ROLE" || exit 1
    cp "$DATADIR/"* "$DESTDIR/$SNAPUID/$ROLE/" || exit 1

    exit 0

* Install the ``snap create binary`` as ``/bin/snap_create.sh``, add the entry for ``whitelist_binpath`` in :ref:`foundationdb-conf-fdbserver`, stop and start the foundationdb service for the configuration change to take effect
* Issue ``snapshot`` command as follows::

    fdb> snapshot /bin/snap_create.sh --destdir /mnt/backup
    Snapshot command succeeded with UID 69a5e0576621892f85f55b4ebfeb4312

* ``snapshot create binary`` gets invoked once for each role namely ``tlog``, ``storage`` and ``coordinator`` in this process with the following arguments::

    --path /mnt/source/datadir --version 6.2.6 --role storage --uid 69a5e0576621892f85f55b4ebfeb4312 --destdir /mnt/backup
    --path /mnt/source/datadir --version 6.2.6 --role tlog --uid 69a5e0576621892f85f55b4ebfeb4312 --destdir /mnt/backup
    --path /mnt/source/datadir --version 6.2.6 --role coord --uid 69a5e0576621892f85f55b4ebfeb4312 --destdir /mnt/backup

* Snapshot is successful and all the snapshot images are in ``destdir`` specified by the user in the command line argument to ``snapshot`` command, here is a sample directory listing of one of the coordinator backup directory::

    $ ls /mnt/backup/69a5e0576621892f85f55b4ebfeb4312/coord/
    coordination-0.fdq                                     log2-V_3_LS_2-b9990ae9bc00672f07264ad43d9d0792.sqlite-wal  processId
    coordination-1.fdq                                     logqueue-V_3_LS_2-b9990ae9bc00672f07264ad43d9d0792-0.fdq   storage-f0e72cdfed12a233e0e58291150ca597.sqlite
    log2-V_3_LS_2-b9990ae9bc00672f07264ad43d9d0792.sqlite  logqueue-V_3_LS_2-b9990ae9bc00672f07264ad43d9d0792-1.fdq   storage-f0e72cdfed12a233e0e58291150ca597.sqlite-wal

* To restore the ``coordinator`` backup image, setup a restore ``datadir`` and copy all the ``coordinator`` related files to it::

    $ cp /mnt/backup/69a5e0576621892f85f55b4ebfeb4312/coord/coord* /mnt/restore/datadir/

* Repeat the above steps to restore ``storage`` and ``tlog`` backup images
* Prepare the ``fdb.cluster`` for the restore with new ``coordinator`` IP address, example::

    znC1NC5b:iYHJLq7z@10.2.80.40:4500 -> znC1NC5b:iYHJLq7z@10.2.80.41:4500
* ``foundationdb.conf`` can be exact same copy as the source cluster for this example
* Once all the backup images are restored, start a new fdbserver with the ``datadir`` pointing to ``/mnt/restore/datadir`` and the new ``fdb.cluster``.
* Verify the cluster is healthy and check the sample keys that we added are there::

    fdb> status

    Using cluster file `/mnt/restore/fdb.cluster'.

    Configuration:
      Redundancy mode        - single
      Storage engine         - ssd-2
      Coordinators           - 1

    Cluster:
      FoundationDB processes - 1
      Zones                  - 1
      Machines               - 1
      Memory availability    - 30.5 GB per process on machine with least available
      Fault Tolerance        - 0 machines
      Server time            - 12/11/19 09:04:53

    Data:
      Replication health     - Healthy
      Moving data            - 0.000 GB
      Sum of key-value sizes - 0 MB
      Disk space used        - 210 MB

    Operating space:
      Storage server         - 72.5 GB free on most full server
      Log server             - 72.5 GB free on most full server

    Workload:
      Read rate              - 7 Hz
      Write rate             - 0 Hz
      Transactions started   - 3 Hz
      Transactions committed - 0 Hz
      Conflict rate          - 0 Hz

    Backup and DR:
      Running backups        - 0
      Running DRs            - 0

    Client time: 12/11/19 09:04:53

    fdb> get key1
    `key1' is `value1'
    fdb> get key2
    `key2' is `value2'
