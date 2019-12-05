
.. _disk-snapshot-backups:

#################################
Disk Snapshot Backup and Restore
#################################

This document covers disk snapshot based backup and restoration of a FoundationDB database. This tool leverages disk level snapshots and gets a point-in-time consistent copy of the database. The disk snapshot backup can be used to provide additional level of protection in case of hardware or software failures, test and development purposes or for compliance reasons.

.. _disk-snapshot-backup-introduction:

Introduction
============

FoundationDB's disk snapshot backup tool makes a consistent, point-in-time backup of FoundationDB database without downtime by taking crash consistent snapshot of all the disk stores that have persistent data.

The prerequisite of this feature is to have crash consistent snapshot support on the filesystem (or the disks) in which FoundationDB is running on.

Disk snapshot backup tool orchestrates the snapshotting of all the disk images and ensures that they are restorable in a point-in-time consistent basis.

Restore is achieved by copying or attaching the disk snapshot images to FoundationDB compute instances. Restore behaves as if the cluster was powered down and restarted.

Backup vs Disk snapshot backup
==============================

Both these tools provide a point-in-time consistent backup of FoundationDB database, they operate at different levels and there are differences in terms of performance, features and external dependency.

Backup/fdbbackup operates at the key-value level, backup will involve copying of all the key-values from the source cluster and the restore will involve applying all the key-values to the destination database. Performance will depend on the amount of data and the throughput with which the data can be read and written. This approach is agnostic to external dependency, there is no requirement for any snapshotting feature from disk system. Additionally, it has an option for continuous backup that enables a restorable point-in-time very close to now. This feature already exists in FoundationDB and is detailed here :ref:`backups`.

Disk snapshot backup and restore are generally high performant because it deals at disk level and data is not read or written through FoundationDB stack. In environments where disk snapshot and restore are highly performant this approach can be very fast. Feature is strictly dependent on crash consistent snapshot feature from disk system. Frequent backups could be done as a substitute to continuous backup if the backups are performant.

Limitations
===========

* No support for continuous backup
* Feature is not supported on Windows operating system
* Data encryption is dependent on the disk system
* Backup and restore involves tooling which are deployment and environment specific to be developed by operators

Backup Steps
=============

``snapshot``
    This command line tool is used to create the snapshot. It takes a full path to a ``snapshot create binary`` and reports the status, optionally, can take additional arguments to be passed down to the ``snapshot create binary``. It returns a unique identifier which can be used to identify all the disk snapshots of a backup. Even in case of failures unique identifier is returned to identify and clear any partially create disk snapshots.

In response to the snapshot request from the user, FoundationDB will run a user specified ``snapshot create binary`` on all processes which has persistent data in it, binary should call filesystem/disk system specific snapshot create API and gather some additional data for the restore.

Before using the ``snapshot`` command the following setup needs to be done
     * Write a program that will snapshot the local disk store when invoked by the ``fdbserver`` with the following arguments:
           * UID - 32 byte alpha-numeric unique identifier, the same identifier will be passed to all the nodes in the cluster, can be used to identify the set of disk snapshots associated with this backup
           * Version - version string of the FoundationDB binary
           * Path - path of the FoundationDB disk store to be snapshotted
           * Role - tlog/storage/coordinator, identifies the role of the node on which the snapshot is being invoked
     * Install ``snapshot create binary`` on the FoundationDB instance in a secure path that can be invoked by the ``fdbserver``
     * Set a new config parameter ``whitelist_binpath`` for ``fdbserver`` section, whose value is the absolute ``snapshot create binary`` path. Running any ``snapshot`` command will validate that it is in the ``whitelist_binpath``. This is a security mechanism to stop running a random/unsecure command on the cluster by a client using ``snapshot`` command
     * ``snapshot create program`` should capture any additional data needed to restore the cluster, additional data could be stored as tags in cloud environments or it could be stored in an additional file/directory in the data repository and then snapshotted. The section below describes a recommended specification of the list of things that can be gathered by the binary:

``snapshot`` is a synchronous command and when it returns successfully backup is considered complete. The time it takes to finish a backup is a function of the time it takes to snapshot the disk store. For eg: if disk snapshot takes 1 second, time to finish backup should be less than < 10 seconds, this is a general guidance and in some cases it may take longer. If the command is aborted by the user then the disk snapshots should not be used for restore, because the state of backup is undefined. If the command fails or aborts, operator can issue the next backup by issuing another ``snapshot``.


Backup Specification
--------------------

================================  ========================================================   ========================================================
Field Name                        Description                                                Source of information
================================  ========================================================   ========================================================
``UID``                           unique identifier passed with all the                      ``snapshot`` CLI command output contains the UID
                                  snapshot create binary invocations associated with
                                  a backup. Disk snapshots could be tagged with this UID.
``FoundationDB Server Version``   software version of the ``fdbserver``                      command line argument to snap create binary
``CreationTime``                  current system date and time                               time obtained by calling the system time
``FoundationDB Cluster File``     cluster file which has cluster-name, magic and             read from the location of the cluster file location
                                  the list of coordinators.                                  mentioned in the command line arguments. Command
                                                                                             line arguments of ``fdbserver`` can be accessed from
                                                                                             /proc/$PPID/cmdline
``Config Knobs``                  command line arguments passed to ``fdbserver``             available from command line arguments of ``fdbserver``
                                                                                             or from foundationdb.conf
``IP Address + Port``             host address and port information of the ``fdbserver``     available from command line arguments of ``fdbserver``
                                  that is invoking the snapshot
``LocalityData``                  machine id, zone id or any other locality information      available from command line arguments of ``fdbserver``
``Name for the snapshot file``    Recommended name for the disk snapshot                     cluster-name:ip-addr:port:UID
================================  ========================================================   ========================================================

``snapshot create binary`` will not be invoked on processes which does not have any persistent data (for eg: Cluster Controller or Master or MasterProxy). Since these processes are completely stateless, there is no need for any state information from them. But, if there are specialized configuration knobs used for one of these stateless processes then they need to be backed up and restored externally.

Management of disk snapshots
----------------------------

Deleting unused disk snapshots or disk snapshots that are part of failed backups have to deleted by the operator externally.

Restore Steps
==============

Restore is the process of building up the cluster from the snapshotted disk images. There is no option to specify a restore version because there is no support for continuous backup. Here is the list of steps for the restore process:
    * Identify the snapshot disk images associated with the backup to be restored with the help of UID or creation time
    * Group disk images of a backup by IP address and/or locality information
    * Bring up a new cluster similar to the source cluster with FoundationDB services stopped and either attach the snapshot disk images or copy the snapshot disk images to the cluster in the following manner:

        * Map the old IP address to new IP address in a one to one fashion and use that mapping to guide the restoration of disk images
    * Compute the new fdb.cluster file based on where the new coordinators disk stores are placed and push it to the all the instances in the new cluster
    * Start the FoundationDB service on all the instances
    * NOTE: if one process share two roles which has persistent data then they will have a shared disk and there will be two snapshots of the disk once for each role. In that case, snapshot disk image needs to be cleaned, If a snapshot image had files that belongs to other roles than they need to be deleted.

Cluster will start and get to healthy state indicating the completion of restore. Applications can optionally do any additional validations and use the cluster.
