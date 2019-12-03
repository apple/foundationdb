
.. _disk-snapshot-backups:

#################################
Disk Snapshot Backup and Restore
#################################

This document covers disk snapshot based backup and restoration of a FoundationDB database. This tool leverages disk level snapshots and gets a point-in-time consistent copy of the database. The disk snapshot backup can be used to provide additional level of protection in case of hardware or software failures, test and development purposes or for compliance reasons.

.. _disk-snapshot-backup-introduction:

Introduction
============

FoundationDB's disk snapshot backup tool makes a consistent, point-in-time backup of FoundationDB database without downtime by taking crash consitent snapshot of all the disk stores that has persistent data.

Crash consitent snapshot feature at file-system or disk level is a pre-requsiste for using this feature.

Disk snapshot backup tool orchestrates the snapshotting of all the disk images and ensures that they are restorable in a point-in-time consistent basis. Externally, all the disk stores of a cluster could be snapshotted, but those disk snapshots will not be point-in-time consistent and hence not restorable.

Resetore is achieved by copying or attaching the disk snapshot images to FoundationDB compute instances. Restore behaves as if the cluster was powered down and restarted.

Backup vs Disk snapshot backup
==============================

Both these tools provide a point-in-time consistent backup of FoundationDB database, they operate at different levels and there are differences in terms of performance, features and external dependency.

Backup/fdbbackup operates at the key-value level, backup will involve copying of all the key-values from the source cluster and the restore will invovle applying all the key-values to the destination database. Performance will depend on the amount of data and the throughput at which the data can be read and written. This approach is agnostic to external dependency, there is no requirement for any snapshotting feature from disk system. Additionally, it has an option for continuous backup that enables a restorable point-in-time very close to now. This feature already exists in FoundationDB and is detailed here :ref:`backups`.

Disk snapshot backup and restore are generally high performant because it deals at disk level and data is not read or written through FoundationDB stack. In environments where disk snapshot and restore are highly performant this approach can be very fast. Feature is dependent on crash consistent snapshot feature from disk system. In environments where disk snapshots and restore are fast, frequent backups could be done as a substitute to the continuos backup.

Limitations
===========

 * data encryption is dependent on the disk system.
 * backup and resotre involves tooling which are deployment and environment specific to be developed by operators.

Backup Steps
=============

``snapshot``
    This command line tool is used to create the snapshot. It takes a full path to a binary and reports the status, optionally, can take additional arguments to be passed down to the binary. It returns a Unique Identifier which can be used to identify all the disk snapshots of a backup.

In response to the snapshot request from the user, FoundationDB will run a user specificed binary on all processes which has persistent data in it, binary should call environment specific snapshot create API and gather some additional data for the restore. Please note that the binary may be invoked multiple times on a single process if it plays two roles say storage and TLog.

Before using the ``snapshot`` command the following setup needs to be done
     * Develop and install a binary on the FoundationDB instances that can take snapshot of the local disk store. Binary should take the arguments mentioned below and be able to create a snapshot of the local disk store and gather any additional data that is needed for restore.
        * binary will be invoked with the following arguments:
           * UID - 32 byte alpha-numeric UID, the same id will be passed to all the nodes for this snapshot create instance, unique way to identify the set of disk snapshots associated with this backup
           * Version - version string of the FoundationDB binary
           * Path - path of the FoundationDB disk store
           * Role - tlog/storage/coordinator, identifies the role of the node on which the snapshot is being invoked
     * Set a new config parameter ``whitelist_binpath`` for fdbserver section, whose value is the full-binary path. Running any snapshot command will validate that it is in the whitelist_binpath. This is a security mechanism to stop running a random/unsecure command on the cluster by a client using snapshot command.
     * snap create binary could capture any additional data needed to restore the cluster, additional data could be stored as tags in cloud environments or it could be stored in a additional file/directory in the data repo and then snapshotted.

The section below describes a recommended specification of the list of things that needs to be gathered as part of backup to aid with restore.

Backup Specification
====================

================================  ========================================================
Field Name                        Source of information
================================  ========================================================
``UID``                           ``snapshot`` commands output contains the UID, this
                                  can be used to catalog the disk images.
``FDB Server Version``            command line argument to snap create binary
``CreationTime``                  Obtained by calling the system time.
``FDB Cluster File``              Read from the location of the cluster file location
                                  mentioned in the command line arguments. Command
                                  line arguments of fdbserver can be accessed from
                                  /proc/$PPID/cmdline
``FDB Server Config Parameters``  Available from command line arguments of fdbserver
                                  or from foundationdb.conf
``IP Address + Port``             Available from command line arguments of fdbserver
``Machine-Id``                    Available from command line arguments of fdbserver
``Name for the snapshot file``    cluster-name:ip-addr:port:snapshotUID
================================  ========================================================

Any machines that does not have any persistent data in it will not have their foundationdb.conf be available in any of the disk images, they need to be backed up externally and restored.

Restore Steps
==============
Restore is the process of building up the cluster from the snapshotted disk images. Here is list of steps for the restore process:
    * Identify the disk images associated with a particular backup
    * Group disk images of a backup by IP address or any other machine identifier
    * Bring up a new cluster similar to the source cluster with FoundationDB services stopped and either attach the snapshot disk images or copy the snapshot disk images to the cluster in the following manner:
        * Map the old IP address to new IP address in a one to one fashion and use that mapping to guide the restoration of disk images
    * compute the new fdb.cluster file based on where the new coordinators disk stores are placed and push it to the all the instances in the new cluster
    * start the FoundationDB service on all the instances
    * NOTE: if one process share two roles which has persistent data then they will have a shared disk and there will be two snapshots of the disk once for each role. In that case, snapshot disk image needs to be cleaned, If a snapshot image had files that belongs to other roles than they need to be deleted.
