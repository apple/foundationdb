.. _backups:

######################
Backup and Restoration
######################

.. include:: guide-common.rst.inc

This document covers backup and restoration of a FoundationDB database. While FoundationDB itself is fault tolerant, the backup tool provides an additional level of protection by supporting recovery from disasters or unintentional modification of the database.

.. _backup-introduction:

Introduction
============

FoundationDB's backup tool makes a consistent, point-in-time backup of a FoundationDB database without downtime. Like FoundationDB itself, the backup/restore software is distributed, with multiple backup agents cooperating to perform a backup or restore faster than a single machine can send or receive data and to continue the backup process seamlessly even when some backup agents fail. 

Since the FoundationDB database usually cannot maintain a consistent snapshot long enough to perform a full backup, the backup tool stores an *inconsistent* copy of the data with a log of database changes that took place during the backup process. The copy and the logs are combined at restore time to reconstruct a consistent, point-in-time snapshot of the database.

FoundationDB can write backup data to local disks, to a blob store instance, or to another FoundationDB cluster.  The location to write a backup to or restore a backup from is called a Backup URL.

There are 6 command line tools for working with backups:

``fdbbackup``
    This command line tool is used to control the backup process. It can ``start`` or ``abort`` a backup, ``discontinue`` a continuous backup, get the ``status`` of an ongoing backup, or ``wait`` for a backup to complete. It controls ``backup_agent`` processes using the database and never does any work itself.

``fdbrestore``
    This command line tool is used to control the restore process.  It can ``start`` or ``abort`` a restore, get the ``status`` of current and recent restore tasks, or ``wait`` for a restore task to complete while printing ongoing progress details.

``backup_agent``
    The backup agent does the actual work of the backup: reading data (and logs) from the database and writing it to the Backup URL.  Any number of backup agents pointed at the same database will cooperate to perform the backup. The Backup URL specified for a backup must be accessible by all ``backup_agent`` processes.

``fdbblob``
    This tool allows listing, deleting, and getting information on backups that are located in a blob store instance (which have Backup URLs starting with blobstore://).

``fdbdr``
    This command line tool is used to control the process of backing up to another FoundationDB cluster. It can ``start``, ``switch``, or ``abort`` a backup. It can also get the ``status`` of an ongoing backup. It controls ``db_agent`` processes using the database and never does any work itself.

``db_agent``
    The database backup agent does the actual work of a the backup: reading data (and logs) from the source database and writing it to the destination database.  Any number of agents pointed at the same database will cooperate to perform the backup.

By default, the FoundationDB packages are configured to start a single ``backup_agent`` process on each FoundationDB server. If you want to perform a backup to a network drive or blob store instance that is accessible to every server, you can immediately use the ``fdbbackup start`` command from any machine with access to your cluster to start the backup::

    user@host$ fdbbackup start -d <Backup_URL>

If instead you want to perform a backup to the local disk of a particular machine or machines which are not network accessible to the FoundationDB servers, then you should disable the backup agents on the FoundationDB servers. This is accomplished by commenting out all of the ``[backup_agent.<ID>]`` sections in :ref:`foundationdb.conf <foundationdb-conf>`. Do not comment out the global ``[backup_agent]`` section. Next, start backup agents on the destination machine or machines. Now, when you start a backup, you can specify the destination directory (as a Backup URL) using a local path on the destination machines. The backup agents will fetch data from the database and store it locally on the destination machines.

Backup URLs
===========

Backup and Restore locations are specified by Backup URLs.  Currently there are two valid Backup URL formats.

Note that items in angle brackets (< and >) are just placeholders and must be replaced (including the brackets) with meaningful values.

For local directories, the Backup URL format is::

    file://</absolute/path/to/base_dir>

An example would be ``file:///home/backups`` which would refer to the directory ``/home/backups``.
Note that since paths must be absolute this will result in three slashes (/) in a row in the URL.

Note that for local directory URLs the actual backup files will not be written to <base_dir> directly but rather to a uniquely timestamped subdirectory.  When starting a restore the path to the timestamped subdirectory must be specified.

For blob store backup locations, the Backup URL format is::

    blobstore://<api_key>:<secret>@<ip>:<port>/<name>[?<param>=<value>[&<param>=<value>]...]

An example blob store Backup URL would be ``blobstore://myKey:mySecret@2.2.2.2:80/dec_1_2015_0400``.

Blob store Backup URLs can have optional parameters which set various limits on interactions with the blob store.  All values must be positive decimal integers.

Under normal circumstances the default values should be succificient.  The most likely parameter that must be changed is the maximum speed at which to send data to the blob store which is called ``max_send_bytes_per_second`` or ``sbps`` for short.

Here is a complete list of valid parameters:

 *connect_tries* (or *ct*) - Number of times to try to connect for each request.

 *request_tries* (or *rt*) - Number of times to try each request until a parseable HTTP response other than 429 is received.

 *requests_per_second* (or *rps*) - Max number of requests to start per second.

 *concurrent_requests* (or *cr*) - Max number of requests in progress at once.

 *multipart_max_part_size* (or *maxps*) - Max part size for multipart uploads.

 *multipart_min_part_size* (or *minps*) - Min part size for multipart uploads.

 *concurrent_uploads* (or *cu*) - Max concurrent uploads (part or whole) that can be in progress at once.

 *concurrent_reads_per_file* (or *crps*) - Max concurrent reads in progress for any one file.

 *read_block_size* (or *rbs*) - Block size in bytes to be used for reads.

 *read_ahead_blocks* (or *rab*) - Number of blocks to read ahead of requested offset.

 *read_cache_blocks_per_file* (or *rcb*) - Size of the read cache for a file in blocks.

 *max_send_bytes_per_second* (or *sbps*) - Max send bytes per second for all requests combined.

 *max_recv_bytes_per_second* (or *rbps*) - Max receive bytes per second for all requests combined


``fdbbackup`` command line tool
===============================

.. program:: fdbbackup

The ``fdbbackup`` command line tool is used to control the backup process. ::

    user@host$ fdbbackup [-h] [-v] [-C <CLUSTER_FILE>] <SUBCOMMAND> <SUBCOMMAND_OPTIONS>

The following optional arguments must be specified *before* a subcommand:

    .. option:: -h

        Get help on the ``fdbbackup`` command.

    .. option:: -v

        Get the version of FoundationDB in use.

    .. option:: -C <CLUSTER_FILE>

        Specify the path to the ``fdb.cluster`` file that should be used to connect to the FoundationDB cluster you want to back up.

        If not specified, a :ref:`default cluster file <default-cluster-file>` will be used.

.. _backup-start:

.. program:: fdbbackup start

``start``
---------

The ``start`` subcommand is used to start a backup.  If there is already a backup in progress, the command will fail and the current backup will be unaffected.  Otherwise, a backup is started. If the wait option is used, the command will wait for the backup to complete; otherwise, it returns immediately.

    .. option:: -h

        Get help on the ``fdbbackup start`` subcommand.

    .. option:: -d <Backup_URL>

        | Specify the Backup URL for backup data to be written to.  The destination must exist and be writable by every active ``backup_agent`` process connected to the FoundationDB cluster, but need not be accessible to the ``fdbbackup`` tool itself. (By default, the ``backup_agent`` process is running as the ``foundationdb`` user and group on each FoundationDB server.)
        | 
        | When using file:// Backup URLs, note that the backup files are not written directly to specified directory but rather in a subdirectory of this directory unique for each backup. For example, if the destination URL is ``file:///shared/database-backups/``, the backup agents will create a subdirectory like ``/shared/database-backups/backup-2012-11-01-08-50-22.285173`` in which the actual backup files will be written.  Once the backup has completed successfully, all the files in the backup subdirectory together make up the backup. For restoring a backup to succeed, all of these files must be present. The last file written will be ``/shared/database-backups/backup-2012-11-01-08-50-22.285173/restorable``.

    .. option:: -w 

        Wait for the backup to complete with behavior identical to that of the :ref:`wait command <backup-wait>`.

    .. option:: -k <KEY_RANGE>+

       | Specify one or more key ranges to be used for the backup. The key ranges are specifed on a single line, separated by spaces. Each key range consists of a single quoted string of the form ``[<BEGIN_KEY> [<END_KEY>]]``. If ``<END_KEY>`` is given, the range extends from ``<BEGIN_KEY>`` (inclusive) to ``<END_KEY>`` (exclusive). If ``<END_KEY>`` is omitted, the range consists of all keys with ``<BEGIN_KEY>`` as a proper prefix.
       |
       | Each key range should be quoted in a manner appropriate for your command line environment. Here are some examples for Bash:
       | ``fdbbackup start -k 'apple bananna' -k 'mango pineapple' <Backup_URL>``
       | ``fdbbackup start -k '@pp1e b*n*nn*' -k '#an&0 p^n3app!e' <Backup_URL>``
       |
       | Here are the equivalent examples for Windows:
       | ``fdbbackup.exe start -k "apple bananna" -k "mango pineapple" <Backup_URL>``
       | ``fdbbackup.exe start -k "@pp1e b*n*nn*" -k "#an&0 p^n3app!e" <Backup_URL>``

    .. option:: -z

        Perform the backup continuously rather than terminating when a full backup is complete. All subsequent writes (in the backup's key ranges) will be included in the backup.

        .. warning:: Restoring a backup will take time proportional to the duration of the backup. Therefore, leaving a backup running with -z for an extended period is not recommended. The continuous backup feature should only be used if you periodically discontinue and restart the backup.

.. program:: fdbbackup abort

``abort``
---------

The ``abort`` subcommand is used to abort a backup that is currently in progress.  If there is no backup in progress, the command will return an error.  The destination backup is NOT deleted automatically, but it cannot be used to perform a restore.

    .. option:: -h

        Get help on the ``fdbbackup abort`` subcommand.

    .. warning:: The ``abort`` command will render any running backup unrestorable. To stop a continuous backup gracefully, use ``discontinue``.

.. program:: fdbbackup discontinue

``discontinue``
---------------

The ``discontinue`` subcommand is only available for backups that were started with the continuous (``-z``) option. Its effect is to discontinue the continous backup. Note that the subcommand does *not* abort the backup; it simply allows the backup to complete as a noncontinuous backup would.

    .. option:: -h

        Get help on the ``fdbbackup discontinue`` subcommand.

    .. option:: -w 

        Wait for the backup to complete with behavior identical to that of the :ref:`wait command <backup-wait>`.


.. _backup-wait:

.. program:: fdbbackup wait

``wait``
--------

The ``wait`` subcommand is used to wait for a backup to complete, which is useful for scripting purposes.  If there is a backup in progress, it waits for it to complete or be aborted and returns a status based on the result of the backup.  If there is no backup in progress, it returns immediately based on the result of the previous backup.  The exit code is zero (success) if the backup was completed successfully and nonzero if it was aborted.

    .. option:: -h

        Get help on the ``fdbbackup wait`` subcommand.

.. program:: fdbbackup status

``status``
----------

The ``status`` subcommand is used to get information on the current status of a backup.  It tells whether there is a backup in progress and backup agents are running. It will also report any errors that have been encountered by backup agents (e.g., errors writing to the output directory).

    ::

        user@host$ fdbbackup status -e 1
        Backup in progress to file:///share/backup_test/out/backup-2012-11-01-10-30-59.270596/.
        WARNING: Some backup agents have reported errors (printing 1):
            rebar06(17403) - Error opening /share/backup_test/out/backup-2012-11-01-10-30-59.270596/temp.aff16af7e28046698bc847dc36f3f0f4.part. (IOError: [Errno 2] No such file or directory: '/share/backup_test/out/backup-2012-11-01-10-30-59.270596/temp.aff16af7e28046698bc847dc36f3f0f4.part')

    .. option:: -h

        Get help on the ``fdbbackup status`` subcommand.

    .. option:: -e <LIMIT>

        Print the last (up to) ``<LIMIT>`` errors that were logged into the database by backup agents.  The default is 10.

``fdbrestore`` command line tool
================================

.. program:: fdbrestore

The ``fdbrestore`` command line tool is used to control restore tasks.

.. warning:: Restoring a backup will clear the contents of your database within the specified key range to restore, so use this tool with caution! 

.. warning:: It is your responsibility to ensure that no clients are accessing the database while it is being restored. During the restore process the database is in an inconsistent state, and writes that happen during the restore process might be partially or completely overwritten by restored data.

::

    user@host$ fdbrestore (start | abort | wait | status) [OPTIONS]

The following options apply to all commands:

.. option:: -h

   Get help on the ``fdbrestore`` command.

.. option:: -t <TAG>

   Specify the tag for the restore task.  Multiple restore tasks can be in progress at once so long as each task uses a different tag.  The default tag is "default".

   .. warning:: If multiple restore tasks are in progress they should be restoring to different prefixes or the result is undefined.

.. option:: -C <CLUSTER_FILE>

   Specify the path to the ``fdb.cluster`` file that should be used to connect to the FoundationDB cluster you want to use.

   If not specified, a :ref:`default cluster file <default-cluster-file>` will be used.

.. _restore-start:

``start``
---------

The ``start`` command will start a new restore on the specified (or default) tag.  The command will fail if a tag is already in use by an active restore.

    .. option:: -r <Backup_URL>

        | Specify the Backup URL for the source backup data to restore to the database.  The source data must be accessible by the ``backup_agent`` processes for the cluster.

    .. option:: -w 

        | Wait for the the restore to reach a final state (such as complete) before exiting.  Prints a progress update every few seconds.  Behavior is identical to that of the wait command.

    .. option:: -k <KEYS>

        | Specify list of key ranges from the backup to restore to the database

    .. option:: --remove_prefix <PREFIX>
        
        | remove PREFIX from the keys read from the backup

    .. option:: --add_prefix <PREFIX>

        | prefix to add to restored keys before writing them to the database

    .. option:: -n

        | Perform a trial run without actually restoring any data.

    .. option:: -v <VERSION>
 
        | Instead of the latest version the backup can be restored to, restore to VERSION.

.. program:: fdbrestore abort

``abort``
---------

The ``abort`` command will stop an active backup on the specified (or default) tag.  It will display the final state of the restore tag.

``wait``
--------

The ``wait`` command will wait for the restore on the specified (or default) tag to reach a final state (such as complete or abort) and then exit.  While waiting it will prints a progress update every few seconds.

.. program:: fdbrestore status

``status``
----------

The ``status`` command will print a detailed status report for either one tag (if a tag is specified) or for all tags.

``backup_agent`` command line tool
==================================

.. program:: backup_agent

``backup_agent`` is started automatically on each server in the default configuration of FoundationDB, so you will not normally need to invoke it at the command line.  One case in which you would need to do so would be to perform a backup to a destination which is not accessible via a shared filesystem. ::

    user@host$ backup_agent [-h] [-v] [-C <CLUSTER_FILE>]

.. option:: -h

    Get help on the ``backup_agent`` command.

.. option:: -v

    Get the version of FoundationDB in use.

.. option:: -C <CLUSTER_FILE>

    Specify the path to the ``fdb.cluster`` file that should be used to connect to the FoundationDB cluster you want to back up.

    If not specified, a :ref:`default cluster file <default-cluster-file>` will be used.

``fdbblob`` command line tool
=============================

The ``fdbblob`` command line tool is used to list, delete, and get info on backups stored in a blob store instance.

To list backups, run::

    fdbblob list <Blobstore_URL>

where <Blobstore_URL> is simply a blobstore:// Backup URL without a backup name specified.  For example, ``blobstore://mykey:mysecret@2.2.2.2:80/``.  The output will be a list of blobstore:// Backup URLs which can then be used for ``delete`` or ``info`` operations.

To delete a backup, run::

    fdbblob delete <Backup_URL>

.. warning:: If you cancel a delete operation while it is in progress then the specified backup will no longer be usable.  Repeat the delete command and allow it to complete to finish removing the backup since it is just wasting space.

To scan a backup to in order to get its size and object count, run::

    fdbblob info <Backup_URL>

.. _fdbdr-intro:

``fdbdr`` command line tool
===========================

.. program:: fdbdr

The ``fdbdr`` command line tool is used to control the process of backing up to another database. ::

    user@host$ fdbdr [-h] [-v] [-d <CLUSTER_FILE>] [-s <CLUSTER_FILE>] <SUBCOMMAND> <SUBCOMMAND_OPTION>*

The following optional arguments must be specified *before* a subcommand:

    .. option:: -h

        Get help on the ``fdbdr`` command.

    .. option:: -v

        Get the version of FoundationDB in use.

    .. option:: -d <CLUSTER_FILE>

    Specify the path to the ``fdb.cluster`` file that should be used to connect to the destination FoundationDB cluster you want to back up into.

.. option:: -s <CLUSTER_FILE>

    Specify the path to the ``fdb.cluster`` file that should be used to connect to the source FoundationDB cluster you want to back up.

.. _dr-start:

.. program:: fdbdr start

``start``
---------

The ``start`` subcommand is used to start a backup.  If there is already a backup in progress, the command will fail and the current backup will be unaffected.  Otherwise, a backup is started.

    .. option:: -h

        Get help on the ``fdbdr start`` subcommand.

    .. option:: -k <KEY_RANGE>+

       | Specify one or more key ranges to be used for the backup. The key ranges are specifed on a single line, separated by spaces. Each key range consists of a single quoted string of the form ``[<BEGIN_KEY> [<END_KEY>]]``. If ``<END_KEY>`` is given, the range extends from ``<BEGIN_KEY>`` (inclusive) to ``<END_KEY>`` (exclusive). If ``<END_KEY>`` is omitted, the range consists of all keys with ``<BEGIN_KEY>`` as a proper prefix.
       |
       | Each key range should be quoted in a manner appropriate for your command line environment. Here are some examples for Bash:
       | ``fdbdr start -k 'apple bananna' -k 'mango pineapple' <Backup_URL>``
       | ``fdbdr start -k '@pp1e b*n*nn*' -k '#an&0 p^n3app!e' <Backup_URL>``
       |
       | Here are the equivalent examples for Windows:
       | ``fdbdr.exe start -k "apple bananna" -k "mango pineapple" <Backup_URL>``
       | ``fdbdr.exe start -k "@pp1e b*n*nn*" -k "#an&0 p^n3app!e" <Backup_URL>``

.. program:: fdbdr switch

``switch``
----------

The ``switch`` subcommand is used to switch the source database and the destination database. This means the destination will be unlocked and start streaming data into the source database. This command requires both databases to be available. While the switch command is working, both databases will be locked for a few seconds.

    .. option:: -h

        Get help on the ``fdbdr switch`` subcommand.

.. program:: fdbdr abort

``abort``
---------

The ``abort`` subcommand is used to abort a backup that is currently in progress.  If there is no backup in progress, the command will return an error. The command will leave the destination database at a consistent snapshot of the source database from sometime in the past.

    .. option:: -h

        Get help on the ``fdbdr abort`` subcommand.

    .. warning:: The ``abort`` command will lose some amount of prior commits.


.. program:: fdbdr status

``status``
----------

The ``status`` subcommand is used to get information on the current status of a backup.  It tells whether there is a backup in progress and backup agents are running. It will also report any errors that have been encountered by backup agents.

    .. option:: -h

        Get help on the ``fdbdr status`` subcommand.

    .. option:: -e <LIMIT>

        Print the last (up to) ``<LIMIT>`` errors that were logged into the database by backup agents.  The default is 10.

``db-agent`` command line tool
==============================

.. program:: db-agent

unlike ``backup_agent``, ``db-agent`` is not started automatically. A ``db-agent`` needs the cluster files for both the source database and the destination database, and can only perform a backup in one direction (from source to destination). ::

    user@host$ db-agent [-h] [-v] [-d <CLUSTER_FILE>] [-s <CLUSTER_FILE>]

.. option:: -h

    Get help on the ``db-agent`` command.

.. option:: -v

    Get the version of FoundationDB in use.

.. option:: -d <CLUSTER_FILE>

    Specify the path to the ``fdb.cluster`` file that should be used to connect to the destination FoundationDB cluster you want to back up into.

.. option:: -s <CLUSTER_FILE>

    Specify the path to the ``fdb.cluster`` file that should be used to connect to the source FoundationDB cluster you want to back up.
    
