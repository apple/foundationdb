.. _backups:

######################################################
Backup, Restore, and Replication for Disaster Recovery
######################################################

.. include:: guide-common.rst.inc

This document covers backup and restoration of a FoundationDB database. While FoundationDB itself is fault tolerant, the backup tool provides an additional level of protection by supporting recovery from disasters or unintentional modification of the database.

.. _backup-introduction:

Introduction
============

FoundationDB's backup tool makes a consistent, point-in-time backup of a FoundationDB database without downtime. Like FoundationDB itself, the backup/restore software is distributed, with multiple backup agents cooperating to perform a backup or restore faster than a single machine can send or receive data and to continue the backup process seamlessly even when some backup agents fail. 

The FoundationDB database usually cannot maintain a consistent snapshot long enough to read the entire database, so full backup consists of an *inconsistent* copy of the data with a log of database changes that took place during the creation of that inconsistent copy.  During a restore, the inconsistent copy and the log of changes are combined to reconstruct  a consistent, point-in-time snapshot of the original database.

A FoundationDB Backup job can run continuously, pushing multiple inconsistent snapshots and logs of changes over time to maintain the backup's restorable point-in-time very close to now.

Backup vs DR
============

FoundationDB can backup a database to local disks, a blob store (such as Amazon S3), or to another FoundationDB database.  

Backing up one database to another is a special form of backup is called DR backup or just DR for short.  DR stands for Disaster Recovery, as it can be used to keep two geographically separated databases in close synchronization to recover from a catastrophic disaster.  Once a DR  operation has reached 'differential' mode, the secondary database (the destination of the DR job) will always contain a *consistent* copy of the primary database (the source of the DR job) but it will be from some past point in time.  If the primary database is lost and applications continue using the secondary database, the "ACI" in ACID is preserved but D (Durability) is lost for some amount of most recent changes.  When DR is operating normally, the secondary database will lag behind the primary database by as little as a few seconds worth of database commits.

While a cluster is being used as the destination for a DR operation it will be locked to prevent accidental use or modification.

Limitations
===========

Backup data is not encrypted at rest on disk or in a blob store account.

Tools
===========

There are 5 command line tools for working with Backup and DR operations:

``fdbbackup``
    This command line tool is used to control (but not execute) backup jobs and manage backup data.  It can ``start``, ``modify`` or ``abort`` a backup, ``discontinue`` a continuous backup, get the ``status`` of an ongoing backup, or ``wait`` for a backup to complete.  It can also ``describe``, ``delete``, ``expire`` data in a backup, or ``list`` the backups at a destination folder URL.

``fdbrestore``
    This command line tool is used to control (but not execute) restore jobs.  It can ``start`` or ``abort`` a restore, get the ``status`` of current and recent restore tasks, or ``wait`` for a restore task to complete while printing ongoing progress details.

``backup_agent``
    The backup agent is a daemon that actually executes the work of the backup and restore jobs.  Any number of backup agents pointed at the same database will cooperate to perform  backups and restores. The Backup URL specified for a backup or restore must be accessible by all ``backup_agent`` processes.

``fdbdr``
    This command line tool is used to control (but not execute) DR jobs - backups from one database to another.  It can ``start``, ``abort`` a DR job, or ``switch`` the DR direction.  It can also get the ``status`` of a running DR job.  

``dr_agent``
    The database backup agent is a daemon that actually executes the work of the DR jobs, writing snapshot and log data to the destination database.  Any number of agents pointed at the same databases will cooperate to perform the backup.

By default, the FoundationDB packages are configured to start a single ``backup_agent`` process on each FoundationDB server. If you want to perform a backup to a network drive or blob store instance that is accessible to every server, you can immediately use the ``fdbbackup start`` command from any machine with access to your cluster to start the backup

::

    user@host$ fdbbackup start -d <BACKUP_URL>

If instead you want to perform a backup to the local disk of a particular machine or machines which are not network accessible to the FoundationDB servers, then you should disable the backup agents on the FoundationDB servers. This is accomplished by commenting out all of the ``[backup_agent.<ID>]`` sections in :ref:`foundationdb.conf <foundationdb-conf>`. Do not comment out the global ``[backup_agent]`` section. Next, start backup agents on the destination machine or machines. Now, when you start a backup, you can specify the destination directory (as a Backup URL) using a local path on the destination machines. The backup agents will fetch data from the database and store it locally on the destination machines.

Backup URLs
===========

Backup and Restore locations are specified by Backup URLs.  Currently there are two valid Backup URL formats: local directories and blob store.

Note that items in angle brackets (< and >) are just placeholders and must be replaced (including the brackets) with meaningful values.  Items within square brackets ([ and ]) are optional.

For local directories, the Backup URL format is

::

    file://</absolute/path/to/base_dir>

An example would be ``file:///home/backups`` which would refer to the directory ``/home/backups``.
Note that since paths must be absolute this will result in three slashes (/) in a row in the URL.

For local directory URLs the actual backup files will not be written to <base_dir> directly but rather to a uniquely timestamped subdirectory.  When starting a restore the path to the timestamped subdirectory must be specified.

For blob store backup locations, the Backup URL format is

::

    blobstore://[<api_key>][:<secret>[:<security_token>]]@<hostname>[:<port>]/<name>?bucket=<bucket_name>[&region=<region_name>][&<param>=<value>]...]

      <api_key>         API key to use for authentication. If S3, it is AWS_ACCESS_KEY_ID. Optional.
      <secret>          API key's secret.  If S3, it is AWS_SECRET_ACCESS_KEY. Optional.
      <security_token>  Security token if temporary credentials are used. If S3, it is AWS_SESSION_TOKEN. Optional.
      <hostname>        Remote hostname or IP address to connect to
      <port>            Remote port to connect to.  Optional.  Default is 80.
      <name>            Name of the backup within the backup bucket.  It can contain '/' characters in order to organize backups into a folder-like structure.
      <bucket_name>     Name of the bucket to use for backup data.
      <region_name>     If <hostname> is not in s3 compatible form (s3.region-name.example.com) and aws v4 signature is enabled, region name is required.
      <param>=<value>   Optional URL parameters.  See below for details.

A single bucket (specified by <bucket_name>) can hold any number of backups, each with a different <name>.

If <secret> is not specified on the URL, it will be looked up in :ref:`blob credential sources<blob-credential-files>`.

An example blob store Backup URL would be ``blobstore://myKey:mySecret@something.domain.com:80/dec_1_2017_0400?bucket=backups``.
If S3 is the target blobstore, the URL would look like: ``blobstore://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}:${AWS_SESSION_TOKEN}@backup-12345-us-west-2.s3.amazonaws.com/dailies?bucket=backup-12345-us-west-2&region=us-west-2``
The <secret> and <security_token> may be ommitted from the URL and instead picked up from ref:`blob credential sources<blob-credential-files>`: ``blobstore://${AWS_ACCESS_KEY_ID}@backup-12345-us-west-2.s3.amazonaws.com/dailies?bucket=backup-12345-us-west-2&region=us-west-2``
To pickup the <api_key>, <secret>, and <security_token> from ref:`blob credential sources<blob-credential-files>`, write the blob backup URL as: ``blobstore://@backup-12345-us-west-2.s3.amazonaws.com/dailies?bucket=backup-12345-us-west-2&region=us-west-2`` (Notice the '@' in front of the hostname).

Blob store Backup URLs can have optional parameters at the end which set various limits or options used when communicating with the store.  All values must be positive decimal integers unless otherwise specified.  The speed related default values are not very restrictive. A parameter is applied individually on each ``backup_agent``, meaning that a global restriction should be calculated based on the number of agent running. The most likely parameter a user would want to change is ``max_send_bytes_per_second`` (or ``sbps`` for short) which determines the upload speed to the blob service. 

Here is a complete list of valid parameters:

 *secure_connection* (or *sc*) - Set 1 for secure connection and 0 for insecure connection. Defaults to secure connection.

 *connect_tries* (or *ct*) - Number of times to try to connect for each request.

 *connect_timeout* (or *cto*) - Number of seconds to wait for a connect request to succeed.

 *max_connection_life* (or *mcl*) - Maximum number of seconds to reuse a single TCP connection.

 *request_timeout_min* (or *rtom*) - Minimum number of seconds to wait for a request to succeed after a connection is established.

 *request_tries* (or *rt*) - Number of times to try each request until a parsable HTTP response other than 429 is received.

 *requests_per_second* (or *rps*) - Max number of requests to start per second.

 *list_requests_per_second* (or *lrps*) - Max number of list requests to start per second.

 *write_requests_per_second* (or *wrps*) - Max number of write requests to start per second.

 *read_requests_per_second* (or *rrps*) - Max number of read requests to start per second.

 *delete_requests_per_second* (or *drps*) - Max number of delete requests to start per second.

 *concurrent_requests* (or *cr*) - Max number of requests in progress at once.

 *concurrent_uploads* (or *cu*) - Max concurrent uploads (part or whole) that can be in progress at once.

 *concurrent_lists* (or *cl*) - Max concurrent list operations that can be in progress at once.

 *concurrent_reads_per_file* (or *crps*) - Max concurrent reads in progress for any one file.

 *concurrent_writes_per_file* (or *cwps*)  Max concurrent uploads in progress for any one file.

 *multipart_max_part_size* (or *maxps*) - Max part size for multipart uploads.

 *multipart_min_part_size* (or *minps*) - Min part size for multipart uploads.

 *enable_read_cache* (or *erc*) - Whether to enable read block cache.

 *read_block_size* (or *rbs*) - Block size in bytes to be used for reads.

 *read_ahead_blocks* (or *rab*) - Number of blocks to read ahead of requested offset.

 *read_cache_blocks_per_file* (or *rcb*) - Size of the read cache for a file in blocks.

 *max_send_bytes_per_second* (or *sbps*) - Max send bytes per second for all requests combined.

 *max_recv_bytes_per_second* (or *rbps*) - Max receive bytes per second for all requests combined.

 *max_delay_retryable_error (or *dre*) - Max seconds to delay before retry again when seeing an retryable error.

 *max_delay_connection_failed (or *dcf*) - Max seconds to delay before retry again when seeing an connection failure.

 *header* - Add an additional HTTP header to each blob store REST API request.  Can be specified multiple times.  Format is *header=<FieldName>:<FieldValue>* where both strings are non-empty.

 *sdk_auth* (or *sa*) - Use the AWS SDK to do credentials and authentication. This supports all aws authentication types, including credential-less iam role-based authentication in aws. Experimental, and only works if FDB was compiled with BUILD_AWS_BACKUP=ON. When this parameter is set, all other credential parts of the backup url can be ignored.
 
  **Example**: The URL parameter *header=x-amz-storage-class:REDUCED_REDUNDANCY* would send the HTTP header required to use the reduced redundancy storage option in the S3 API.

Signing Protocol
=================

AWS signature version 4 is the default signing protocol choice. This boolean knob ``--knob_http_request_aws_v4_header`` can be used to select between v4 style and v2 style signatures.
If the knob is set to ``true`` then v4 signature will be used and if set to ``false`` then v2 signature will be used.

.. _blob-credential-files:

Blob Credential Files
==============================

In order to help safeguard blob store credentials, the <secret> can optionally be omitted from ``blobstore://`` URLs on the command line.
Omitted secrets will be resolved at connect time using 1 or more Blob Credential files
(In fact, the <api_key>, <secret>, and <security_token> may all be omitted from the backup URL and instead read from the credential file;
for how to write the backup URL in this case see ref:`backup URLs<backup-urls>`).

Blob Credential files can be specified on the command line (via ``--blob-credentials <FILE>``) or via the environment variable ``FDB_BLOB_CREDENTIALS`` which can be set to a colon-separated list of files.  The command line takes priority over the environment variable however all files from both sources will be used.

At connect time, the specified files are read in order and the first matching account specification (user@host)
will be used to obtain the secret key.

The Blob Credential File format is JSON with the following schema:

::

  {
    "accounts" : {
      "user@host" :   { "secret" : "SECRETKEY" },
      "user2@host2" : { "secret" : "SECRETKEY2" }
    }
  }

If temporary credentials are being used, the following schema is also supported

::

  {
    "accounts" : {
      "@host" :     { "api_key" : user, "secret" : "SECRETKEY", "token": "TOKEN1" },
      "@host2" :    { "api_key" : user2, "secret" : "SECRETKEY2", "token": "TOKEN2" }
    }
  }

For example:

::

  {
    "accounts": {
      "@backup-12345-us-west-2.s3.amazonaws.com": {
        "api_key": "AWS_ACCESS_KEY_ID",
        "secret": "AWS_SECRET_ACCESS_KEY",
        "token": "AWS_SESSION_TOKEN"
      }
    }
  }

TLS Support
===========

In-flight traffic for blob store or disaster recovery backups can be encrypted with the following environment variables. They are also offered as command-line flags or can be specified in ``foundationdb.conf`` for backup agents.

============================ ====================================================
Environment Variable         Purpose
============================ ====================================================
``FDB_TLS_CERTIFICATE_FILE`` Path to the file from which the local certificates
                             can be loaded, used by the plugin
``FDB_TLS_KEY_FILE``         Path to the file from which to load the private
                             key, used by the plugin
``FDB_TLS_PASSWORD``         The byte-string representing the passcode for
                             unencrypting the private key
``FDB_TLS_CA_FILE``          Path to the file containing the CA certificates
                             to trust. Specify to override the default openssl
                             location.
``FDB_TLS_VERIFY_PEERS``     The byte-string for the verification of peer
                             certificates and sessions.
============================ ====================================================

Blob store backups can be configured to use HTTPS/TLS by setting the ``secure_connection`` or ``sc`` backup URL option to ``1``, which is the default. Disaster recovery backups are secured by using TLS for both the source and target clusters and setting the TLS options for the ``fdbdr`` and ``dr_agent`` commands.

``fdbbackup`` command line tool
===============================

.. program:: fdbbackup

The ``fdbbackup`` command line tool is used to control backup jobs or to manage backup data.

::

   user@host$ fdbbackup [-h] <SUBCOMMAND> <SUBCOMMAND_OPTIONS>

The following options apply to most subcommands:

``-C <CLUSTER_FILE>``
  Path to the cluster file that should be used to connect to the FoundationDB cluster you want to use.  If not specified, a :ref:`default cluster file <default-cluster-file>` will be used.

``-d <BACKUP_URL>``
  The Backup URL which the subcommand should read, write, or modify.  For ``start`` and ``modify`` operations, the Backup URL must be accessible by the ``backup_agent`` processes.

``-t <TAG>``
  A "tag" is a named slot in which a backup task executes.  Backups on different named tags make progress and are controlled independently, though their executions are handled by the same set of backup agent processes.  Any number of unique backup tags can be active at once.  It the tag is not specified, the default tag name "default" is used.

``--blob-credentials <FILE>``
  Use FILE as a :ref:`Blob Credential File<blob-credential-files>`.  Can be used multiple times.

.. _backup-start:

.. program:: fdbbackup start

``start``
---------

The ``start`` subcommand is used to start a backup.  If there is already a backup in progress, the command will fail and the current backup will be unaffected.  Otherwise, a backup is started. If the wait option is used, the command will wait for the backup to complete; otherwise, it returns immediately.

::

   user@host$ fdbbackup start [-t <TAG>] -d <BACKUP_URL> [-z] [-s <DURATION>] [--partitioned-log-experimental] [-w] [-k '<BEGIN>[ <END>]']...

``-z``
  Perform the backup continuously rather than terminating once a restorable backup is achieved.  Database mutations within the backup's target key ranges will be continuously written to the backup as well as repeated inconsistent snapshots at the configured snapshot rate.

``-s <DURATION>`` or ``--snapshot-interval <DURATION>``  
  Specifies the duration, in seconds, of the inconsistent snapshots written to the backup in continuous mode.  The default is 864000 which is 10 days.

``--initial-snapshot-interval <DURATION>``  
  Specifies the duration, in seconds, of the first inconsistent snapshot written to the backup.  The default is 0, which means as fast as possible.

``--partitioned-log-experimental``
  Specifies the backup uses the partitioned mutation logs generated by backup workers. Since FDB version 6.3, this option is experimental and requires using fast restore for restoring the database from the generated files. The default is to use non-partitioned mutation logs generated by backup agents.

``-w``
  Wait for the backup to complete with behavior identical to that of the :ref:`wait command <backup-wait>`.

``-k '<BEGIN>[ <END>]'``
  Specify a key range to be included in the backup.  Can be used multiple times to specify multiple key ranges.  The argument should be a single string containing either a BEGIN alone or both a BEGIN and END separated by a space.  If only the BEGIN is specified, the END is assumed to be BEGIN + '\xff'.  If no key ranges are different, the default is all user keys ('' to '\xff').

  Each key range should be quoted in a manner appropriate for your command line environment. Here are some examples for Bash: ::

     user@host$ fdbbackup start -k 'apple bananna' -k 'mango pineapple' -d <BACKUP_URL>
     user@host$ fdbbackup start -k '@pp1e b*n*nn*' -k '#an&0 p^n3app!e' -d <BACKUP_URL>

.. program:: fdbbackup modify

``modify``
----------

The ``modify`` subcommand is used to modify parameters of a running backup.  All specified changes are made in a single transaction.

::

   user@host$ fdbbackup modify [-t <TAG>] [-d <BACKUP_URL>] [-s <DURATION>] [--active-snapshot-interval <DURATION>] [--verify-uid <UID>]

``-d <BACKUP_URL>``
  Sets a new Backup URL for the backup to write to.  This is most likely to be used to change only URL parameters or account information.  However, it can also be used to start writing to a new destination mid-backup.  The new old location will cease gaining any additional restorability, while the new location will not be restorable until a new snapshot begins and completes.  Full restorability would be regained, however, if the contents of the two destinations were to be combined by the user.

``-s <DURATION>`` or ``--snapshot-interval <DURATION>``  
  Sets a new duration for backup snapshots, in seconds.

``--active-snapshot-interval <DURATION>``  
  Sets new duration for the backup's currently active snapshot, in seconds, relative to the start of the snapshot.

``--verify-uid <UID>``
  Specifies a UID to verify against the BackupUID of the running backup.  If provided, the UID is verified in the same transaction which sets the new backup parameters (if the UID matches).

.. program:: fdbbackup abort

``abort``
---------

The ``abort`` subcommand is used to abort a backup that is currently in progress.  If there is no backup in progress, the command will return an error.  The destination backup is NOT deleted automatically, and it may or may not be restorable depending on when the abort is done.

::

   user@host$ fdbbackup abort [-t <TAG>]

.. program:: fdbbackup discontinue

``discontinue``
---------------

The ``discontinue`` subcommand is only available for backups that were started with the continuous (``-z``) option. Its effect is to discontinue the continuous backup. Note that the subcommand does *not* abort the backup; it simply allows the backup to complete as a noncontinuous backup would.

::

   user@host$ fdbbackup discontinue [-t <TAG>] [-w]

``-w``
 Wait for the backup to complete with behavior identical to that of the :ref:`wait command <backup-wait>`.

.. _backup-wait:

.. program:: fdbbackup wait

``wait``
--------

The ``wait`` subcommand is used to wait for a backup to complete, which is useful for scripting purposes.  If there is a backup in progress, it waits for it to complete or be aborted and returns a status based on the result of the backup.  If there is no backup in progress, it returns immediately based on the result of the previous backup.  The exit code is zero (success) if the backup was completed successfully and nonzero if it was aborted.

::

   user@host$ fdbbackup wait [-t <TAG>]

.. program:: fdbbackup status

``status``
----------

The ``status`` subcommand is used to get information on the current status of backup.  It will show several backup metrics as well as recent errors which organized by whether or not they appear to be preventing backup progress.

::

   user@host$ fdbbackup status [-t <TAG>]

.. program:: fdbbackup delete

``delete``
----------

The ``delete`` subcommand will delete the specified backup.

::

   user@host$ fdbbackup delete -d <BACKUP_URL>

.. warning:: If you cancel a delete operation while it is in progress the specified backup is in an unknown state and is likely no longer usable.  Repeat the delete command to finish deleting the backup.

.. program:: fdbbackup expire

``expire``
----------

The ``expire`` subcommand will remove data from a backup prior to some point in time referred to as the 'cutoff'.

::

   user@host$ fdbbackup expire -d <BACKUP_URL> <CUTOFF> [<RESTORABILITY>] [--force]

The expiration CUTOFF must be specified by one of the two following arguments:
   
  ``--expire-before-timestamp <DATETIME>``
    Specifies the expiration cutoff to DATETIME.  Requires a cluster file and will use version/timestamp metadata in the database to convert DATETIME to a database commit version.  DATETIME must be in the form "YYYY/MM/DD.HH:MI:SS+hhmm", for example "2018/12/31.23:59:59-0800".

  ``--expire-before-version <VERSION>``
    Specifies the cutoff by a database commit version.

Optionally, the user can specify a minimum RESTORABILITY guarantee with one of the following options.

  ``--restorable-after-timestamp <DATETIME>``
    Specifies that the backup must be restorable to DATETIME and later.  Requires a cluster file and will use version/timestamp metadata in the database to convert DATETIME to a database commit version.  DATETIME must be in the form "YYYY/MM/DD.HH:MI:SS+hhmm", for example "2018/12/31.23:59:59-0800".

  ``--restorable-after-version <VERSION>``
    Specifies that the backup must be restorable as of VERSION and later.

``-f`` or ``--force``
  If the designated cutoff will result in removal of data such that the backup's restorability would be reduced to either unrestorable or less restorable than the optional restorability requirement then the --force option must be given or the result will be an error and no action will be taken.

.. program:: fdbbackup describe

``describe``
------------

The ``describe`` subcommand will analyze the given backup and print a summary of the snapshot and mutation data versions it contains as well as the version range of restorability the backup can currently provide.

::

   user@host$ fdbbackup describe -d <BACKUP_URL> [--version-timestamps] [-C <CLUSTER_FILE>] 

``--version-timestamps``
  If the originating cluster is still available and is passed on the command line, this option can be specified in order for all versions in the output to also be converted to timestamps for better human readability.


.. program:: fdbbackup list

``list``
----------

The ``list`` subcommand will list the backups at a given 'base' or shortened Backup URL.

::

   user@host$ fdbbackup list -b <BASE_URL>

``-b <BASE_URL>`` or ``--base-url <BASE_URL>``
  This a shortened Backup URL which looks just like a Backup URL but without the backup <name> so that the list command will discover and list all of the backups in the bucket.

.. program:: fdbbackup list

``tags``
----------

The ``tags`` subcommand will list the tags of all backups on a source cluster.

::

   user@host$ fdbbackup tags [-C <CLUSTER_FILE>]

.. program:: fdbbackup cleanup

``cleanup``
------------

The ``cleanup`` subcommand will list orphaned backups and DRs and optionally remove their mutations.

::

   user@host$ fdbbackup cleanup [--delete-data] [--min-cleanup-seconds] [-C <CLUSTER_FILE>] 

``--delete-data``
  This flag will cause ``cleanup`` to remove mutations for the most stale backup or DR.

``--min-cleanup-seconds``
  Specifies the amount of time a backup or DR needs to be stale before ``cleanup`` will remove mutations for it. By default this is set to one hour.


``fdbrestore`` command line tool
================================

.. program:: fdbrestore

The ``fdbrestore`` command line tool is used to control restore tasks.  Note that a restore operation will not clear the target key ranges, for safety reasons, so you must manually clear the ranges to be restored prior to starting the restore.

.. warning:: It is your responsibility to ensure that no clients are accessing the database while it is being restored. During the restore process the database is in an inconsistent state, and writes that happen during the restore process might be partially or completely overwritten by restored data.

::

    user@host$ fdbrestore (start | abort | wait | status) [OPTIONS]

The following options apply to all commands:

``-h`` 
  Get help on the ``fdbrestore`` command.

``-t <TAG>``
  Specify the tag for the restore task.  Multiple restore tasks can be in progress at once so long as each task uses a different tag.  The default tag is "default".

  .. warning:: If multiple restore tasks are in progress they should be restoring to different prefixes or the result is undefined.

``--blob-credentials <FILE>``
  Use FILE as a :ref:`Blob Credential File<blob-credential-files>`.  Can be used multiple times.

``--dest-cluster-file <CONNFILE>``
  Required.  Path to the cluster file that should be used to connect to the FoundationDB cluster you are restoring to.

.. _restore-start:

``start``
---------

The ``start`` command will start a new restore on the specified (or default) tag.  The command will fail if a tag is already in use by an active restore.

::
    user@host$ fdbrestore start -r <BACKUP_URL> [OPTIONS]

``-r <BACKUP_URL>``
  Required.  Specifies the Backup URL for the source backup data to restore to the database.  The source data must be accessible by the ``backup_agent`` processes for the cluster.

``-w``
  Wait for the restore to reach a final state (such as complete) before exiting.  Prints a progress update every few seconds.  Behavior is identical to that of the wait command.

``-k <KEYS>``
  Specify list of key ranges from the backup to restore to the database

``--remove-prefix <PREFIX>``
  Remove PREFIX from the keys read from the backup

``--add-prefix <PREFIX>``
  Add PREFIX to restored keys before writing them to the database

``-n``
  Perform a trial run without actually restoring any data.

``-v <VERSION>``
  Instead of the latest version the backup can be restored to, restore to VERSION.

``--timestamp <DATETIME>``
  Instead of the latest version the backup can be restored to, restore to a version from approximately the given timestamp.  Requires orig_cluster_file to be specified.  DATETIME must be in the form "YYYY/MM/DD.HH:MI:SS+hhmm", for example "2018/12/31.23:59:59-0800".

``--orig-cluster-file <CONNFILE>``
  The cluster file for the original database from which the backup was created.  The original database is only needed to convert a --timestamp argument to a database version.

``--inconsistent-snapshot-only``
  Ignore mutation log files during the restore to speedup the process. Because only range files are restored, this option gives an inconsistent snapshot in most cases and is not recommended to use.

``--user-data``
  Restore only the user keyspace. This option should NOT be used alongside --system-metadata (below) and CANNOT be used alongside other specified key ranges.

``--system-metadata``
  Restore only the relevant system keyspace. This option should NOT be used alongside --user-data (above) and CANNOT be used alongside other specified key ranges.

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

The ``status`` command will print a detailed status report on restore job progress.  If a tag is specified, it will only show status for that specific tag, otherwise status for all tags will be shown.

``backup_agent`` command line tool
==================================

.. program:: backup_agent

``backup_agent`` is started automatically on each server in the default configuration of FoundationDB, so you will not normally need to invoke it at the command line.  One case in which you would need to do so would be to perform a backup to a destination which is not accessible via a shared filesystem. ::

    user@host$ backup_agent [-h] [-v] [-C <CLUSTER_FILE>]

``-h``
  Get help on the ``backup_agent`` command.

``-v``
  Get the version of FoundationDB in use.

``-C <CLUSTER_FILE>``
  Specify the path to the ``fdb.cluster`` file that should be used to connect to the FoundationDB cluster you want to back up.
 
  If not specified, a :ref:`default cluster file <default-cluster-file>` will be used.

``--blob-credentials <FILE>``
  Use FILE as a :ref:`Blob Credential File<blob-credential-files>`.  Can be used multiple times.

.. _fdbdr-intro:

``fdbdr`` command line tool
===========================

.. program:: fdbdr

The ``fdbdr`` command line tool is used to manage DR tasks.

::

    user@host$ fdbdr [-h] <SUBCOMMAND> [<SUBCOMMAND_OPTIONS>] -d <CLUSTER_FILE> -s <CLUSTER_FILE> [-v]

The following arguments are used by multiple subcommands:

``-h``
  Get help on the ``fdbdr`` command.

``-v``
  Get the version of FoundationDB in use.

``-d <CLUSTER_FILE>``
  Specify the path to the ``fdb.cluster`` file for the destination cluster of the DR operation.

``-s <CLUSTER_FILE>``
  Specify the path to the ``fdb.cluster`` file for the source cluster of the DR operation.

.. _dr-start:

.. program:: fdbdr start

``start``
---------

The ``start`` subcommand is used to start a DR backup.  If there is already a DR backup in progress, the command will fail and the current DR backup will be unaffected.

``-k '<BEGIN>[ <END>]'``
  Specify a key range to be included in the DR.  Can be used multiple times to specify multiple key ranges.  The argument should be a single string containing either a BEGIN alone or both a BEGIN and END separated by a space.  If only the BEGIN is specified, the END is assumed to be BEGIN + '\xff'.  If no key ranges are different, the default is all user keys ('' to '\xff').

.. program:: fdbdr switch

``switch``
----------

The ``switch`` subcommand is used to swap the source and destination database clusters of an active DR in differential mode.  This means the destination will be unlocked and start streaming data into the source database, which will subsequently be locked.

This command requires both databases to be available.  On the destination cluster, a ``dr_agent`` that points to the source cluster must be running.  While the switch command is working, both databases will be locked for a few seconds.

.. program:: fdbdr abort

``abort``
---------

The ``abort`` subcommand is used to abort a DR that is currently in progress.  If there is no backup in progress, the command will return an error.  If the DR had already reached differential status, the abort command will leave the destination database at consistent snapshot of the source database from sometime in the past.

  .. warning:: The ``abort`` command will lose some amount of prior commits.

.. program:: fdbdr status

``status``
----------

The ``status`` subcommand is used to get information on the current status of DR backup.  It tells whether or not there is a DR in progress and whether or not there are active DR agents.  It will also report any errors that have been encountered by the DR agents.

``-e <LIMIT>``
  Print the last (up to) ``<LIMIT>`` errors that were logged into the database by backup agents.  The default is 10.

``dr_agent`` command line tool
==============================

.. program:: dr_agent

Unlike ``backup_agent``, ``dr_agent`` is not started automatically in a default FoundationDB installation.  A ``dr_agent`` needs the cluster files for both the source database and the destination database, and can only perform a backup in one direction (from source to destination) at a time.

::

    user@host$ dr_agent [-h] [-v] -d <CLUSTER_FILE> -s <CLUSTER_FILE>

``-h``
  Get help on the ``fdbdr`` command.

``-v``
  Get the version of FoundationDB in use.

``-d <CLUSTER_FILE>``
  Specify the path to the ``fdb.cluster`` file for the destination cluster of the DR operation.

``-s <CLUSTER_FILE>``
  Specify the path to the ``fdb.cluster`` file for the source cluster of the DR operation.
