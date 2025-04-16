###################
BulkLoad User Guide
###################
.. toctree::
   :maxdepth: 1
   :hidden:
   :titlesonly:



Below we describe the :command:`BulkDump` and :command:`BulkLoad` 'fdbcli' commands and basic troubleshooting tips bulkloading
but first a quickstart on how to use this feature.

.. _quickstart:

Quickstart
=============
Below we run a simple bulkdump, a clear of the cluster, and then a bulkload to repopulate the cluster.

Start a cluster::

    <FDB_SRC_FOLDER>/tests/loopback_cluster/run_custom_cluster.sh . --storage_count 8 \
      --stateless_count 4 --stateless_taskset 0xf --logs_count 8 --logs_taskset 0xff0 --storage_taskset 0xffff000 \
      --knobs '--knob_shard_encode_location_metadata=1 --knob_desired_teams_per_server=10 --knob_enable_read_lock_on_range=1'

Start a sufficient number of SSs because too few can cause bulkload fail (In the above we started 8 SSs).

Start 'fdbcli'::

    <FDB_BUILD_FOLDER>/bin/fdbcli —cluster=<FDB_BUILD_FOLDER>/loopback-cluster/fdb.cluster --log-dir=/tmp/bulkload/ --log

Populate some data::

    fdb> writemode on
    fdb> set a b
    fdb> get a
    \`a' is \`b''
    fdb> bulkdump mode on
    fdb> bulkdump status
    fdb> bulkdump dump "" \xff /tmp/bulkload
    Received Job ID: de6b2ae7197cef28cac38d7ad7a6d3e7

    # Keep checking bulkdump status until…
    fdb> bulkdump status
    No bulk dumping job is running

Check if the bulkdump folder cited above has been created and populated. It should look something like this::

  find /tmp/bulkload/de6b2ae7197cef28cac38d7ad7a6d3e7
  .
  |____3cdf8d1534e3077f0a9d3ebd5aaa4df0
  | |____0
  | | |____133445450-manifest.txt
  | | |____133445450-data.sst
  |____job-manifest.txt


:command:`job-manifest.txt` is the metadata file for the entire bulkdump job. This file will be used by the bulkload job to check the file path to load given a range.

In each folder, there is one manifest file, at most one data file, and at most 1 byte sample file.
If the data file is missing, it means that the range is empty. If the byte sample file is missing, it means that the number of keys in the range is too small for a sample.

Now, lets clear our database and bulkload the above bulkdump::

    fdb> clearrange "" \xff
    Committed (3759447445)

    fdb> get a
    \`a\`: not found

    fdb> bulkload mode on
    fdb> bulkload status
    No bulk loading job is running

    fdb> bulkload load de6b2ae7197cef28cac38d7ad7a6d3e7 "" \xff /tmp/bulkload
    Received Job ID: de6b2ae7197cef28cac38d7ad7a6d3e7

    fdb> bulkload status
    Running bulk loading job: de6b2ae7197cef28cac38d7ad7a6d3e7
    Job information: [BulkLoadJobState]: [JobId]: de6b2ae7197cef28cac38d7ad7a6d3e7, [JobRoot]: /root/bulkload, [JobRange]: { begin=  end=\xff }, [Phase]: Submitted, [TransportMethod]: LocalFileCopy, [SubmitTime]: 1744830891.011401, [SinceSubmitMins]: 0.233608, [TaskCount]: 1
    Submitted 1 tasks
    Finished 0 tasks
    Error 0 tasks
    Total 1 tasks

    // wait until complete
    fdb> bulkload status
    No bulk loading job is running

    fdb> bulkload history
    Job de6b2ae7197cef28cac38d7ad7a6d3e7 submitted at 1744830891.011401 for range { begin=  end=\xff }. The job has 1 tasks. The job ran for 1.246195 mins and exited with status Complete.

    // Try with get
    fdb> get a
    \`a' is \`b''


We are done. You can shutdown your cluster now::

    ps auxwww|grep fdbserver |awk '{print $2}'|xargs kill -9

.. _bulkdump:

BulkDump
==========

Type :command:`help bulkdump` on the :command:`fdbcli` command-line to see the :command:`bulkdump` usage::

    fdb> help bulkdump

    bulkdump [mode|dump|status|cancel] [ARGs]

    Bulkdump commands.

    To set bulkdump mode: bulkdump mode [on|off]
    To dump a range of key/values: bulkdump dump <BEGINKEY> <ENDKEY> <DIR>
     where <BEGINKEY> to <ENDKEY> denotes the key/value range and <DIR> is
     a local directory OR blobstore url to dump SST files to.
    To get status: bulkdump status
    To cancel current bulkdump job: bulkdump cancel <JOBID>


To use the :command:`BulkDump` facility, you must first enable it as follows::

    fdb> bulkdump mode on

To dump all of the key/values in user space to a local directory::

    fdb> bulkdump dump "" \xff /tmp/region
    Received Job ID: 62d4548cf46dc0a9d06889ba3f6d1c08

 To monitor the status of your running :command:`BulkDump` job, type::

    fdb> bulkdump status
    Running bulk dumping job: b20ce68b03a28d654ee948ca4b5d859a
    Finished 1 tasks
 
 The :command:`status` command will return variants on the above until finally after the job completes it will print::

    fdb> bulkdump status
    No bulk dumping job is running
    
To cancel a running job::

    fdb> bulkdump cancel c9de5a364ecc2abb6f7a8bd890a175cf
    Job c9de5a364ecc2abb6f7a8bd890a175cf has been cancelled. No new tasks will be spawned.

Only one :command:`BulkDump` (or :command:`BulkLoad`) job can be run at a time.

.. _bulkload:

BulkLoad
==========
:command:`BulkLoad` is the inverse of :command:`BulkDump` but then adds a history dimension so you can see status or previous :command:`BulkLoad` runs.

Type :command:`help bulkload` for usage::

    fdb> help bulkload

    bulkload [mode|load|status|cancel|history] [ARGs]

    Bulkload commands.

    To set bulkload mode: bulkload mode [on|off]
    To load a range of key/values: bulkload load <JOBID> <BEGINKEY> <ENDKEY> <DIR>
     where <JOBID> is the id of the bulkdumped job to load, <BEGINKEY> to <ENDKEY>
     denotes the key/value range to load, and <DIR> is a local directory OR
     blobstore url to load SST files from.
    To get status: bulkload status
    To cancel current bulkload job: bulkload cancel <JOBID>
    To print bulkload job history: bulkload history
    To clear history: bulkload history clear [all|id]

In the below we presume an empty cluster.

As per :command:`BulkDump`, first you must enable :command:`BulkLoad` to make use of this feature::

    fdb> bulkload mode on

To load a :command:`BulkDump`, say :command:`c9de5a364ecc2abb6f7a8bd890a175cf` from the above :command:`BulkDump` section::

    fdb> bulkload load c9de5a364ecc2abb6f7a8bd890a175cf "" \xff /tmp/dump
    Received Job ID: c9de5a364ecc2abb6f7a8bd890a175cf

To monitor the state of your running job, as per :command:`BulkDump`, type status::

    fdb> bulkload status

Eventually status will return there are no jobs running.

To see recent history of :command:`BulkLoad` runs, type::

    fdb> bulkload history
    Job b20ce68b03a28d654ee948ca4b5d859a submitted at 1741926085.210577 for range { begin=  end=\xff }. The job ran for 0.162005 mins and exited with status Complete.

You can also clear 'all' history or by selectively remove jobs from the history list by 'id'.

.. _blobstore:

BulkDump/BulkLoad and S3
========================

In the above we illustrate dumping to a directory on the local filesystem.
It is also possible to :command:`BulkDump` to, and
:command:`BulkLoad` from, amazon's `S3 <https://aws.amazon.com/s3/>`_.
Rather than reference a directory when dumping or loading,
instead we make use of the fdb 'blobstore' url, or 'backup' url as it is also known, described in
`Backup, Restore, and Replication for Disaster Recovery <https://apple.github.io/foundationdb/backups.html#backup-urls>`_.
All backup configurations pertaining to 'S3' -- such as 'BLOBSTORE_CONCURRENT_UPLOADS', 'HTTP_VERBOSE_LEVEL',
and 'BLOBSTORE_ENCRYPTION_TYPE' including how we specify credentials to 'S3'-- apply when running bulkload
against 'S3' since bulkload uses the same underlying 'S3' accessing machinery.

For illustration, let the 'S3' bucket that we want to dump be called
'backup-123456789-us-west-2' and that this bucket is the 'us-west-2' amazon region.
Let the 'prefix' that we want our dump to have in 'S3' be 'bulkload/test'. Then, in
accordance with `Backup URLs <https://apple.github.io/foundationdb/backups.html#backup-urls>`_,
our resulting url will be::

    blobstore://@backup-123456789-us-west-2.s3.us-west-2.amazonaws.com/bulkload/test?bucket=backup-123456789-us-west-2&region=us-west-2
 
Presuming the fdb cluster has been setup with the appropriate `blob credentials <https://apple.github.io/foundationdb/backups.html#blob-credential-files>`_ and 'mTLS' -- a site-specific affair -- below is how we'd dump a cluster to 'S3'::

    fdb> bulkdump mode on
    fdb> bulkdump dump "" \xff blobstore://@backup-123456789-us-west-2.s3.us-west-2.amazonaws.com/bulkload/test?bucket=backup-123456789-us-west-2&region=us-west-2
    fdb> bulkdump status
    ...

Once :command:`status` reports 'No bulk dumping job is running', inspect the dumped dataset in 'S3' via your aws console or `aws s3 command-line tool <https://aws.amazon.com/cli/>`_. The job can take minutes or hours dependent on the amount of data hosted by your cluster.

To load from 'S3' (presuming an empty cluster and presuming a previous dump whose 'id' is '123456789')::

    fdb> bulkload mode on
    fdb> bulkload load 123456789 "" \xff blobstore://@backup-123456789-us-west-2.s3.us-west-2.amazonaws.com/bulkload/test?bucket=backup-123456789-us-west-2&region=us-west-2
    fdb> bulkload status
    ...

.. _troubleshooting:

Troubleshooting
===============

:command:`BulkLoad` and :command:`BulkDump` require '--knob_shard_encode_location_metadata=1'.

As for backup, enable '--knob_http_verbose_level 10' to debug connection issues: the http request/response will be dumped on STDOUT.

To watch your job in operation, search 'DDBulkLoad*', 'SSBulkLoad*', 'DDBulkDump*', 'SSBulkDump*', 'S3Client*' in trace events to see more details. 
