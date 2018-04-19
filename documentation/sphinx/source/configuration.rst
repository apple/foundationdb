.. |multiplicative-suffixes| replace:: Sizes must be specified as a number of bytes followed by one of the multiplicative suffixes B=1, KB=10\ :sup:`3`, KiB=2\ :sup:`10`, MB=10\ :sup:`6`, MiB=2\ :sup:`20`, GB=10\ :sup:`9`, GiB=2\ :sup:`30`, TB=10\ :sup:`12`, or TiB=2\ :sup:`40`.

#############
Configuration
#############

.. include:: guide-common.rst.inc

This document contains *reference* information for configuring a new FoundationDB cluster. We recommend that you read this document before setting up a cluster for performance testing or production use. For *step-by-step instructions* to follow when setting up a cluster, see :doc:`building-cluster`.

.. note:: In FoundationDB, a "cluster" refers to one or more FoundationDB processes spread across one or more physical machines that together host a FoundationDB database.

To plan an externally accessible cluster, you need to understand some basic aspects of the system. You can start by reviewing the :ref:`system requirements <system-requirements>`, then how to :ref:`choose <configuration-choosing-coordination-servers>` and :ref:`change coordination servers <configuration-changing-coordination-servers>`. Next, you should look at the :ref:`configuration file <foundationdb-conf>`, which controls most other aspects of the system. Then, you should understand how to :ref:`choose a redundancy mode <configuration-choosing-redundancy-mode>` and :ref:`configure the storage subsystem <configuration-configuring-storage-subsystem>`. Finally, there are some guidelines for setting :ref:`process class configurations <guidelines-process-class-config>`.

.. _system-requirements:

System requirements
===================

* One of the following 64-bit operating systems:

  * A supported Linux distribution:
  
    * RHEL/CentOS 6.x and 7.x
    * Ubuntu 12.04 or later (but see :ref:`Platform Issues for Ubuntu 12.x <platform-ubuntu-12>`)

  * Or, an unsupported Linux distribution with:

    * Kernel version between 2.6.33 and 3.0.x (inclusive) or 3.7 or greater
    * Works with .deb or .rpm packages
  
  * Or, macOS 10.7 or later
  
  .. warning:: The macOS version of the FoundationDB server is intended for use on locally accessible development machines only. Other uses are not supported.
  
* 4GB **ECC** RAM (per fdbserver process)
* Storage

  * SSDs are required when storing data sets larger than memory (using the ``ssd`` storage engine).
  * HDDs are OK when storing data sets smaller than memory (using the ``memory`` storage engine).
  * For more information, see :ref:`configuration-configuring-storage-subsystem`.

For a description of issues on particular platforms that affect the operation of FoundationDB, see :doc:`platforms`.

.. _configuration-choosing-coordination-servers:

Choosing coordination servers
=============================

FoundationDB uses a set of *coordination servers* (or *coordinators* for short) to maximize the fault tolerance (and, in particular, the availability) of the cluster. The coordinators work by communicating and storing a small amount of shared state. If one or more machines are down or unable to communicate with the network, the cluster may become partitioned. In that event, FoundationDB selects the partition in which a majority of coordinators are reachable as the one that will remain available.

Any FoundationDB process can be used as a coordinator for any set of clusters. The performance impact of acting as a coordinator is negligible. The coordinators aren't involved at all in committing transactions.

Administrators should choose the number and physical location of coordinators to maximize fault tolerance. Most configurations should follow these guidelines:

* Choose an odd number of coordinators.
* Use enough coordinators to complement the :ref:`redundancy mode <configuration-choosing-redundancy-mode>` of the cluster, often 3 or 5.
* Place coordinators in different racks, circuits, or datacenters with independence of failure.
* It is OK to place coordinators in distant datacenters; in normal operation the latency to a coordinator does not affect system latency.

The set of coordinators is stored on each client and server in the :ref:`cluster file <foundationdb-cluster-file>`.

.. _configuration-changing-coordination-servers:

Changing coordination servers
=============================

It is sometimes necessary to change the set of coordinators servers. You may want to do so because of changing network conditions, machine failures, or just planning adjustments. You can change coordinators using an automated ``fdbcli`` command. FoundationDB will maintain ACID guarantees during the changes.

You can change coordinators when the following conditions are met:

* a majority of the current coordinators are available;
* all of the new coordinators are available; and
* client and server cluster files are writable.

``fdbcli`` supports a ``coordinators`` command to specify the new list of coordinators::

    user@host$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> coordinators 10.0.4.1:4500 10.0.4.2:4500 10.0.4.3:4500
    Coordinators changed

After running this command, you can check that it completed successfully by using the ``status details`` command::

    fdb> status details

    Configuration:
      Redundancy mode        - triple
      Storage engine         - ssd
      Coordinators           - 3

    Cluster:
      FoundationDB processes - 3
      Machines               - 3
      Memory availability    - 4.1 GB per process on machine with least available
      Fault Tolerance        - 0 machines
      Server time            - Thu Mar 15 14:41:34 2018

    Data:
      Replication health     - Healthy
      Moving data            - 0.000 GB
      Sum of key-value sizes - 8 MB
      Disk space used        - 103 MB

    Operating space:
      Storage server         - 1.0 GB free on most full server
      Transaction log        - 1.0 GB free on most full server

    Workload:
      Read rate              - 2 Hz
      Write rate             - 0 Hz
      Transactions started   - 2 Hz
      Transactions committed - 0 Hz
      Conflict rate          - 0 Hz

    Backup and DR:
      Running backups        - 0
      Running DRs            - 0

    Process performance details:
      10.0.4.1:4500       ( 3% cpu;  2% machine; 0.004 Gbps;  0% disk; 2.5 GB / 4.1 GB RAM  )
      10.0.4.2:4500       ( 1% cpu;  2% machine; 0.004 Gbps;  0% disk; 2.5 GB / 4.1 GB RAM  )
      10.0.4.3:4500       ( 1% cpu;  2% machine; 0.004 Gbps;  0% disk; 2.5 GB / 4.1 GB RAM  )

    Coordination servers:
      10.0.4.1:4500
      10.0.4.2:4500
      10.0.4.3:4500

    Client time: Thu Mar 15 14:41:34 2018
    
The list of coordinators verifies that the coordinator change succeeded. A few things might cause this process to not go smoothly:

* If any of the new coordination servers fail before you run the ``coordinators`` command, the change will not occur, and the database will continue to use the old coordinators.
* If a majority of the new coordination servers fail during or after the change, the database will not be available until a majority of them are available again.
* If a majority of the old coordination servers fail before the change is completed, the database will be unavailable until a majority of them are available again. Consequently, the ``coordinators`` command cannot be used to repair a database which is unavailable because its coordinators are down.

Once the change is complete, database servers and clients need to communicate with the old coordinators in order to update their cluster file to point to the new coordinators. (Each database server and client will physically re-write their cluster file to reference the new coordinators.) In most cases this process occurs automatically within a matter of seconds.

If some servers or clients are unable to write to their cluster file or are disconnected during the change of coordinators (and too many of the old coordinators become unavailable before they come back up), an administrator will need to manually copy the new cluster file to each affected machine and restart the database server or client, as if adding a new server or client to the cluster.

The ``coordinators`` command also supports a convenience option, ``coordinators auto``, that automatically selects a set of coordination servers appropriate for the redundancy mode::

    user@host1$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> coordinators auto
    Coordinators changed

``coordinators auto`` will not make any changes if the current coordinators are all available and support the current redundancy level.

.. note:: |coordinators-auto|

.. _configuration-setting-cluster-description:

Changing the cluster description
================================

A cluster is named by the ``description`` field recorded in the ``fdb.cluster`` file (see :ref:`cluster-file-format`). For convenience of reference, you may want to change the ``description`` if you operate more than one cluster on the same machines. (Each cluster must be uniquely identified by the combination of ``description`` and ``ID``.)

You can change the ``description`` using the ``coordinators`` command within ``fdbcli``::

    fdb> coordinators description=cluster_b
    Coordination state changed

You can also combine a change of ``description`` with :ref:`changing coordinators <configuration-changing-coordination-servers>`, whether by listing the coordinators or with ``coordinators auto``::

    fdb> coordinators auto description=cluster_c
    Coordination state changed

.. _foundationdb-conf:

The configuration file
======================

The ``fdbserver`` server process is run and monitored on each server by the ``fdbmonitor`` :ref:`daemon <administration_fdbmonitor>`. ``fdbmonitor`` and ``fdbserver`` itself are controlled by the ``foundationdb.conf`` file located at:

* ``/etc/foundationdb/foundationdb.conf`` on Linux
* ``/usr/local/etc/foundationdb/foundationdb.conf`` on macOS

The ``foundationdb.conf`` file contains several sections, detailed below. Note that the presence of individual ``[fdbserver.<ID>]`` sections actually cause ``fdbserver`` processes to be run.

.. note:: |conf-file-change-detection|

.. warning:: Do not attempt to stop FoundationDB services by removing the configuration file. Removing the file will not stop the services; it will merely remove your ability to control them in the manner supported by FoundationDB. During normal operation, services can be stopped by commenting out or removing the relevant sections of the configuration file. You can also disable a service at the operating system level or by removing the software.

``[fdbmonitor]`` section
------------------------

.. code-block:: ini

    ## foundationdb.conf 
    ##
    ## Configuration file for FoundationDB server processes 
    ## Full documentation is available in the FoundationDB Administration document.

    [fdbmonitor]
    restart_delay = 60
    user = foundationdb
    group = foundationdb

Contains basic configuration parameters of the ``fdbmonitor`` process. ``restart_delay`` specifies the number of seconds that ``fdbmonitor`` waits before restarting a failed process. ``user`` and ``group`` are used on Linux systems to control the privilege level of child processes.

``[general]`` section
-----------------------

.. code-block:: ini

    [general]
    cluster_file = /etc/foundationdb/fdb.cluster

Contains settings applicable to all processes (e.g. fdbserver, backup_agent). The main setting of interest is ``cluster_file``, which specifies the location of the cluster file. This file and the directory that contains it must be writable by all processes (i.e. by the user or group set in the [fdbmonitor] section).

.. _foundationdb-conf-fdbserver:

``[fdbserver]`` section
-----------------------

.. code-block:: ini
   
    ## Default parameters for individual fdbserver processes
    [fdbserver]
    command = /usr/sbin/fdbserver
    public_address = auto:$ID
    listen_address = public
    datadir = /var/lib/foundationdb/data/$ID
    logdir = /var/log/foundationdb
    # logsize = 10MiB
    # maxlogssize = 100MiB
    # class = 
    # memory = 8GiB
    # storage_memory = 1GiB
    # locality_machineid = 
    # locality_zoneid = 
    # locality_data_hall = 
    # locality_dcid = 
    # io_trust_seconds = 20

Contains default parameters for all fdbserver processes on this machine. These same options can be overridden for individual processes in their respective ``[fdbserver.<ID>]`` sections. In this section, the ID of the individual fdbserver can be substituted by using the ``$ID`` variable in the value. For example, ``public_address = auto:$ID`` makes each fdbserver listen on a port equal to its ID.

.. note:: |multiplicative-suffixes|
.. note:: In general locality id's are used to specify the location of processes which in turn is used to determine fault and replication domains.

* ``command``: The location of the ``fdbserver`` binary.
* ``public_address``: The publicly visible IP:Port of the process. If ``auto``, the address will be the one used to communicate with the coordination servers.
* ``listen_address``: The IP:Port that the server socket should bind to. If ``public``, it will be the same as the public_address.
* ``datadir``: A writable directory (by root or by the user set in the [fdbmonitor] section) where persistent data files will be stored.
* ``logdir``: A writable directory (by root or by the user set in the [fdbmonitor] section) where FoundationDB will store log files.
* ``logsize``: Roll over to a new log file after the current log file reaches the specified size. The default value is 10MiB.
* ``maxlogssize``: Delete the oldest log file when the total size of all log files exceeds the specified size. If set to 0B, old log files will not be deleted. The default value is 100MiB.
* ``class``: Process class specifying the roles that will be taken in the cluster. Recommended options are ``storage``, ``transaction``, ``stateless``. See :ref:`guidelines-process-class-config` for process class config recommendations.
* ``memory``: Maximum memory used by the process. The default value is 8GiB. When specified without a unit, MiB is assumed. This parameter does not change the memory allocation of the program. Rather, it sets a hard limit beyond which the process will kill itself and be restarted. The default value of 8GiB is double the intended memory usage in the default configuration (providing an emergency buffer to deal with memory leaks or similar problems). It is *not* recommended to decrease the value of this parameter below its default value. It may be *increased* if you wish to allocate a very large amount of storage engine memory or cache. In particular, when the ``storage_memory`` parameter is increased, the ``memory`` parameter should be increased by an equal amount.
* ``storage_memory``: Maximum memory used for data storage. This paramenter is used *only* with memory storage engine, not the ssd storage engine. The default value is 1GiB. When specified without a unit, MB is assumed. Clusters will be restricted to using this amount of memory per process for purposes of data storage. Memory overhead associated with storing the data is counted against this total. If you increase the ``storage_memory``, you should also increase the ``memory`` parameter by the same amount.
* ``locality_machineid``: Machine identifier key. All processes on a machine should share a unique id. By default, processes on a machine determine a unique id to share. This does not generally need to be set.
* ``locality_zoneid``: Zone identifier key.  Processes that share a zone id are considered non-unique for the purposes of data replication. If unset, defaults to machine id.
* ``locality_dcid``: Data center identifier key. All processes physically located in a data center should share the id. No default value. If you are depending on data center based replication this must be set on all processes.
* ``locality_data_hall``: Data hall identifier key. All processes physically located in a data hall should share the id. No default value. If you are depending on data hall based replication this must be set on all processes.
* ``io_trust_seconds``: Time in seconds that a read or write operation is allowed to take before timing out with an error. If an operation times out, all future operations on that file will fail with an error as well. Only has an effect when using AsyncFileKAIO in Linux. If unset, defaults to 0 which means timeout is disabled.
.. note:: In addition to the options above, TLS settings as described for the :ref:`TLS plugin <configuring-tls-plugin>` can be specified in the [fdbserver] section.

``[fdbserver.<ID>]`` section(s)
---------------------------------

.. code-block:: ini

    ## An individual fdbserver process with id 4500
    ## Parameters set here override defaults from the [fdbserver] section
    [fdbserver.4500]

Each section of this type represents an ``fdbserver`` process that will be run. IDs cannot be repeated. Frequently, an administrator will choose to run one ``fdbserver`` per CPU core. Parameters set in this section apply to only a single fdbserver process, and overwrite the defaults set in the ``[fdbserver]`` section. Note that by default, the ID specified in this section is also used as the network port and the data directory.

Backup agent sections
----------------------

.. code-block:: ini

    [backup_agent]
    command = /usr/lib/foundationdb/backup_agent/backup_agent
    
    [backup_agent.1]

These sections run and configure the backup agent process used for :doc:`point-in-time backups <backups>` of FoundationDB. These don't usually need to be modified. The structure and functionality is similar to the ``[fdbserver]`` and ``[fdbserver.<ID>]`` sections.


.. _configuration-choosing-redundancy-mode:

Choosing a redundancy mode
==========================

FoundationDB supports a variety of redundancy modes. These modes define storage requirements, required cluster size, and resilience to failure. To change the redundancy mode, use the ``configure`` command ``fdbcli``. For example::

    user@host$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> configure double
    Configuration changed.

The available redundancy modes are described below.

Single datacenter modes
-----------------------

+------------------------------+--+-----------------+-----------------+----------------+
|                              |  | single          | double          | triple         |
+==============================+==+=================+=================+================+
| Best for                     |  | 1-2 machines    | 3-4 machines    | 5+ machines    | 
+------------------------------+--+-----------------+-----------------+----------------+
| Replication                  |  | 1 copy          | 2 copy          | 3 copy         |
+------------------------------+--+-----------------+-----------------+----------------+
| # live machines              |  |                 |                 |                |
| to make progress             |  | 1               | 2               | 3              |
+------------------------------+--+-----------------+-----------------+----------------+
| Minimum # of machines        |  |                 |                 |                |
| for fault tolerance          |  | impossible      | 3               | 4              |
+------------------------------+--+-----------------+-----------------+----------------+
| Ideal # of                   |  |                 |                 |                |
| coordination servers         |  | 1               | 3               | 5              |
+------------------------------+--+-----------------+-----------------+----------------+
| # simultaneous failures      |  |                 |                 |                |
| after which data may be lost |  | any machine     | 2+ machines     | 3+ machines    |
+------------------------------+--+-----------------+-----------------+----------------+

In the three single datacenter redundancy modes, FoundationDB replicates data across the required number of machines in the cluster, but without aiming for datacenter redundancy. Although machines may be placed in more than one datacenter, the cluster will not be tolerant of datacenter-correlated failures. 

FoundationDB will never use processes on the same machine for the replication of any single piece of data. For this reason, references to the number of "machines" in the summary table above are the number of physical machines, not the number of ``fdbserver`` processes.

``single`` mode
    *(best for 1-2 machines)*

    FoundationDB does not replicate data and needs only one physical machine to make progress. Because data is not replicated, the database is not fault-tolerant. This mode is recommended for testing on a single development machine. (``single`` mode *will* work with clusters of two or more computers and will partition data for increased performance but the cluster will not tolerate the loss of any machines.)

``double`` mode
    *(best for 3-4 machines)*

    FoundationDB replicates data to two machines, so two or more machines are required to make progress. The loss of one machine can be survived without losing data, but if only two machines were present originally, the database will be unavailable until the second machine is restored, another machine is added, or the replication mode is changed.

    .. note:: In double redundancy mode, we recommend using three coordinators. In a two machine double redundancy cluster without a third coordinator, the coordination state has no redundancy.  Losing either machine will make the database unavailable, and recoverable only by an unsafe manual recovery of the coordination state.

.. _configuration-redundancy-mode-triple:

``triple`` mode
    *(best for 5+ machines)*

    FoundationDB replicates data to three machines, and at least three available machines are required to make progress. This is the recommended mode for a cluster of five or more machines in a single datacenter.

``three_data_hall`` mode
    FoundationDB replicates data to three machines, and at least three available machines are required to make progress. Every piece of data that has been committed to storage servers
    will be replicated onto three different data halls, and the cluster will
    remain available after losing a single data hall and one machine in another
    data hall.

Datacenter-aware mode
---------------------

In addition to the more commonly used modes listed above, this version of FoundationDB has support for redundancy across multiple datacenters. Although data will always be triple replicated in this mode, it may not be replicated across all datacenters.

    .. note:: When using the datacenter-aware mode, all ``fdbserver`` processes should be passed a valid datacenter identifier on the command line.

``three_datacenter`` mode
    *(for 5+ machines in 3 datacenters)*

    FoundationDB attempts to replicate data across two datacenters and will stay up with only two available. Data is triple replicated.  For maximum availability, you should use five coordination servers: two in two of the datacenters and one in the third datacenter.

Changing redundancy mode
------------------------

You can change the redundancy mode of a database at any time using ``fdbcli``. For example, after adding more machines to a cluster that was initially configured in ``single`` mode, you might increase the redundancy mode to ``triple``. After the change, FoundationDB will replicate your data accordingly (and remain available while it does so).

If a database is unavailable because it has too few machines to function in its current redundancy mode, you can restore it to operation by changing the redundancy mode to one with lower requirements. For example, if a database configured in ``triple`` mode in a 4 server cluster loses two servers, it will stop operating because the configured redundancy is unachievable. It will start working immediately if you configure it to ``double`` mode. Consider the consequences of reducing the redundancy level carefully before doing so. If you reduce or eliminate redundancy and there are further hardware failures, your data could be lost permanently. The best option, if available, is to add new hardware to the cluster to restore it to its minimum operating size.

Similarly, if you change the redundancy mode to a mode that cannot make progress with currently available hardware (for example, to ``triple`` when there are only two machines available), the database will immediately become unavailable. Changing the mode back or adding the necessary machines to the cluster will restore it to operation.

.. _configuration-configuring-storage-subsystem:

Configuring the storage subsystem
=================================

.. _configuration-storage-engine:

Storage engines
---------------

A storage engine is the part of the database that is responsible for storing data to disk. FoundationDB has two storage engines options, ``ssd`` and ``memory``.

For both storage engines, FoundationDB commits transactions to disk with the number of copies indicated by the redundancy mode before reporting them committed. This procedure guarantees the *durability* needed for full ACID compliance. At the point of the commit, FoundationDB may have only *logged* the transaction, deferring the work of updating the disk representation. This deferral has significant advantages for latency and burst performance. Due to this deferral, it is possible for disk space usage to continue increasing after the last commit.

To change the storage engine, use the ``configure`` command of ``fdbcli``. For example::

    user@host$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> configure ssd
    Configuration changed.

.. note:: The :ref:`storage space requirements <storage-space-requirements>` of each storage engine are discussed in the Administration Guide.

.. _configuration-storage-engine-ssd:

``ssd`` storage engine
    *(optimized for SSD storage)*

    Data is stored on disk in B-tree data structures optimized for use on :ref:`SSDs <ssd-info>`. This engine is more robust when the right disk hardware is available, as it can store large amounts of data.

    The ``ssd`` engine recovers storage space on a deferred basis after data is deleted from the database. Following a deletion, the engine slowly shuffles empty B-tree pages to the end of the database file and truncates them. This activity is given a low priority relative to normal database operations, so there may be a delay before the database reaches minimum size.

    .. note :: The ``ssd`` storage engine can be slow to return space from deleted data to the filesystem when the number of free pages is very large. This condition can arise when you have deleted most of the data in your database or greatly increased the number of processes in your cluster. In this condition, the database is still able to reuse the space, but the disk files may remain large for an extended period. 

    .. note :: Because the database is able to reuse the space, action is required only if your application requires the space for some non-database purpose. In this case, you can reclaim space by :ref:`excluding affected server processes<removing-machines-from-a-cluster>` (either singly or a few at a time) and then including them again. Excluding removes the storage server and its data files completely, and when the server is included again, it uses a fresh database file.

    Because this engine is tuned for SSDs, it may have poor performance or even availability problems when run on weaker I/O subsystems such as spinning disks or network attached storage.

.. _configuration-storage-engine-memory:

``memory`` storage engine
    *(optimized for small databases)*

    Data is stored in memory and logged to disk. In this storage engine, all data must be resident in memory at all times, and all reads are satisfied from memory. Additionally, all writes are saved to disk to ensure that data is always fully durable. This engine works well with storage subsystems, such as spinning disks, that have good sequential write performance but poor random I/O performance. 
    
    By default, each process using the memory storage engine is limited to storing 1 GB of data (including overhead). This limit can be changed using the ``storage_memory`` parameter as documented in :ref:`foundationdb.conf <foundationdb-conf>`.
	
    When using the ``memory`` engine, especially with a larger memory limit, it can take some time (seconds to minutes) for a storage machine to start up. This is because it needs to reconstruct its in-memory data structure from the logs stored on disk.

Storage locations
---------------------

Each FoundationDB process stores its state in a subdirectory of the directory supplied to it by the ``datadir`` configuration parameter in the :ref:`configuration file <foundationdb-conf>`. By default this directory is:

* ``/var/lib/foundationdb/data/<port>`` on Linux.
* ``/usr/local/foundationdb/data/<port>`` on macOS.

.. warning:: You must always use different ``datadir`` settings for different processes!

To use multiple disks for performance and capacity improvements, configure the ``datadir`` of individual ``fdbserver`` processes to point to different disks, as follows:

1. :ref:`Stop <administration-running-foundationdb>` the FoundationDB server processes on a machine.
2. Transfer the contents of the state subdirectories to new directories on the desired disks, and make sure the directories are writable by the ``fdbserver``.
3. Edit :ref:`foundationdb.conf <foundationdb-conf>` to reference the new directories. For example::

    [fdbserver.4500]
    datadir=/ssd1/foundationdb/4500

    [fdbserver.4501]
    datadir=/ssd2/foundationdb/4501

4. :ref:`Start <administration-running-foundationdb>` the FoundationDB server

.. _ssd-info:

SSD considerations
------------------

FoundationDB is designed to work with SSDs (solid state drives). SSD performance is both highly variable and hard to get good data on from public sources. Since OS configuration, controller, firmware, and driver can each have a large impact on performance (up to an order of magnitude), drives should be tested to make sure the advertised performance characteristics are being achieved. Tests should be conducted with the same load profile as FoundationDB, which typically creates a workload of mixed small reads and writes at a high queue depth.

SSDs are unlike HDDs in that they fail after a certain amount of wear. FoundationDB will use all of the SSDs in the cluster at approximately the same rate. Therefore, if a cluster is started on brand new SSDs, all of them will fail due to wear at approximately the same time. To prevent this occurrence, rotate new drives into the cluster on a continuous basis and monitor their S.M.A.R.T. attributes to check wear levels.

Filesystem
------------

FoundationDB recommends the ext4 filesystem. (However, see :ref:`Platform Issues for Ubuntu 12.x <platform-ubuntu-12>` for an issue relating to ext4 on that platform.)

.. warning::
    * FoundationDB requires filesystem support for kernel asynchronous I/O. 
    * Older filesystems such as ext3 lack important features for operating safely and efficiently with an SSD.
    * Copy-on-write type filesystems (such as Btrfs) will likely have poor performance with FoundationDB.


Ext4 filesystems should be mounted with mount options ``default,noatime,discard``.

.. note ::
    The ``noatime`` option disables updating of access times when reading files, an unneeded feature for FoundationDB that increases write activity on the disk. The discard option enables `TRIM <http://en.wikipedia.org/wiki/TRIM>`_ support, allowing the operating system to efficiently inform the SSD of erased blocks, maintaining high write speed and increasing drive lifetime.
    
Durability and caching
-------------------------

FoundationDB relies on the correct operation of ``fsync()`` to ensure the durability of transactions in the event of power failures. The combination of the file system, mount options, hard disk controller, and hard disk all need to work together properly to ensure that the operating system is correct when it reports to FoundationDB that data is safely stored.

If you are unsure about your hardware setup and need to ensure that data durability is maintained in all possible situations, we recommend that you test your hardware.

Disk partitioning
-------------------

.. note:: Modern Linux distributions already adhere to the suggestions below for newly formatted disks. If your system is recently installed this section can be skipped.

For performance reasons, it is critical that partitions on SSDs in Linux be aligned with the Erase Block Size (EBS) of the drive. The value of the EBS is vendor specific, but a value of 1024 KiB is both greater than or equal to and a multiple of any current EBS, so is safe to use with any SSD. (Defaulting to 1024 KiB is how Windows 7 and recent Linux distributions guarantee efficient SSD operation.)
To verify that the start of the partition is aligned with the EBS of the drive use the ``fdisk`` utility::

    user@host$ sudo fdisk -l /dev/sdb

    Disk /dev/sdb: 128.0 GB, 128035676160 bytes
    30 heads, 63 sectors/track, 132312 cylinders, total 250069680 sectors
    Units = sectors of 1 * 512 = 512 bytes
    Sector size (logical/physical): 512 bytes / 512 bytes
    I/O size (minimum/optimal): 512 bytes / 512 bytes
    Disk identifier: 0xe5169faa

       Device Boot     Start         End      Blocks   Id  System
         /dev/sdb1      2048   250069679   125033816   83  Linux

This output shows a properly partitioned disk in Ubuntu 12.04.

When creating a partition for use with FoundationDB using the standard Linux fdisk utility, DOS compatibility mode should be disabled (``c`` command in interactive mode) and display units should be set to sectors (``u`` command in interactive mode), rather than cylinders. Again, this is the default mode in recent Linux distributions but must be configured on older systems.

For an SSD with a single partition, the partition should typically begin at sector 2048 (512 byte sectors yields 1024 KiB alignment).

.. _guidelines-process-class-config:

Guidelines for setting process class
====================================

In a FoundationDB cluster, each of the ``fdbserver`` processes perform different tasks. Each process is recruited to do a particular task based on its process ``class``. For example, processes with ``class=storage`` are given preference to be recruited for doing storage server tasks, ``class=transaction`` are for log server processes and ``class=stateless`` are for stateless processes like proxies, resolvers, etc.,

The recommended minimum number of ``class=transaction`` (log server) processes is 8 (active) + 2 (standby) and the recommended minimum number for ``class=stateless`` processes is 4 (proxy) + 1 (resolver) + 1 (cluster controller) + 1 (master) + 2 (standby). It is better to spread the transaction and stateless processes across as many machines as possible.

``fdbcli`` is used to set the desired number of processes of a particular process type. To do so, you would issue the ``fdbcli`` commands::

  fdb> configure proxies=5
  fdb> configure logs=8

.. note:: In the present release, the default value for proxies and log servers is 3 and for resolvers is 1. You should not set the value of a process type to less than its default.

.. warning:: The conflict-resolution algorithm used by FoundationDB is conservative: it guarantees that no conflicting transactions will be committed, but it may fail to commit some transactions that theoretically could have been. The effects of this conservatism may increase as you increase the number of resolvers. It is therefore important to employ the recommended techniques for :ref:`minimizing conflicts <developer-guide-transaction-conflicts>` when increasing the number of resolvers.

You can contact us on the `community forums <https://forums.foundationdb.org>`_ if you are interested in more details or if you are benchmarking or performance-tuning on large clusters. Also see our `performance benchmarks </performance>`_ for a baseline of how a well-configured cluster should perform.
