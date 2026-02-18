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
    * Preferably .deb or .rpm package support
  
  * Or, macOS 10.7 or later
  
  .. warning:: The macOS and Windows versions of the FoundationDB server are intended for use on locally accessible development machines only. Other uses are not supported.
  
* 4GB **ECC** RAM (per fdbserver process)
* Storage

  * SSDs are required when storing data sets larger than memory (using the ``ssd`` or ``ssd-redwood-v1`` or ``ssd-rocksdb-v1`` storage engine).
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
* client and server cluster files and their parent directories are writable.

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

    [fdbmonitor]
    user = foundationdb
    group = foundationdb

Contains basic configuration parameters of the ``fdbmonitor`` process. ``user`` and ``group`` are used on Linux systems to control the privilege level of child processes.

``[general]`` section
-----------------------

.. code-block:: ini

    [general]
    cluster-file = /etc/foundationdb/fdb.cluster
    restart-delay = 60
    ## restart-backoff and restart-delay-reset-interval default to the value that is used for restart-delay
    # initial-restart-delay = 0
    # restart-backoff = 60.0
    # restart-delay-reset-interval = 60
    # delete-envvars =
    # kill-on-configuration-change = true
    # disable-lifecycle-logging = false

Contains settings applicable to all processes (e.g. fdbserver, backup_agent).

* ``cluster-file``: Specifies the location of the cluster file. This file and the directory that contains it must be writable by all processes (i.e. by the user or group set in the ``[fdbmonitor]`` section).
* ``delete-envvars``: A space separated list of environment variables to remove from the environments of child processes. This can be used if the ``fdbmonitor`` process needs to be run with environment variables that are undesired in its children.
* ``kill-on-configuration-change``: If ``true``, affected processes will be restarted whenever the configuration file changes. Defaults to ``true``.
* ``disable-lifecycle-logging``: If ``true``, ``fdbmonitor`` will not write log events when processes start or terminate. Defaults to ``false``.

.. _configuration-restarting:

The ``[general]`` section also contains some parameters to control how processes are restarted when they die. ``fdbmonitor`` uses backoff logic to prevent a process that dies repeatedly from cycling too quickly, and it also introduces up to +/-10% random jitter into the delay to avoid multiple processes all restarting simultaneously. ``fdbmonitor`` tracks separate backoff state for each process, so the restarting of one process will have no effect on the backoff behavior of another.

* ``restart-delay``: The maximum number of seconds (subject to jitter) that fdbmonitor will delay before restarting a failed process.
* ``initial-restart-delay``: The number of seconds ``fdbmonitor`` waits to restart a process the first time it dies. Defaults to 0 (i.e. the process gets restarted immediately). 
* ``restart-backoff``: Controls how quickly ``fdbmonitor`` backs off when a process dies repeatedly. The previous delay (or 1, if the previous delay is 0) is multiplied by ``restart-backoff`` to get the next delay, maxing out at the value of ``restart-delay``. Defaults to the value of ``restart-delay``, meaning that the second and subsequent failures will all delay ``restart-delay`` between restarts.
* ``restart-delay-reset-interval``: The number of seconds a process must be running before resetting the backoff back to the value of ``initial-restart-delay``. Defaults to the value of ``restart-delay``.

 These ``restart-`` parameters are not applicable to the ``fdbmonitor`` process itself. See :ref:`Configuring autorestart of fdbmonitor <configuration-restart-fdbmonitor>` for details.

As an example, let's say the following parameters have been set:

.. code-block:: ini

    restart-delay = 60
    initial-restart-delay = 0
    restart-backoff = 2.0
    restart-delay-reset-interval = 180

The progression of delays for a process that fails repeatedly would be ``0, 2, 4, 8, 16, 32, 60, 60, ...``, each subject to a 10% random jitter. After the process stays alive for 180 seconds, the backoff would reset and the next failure would restart the process immediately.

Using the default parameters, a process will restart immediately if it fails and then delay ``restart-delay`` seconds if it fails again within ``restart-delay`` seconds. 

.. _foundationdb-conf-fdbserver:

``[fdbserver]`` section
-----------------------

.. code-block:: ini
   
    ## Default parameters for individual fdbserver processes
    [fdbserver]
    command = /usr/sbin/fdbserver
    public-address = auto:$ID
    listen-address = public
    datadir = /var/lib/foundationdb/data/$ID
    logdir = /var/log/foundationdb
    # logsize = 10MiB
    # maxlogssize = 100MiB
    # class = 
    # memory = 8GiB
    # memory-vsize =
    # storage-memory = 1GiB
    # cache-memory = 2GiB
    # locality-machineid =
    # locality-zoneid =
    # locality-data-hall =
    # locality-dcid =
    # io-trust-seconds = 20

Contains default parameters for all fdbserver processes on this machine. These same options can be overridden for individual processes in their respective ``[fdbserver.<ID>]`` sections. In this section, the ID of the individual fdbserver can be substituted by using the ``$ID`` variable in the value. For example, ``public-address = auto:$ID`` makes each fdbserver listen on a port equal to its ID.

.. note:: |multiplicative-suffixes|
.. note:: In general locality id's are used to specify the location of processes which in turn is used to determine fault and replication domains.

* ``command``: The location of the ``fdbserver`` binary.
* ``public-address``: The publicly visible IP:Port of the process. If ``auto``, the address will be the one used to communicate with the coordination servers.
* ``listen-address``: The IP:Port that the server socket should bind to. If ``public``, it will be the same as the public-address.
* ``datadir``: A writable directory (by root or by the user set in the [fdbmonitor] section) where persistent data files will be stored.
* ``logdir``: A writable directory (by root or by the user set in the [fdbmonitor] section) where FoundationDB will store log files.
* ``logsize``: Roll over to a new log file after the current log file reaches the specified size. The default value is 10MiB.
* ``maxlogssize``: Delete the oldest log file when the total size of all log files exceeds the specified size. If set to 0B, old log files will not be deleted. The default value is 100MiB.
* ``class``: Process class specifying the roles that will be taken in the cluster. Recommended options are ``storage``, ``transaction``, ``stateless``. See :ref:`guidelines-process-class-config` for process class config recommendations.
* ``memory``: Maximum resident memory used by the process. The default value is 8GiB. When specified without a unit, MiB is assumed. Setting to 0 means unlimited. This parameter does not change the memory allocation of the program. Rather, it sets a hard limit beyond which the process will kill itself and be restarted. The default value of 8GiB is double the intended memory usage in the default configuration (providing an emergency buffer to deal with memory leaks or similar problems). It is *not* recommended to decrease the value of this parameter below its default value. It may be *increased* if you wish to allocate a very large amount of storage engine memory or cache. In particular, when the ``storage-memory``  or ``cache-memory`` parameters are increased, the ``memory`` parameter should be increased by an equal amount.
* ``memory-vsize``: Maximum virtual memory used by the process. The default value is 0, which means unlimited. When specified without a unit, MiB is assumed. Same as ``memory``, this parameter does not change the memory allocation of the program. Rather, it sets a hard limit beyond which the process will kill itself and be restarted.
* ``storage-memory``: Maximum memory used for data storage. This parameter is used *only* with memory storage engine, not with the other storage engines. The default value is 1GiB. When specified without a unit, MB is assumed. Clusters will be restricted to using this amount of memory per process for purposes of data storage. Memory overhead associated with storing the data is counted against this total. If you increase the ``storage-memory`` parameter, you should also increase the ``memory`` parameter by the same amount.
* ``cache-memory``: Maximum memory used for caching pages from disk. The default value is 2GiB. When specified without a unit, MiB is assumed. If you increase the ``cache-memory`` parameter, you should also increase the ``memory`` parameter by the same amount.
* ``locality-machineid``: Machine identifier key. All processes on a machine should share a unique id. By default, processes on a machine determine a unique id to share. This does not generally need to be set.
* ``locality-zoneid``: Zone identifier key.  Processes that share a zone id are considered non-unique for the purposes of data replication. If unset, defaults to machine id.
* ``locality-dcid``: Datacenter identifier key. All processes physically located in a datacenter should share the id. No default value. If you are depending on datacenter based replication this must be set on all processes.
* ``locality-data-hall``: Data hall identifier key. All processes physically located in a data hall should share the id. No default value. If you are depending on data hall based replication this must be set on all processes.
* ``io-trust-seconds``: Time in seconds that a read or write operation is allowed to take before timing out with an error. If an operation times out, all future operations on that file will fail with an error as well. Only has an effect when using AsyncFileKAIO in Linux. If unset, defaults to 0 which means timeout is disabled.
* ``parentpid``: Die if the process ID of its parent differs from the one given.  The argument should always be ``$PID``, which will be substituted with the process ID of fdbmonitor.  Using this parameter will cause all fdbserver processes started by fdbmonitor to die if fdbmonitor is killed.

.. note:: In addition to the options above, TLS settings as described for the :ref:`TLS plugin <configuring-tls>` can be specified in the [fdbserver] section.

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

.. _configuration-restart-fdbmonitor:

Configuring autorestart of fdbmonitor
=====================================

Configuring the restart parameters for ``fdbmonitor`` is operating system-specific.

Linux (RHEL/CentOS)
-------------------

 ``systemd`` controls the ``foundationdb`` service. When ``fdbmonitor`` is killed unexpectedly, by default, systemd restarts it in 60 seconds. To adjust this value you have to create a file ``/etc/systemd/system/foundationdb.service.d/override.conf`` with the overriding values. For example:

.. code-block:: ini

    [Service]
    RestartSec=20s

To disable auto-restart of ``fdbmonitor``, put ``Restart=no`` in the same section.

.. _configuration-choosing-redundancy-mode:

Choosing a redundancy mode
==========================

FoundationDB supports a variety of redundancy modes. These modes define storage requirements, required cluster size, and resilience to failure. To change the redundancy mode, use the ``configure`` command of ``fdbcli``. For example::

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
| Total Replicas               |  | 1 copy          | 2 copies        | 3 copies       |
+------------------------------+--+-----------------+-----------------+----------------+
| Live machines required       |  |                 |                 |                |
| to make progress             |  | 1               | 2               | 3              |
+------------------------------+--+-----------------+-----------------+----------------+
| Required machines            |  |                 |                 |                |
| for fault tolerance          |  | impossible      | 3               | 4              |
+------------------------------+--+-----------------+-----------------+----------------+
| Ideal number of              |  |                 |                 |                |
| coordination servers         |  | 1               | 3               | 5              |
+------------------------------+--+-----------------+-----------------+----------------+
| Simultaneous failures        |  |                 |                 |                |
| after which data may be lost |  | any process     | 2+ machines     | 3+ machines    |
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

    .. note:: When running in cloud environments with managed disks that are already replicated and persistent, ``double`` replication may still be considered for 5+ machine clusters.  This will result in lower availability fault tolerance for planned or unplanned failures and lower total read throughput, but offers a reasonable tradeoff for cost.

``three_data_hall`` mode
    FoundationDB stores data in triplicate, with one copy on a storage server in each of three data halls. The transaction logs are replicated four times, with two data halls containing two replicas apiece. Four available machines (two in each of two data halls) are therefore required to make progress. This configuration enables the cluster to remain available after losing a single data hall and one machine in another data hall.

``three_data_hall_fallback`` mode
    FoundationDB stores data in duplicate, with one copy each on a storage server in two of three data halls. The transaction logs are replicated four times, with two data halls containing two replicas apiece. Four available machines (two in each of two data halls) are therefore required to make progress. This configuration is similar to ``three_data_hall``, differing only in that data is stored on two instead of three replicas. This configuration is useful to unblock data distribution when a data hall becomes temporarily unavailable. Because ``three_data_hall_fallback`` reduces the redundancy level to two, it should only be used as a temporary measure to restore cluster health during a datacenter outage.

Datacenter-aware mode
---------------------

In addition to the more commonly used modes listed above, this version of FoundationDB has support for redundancy across multiple datacenters.

.. note:: When using the datacenter-aware mode, all ``fdbserver`` processes should be passed a valid datacenter identifier on the command line.

``three_datacenter`` mode
    *(for 5+ machines in 3 datacenters)*

    FoundationDB attempts to replicate data across three datacenters and will stay up with only two available. Data is replicated 6 times. Transaction logs are stored in the same configuration as the ``three_data_hall`` mode, so commit latencies are tied to the latency between datacenters. For maximum availability, you should use five coordination servers: two in two of the datacenters and one in the third datacenter.

.. warning:: ``three_datacenter`` mode is not compatible with region configuration.

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

A storage engine is the part of the database that is responsible for storing data to disk. FoundationDB has four storage engines options, ``ssd``, ``ssd-redwood-v1``, ``ssd-rocksdb-v1`` and ``memory``.

For all storage engines, FoundationDB commits transactions to disk with the number of copies indicated by the redundancy mode before reporting them committed. This procedure guarantees the *durability* needed for full ACID compliance. At the point of the commit, FoundationDB may have only *logged* the transaction, deferring the work of updating the disk representation. This deferral has significant advantages for latency and burst performance. Due to this deferral, it is possible for disk space usage to continue increasing after the last commit.

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

.. _configuration-storage-engine-redwood:

``ssd-redwood-v1`` storage engine
    *(optimized for SSD storage)*

    Data is stored on disk in B-tree data structures optimized for use on :ref:`SSDs <ssd-info>`. This is an improved version of the SSD storage engine SQLite, expected to offer higher throughput and lower write amplification depending on the workload.

.. _configuration-storage-engine-rocksdb:

``ssd-rocksdb-v1`` storage engine
    *(optimized for SSD storage)*

    Data is stored on disk using LSM-tree (Log-Structured Merge Tree) data structures optimized for use on :ref:SSDs <ssd-info>. Data can be compressed and undergoes regular compaction, optimizing storage efficiency and reducing read amplification. RocksDB’s extensive tuning options allow the storage engine to be customized for both workload characteristics and hardware configurations.

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


Ext4 filesystems should be mounted with mount options ``defaults,noatime,discard``.

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

.. _configuration-configuring-regions:

Configuring regions
===================

.. note:: In the following text, the term ``datacenter`` is used to denote unique locations that are failure independent from one another. Cloud providers generally expose this property of failure independence with Availability Zones.

Regions configuration enables automatic failover between two datacenters, without adding a WAN latency for commits, while still maintaining all the consistency properties FoundationDB provides.

This is made possible by combining two features. The first is asynchronous replication between two regions. By not waiting for the commits to become durable in the remote region before reporting a commit as successful, it means the remote region will slightly lag behind the primary.  This is similar to ``fdbdr``, except that the asynchronous replication is done within a single cluster instead of between different FoundationDB clusters.

The second feature is the ability to add one or more synchronous replicas of the mutation log in a different datacenter. Because this datacenter is only holding a transient copy of the mutations being committed to the database, only a few FoundationDB processes are required to fulfill this role.  If the primary datacenter fails, the external mutation log replicas will still allow access to the most recent commits. This allows the lagging remote replica to catch up to the primary. Once the remote replica has applied all the mutations, it can start accepting new commits without suffering any data loss.

An example configuration would be four total datacenters, two on the east coast, two on the west coast, with a preference for fast write latencies from the west coast. One datacenter on each coast would be sized to store a full copy of the data. The second datacenter on each coast would only have a few FoundationDB processes.

While everything is healthy, writes need to be made durable in both west coast datacenters before a commit can succeed. The geographic proximity of the two datacenters minimizes the additional commit latency. Reads can be served from either region, and clients can get data from whichever region is closer. Getting a read version from east coast region will still require communicating with a west coast datacenter. Clients can cache read versions if they can tolerate reading stale data to avoid waiting on read versions.

If either west coast datacenter fails, the last few mutations will be propagated from the remaining west coast datacenter to the east coast. At this point, FoundationDB will start accepting commits on the east coast. Once the west coast comes back online, the system will automatically start copying all the data that was committed to the east coast back to the west coast replica. Once the west coast has caught up, the system will automatically switch back to accepting writes from the west coast again.

The west coast mutation logs will maintain their copies of all committed mutations until they have been applied by the east coast datacenter.  In the event that the east coast has failed for long enough that the west coast mutation logs no longer have enough disk space to continue storing the mutations, FoundationDB can be requested to drop the east coast replica completely. This decision is not automatic, and requires a manual change to the configuration. The west coast database will then act as a single datacenter database until the east coast comes back online. Because the east coast datacenter was completely dropped from the configuration, FoundationDB will have to copy all the data between the regions in order to bring it back online.

If a region failover occurs, clients will generally only see a latency spike of a few seconds.

Specifying datacenters
----------------------

To use region configurations all processes in the cluster need to specify in which datacenter they are located. This can be done on the command line with either ``--locality-dcid`` or ``--datacenter-id``. This datacenter identifier is case sensitive.

Clients should also specify their datacenter with the database option ``datacenter-id``. If a client does not specify their datacenter, they will use latency estimates to balance traffic between the two regions. This will result in about 5% of requests being served by the remote regions, so reads will suffer from high tail latencies.

Changing the region configuration
---------------------------------

To change the region configuration, use the ``fileconfigure`` command ``fdbcli``. For example::

    user@host$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> fileconfigure regions.json
    Configuration changed.


Regions are configured in FoundationDB as a json document. For example::

    "regions":[{
        "datacenters":[{
            "id":"WC1",
            "priority":1,
            "satellite":1,
            "satellite_logs":2
        }],
        "satellite_redundancy_mode":"one_satellite_double",
        "satellite_logs":2
    }]

The ``regions`` object in the json document should be an array. Each element of the array describes the configuration of an individual region.

Each region is described using an object that contains an array of ``datacenters``. Each region may also optionally provide a ``satellite_redundancy_mode`` and ``satellite_logs``.

Each datacenter is described with an object that contains the ``id`` and ``priority`` of that datacenter. An ``id`` may be any unique alphanumeric string. Datacenters which hold a full replica of the data are referred to as primary datacenters. Datacenters that only store transaction logs are referred to as satellite datacenters. To specify a datacenter is a satellite, it needs to include ``"satellite" : 1``. The priorities of satellite datacenters are only compared to other satellites datacenters in the same region. The priorities of primary datacenters are only compared to other primary datacenters.

.. warning:: In release 6.0, FoundationDB supports at most two regions.

Each region can only have one primary datacenter. A negative priority for a datacenter denotes that the system should not recover the transaction subsystem in that datacenter. The region with the transaction subsystem is referred to as the active region.

One primary datacenter must have a priority >= 0. The cluster will make the region with the highest priority the active region. If two datacenters have equal priority the cluster will make one of them the active region arbitrarily.

The ``satellite_redundancy_mode`` is configured per region, and specifies how many copies of each mutation should be replicated to the satellite datacenters.

``one_satellite_single`` mode

Keep one copy of the mutation log in the satellite datacenter with the highest priority. If the highest priority satellite is unavailable it will put the transaction log in the satellite datacenter with the next highest priority.

``one_satellite_double`` mode

Keep two copies of the mutation log in the satellite datacenter with the highest priority.

``one_satellite_triple`` mode

Keep three copies of the mutation log in the satellite datacenter with the highest priority.

``two_satellite_safe`` mode

Keep two copies of the mutation log in each of the two satellite datacenters with the highest priorities, for a total of four copies of each mutation. This mode will protect against the simultaneous loss of both the primary and one of the satellite datacenters. If only one satellite is available, it will fall back to only storing two copies of the mutation log in the remaining datacenter.

``two_satellite_fast`` mode

Keep two copies of the mutation log in each of the two satellite datacenters with the highest priorities, for a total of four copies of each mutation. FoundationDB will only synchronously wait for one of the two satellite datacenters to make the mutations durable before considering a commit successful. This will reduce tail latencies caused by network issues between datacenters. If only one satellite is available, it will fall back to only storing two copies of the mutation log in the remaining datacenter.

.. warning:: In release 6.0 this is implemented by waiting for all but 2 of the transaction logs. If ``satellite_logs`` is set to more than 4, FoundationDB will still need to wait for replies from both datacenters.

The number of ``satellite_logs`` is also configured per region. It represents the desired number of transaction logs that should be recruited in the satellite datacenters. The satellite transaction logs do slightly less work than the primary datacenter transaction logs. So while the ratio of logs to replicas should be kept roughly equal in the primary datacenter and the satellites, a slightly fewer number of satellite transaction logs may be the optimal balance for performance.

The number of replicas in each region is controlled by redundancy level. For example ``double`` mode will put 2 replicas in each region, for a total of 4 replicas.

Asymmetric configurations
-------------------------

The fact that satellite policies are configured per region allows for asymmetric configurations. For example, FoundationDB can have a three datacenter setup where there are two datacenters on the west coast (WC1, WC2) and one datacenter on the east coast (EC1). The west coast region can be set as the preferred active region by setting the priority of its primary datacenter higher than the east coast datacenter. The west coast region should have a satellite policy configured, so that when it is active, FoundationDB is making mutations durable in both west coast datacenters. In the rare event that one of the west coast datacenters has failed, FoundationDB will fail over to the east coast datacenter. Because this region does not have a satellite datacenter, the mutations will only be made durable in one datacenter while the transaction subsystem is located here. However, this is justifiable because the region will only be active if a datacenter has already been lost.

This is the region configuration that implements the example::

    "regions":[{
        "datacenters":[{
            "id":"WC1",
            "priority":1,
        },{
            "id":"WC2",
            "priority":0,
            "satellite":1,
            "satellite_logs":2
        }],
        "satellite_redundancy_mode":"one_satellite_double"
    },{
        "datacenters":[{
            "id":"EC1",
            "priority":0,
        }]
    }]

Changing the usable_regions configuration
-----------------------------------------

The ``usable_regions`` configuration option determines the number of regions which have a replica of the database.

.. warning:: In release 6.0, ``usable_regions`` can only be configured to the values of ``1`` or ``2``, and a maximum of 2 regions can be defined in the ``regions`` json object.

Increasing the ``usable_regions`` will start copying data from the active region to the remote region. Reducing the ``usable_regions`` will immediately drop the replicas in the remote region. During these changes, only one primary datacenter can have priority >= 0. This enforces exactly which region will lose its replica.

Changing the log routers configuration
--------------------------------------

FoundationDB is architected to copy every mutation between regions exactly once. This copying is done by a new role called the log router. When a mutation is committed, it will be randomly assigned to one log router, which will be responsible for copying it across the WAN.

This log router will pull the mutation from exactly one of the transaction logs. This means a single socket will be used to copy mutations across the WAN per log router. Because of this, if the latency between regions is large the bandwidth-delay product means that the number of log routers could limit the throughput at which mutations can be copied across the WAN. This can be mitigated by either configuring more log routers, or increasing the TCP window scale option. 

To keep the work evenly distributed on the transaction logs, the number of log routers should be a multiple of the number of transaction logs.

The ``log_routers`` configuration option determines the number of log routers recruited in the remote region.

Migrating a database to use a region configuration
--------------------------------------------------

To configure an existing database to regions, do the following steps:

1. Ensure all processes have their dcid locality set on the command line. All processes should exist in the same datacenter. If converting from a ``three_datacenter`` configuration, first configure down to using a single datacenter by changing the replication mode. Then exclude the machines in all datacenters but the one that will become the initial active region.

2. Configure the region configuration. The datacenter with all the existing processes should have a non-negative priority. The region which will eventually store the remote replica should be added with a negative priority.

3. Add processes to the cluster in the remote region. These processes will not take data yet, but need to be added to the cluster. If they are added before the region configuration is set they will be assigned data like any other FoundationDB process, which will lead to high latencies.

4. Configure ``usable_regions=2``. This will cause the cluster to start copying data between the regions.

5. Watch ``status`` and wait until data movement is complete. This will signal that the remote datacenter has a full replica of all of the data in the database.

6. Change the region configuration to have a non-negative priority for the primary datacenters in both regions. This will enable automatic failover between regions.

Handling datacenter failures
----------------------------

When a primary datacenter fails, the cluster will go into a degraded state. It will recover to the other region and continue accepting commits, however the mutations bound for the other side will build up on the transaction logs. Eventually, the disks on the primary's transaction logs will fill up, so the database cannot be left in this condition indefinitely. 

.. warning:: While a datacenter has failed, the maximum write throughput of the cluster will be roughly 1/3 of normal performance. This is because the transaction logs need to store all of the mutations being committed, so that once the other datacenter comes back online, it can replay history to catch back up.

To drop the dead datacenter do the following steps:

1. Configure the region configuration so that the dead datacenter has a negative priority.

2. Configure ``usable_regions=1``.

If you are running in a configuration without a satellite datacenter, or you have lost all machines in a region simultaneously, the ``force_recovery_with_data_loss`` command from ``fdbcli`` allows you to force a recovery to the other region.  This will discard the portion of the mutation log which did not make it across the WAN. Once the database has recovered, immediately follow the previous steps to drop the dead region the normal way.

Region change safety
--------------------

The steps described above for both adding and removing replicas are enforced by ``fdbcli``. The following are the specific conditions checked by ``fdbcli``:

* You cannot change the ``regions`` configuration while also changing ``usable_regions``.
* You can only change ``usable_regions`` when exactly one region has priority >= 0.
* When ``usable_regions`` > 1, all regions with priority >= 0 must have a full replica of the data.
* All storage servers must be in one of the regions specified by the region configuration.

Monitoring
----------

It is important to ensure the remote replica does not fall too far behind the active replica. To failover between regions, all of the mutations need to be flushed from the active replica to the remote replica. If the remote replica is too far behind, this can take a very long time. The version difference between the datacenters is available in ``status json`` as ``datacenter_version_difference``. This number should be less than 5 million. A large datacenter version difference could indicate that more log routers are needed. It could also be caused by network issues between the regions. If the difference becomes too large the remote replica should be dropped, similar to a datacenter outage that goes on too long.

Because of asymmetric write latencies in the two regions, it important to route client traffic to the currently active region. The current active region is written in the system key space as the key ``\xff/primaryDatacenter``. Clients can read and watch this key after setting the ``read_system_keys`` transaction option.

Choosing coordinators
---------------------

Choosing coordinators for a multi-region configuration provides its own set of challenges. A majority of coordinators need to be alive for the cluster to be available. There are two common coordinators setups that allow a cluster to survive the simultaneous loss of a datacenter and one additional machine.

The first is five coordinators in five different datacenters. The second is nine total coordinators spread across three datacenters. There is some additional benefit to spreading the coordinators across regions rather than datacenters. This is because if an entire region fails, it is still possible to recover to the other region if you are willing to accept a small amount of data loss. However, if you have lost a majority of coordinators, this becomes much more difficult.

Additionally, if a datacenter fails and then the second datacenter in the region fails 30 seconds later, we can generally survive this scenario. The second datacenter only needs to be alive long enough to copy the tail of the mutation log across the WAN. However if your coordinators are in this second datacenter, you will still experience an outage.

These considerations mean that best practice is to put three coordinators in the main datacenters of each of the two regions, and then put three additional coordinators in a third region.

Comparison to other multiple datacenter configurations
------------------------------------------------------

Region configuration provides very similar functionality to ``fdbdr``.

If you are not using satellite datacenters, the main benefit of a region configuration compared to ``fdbdr`` is that each datacenter is able to restore replication even after losing all copies of a key range. If we simultaneously lose two storage servers in a double replicated cluster, with ``fdbdr`` we would be forced to fail over to the remote region. With region configuration the cluster will automatically copy the missing key range from the remote replica back to the primary datacenter.

The main disadvantage of using a region configuration is that the total number of processes we can support in a single region is around half when compared against ``fdbdr``. This is because we have processes for both regions in the same cluster, and some singleton components like the failure monitor will have to do twice as much work. In ``fdbdr``, there are two separate clusters for each region, so the total number of processes can scale to about twice as large as using a region configuration.

Region configuration is better in almost all ways than the ``three_datacenter`` replication mode. Region configuration gives the same ability to survive the loss of one datacenter, however we only need to store two full replicas of the database instead of three. Region configuration is more efficient with how it sends mutations across the WAN. The only reason to use ``three_datacenter`` replication is if low latency reads from all three locations is required.

Known limitations
-----------------

The 6.2 release still has a number of rough edges related to region configuration. This is a collection of all the issues that have been pointed out in the sections above. These issues should be significantly improved in future releases of FoundationDB:

* FoundationDB supports replicating data to at most two regions.
* ``two_satellite_fast`` does not hide latency properly when configured with more than 4 satellite transaction logs.

.. _guidelines-process-class-config:

Guidelines for setting process class
====================================

In a FoundationDB cluster, each of the ``fdbserver`` processes perform different tasks. Each process is recruited to do a particular task based on its process ``class``. For example, processes with ``class=storage`` are given preference to be recruited for doing storage server tasks, ``class=transaction`` are for log server processes and ``class=stateless`` are for stateless processes like commit proxies, resolvers, etc.,

The recommended minimum number of ``class=transaction`` (log server) processes is 8 (active) + 2 (standby) and the recommended minimum number for ``class=stateless`` processes is 1 (GRV proxy) + 3 (commit proxy) + 1 (resolver) + 1 (cluster controller) + 1 (master) + 2 (standby). It is better to spread the transaction and stateless processes across as many machines as possible.

``fdbcli`` is used to set the desired number of processes of a particular process type. To do so, you would issue the ``fdbcli`` commands::

  fdb> configure grv_proxies=1
  fdb> configure grv_proxies=4
  fdb> configure logs=8

.. note:: In the present release, the default value for commit proxies and log servers is 3 and for GRV proxies and resolvers is 1. You should not set the value of a process type to less than its default.

.. warning:: The conflict-resolution algorithm used by FoundationDB is conservative: it guarantees that no conflicting transactions will be committed, but it may fail to commit some transactions that theoretically could have been. The effects of this conservatism may increase as you increase the number of resolvers. It is therefore important to employ the recommended techniques for :ref:`minimizing conflicts <developer-guide-transaction-conflicts>` when increasing the number of resolvers.

You can contact us on the `community forums <https://forums.foundationdb.org>`_ if you are interested in more details or if you are benchmarking or performance-tuning on large clusters. Also see our :doc:`performance benchmarks <performance>` for a baseline of how a well-configured cluster should perform.
