##############
Administration
##############

.. include:: guide-common.rst.inc

.. toctree::
   :maxdepth: 1
   :hidden:
   :titlesonly:

   configuration
   moving-a-cluster
   tls
   authorization

This document covers the administration of an existing FoundationDB cluster. We recommend you read this document before setting up a cluster for performance testing or production use.

.. note:: In FoundationDB, a "cluster" refers to one or more FoundationDB processes spread across one or more physical machines that together host a FoundationDB database.

To administer an externally accessible cluster, you need to understand basic system tasks. You should begin with how to :ref:`start and stop the database <administration-running-foundationdb>`. Next, you should review management of a cluster, including :ref:`adding <adding-machines-to-a-cluster>` and :ref:`removing <removing-machines-from-a-cluster>` machines, and monitoring :ref:`cluster status <administration-monitoring-cluster-status>` and the basic :ref:`server processes <administration_fdbmonitor>`. You should be familiar with :ref:`managing trace files <administration-managing-trace-files>` and :ref:`other administrative concerns <administration-other-administrative-concerns>`. Finally, you should know how to :ref:`uninstall <administration-removing>` or :ref:`upgrade <upgrading-foundationdb>` the database.

FoundationDB also provides a number of different :doc:`configuration <configuration>` options which you should know about when setting up a FoundationDB database.

.. _administration-running-foundationdb:

Starting and stopping
=====================

After installation, FoundationDB is set to start automatically. You can manually start and stop the database with the commands shown below.

Linux
-----

On Linux, FoundationDB is started and stopped using the ``service`` command as follows::

    user@host$ sudo service foundationdb start
    user@host$ sudo service foundationdb stop

On Ubuntu, it can be prevented from starting at boot as follows (without stopping the service)::

    user@host$ sudo update-rc.d foundationdb disable

On RHEL/CentOS, it can be prevented from starting at boot as follows (without stopping the service)::

    user@host$ sudo chkconfig foundationdb off

macOS
-----

On macOS, FoundationDB is started and stopped using ``launchctl`` as follows::

    host:~ user$ sudo launchctl load /Library/LaunchDaemons/com.foundationdb.fdbmonitor.plist
    host:~ user$ sudo launchctl unload /Library/LaunchDaemons/com.foundationdb.fdbmonitor.plist

It can be stopped and prevented from starting at boot as follows::

    host:~ user$ sudo launchctl unload -w /Library/LaunchDaemons/com.foundationdb.fdbmonitor.plist

Start, stop and restart behavior
=================================

These commands above start and stop the ``fdbmonitor`` process, which in turn starts ``fdbserver`` and ``backup-agent`` processes.  See :ref:`administration_fdbmonitor` for details.

After any child process has terminated by any reason, ``fdbmonitor`` tries to restart it. See :ref:`restarting parameters <configuration-restarting>`.

When ``fdbmonitor`` itself is killed unexpectedly (for example, by the ``out-of-memory killer``), all the child processes are also terminated. Then the operating system is responsible for restarting it. See :ref:`Configuring autorestart of fdbmonitor <configuration-restart-fdbmonitor>`.

.. _foundationdb-cluster-file:

Cluster files
=============

FoundationDB servers and clients use a cluster file (usually named ``fdb.cluster``) to connect to a cluster. The contents of the cluster file are the same for all processes that connect to the cluster. An ``fdb.cluster`` file is created automatically when you install a FoundationDB server and updated automatically when you :ref:`change coordination servers <configuration-choosing-coordination-servers>`. To connect to a cluster from a client machine, you will need access to a copy of the cluster file used by the servers in the cluster.  Typically, you will copy the ``fdb.cluster`` file from the :ref:`default location <default-cluster-file>` on a FoundationDB server to the default location on each client.

.. warning:: This file should not normally be modified manually. To change coordination servers, see :ref:`configuration-choosing-coordination-servers`.

.. _default-cluster-file:

Default cluster file
--------------------

When you initially install FoundationDB, a default ``fdb.cluster`` file will be placed at a system-dependent location:

* Linux: ``/etc/foundationdb/fdb.cluster``
* macOS: ``/usr/local/etc/foundationdb/fdb.cluster``

.. _specifying-a-cluster-file:

Specifying the cluster file
---------------------------

All FoundationDB components can be configured to use a specified cluster file: 

* The ``fdbcli`` tool allows a cluster file to be passed on the command line using the ``-C`` option.
* The :doc:`client APIs <api-reference>` allow a cluster file to be passed when connecting to a cluster, usually via ``open()``.
* A FoundationDB server or ``backup-agent`` allow a cluster file to be specified in :ref:`foundationdb.conf <foundationdb-conf>`.

In addition, FoundationDB allows you to use the environment variable ``FDB_CLUSTER_FILE`` to specify a cluster file. This approach is helpful if you operate or access more than one cluster.

All FoundationDB components will determine a cluster file in the following order:

1. An explicitly provided file, whether a command line argument using ``-C`` or an argument to an API function, if one is given;
2. The value of the ``FDB_CLUSTER_FILE`` environment variable, if it has been set;
3. An ``fdb.cluster`` file in the current working directory, if one is present;
4. The :ref:`default file <default-cluster-file>` at its system-dependent location.

This automatic determination of a cluster file makes it easy to write code using FoundationDB without knowing exactly where it will be installed or what database it will need to connect to.

.. warning:: A cluster file must have the :ref:`required permissions <cluster_file_permissions>` in order to be used.

.. warning:: If ``FDB_CLUSTER_FILE`` is read and has been set to an invalid value (such as an empty value, a file that does not exist, or a file that is not a valid cluster file), an error will result. FoundationDB will not fall back to another file.

.. _cluster_file_permissions:

Required Permissions
--------------------

FoundationDB servers and clients require read *and* write access to the cluster file and its parent directory. This is because certain administrative changes to the cluster configuration (see :ref:`configuration-choosing-coordination-servers`) can cause this file to be automatically modified by all servers and clients using the cluster. If a FoundationDB process cannot update the cluster file, it may eventually become unable to connect to the cluster.

.. _cluster-file-format:

Cluster file format
-------------------

The cluster file contains a connection string consisting of a cluster identifier and a comma-separated list of IP addresses (not hostnames) specifying the coordination servers. The format for the file is::

    description:ID@IP:PORT,IP:PORT,...

* |cluster-file-rule1|
* |cluster-file-rule2|
* |cluster-file-rule3|

Together the ``description`` and the ``ID`` should uniquely identify a FoundationDB cluster.

A cluster file may contain comments, marked by the ``#`` character. All characters on a line after the first occurrence of a ``#`` will be ignored.

Generally, a cluster file should not be modified manually. Incorrect modifications after a cluster is created could result in data loss. To change the set of coordination servers used by a cluster, see :ref:`configuration-choosing-coordination-servers`. To change the cluster ``description``, see :ref:`configuration-setting-cluster-description`.

It is very important that each cluster use a unique random ID. If multiple processes use the same database description and ID but different sets of coordination servers, data corruption could result.

.. _cluster-file-client-access:

Accessing cluster file information from a client
------------------------------------------------

Any client connected to FoundationDB can access information about its cluster file directly from the database:

* To get the path to the cluster file, read the key ``\xFF\xFF/cluster_file_path``.
* To get the desired contents of the cluster file, read the key ``\xFF\xFF/connection_string``. Make sure the client can write to the cluster file and keep it up to date.

.. _ipv6-support:

IPv6 Support
============

FoundationDB (since v6.1) can accept network connections from clients connecting over IPv6. IPv6 address/port pair is represented as ``[IP]:PORT``, e.g. "[::1]:4800", "[abcd::dead:beef]:4500".

1) The cluster file can contain mix of IPv4 and IPv6 addresses. For example::

     description:ID@127.0.0.1:4500,[::1]:4500,...

2) Starting ``fdbserver`` with IPv6::

     $ /path/to/fdbserver -C fdb.cluster -p \[::1\]:4500

.. _adding-machines-to-a-cluster:

Adding machines to a cluster
============================

.. warning:: |development-use-only-warning|

You can add new machines to a cluster at any time:

1) :doc:`Install FoundationDB <getting-started-linux>` on the new machine.

2) |optimize-configuration|

3) Copy an :ref:`existing cluster file <specifying-a-cluster-file>` from a server in your cluster to the new machine, overwriting the existing ``fdb.cluster`` file.

4) Restart FoundationDB on the new machine so that it uses the new cluster file::

    user@host2$ sudo service foundationdb restart

5) If you have previously :ref:`excluded <removing-machines-from-a-cluster>` a machine from the cluster, you will need to take it off the exclusion list using the ``include <ip>`` or ``include <locality>`` command of fdbcli before it can be a full participant in the cluster.

.. note:: Addresses have the form ``IP``:``PORT``. This form is used even if TLS is enabled.

.. _removing-machines-from-a-cluster:

Removing machines from a cluster
==================================

To temporarily or permanently remove one or more machines from a FoundationDB cluster without compromising fault tolerance or availability, perform the following steps:

1) Make sure that your current redundancy mode will still make sense after removing the machines you want to remove. For example, if you are currently using ``triple`` redundancy and are reducing the number of servers to fewer than five, you should probably switch to a lower redundancy mode first. See :ref:`configuration-choosing-redundancy-mode`.

2) If any of the machines that you would like to remove is a coordinator, you should :ref:`change coordination servers <configuration-changing-coordination-servers>` to a set of servers that you will not be removing. Remember that even after changing coordinators, the old coordinators need to remain available until all servers and clients of the cluster have automatically updated their cluster files.

3) Use the ``exclude`` command in ``fdbcli`` on the machines you plan to remove:
    
::

    user@host1$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> exclude 1.2.3.4 1.2.3.5 1.2.3.6 locality_dcid:primary-satellite locality_zoneid:primary-satellite-log-2 locality_machineid:primary-stateless-1 locality_processid:223be2da244ca0182375364e4d122c30 or any locality
    Waiting for state to be removed from all excluded servers.  This may take a while.
    It is now safe to remove these machines or processes from the cluster.


``exclude`` can be used to exclude either machines (by specifying an IP address) or individual processes (by specifying an ``IP``:``PORT`` pair or by specifying a locality match).

.. note:: Addresses have the form ``IP``:``PORT``. This form is used even if TLS is enabled.

Excluding a server doesn't shut it down immediately; data on the machine is first moved away. When the ``exclude`` command completes successfully (by returning control to the command prompt), the machines that you specified are no longer required to maintain the configured redundancy mode. A large amount of data might need to be transferred first, so be patient. When the process is complete, the excluded machine or process can be shut down without fault tolerance or availability consequences.

If you interrupt the exclude command with Ctrl-C after seeing the "waiting for state to be removed" message, the exclusion work will continue in the background. Repeating the command will continue waiting for the exclusion to complete. To reverse the effect of the ``exclude`` command, use the ``include`` command.

    Excluding a server with the ``failed`` flag will shut it down immediately; it will assume that it has already become unrecoverable or unreachable, and will not attempt to move the data on the machine away. This may break the guarantee required to maintain the configured redundancy mode, which will be checked internally, and the command may be denied if the guarantee is violated. This safety check can be ignored by using the command ``exclude FORCE failed``.

    In case you want to include a new machine with the same address as a server previously marked as failed, you can allow it to join by using the ``include failed`` command.

4) On each removed machine, stop the FoundationDB server and prevent it from starting at the next boot. Follow the :ref:`instructions for your platform <administration-running-foundationdb>`. For example, on Ubuntu::

    user@host3$ sudo service foundationdb stop
    user@host3$ sudo update-rc.d foundationdb disable

5) :ref:`test-the-database` to double check that everything went smoothly, paying particular attention to the replication health.

6) You can optionally :ref:`uninstall <administration-removing>` the FoundationDB server package entirely and/or delete database files on removed servers.

7) If you ever want to add a removed machine back to the cluster, you will have to take it off the excluded servers list to which it was added in step 3. This can be done using the ``include`` command of ``fdbcli``. If attempting to re-include a failed server, this can be done using the ``include failed`` command of ``fdbcli``. Typing ``exclude`` with no parameters will tell you the current list of excluded and failed machines.

As of api version 700, excluding servers can be done with the :ref:`special key space management module <special-key-space-management-module>` as well.

Moving a cluster
================

The procedures for adding and removing machines can be combined into a recipe for :doc:`moving an existing cluster to new machines <moving-a-cluster>`.

Converting an existing cluster to use TLS
=========================================

A FoundationDB cluster has the option of supporting :doc:`Transport Layer Security (TLS) <tls>`. To enable TLS on an existing, non-TLS cluster, see :ref:`Converting a running cluster <converting-existing-cluster-after-6.1>`.

.. _administration-monitoring-cluster-status:

Monitoring cluster status
=========================

Use the ``status`` command of ``fdbcli`` to determine if the cluster is up and running::

    user@host$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> status

    Configuration:
      Redundancy mode        - triple
      Storage engine         - ssd-2
      Coordinators           - 5
      Desired GRV Proxies    - 1
      Desired Commit Proxies - 4
      Desired Logs           - 8

    Cluster:
      FoundationDB processes - 272
      Machines               - 16
      Memory availability    - 14.5 GB per process on machine with least available
      Retransmissions rate   - 20 Hz
      Fault Tolerance        - 2 machines
      Server time            - 03/19/18 08:51:52
    
    Data:
      Replication health     - Healthy
      Moving data            - 0.000 GB
      Sum of key-value sizes - 3.298 TB
      Disk space used        - 15.243 TB
    
    Operating space:
      Storage server         - 1656.2 GB free on most full server
      Log server             - 1794.7 GB free on most full server
    
    Workload:
      Read rate              - 55990 Hz
      Write rate             - 14946 Hz
      Transactions started   - 6321 Hz
      Transactions committed - 1132 Hz
      Conflict rate          - 0 Hz
    
    Backup and DR:
      Running backups        - 1
      Running DRs            - 1 as primary
    
    Client time: 03/19/18 08:51:51

The summary fields are interpreted as follows:

====================== ==========================================================================================================
Redundancy mode         The currently configured redundancy mode (see the section :ref:`configuration-choosing-redundancy-mode`)
Storage engine          The currently configured storage engine (see the section :ref:`configuration-configuring-storage-subsystem`)
Coordinators            The number of FoundationDB coordination servers
Desired GRV Proxies     Number of GRV proxies desired. (default 1)
Desired Commit Proxies  Number of commit proxies desired. If replication mode is 3 then default number of commit proxies is 3
Desired Logs            Number of logs desired. If replication mode is 3 then default number of logs is 3
FoundationDB processes  Number of FoundationDB processes participating in the cluster
Machines                Number of physical machines running at least one FoundationDB process that is participating in the cluster
Memory availability     RAM per process on machine with least available (see details below)
Retransmissions rate    Ratio of retransmitted packets to the total number of packets.
Fault tolerance         Maximum number of machines that can fail without losing data or availability (number for losing data will be reported separately if lower)
Server time             Timestamp from the server
Replication health      A qualitative estimate of the health of data replication
Moving data             Amount of data currently in movement between machines
Sum of key-value sizes	Estimated total size of keys and values stored (not including any overhead or replication)
Disk space used         Overall disk space used by the cluster
Storage server          Free space for storage on the server with least available. For ``ssd`` storage engine, includes only disk; for ``memory`` storage engine, includes both RAM and disk.
Log server              Free space for log server on the server with least available.
Read rate               The current number of reads per second
Write rate              The current number of writes per second
Transaction started     The current number of transactions started per second
Transaction committed   The current number of transactions committed per second
Conflict rate           The current number of conflicts per second
Running backups         Number of backups currently running. Different backups could be backing up to different prefixes and/or to different targets.
Running DRs             Number of DRs currently running. Different DRs could be streaming different prefixes and/or to different DR clusters.
====================== ==========================================================================================================

The "Memory availability" is a conservative estimate of the minimal RAM available to any ``fdbserver`` process across all machines in the cluster. This value is calculated in two steps. Memory available per process is first calculated *for each machine* by taking::

    availability = ((total - committed) + sum(processSize)) / processes

where:

===========   ==================================================
total         total RAM on the machine
committed     committed RAM on the machine
processSize   total physical memory used by a given ``fdbserver`` process
processes     number of ``fdbserver`` processes on the machine
===========   ==================================================

The reported value is then the *minimum* of memory available per process *over all machines* in the cluster. If this value is below 4.0 GB, a warning message is added to the status report.

Process details
---------------

The ``status`` command can provide detailed statistics about the cluster and the database by giving it the ``details`` argument::

    user@host$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> status details


    Configuration:
      Redundancy mode        - triple
      Storage engine         - ssd-2
      Coordinators           - 5
    
    Cluster:
      FoundationDB processes - 85
      Machines               - 5
      Memory availability    - 7.4 GB per process on machine with least available
      Retransmissions rate   - 5 Hz
      Fault Tolerance        - 2 machines
      Server time            - 03/19/18 08:59:37
    
    Data:
      Replication health     - Healthy
      Moving data            - 0.000 GB
      Sum of key-value sizes - 87.068 GB
      Disk space used        - 327.819 GB
    
    Operating space:
      Storage server         - 888.2 GB free on most full server
      Log server             - 897.3 GB free on most full server
    
    Workload:
      Read rate              - 117 Hz
      Write rate             - 0 Hz
      Transactions started   - 43 Hz
      Transactions committed - 1 Hz
      Conflict rate          - 0 Hz

    Process performance details:
      10.0.4.1:4500     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 3.2 GB / 7.4 GB RAM  )
      10.0.4.1:4501     (  1% cpu;  2% machine; 0.010 Gbps;  3% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.1:4502     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.1:4503     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.1:4504     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.1:4505     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.1:4506     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.1:4507     (  2% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.1:4508     (  2% cpu;  2% machine; 0.010 Gbps;  1% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.1:4509     (  2% cpu;  2% machine; 0.010 Gbps;  1% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.1:4510     (  1% cpu;  2% machine; 0.010 Gbps;  1% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.1:4511     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.1:4512     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.1:4513     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.1:4514     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.1:4515     ( 12% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.1:4516     (  0% cpu;  2% machine; 0.010 Gbps;  0% disk IO; 0.3 GB / 7.4 GB RAM  )
      10.0.4.2:4500     (  2% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 3.2 GB / 7.4 GB RAM  )
      10.0.4.2:4501     ( 15% cpu;  3% machine; 0.124 Gbps; 19% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4502     (  2% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4503     (  2% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4504     (  2% cpu;  3% machine; 0.124 Gbps;  1% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4505     ( 18% cpu;  3% machine; 0.124 Gbps; 18% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4506     (  2% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4507     (  2% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4508     (  2% cpu;  3% machine; 0.124 Gbps; 19% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4509     (  0% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4510     (  0% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4511     (  2% cpu;  3% machine; 0.124 Gbps;  1% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4512     (  2% cpu;  3% machine; 0.124 Gbps; 19% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.2:4513     (  0% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.2:4514     (  0% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.2:4515     ( 11% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.2:4516     (  0% cpu;  3% machine; 0.124 Gbps;  0% disk IO; 0.6 GB / 7.4 GB RAM  )
      10.0.4.3:4500     ( 14% cpu;  3% machine; 0.284 Gbps; 26% disk IO; 3.0 GB / 7.4 GB RAM  )
      10.0.4.3:4501     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.8 GB / 7.4 GB RAM  )
      10.0.4.3:4502     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.8 GB / 7.4 GB RAM  )
      10.0.4.3:4503     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.3:4504     (  7% cpu;  3% machine; 0.284 Gbps; 12% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.3:4505     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.3:4506     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.3:4507     (  2% cpu;  3% machine; 0.284 Gbps; 26% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.3:4508     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.3:4509     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.3:4510     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.3:4511     (  2% cpu;  3% machine; 0.284 Gbps; 12% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.3:4512     (  2% cpu;  3% machine; 0.284 Gbps;  3% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.3:4513     (  2% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.3:4514     (  0% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 0.1 GB / 7.4 GB RAM  )
      10.0.4.3:4515     (  0% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 0.1 GB / 7.4 GB RAM  )
      10.0.4.3:4516     (  0% cpu;  3% machine; 0.284 Gbps;  0% disk IO; 0.1 GB / 7.4 GB RAM  )
      10.0.4.4:4500     (  2% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 3.2 GB / 7.4 GB RAM  )
      10.0.4.4:4501     (  2% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4502     (  0% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4503     (  2% cpu;  4% machine; 0.065 Gbps; 16% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4504     (  2% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.4:4505     (  0% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4506     (  0% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4507     (  2% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4508     (  0% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4509     (  2% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4510     ( 24% cpu;  4% machine; 0.065 Gbps; 15% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4511     (  2% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.8 GB / 7.4 GB RAM  )
      10.0.4.4:4512     (  2% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.4:4513     (  0% cpu;  4% machine; 0.065 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.4:4514     (  0% cpu;  4% machine; 0.065 Gbps;  1% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.4:4515     (  0% cpu;  4% machine; 0.065 Gbps;  1% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.4:4516     (  0% cpu;  4% machine; 0.065 Gbps;  1% disk IO; 0.6 GB / 7.4 GB RAM  )
      10.0.4.5:4500     (  6% cpu;  2% machine; 0.076 Gbps;  7% disk IO; 3.2 GB / 7.4 GB RAM  )
      10.0.4.5:4501     (  2% cpu;  2% machine; 0.076 Gbps; 19% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4502     (  1% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4503     (  0% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4504     (  2% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.5:4505     (  2% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.5:4506     (  0% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4507     (  2% cpu;  2% machine; 0.076 Gbps;  6% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4508     ( 31% cpu;  2% machine; 0.076 Gbps;  8% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.5:4509     (  0% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4510     (  2% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.7 GB / 7.4 GB RAM  )
      10.0.4.5:4511     (  2% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4512     (  2% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4513     (  0% cpu;  2% machine; 0.076 Gbps;  3% disk IO; 2.6 GB / 7.4 GB RAM  )
      10.0.4.5:4514     (  0% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.5:4515     (  0% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 0.2 GB / 7.4 GB RAM  )
      10.0.4.5:4516     (  0% cpu;  2% machine; 0.076 Gbps;  0% disk IO; 0.6 GB / 7.4 GB RAM  )

    Coordination servers:
      10.0.4.1:4500  (reachable)
      10.0.4.2:4500  (reachable)
      10.0.4.3:4500  (reachable)
      10.0.4.4:4500  (reachable)
      10.0.4.5:4500  (reachable)
    
    Client time: 03/19/18 08:59:37

Several details about individual FoundationDB processes are displayed in a list format in parenthesis after the IP address and port:

======= =========================================================================
cpu	    CPU utilization of the individual process
machine	CPU utilization of the machine the process is running on (over all cores)
Gbps	Total input + output network traffic, in Gbps
disk IO	Percentage busy time of the disk subsystem on which the data resides
REXMIT! Displayed only if there have been more than 10 TCP segments retransmitted in last 5s
RAM     Total physical memory used by process / memory available per process
======= =========================================================================

In certain cases, FoundationDB's overall performance can be negatively impacted by an individual slow or degraded computer or subsystem. If you suspect this is the case, this detailed list is helpful to find the culprit.

If a process has had more than 10 TCP segments retransmitted in the last 5 seconds, the warning message ``REXMIT!`` is displayed between its disk and RAM values, leading to an output under ``Process performance details`` of the form::

      10.0.4.1:4500       ( 3% cpu;  2% machine; 0.004 Gbps;  0% disk; REXMIT! 2.5 GB / 4.1 GB RAM  )

Machine-readable status
-----------------------

The status command can provide a complete summary of statistics about the cluster and the database with the ``json`` argument. Full documentation for ``status json`` output can be found :doc:`here <mr-status>`.
From the output of ``status json``, operators can find useful health metrics to determine whether or not their cluster is hitting performance limits.

====================== ==============================================================================================================
Ratekeeper limit        ``cluster.qos.transactions_per_second_limit`` contains the number of read versions per second that the cluster can give out. A low ratekeeper limit indicates that the cluster performance is limited in some way. The reason for a low ratekeeper limit can be found at ``cluster.qos.performance_limited_by``. ``cluster.qos.released_transactions_per_second`` describes the number of read versions given out per second, and can be used to tell how close the ratekeeper is to throttling.
Storage queue size      ``cluster.qos.worst_queue_bytes_storage_server`` contains the maximum size in bytes of a storage queue. Each storage server has mutations that have not yet been made durable, stored in its storage queue. If this value gets too large, it indicates a storage server is falling behind. A large storage queue will cause the ratekeeper to increase throttling. However, depending on the configuration, the ratekeeper can ignore the worst storage queue from one fault domain. Thus, ratekeeper uses ``cluster.qos.limiting_queue_bytes_storage_server`` to determine the throttling level.
Durable version lag     ``cluster.qos.worst_durability_lag_storage_server`` contains information about the worst storage server durability lag. The ``versions`` subfield contains the maximum number of versions in a storage queue. Ideally, this should be near 5 million. The ``seconds`` subfield contains the maximum number of seconds of non-durable data in a storage queue. Ideally, this should be near 5 seconds. If a storage server is overwhelmed, the durability lag could rise, causing performance issues.
Transaction log queue   ``cluster.qos.worst_queue_bytes_log_server`` contains the maximum size in bytes of the mutations stored on a transaction log that have not yet been popped by storage servers. A large transaction log queue size can potentially cause the ratekeeper to increase throttling.
====================== ==============================================================================================================

Server-side latency band tracking
---------------------------------

As part of the status document, ``status json`` provides some sampled latency metrics obtained by running probe transactions internally. While this can often be useful, it does not necessarily reflect the distribution of latencies for requests originated by clients.

FoundationDB additionally provides optional functionality to measure the latencies of all incoming get read version (GRV), read, and commit requests and report some basic details about those requests. The latencies are measured from the time the server receives the request to the point when it replies, and will therefore not include time spent in transit between the client and server or delays in the client process itself.

The latency band tracking works by configuring various latency thresholds and counting the number of requests that occur in each band (i.e. between two consecutive thresholds). For example, if you wanted to define a service-level objective (SLO) for your cluster where 99.9% of read requests were answered within N seconds, you could set a read latency threshold at N. You could then count the number of requests below and above the threshold and determine whether the required percentage of requests are answered sufficiently quickly.

Configuration of server-side latency bands is performed by setting the ``\xff\x02/latencyBandConfig`` key to a string encoding the following JSON document::

  { 
    "get_read_version" : { 
      "bands" : [ 0.01, 0.1] 
    }, 
    "read" : { 
      "bands" : [ 0.01, 0.1],
      "max_key_selector_offset" : 1000,
      "max_read_bytes" : 1000000 
    }, 
    "commit" : { 
      "bands" : [ 0.01, 0.1], 
      "max_commit_bytes" : 1000000 
    } 
  }

Every field in this configuration is optional, and any missing fields will be left unset (i.e. no bands will be tracked or limits will not apply). The configuration takes the following arguments:

* ``bands`` - a list of thresholds (in seconds) to be measured for the given request type (``get_read_version``, ``read``, or ``commit``) 
* ``max_key_selector_offset`` - an integer specifying the maximum key selector offset a read request can have and still be counted
* ``max_read_bytes`` - an integer specifying the maximum size in bytes of a read response that will be counted
* ``max_commit_bytes`` - an integer specifying the maximum size in bytes of a commit request that will be counted

Setting this configuration key to a value that changes the configuration will result in the cluster controller server process logging a ``LatencyBandConfigChanged`` event. This event will indicate whether a configuration is present or not using its ``Present`` field. Specifying an invalid configuration will result in the latency band feature being unconfigured, and the server process running the cluster controller will log a ``InvalidLatencyBandConfiguration`` trace event.

.. note:: GRV requests are counted only at default and immediate priority. Batch priority GRV requests are ignored for the purposes of latency band tracking.

When configured, the ``status json`` output will include additional fields to report the number of requests in each latency band located at ``cluster.processes.<ID>.roles[N].*_latency_bands``::

  "grv_latency_bands" : {
    0.01: 10,
    0.1: 0,
    inf: 1,
    filtered: 0
  },
  "read_latency_bands" : {
    0.01: 12,
    0.1: 1,
    inf: 0,
    filtered: 0
  },
  "commit_latency_bands" : {
    0.01: 5,
    0.1: 5,
    inf: 2,
    filtered: 1
  }

The ``grv_latency_bands`` objects will only be logged for ``grv_proxy`` roles, ``commit_latency_bands`` objects will only be logged for ``commit_proxy`` roles, and ``read_latency_bands`` will only be logged for storage roles. Each threshold is represented as a key in the map, and its associated value will be the total number of requests in the lifetime of the process with a latency smaller than the threshold but larger than the next smaller threshold. 

For example, ``0.1: 1`` in ``read_latency_bands`` indicates that there has been 1 read request with a latency in the range ``[0.01, 0.1)``. For the smallest specified threshold, the lower bound is 0 (e.g. ``[0, 0.01)`` in the example above). Requests that took longer than any defined latency band will be reported in the ``inf`` (infinity) band. Requests that were filtered by the configuration (e.g. using ``max_read_bytes``) are reported in the ``filtered`` category.

Because each threshold reports latencies strictly in the range between the next lower threshold and itself, it may be necessary to sum up the counts for multiple bands to determine the total number of requests below a certain threshold.

.. note:: No history of request counts is recorded for processes that ran in the past. This includes the history prior to restart for a process that has been restarted, for which the counts get reset to 0. For this reason, it is recommended that you collect this information periodically if you need to be able to track requests from such processes.

.. _administration_fdbmonitor:

``fdbmonitor`` and ``fdbserver``
================================

The core FoundationDB server process is ``fdbserver``.  Each ``fdbserver`` process uses up to one full CPU core, so a production FoundationDB cluster will usually run N such processes on an N-core system.

To make configuring, starting, stopping, and restarting ``fdbserver`` processes easy, FoundationDB also comes with a singleton daemon process, ``fdbmonitor``, which is started automatically on boot.  ``fdbmonitor`` reads the :ref:`foundationdb.conf <foundationdb-conf>` file and starts the configured set of ``fdbserver`` processes.  It is also responsible for starting ``backup-agent``.  

.. note:: |conf-file-change-detection|

During normal operation, ``fdbmonitor`` is transparent, and you interact with it only by modifying the configuration in :ref:`foundationdb.conf <foundationdb-conf>` and perhaps occasionally by :ref:`starting and stopping <administration-running-foundationdb>` it manually. If some problem prevents an ``fdbserver`` or ``backup-agent`` process from starting or causes it to stop unexpectedly, ``fdbmonitor`` will log errors to the system log.

If ``kill_on_configuration_change`` parameter is unset or set to ``true`` in foundationdb.conf then fdbmonitor will restart monitored processes on changes automatically. If this parameter is set to ``false`` it will not restart any monitored processes on changes.

.. _administration-managing-trace-files:

Managing trace files
====================

By default, trace files are output to:

* ``/var/log/foundationdb/`` on Linux
* ``/usr/local/foundationdb/logs/`` on macOS

Trace files are rolled every 10MB. These files are valuable to the FoundationDB development team for diagnostic purposes, and should be retained in case you need support from FoundationDB. Old trace files are automatically deleted so that there are no more than 100 MB worth of trace files per process. Both the log size and the maximum total size of the log files are configurable on a per process basis in the :ref:`configuration file <foundationdb-conf>`.

.. _administration-disaster-recovery:

Disaster Recovery
=================

In the present version of FoundationDB, disaster recovery (DR) is implemented via asynchronous replication of a source cluster to a destination cluster residing in another datacenter. The asynchronous replication updates the destination cluster using transactions consistent with those that have been committed in the source cluster. In this way, the replication process guarantees that the destination cluster is always in a consistent state that matches a present or earlier state of the source cluster.

Recovery takes place by reversing the asynchronous replication, so the data in the destination cluster is streamed back to a source cluster. For further information, see the :ref:`overview of backups <backup-introduction>` and the :ref:`fdbdr tool <fdbdr-intro>` that performs asynchronous replication.

Managing traffic
================

If clients of the database make use of the :doc:`transaction tagging feature <transaction-tagging>`, then the number of transactions allowed to start for different tags can be controlled using the :ref:`throttle command <cli-throttle>` in ``fdbcli``. 

.. _administration-other-administrative-concerns:

Other administrative concerns
=============================

.. _storage-space-requirements:

Storage space requirements
--------------------------

FoundationDB's storage space requirements depend on which storage engine is used.

Using the ``ssd`` storage engine, data is stored in B-trees that add some overhead.

* For key-value pairs larger than about 100 bytes, overhead should usually be less than 2x per replica. In a triple-replicated configuration, the raw capacity required might be 5x the size of the data. However, SSDs often require over-provisioning (e.g. keeping the drive less than 75% full) for best performance, so 7x would be a reasonable number. For example, 100GB of raw key-values would require 700GB of raw capacity.

* For very small key-value pairs, the overhead can be a large factor but not usually more than about 40 bytes per replica. Therefore, with triple replication and SSD over-provisioning, allowing 200 bytes of raw storage capacity for each very small key-value pair would be a reasonable guess. For example, 1 billion very small key-value pairs would require 200GB of raw storage.

Using the ``memory`` storage engine, both memory and disk space need to be considered.

* There is a fixed overhead of 72 bytes of memory for each key-value pair. Furthermore, memory is allocated in chunks whose sizes are powers of 2, leading to a variable padding overhead for each key-value pair. Finally, there is some overhead within memory chunks. For example, a 32 byte chunk has 6 bytes of overhead and therefore can only contain 26 bytes. As a result, a 27-byte key-value pair will be stored in a 64 byte chunk. The absolute amount of overhead within a chunk increases for larger chunks.

* Disk space usage is about 8x the original data size. The memory storage engine interleaves a snapshot on disk with a transaction log, with the resulting snapshot 2x the data size. A snapshot can't be dropped from its log until the next snapshot is completely written, so 2 snapshots must be kept at 4x the data size. The two-file durable queue can't overwrite data in one file until all the data in the other file has been dropped, resulting in 8x the data size. Finally, it should be noted that disk space is not reclaimed when key-value pairs are cleared.

For either storage engine, there is possible additional overhead when running backup or DR. In usual operation, the overhead is negligible but if backup is unable to write or a secondary cluster is unavailable, mutation logs will build up until copying can resume, occupying space in your cluster.

Running out of storage space
----------------------------

FoundationDB is aware of the free storage space on each node. It attempts to distribute data equally on all the nodes so that no node runs out of space before the others. The database attempts to gracefully stop writes as storage space decreases to 100 MB, refusing to start new transactions with priorities other than ``SYSTEM_IMMEDIATE``. This lower bound on free space leaves space to allow you to use ``SYSTEM_IMMEDIATE`` transactions to remove data.

The measure of free space depends on the storage engine. For the memory storage engine, which is the default after installation, total space is limited to the lesser of the ``storage_memory`` configuration parameter (1 GB in the default configuration) or a fraction of the free disk space.

If the disk is rapidly filled by other programs, trace files, etc., FoundationDB may be forced to stop with significant amounts of queued writes. The only way to restore the availability of the database at this point is to manually free storage space by deleting files.

Virtual machines
----------------

Processes running in different VMs on a single machine will appear to FoundationDB as being hardware isolated. FoundationDB takes pains to assure that data replication is protected from hardware-correlated failures. If FoundationDB is run in multiple VMs on a single machine this protection will be subverted. An administrator can inform FoundationDB of this hardware sharing, however, by specifying a machine ID using the ``locality_machineid`` parameter in :ref:`foundationdb.conf <foundationdb-conf>`. All processes on VMs that share hardware should specify the same ``locality_machineid``.

Datacenters
-----------

FoundationDB is datacenter aware and supports operation across datacenters. In a multiple-datacenter configuration, it is recommended that you set the :ref:`redundancy mode <configuration-choosing-redundancy-mode>` to ``three_datacenter`` and that you set the ``locality_dcid`` parameter for all FoundationDB processes in :ref:`foundationdb.conf <foundationdb-conf>`.

If you specify the ``--datacenter-id`` option to any FoundationDB process in your cluster, you should specify it to all such processes. Processes which do not have a specified datacenter ID on the command line are considered part of a default "unset" datacenter. FoundationDB will incorrectly believe that these processes are failure-isolated from other datacenters, which can reduce performance and fault tolerance.

(Re)creating a database
-----------------------

Installing FoundationDB packages usually creates a new database on the cluster automatically. However, if a cluster does not have a database configured (because the package installation failed to create it, you deleted your data files, or you did not install from the packages, etc.), then you may need to create it manually using the ``configure new`` command in ``fdbcli`` with the desired redundancy mode and storage engine::

    > configure new single memory

.. warning:: In a cluster that hasn't been configured, running ``configure new`` will cause the processes in the cluster to delete all data files in their data directories. If a process is reusing an existing data directory, be sure to backup any files that you want to keep. Do not use ``configure new`` to fix a previously working cluster that reports itself missing unless you are certain any necessary data files are safe.

.. _administration-removing:

Uninstalling
============

To uninstall FoundationDB from a cluster of one or more machines:

1. Uninstall the packages on each machine in the cluster.

   * On Ubuntu use::
   
       user@host$ sudo dpkg -P foundationdb-clients foundationdb-server
  
   * On RHEL/CentOS use::
   
       user@host$ sudo rpm -e foundationdb-clients foundationdb-server
  
   * On macOS use::
   
       host:~ user$ sudo /usr/local/foundationdb/uninstall-FoundationDB.sh

2. Delete all the data and configuration files stored by FoundationDB.

   * On Linux these will be in ``/var/lib/foundationdb/``, ``/var/log/foundationdb/``, and ``/etc/foundationdb/`` by default.
   * On macOS these will be in ``/usr/local/foundationdb/`` and ``/usr/local/etc/foundationdb/`` by default.

.. _upgrading-foundationdb:

Upgrading
=========

When a FoundationDB package is installed on a machine that already has a previous version, the package will upgrade FoundationDB to the newer version. For recent versions, the upgrade will preserve all previous data and configuration settings. (See the :ref:`notes on specific versions <version-specific-upgrading>` for exceptions.)

To upgrade a FoundationDB cluster, you must install the updated version of FoundationDB on each machine in the cluster. As the installations are taking place, the cluster will become unavailable until a sufficient number of machines have been upgraded. By following the steps below, you can perform a production upgrade with minimal downtime (seconds to minutes) and maintain all database guarantees. The instructions below assume that Linux packages are being used.

.. warning:: |development-use-only-warning|

.. note:: For information about upgrading client application code to newer API versions, see the :doc:`api-version-upgrade-guide`.

Install updated client binaries
-------------------------------

Apart from patch version upgrades, you should install the new client binary on all your clients and restart them to ensure they can reconnect after the upgrade. See :ref:`multi-version-client-api` for more information. Running ``status json`` will show you which versions clients are connecting with so you can verify before upgrading that clients are correctly configured.

Stage the packages
------------------

Go to :doc:`downloads` and select Ubuntu or RHEL/CentOS, as appropriate for your system. Download both the client and server packages and copy them to each machine in your cluster.

Perform the upgrade
-------------------

For **Ubuntu**, perform the upgrade using the dpkg command:

.. parsed-literal::

    user@host$ sudo dpkg -i |package-deb-clients| \\
    |package-deb-server|

For **RHEL/CentOS**, perform the upgrade using the rpm command:

.. parsed-literal::

    user@host$ sudo rpm -Uvh |package-rpm-clients| \\
    |package-rpm-server|

The ``foundationdb-clients`` package also installs the :doc:`C <api-c>` API. If your clients use :doc:`Ruby <api-ruby>`, :doc:`Python <api-python>`, `Java <javadoc/index.html>`_, or `Go <https://godoc.org/github.com/apple/foundationdb/bindings/go/src/fdb>`_, follow the instructions in the corresponding language documentation to install the APIs.

Test the database
-----------------

Test the database to verify that it is operating normally by running ``fdbcli`` and :ref:`reviewing the cluster status <administration-monitoring-cluster-status>`.

Remove old client library versions
----------------------------------

You can now remove old client library versions from your clients. This is only to stop creating unnecessary connections.

.. _version-specific-upgrading:

Version-specific notes on upgrading
===================================

Upgrading to 7.1.x
--------------------

Upgrades to 7.1.0 or later will break any client using ``fdb_transaction_get_range_and_flat_map``, as it is removed in version 7.1.0.

Upgrading from 6.2.x
--------------------

Upgrades from 6.2.x will keep all your old data and configuration settings.

Upgrading from 6.1.x
--------------------

Upgrades from 6.1.x will keep all your old data and configuration settings. Data distribution will slowly reorganize how data is spread across storage servers.

Upgrading from 6.0.x
--------------------

Upgrades from 6.0.x will keep all your old data and configuration settings.

Upgrading from 5.2.x
--------------------

Upgrades from 5.2.x will keep all your old data and configuration settings. Some affinities that certain roles have for running on processes that haven't set a process class have changed, which may result in these processes running in different locations after upgrading. To avoid this, set process classes as needed. The following changes were made:

* The proxies and master no longer prefer ``resolution`` or ``transaction`` class processes to processes with unset class.
* The resolver no longer prefers ``transaction`` class processes to processes with unset class.
* The cluster controller no longer prefers ``master``, ``resolution`` or ``proxy`` class processes to processes with unset class.

See :ref:`guidelines-process-class-config` for recommendations on setting process classes. All of the above roles will prefer ``stateless`` class processes to ones that don't set a class.

Upgrading from 5.0.x - 5.1.x
----------------------------

Upgrades from versions between 5.0.x and 5.1.x will keep all your old data and configuration settings. Backups that are running will automatically be aborted and must be restarted. 

.. _upgrading-from-older-versions:

Upgrading from Older Versions
-----------------------------

Upgrades from versions older than 5.0.0 are no longer supported.

Version-specific notes on downgrading
=====================================

In general, downgrades between non-patch releases (i.e. 6.2.x - 6.1.x) are not supported.

.. _downgrade-specific-version:

Downgrading from 6.3.13 - 6.2.33
--------------------------------
After upgrading from 6.2 to 6.3, the option of rolling back and downgrading to 6.2 is still possible, given that the following conditions are met:

* The 6.3 cluster cannot have ``TLogVersion`` greater than V4 (6.2).
* The 6.3 cluster cannot use storage engine types that are not ``ssd-1``, ``ssd-2``, or ``memory``.
* The 6.3 cluster must not have any key servers serialized with tag encoding. This condition can only be guaranteed if the ``TAG_ENCODE_KEY_SERVERS`` knob has never been changed to ``true`` on this cluster.
