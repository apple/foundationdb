.. include:: guide-common.rst.inc

.. _building-cluster:

##################
Building a Cluster
##################

This guide walks through the steps to build an externally accessible FoundationDB cluster of one or more machines. Before setting up a cluster for performance testing or production use, you should also read the reference material in :doc:`configuration` and :doc:`administration`.

.. warning:: |development-use-only-warning|

To build an externally accessible FoundationDB cluster, perform the following steps:

.. contents::
   :depth: 1
   :local:
   :backlinks: none

Install FoundationDB
====================

Follow the steps in :doc:`getting-started-linux` to install FoundationDB locally on each of the Linux machines that you wish to use in your cluster.

.. warning:: When building a cluster, do **not** simply copy the FoundationDB installation, and in particular the **data** files, from one machine to another, whether by direct copying or by cloning a VM.

Optimize for your hardware
==========================

|optimize-configuration|

We recommend changing the configuration file once and copying to other machines in the cluster.

Make FoundationDB externally accessible
=======================================

By default, FoundationDB installs on a single server in a locally accessible mode suitable for development --- only clients on the same machine will be able to access the database. To allow external access, you will have to make your :ref:`cluster file <foundationdb-cluster-file>` public.

Choose a machine to be the starting machine for your cluster. The database on this machine will be the one that we grow to span the cluster. Use the ``/usr/lib/foundationdb/make_public.py`` script on that server to update your cluster file to use a public interface. For example:: 

    user@host1$ sudo /usr/lib/foundationdb/make_public.py
    /etc/foundationdb/fdb.cluster is now using address 10.0.1.1

.. note:: A FoundationDB cluster has the option of supporting :doc:`Transport Layer Security (TLS) <tls>` for all connections (between server processes and between clients and servers). To enable TLS on a new cluster, see :ref:`Enabling TLS <enable-TLS>`.

By default, the script will pick a local network interface that can access the internet. To specify the address manually, use the ``-a`` flag and choose an address that is accessible by all machines in the cluster as well as by all intended clients.::

    user@host1$ sudo /usr/lib/foundationdb/make_public.py -a 10.0.1.1
    /etc/foundationdb/fdb.cluster is now using address 10.0.1.1

.. _test-the-database:

Test the database
=================

At this point and after each subsequent step, it is a good idea to test the database to make sure it is operating normally. Run ``fdbcli`` on the starting machine::

  user@host1$ fdbcli
  Using cluster file `/etc/foundationdb/fdb.cluster'.

  The database is available.

  Welcome to the fdbcli. For help, type `help'.
  fdb> status
  
  Configuration:
    Redundancy mode        - single
    Storage engine         - ssd
    Coordinators           - 1

  Cluster:
    FoundationDB processes - 1
    Machines               - 1
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

  Client time: Thu Mar 15 14:41:34 2018

.. note:: If the database is not operational the status command will provide diagnostic information to help you resolve the issue. For more help, please post a question (and the results of the status command) on the `community forums <https://forums.foundationdb.org>`_.

Add machines to the cluster
===========================

To add the rest of your machines to the cluster, perform the following steps on each one:

* Copy the cluster file from a server already in the cluster (located at ``/etc/foundationdb/fdb.cluster``) to the new machine, overwriting the existing ``fdb.cluster`` file.
* Restart FoundationDB on the new machine so that it uses the new cluster file::

    user@host2$ sudo service foundationdb restart

.. _change-redundancy-mode-and-storage-engine:

Change redundancy mode and storage engine
=========================================

By default, the database will be in ``single`` redundancy mode and use the ``memory`` storage engine. You should change the redundancy mode (see :ref:`configuration-choosing-redundancy-mode`) and storage engine (see :ref:`configuration-storage-engine`) to appropriate values for your cluster.

For example, to use a triple-replicated database with the SSD storage engine, use the ``configure`` command in the ``fdbcli``::

    user@host1$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> configure triple ssd
    Configuration changed

If the configure command hangs or returns an error message, see :ref:`test-the-database`.

Change coordination servers
===========================

At this point, your cluster will be using the starting machine as the only coordination server, leaving that as a single point of failure. You should therefore select a fault-tolerant set of coordinators according to the criteria in :ref:`configuration-choosing-coordination-servers`. To switch the cluster to your chosen coordinators, run the ``fdbcli`` command on one of the servers and use the ``coordinators`` command to :ref:`set the coordinators <configuration-changing-coordination-servers>`. ::

    user@host$ fdbcli
    Using cluster file `/etc/foundationdb/fdb.cluster'.

    The database is available.

    Welcome to the fdbcli. For help, type `help'.
    fdb> coordinators 10.0.4.1:4500 10.0.4.2:4500 10.0.4.3:4500
    Coordinators changed

There is also a convenience option, ``coordinators auto``, that will automatically select a set of coordinators based on your redundancy mode.

.. note:: |coordinators-auto|

You can also change the cluster ``description``, as described in :ref:`configuration-setting-cluster-description`.

Next steps
==========

To add or remove machines from the cluster or perform other administrative tasks, see :doc:`administration`.
