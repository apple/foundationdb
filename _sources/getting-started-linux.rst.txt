.. _getting-started-linux:

########################
Getting Started on Linux
########################

.. include:: guide-common.rst.inc

This guide walks through installing a locally accessible FoundationDB server that is suitable for development on Linux.

To install an externally accessible FoundationDB cluster on one or more machines, see :doc:`building-cluster`.

First steps
===========

* Validate your system meets the :ref:`system-requirements`.

* Download the FoundationDB packages for your system from :doc:`downloads`.

* Before upgrading from a previous version of FoundationDB, see :ref:`upgrading-foundationdb`.

Installing or upgrading FoundationDB packages
=============================================

.. warning:: |upgrade-client-server-warning|

To install on **Ubuntu** use the dpkg command:

.. parsed-literal::

    user@host$ sudo dpkg -i |package-deb-clients| \\
    |package-deb-server|

To install on **RHEL/CentOS 6** use the rpm command:

.. parsed-literal::

    user@host$ sudo rpm -Uvh |package-rpm-clients| \\
    |package-rpm-server|

To install on **RHEL/CentOS 7** use the rpm command:

.. parsed-literal::

    user@host$ sudo rpm -Uvh |package-rpm-clients| \\
    |package-rpm-server|

|simple-installation-mode-warnings|

|networking-clarification|
	
Testing your FoundationDB installation
======================================

To verify that the local FoundationDB database is operational, open the command line interface (``fdbcli``) and use the status command. ::

  user@host$ fdbcli
  Using cluster file `/etc/foundationdb/fdb.cluster'.

  The database is available.

  Welcome to the fdbcli. For help, type `help'.
  fdb> status

  Configuration:
    Redundancy mode        - single
    Storage engine         - memory
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

If these steps were successful you have installed and validated FoundationDB. You can now start using the database!

.. note:: If the database is not operational the ``status`` command will provide diagnostic information to help you resolve the issue.

Managing the FoundationDB service
==================================

* See :ref:`administration-running-foundationdb`.
* See :ref:`administration-removing`.

Next steps
==========

* Install the APIs for :doc:`Ruby <api-ruby>`, :doc:`Python <api-python>`, `Java <javadoc/index.html>`_ or `Go <https://godoc.org/github.com/apple/foundationdb/bindings/go/src/fdb>`_ if you intend to use those languages. The :doc:`C <api-c>` API was installed along with the ``foundationdb-clients`` package above.
* See :doc:`tutorials` for samples of developing applications with FoundationDB.
* See :doc:`developer-guide` for information of interest to developers, including common design patterns and performance considerations.
* See :doc:`administration` for detailed administration information.
* See :doc:`known-limitations` of the system.
* See :doc:`building-cluster` for step-by-step instructions on converting your local single-machine cluster to an externally visible cluster of one or more machines.

