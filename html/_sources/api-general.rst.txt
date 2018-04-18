##########################
Using FoundationDB Clients
##########################

.. include:: guide-common.rst.inc

.. _versioning:

Versioning
==========

FoundationDB supports a robust versioning system for both its API and binaries. This system allows clusters to be upgraded with minimal changes to both application code and FoundationDB binaries. The API and the FoundationDB binaries are each released in numbered versions. Each version of the binaries has a corresponding API version.

API versions
------------

In general, a given client will support both its corresponding API version *and earlier API versions*. A client selects the API version it will use by explicitly specifying the version number upon initialization. The purpose of this design is to allow the server, client libraries, or bindings to be upgraded without having to modify application code. You can therefore upgrade to more recent packages (and so receive their various improvements) without having to change application code. 

Binary versions
---------------

By default, client and server binaries installed for a given cluster must have the same version. However, there is also a multi-version feature that allows a client to connect to a cluster whose server processes have a different version. This feature works by loading a separate client library that is version-compatible with the cluster, which then proxies all API calls. 

.. note:: The interaction between API versions and binary versions follows a simple rule: *the API version cannot be set higher than that supported by the client library in use*, including any client library loaded using the multi-version feature.

For more details, see :ref:`api-python-versioning` and :ref:`multi-version-client-api`.

Cluster file
============

FoundationDB servers and clients use a cluster file (typically named ``fdb.cluster``) to connect to a cluster. The contents of the cluster file are the same for all processes that connect to the cluster. When connecting to a cluster, the :doc:`client APIs <api-reference>` allow a cluster file to either be explicitly provided or automatically determined by default. For example, using the Python API:: 

  db = fdb.open('/path/to/specific/file') 

uses only the specified file and errors if it is invalid. In contrast::

  db = fdb.open()

checks the ``FDB_CLUSTER_FILE`` environment variable, then the current working directory, then the :ref:`default file <default-cluster-file>`. FoundationDB's procedure for determining a cluster file is described in :ref:`Specifying a cluster file <specifying-a-cluster-file>`.

.. _installing-client-binaries:

Installing FoundationDB client binaries
=======================================

To access a FoundationDB cluster from a machine that won't need to run the server, you can install just the FoundationDB client binaries (available for download at :doc:`downloads`).

.. warning:: |upgrade-client-server-warning|

If you don't already have a FoundationDB cluster to connect to, you should instead follow the instructions in :doc:`getting-started-mac` or :doc:`getting-started-linux`.

To install on **Ubuntu** use the dpkg command:

.. parsed-literal::

    user@host$ sudo dpkg -i |package-deb-clients|

To install on **RHEL/CentOS** use the rpm command:

.. parsed-literal::

    user@host$ sudo rpm -Uvh |package-rpm-clients|

To install on **macOS**, run the installer as in :doc:`getting-started-mac`, but deselect the "FoundationDB Server" feature.

The client binaries include the ``fdbcli`` tool and language bindings for C and Python.  Other language bindings must be installed separately.

Clients will also need a :ref:`cluster file <foundationdb-cluster-file>` to connect to a FoundationDB cluster.  You should copy the ``fdb.cluster`` file from the :ref:`default location <default-cluster-file>` on one of your FoundationDB servers to the default location on the client machine.

.. _multi-version-client-api:

Multi-version client
====================

The FoundationDB client library supports connecting to clusters with server processes running at a different version than the client. To do so, it must have access to a version of the client compatible with the cluster, which it loads and then proxies API calls through.

To make use of the multi-version client capabilities, you must set at least one of two network options (``EXTERNAL_CLIENT_LIBRARY`` and/or ``EXTERNAL_CLIENT_DIRECTORY``) that specify the location of these additional client libraries. The client library will start a new network thread for each external client and attempt to make connections to the cluster over all of them (as well as the local library). When making API calls, the library will use the version that is able to communicate with the cluster (choosing an arbitrary one if there are multiple compatible clients), and it will also automatically switch to the appropriate client should the cluster's protocol version change.

The multi-version client API adds a new ``cluster_version_changed`` error that is used to cancel operations performed by the client when it switches to using a different version of the client library. This error is a retryable one, meaning that all standard retry loops will automatically rerun a transaction that fails with ``cluster_version_changed``. 

.. warning:: Setting an API version that is not supported by a particular client library will prevent that client from being used to connect to the cluster. In particular, you should not advance the API version of your application after upgrading your client until the cluster has also been upgraded.

.. warning:: You should avoid including multiple protocol-compatible clients in the external client libraries list. While the client will still work, it will consume more resources than necessary. Additionally, different patch releases of the same version (e.g. ``x.y.z`` and ``x.y.w``) are protocol compatible, and including multiple may result in not using the most recent compatible client.    

.. note:: It is recommended that you not include more external clients than necessary. For example, a client that has been upgraded to a newer version than its cluster may need to include a single external client that matches the version of the cluster, but it generally won't require a copy of every prior version.

.. note:: If ``cluster_version_changed`` is thrown during commit, it should be interpreted similarly to ``commit_unknown_result``. The commit may or may not have been completed.

.. _network-options-using-environment-variables:

Setting network options with environment variables
==================================================

Client network options can be set automatically upon client startup using specially crafted environment variables. To set a particular network option, add a variable of the following form to your environment::

	FDB_NETWORK_OPTION_<UPPERCASE_OPTION_NAME> = value

For example, you can enable trace logging for a client by setting the following environment variable::

	FDB_NETWORK_OPTION_TRACE_ENABLE = ""

If you want to set the same option multiple times (e.g. to add multiple client libraries, for instance), you can separate the values with the default path separator on your system (``:`` on Linux/macOS, ``;`` on Windows). For example::

	FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY = /path/to/dir1:/path/to/dir2:...

Network options specified using environment variables are set at the end of the call to set the API version. They will be applied in the order of the corresponding option codes assigned to the options. If there is an error reading the appropriate environment variables or if the option cannot be set with the specified value, then the call to set the API version may return an error. In that case, you may attempt to set the API version again, but you must do so with the same version as the attempt that failed.
