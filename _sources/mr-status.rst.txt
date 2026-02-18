.. default-domain:: py
.. highlight:: python

.. _mr-status:

#######################
Machine-Readable Status
#######################

.. include:: guide-common.rst.inc

FoundationDB provides status information in machine-readable JSON form (in addition to the human-readable form made available by :ref:`the command line interface <cli-status>`). This document explains how to access the machine-readable status, provides guidance for its use, and describes the JSON format used to encode it.

.. _mr-status-key:

Accessing machine-readable status
=================================

You can access machine-readable status in three ways ways:

* Within ``fdbcli``, issue the command ``status json``. This command will output status information in JSON (rather than the human-readable format output by ``status`` and ``status details``). See the :ref:`cli-status` command for more information.
* From a command shell, use fdbcli by running ``fdbcli --exec "status json"``
* From any client, read the key ``\xFF\xFF/status/json``. The value of this key is a JSON object serialized to a byte string with UTF-8 encoding. In Python, given an open database ``db``, the JSON object can be read and deserialized with::

    import json
    status = json.loads(db['\xff\xff/status/json'])

Guidance regarding versioning
=============================

The JSON format of the machine-readable status is not considered part of our API and, in particular, is not governed by the :ref:`versioning mechanism <api-python-versioning>` used to facilitate API upgrades. A client that makes use of the machine-readable status should be prepared to handle possible format changes across versions.

Format changes will be governed as follows:

* We will not make arbitrary changes to the JSON format; we will make such changes only as required by changes in the underlying system characteristics relevant to status reporting.
* We may add fields as needed to report new categories of data.
* We may remove a field if a new version of FoundationDB renders the field obsolete.
* We will *not* change the semantics of an existing field. If the data relating to a field changes in a manner that is incompatible with previous usage, the field will be deleted and replaced by a newly named field.

JSON format
===========

The following format informally describes the JSON containing the status data. The possible values of ``<name_string>`` and ``<description_string>`` are described in :ref:`mr-status-message`. The format is representative: *any field can be missing at any time*, depending on the database state. Clients should be prepared to flexibly handle format variations resulting from different database states.

.. include:: mr-status-json-schemas.rst.inc

.. _mr-status-message:

Message components
------------------

Several fields in the JSON object may contain messages in the format:

.. code-block:: javascript

    "messages": [
      {
        "name": <name_string>,
        "description": <description_string>
      }
    ]

Each message is an Object having at least a ``"name"`` field.  The ``"description"`` is present only in some messages.  Other fields may be present based on specific message instance details.  The possible name and description values of a message found at a given location in the JSON object are described in the tables below.

====================================  ====================================  =================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
JSON Path                             Name                                  Description
====================================  ====================================  =================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
client.messages                       inconsistent_cluster_file             Cluster file is not up to date. It contains the connection string ‘<value>’. The current connection string is ‘<value>’. This must mean that file permissions or other platform issues have prevented the file from being updated. To change coordinators without manual intervention, the cluster file and its containing folder must be writable by all servers and clients. If a majority of the coordinators referenced by the old connection string are lost, the database will stop working until the correct cluster file is distributed to all processes.
client.messages                       no_cluster_controller                 Unable to locate a cluster controller within 2 seconds. Check that there are server processes running.
client.messages                       quorum_not_reachable                  Unable to reach a quorum of coordinators.
client.messages                       server_overloaded                     Server is under too much load and cannot respond.
client.messages                       status_incomplete_client              Could not retrieve client status information.
client.messages                       status_incomplete_cluster             Could not retrieve cluster status information.
client.messages                       status_incomplete_coordinators        Could not fetch coordinator info.
client.messages                       status_incomplete_error               Cluster encountered an error fetching status.
client.messages                       status_incomplete_timeout             Timed out fetching cluster status.
client.messages                       unreachable_cluster_controller        No response received from the cluster controller.
cluster.messages                      client_issues                         Some clients of this cluster have issues.
cluster.messages                      commit_timeout                        Unable to commit after __ seconds.
cluster.messages                      read_timeout                          Unable to read after __ seconds.
cluster.messages                      status_incomplete                     Unable to retrieve all status information.
cluster.messages                      storage_servers_error                 Timed out trying to retrieve storage servers.
cluster.messages                      log_servers_error                     Timed out trying to retrieve log servers.
cluster.messages                      transaction_start_timeout             Unable to start transaction after __ seconds.
cluster.messages                      unreachable_master_worker             Unable to locate the master worker.
cluster.messages                      unreachable_dataDistributor_worker    Unable to locate the data distributor worker.
cluster.messages                      unreachable_ratekeeper_worker         Unable to locate the ratekeeper worker.
cluster.messages                      unreachable_processes                 The cluster has some unreachable processes.
cluster.messages                      unreadable_configuration              Unable to read database configuration.
cluster.messages                      layer_status_incomplete               Some or all of the layers subdocument could not be read.
cluster.messages                      primary_dc_missing                    Unable to determine primary datacenter.
cluster.messages                      fetch_primary_dc_timeout              Fetching primary DC timed out.
cluster.processes.<process>.messages  file_open_error                       Unable to open ‘<file>’ (<os_error>).
cluster.processes.<process>.messages  incorrect_cluster_file_contents       Cluster file contents do not match current cluster connection string. Verify cluster file is writable and has not been overwritten externally.
cluster.processes.<process>.messages  io_error                              <error> occured in <subsystem>
cluster.processes.<process>.messages  platform_error                        <error> occured in <subsystem>
cluster.processes.<process>.messages  process_error                         <error> occured in <subsystem>
====================================  ====================================  =================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

The JSON path ``cluster.recovery_state``, when it exists, is an Object containing at least ``"name"`` and ``"description"``.  The possible values for those fields are in the following table:

================================  =========================================================================================================================================================================================
Name                              Description
================================  =========================================================================================================================================================================================
reading_coordinated_state         Requesting information from coordination servers. Verify that a majority of coordination server processes are active.
locking_coordinated_state         Locking coordination state. Verify that a majority of coordination server processes are active.
reading_transaction_system_state  Recovering transaction server state. Verify that the transaction server processes are active.
configuration_missing             There appears to be a database, but its configuration does not appear to be initialized.
configuration_never_created       The coordinator(s) have no record of this database. Either the coordinator addresses are incorrect, the coordination state on those machines is missing, or no database has been created.
configuration_invalid             The database configuration is invalid. Set a new, valid configuration to recover the database.
recruiting_transaction_servers    Recruiting new transaction servers.
initializing_transaction_servers  Initializing new transaction servers and recovering transaction logs.
recovery_transaction Performing   recovery transaction.
writing_coordinated_state         Writing coordinated state. Verify that a majority of coordination server processes are active.
fully_recovered                   Recovery complete.
================================  =========================================================================================================================================================================================

The JSON path ``cluster.qos.performance_limited_by``, when it exists, is an Object containing at least ``"name"`` and ``"description"``.  The possible values for those fields are in the following table:

=================================== ====================================================
Name                                Description
=================================== ====================================================
workload                            The database is not being saturated by the workload.
storage_server_write_queue_size     Storage server performance (storage queue).
storage_server_write_bandwidth_mvcc Storage server MVCC memory.
storage_server_readable_behind      Storage server version falling behind.
log_server_mvcc_write_bandwidth     Log server MVCC memory.
log_server_write_queue              Storage server performance (log queue).
min_free_space                      Running out of space (approaching 100MB limit).
min_free_space_ratio                Running out of space (approaching 5% limit).
log_server_min_free_space           Log server running out of space (approaching 100MB limit).
log_server_min_free_space_ratio     Log server running out of space (approaching 5% limit).
storage_server_durability_lag       Storage server durable version falling behind.
storage_server_list_fetch_failed    Unable to fetch storage server list.
blob_worker_lag                     Blob worker granule version falling behind.
blob_worker_missing                 No blob workers are reporting metrics.
=================================== ====================================================

The JSON path ``cluster.qos.throttled_tags``, when it exists, is an Object containing ``"auto"`` , ``"manual"`` and ``"recommended"``.  The possible fields for those object are in the following table:

=================================== ====================================================
Name                                Description
=================================== ====================================================
count                               How many tags are throttled
busy_read                           How many tags are throttled because of busy read
busy_write                          How many tags are throttled because of busy write
=================================== ====================================================