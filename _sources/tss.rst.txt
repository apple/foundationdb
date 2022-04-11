.. _testing-storage-server:

############################
Testing Storage Server (TSS)
############################

.. include:: guide-common.rst.inc

This document covers the operation and architecture of the Testing Storage Server feature, or TSS for short.

.. _tss-introduction:

Summary
============

The TSS feature allows FoundationDB to run an "untrusted" storage engine (the *testing storage engine*) directly in a QA or production environment with identical workload to the current storage engine, with zero impact on durability or correctness, and minimal impact on performance.

This allows a FoundationDB cluster operator to validate the correctness and performance of a different storage engine on the exact cluster workload before migrating data to the different storage engine.

A Testing Storage Server is paired to a normal Storage Server. Both servers in a new pair start empty and take on exactly the same data and serve exactly the same read requests. The SS and TSS responses are compared client-side to ensure that they match, and performance metrics are recorded for the pair.

Configuring TSS
===============

You can configure TSS via the FDB :ref:`command line interface <command-line-interface>`.

Because of the performance overhead of duplicating and comparing read requests, it is recommended to configure the number of TSS processes to a small percentage of the number of storage processes in the cluster (at most 5% - 10%). It is also recommended to add enough servers to the cluster before enabling TSS to end up with the same number of normal Storage processes as the cluster had before enabling TSS.

Because TSS recruitment only pairs *new* storage processes, you must add processes to the cluster and/or enable the *perpetual wiggle* to actually start recruiting TSS processes.

Example commands
----------------

Set the desired TSS processes count to 4, using the redwood storage engine: ``configure tss ssd-redwood-1-experimental count=4``.

Change the desired TSS process count to 2: ``configure tss count=2``.

Disable TSS on the cluster: ``configure tss count=0``.

Monitoring TSS
==============

The ``status`` command in the FDB :ref:`command line interface <command-line-interface>` will show the current and desired number of TSS processes in the cluster, and the full status JSON will include the full TSS configuration parameters.

Trace Events
----------------------
Whenever a client detects a *TSS Mismatch*, or when the SS and TSS response differ, and the difference can only be explained by different storage engine contents, it will emit an error-level trace event with a type starting with ``TSSMismatch``, with a different type for each read request. This trace event will include all of the information necessary to investigate the mismatch, such as the TSS storage ID, the full request data, and the summarized replies (full keys and checksummed values) from both the SS and TSS.

Each client emits a ``TSSClientMetrics`` trace event for each TSS pair in the cluster that it has sent requests to recently, similar to the ``TransactionMetrics`` trace event.
It contains the TSS storage ID, and latency statistics for each type of read request. It also includes a count of any mismatches, and a histogram of error codes received by the SS and TSS to ensure the storage engines have similar error rates and types.

The ``StorageMetrics`` trace event emitted by storage servers includes the storage ID of its pair if part of a TSS pairing, and includes a ``TSSJointID`` detail with a unique id for the SS/TSS pair that enables correlating the separate StorageMetrics events from the SS and TSS.


Quarantined TSS
---------------
If a *TSS Mismatch* is detected for a given TSS, instead of killing the TSS, it will be put into a *quarantined* state. In this state, the TSS doesn't respond to any data requests, but is still recruited on the worker, preventing a new storage process from replacing it.
This is so that the cluster operator can investigate the storage engine file of the TSS's *testing storage engine* to determine the cause of the data inconsistency.

You can also manually quarantine a TSS, or dispose of a quarantined TSS once you're done investigating using the ``tssq`` command in the FDB :ref:`command line interface <command-line-interface>`.

The typical flow of operations would be

* ``tssq start <StorageUID>``: manually quarantines a TSS process, if it is not already quarantined.
* ``tssq list``: lists all TSS processes currently in quarantine to see if any were automatically quarantined.
* Investigate the quarantined TSS to determine the cause of the mismatch.
* ``tssq stop <StorageUID>``: remove a TSS process from quarantine, disposing of the TSS and allowing Data Distribution to recruit a new storage process on the worker.

The Storage Consistency Check will also check TSS processes against the rest of that shard's team, and fail if there is a mismatch, but it will not automatically quarantine the offending TSS.

Other Failure Types
-------------------

The TSS feature is designed to insulate the cluster from an incorrect storage engine, but it is also designed to insulate the cluster as much as possible from a poor-performing storage engine.

Read Requests to TSS processes are never on the critical path for a client transaction, so a slow-responding TSS will not affect client latencies.

If the TSS falls behind its pair for ingesting new mutations or processing data movements, it will be killed by the cluster.

Architecture Overview
=====================

TSS Mapping
-----------

The *TSS Mapping*, the mapping containing the (SS Storage ID, TSS Storage ID) pairs for all active TSS pairs in the cluster, is stored in the system keyspace under ``\xff/tss/``.

This information is lazily propagated to clients in the ``KeyServerLocationsRequest`` from the Commit Proxies, which read the TSS mapping from the Transaction State Store.

Clients will use this mapping to duplicate any request to a storage server that has a TSS pair.

Other cluster components that need the full, up-to-date TSS mapping (MoveKeys, ConsistencyCheck, etc...) read it directly from the system keyspace as part of a transaction.

Recruitment
-----------

TSS Pair recruitment is designed to use the *Perpetual Storage Wiggle*, but will also work if multiple storage processes are added to the cluster.

When Data Distribution detects a deficit of TSS processes in the cluster, it will begin recruiting a TSS pair.
The pair recruitment logic is as follows:

* Once DD gets a candidate worker from the Cluster Controller, hold that worker as a desired TSS process.
* Once DD gets a second candidate worker from the Cluster Controller, initialize that worker as a normal SS.
* Once the second candidate worker is successfully initialized, initialize the first candidate worker as a TSS, passing it the storage ID, starting tag + version, and other information from its SS pair. Because the TSS reads from the same tag starting at the same version, it is guaranteed to receive the same mutations and data movements as its pair.

One implication of this is, during TSS recruitment, the cluster is effectively down one storage process until a second storage process becomes available.
While clusters should be able to handle being down a single storage process anyway to tolerate machine failure, an active TSS recruitment will be cancelled if the lack of that single storage process is causing the cluster to be unhealthy. Similarly, if the cluster is unhealthy and unable to find new teams to replicate data to, any existing TSS processes may be killed to make room for new storage servers.

TSS Process
-----------

A TSS process is almost identical to a normal Storage process, with the exception that it does not pop its tag from the TLog mutation stream, and it listens to and processes private mutations for both itself and its SS pair.


Caveats
=======

Despite its usefulness, the TSS feature does not give a perfect performance comparison of the storage engine(s) under test.

Because it is only enabled on a small percentage of the cluster and only compares single storage processes for the same workload, it may miss potential aggregate performance problems, such as the testing storage engine overall consuming more cpu/memory, especially on a host with many storage instances.

TSS testing using the recommended small number of TSS pairs may also miss performance pathologies from workloads not experienced by the specific storage teams with TSS pairs in their membership.

TSS testing is not a substitute for full-cluster performance and correctness testing or simulation testing.
