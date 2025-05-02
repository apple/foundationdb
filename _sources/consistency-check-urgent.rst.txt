##############################
Consistency Checker Urgent
##############################

| Author: Zhe Wang
| Reviewer: Jingyu Zhou
| Audience: FDB developers, SREs and expert users.


Overview
========
In a FoundationDB (FDB) key-value cluster, every key-value pair is replicated across multiple storage servers. 
The Consistency Checker Urgent tool can be used to validate the consistency of all replicas for each key-value pair. 
If any data inconsistency is detected, the tool generates ConsistencyCheck_DataInconsistent trace events for the corresponding shard. 
There are two types of data inconsistencies: 

1. Value mismatch, where the value of a key on one server differs from that on another server, 
2. Unique key, where a key exists on one server but not on another. 

The ConsistencyCheck_DataInconsistent trace event differentiates between these two types of corruption.

Key features
------------
The Consistency Checker Urgent tool is designed to ensure safe, fast, and comprehensive checking of data consistency across the entire key space 
(i.e., " " ~ "\\xff\\xff"). It achieves this through the following features:

* End-to-end completeness check --- The checker continues until all ranges are marked as complete.
* Scalability --- Adding more testers results in nearly linear speedup with the number of testers.
* Progress monitoring --- A single trace event (i.e. ConsistencyCheckUrgent_GotShardsToCheck) indicates the remaining number of shards to check.
* Independence from the existing cluster --- The tool does not store any data in the cluster being checked.
* Fault tolerance --- Tester failures do not impact the checker process. Shard checking failures are automatically retried.
* Workload throttling --- Each tester can handle one task at a time, with a maximum read rate of 50MB/s (specified by knob CONSISTENCY_CHECK_RATE_LIMIT_MAX, though the actual value should be much smaller).
* Custom input ranges --- Users can specify at most 4 custom ranges in knobs. By default, the knob is set to check the entire key space (i.e., " " ~ "\\xff\\xff").

How to use?
-----------
To run the ConsistencyCheckerUrgent, you need 1 checker and N testers. The process is as follows:

* If you want to specify ranges to do consistency checker, set ranges via knobs: CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_* and CONSISTENCY_CHECK_URGENT_RANGE_END_*. The custom range's start and end points must be represented in hexadecimal ASCII format, strictly adhering to the "\\\\x" prefix (including two escape chars, followed by 'x'). By default, the knob is set to check the entire key space (i.e., " " ~ "\\xff\\xff").
* Start N testers (i.e. fdbserver --class test --consistency-check-urgent-mode). When setting consistency-check-urgent-mode, the tester server will check whether the received workload is a "ConsistencyCheckUrgent" workload. If yes, accept the workload. Otherwise, drop the workload with a SevError trace event: StartingTesterServerCoreUnexpectedWorkload.
* Start the checker (i.e. fdbserver -r consistencycheckurgent --num-testers={num-testers}), which initiates the consistency checking automatically.
* Once the checking is complete, the checker exits automatically, leaving the testers alive but idle.
* If you need to rerun the checking, simply restart the checker process.

Users should manually remove testers when they are no longer needed. 
This approach allows for re-running the one-shot checking by restarting the checker process.

Compared to Consistency Checker
-------------------------------

ConsistencyCheckerUrgent offers significant improvements over the existing consistency checker in several key areas:

* The existing checker cannot guarantee a complete check, while ConsistencyCheckerUrgent ensures a thorough and complete check.
* The existing checker operates on a single thread, resulting in slow performance. In contrast, ConsistencyCheckerUrgent utilizes a distributed processing model, enabling faster processing. Additionally, adding more resources allows for linear speed improvements.
* The existing checker lacks reliability, as it may silently error out and lose progress. ConsistencyCheckerUrgent, on the other hand, is designed with retriable components and mechanisms, making it much more reliable.
* Tracking progress with the existing checker is challenging. In contrast, ConsistencyCheckerUrgent provides clear progress information.
* The existing checker is limited to checking the entire key space, while ConsistencyCheckerUrgent allows users to specify any key range for checking.

Distributed consistency-checking model
======================================
The ConsistencyCheckerUrgent system conducts consistency checks in a distributed, client-server manner. It comprises a centralized leader (referred to as the Checker) and N agents (referred to as Testers). The Checker manages the checking process by:

* Collecting ranges for checking.
* Assigning ranges to agents.
* Collecting results from agents.
* Managing the progress of checking.

The agents perform consistency checking tasks, comparing every key in the assigned range across all source servers at a specific version. As agents complete tasks or encounter failures, the leader is informed and updates the progress of the checking process accordingly.

Workflow
--------

The checker operates in the following steps:

1. Initially, the checker loads ranges to check from knobs (which defaults to " " ~ "\xff\xff"). See: CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_* and CONSISTENCY_CHECK_URGENT_RANGE_END_*.
2. The checker contacts the cluster controller (CC) to obtain the tester interfaces.
3. The checker continues to contact the CC until it receives a sufficient number of testers (as specified by user input when starting the consistencycheckurgent process: --num-testers).
4. After collecting enough testers, the checker partitions the range to be checked according to the shard boundary (shard information is retrieved from the FDB system metadata).
5. The checker distributes shards among the selected testers. Each tester is allocated a fixed number of shards at a time (as specified by the knob CONSISTENCY_CHECK_URGENT_BATCH_SHARD_COUNT). Currently, the knob is set to 10 by default and testers are expected to complete these shards by approximately 1 hour.
6. The checker sends assignments to the testers, and the testers begin processing their assigned ranges. This is done in batches; the checker sends assignments to all testers simultaneously and waits for replies from all testers. The checker proceeds only after receiving replies from all testers or if a tester fails or crashes.
7. When a tester completes its assigned shards, it reports whether it has completed all assigned shards. If so, the checker marks the assigned shards as complete. Otherwise, the checker marks all the assigned shards to that tester as failed and retries them later.
8. The checker collects all unfinished shards from memory and returns to step 2.
9. When the entire key space is marked as complete, the checker terminates with a single trace event ConsistencyCheckUrgent_Complete.

The tester operates in the following steps:

1. The tester receives a set of ranges to check from the checker at a time (specified by CONSISTENCY_CHECK_URGENT_BATCH_SHARD_COUNT).
2. For each shard, the tester obtains the storage server (SS) interfaces of all data centers.
3. The tester issues a read range request to each SS interface, ensuring they are at the same version.
4. Key by key, the tester compares the values and records any inconsistencies, populating ConsistencyCheck_DataInconsistent in the presence of shard inconsistency.
5. If any keys fail to compare, the tester collects them and retries the comparison process for those keys, returning to step 2.
6. Once all shards have been compared or the tester has retried for a specified number of times, the tester notifies the checker.