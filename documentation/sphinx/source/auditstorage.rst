##############
Audit Storage
##############

| Author: Zhe Wang
| Reviewer: Jingyu Zhou
| Audience: FDB developers, SREs and expert users.


Overview
========
In a FoundationDB (FDB) key-value cluster, every key-value pair is replicated across multiple storage servers.
The location of key-value pairs is decided by the data distribution system (DD). 
When DD relocates a range, DD updates the location metadata accordingly.
The location metadata stores the ground truth of the range location. Therefore, the location metadata consistency is crucial for the correctness of the system.
The AuditStorage tool can be used to validate the consistency of all replicas for each key-value pair. Moreover, it can also be used to validate the location metadata consistency. 
If any data inconsistency is detected, the tool generates Error trace events. 

Key features
------------
Detecting replica consistency and location metadata consistency are two crucial tasks for the AuditStorage tool.
For the replica consistency check, the auditstorage has a different implementation compared to the Consistency Checker Urgent tool, which results in a faster
checking process. We will introduce the design in the following sections.
AuditStorage tool also checks the consistency of the location metadata. 
In particular, the tool checks if ``KeyServer`` and ``ServerKey`` are consistent: a range is assigned to a set of servers in the ``KeyServer`` if and only if the servers have the range in the ``ServerKey``.
The tool also checks if the ``ServerKeys`` are consistent to StorageServer (SS) location shard mapping.
Note that ``ServerKeys`` is a part of the system key space while the SS location shard mapping is stored in the storage server locally.
The tool checks a range is assigned to the SS in ``ServerKeys`` if and only if the range is owned by the SS in the local shard mapping.

The AuditStorage tool has following features:

* End-to-end completeness check --- The checker continues until all ranges are marked as complete. AuditStorage persists progress to the system metadata. "``\xff/auditRanges/ ~ \xff/auditRanges0``" is for replica and locationmetadata checking, and "``\xff/auditServers/ ~ \xff/auditServers0``" is for ssshard checking. This is a major difference from the Consistency Checker Urgent tool.
* Scalability --- Adding more parallelism results in nearly linear speedup. This is controlled by the knob ``CONCURRENT_AUDIT_TASK_COUNT_MAX``.
* Progress monitoring --- Providing FDBCLI command to check the progress of the checking.
* Fault tolerance --- Failures do not impact the checker process. Checking failures are automatically retried.
* Simple to setup --- Different from the Consistency Checker Urgent tool, the AuditStorage tool uses DD and SSes to conduct checking. Therefore, a cluster can run the AuditStorage tool without any additional setup.

How to use?
-----------
Start new audit job

``audit_storage [replica\|locationmetadata\|ssshard] [BeginKey] [EndKey]``

For example, to check all replicas, we can run ``audit_storage replica "" \xff\xff``.

List recent jobs

``get_audit_status [replica\|locationmetadata\|ssshard] recent``

Check a job progress

``get_audit_status [replica\|locationmetadata\|ssshard] progress [AuditID]``

Cancel an audit

``audit_storage cancel [replica\|locationmetadata\|ssshard] [AuditID]``

There three auditTypes:

1. ``replica``: This audit checks the consistency of user data between replicas of all DCs. ``SSAuditStorageShardReplicaError`` trace event is generated if any inconsistency is detected.
2. ``locationmetadata``: This audit checks the consistency between ``KeyServer`` and ``ServerKey`` space. ``DDDoAuditLocationMetadataError`` trace event is generated if any inconsistency is detected.
3. ``ssshard``: This audit checks the consistency between ``KeyServer`` and storage server in-memory shard information. ``SSAuditStorageSsShardError`` trace event is generated if any inconsistency is detected.

``BeginKey`` and ``EndKey`` decide the scope of the consistency check of user data (replica). Note that the ``locationmetadata`` and ``ssshard`` always check the consistency on all key space no matter the user input. 

Compared to Consistency Checker Urgent
--------------------------------------

AuditStorage offers significant improvements over the existing consistency checker urgent in several key areas:

* ConsistencyCheckerUrgent is not efficient because it does not persist progress to the database. On the other hand, AuditStorage persists progress to the metadata stored in the database. As a result, the AuditStorage tool improves the efficiency by avoiding doing repeated work.
* ConsistencyCheckerUrgent does not support location metadata consistency check. AuditStorage supports location metadata consistency check.
* ConsistencyCheckerUrgent does not provide fdbcli tools for job submit and progress check. AuditStorage provides fdbcli tools for job submit and progress check.

AuditStorage design
======================================
The AuditStorage system conducts consistency checks in a distributed, client-server manner. 
In the system, DD serves as a centralized leader to monitor the progress and dispatch tasks. 
SSes are agents to performa actual checking. The checking progress is shared between DD and SSes on system metadata.
When a SS completes a range, the SS marks the range as completed in the metadata using a transaction.
Then, the DD will know this range has been completed and does not schedule task to check this range again.

Replica check (replica)
-----------------------
The AuditStorage tool checks the consistency of all replicas for each key-value pair.
Given a job range, DD partitions the job range in the unit of shards. DD randomly chooses a storage server from the set of shard owner to conduct the check.
This mechanism implicitly balances the workload among storage servers because the shard is stored among storage servers in a balanced way.

KeyServer and ServerKey consistency check (locationmetadata)
------------------------------------------------------------
The AuditStorage tool checks the consistency between KeyServer and ServerKey.
This job is conduct by DD only since the workload is small. It only needs to read all KeyServer and ServerKey metadata.

ServerKey and SS shard mapping consistency check (ssshard)
----------------------------------------------------------
The AuditStorage tool checks the consistency between ServerKey and SS local shard mapping.
In this job, the tool needs to check each storage server to see each SS has the shard mapping consistent with ServerKey.
For each SS, DD partitions the job range in the unit of shards. Given a shard, DD requests the SS to check the consistency between the shard mapping and the ServerKey.
The SS reads the shard mapping and compares it with the ServerKey.
