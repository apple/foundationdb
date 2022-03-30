# The New FDB Backup System: Requirements & Design

Github tracking issue: https://github.com/apple/foundationdb/issues/1003

## Purpose and Audience

The purpose of this document is to capture functional requirements as well as propose a high level design for implementation of  the new backup system in FoundationDB. The intended audience for this document includes:

* **FDB users** - Users can understand what are the changes in the new backup system, especially how to start a backup using the new backup system. The restore for new backup is handled by the [Performant Restore System](https://github.com/apple/foundationdb/issues/1049).
* **SRE's and Support** - can understand the high level architecture and know the requirements, including the metrics, tooling, and documentation to ensure that the new FDB backup can be supported.
* **Developers** - can know why this feature is needed, what it does, and how it is to be implemented. The hope is that this document becomes the starting point for any developer wishing to understand or be involved in the related aspects of FDB.

## Functional Requirements

As an essential component of a database system, backup and restore is commonly used technique for disaster recovery, reliability, audit and compliance purposes. The current FDB backup system consumes about half of the cluster’s write bandwidth, causes write skew among storage servers, increases storage space usage, and results in data balancing. The new backup system aims to double cluster’s write bandwidth for *HA clusters* (old DR clusters still need old style backup system).

## Background

FDB backup system continuously scan the database’s key-value space, save key-value pairs and mutations at versions into range files and log files in blob storage. Specifically, mutation logs are generated at CommitProxy, and are written to transaction logs along with regular mutations. In production clusters like CK clusters, backup system is always on, which means each mutation is written twice to transaction logs, consuming about half of write bandwidth and about 40% of CommitProxy CPU time.

The design of old backup system is [here](https://github.com/apple/foundationdb/blob/main/design/backup.md), and the data format of range files and mutations files is [here](https://github.com/apple/foundationdb/blob/main/design/backup-dataFormat.md). The technical overview of FDB is [here](https://github.com/apple/foundationdb/wiki/Technical-Overview-of-the-Database). The FDB recovery is described in this [doc](https://github.com/apple/foundationdb/blob/main/design/recovery-internals.md).


## Terminology

* **Blob storage**: blob storage is an object storage for unstructed data. Backup files are encoded in binary format and saved in blob storage, e.g., Amazon S3.
* **Version**: FDB continuously generate increasing number as version and use version to decide mutation ordering. Version number typically advance one million per second. To restore a FDB cluster to a specified date and time, the restore system first convert the date and time to the corresponding version number and restore the cluster to the version number.
* **Epoch**: A generation of FDB’s transaction system. After a component of the transaction system failed, FDB automatically initiates a recovery and restores the system in a new healthy generation, which is called an epoch.
* **Backup worker**: is a new role added to the FDB cluster that is responsible for pulling mutations from transaction logs and saving them to blob storage.
* **Tag**: A tag is a short address for a mutation’s destination, which includes a locality (`int8_t`, representing the data center ID and a negative number denotes special system locality) and an ID (`int16_t`). The idea is that the tag is a small data structure that consumes less bytes than using IP addresses or storage server’s UIDs (16 bytes each), since tags are associated with each mutation and are stored both in memory and on disk.
* **Tag partitioned log system**: FDB’s write-ahead log is a tag partitioned log system, where each mutation is assigned a number of tags.
* **Log router tag**: is a special system tag, e.g., `-2:0` where locality `-2` means log router tag and `0` means ID. If attached to a mutation, originally this tag means the mutation should be sent to a remote log router. In the new backup system, we reuse this tag for backup workers to receive all mutations in a number of partitioned streams.
* **Restorable version:** The version that a backup can be restored to.  A version `v` is a restorable version if the entire key-space and mutations in version `[v1, v)` are recorded in backup files.
* **Node**: A node is a machine or a process in a cluster.

## Detailed Feature Requirements

Feature priorities: Feature 1, 2, 3, 4, 5 are must-have; Feature 6 is better to have.

1. **Write bandwidth reduction by half**: removes the requirement to generate backup mutations at the CommitProxy, thus reduce TLog write bandwidth usage by half and significantly improve CommitProxy CPU usage;
2. **Correctness**: The restored database must be consistent: each *restored* state (i.e., key-value pair) at a version `v` must match the original state at version `v`.
3. **Performance**: The backup system should be performant, mostly measured as a small CPU overhead on transaction logs and backup workers. The version lag on backup workers is an indicator of performance.
4. **Fault-tolerant**: The backup system should be fault-tolerant to node failures in the FDB cluster.
5. **Restore ready**: The new backup system should be restored by the Performant Restore System. As a fallback for new performant restore system, we can convert new backup logs into the format of old backup logs, thus enabling restore of the new backup with existing old restore system.
6. **Backward compatibility**:  The new backup system should allow both old style backup and DR (FDB 6.2 and below) to be performed, as well as support new backup in FDB 6.3 and above.

## Security and Privacy Requirements

**Security**: The backup system’s components are assumed to be trusted components, because they are running on the nodes in a FDB cluster. The transmission from cluster to blob store is through SSL connections. Blob credentials are passed in from “fdbserver” command line.

**Privacy**: Backup data are stored in blob store with appropriate access control. Data retention policy can be set with “fdbbackup” tool to delete older backup data.

## Operational and Maintainability Requirements

This section discusses changes that may need to be identified or accounted for on the back-end in order to support the feature from a monitoring or management perspective.

### Tooling / Front-End

Workflow is needed for DBA to start, pause, resume, abort the new type of backups. The difference from the old type of backups should be only a flag change for starting the backup. The FDB cluster then generates backups as specified by the flag.

A command line tool `fdbconvert` has been written to convert new backup logs into the format of old backup logs. Thus, if the new restore system has issues, we can still restore the new backup with existing old restore system.

**Deployment instructions for tooling development**

* A new stateless role “`Backup Worker`” (or “`BW`” for abbreviation) is introduced in a FDB cluster. The number of BW processes is based on the number of log routers (usually they are the same). If there is no log routers, the number of transaction logs is used. Note that occasionally the cluster may recruit more backup workers for version ranges in the old epoch. Since these version ranges are small, the resource requirements for these short-lived backup workers are very small.
* As in the old backup system, backup agents need to be started for saving snapshot files to blob storage. In contrast, backup workers in the new backup system running in the primary DC are responsible for saving mutation logs to blob storage.
* Backup worker’s memory should be large enough to hold 10s of seconds worth of mutation data from TLogs. The memory requirement can be calculated as: `WriteThroughput * BufferPeriod / partitions + SafetyMargin`, where `WriteThroughput` is the aggregated TLog write bandwidth, `partitions` is the number of log router tags.
* A new process class “backup” is defined for backup workers.
* How to start a new type backup: e.g.,

  ```
  fdbbackup start -C fdb.cluster -p -d blob_url
  ```

### KPI's and Health

The solution must provide at least the following KPIs:

* How fast (MB/s) does the transaction logs commit writes (already existed);
* How much backup data has been processed;
* An estimation of backup delay;

### Customer Care

The feature does not require any specific customer care awareness or interaction.

### Roll-out

The feature must follow the usual roll-out process. It needs to coexist with the existing backup system and periodically restore clusters to test its correctness. Only after we gain enough confidence will we deprecate the existing backup system.

Note the new backup system is designed for HA clusters. Existing DR clusters still uses the old backup system. Thus, rolling out of the new backup system is only for HA clusters.

### Quota

This feature requires a blob storage for saving all log files. The blob storage must have enough:

* disk capacity for all backup data;
* write bandwidth for uploading backup data;
* file count for backup data: the new backup system stored partitioned mutation logs, thus expecting several time increases of the file count.

## Success Criteria

* Write bandwidth reduction meets the expectation: TLog write bandwidth is reduced by half;
* New backup workflow is available to SREs;
* Continuous backup and restore should be performed to validate the restore.

# Design

**One sentence summary**: the new backup system introduces a new role, backup worker, to pull mutations from transaction logs and save them, thus removing the burden of saving mutation logs into the database.

The old backup system writes the mutation log to the database itself, thus doubling the write bandwidth usage. Backup agents later fetch mutation logs from the database, upload them to blob storage, and then remove the mutation logs from the database.

This project saves the mutation log to blob storage directly from the FDB cluster, which should almost double the database's write bandwidth when backup is enabled. In FDB, every mutation already has exactly one log router tag, so the idea of the new system is to backup data for each log router tag individually (i.e., saving mutation logs into multiple partitioned logs). During restore time, these partitioned mutation logs are combined together to form a continuous mutation log stream.

## Design choices

**Design question 1**: Should backup workers be recruited as part of log system or not?
There are two design alternatives:

1. Backup worker is external to the log system. In other words, backup workers survive cluster recovery. Thus, backup workers are recruited and monitored by the cluster controller.
    1. The advantage is that the failure of backup workers does not cause cluster recovery.
    2. The disadvantage is that backup workers need to monitor cluster recovery, especially configuration changes. Because the number of log routers can change after a recovery, we might need to recruit more backup workers for an increase and need to pause/shutdown backup workers for a decrease, which complicates the recruitment logic; or we might need to changing the mapping of tags to backup workers, which is also complex. A further complication is that backup workers need to constantly monitor cluster recovery and be very careful about the version boundary between two consecutive epochs, because the number of tags may change.
2. Backup worker is recruited during cluster recovery as part of log system. The Cluster Controller recruits a fixed number of backup workers, i.e., the same number as LogRouters.
    1. The advantage is that recruiting and mapping from backup worker to LogRouter tags are simple, i.e., one tag per worker.
    2. The disadvantages is that backup workers are tied with cluster recovery -- a failure of a backup worker results in a cluster recovery, and a cluster recovery stops old backup workers and starts new ones.

**Decision**: We choose the second approach for the simplicity of the recruiting process and handling of mapping of LogRouter tags to backup workers.

**Design question 2**: Place of backup workers on the primary or remote Data Center (DC)?
Placing backup workers on the primary side has the advantage of supporting any deployment configurations (single DC, multi DC).

Placing on the remote is desirable to reduce the workload on the primary DC’s transaction logs. Since log routers on the remote side is already pulling mutations from primary DC, backup workers can simply pull from these log routers.

**Decision**: We choose to recruit backup workers on the primary DC, because not all clusters are configured with multiple DCs and the backup system needs to support all types of deployment.

## Design Assumptions

The design proposed below is based upon the following assumptions:

* Blob system has enough write bandwidth and storage space for backup workers to save log files.
* FDB cluster has enough stateless processes to run as backup workers and these processes have memory capacity to buffer 10s of seconds of commit data. 

## Design Challenges

The requirement of the new backup system raises several design challenges:

1. Correctness of the new backup files. Backup files must be complete and accurate to capture all data, otherwise we end up with corrupted data in the backup. The challenge here is to make sure no mutation is missing, even when the FDB cluster experiences failures and has to perform recovery.
2. Testing of the new backup system. How can we test the new backup system when there is no restore system available? We need to verify backup files are correct without performing a full restore.

## System components

**Backup Worker**: This is a new role introduced in the new backup system. A backup worker is a `fdbserver` process running inside a FDB cluster, responsible for pulling mutations from transaction logs and saving the mutations to blob storage.

**Cluster Controller (CC)**: The CC is responsible for coordinating the transition of the FDB transaction sub-system from one generation to the next. In particular, the CC recruits backup workers during the recovery.

**Transaction Logs (TLogs)**: The transaction logs make mutations durable to disk for fast commit latencies. The logs receive commits from the commit proxy in version order, and only respond to the commit proxy once the data has been written and fsync'ed to an append only mutation log on disk. Storage servers retrieve mutations from TLogs. Once the storage servers have persisted mutations, storage servers then pop the mutations from the TLogs.

**CommitProxy**: The commit proxies are responsible for committing transactions, and tracking the storage servers responsible for each range of keys. In the old backup system, commit proxies are responsible to group mutations into backup mutations and write them to the database.

**GrvProxy**: The GRV proxies are responsible for providing read versions.
## System overview

From an end-to-end perspective, the new backup system works in the following steps:

1. Operators issue a new backup request via `fdbbackup` command line tool;
2. FDB cluster receives the request and registers the request in the database (internal `TaskBucket` and system keys);
3. Backup workers monitor changes to system keys, register the request in its own internal queue, and starts logging mutations for the request key range; at the same time, backup agents (scheduled by `TaskBucket`) starts taking snapshots of key ranges in the database;
4. Periodically, backup workers upload mutations to the requested blob storage, and save the progress into the database;
5. The backup is restorable when backup workers have saved versions that are larger than the complete snapshot’s end version, and the backup is stopped if a stop on restorable flag is set in the request.

The new backup has four major components: 1) backup workers; 2) recruitment of backup workers; 3) extension of tag partitioned log system to support pseudo tags; 4) integration with existing `TaskBucket` based backup command interface; and 5) integration with the Performant Restore System.

### Backup workers

Backup worker is a new role introduced in the new backup system. A backup worker is responsible for pulling mutations from transaction logs and saving the mutations to blob storage. Internally, a backup worker maintains a message buffer, which keeps mutations pulled from transaction logs, but have not been saved to blob storage yet. Periodically, the backup worker parses mutations in the message buffer, extracts those mutations that are within user specified key ranges, and then uploads mutation data to blob storage. After data is saved, the backup worker removes these messages from its internal buffer and saves its progress in the database, so that after a failure, a new backup worker starts from the previously saved version.

Backup worker has two modes of operation: *no-op* mode, and *working* mode. When there is no active backup in the cluster, backup worker operates in the no-op mode, which simply obtains the recently committed version from Proxies and then pops mutations from transaction logs. After operators submit a new backup request to the cluster, backup workers transition into the working mode that starts pulling mutations from transaction logs and saving the mutation data to blob storage.

In the working mode, the popping of backup workers need to follow a strictly increasing version order. For the same tag, there could be multiple backup workers, each is responsible for a different epoch. These backup workers must coordinating their popping order, otherwise the backup can miss some mutation data. This coordination among backup workers is achieved by deferring popping of a later epoch and only allowing the oldest epoch to pop first. After the oldest epoch has finished, these corresponding backup workers notifies the CC, which will then advances the oldest backup epoch so that the next epoch can proceed the popping.

A subtle issue for a displaced backup worker (i.e., being displaced because a new epoch begins), is that the last pop of the backup worker can cause missing version ranges in mutation logs. This is because the transaction for saving the progress may be delayed during recovery. As a result, the CC could already recruited a new backup worker for the old epoch starting at the previously saved progress version. Then the saving transaction succeeds, and the worker pops mutations that the new backup worker is supposed to save, resulting in missing data for new backup worker’s log. The solution to this problem can be: 1) the old backup worker aborts immediately after knowing itself is displaced, thus not trying to save its progress; or 2) the old backup worker skip its last pop, since the next epoch will pop versions larger than its progress. Because the second approach avoids doing duplicated work in the new epoch, we choose to the second approach.

Finally, multiple concurrent backups are supported. Each backup worker keeps track of current backup jobs and saves mutations to corresponding backup containers for the same batch of mutations.

### Recruitment of Backup workers

Backup workers are recruited during cluster recovery as part of log system. The CC recruits a fixed number of backup workers, one for each log router tag. During the recruiting process, the CC sends backup worker initialization request as:

```
struct InitializeBackupRequest {
    UID reqId;
    LogEpoch epoch; // epoch this worker is recruited
    LogEpoch backupEpoch; // epoch that this worker actually works on
    Tag routerTag;
    Version startVersion;
    Optional<Version> endVersion; // Only present for unfinished old epoch
    ReplyPromise<struct BackupInterface> reply;
    … // additional methods elided
};

```

Note we need two epochs here: one for the recruited epoch and one for backing up epoch. The recruited epoch is the epoch of the log system, which is used by a backup worker to find out if it works for the current epoch. If so, the worker should save its progress and immediately exit. The `backupEpoch` is used for saving progress. The `backupEpoch` is usually the same as the epoch that the worker is recruited. However, it can be some earlier epoch than the recruiting epoch, signifying that the worker is responsible for data in that earlier epoch. In this case, when the worker is done and exits, the CC should not flag its departure as a trigger of recovery. This is solved by the following protocol:

1. The backup worker finishes its work, including saving progress to the key value store and uploading to cloud storage, and then sends a `BackupWorkerDoneRequest` to the CC;
2. The CC receives the request, removes the worker from its log system, and updates the oldest backing up epoch `oldestBackupEpoch`;
3. The CC sends backup a reply message to the backup worker and registers the new log system with cluster controller;
4. The backup worker exits after receiving the reply. Other backup workers in the system get the new log system from the cluster controller. If a backup worker’s `backupEpoch` is equal to `oldestBackupEpoch`, then the worker may start popping from TLogs.

Note `oldestBackupEpoch` is introduced to prevent a backup worker for a newer epoch from popping when there are backup workers for older epochs. Otherwise, these older backup workers may lose data.

### Extension of tag partitioned log system to support pseudo tags

The tag partitioned log system is modeled like a FIFO queue, where Proxies push mutations to the queue and Storage Servers or Log Routers pop mutations from the queue. Specifically, consumers of the tag partitioned log system use two operations, `peek` and `pop`, to read mutations for a given tag and to pop mutations from the queue. Because Proxies assign each mutation a unique log router tag, the backup system reuses this tag to obtain the whole mutation stream. As a result, each log router tag now has two consumers, a log router and a backup worker.

To support multiple consumers of the log router tag, the peek and pop has been extended to support pseudo tags. In other words, each log router tag can be mapped to multiple pseudo tags. Log routers and Backup workers still `peek` mutations with the log router tag, but `pop` with different pseudo tags. Only after both pseudo tags are popped, TLogs can pop the mutations from its internal queue.

Note the introduction of pseudo tags opens the possibility for more usage scenarios. For instance, a change stream can be implemented with a pseudo tag, where the new consumer can look at each mutation and emit mutations on specified key ranges.

### Integration with existing taskbucket based backup command interface

We strive to keep the operational interface the same as the old backup system. That is, the new backup is initiated by the client as before with an additional flag. FDB cluster receives the backup request, sees the flag being set, and uses the new system for generating mutation logs.

By default, backup workers are not enabled in the system. When operators submit a new backup request for the first time, the database performs a configuration change (`backup_worker_enabled:=1`) that enables backup workers.

The operator’s backup request can indicate if an old backup or a new backup is used. This is a command line option (i.e., `-p` or `--partitioned-log`) in the `fdbbackup` command. A backup request of the new type is started in the following steps:

1. Operators use `fdbbackup` tool to write the backup range to a system key, i.e., `\xff\x02/backupStarted`.
2. All backup workers monitor the key `\xff\x02/backupStarted`, see the change, and start logging mutations.
3. After all backup workers have started, the `fdbbackup` tool initiates the backup of all or specified key ranges by issuing a transaction `Ts`.

Compared to the old backup system, the above step 1 and 2 are new and is only triggered if client requests for a new type of backup. The purpose is to allow backup workers to function as no-op if there are no ongoing backups. However, the backup workers should still continuously pop their corresponding tags, otherwise mutations will be kept in the TLog. In order to know the version to pop, backup workers can obtain the read version from any GRV proxy. Because the read version must be a committed version, so popping to this version is safe.

**Backup Submission Protocol**
Protocol for `submitBackup()` to ensure that all backup workers of the current epoch have started logging mutations:

1. After the `submitBackup()` call, the task bucket (i.e., `StartFullBackupTaskFunc`) starts by creating a `BackupConfig` object in the system key space.
2. Each backup worker monitors the `\xff\x02/backupStarted` key and notices the new backup job. Then the backup worker inserts the new job into its internal queue, and writes to `startedBackupWorkers` key in the `BackupConfig` object if the worker’s `backupEpoch` is the current epoch. Among these workers, the worker with Log Router Tag `-2:0` monitors the `startedBackupWorkers` key, and sets `allWorkerStarted` key after all workers have updated the `startedBackupWorkers` key.
3. The task bucket watches change to the `startedBackupWorkers` key and declares the job submission successful.

This protocol was implemented after another abandoned protocol: the `startedBackupWorkers` key is set after all backup workers have saved logs with versions larger than the version of `submitBackup()` call. This protocol fails if there is already a backup job and there is a backup worker that doesn’t notice the change to the `\xff\x02/backupStarted` key. As a result, the worker is saving versions larger than the new job’s start version, but in the old backup container. Thus the new container misses some mutations.

**Protocol for Determining A Backup is Restorable**

1. Each backup worker independently logs mutations to a backup container and updates its progress in the system key space.
2. The worker with Log Router Tag `-2:0` of current epoch monitors all workers’ progress. If the oldest backup epoch is the current epoch (i.e, there are no backup workers for any old epochs, thus no version ranges missing before this epoch), this worker updates `latestBackupWorkerSavedVersion` key in the `BackupConfig` object with the minimum saved version among workers.
3. The client calls `describeBackup()`, which eventually calls `getLatestRestorableVersion` to read the value from the `latestBackupWorkerSavedVersion` key. If this version is larger than the first snapshot’s end version, then the backup is restorable.

**Pause and Resume Backups**
The command line for pause or resume backups remains the same, but the implementation for the new backup system is different from the old one. This is because in the old backup system, both mutation logs and range logs are handled by `TaskBucket`, an asynchronous task scheduling framework that stores states in the FDB database. Thus, the old backup system simply pauses or resumes the `TaskBucket`. In the new backup system, mutation logs are generated by backup workers, thus the pause or resume command needs to tell all backup workers to pause or resume pulling mutations from TLogs. Specifically,

1. The operator issues a pause or resume request that updates both the `TaskBucket` and `\xff\x02/backupPaused` key.
2. Each backup worker monitors the `\xff\x02/backupPaused` key and notices the change. Then the backup worker pauses or resumes pulling from TLogs.

**Backup Container Changes**

* Partitioned mutation logs are stored in `plogs/XXXX/XXXX` directory and their names are in the format of `log,[startVersion],[endVersion],[UID],[N-of-M],[blockSize]`, where `M` is total partition number, `N` can be any number from `0` to `M - 1`. In contrast, old mutation logs are stored in `logs/XXXX/XXXX` directory and are named differently.
* To restore a version range, all partitioned logs for the range needs to be available. The restore process should read all partitioned logs, and combine mutations from different logs into one mutation stream, ordered by `(commit_version, subsequence)` pair. It is guaranteed that all mutations form a total order. Note in the old backup files, there is no subsequence number, as each version’s mutations are serialized in order in one file.

### Integration with the [Performant Restore System](https://github.com/apple/foundationdb/issues/1049)

As discussed above, the new backup system split mutation logs into multiple partitions. Thus, the restore process must verify the backup files are continuous for all partitions with the restore’s version range. This is possible because each log file name has the information about its partition number and the total number of partitions.

Once the restore system verifies the version range is continuous, the restore system needs to filter out duplicated version range among different log files (both log continuity analysis and dedup logic are implemented in `BackupContainer` abstraction). A given version range may be stored in **multiple** mutation log files. This can happen because a recruited backup worker can upload mutation files successfully, but doesn’t save the progress before another recovery happens. As a result, the new epoch tries to backup this version range again, producing the same version ranges (though the file names are different).

Finally, the restore system loads the same version’s mutations from all partitions, and then merges these mutations in the order of their subsequence number before they are applied on the restore cluster. Note the mutations in the old backup system lack subsequence numbers. As a result, restoring old backups needs to assign subsequence number to mutations.

## Ordered and Complete Guarantee of Mutation Logs

The backup system must generate log files that the restore system can apply all the mutations on the backup cluster in the same order exactly once.

**Ordering guarantee**. To maintain the ordering of mutations, each mutation is stored with its commit version and a subsequence number, both are assigned by Proxies during commit. The restore system can load all mutations and derive a total order among all the mutations.

**Completeness guarantee**. All mutations should be saved in log files. We cannot allow any mutations missing from the backup. This is guaranteed by the fault tolerance discussed below. Essentially all backup workers checkpoint their progress in the database. After the recovery, the new CC reads previous checkpoints and recruit new backup workers for any missing version ranges.

## Backup File Format

The old backup file format is documented [here](https://github.com/apple/foundationdb/blob/release-6.2/design/backup-dataFormat.md). We can’t use this file format, because our backup files are created for log router tags. When there are more than one log routers (almost always the case), the mutations in one transaction can be given different log router tags. As a result, for the same version, mutations are distributed in many files. Another subtle issue is that, there can be two mutations, (e.g., `a = 1` and `a = 2` in a transaction), which are given two different tags. We have to preserve the order of these two mutations in the restore process. Even though the order is saved in the sub-sequence number of a version, we still need to merge mutations from multiple files and apply them in the correct order.

In the new backup system, mutation log file is named as `log,[startVersion],[endVersion],[UID],[N-of-M],[blockSize]`, where `startVersion` is inclusive and `endVersion` is *not* inclusive, e.g., `log,332850851,332938927,7be23c0a3e80df8ab1530fa76fa66980,1-of-4,1048576`. With the information from all file names, the restore process can find all files for a version range, i.e., versions intersect with the range and all log router tags. “`M`” is the total number of tags, and “`N`” is from `0` to `m - 1`.Note `tagId` is not required in the old backup filename, since all mutations for a version are included in one file.

Each file content is a list of fixed size blocks. Each block contains a sequence of mutations, where each mutation consists of a serialized `Version`, `int32_t`, `int32_t`, (all these three numbers are in big endian) and `Mutation`, where `Mutation` is of format `type|kLen|vLen|Key|Value`, where `type` is the mutation type (e.g., `Set` or `Clear`), `kLen` and `vLen` respectively are the lengths of the key and value in the mutation. `Key` and `Value` are the serialized value of the Key and Value in the mutation. The paddings at the end of the block are bytes of `0xFF`.

```
`<BlockHeader>`
`<Version_1><Subseq_1><Mutation1_len><Mutation1>`
`<Version_2><Subseq_2><Mutation2_len><Mutation2>`
`…`
`<Padding>
`
```

Note the big Endianness for version is required, as `0xFF` is used as the padding to indicate block end. A little endian number can easily be mistaken as the end. In contrast, big endian for version almost guarantee the first byte is not `0xFF` (should always be `0x00`).

## Performance optimization

### Future Optimizations

Add a metadata file describe the backup file:

* The number of mutations;
* The number of atomic operations;
* key range and version range of mutations in each backup file;

The information can be used to optimize the restore process. For instance, the number of mutations can be used to make better load balancing decisions; if there is no atomic operations, the restore can apply mutation in a backward fashion -- skipping mutations with earlier versions.

## Fault Tolerance

Failures of a backup worker will trigger a cluster recovery. After the recovery, the new CC recruits a new set of backup workers. Among them, a new backup worker shall continue the work of the failed backup worker from the previous epoch.

The interesting part is the handling of old epochs, since the backup workers for the old epoch are in the “displaced” state and should exit. So the basic idea is that we need a set of backup workers for the data left in the old epochs. To figure out the set of data not backed up yet, the CC first loads saved backup progress data `<Worker_UID, LogEpoch, SavedVersion, Tag, TotalTags> `from the database, and then computes for each epoch, what version ranges have not been backed up. For each of the version range and tag, the CC recruits a worker to resume the backup for that version range and tag. Note that this worker has a different worker UID from the worker in the original epoch. As a result, for a given epoch and a tag, there might be multiple progress status, as these workers are recruited at different epochs.

## KPI's and Metrics

The backup system emits the following metrics:

* How much backup data has been processed: the backup command line tool `fdbbackup` can show the status of backup, including the size of mutation logs (`LogBytes written`) and snapshots (`RangeBytes written`). By taking two consecutive backup status, the backup speed can be estimated as (`2nd_LogBytes - 1st_LogBytes) / interval`.
* An estimation of backup delay: Each backup worker emits `BackupWorkerMetrics` trace events every 5 seconds, which includes `SavedVersion`, `MinKnownCommittedVersion`, and `MsgQ`. The backup delay can be estimated as (`MinKnownCommittedVersion - SavedVersion) / 1,000,000` seconds, which is the difference between a worker’s saved version and current committed version, divided by 1M version per second. `MsgQ` is the queue size of memory buffer of the backup worker.

## Controlling Properties

System operator can control the following backup properties:

* **Backup key ranges**: The non-overlapped key ranges that will be backed up to the blob storage.
* **Blob url**: The root path in blob that host all backup files.
* **Performance knobs**: The knobs that control the performance
    * The backup interval (knob `BACKUP_UPLOAD_DELAY`) for saving mutation logs to blob storage;

## Testing

The feature will be tested both in simulation and in real clusters:

* New test cases are added into the test folder in FDB. The nightly correctness (i.e., simulation) tests will test the correctness of both backup and restore.
* Tests will be added to constantly backup a cluster with the new backup system and restore the backup to ensure the restore works on real clusters. During the time period of active backup, the cluster should have better write performance than using old backup system.
* Tests should also be conducted with production data. This ensures backup data is restorable and catches potential bugs in backup and restore. This test is preferably conducted regularly, e.g., weekly per cluster.

Before the restore system is available, the testing strategy for backup files is to keep old backup system running. Thus, both new backup files and old backup files are generated. Then both types of log files are decoded and compared against. The new backup file is considered correct if its content matches the content of old log files.
