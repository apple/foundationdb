** FoundationDB Full Restore V1 Internals **

# Background

This document explains internal mechanism of restoring backup files. The backup mechanism generates two types of files: (1) snapshot files (i.e. range files) and (2) mutation log files. The backup mechanism uploads the files to a blob store (e.g. S3). The snapshot files describes the complete set of key-value pairs within the range taken at a single version. Different snapshot files do not have any overlapping range and they are very likely at different versions. The mutation log file contains a set of mutations taken between a begin version and an end version. Note that two mutation log files may have overlapping key range and overlapping version range. For details, please check the backup data format document.

To trigger FDB full restore, we have a fdbrestore command line tool which takes following as inputs:

- A backup cluster identifier (-r), a URL used to target the data source of restoring. Must provide. In the below example, the URL is blobstore://@s3XXXamazonaws.com:XXX?bucket=XXX

- A list of key ranges (-k): If provided, only restore the database within the key ranges. Otherwise, restore the entire user key space ("" ~ \xff). By providing the key ranges to restore, the restore time can be decently reduced because fewer mutations are needed to apply to the database. However, the restore process still has to download many unnecessary backup data files because different key ranges mutations are mixed up in the same mutation log files.

- A target version (-v). Restore the database to the target version. If not provided, then setting the target version as the maximum restorable version. When CK team wants to do restore, they often provide a timestamp to us and we use this timestamp to get the right version as the target version setting to the restore workflow. Additionally, "fdbbackup describe ..." can be used to check whether a version is restorable.

For more details, please check the fdbrestore documentation.

The restore command triggers the FDB cluster to download snapshot files and mutation log files from the blob store and those files are applied to the database (snapshot/range files and mutation logs are explained in Backup Design V1). When restoring a snapshot file, a set of key-value pairs are read from the snapshot file and they are set to database directly using transactions. When restoring a mutation log file, a set of mutations are read from the mutation log file, and they are "applied" to the database through the commit proxy. Collectively these two sets of files contain the entire contents of the restored database. This document explains mechanisms to manage and execute the process in a distributed fashion, mainly focusing on answering two questions:

1. Since we want to restore the full database to a target version, the restoring process should include both snapshot restore and mutation log restore. How to organize the two types of file restore is an important question.

2. We want to do restoring process fast by leveraging the parallelism of distributed system, while the mutation restore should be in a strict version order. How to enforce the mutation version order while leveraging the parallelism is another important question. 

Note that there are existing two versions of restore: (1) Naive one; (2) Restoring the partitioned log. We focus on the restore V1 (restore V1 and restore V2 share the common framework). 

# Related doc

- Backup Design V1 Illustrations of how backup works using snapshot/range files and mutations logs.
- Backup data format For v1 (current/7.4) version of our backups. For v1 layout example, see Backup v1 Layout
- Backup V2 (range partitioned): https://github.com/apple/foundationdb/blob/main/design/backup_v2_partitioned_logs.md

# Core components

Following core components are involved in restore. We've mentioned some already but below we go into more detail:

- FDB Restore Command Line: The command line to start a restore job or track the progress of restoring, which is distinguished by an input option (i.e. fdbrestore start [options...] or fdbrestore status [options...]). Once the job is submitted, the job is executed by the FDB cluster and the tool process exits immediately. 

- Task Bucket: A distributed framework that organizes and executes restore tasks. When the restore tool submits a restore job, the job spawns the first task in the task bucket, which will spawn a series of children tasks to complete the restore. Users should also run a set of agents to execute the restore. Multiple agents execute the tasks in the bucket. Some tasks are restoring snapshots and some tasks are restoring mutations. Snapshots are applied to DB directly using transactions while mutations are applied to DB by writing to aLog at first.

- aLog (apply mutation log): A key range of system key space \xff\x02/alog/ to stage mutations before applying them. Using task bucket allows to restore mutation log files in parallel. However, mutations must be applied in a strict order --- version by version. To achieve this, before mutations are applied, restored mutations are staged in aLog with a key prefix, ordered by increasing versions so that later a getRange request can retrieve mutations in order.

- Commit proxy takes to-restore mutations from aLog and then applies mutations to database version by version. More specially, the commit proxy passively listens on the "set" operation mutation to the system key --- applyEndVersion (will introduce later), when the commit proxy receives the mutation, the commit proxy starts restoring the mutations from aLog.

- RestoreConfig:All configuration and metadata are stored in a key range of the system key space: \xff\x02/restore-agent/.

# Global Shared State (i.e. RestoreConfig)
RestoreConfig stores metadata to system key space which helps to organize metadata with key-value pairs with organized prefix in the system key space. The RestoreConfig includes following:

- Restore state: ERestoreState tracks states of restore progress:: QUEUED → STARTING/ABORT → RUNNING/ABORT → COMPLETED

- Configuration: such as backup URL, restore range, beginVersion, restoreVersion

- Restore files: fileSet, logFileSet, rangeFileSet

- Progress tracking: such as file count, block count

- Mutation applying control: ApplyBeginVersion, ApplyEndVersion

# Restore execution framework

The restore process is achieved by the collaboration of multiple distributed restore agents. This is achieved by a persistent distributed task execution tool, called task bucket. Since the restore process heavily relies on the task bucket, we explain the task bucket and restore tasks before delving into the restore process.

## Task Bucket

Task bucket is a framework of executing distributed tasks with online expansion. Tasks are generated online and executed by a distributed set of workers. Each task has a task-local state and a task can be executed by one worker at a time (because the task reservation mechanism, introduced in the following red text). To achieve this, each task (inherited from TaskFuncBase) must implement ::execute() and ::finish(). Each task will do execute at first() and then do finish() upon success. If a task (say A) spawns a new task (say B), we call A the parent task and B the child task. Typically, in the taskA::execute(), it does some user defined operations. Then, in the taskA::finish(), it spawns taskB by calling taskB::addTask(). User can define operations and task spawn logic by implementing the execute() and the finish() method of each user's task.

Specifically, A Task is a set of key=value parameters that constitute a unit of work for a TaskFunc to perform.
The parameter keys are specific to the TaskFunc that the Task is for, except for a set of reserved
parameter keys which are used by TaskBucket to determine which TaskFunc to run and provide
several other core features of TaskBucket.

Task Life Cycle:

1. Task is created in database transaction.

2. An executor (see TaskBucket class) will reserve an begin executing the task

3. Task's _execute() function is run.  This is non-transactional, and can run indefinitely.

4. If the executor loses contact with FDB, another executor may begin at step 2.  The first Task execution can detect this by checking the result of keepRunning() periodically.

5. Once a Task execution's _execute() call returns, the _finish() step is called. _finish() is transactional and is guaranteed to never be called more than once for the same Task.

## Restore

In the restore mechanism, we define five core tasks:

1. StartFullRestoreTaskFunc:

    - execute: Setting up a global restore configuration on system metadata (called RestoreConfig), including: beginVersion, targetVersion, setting ERestoreState::STARTING, gathering the snapshot file list and the mutation log file list to restore.
    
    - finish: UpdatingERestoreState::RUNNING, initialized ApplyBeginVersion and ApplyEndVersion (core to the restore process, introduced later). Spawning the first task for dispatching (called dispatch task). Whether the restore is running at V1 or V2 is decided by the first task (RestoreDispatchTaskFunc is for V1 and RestoreDispatchPartitionedTaskFunc is for V2).

2. RestoreDispatchTaskFunc (aka. dispatch task):

    - Param: beginVersion (determines which version's files to start with)

    - execute: Void

    - finish: Getting a batch of files to restore from beginVersion and beginFile; Spawning tasks for each file. If a file is a snapshot file, spawn a snapshot restore task, if a file is a mutation log file, spawn a mutation log restore task.Spawn a new dispatch task after the current one complete. The new dispatch task will start processing the file which is right after the processed files by the old task.

    - Related concept: task batch is a set of tasks that can be handled in parallel at a time by distributed agents. A new batch is spawned only when the existing batch completes. When adding tasks to a batch, only queue up to RESTORE_DISPATCH_ADDTASK_SIZE=150 files to restore and target RESTORE_DISPATCH_BATCH_SIZE=30000 total per batch but a batch must end on a complete version boundary so exceed the limit if necessary to reach the end of a version of files.

        1. RESTORE_DISPATCH_BATCH_SIZE specifies how many file blocks are downloaded before applying the mutations from aLog to the database. We want to choose the value not too small to leverage parallelism of download files. We want to choose the value not too large to avoid doing too much downloading work in advance.

        2. RESTORE_DISPATCH_ADDTASK_SIZE specifies how many works are dispatched by a single transaction, where we do not want this value too large to make the transaction too large to commit successfully. We do not want this value too small to have too much overhead of transactions to dispatch tasks.

    - When usePartitionedLog is set, the StartFullRestoreTaskFunc spawns RestoreDispatchPartitionedTaskFunc instead, as the core of Restore V2. We omit it here since we focus on the Restore V1.

3. RestoreRangeTaskFunc (aka. snapshot restore task)

    - Param: file, data offset

    - execute: Read block from the snapshot file and then write to database via a transaction.

    - finish: update applyMutationsMap (core to the restore process, introduced later).

4. RestoreLogDataTaskFunc (aka. mutation log restore task)

    - Param: file, data offset

    - execute: Read block from the mutation log file and then stage mutations to aLog

    - finish: update counters for progress monitoring

5. RestoreCompleteTaskFunc

    execute: Not implemented

    finish: set ERestoreState::COMPLETED, cleanup

## Worker pool and parallel execution with task bucket

Each worker continuously tries to get tasks using transactions guaranteeing atomic reservation. See getOne method. There is a task timeout mechanism and a task error handling mechanism. All worker contribute to completing a batch. Load balancing is achieved by each worker dynamically popping from a global task queue. 

All workers march through snapshot files and mutation logs applying as we go. These two operations can go in tandem because the mutation logs are not written into the database directly but via aLog. Note that no task in task bucket is for moving mutations from aLog into the database. Instead, the mutation log task triggers a system key mutation to commit proxy and commit proxy moves mutations from aLog to the database. This topic is covered in details by the next section. 

# Restore process management

In general, data is restored version by version. The process is controlled by a sliding window. The "window" is the range `[applyBeginVersion, applyEndVersion)`. applyBeginVersion and applyEndVersion are stored in the system key space.

- applyBeginVersion is the version that all versions below this version have been already applied.

- applyEndVersion is the version can be applied up to.

- The restore mechanism enforces that snapshots and mutation logs are applied version by version. Initially, applyBeginVersion and applyEndVersion are set to the BeginVersion of restore by StartFullRestoreTaskFunc. Then, a series of RestoreDispatchTaskFunc spawned to operate restore. When RestoreDispatchTaskFunc::finish(), applyEndVersion is updated using a transaction. Then the transaction arrives at a commit proxy and triggers applyMutations(applyBeginVersion, applyEndVersion). The applyMutations take mutations from aLog and send mutations to commit proxy mutation apply routine, then the applyBeginVersion is updated to the applied version + 1.

- applyMutationsMap is used to make sure the version of applied mutations on a range is larger than the snapshot range. The map locates at a system key space (\xff/applyMutationsKeyVersionMap). The key represents ranges of restored snapshot files, and the value represents the snapshot version. The map starts with empty and gradually gets filled when a snapshot is applied to the database. Initially, all ranges’ version is set to invalidVersion. When a range is restored by RestoreRangeTaskFunc, the range's version is set to the snapshot version. The commit proxy uses keyVersion keeps track of updates of the snapshot versions by range. When the commit proxy applying mutations, it only applies mutations on a range which snapshot has been set and the snapshot version is smaller than the mutation version.

# Applying mutations in order by using aLog metadata

aLog is a system metadata for staging mutations to restore. Note that the task bucket framework allow us to restore mutation log files on multiple workers in parallel. It is critical to make sure the mutations are applied in a strict order --- version by version. ALog is used for staging mutations extracted from the mutation files by multiple workers in parallel. Then, commit proxyapplies the mutationsversion by version with a single threaded process.

The aLog key encodes the commit version in the prefix so that when reading range of the metadata, we get the sorted mutations immediately.

Key:

`\xff\x02/alog/[restore uid][hash:uint8_t][commitVersion:uint64_t][partNumber:int32_t]`

Hash is generated by hash(commitVersion/1e6). 1e6 is the number of versions within a single second. The hash value represents the bucket uid of a single second. Within a bucket, all versions are sorted. When the commit proxy applying a version of mutations, the commit proxy firstly decides which bucket the version belongs to. Then, the commit proxy builds the key ranges that the CP should do the transaction read from the aLog. Adding the hash prefix is helpful to distribute any two successive versions of mutations to random two key ranges, preventing persistent hot-shard write to aLog key space.

partNumber is used when the number of committed mutations at a single version is too large. When the serialized mutations for a version is larger than the FDB's max value size, then we split the data into multiple keys, where each key is a "part".

The aLog value includes all mutation information committed at that commitVersion indicated in the key.

Value:

`[protocolVersion:uint64_t][val_length:uint32_t][mutation_1][mutation_2]...[mutation_k]`

The commit proxydecodes the value and applies the mutation in the order.

# Backup v1 Layout Example

Backup layout taken from 7.4 fdbback/tests/dir_backup_test.sh ctest:

```
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/kvranges
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/kvranges/snapshot.000000000001923285
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/kvranges/snapshot.000000000001923285/0
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/kvranges/snapshot.000000000001923285/0/range,1980422,c5c81efaa67c1b7bb5e17c756f3b2416,1048576
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/kvranges/snapshot.000000000001923285/0/range,1998818,192536233eafb59e5e854faf1b35d5ca,1048576
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/kvranges/snapshot.000000000001923285/0/range,1998818,db85ff16b3ea6ce5180450f3afc21925,1048576
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/kvranges/snapshot.000000000001923285/0/range,2025711,84ff114f9ed67c584d0c0ce2a4026458,1048576
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/snapshots
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/snapshots/snapshot,1980422,2025711,570
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/logs
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/logs/0000
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/logs/0000/0000
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/logs/0000/0000/log,1923285,21923285,392f2edb4fa32c2af5171686a6b7f8bb,1048576
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/properties
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/properties/log_begin_version
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/properties/log_end_version
.../backup/s3backup.E4hE/backups/backup-2025-08-29-23-17-10.284674/properties/mutation_log_type
```
