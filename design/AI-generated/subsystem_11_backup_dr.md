# Subsystem 11: Backup, Restore & DR

**[Diagrams](diagram_11_backup_dr.md)**

**Location:** [`fdbserver/backupworker/`](https://github.com/apple/foundationdb/tree/main/fdbserver/backupworker), `fdbclient/FileBackupAgent*`, `fdbclient/BackupContainer*`, [`fdbbackup/`](https://github.com/apple/foundationdb/tree/main/fdbbackup)
**Size:** ~10K server, ~20K client  
**Role:** Continuous backup to external storage, point-in-time restore, cross-cluster DR.

---

## Overview

FDB supports continuous backup of the mutation stream to external storage, enabling point-in-time restore. The backup system has two main components: server-side BackupWorkers that pull mutations from TLogs, and client-side BackupAgents that orchestrate the backup lifecycle and manage backup containers.

---

## Backup Architecture

### Two Backup File Types

**Log files** -- continuous mutation stream:
```
struct LogFile {
    Version beginVersion, endVersion;
    uint32_t blockSize;
    std::string fileName;
    int64_t fileSize;
    int tagId, totalTags;      // for partitioned logs
};
```

**Range files** -- snapshot of key-value pairs at a version:
```
struct RangeFile {
    Version version;
    uint32_t blockSize;
    std::string fileName;
    int64_t fileSize;
};
```

Together, range files + log files enable point-in-time restore: restore the range snapshot, then replay log files to reach any target version.

---

## BackupWorker -- [`fdbserver/backupworker/BackupWorker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/backupworker/BackupWorker.cpp)

Server-side role that pulls mutations from TLogs (like a storage server, but writes to backup storage).

### BackupData (lines 105-287)

```
struct BackupData {
    const UID myId;
    const Tag tag;                          // log router tag (-2, i)
    const int totalTags;
    const Version startVersion;
    const Optional<Version> endVersion;     // old epoch end
    const LogEpoch recruitedEpoch, backupEpoch;
    
    Version savedVersion;                   // largest version saved to storage
    AsyncVar<Reference<ILogSystem>> logSystem;
    std::vector<VersionedMessage> messages; // in-memory mutation buffer
    std::map<UID, PerBackupInfo> backups;   // per-backup configurations
};
```

### PerBackupInfo (lines 125-258)

```
struct PerBackupInfo {
    Version startVersion, lastSavedVersion;
    Future<Optional<Reference<IBackupContainer>>> container;
    Future<Optional<std::vector<KeyRange>>> ranges;
    bool stopped;
};
```

### Mutation Pulling

1. BackupWorker peeks TLog via its assigned `tagLocalityLogRouter` tag
2. Filters mutations based on backup key ranges
3. Buffers in `messages` vector
4. Writes to backup container when buffer reaches threshold
5. Tracks progress in system keys

---

## BackupAgent -- [`fdbclient/BackupAgent.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/BackupAgent.h), [`fdbclient/FileBackupAgent.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/FileBackupAgent.cpp)

Client-side orchestration of backup lifecycle.

### Backup States

```
enum class EnumState {
    STATE_ERRORED = 0,
    STATE_SUBMITTED = 1,
    STATE_RUNNING = 2,
    STATE_RUNNING_DIFFERENTIAL = 3,
    STATE_COMPLETED = 4,
    STATE_NEVERRAN = 5,
    STATE_ABORTED = 6
};
```

### FileBackupAgent Methods

| Method | Purpose |
|--------|---------|
| `submitBackup()` | Queue a new backup task |
| `restore()` | Start point-in-time restore |
| `atomicRestore()` | Atomic restore (single transaction) |
| `abortRestore()` | Cancel in-progress restore |
| `waitRestore()` | Wait for restore completion |
| `restoreStatus()` | Query restore progress |

### TaskBucket Pattern

Backup/restore operations use a distributed task execution pattern:
- Tasks stored as key-value entries in the database
- `TaskBucket` polls for ready tasks at `pollDelay` intervals
- Tasks can spawn sub-tasks for parallelism
- `FutureBucket` tracks task completion
- Provides reliability: tasks survive process failures

```
Future<Void> run(Database cx, double pollDelay, int maxConcurrentTasks);
```

---

## Backup Container -- [`fdbclient/BackupContainer.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/BackupContainer.h)

### IBackupContainer Interface (lines 249-366)

```
class IBackupContainer {
    // Write operations
    Future<Reference<IBackupFile>> writeLogFile(beginVersion, endVersion, blockSize);
    Future<Reference<IBackupFile>> writeRangeFile(version, snapshotFileCount, ...);
    Future<Reference<IBackupFile>> writeTaggedLogFile(...);
    
    // Read/describe
    Future<Reference<IAsyncFile>> readFile(name);
    Future<BackupDescription> describeBackup(bool deepScan);
    Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion);
    
    // Lifecycle
    Future<Void> create();
    Future<bool> exists();
    Future<Void> expireData(Version expireEndVersion, bool force);
    Future<Void> deleteContainer(int* pNumDeleted);
};
```

### Implementations

- **BackupContainerLocalDirectory** -- local filesystem
- **BackupContainerS3BlobStore** -- S3-compatible object storage

### File Format Versions

| Version | Format |
|---------|--------|
| `BACKUP_AGENT_MLOG_VERSION = 2001` | Old FileBackupAgent mutation logs |
| `PARTITIONED_MLOG_VERSION = 4110` | BackupWorker partitioned logs |
| `RANGE_PARTITIONED_MLOG_VERSION = 5001` | Range-partitioned logs |
| `BACKUP_AGENT_SNAPSHOT_FILE_VERSION = 1001` | Range file snapshots |
| `BACKUP_AGENT_ENCRYPTED_SNAPSHOT_FILE_VERSION = 1002` | Encrypted snapshots |

### RestorableFileSet (BackupContainer.h:221-234)

```
struct RestorableFileSet {
    Version targetVersion;
    std::vector<RangeFile> ranges;           // snapshot files
    std::vector<LogFile> logs;               // mutation log files
    Version continuousBeginVersion;
    Version continuousEndVersion;
    std::vector<KeyspaceSnapshotFile> snapshots;
};
```

---

## Restore Flow

### Restore Modes

| Mode | Description |
|------|-------------|
| `RANGEFILE` | Traditional: read range files + apply log files |
| `BULKLOAD` | Efficient: SST file ingestion via BulkLoad |

### Traditional Restore

1. Read range files to reconstruct base key-value state at snapshot version
2. Apply log files from snapshot version to target version
3. Each log file contains mutations that are replayed in order
4. Supports key prefix transformation (add/remove prefix during restore)

### RestoreConfig

```
class RestoreConfig : public KeyBackedTaskConfig {
    KeyBackedProperty<ERestoreState> stateEnum();
    KeyBackedProperty<Key> addPrefix(), removePrefix();
    KeyBackedProperty<bool> onlyApplyMutationLogs();
    KeyBackedProperty<std::string> bulkDumpJobId();
    KeyBackedProperty<Version> firstConsistentVersion();
};
```

---

## Disaster Recovery (DR) -- [`fdbclient/DatabaseBackupAgent.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/DatabaseBackupAgent.cpp)

Streams mutations from one cluster to another in near-real-time.

### DatabaseBackupAgent

```
class DatabaseBackupAgent {
    static const Key keyAddPrefix, keyRemovePrefix;
    static const Key keyRangeVersions;
    static const Key keyCopyStop, keyDatabasesInSync;
};
```

### DR Flow

1. Source cluster runs backup agent that reads committed mutations
2. Mutations streamed to destination cluster in near-real-time
3. Destination cluster applies mutations with optional prefix transformation
4. Enables warm standby cluster for disaster recovery

### BackupRangeTaskFunc

- Executes range-based copying for DR
- Finds shard boundaries, creates sub-tasks for parallelism
- Uses FlowLock for memory management (`CLIENT_KNOBS->BACKUP_LOCK_BYTES`)

---

## Data Flow

### Backup
```
TLog ──peek(logRouterTag)──▶ BackupWorker
                                │
                                ▼
                            Filter by backup key ranges
                                │
                                ▼
                            Buffer mutations
                                │
                                ▼
                            Write to IBackupContainer (log files)
                                │
                            + periodic range file snapshots
```

### Restore
```
IBackupContainer ──read range files──▶ Restore Agent
                                          │
                                          ▼
                                      Write base state to empty cluster
                                          │
                                          ▼
                  ──read log files──▶ Replay mutations to target version
                                          │
                                          ▼
                                      Cluster at target version state
```

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/backupworker/BackupWorker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/backupworker/BackupWorker.cpp) | Server-side mutation pulling and writing |
| [`fdbclient/include/fdbclient/BackupAgent.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/BackupAgent.h) | BackupAgentBase, FileBackupAgent API |
| [`fdbclient/FileBackupAgent.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/FileBackupAgent.cpp) | Backup/restore orchestration, TaskBucket |
| [`fdbclient/DatabaseBackupAgent.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/DatabaseBackupAgent.cpp) | DR agent, cross-cluster streaming |
| [`fdbclient/include/fdbclient/BackupContainer.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/BackupContainer.h) | IBackupContainer, file format definitions |
| [`fdbclient/BackupContainerFileSystem.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/BackupContainerFileSystem.cpp) | Filesystem-based container implementation |
| [`fdbbackup/backup.cpp`](https://github.com/apple/foundationdb/blob/main/fdbbackup/backup.cpp) | Backup CLI tool |
