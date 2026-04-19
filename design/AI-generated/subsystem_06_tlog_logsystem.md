# Subsystem 6: Transaction Log (TLog) & Log System

**[Diagrams](diagram_06_tlog_logsystem.md)**

**Location:** [`fdbserver/tlog/`](https://github.com/apple/foundationdb/tree/main/fdbserver/tlog), [`fdbserver/logsystem/`](https://github.com/apple/foundationdb/tree/main/fdbserver/logsystem), [`fdbserver/logrouter/`](https://github.com/apple/foundationdb/tree/main/fdbserver/logrouter)
**Size:** ~17K  
**Role:** Durable mutation logging, tag-partitioned replication, peek cursors for storage servers.

---

## Overview

The TLog subsystem is FDB's durability guarantee. All committed mutations flow through TLogs before being applied to storage servers. A mutation is committed when a quorum of TLog replicas acknowledge. Storage servers then pull (peek) mutations asynchronously.

---

## TLog Server -- [`TLogServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/tlog/TLogServer.actor.cpp)

### TLogData (lines 295-420)

Top-level container for all TLog data in a process:

```
struct TLogData {
    Deque<UID> popOrder;                     // pop ordering across generations
    Deque<UID> spillOrder;                   // spill ordering
    std::map<UID, Reference<LogData>> id_data;  // all log generations by UID
    
    IKeyValueStore* persistentData;          // spilled data on disk (KVStore)
    IDiskQueue* rawPersistentQueue;          // physical disk queue
    TLogQueue* persistentQueue;              // logical queue wrapper
    
    int64_t diskQueueCommitBytes;
    int64_t bytesInput, bytesDurable;
    int64_t targetVolatileBytes;             // spill threshold
    
    NotifiedVersion queueCommitEnd;          // durable commit location
    Version queueCommitBegin;
};
```

### LogData (lines 422-810)

Per-epoch/generation log container:

```
struct LogData {
    struct TagData : ReferenceCounted<TagData> {
        std::deque<std::pair<Version, LengthPrefixedStringRef>> versionMessages;
        bool nothingPersistent;
        Version popped;                      // consumed up to this version
        Version persistentPopped;            // durable pop point
        Tag tag;
    };
    
    Map<Version, std::pair<IDiskQueue::location, IDiskQueue::location>> versionLocation;
    Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
    std::vector<std::vector<Reference<TagData>>> tag_data;  // [locality][id]
    
    VersionMetricHandle persistentDataVersion;
    VersionMetricHandle persistentDataDurableVersion;
    NotifiedVersion version;                 // current log version
    NotifiedVersion queueCommittedVersion;   // disk queue durability point
    Version knownCommittedVersion;
};
```

### Push Flow (`tLogPush`)

1. Receive mutation batch from commit proxy
2. Deserialize `TLogQueueEntry` containing tagged mutations
3. Route each message by tag to appropriate `TagData` queue
4. Append to `versionMessages` deque (in-memory)
5. Write to `persistentQueue` (DiskQueue) for durability
6. Update `versionLocation` map
7. Reply when disk write confirmed

### Peek Flow (`tLogPeekStream`)

1. Storage server requests mutations for tag starting at version
2. Iterate `TagData::versionMessages` from requested version
3. For versions not in memory: read from `persistentData` (KVStore)
4. Send results in `TLogPeekStreamReply` chunks
5. Respect `limitBytes` and `peekMemoryLimiter` for backpressure
6. Track popped version and cursor position

### DiskQueue -- `TLogQueue` (lines 112-285)

Append-only durable queue:
- `push(entry)` -- add entry with tag metadata
- `pop(location)` -- remove committed entries up to location
- Entries prefixed with length + protocol version
- Recovery-safe: CRC checks for partial writes
- Linked to version boundaries for efficient cleanup

### Spilling

When in-memory data exceeds `targetVolatileBytes`:
- Oldest data spilled from memory to `persistentData` (KVStore on disk)
- `spillOrder` determines which generation spills first
- Peek reads transparently merge in-memory and spilled data

---

## Log System -- [`TagPartitionedLogSystem.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logsystem/TagPartitionedLogSystem.cpp)

Abstraction over the set of TLog replicas.

### ILogSystem Interface -- [`LogSystem.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/LogSystem.h)`:57-347`

**Core methods:**

| Method | Purpose |
|--------|---------|
| `push(versionSet, data, spanContext)` | Send mutations to TLog replicas, wait for quorum |
| `peek(dbgid, begin, end, tag)` | Create cursor for reading mutations by tag |
| `peekSingle(dbgid, begin, tag, history)` | Peek from preferred TLog |
| `peekLogRouter(dbgid, begin, tag)` | Peek from same-generation logs only |
| `peekTxs(dbgid, begin, tag, locality, isLong)` | Peek transaction state mutations |
| `pop(upTo, tag, knownCommittedVersion)` | Allow TLog to discard messages before version |
| `onCoreStateChanged()` | Future that fires when log set changes |
| `newEpoch(recr, fRemoteWorkers, config, ...)` | Create new log epoch during recovery |

### IPeekCursor Interface (lines 60-141)

Lazy iterator over log messages:

```
struct IPeekCursor {
    bool hasMessage() const;               // current message exists?
    VectorRef<Tag> getTags() const;        // tags for current message
    Arena& arena();                        // arena for message data
    ArenaReader* reader();                 // deserialize current message
    void nextMessage();                    // advance to next
    Future<Void> getMore(TaskPriority);    // wait for more messages
    LogMessageVersion version();           // current message version
    Version popped();                      // earliest available version
    bool isActive() / isExhausted();       // cursor status
};
```

### Push Implementation

1. **Tag distribution**: Partition mutations by tag (from commit proxy's `tagsForKey()`)
2. **TLog selection**: `getPushLocations()` determines which TLogs handle each tag
3. **Message formatting**: Build `LogPushData` with serialized mutations
4. **Parallel push**: Send to all relevant TLog replicas
5. **Quorum wait**: Block until `f+1` of `2f+1` TLogs acknowledge
6. **Return**: Promise resolves to next pushable version

### Epoch Management

Each recovery creates a new "epoch" of TLogs:
- `newEpoch()` recruits new TLogs and creates new log generation
- Old epochs kept until all storage servers have consumed their data
- `purgeOldRecoveredGenerations()` cleans up fully-consumed epochs
- Peek cursors can span multiple epochs during recovery

---

## Log Router -- [`LogRouter.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logrouter/LogRouter.cpp)

Routes mutations across regions in multi-datacenter deployments:

- Reads mutations from primary TLogs via peek cursor
- Forwards to remote region's TLogs
- Reduces cross-region traffic by batching
- Uses `tagLocalityLogRouter` tags
- Bridges tag-partitioned log system across datacenters

---

## Tag System

### Tag Structure

```
struct Tag {
    int8_t locality;   // identifies the shard location
    uint16_t id;       // identifies the specific storage server
};
```

### Special Tag Localities

| Locality | Purpose |
|----------|---------|
| `tagLocalitySpecial` | Special/system tags |
| `tagLocalityLogRouter` | Log router tags for cross-region |
| `tagLocalityRemoteLog` | Remote region log tags |
| `tagLocalityUpgraded` | Tags from older versions |
| `tagLocalitySS` | Standard storage server tags |

### Tag Assignment Flow

1. **Cluster controller** assigns tags to storage servers during recruitment
2. **Tags stored** in `serverTagKeys` system key range
3. **Commit proxy** maintains `keyInfo` map: key range → storage servers → tags
4. **During commit**: `tagsForKey(key)` looks up tags for each mutation's key
5. **TLog routing**: Tags attached to mutations; TLog stores per-tag queues
6. **Storage server**: Peeks its assigned tag to pull only relevant mutations

### Version Vector

Optional per-storage-server version tracking:
- Master maintains `VersionVector ssVersionVector`
- GRV proxy caches and sends delta in `GetReadVersionReply`
- Enables per-shard causal consistency without global ordering

---

## Data Flow: Mutation Lifecycle

```
1. Client commits transaction with mutations
2. CommitProxy assigns tags: mutation.key → tagsForKey() → [Tag1, Tag2, ...]
3. CommitProxy calls logSystem->push(mutations_with_tags)
4. LogSystem sends to all relevant TLog replicas
5. Each TLog:
   a. Routes mutation to TagData[tag.locality][tag.id].versionMessages
   b. Writes to DiskQueue for durability
   c. Acks to LogSystem
6. LogSystem waits for quorum → commit confirmed
7. Storage server (background):
   a. logSystem->peek(myTag, myVersion) → IPeekCursor
   b. cursor.getMore() → new mutations
   c. Apply mutations to local KVStore
   d. Advance version
   e. logSystem->pop(myVersion, myTag) → allow TLog cleanup
```

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/tlog/TLogServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/tlog/TLogServer.actor.cpp) | TLog server: push, peek, spill, DiskQueue |
| [`fdbserver/logsystem/TagPartitionedLogSystem.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logsystem/TagPartitionedLogSystem.cpp) | LogSystem: push/peek/pop across TLog replicas |
| [`fdbserver/core/include/fdbserver/core/LogSystem.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/LogSystem.h) | ILogSystem, IPeekCursor interfaces |
| [`fdbserver/core/include/fdbserver/core/LogSystemConfig.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/LogSystemConfig.h) | Log system configuration and epoch tracking |
| [`fdbserver/core/include/fdbserver/core/IDiskQueue.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/IDiskQueue.h) | IDiskQueue interface for TLog persistence |
| [`fdbserver/logrouter/LogRouter.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logrouter/LogRouter.cpp) | Cross-region mutation routing |
