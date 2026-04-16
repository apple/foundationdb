# Subsystem 7: Storage Server & Engines

**[Diagrams](diagram_07_storage_server.md)**

**Location:** [`fdbserver/storageserver/`](https://github.com/apple/foundationdb/tree/main/fdbserver/storageserver), [`fdbserver/kvstore/`](https://github.com/apple/foundationdb/tree/main/fdbserver/kvstore)
**Size:** ~63K  
**Role:** Serves reads, applies mutations from log, pluggable storage backends.

---

## Overview

Storage servers are the read path and the materialized state of the database. Each SS owns a set of key ranges (shards), continuously pulls committed mutations from the TLog via tagged peek cursors, applies them to a local key-value store, and serves client reads at specific versions.

---

## StorageServer Structure -- [`storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp)`:850-1528`

```
struct StorageServer : IStorageMetricsService {
    // MVCC data
    VersionedData versionedData;                         // versioned sets/clears
    std::map<Version, Standalone<VerUpdateRef>> mutationLog;  // (durableVersion, version]
    
    // Shard ownership
    KeyRangeMap<Reference<ShardInfo>> shards;            // key range → shard state
    KeyRangeMap<Version> newestAvailableVersion;         // key availability per version
    
    // Version tracking
    NotifiedVersion version;                             // current SS version
    NotifiedVersion desiredOldestVersion;                // target for advancing durable
    NotifiedVersion oldestVersion;                       // minimum readable version
    NotifiedVersion durableVersion;                      // committed to disk
    
    // Log system connection
    Reference<ILogSystem> logSystem;
    Reference<ILogSystem::IPeekCursor> logCursor;
    
    // Storage engine
    StorageServerDisk storage;                           // wraps IKeyValueStore
    
    // Move-in tracking
    std::unordered_map<UID, std::shared_ptr<MoveInShard>> moveInShards;
};
```

---

## Update Loop -- [`storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp)`:9535+`

The `update()` actor pulls mutations from the log system and applies them locally.

### Flow

1. **Emergency brake** (lines 9548-9583):
   - If `queueSize >= STORAGE_HARD_LIMIT_BYTES` and `durableVersion < desiredOldestVersion`: block
   - Prevents OOM from unbounded mutation accumulation

2. **Log cursor read** (lines 9609-9625):
   - `cursor->getMore()` -- wait for new mutations from TLog
   - Validate cursor hasn't been popped (would mean SS removed)

3. **Eager read collection** (lines 9646-9744):
   - Clone cursor, extract mutations to identify keys needing eager reads
   - `doEagerReads()` prefetches values for clear-range optimization
   - Handles shard changes during read, retries if needed

4. **Mutation application** (lines 9747-9860):
   - Create `StorageUpdater` for version transitions
   - For each mutation at each version:
     - `applyMutation(data, mutation, version)` -- update MVCC data
     - Advance `data->version` on new version boundary
     - Yield if accumulated bytes exceed `DESIRED_UPDATE_BYTES`

### Version Management

| Version | Meaning |
|---------|---------|
| `version` | Latest version SS knows about (from TLog stream) |
| `durableVersion` | Latest version committed to disk |
| `oldestVersion` | Minimum version readable from storage |
| `desiredOldestVersion` | Target set by data distributor for MVCC cleanup |

**Gap `(durableVersion, version]`**: Held in MVCC `mutationLog` (memory).  
**Gap `(oldestVersion, durableVersion]`**: On disk but not yet MVCC-freeable.

---

## Read Serving

### Point Read -- `getValueQ()` (lines 2174-2311)

1. **Priority management**: Acquire `PriorityMultiLock` with request priority
2. **Version wait**: `waitForVersion(data, commitVersion, req.version)` -- block until SS version >= requested
3. **Shard check**: `data->shards[key]->isReadable()` -- throw `wrong_shard_server()` if not owned
4. **Value read**:
   - Try MVCC data at version: `data->data().at(version).lastLessOrEqual(key)`
   - If not in MVCC: read from storage engine: `data->storage.readValue(key)`
   - Validate version hasn't become too old, shard ownership hasn't changed
5. **Response**: Return value with penalty metric

### Range Read -- `getKeyValuesQ()` (lines 3346-3555)

1. **Shard validation**: Ensure begin and end resolve within single shard
2. **Key selector resolution**: Resolve begin/end `KeySelector` to concrete keys via `findKey()`
3. **Range read**: `readRange(data, version, range, limit, &remainingBytes)`
4. **Shard change detection**: Double-check ownership via change counters before returning

---

## Shard Management

### ShardInfo (lines 448-600)

Each shard is in one of four states:

| State | Meaning |
|-------|---------|
| `readWrite` | Assigned, readable at storageVersion and later |
| `adding` | Being fetched from another SS (fetchKeys in progress) |
| `moveInShard` | Physical shard being moved via checkpoint |
| `notAssigned` | SS has no data for this range |

### AddingShard (lines 386-446)

Active data transfer state:
```
struct AddingShard {
    KeyRange keys;
    Future<Void> fetchClient;                  // fetchKeys() actor
    Promise<Void> fetchComplete, readWrite;
    std::deque<Standalone<VerUpdateRef>> updates;  // mutations during fetch
    enum Phase { WaitPrevious, Fetching, FetchingCF, Waiting };
    Version transferredVersion, fetchVersion;
};
```

### fetchKeys() Actor (lines 6812-7200)

Transfers shard data from source to destination SS:

1. **Setup**: Wait for `coreStarted`, version advancement, overlapping ranges durable
2. **Fetch preparation**: Acquire `fetchKeysParallelismLock`, set phase to `Fetching`
3. **Data fetch loop**:
   - Create transaction at `fetchVersion`
   - Read key-value blocks from source SS via `tryGetRange()`
   - Apply deferred mutations from `updates` deque
   - Write to storage: `data->storage.set()` for each KV pair
4. **Completion**: Set phase to `Waiting`, fire `fetchComplete`, update `newestAvailableVersion`

### changeServerKeys

Modifies shard map to move ranges between states:
- Updates `serverKeys` system namespace reflecting ownership
- Contexts: `CSK_UPDATE` (normal), `CSK_RESTORE` (recovery), `CSK_ASSIGN_EMPTY` (new), `CSK_FALL_BACK` (fallback)

---

## Storage Engines -- [`fdbserver/kvstore/`](https://github.com/apple/foundationdb/tree/main/fdbserver/kvstore)

### IKeyValueStore Interface

```
struct IKeyValueStore {
    void set(KeyValueRef, Arena*);
    void clear(KeyRangeRef, Arena*);
    Future<Void> commit(bool sequential=false);
    Future<Optional<Value>> readValue(KeyRef, Optional<ReadOptions>);
    Future<RangeResult> readRange(KeyRangeRef, rowLimit, byteLimit, ReadOptions);
    KeyValueStoreType getType() const;
    
    // Shard-aware operations (RocksDB sharded)
    bool shardAware() const;
    Future<Void> addRange(KeyRangeRef, string id, bool active);
    std::vector<string> removeRange(KeyRangeRef);
    
    // Checkpoints
    Future<CheckpointMetaData> checkpoint(CheckpointRequest);
    Future<Void> restore(checkpoints);
    Future<Void> ingestSSTFiles(BulkLoadFileSetKeyMap);
};
```

### RocksDB Engine -- [`KeyValueStoreRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp)

Primary production engine.

**Configuration** (`SharedRocksDBState`, lines 95-252):
- Write buffer size: `ROCKSDB_MEMTABLE_BYTES`
- Compaction triggers: `ROCKSDB_LEVEL0_FILENUM_COMPACTION_TRIGGER`
- Block cache: `ROCKSDB_BLOCK_CACHE_SIZE`
- Bloom filter: `ROCKSDB_BLOOM_BITS_PER_KEY` (typically 10)
- Direct I/O: `ROCKSDB_USE_DIRECT_READS`

**Sharded variant** ([`KeyValueStoreShardedRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreShardedRocksDB.actor.cpp)):
- Per-shard column families
- Better isolation between shards
- Supports physical shard operations (addRange/removeRange)

### SQLite Engine -- [`KeyValueStoreSQLite.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreSQLite.actor.cpp)

Legacy engine. Simple SQL-backed KV store.

### Memory Engine -- [`KeyValueStoreMemory.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreMemory.actor.cpp)

In-memory map-based engine:
- Used for testing
- Used for `txnStateStore` during recovery (ephemeral metadata)

---

## Data Flow Summary

```
TLog stream ──peek(myTag)──▶ StorageServer update() loop
                                │
                                ▼
                            Parse mutations from cursor
                                │
                                ▼
                            applyMutation() → MVCC versionedData
                                │
                                ▼
                            Advance version
                                │
                                ▼
                            updateStorage() → KVStore.commit() → durableVersion advances
                                │
                                ▼
                            pop(durableVersion, myTag) → TLog can discard old data


Client ──GetValueRequest──▶ StorageServer getValueQ()
                                │
                                ▼
                            waitForVersion(requestedVersion)
                                │
                                ▼
                            Check shard ownership
                                │
                                ▼
                            Read from MVCC or KVStore at version
                                │
                                ▼
                            Return value to client
```

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/storageserver/storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp) | SS main loop, update, read serving, shard management |
| [`fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp) | RocksDB engine implementation |
| [`fdbserver/kvstore/KeyValueStoreShardedRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreShardedRocksDB.actor.cpp) | Sharded RocksDB engine |
| [`fdbserver/kvstore/KeyValueStoreSQLite.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreSQLite.actor.cpp) | SQLite engine |
| [`fdbserver/kvstore/KeyValueStoreMemory.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreMemory.actor.cpp) | Memory engine |
| [`fdbserver/core/include/fdbserver/core/IKeyValueStore.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/IKeyValueStore.h) | IKeyValueStore interface |
| [`fdbserver/core/StorageMetrics.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/StorageMetrics.cpp) | Storage metrics tracking |
