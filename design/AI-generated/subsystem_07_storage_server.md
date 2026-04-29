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
    Reference<LogSystem> logSystem;
    Reference<IPeekCursor> logCursor;
    
    // Storage engine
    StorageServerDisk storage;                           // wraps IKeyValueStore
    
    // Move-in tracking
    std::unordered_map<UID, std::shared_ptr<MoveInShard>> moveInShards;
};
```

---

## StorageServerDisk Wrapper -- [`storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp)`:602-719`

`StorageServerDisk` is the thin layer between the storage server logic and the `IKeyValueStore` interface. It delegates all I/O to the underlying engine and adds metrics counters (`kvGets`, `kvScans`, `kvCommits`, `kvCommitLogicalBytes`, `kvClearRanges`, `kvClearSingleKey`).

Key methods:
- **`readValue(key)`** / **`readValuePrefix(key, maxLength)`** -- point reads delegated to `storage->readValue()`, incrementing `kvGets`
- **`readRange(keys, rowLimit, byteLimit)`** -- range reads delegated to `storage->readRange()`, incrementing `kvScans`
- **`readNextKeyInclusive(key)`** -- helper that does a `readRange(key, allKeys.end, rowLimit=1)` and returns the first key found (or `allKeys.end`)
- **`makeVersionMutationsDurable(prevVersion, newVersion, bytesLeft, ...)`** -- iterates the in-memory `mutationLog` and writes `set()`/`clear()` calls to the storage engine for each mutation
- **`commit()`** -- calls `storage->commit()` to make buffered writes durable
- **Checkpoint operations** -- `checkpoint()`, `restore()`, `deleteCheckpoint()` -- forwarded for physical shard movement

---

## Update Loop -- [`storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp)`:9562+`

The `update()` function (a coroutine) pulls mutations from the log system and applies them locally.

### Flow

1. **Emergency brake** (lines 9576-9610):
   - If `queueSize >= STORAGE_HARD_LIMIT_BYTES` and `durableVersion < desiredOldestVersion`: block
   - Prevents OOM from unbounded mutation accumulation
   - Tracks `lastBytesInputEBrake` and `lastDurableVersionEBrake` to allow progress between braking periods

2. **Log cursor read** (lines 9636-9651):
   - `cursor->getMore()` -- wait for new mutations from TLog
   - Validate cursor hasn't been popped (would mean SS removed via `worker_removed()`)

3. **Eager read collection** (lines 9674+):
   - Clone cursor, extract mutations to identify keys needing eager reads
   - `UpdateEagerReadInfo` collects keys that need pre-reading (for atomic ops like `AppendIfFits`, `ByteMin`/`ByteMax`, `CompareAndClear`, and `ClearRange` endpoint resolution)
   - `doEagerReads()` prefetches values from the storage engine
   - Handles shard changes during read, retries if needed

4. **Mutation application** (version loop):
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

## Durability Loop -- `updateStorage()` (line 10274)

The `updateStorage` actor runs continuously and flushes in-memory mutations to the storage engine.

### Flow

1. **Wait** until `desiredOldestVersion > storageVersion` (i.e., the data distributor has indicated it's safe to advance)
2. **Check pending operations** -- inspect `pendingCheckpoints` and clamp `desiredVersion` accordingly
3. **Write mutations** via `storage.makeVersionMutationsDurable()` which iterates the `mutationLog` from `storageVersion` to `desiredVersion`, calling `storage.set()` and `storage.clear()` for each mutation. This is bounded by `STORAGE_COMMIT_BYTES` (bytes limit) and for RocksDB also by `ROCKSDB_CLEARRANGES_LIMIT_PER_COMMIT`
4. **Advance `oldestVersion`** and asynchronously free MVCC data for versions before `newOldestVersion`
5. **Checkpoint handling** -- if a pending checkpoint version was reached, create the checkpoint via `storage.checkpoint()`
6. **`storage.commit()`** -- atomically make all the above mutations durable
7. **Advance `durableVersion`** and signal `durableInProgress` to unblock the update loop
8. **Pop** old TLog data via `data->logSystem->pop()` now that mutations are durable

### Persistent Version Tracking

Each storage engine stores a special key `\xff\xffVersion` (`persistVersion`) that records the durable version. This is written by `makeVersionDurable()` on every commit and read during `restoreDurableState()` on startup. On recovery, the storage server reads `persistVersion` from the engine to learn how far it had committed, then resumes pulling mutations from the TLog starting from that version. If `persistVersion` is absent, the database was never initialized and the storage server self-destructs.

### RocksDB-Specific Tuning

For RocksDB backends, `updateStorage()` has additional controls:
- **`ROCKSDB_CLEARRANGES_LIMIT_PER_COMMIT`** -- limits the number of `DeleteRange` operations per commit to avoid excessive compaction pressure
- **`ROCKSDB_ENABLE_CLEAR_RANGE_EAGER_READS`** -- enables pre-reading clear range endpoints for optimization
- **`canCommit()` backpressure** -- before committing, the RocksDB engine checks pending compaction bytes and immutable memtable count; if overloaded, it delays the commit

---

## Read Serving

### Point Read -- `getValueQ()` (line 2148)

1. **Priority management**: Acquire `PriorityMultiLock` with request priority
2. **Version wait**: `waitForVersion(data, commitVersion, req.version)` -- block until SS version >= requested
3. **Shard check**: `data->shards[key]->isReadable()` -- throw `wrong_shard_server()` if not owned
4. **Value read**:
   - Try MVCC data at version: `data->data().at(version).lastLessOrEqual(key)`
   - If not in MVCC: read from storage engine: `data->storage.readValue(key)`
   - Validate version hasn't become too old, shard ownership hasn't changed
5. **Response**: Return value with penalty metric

### Range Read -- `getKeyValuesQ()` (line 3314)

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

### fetchKeys() Actor (line 6850)

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

### IKeyValueStore Interface -- [`IKeyValueStore.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/include/fdbserver/kvstore/IKeyValueStore.h)

All storage engines implement the `IKeyValueStore` interface (which extends `IClosable`). This is the contract between the storage server and the underlying storage mechanism.

```
class IKeyValueStore : public IClosable {
public:
    // === Core mutation operations (buffered, not immediately durable) ===
    virtual void set(KeyValueRef keyValue, const Arena* arena = nullptr) = 0;
    virtual void clear(KeyRangeRef range, const Arena* arena = nullptr) = 0;
    
    // === Durability ===
    virtual Future<Void> canCommit() { return Void(); }  // backpressure hook
    virtual Future<Void> commit(bool sequential = false) = 0;  // atomically make prior set/clear durable
    
    // === Read operations (async) ===
    virtual Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions>) = 0;
    virtual Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<ReadOptions>) = 0;
    virtual Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit, int byteLimit, Optional<ReadOptions>) = 0;
    
    // === Metadata ===
    virtual KeyValueStoreType getType() const = 0;
    virtual StorageBytes getStorageBytes() const = 0;
    
    // === Checkpoints (physical shard movement) ===
    virtual Future<CheckpointMetaData> checkpoint(const CheckpointRequest& request) { throw not_implemented(); }
    virtual Future<Void> restore(const std::vector<CheckpointMetaData>& checkpoints) { throw not_implemented(); }
    virtual Future<Void> deleteCheckpoint(const CheckpointMetaData& checkpoint) { throw not_implemented(); }
    
    // === Bulk operations ===
    virtual Future<Void> compactRange(KeyRangeRef range) { throw not_implemented(); }
    virtual Future<Void> replaceRange(KeyRange range, Standalone<VectorRef<KeyValueRef>> data);
    virtual Future<Void> ingestSSTFiles(std::shared_ptr<BulkLoadFileSetKeyMap> localFileSets) { throw not_implemented(); }
    
    // === Lifecycle ===
    virtual Future<Void> init() { return Void(); }  // MUST be idempotent (may be called again on rollback)
};
```

#### Concurrency Contract

From the header comment:

> **Causal consistency**: A read which begins after a commit ends sees the effects of the commit. A read which ends before a commit begins does not see the effects of the commit.

In practice: reads return data as of a version established by the most recent `commit()` that completed before the read ends, such that no subsequent commit also completed before the read began. This means reads and writes can overlap, but reads always see a consistent committed snapshot.

#### ReadOptions

```
struct ReadOptions {
    ReadType type = ReadType::NORMAL;  // NORMAL or FETCH
    Optional<UID> debugID;
};
```

`ReadType::FETCH` is used during shard data movement (fetchKeys). The engine may apply different throttling or priority to fetch reads vs. normal client reads.

#### Factory -- [`IKeyValueStore.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/IKeyValueStore.cpp)

```
IKeyValueStore* openKVStore(KeyValueStoreType storeType, std::string filename, UID logID, ...) {
    switch (storeType) {
        case SSD_BTREE_V1:       return keyValueStoreSQLite(...);
        case SSD_BTREE_V2:       return keyValueStoreSQLite(...);
        case MEMORY:             return keyValueStoreMemory(...);
        case SSD_REDWOOD_V1:     return keyValueStoreRedwoodV1(...);
        case SSD_ROCKSDB_V1:     return keyValueStoreRocksDB(...);
        case MEMORY_RADIXTREE:   return keyValueStoreMemory(..., "fdr", MEMORY_RADIXTREE);
    }
}
```

---

### RocksDB Engine -- [`KeyValueStoreRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp) (~3K lines)

The primary production storage engine. Wraps Facebook's RocksDB LSM-tree engine, adapting it to the `IKeyValueStore` interface via thread pools for non-blocking I/O.

#### Architecture

```
RocksDBKeyValueStore (IKeyValueStore)
├── Writer (IThreadPoolReceiver, single thread)
│   ├── OpenAction        -- opens/creates the RocksDB database
│   ├── CommitAction      -- writes accumulated WriteBatch to RocksDB
│   ├── CloseAction       -- closes the database
│   ├── CheckpointAction  -- creates a RocksDB checkpoint
│   ├── RestoreAction     -- restores from checkpoint
│   ├── CompactRangeAction
│   └── IngestSSTFilesAction
├── Reader (IThreadPoolReceiver, N parallel threads)
│   ├── ReadValueAction
│   ├── ReadValuePrefixAction
│   └── ReadRangeAction
├── SharedRocksDBState    -- holds RocksDB options configured from knobs
├── ReadIteratorPool      -- recycles rocksdb::Iterator objects across reads
└── FlowLock semaphores   -- readSemaphore, fetchSemaphore for backpressure
```

In simulation, the reader/writer threads use `CoroThreadPool` (running on the network thread) to avoid time-advancement issues. In production, they use real OS threads.

#### Write Path

**`set(KeyValueRef kv)`** (line 2183):
1. Lazy-initializes a `rocksdb::WriteBatch` (with configurable per-key protection bytes)
2. Calls `writeBatch->Put(columnFamily, key, value)`
3. If `ROCKSDB_SINGLEKEY_DELETES_ON_CLEARRANGE` is enabled, tracks the key in `keysSet` for later delete optimization

**`clear(KeyRangeRef range)`** (line 2202):
Three strategies depending on knobs and range shape:
1. **Single-key delete** -- if `range.singleKeyRange()` and not `ROCKSDB_FORCE_DELETERANGE_FOR_CLEARRANGE`: `writeBatch->Delete(key)`. This is preferred because RocksDB handles point tombstones more efficiently than range tombstones.
2. **Converted deletes** -- if `ROCKSDB_SINGLEKEY_DELETES_ON_CLEARRANGE` is enabled and the delete budget (`maxDeletes`) hasn't been exhausted: iterate the range using a RocksDB iterator and emit individual `Delete()` calls for each key found. Also deletes any keys in `keysSet` and `previousCommitKeysSet` that fall within the range. Falls back to `DeleteRange()` if iteration fails or budget exhausted.
3. **Native `DeleteRange()`** -- `writeBatch->DeleteRange(begin, end)`. Used as the default when conversion is disabled.

**`commit()`** (line 2308, via `commitInRocksDB`):
1. If `writeBatch` is null (no mutations), returns immediately
2. Creates a `Writer::CommitAction` and transfers ownership of the `WriteBatch`
3. Records delete histograms, moves `keysSet` to `previousCommitKeysSet`
4. Posts the action to `writeThread`
5. The writer thread calls `rocksdb::DB::Write(writeOptions, writeBatch)` -- this atomically persists all buffered mutations

**`canCommit()` backpressure** (line 2282):
Before each commit, the storage server calls `canCommit()` which invokes `checkRocksdbState()`. This checks:
- `EstimatePendingCompactionBytes` > `ROCKSDB_CAN_COMMIT_COMPACT_BYTES_LIMIT`
- `NumImmutableMemTable` >= `ROCKSDB_CAN_COMMIT_IMMUTABLE_MEMTABLES_LIMIT`

If overloaded, it delays up to `ROCKSDB_CAN_COMMIT_DELAY_TIMES_ON_OVERLOAD * ROCKSDB_CAN_COMMIT_DELAY_ON_OVERLOAD` seconds, rechecking each iteration.

#### Read Path

**`readValue(KeyRef key, ReadOptions)`** (line 2365):
1. Extract `ReadType` (NORMAL or FETCH) from options
2. If the key is a system key or an eager read, skip throttling and post directly to `readThreads`
3. Otherwise, check `FlowLock` waiter count; throw `server_overloaded()` if too many waiters
4. Acquire `FlowLock` semaphore with timeout (`ROCKSDB_READ_QUEUE_WAIT`)
5. Post `ReadValueAction` to `readThreads`; the reader thread calls `rocksdb::DB::Get()`
6. Return the result future

**`readRange(KeyRangeRef keys, rowLimit, byteLimit, ReadOptions)`** (line 2433):
Similar throttling path. The reader thread:
1. Gets an iterator from `ReadIteratorPool` (reuse if configured, or create new with bounded key range)
2. For forward reads: `Seek(begin)`, iterate with `Next()` until `end`/limit
3. For reverse reads: `SeekForPrev(end)`, iterate with `Prev()` until `begin`/limit
4. Enforces read timeouts (`ROCKSDB_SET_READ_TIMEOUT`) and returns `transaction_too_old` if exceeded
5. Returns the iterator to the pool for potential reuse

#### ReadIteratorPool (line 539)

Manages a pool of `rocksdb::Iterator` objects to avoid the cost of creating/destroying them for every read:
- **On read**: provides an unused iterator if available (matching key range for bounded iterators), or creates a new one
- **On commit**: marks all iterators as stale (increments `deletedUptoIndex`), since the underlying data has changed
- **Periodic refresh**: a background actor (`refreshReadIteratorPool`) periodically removes stale and expired iterators
- **Bounded iterators**: when `ROCKSDB_READ_RANGE_REUSE_BOUNDED_ITERATORS` is enabled, iterators are created with `iterate_lower_bound`/`iterate_upper_bound` set, and can only be reused for sub-ranges
- **Pool size limit**: `ROCKSDB_READ_RANGE_BOUNDED_ITERATORS_MAX_LIMIT` caps the number of cached iterators to avoid memory pressure

#### Error Handling

`RocksDBErrorListener` (an `rocksdb::EventListener`) catches background errors from compaction and flush:
- IO errors → `io_error()`
- Corruption → `file_corrupt()`
- Other → `unknown_error()`

These errors are forwarded to the storage server via a promise, causing the SS to shut down and be re-recruited.

#### Configuration (Knobs)

`SharedRocksDBState` constructs RocksDB options from server knobs. Key settings:

| Category | Knobs |
|----------|-------|
| **Write buffers** | `ROCKSDB_MEMTABLE_BYTES`, `ROCKSDB_WRITE_BUFFER_SIZE`, `ROCKSDB_MAX_WRITE_BUFFER_NUMBER`, `ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE` |
| **Compaction** | `ROCKSDB_LEVEL0_FILENUM_COMPACTION_TRIGGER`, `ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER`, `ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER`, `ROCKSDB_PERIODIC_COMPACTION_SECONDS` (with random 2-day jitter), `ROCKSDB_MAX_COMPACTION_BYTES`, `ROCKSDB_MAX_BYTES_FOR_LEVEL_MULTIPLIER`, `ROCKSDB_COMPACTION_PRI` |
| **Block cache** | `ROCKSDB_BLOCK_CACHE_SIZE` (LRU cache), `ROCKSDB_CACHE_HIGH_PRI_POOL_RATIO`, `ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS` |
| **Bloom filter** | `ROCKSDB_BLOOM_BITS_PER_KEY` (typically 10, ~1% false positive rate), `ROCKSDB_PREFIX_LEN` (fixed prefix for prefix bloom), `ROCKSDB_BLOOM_WHOLE_KEY_FILTERING` |
| **I/O** | `ROCKSDB_USE_DIRECT_READS`, `ROCKSDB_USE_DIRECT_IO_FLUSH_COMPACTION`, `ROCKSDB_BACKGROUND_PARALLELISM`, `ROCKSDB_MAX_SUBCOMPACTIONS`, `ROCKSDB_MAX_OPEN_FILES` |
| **Protection** | `ROCKSDB_MEMTABLE_PROTECTION_BYTES_PER_KEY`, `ROCKSDB_BLOCK_PROTECTION_BYTES_PER_KEY`, `ROCKSDB_PARANOID_FILE_CHECKS`, `ROCKSDB_FULLFILE_CHECKSUM` (CRC32c) |
| **Write rate** | `ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC`, `ROCKSDB_WRITE_RATE_LIMITER_FAIRNESS`, `ROCKSDB_WRITE_RATE_LIMITER_AUTO_TUNE` |
| **Deletion optimization** | `ROCKSDB_ENABLE_COMPACT_ON_DELETION` (triggers compaction when tombstone density is high in a sliding window: `ROCKSDB_CDCF_SLIDING_WINDOW_SIZE`, `ROCKSDB_CDCF_DELETION_TRIGGER`, `ROCKSDB_CDCF_DELETION_RATIO`), `ROCKSDB_SINGLEKEY_DELETES_ON_CLEARRANGE`, `ROCKSDB_SINGLEKEY_DELETES_MAX` |
| **Read throttling** | `ROCKSDB_READ_QUEUE_SOFT_MAX`, `ROCKSDB_READ_QUEUE_HARD_MAX`, `ROCKSDB_FETCH_QUEUE_SOFT_MAX`, `ROCKSDB_FETCH_QUEUE_HARD_MAX`, `ROCKSDB_READ_QUEUE_WAIT` |
| **Read parallelism** | `ROCKSDB_READ_PARALLELISM` (number of reader threads), `ROCKSDB_WRITER_THREAD_PRIORITY`, `ROCKSDB_READER_THREAD_PRIORITY` |

#### Observability

Comprehensive metrics are emitted via `rocksDBMetricLogger` (line 1008):
- **RocksDB ticker stats**: block cache hits/misses, bloom filter stats, bytes read/written, compaction bytes, WAL syncs, memtable hits, iterator counts
- **RocksDB histogram stats** (when `ROCKSDB_STATS_LEVEL > kExceptHistogramOrTimers`): compaction time, compression time, write stall duration
- **RocksDB property stats**: live SST size, pending compaction bytes, memtable counts, snapshot counts, estimated key count
- **FDB-side histograms**: commit latency, read value/range latency, queue wait times, deletes per commit
- **PerfContext metrics** (when `ROCKSDB_PERFCONTEXT_ENABLE`): per-thread breakdown of block reads, seeks, memtable operations, environment operations

---

### Redwood (VersionedBTree) Engine -- [`VersionedBTree.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/VersionedBTree.actor.cpp) (~11K lines)

A custom versioned copy-on-write B-tree storage engine built from scratch for FoundationDB. Named "Redwood," it is designed for high space efficiency through delta compression and for tight integration with FDB's versioned storage model.

#### Architecture

```
KeyValueStoreRedwood (IKeyValueStore)
└── VersionedBTree
    ├── IPager2* m_pager              -- DWALPager instance (physical page I/O)
    ├── MutationBuffer* m_pBuffer     -- in-memory pending mutations (ART-based)
    ├── BTreeCommitHeader m_header    -- persistent root pointer + metadata
    ├── LazyClearQueue                -- deferred page deletions (FIFO)
    └── BTreeCursor                   -- read cursor for tree traversal

DWALPager (IPager2)
├── PageCacheT pageCache              -- LRU page cache (ObjectCache<LogicalPageID, PageCacheEntry>)
├── PageToVersionedMapT remappedPages -- logical→physical page remapping for COW
├── DelayedFreePageQueueT             -- pages freed at a version (deferred reclamation)
├── RemapQueueT                       -- remap cleanup queue
├── PriorityMultiLock ioLock          -- I/O prioritization
└── IAsyncFile* pageFile              -- the underlying data file

BTreePage (in-memory representation of a disk page)
├── height (1 = leaf, >1 = internal)
├── kvBytes (total uncompressed KV bytes)
└── DeltaTree2<RedwoodRecordRef> tree -- delta-compressed sorted record storage
```

#### KeyValueStoreRedwood (line 7609)

The `IKeyValueStore` adapter. Constructor creates a `DWALPager` and wraps it in a `VersionedBTree`.

Note: Although `VersionedBTree` supports multi-version reads via the pager's snapshot mechanism, `KeyValueStoreRedwood` currently only keeps one version -- each `commit()` immediately sets `oldestReadableVersion` to the commit version, discarding all prior versions. All reads go against `getLastCommittedVersion()`. This means the storage server's MVCC layer (in-memory `versionedData`) provides version history, not the storage engine itself.

```cpp
KeyValueStoreRedwood(filename, logID, db, encodingType, pageCacheBytes) {
    IPager2* pager = new DWALPager(pageSize, extentSize, filename, pageCacheBytes,
                                   remapCleanupWindowBytes, concurrentExtentReads, false);
    m_tree = new VersionedBTree(pager, filename, logID, db);
}
```

Key parameters:
- **`pageSize`**: `REDWOOD_DEFAULT_PAGE_SIZE` (default ~4096), BUGGIFied in simulation to random values 1000-16384
- **`extentSize`**: `REDWOOD_DEFAULT_EXTENT_SIZE` (multiple pages per I/O for large sequential reads)
- **`pageCacheBytes`**: configurable via `PAGE_CACHE_4K` knob (or `SIM_PAGE_CACHE_4K` in simulation)
- **`remapCleanupWindowBytes`**: `REDWOOD_REMAP_CLEANUP_WINDOW_BYTES` (100MB default), controls how much remap history to keep before cleaning up; BUGGIFied in simulation

#### Write Path

**`set(KeyValueRef kv)`** / **`clear(KeyRangeRef range)`** -- delegated directly to `VersionedBTree`:

```cpp
// VersionedBTree::set() (line 4916)
void set(KeyValueRef keyValue) {
    ++m_mutationCount;
    m_pBuffer->insert(keyValue.key)
        .mutation().setBoundaryValue(m_pBuffer->copyToArena(keyValue.value));
}

// VersionedBTree::clear() (line 4924)
void clear(KeyRangeRef clearedRange) {
    ++m_mutationCount;
    if (clearedRange.singleKeyRange()) {
        m_pBuffer->insert(clearedRange.begin).mutation().clearBoundary();
    } else {
        auto iBegin = m_pBuffer->insert(clearedRange.begin);
        auto iEnd = m_pBuffer->insert(clearedRange.end);
        iBegin.mutation().clearAll();
        ++iBegin;
        m_pBuffer->erase(iBegin, iEnd);
    }
}
```

Mutations are buffered in `MutationBuffer` (either an ART (Adaptive Radix Tree) implementation for performance, or `std::map`-based fallback). The buffer represents pending mutations as a sorted map from key boundaries to `RangeMutation` structures, which track whether the boundary is set/cleared and whether the range after it is cleared.

**`commit(Version v)`** (line 5168):
1. Swaps the current `MutationBuffer` with a fresh one (so new mutations can arrive during commit)
2. Creates a `CommitBatch` with the mutations, read version (from pager), and write version
3. Takes a snapshot for reading the current tree state
4. Walks the B-tree, merging buffered mutations with existing pages:
   - Reads existing leaf pages
   - Applies mutations (sets, clears) to produce updated records
   - Rebuilds modified pages using `PageToBuild` to calculate optimal page splits
   - Writes modified pages via `m_pager->updatePage()` or `atomicUpdatePage()` (copy-on-write)
   - Propagates changes up through internal pages as needed
5. Runs lazy clear to free pages from deleted subtrees
6. Updates `BTreeCommitHeader` with new root pointer and lazy delete queue state
7. Calls `m_pager->commit(version, commitRecord)` to make everything durable

#### Read Path

**`readValue(key)`** (line 7879):
1. Initialize a `BTreeCursor` at the last committed version
2. `cursor.seekGTE(key)` -- traverse the B-tree from root to leaf
3. If the cursor lands on the exact key, return the value (with arena dependency on the page buffer)
4. Otherwise return `Optional<Value>()`

**`readRange(keys, rowLimit, byteLimit)`** (line 7738):
1. Initialize `BTreeCursor` at last committed version
2. For forward reads: `seekGTE(keys.begin)`, optionally `prefetch(keys.end, ...)`
3. Walk through leaf pages using the `BTreePage::BinaryTree::Cursor` directly (no async waits needed within a leaf page)
4. For each leaf: check whether the entire leaf is within the query range to skip per-key bounds checking
5. Accumulate results until `rowLimit` or `byteLimit` is reached
6. When a leaf is exhausted, `popPath()` and `moveNext()` to traverse to the next leaf (this may require async page reads)
7. For reverse reads: `seekLT(keys.end)`, traverse with `movePrev()`

The `prefetch()` mechanism pre-reads pages along the expected traversal path, reducing latency for sequential scans.

#### DWALPager (Double Write-Ahead Log Pager) (line 1934)

The physical page storage layer for Redwood. Manages pages on disk with copy-on-write versioning.

**Key concepts:**
- **Logical vs. Physical pages**: The B-tree operates on logical page IDs. The pager maintains a mapping (`PageToVersionedMapT`) from logical to physical page IDs, which changes when a page is updated (copy-on-write). Old physical pages are freed at the version they were superseded.
- **Page cache**: An LRU `ObjectCache<LogicalPageID, PageCacheEntry>` that caches pages in memory. Each entry tracks read and write futures and is evictable when both are complete.
- **Header pages**: Two header pages (primary at physical page 0, backup at page 1) store the pager's metadata. They are alternately updated for crash safety.
- **Recovery**: On startup, `recover()` reads header pages, validates checksums, and reconstructs the page mapping from the remap queue.

**Key operations:**
- `updatePage(logicalPageIDs, data)` -- writes a page, potentially to a new physical location (copy-on-write)
- `atomicUpdatePage(logicalPageID, data, version)` -- creates a new physical page and remaps the logical ID to it at the given version
- `readPage(physicalPageID)` -- reads a page from cache or disk
- `newPageID()` -- allocates a new logical page ID from the free list
- `freePage(logicalPageID, version)` -- schedules a page for deferred deletion at the given version
- `commit(version, commitRecord)` -- flushes all pending writes, updates the header, and syncs to disk

**Queues:**
- `DelayedFreePageQueueT` -- pages whose physical storage can be reclaimed after `oldestReadableVersion` advances past them
- `RemapQueueT` -- tracks logical→physical remappings that need cleanup; entries have types: `REMAP` (logical moved to new physical), `FREE` (logical page freed), `DETACH` (logical page detached from remap tracking)
- `LazyClearQueueT` (in VersionedBTree) -- subtrees that need their pages freed; processed incrementally between commits by `incrementalLazyClear()`

#### BTreePage and DeltaTree Compression (line 4566)

Each B-tree page stores its records in a `DeltaTree2<RedwoodRecordRef>` -- a sorted, delta-compressed structure optimized for space efficiency.

**RedwoodRecordRef** (line 4106):
```
struct RedwoodRecordRef {
    KeyRef key;
    Optional<ValueRef> value;  // absent = tombstone (cleared key)
    // For internal pages, value = child page IDs (BTreeNodeLinkRef)
};
```

**Delta encoding**: Each record is stored as a delta relative to a reference record (typically the previous sibling in the tree). The `Delta` structure uses a flags byte and variable-length fields:
- **Prefix borrowing**: Keys share a common prefix with a reference record (left or right neighbor). The delta stores only the differing suffix.
- **4 length formats** (encoded in 2 bits of the flags byte): 3, 4, 6, or 7 bytes of length fields, supporting keys and values from small (<256 byte) to large (multi-GB)
- **Flags**: `PREFIX_SOURCE_PREV` (borrow from left vs. right ancestor), `IS_DELETED` (tombstone), `HAS_VALUE` (value present)

This typically achieves 50-70% space savings over raw key-value storage for workloads with key locality.

**Page structure:**
- `height`: 1 = leaf (stores user key-value pairs), >1 = internal (stores separator keys → child page ID pointers)
- `kvBytes`: total uncompressed KV bytes on the page (for metrics)
- The rest of the page is the `DeltaTree2` binary data

**Page building** (`PageToBuild`, line 5469): During commits, records are packed into pages with awareness of delta sizes, block boundaries, and page utilization. The `shiftItem()` method rebalances records between adjacent pages to improve space efficiency.

#### Lazy Clear Queue

When a subtree is deleted (e.g., via a range clear that removes an entire internal page's worth of data), the internal page is pushed onto the `LazyClearQueue` rather than immediately freeing all descendant pages. The `incrementalLazyClear()` actor (line 4974) processes this queue between commits:
1. Pop entries from the queue in batches of `REDWOOD_LAZY_CLEAR_BATCH_SIZE_PAGES`
2. For each entry, read the page and iterate its children:
   - Height 2 pages: free leaf children directly (`freeBTreePage`)
   - Height >2 pages: re-queue children for later processing
3. Free the page itself
4. Stop when the queue is exhausted or `REDWOOD_LAZY_CLEAR_MAX_PAGES` have been freed

This amortizes the cost of large deletions across multiple commits.

#### BTreeCommitHeader (line 4862)

Persistent metadata stored in the pager's commit record:
```
struct BTreeCommitHeader {
    uint32_t formatVersion;      // currently FORMAT_VERSION = 17
    EncodingType encodingType;   // page encoding (XXHash64, etc.)
    uint8_t height;              // tree height
    LazyClearQueueT::QueueState lazyDeleteQueue;  // lazy clear queue state
    BTreeNodeLink root;          // root page ID(s) -- may be multi-page for oversized root
};
```

---

### SQLite Engine -- [`KeyValueStoreSQLite.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreSQLite.cpp) (~2.4K lines)

The original FDB storage engine, using a modified embedded SQLite B-tree. Available as `SSD_BTREE_V1` (`.fdb` files) and `SSD_BTREE_V2` (`.sqlite` files, with checksums and an improved page format). This is a legacy engine; RocksDB and Redwood are the active production engines.

#### Architecture

```
KeyValueStoreSQLite (IKeyValueStore)
├── Writer (IThreadPoolReceiver, single CoroThreadPool thread)
│   ├── InitAction       -- opens SQLite DB, runs checkpoint, optional integrity check
│   ├── SetAction         -- cursor->set(kv)
│   ├── ClearAction       -- cursor->fastClear(range) + cursor->clear(range)
│   ├── CommitAction      -- cursor->commit(), fullCheckpoint()
│   └── SpringCleaningAction -- lazy page deletion + vacuuming
├── Reader (IThreadPoolReceiver, SQLITE_READER_THREADS CoroThreadPool threads)
│   ├── ReadValueAction
│   ├── ReadValuePrefixAction
│   └── ReadRangeAction
├── ReadCursor[]          -- per-reader cursor, recycled up to SQLITE_CURSOR_MAX_LIFETIME_BYTES
├── PageChecksumCodec     -- per-page CRC32c checksums (V2 only)
└── VFSAsync              -- async VFS layer bridging SQLite to FDB's IAsyncFile
```

Like RocksDB, all I/O runs on `CoroThreadPool` threads (coroutine-based, running on the network thread). The writer thread handles all mutations and commits; multiple reader threads handle reads in parallel.

#### Write Path

**`set(KeyValueRef kv)`** (line 2231):
Posts a `Writer::SetAction` to the write thread. The writer calls `cursor->set(kv)` on the SQLite B-tree cursor. Before each set, `checkFreePages()` runs lazy deletion if the free list is below `CHECK_FREE_PAGE_AMOUNT`.

**`clear(KeyRangeRef range)`** (line 2235):
Posts a `Writer::ClearAction`. The writer calls both:
1. `cursor->fastClear(range)` -- marks B-tree pages as pending lazy deletion (added to the "free table") without immediately reclaiming them
2. `cursor->clear(range)` -- removes the key range from the B-tree index

**`commit()`** (line 2239):
Posts a `Writer::CommitAction`. The writer:
1. Calls `cursor->commit()` to finalize the SQLite transaction
2. Destroys the cursor (releasing the read transaction)
3. Calls `fullCheckpoint()` -- resets all reader cursors, then runs `conn.checkpoint()` twice (once to flush WAL to the main DB file, once to reset the WAL). This ensures the WAL file is truncated after every commit.
4. Creates a new write cursor for the next batch of mutations

#### Read Path

Reads are posted directly to the reader thread pool without throttling (unlike RocksDB):
- **`readValue(key)`** -- `getCursor()->get(key)` on the SQLite B-tree
- **`readValuePrefix(key, maxLength)`** -- `getCursor()->getPrefix(key, maxLength)`
- **`readRange(keys, rowLimit, byteLimit)`** -- `getCursor()->getRange(keys, rowLimit, byteLimit)`

Each reader thread holds a `ReadCursor` that is recycled after `SQLITE_CURSOR_MAX_LIFETIME_BYTES` of data read. All reader cursors are invalidated on each commit via `resetReaders()` in `fullCheckpoint()`.

#### Spring Cleaning

A background `cleanPeriodically()` actor runs `SpringCleaningAction` on the writer thread to reclaim space:
1. **Lazy deletion** -- processes the "free table" (B-tree pages marked for deletion by `fastClear`), freeing them in batches of `SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE`. Bounded by time (`SPRING_CLEANING_LAZY_DELETE_TIME_ESTIMATE`) and page count (`SPRING_CLEANING_MIN/MAX_LAZY_DELETE_PAGES`).
2. **Vacuuming** -- calls `conn.vacuum()` to compact the database file by moving pages from the end to free slots, enabling file truncation. Bounded by `SPRING_CLEANING_MIN/MAX_VACUUM_PAGES` and `SPRING_CLEANING_VACUUM_TIME_ESTIMATE`.
3. The relative frequency of lazy deletion vs. vacuuming is controlled by `SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE`.

The cleaning interval adapts: `SPRING_CLEANING_LAZY_DELETE_INTERVAL` if there's more work to do, `SPRING_CLEANING_NO_ACTION_INTERVAL` if idle.

#### Page Checksums (V2)

`PageChecksumCodec` computes an 8-byte checksum (two 32-bit halves: `part1` and `part2`) stored in the reserved bytes at the end of each SQLite page.

**Writes** always use xxHash3 (with `part1`'s top 8 bits zeroed as a format marker).

**Reads** try three algorithms in order to handle pages written by older code:
1. **CRC32c** -- tried first if `part1 == 0` (legacy format)
2. **xxHash3** -- tried next if `part1`'s top 8 bits are zero (current format)
3. **hashlittle2** -- fallback for the oldest format (seeded with page number)

If all three fail, the page is corrupt. The V1 format (`SSD_BTREE_V1`) does not use page checksums.

#### Integrity Checking

On open, optional integrity checks can be performed:
- **`checkAllChecksumsOnOpen`** -- scans every page and validates checksums; throws `file_corrupt()` on mismatch
- **`checkIntegrityOnOpen`** -- runs a full B-tree structural integrity check via `conn.check()`

A standalone `KVFileCheck()` utility function (line 2311) can validate `.fdb`/`.sqlite` files offline, and `KVFileDump()` can dump all key-value pairs.

---

### Memory Engine -- [`KeyValueStoreMemory.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreMemory.cpp) (~960 lines)

In-memory key-value store backed by a [`DiskQueue`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/DiskQueue.cpp) (append-only circular log) for durability:
- All mutations are logged to `.fdq` files (a pair of alternating files, checksummed with xxhash3)
- On recovery, the DiskQueue log is replayed to reconstruct the in-memory state
- Periodic snapshots write the full state to the log so older entries can be reclaimed
- Two backing containers: `IKeyValueContainer` (std::map-like, default) or `radix_tree` (`MEMORY_RADIXTREE` type)

**Users:**
- **Coordinator Paxos state** -- `OnDemandStore` creates a `KeyValueStoreMemory` per coordinator to persist generation register state (see [Cluster Controller & Coordination](subsystem_04_cluster_controller.md#coordinator-storage))
- **Transaction log mutation queues** -- TLogs use `KeyValueStoreMemory` with a DiskQueue for their durable mutation log
- **`txnStateStore`** during recovery -- ephemeral metadata reconstructed from TLog data
- **Testing** -- in-memory storage engine for simulation tests

---

## Data Flow Summary

```
TLog stream ──peek(myTag)──▶ StorageServer update() loop
                                │
                                ▼
                            Parse mutations from cursor
                                │
                                ▼
                          Eager reads (prefetch for atomic ops)
                                │
                                ▼
                            applyMutation() → MVCC versionedData
                                │
                                ▼
                            Advance version
                                │
                                ▼
                        updateStorage() → makeVersionMutationsDurable()
                                │
                                ▼
                        storage.set()/clear() → engine-specific write batch
                                │
                                ▼
                        canCommit() (RocksDB backpressure check)
                                │
                                ▼
                        storage.commit() → durableVersion advances
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

## Engine Comparison

| Aspect | RocksDB | Redwood | SQLite | Memory |
|--------|---------|---------|--------|--------|
| **Type** | LSM-tree | Copy-on-write B-tree | Modified SQLite B-tree | In-memory + WAL |
| **Store types** | `SSD_ROCKSDB_V1` | `SSD_REDWOOD_V1` | `SSD_BTREE_V1`, `SSD_BTREE_V2` | `MEMORY`, `MEMORY_RADIXTREE` |
| **Write amplification** | Higher (compaction) | Lower (in-place COW) | Low (WAL + checkpoint) | None (append-only log) |
| **Space amplification** | Moderate (dead data in LSM levels) | Low (delta compression) | Moderate (free-list fragmentation) | N/A (RAM-bound) |
| **Read amplification** | Moderate (bloom filter helps) | Low (direct B-tree traversal) | Low (direct B-tree lookup) | None (direct map lookup) |
| **Compression** | RocksDB block compression | DeltaTree prefix/suffix sharing | None | None |
| **Checkpoints** | Native RocksDB checkpoints | Not implemented | Not implemented | Not implemented |
| **Page checksums** | Block protection bytes | XXHash64 page encoding | xxHash3 per-page (V2 only; reads also accept CRC32c and hashlittle2 legacy formats) | N/A |
| **Threading** | Dedicated read/write thread pools | Flow event loop (async page I/O) | CoroThreadPool (read + write) | Single-threaded |
| **Space reclamation** | Background compaction | Lazy clear queue + remap cleanup | Spring cleaning (lazy delete + vacuum) | DiskQueue log reclamation |
| **Primary use** | Production storage servers | Alternative storage engine | Legacy (original engine) | Coordinators, TLogs, testing |

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/storageserver/storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp) | SS main loop, update, read serving, shard management |
| [`fdbserver/kvstore/include/fdbserver/kvstore/IKeyValueStore.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/include/fdbserver/kvstore/IKeyValueStore.h) | IKeyValueStore interface definition |
| [`fdbserver/kvstore/IKeyValueStore.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/IKeyValueStore.cpp) | Factory function `openKVStore()` |
| [`fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp) | RocksDB engine implementation |
| [`fdbserver/kvstore/VersionedBTree.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/VersionedBTree.actor.cpp) | Redwood engine (VersionedBTree + DWALPager) |
| [`fdbserver/kvstore/IPager.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/IPager.h) | Pager interface (IPager2) for Redwood |
| [`fdbserver/kvstore/DeltaTree.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/DeltaTree.h) | DeltaTree2 delta-compressed sorted structure |
| [`fdbserver/kvstore/KeyValueStoreSQLite.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreSQLite.cpp) | SQLite engine (V1/V2) |
| [`fdbserver/kvstore/KeyValueStoreMemory.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreMemory.cpp) | Memory engine |
| [`fdbserver/kvstore/DiskQueue.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/DiskQueue.cpp) | Append-only circular log (for Memory engine durability) |
