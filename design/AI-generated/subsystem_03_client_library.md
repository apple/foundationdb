# Subsystem 3: Client Library

**[Diagrams](diagram_03_client_library.md)**

**Location:** [`fdbclient/`](https://github.com/apple/foundationdb/tree/main/fdbclient)
**Size:** ~75K implementation, ~34K headers  
**Role:** Everything an application (or internal FDB component) uses to read and write data.

---

## Overview

The client library provides a layered transaction API. From outside in: C API / language bindings -> MultiVersionTransaction -> ReadYourWritesTransaction -> NativeAPI Transaction. Most internal FDB code uses ReadYourWritesTransaction. External applications go through the C API.

---

## Transaction Layers

### Layer 1: NativeAPI / Transaction -- `NativeAPI.actor.h:311-525`

The raw transaction. Directly communicates with proxies and storage servers.

**TransactionState** (`NativeAPI.actor.h:255-309`):
```
- Database cx                    // DatabaseContext reference
- Future<Version> readVersionFuture
- TransactionOptions options
- int64_t totalCost             // accumulated operation cost for throttling
- Version committedVersion      // set after successful commit
```

**Key methods:**
- `getReadVersion()` -- fetches version from GRV proxy (batched via versionBatcher)
- `get(key, Snapshot)` -- point read from storage server, adds read conflict range if !snapshot
- `getRange(begin, end, limits, Snapshot, Reverse)` -- range scan from storage server(s)
- `set(key, value)` -- appends `MutationRef(SetValue, key, value)` to mutations vector
- `clear(range)` -- appends `MutationRef(ClearRange, begin, end)`
- `commit()` -- sends `CommitTransactionRequest` to commit proxy via `tryCommit()`
- `onError(error)` -- handles retryable errors, resets transaction, implements backoff

### Layer 2: ReadYourWritesTransaction -- `ReadYourWrites.h:69-268`

Adds read-your-own-writes semantics on top of NativeAPI:

```
class ReadYourWritesTransaction {
    Arena arena;
    Transaction tr;              // underlying native transaction
    SnapshotCache cache;         // cached reads from storage servers
    WriteMap writes;             // pending local writes
    CoalescedKeyRefRangeMap<bool> readConflicts;
};
```

**RYW read flow:**
1. Check `writes` (local write map) for the key
2. If found: return local value (or "cleared" indicator)
3. If not found: check `cache` (snapshot cache) for previously-read value
4. If not cached: issue read to storage server, cache result
5. Merge results using `RYWIterator` which walks both maps in parallel

**RYW write flow:**
- `set()`/`clear()` buffer in local `WriteMap`
- On `commit()`: flush all writes to underlying `Transaction`, then commit

### Layer 3: MultiVersionTransaction -- [`MultiVersionTransaction.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/MultiVersionTransaction.cpp)

Supports running against clusters with different protocol versions:

- **DLTransaction**: Wraps C API function pointers from dynamically-loaded client library
- **MultiVersionApi**: Loads multiple client `.so` files for protocol compatibility
- Converts between `ThreadFuture<T>` (cross-thread) and internal `Future<T>`
- Enables rolling upgrades with mixed-version clusters

### Layer 4: C API / Language Bindings -- [`fdb_c.h`](https://github.com/apple/foundationdb/blob/main/bindings/c/foundationdb/fdb_c.h)

FFI boundary for external languages:
- `fdb_setup_network()` / `fdb_run_network()` / `fdb_stop_network()` -- lifecycle
- `fdb_database_create_transaction()` -- create transaction
- `fdb_transaction_get()` / `fdb_transaction_set()` / `fdb_transaction_commit()` -- operations
- All async operations return `FDBFuture*` handles

---

## DatabaseContext -- `DatabaseContext.h`, [`DatabaseContext.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/DatabaseContext.cpp)

Central state for a database connection. Shared by all transactions on this connection.

### Location Cache (`DatabaseContext.h:410`)

```
CoalescedKeyRangeMap<Reference<LocationInfo>> locationCache;
```

Maps key ranges to storage server locations. Used to route reads directly to the right SS without asking a proxy every time.

- `getCachedLocation(key)` -- O(log n) lookup in range map
- `setCachedLocation(range, servers)` -- insert/update, evicts old entries when cache full
- `invalidateCache(key)` / `invalidateCache(range)` -- clear on shard movement

### Proxy Management

```
Reference<CommitProxyInfo> commitProxies;   // for commits and location queries
Reference<GrvProxyInfo> grvProxies;         // for read version assignment
AsyncTrigger onProxiesChanged;              // fires when proxy set updates
```

Updated by `monitorProxies()` actor which watches for cluster configuration changes.

### GRV Cache

```
double lastGrvTime;
Version cachedReadVersion;
```

Caches recent read versions to reduce GRV proxy load. Background updater via `backgroundGrvUpdater()`.

---

## Commit Flow -- [`NativeAPI.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/NativeAPI.actor.cpp)`:4369+`

### tryCommit() Actor

1. **Estimate cost**: If tagged transaction, calculate cost for throttling
2. **Build CommitTransactionRequest**:
   - `read_snapshot` = read version
   - `mutations[]` = all set/clear/atomic operations
   - `read_conflict_ranges[]` = keys that were read
   - `write_conflict_ranges[]` = keys that were written
3. **Select proxy**: `basicLoadBalance()` across commit proxies, or first proxy if `commitOnFirstProxy`
4. **Send and wait**: Retry on proxy set change
5. **Handle reply**:
   - **Success** (version != invalidVersion): update `committedVersion`, send versionstamp, update metadata cache
   - **Conflict** (version == invalidVersion): extract conflicting key info, throw `not_committed()`
   - **Unknown result**: use idempotency ID to determine final status

### CommitTransactionRequest structure

```
struct CommitTransactionRequest {
    SpanContext spanContext;
    TransactionRef transaction {
        VectorRef<MutationRef> mutations;
        VectorRef<KeyRangeRef> read_conflict_ranges;
        VectorRef<KeyRangeRef> write_conflict_ranges;
        Version read_snapshot;
    };
    bool report_conflicting_keys;
    // ...
};
```

### MutationRef -- [`CommitTransaction.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/CommitTransaction.h)`:64-150`

```
struct MutationRef {
    enum Type : uint8_t {
        SetValue=0, ClearRange, AddValue, CompareAndClear,
        SetVersionstampedKey, SetVersionstampedValue,
        // atomic ops: And, Or, Xor, AppendIfFits, Max, Min, ByteMin, ByteMax, ...
    };
    uint8_t type;
    StringRef param1;  // key (or begin key for ClearRange)
    StringRef param2;  // value (or end key for ClearRange)
};
```

---

## Read Flow

### Point Read -- `get(key)`

```
1. getReadVersion()  →  GRV proxy (batched)
2. getKeyLocation(key)  →  location cache or commit proxy
3. getValue(key, version)  →  storage server via loadBalance()
4. Return value (or Optional<Value>() if not found)
```

### Range Read -- `getRange(begin, end, limits)`

```
1. getReadVersion()
2. Resolve KeySelectors to concrete keys
3. For each shard in range:
   a. getKeyLocation()  →  find storage server
   b. Send GetKeyValuesRequest to SS
   c. Accumulate results respecting row/byte limits
   d. If more data and limits not reached, continue to next shard
4. Return RangeResult
```

### KeySelector Resolution

```
struct KeySelectorRef {
    KeyRef key;
    bool orEqual;
    int offset;
};

firstGreaterOrEqual(k) = KeySelector(k, false, +1)
lastLessOrEqual(k)     = KeySelector(k, true, 0)
lastLessThan(k)        = KeySelector(k, false, 0)
firstGreaterThan(k)    = KeySelector(k, true, +1)
```

### Proxy Load Balancing -- `ProxyLoadBalance.h`

```cpp
// Pattern: send request, race against proxy set change
while (true) {
    auto reply = basicLoadBalance(cx->getCommitProxies(), channel, req);
    auto result = co_await race(reply, cx->onProxiesChanged());
    if (result.index() == 0) co_return std::get<0>(result);
    // proxy set changed, retry with new proxies
}
```

---

## Cluster Discovery -- [`MonitorLeader.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/MonitorLeader.h)

### Connection Flow

1. Parse cluster file: `description:id@coord1,coord2,coord3`
2. `monitorLeader()` contacts coordinators, tracks current cluster controller
3. `monitorProxies()` fetches commit/GRV proxy lists from cluster controller
4. `DatabaseContext` updated with new proxy interfaces
5. Transactions use proxies for all operations

### Watch Operations

```cpp
Future<Void> watch = tr.watch(key);  // register interest
wait(tr.commit());                    // commit first
wait(watch);                          // then wait for change
```

- Watch metadata tracked in `DatabaseContext::watchMap`
- Storage server monitors key, notifies on value change
- Watch survives transaction commit

---

## System Keys -- [`SystemData.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/SystemData.h)

Special key ranges for cluster metadata (prefix `\xff`):

| Range | Purpose |
|-------|---------|
| `keyServersKeys` (`\xff/keyServers/`) | key range → storage server mapping |
| `serverKeysRange` (`\xff/serverKeys/`) | storage server → owned key ranges (reverse map) |
| `serverListKeys` (`\xff/serverList/`) | storage server interfaces |
| `configKeys` (`\xff/conf/`) | database configuration |
| `tagLocalityListKeys` | datacenter locality assignments |
| `serverTagKeys` | storage server tag assignments |
| `moveKeysLockOwnerKey` | data distribution lock |
| `backupStartedKey` | active backup state |

---

## Special Key Space (`\xff\xff` prefix)

Management API exposed as key-value operations:

- Read `\xff\xff/status/json` for cluster status
- Write to `\xff\xff/management/` for configuration changes
- `SpecialKeyRangeReadImpl` / `SpecialKeyRangeRWImpl` abstract handlers

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbclient/NativeAPI.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/NativeAPI.actor.cpp) | Core transaction implementation, tryCommit, getValue, getRange |
| `fdbclient/include/fdbclient/NativeAPI.actor.h` | Transaction class, TransactionState |
| [`fdbclient/ReadYourWrites.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/ReadYourWrites.actor.cpp) | RYW layer, write map, snapshot cache merging |
| `fdbclient/include/fdbclient/DatabaseContext.h` | DatabaseContext: location cache, proxy tracking, watches |
| [`fdbclient/include/fdbclient/CommitTransaction.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/CommitTransaction.h) | MutationRef, CommitTransactionRef |
| [`fdbclient/MultiVersionTransaction.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/MultiVersionTransaction.cpp) | Multi-version client, DLTransaction |
| `fdbclient/MonitorLeader.actor.cpp` | Cluster discovery, leader tracking |
| [`fdbclient/include/fdbclient/SystemData.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/SystemData.h) | System key range definitions |
| `fdbclient/ProxyLoadBalance.h` | Proxy load balancing templates |
