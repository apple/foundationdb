# Subsystem 5: Transaction Commit Pipeline

**[Diagrams](diagram_05_commit_pipeline.md)**

**Location:** [`fdbserver/commitproxy/`](https://github.com/apple/foundationdb/tree/main/fdbserver/commitproxy), [`fdbserver/grvproxy/`](https://github.com/apple/foundationdb/tree/main/fdbserver/grvproxy), [`fdbserver/resolver/`](https://github.com/apple/foundationdb/tree/main/fdbserver/resolver), [`fdbserver/sequencer/`](https://github.com/apple/foundationdb/tree/main/fdbserver/sequencer)
**Size:** ~12K  
**Role:** Version assignment, conflict detection, commit batching -- the write-path orchestration.

---

## Overview

Four server roles collaborate in a strict pipeline to commit transactions:

```
Client ──CommitTransactionRequest──▶ CommitProxy
                                        │
Phase 1: GET VERSION                    ▼
                                    Master/Sequencer  (monotonic version)
Phase 2: RESOLVE                        ▼
                                    Resolver(s)  (conflict detection)
Phase 3: LOG                            ▼
                                    TLog replicas  (durable write)
Phase 4: REPLY                          ▼
                                    Client (committed!)
```

---

## Commit Proxy -- [`CommitProxyServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/commitproxy/CommitProxyServer.actor.cpp)

The workhorse of the commit pipeline. Batches client commits and drives them through all phases.

### ProxyCommitData -- `ProxyCommitData.h:183`

```
struct ProxyCommitData {
    MasterInterface master;                           // for version assignment
    std::vector<ResolverInterface> resolvers;         // for conflict detection
    Reference<LogSystem> logSystem;                  // for TLog push
    IKeyValueStore* txnStateStore;                    // persistent metadata
    
    NotifiedVersion committedVersion;                 // largest durable version
    NotifiedVersion version;                          // current proxy version
    
    KeyRangeMap<ServerCacheInfo> keyInfo;             // key range → storage servers + tags
    
    const std::vector<Tag>& tagsForKey(StringRef key); // tag lookup for mutations
};
```

### commitBatch() -- 5-Phase Pipeline

**CommitBatchContext** (`CommitProxyServer.actor.cpp:491`):
```
struct CommitBatchContext {
    std::vector<CommitTransactionRequest> trs;        // batch of transactions
    LogPushData toCommit;                             // serialized mutations for TLogs
    Version commitVersion, prevVersion;
    std::vector<ResolveTransactionBatchReply> resolution;  // conflict results
    std::set<Tag> writtenTags;
};
```

**Phase 1: Pre-Resolution** (`preresolutionProcessing`):
- Validate transaction sizes
- Compute read/write conflict ranges
- Determine storage server tags via `tagsForKey()` for each mutation
- Check hot shard throttling

**Phase 2: Resolution** (`getResolution`):
- Send `ResolveTransactionBatchRequest` to all resolvers in parallel
- Each resolver handles a partition of the key space
- Wait for all responses

**Phase 3: Post-Resolution** (`postResolution`):
- Process conflict results: mark conflicting transactions
- Apply metadata mutations (`applyMetadataToCommittedTransactions()`)
- Update storage server mappings
- Process idempotency keys

**Phase 4: Transaction Logging** (`transactionLogging`):
- Call `logSystem->push()` with tagged mutations
- Block until mutations are durable (TLog quorum ack)
- Update `queueCommittedVersion`

**Phase 5: Reply** (`reply`):
- Send `CommitTransactionReply` to each client
- Conflicting transactions get `not_committed` error
- Successful transactions get their commit version

---

## GRV Proxy -- [`GrvProxyServer.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/grvproxy/GrvProxyServer.cpp)

Assigns read versions to client transactions. The GRV proxy is the primary enforcement point for cluster-wide flow control: it is where ratekeeper's back-pressure decisions become concrete delays and rejections applied to client requests.

### GrvProxyData (line 202)

```
struct GrvProxyData {
    MasterInterface master;                    // version source
    GrvProxyTagThrottler tagThrottler;         // per-tag throttling
    VersionVector ssVersionVectorCache;        // storage server version tracking
    Version version;
};
```

### End-to-End Flow Control: Ratekeeper → GRV Proxy → Client

Flow control is the mechanism that prevents the cluster from being overwhelmed when storage servers or TLogs fall behind. It works as a feedback loop:

**1. Ratekeeper computes global TPS limits** (`Ratekeeper::updateRate()`, `Ratekeeper.cpp:642`):
- Runs periodically (every `METRIC_UPDATE_RATE` seconds, default 0.1s).
- Polls every storage server and TLog for queue depths, durable bytes rates, and free disk space.
- Computes a `tpsLimit` for both `normalLimits` (DEFAULT + SYSTEM priority) and `batchLimits` (BATCH priority) independently.
- The core formula scales `actualTps` (smoothed observed transaction rate) by the ratio of durable-bytes throughput to input-bytes rate, further modulated by a `targetRateRatio` derived from how full the storage/TLog queue is relative to target and spring thresholds. The result is: if queues are within target, `tpsLimit ≈ infinity`; as queues grow past the target, the limit ramps down toward zero.
- Batch limits use more aggressive thresholds (larger `storageTargetBytes`, `logTargetBytes`) so batch transactions are throttled first.

**2. Ratekeeper sends per-proxy rate limits** (`handleGetRateInfoReqs()`, `Ratekeeper.cpp:347`):
- Each GRV proxy periodically sends a `GetRateInfoRequest` to ratekeeper (every `leaseDuration/2` seconds, jittered).
- Ratekeeper replies with `transactionRate = normalLimits.tpsLimit / numProxies` and `batchTransactionRate = batchLimits.tpsLimit / numProxies` — the global limit divided evenly across all GRV proxies.
- The reply also includes a `leaseDuration` (default `METRIC_UPDATE_RATE` = 0.1s). If the proxy doesn't hear back within the lease, it disables rate limiting (sets rate to 0 via `GrvTransactionRateInfo::disable()`), which smoothly ramps the allowed rate to zero and stops releasing transactions.
- Optionally includes per-tag throttle information for `GrvProxyTagThrottler`.

**3. GRV proxy enforces the rate via a token bucket** (`GrvTransactionRateInfo`, `GrvTransactionRateInfo.h`):
- Each proxy maintains two `GrvTransactionRateInfo` objects: `normalRateInfo` (for SYSTEM + DEFAULT) and `batchRateInfo` (for BATCH).
- Each object is a smoothed token bucket: `setRate()` feeds the ratekeeper-provided rate through a `Smoother`, and `startReleaseWindow()` computes a `limit` from the smoothed difference between the allowed rate and the actual release rate, times the rate window.
- `canStart(numAlreadyStarted, count)` checks if `numAlreadyStarted + count <= limit + budget`. The `budget` accumulates unused capacity across release windows, bounded by `maxEmptyQueueBudget` when queues are empty (to avoid stale budget causing bursts).
- The smoothing prevents sudden rate changes from causing oscillation.

**4. The `transactionStarter` loop releases batches** (`transactionStarter()`, line 877):
- On each GRVTimer tick, it drains the three priority queues in strict order: SYSTEM first, then DEFAULT, then BATCH.
- SYSTEM transactions bypass rate limiting entirely (`canStart` is never checked for them).
- DEFAULT transactions are gated by `normalRateInfo.canStart()`.
- BATCH transactions are gated by `batchRateInfo.canStart()`, which uses the more aggressive batch rate limit.
- Transactions that pass rate-limiting are batched into a single `getLiveCommittedVersion()` call to the master, and replies are sent to all clients in the batch simultaneously.

### Dynamic Batching Based on Reply Latency

The GRV batch interval is adaptively tuned based on observed round-trip latency (`queueGetReadVersionRequests`, line 518):

- After each batch of non-risky-read requests completes, `timeReply()` measures the round-trip time and feeds it into a `normalGRVLatency` stream.
- The `queueGetReadVersionRequests` actor receives this latency and computes:
  ```
  target_latency = reply_latency * START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION
  GRVBatchTime = α * target_latency + (1 - α) * GRVBatchTime
  ```
  clamped to `[START_TRANSACTION_BATCH_INTERVAL_MIN, START_TRANSACTION_BATCH_INTERVAL_MAX]`.
- When the first request arrives into empty queues, a GRVTimer is scheduled with delay `max(0, GRVBatchTime - timeSinceLastGRV)`. This means: when the system is fast, batching intervals shrink (lower latency for clients); when the system is slow, batches grow larger (better amortization of the master round-trip).

### Lower Priorities Dropped Under Pressure

When the total GRV queue depth exceeds `START_TRANSACTION_MAX_QUEUE_SIZE`, the proxy drops lower-priority requests to make room (`queueGetReadVersionRequests`, line 541):

- A new **BATCH** request is rejected immediately with `grv_proxy_memory_limit_exceeded`.
- A new **DEFAULT** request evicts one BATCH request from the front of its queue (if any); if none exist, the DEFAULT request itself is rejected.
- A new **SYSTEM** request evicts one BATCH request, or failing that one DEFAULT request; only if both queues are empty is the SYSTEM request itself rejected.
- Additionally, when the batch rate from ratekeeper drops to effectively zero (`batchRateInfo.getRate() <= 1/numProxies`), BATCH requests are immediately rejected with `batch_transaction_throttled` before even being queued (line 605).

### GetReadVersion Flow

1. **Queuing** (`queueGetReadVersionRequests`, line 518):
   - Three priority queues: SYSTEM, DEFAULT, BATCH
   - Tagged requests optionally routed to `GrvProxyTagThrottler` for per-tag rate limiting
   - Dynamic batching as described above

2. **Version fetch** (`getLiveCommittedVersion`, line 670):
   - Request to master: `master.getLiveCommittedVersion.getReply()`
   - Causal read check (unless `CAUSAL_READ_RISKY`): `updateLastCommit()` confirms epoch is still live by calling `logSystem->confirmEpochLive()`. This ensures the version returned actually represents committed state.
   - Version vector integration if enabled: applies delta from master to local SS version cache

3. **Reply** (`sendGrvReplies`, line 747):
   - Sends the version to all requests in the batch
   - Attaches per-tag throttle information so clients can self-throttle
   - Sets `rkBatchThrottled` / `rkDefaultThrottled` flags if sustained throttling detected (sustained for `GRV_SUSTAINED_THROTTLING_THRESHOLD` seconds), signaling clients to back off

---

## Master/Sequencer -- [`masterserver.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/sequencer/masterserver.cpp)

Assigns monotonically increasing commit versions.

### MasterData -- `MasterData.h:50`

```
struct MasterData {
    Version lastEpochEnd;                      // last version from prior epoch
    Version recoveryTransactionVersion;        // first version in this epoch
    NotifiedVersionValue liveCommittedVersion;  // largest live-committed version
    Version version;                           // last assigned version
    double lastVersionTime;                    // timestamp of last version
    Optional<Version> referenceVersion;        // for wall-clock alignment
    
    std::map<UID, CommitProxyVersionReplies> lastCommitProxyVersionReplies;
    VersionVector ssVersionVector;             // per-SS commit versions
    ResolutionBalancer resolutionBalancer;
};
```

### Version Assignment -- `getVersion()` (lines 74-178)

1. **Proxy validation**: Check proxy is known
2. **Request ordering**: Wait for prior request (`latestRequestNum.whenAtLeast(requestNum - 1)`)
3. **Deduplication**: Return cached reply if already processed
4. **Version calculation**:
   ```
   toAdd = max(1, min(MAX_READ_TRANSACTION_LIFE_VERSIONS,
                      VERSIONS_PER_SECOND * (now - lastVersionTime)))
   ```
   If `referenceVersion` set, align to wall clock via `figureVersion()`
5. **Reply**: `GetCommitVersionReply` with `prevVersion` and `version`

### figureVersion() -- Wall-Clock Alignment (lines 43-72)

```
expectedVersion = now * VERSIONS_PER_SECOND - referenceVersion
version = clamp(version + toAdd, expectedVersion ± scaled_bounds)
```

Keeps versions roughly aligned with wall-clock microseconds while maintaining monotonicity.

### Live Committed Version Tracking

- `serveLiveCommittedVersion()`: Responds to GRV proxy and proxy reports
- `updateLiveCommittedVersion()`: Updates when proxy reports new committed version
- If Version Vectors enabled: tracks per-tag commit info

---

## Resolver -- [`Resolver.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/Resolver.cpp), [`ConflictSet.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/ConflictSet.cpp)

Pure conflict detection. Maintains a sliding window of committed write ranges.

### Resolver Structure (line 128)

```
struct Resolver {
    int commitProxyCount, resolverCount;
    NotifiedVersion version;
    ConflictSet* conflictSet;                         // SkipList-based conflict tracking
    IKeyValueStore* txnStateStore;                    // metadata KVS
    std::map<NetworkAddress, ProxyRequestsInfo> proxyInfoMap;
    KeyRangeMap<ServerCacheInfo> keyInfo;
};
```

### ConflictSet

```
struct ConflictSet {
    SkipList versionHistory;  // version → conflict data
    Version oldestVersion;
};
```

### resolveBatch() (line 261)

1. **Memory check**: Block if `totalStateBytes > RESOLVER_STATE_MEMORY_LIMIT`
2. **Version ordering** (`versionReady()`): Wait for resolver to reach `prevVersion`
3. **Conflict detection**:
   ```cpp
   ConflictBatch conflictBatch(self->conflictSet, &reply.conflictingKeyRangeMap);
   for (auto& txn : req.transactions)
       conflictBatch.addTransaction(txn, newOldestVersion);
   conflictBatch.detectConflicts(req.version, newOldestVersion, commitList, &tooOldList);
   ```
   - For each transaction: check if read ranges overlap with committed writes at newer versions
   - Populates `reply.committed[]` with per-transaction conflict status
4. **State transaction processing**: Apply metadata mutations if enabled
5. **Version cleanup**: Erase old state transactions to bound memory

### ConflictBatch::detectConflicts()

```
void detectConflicts(Version now, Version newOldestVersion,
                    std::vector<int>& nonConflicting,
                    std::vector<int>* tooOldTransactions)
```

Algorithm: For each transaction's read ranges, check overlap with committed write ranges in the SkipList at versions newer than the transaction's read version.

---

## Tag Assignment

Tags connect mutations to storage servers:

1. **During pre-resolution**: For each mutation, `tagsForKey(mutation.key)` returns tags
2. **Tag source**: `KeyRangeMap<ServerCacheInfo> keyInfo` maps key ranges → storage servers → tags
3. **Tags added to LogPushData**: `toCommit.addTags(tags)` for each mutation
4. **In TLog**: Messages routed to `TagData[tag.locality][tag.id]` queues
5. **Storage servers**: Peek their assigned tag to pull relevant mutations

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/commitproxy/CommitProxyServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/commitproxy/CommitProxyServer.actor.cpp) | 5-phase commit batch pipeline |
| `fdbserver/commitproxy/ProxyCommitData.h` | ProxyCommitData, tag lookup |
| [`fdbserver/grvproxy/GrvProxyServer.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/grvproxy/GrvProxyServer.cpp) | Read version assignment, rate limiting enforcement |
| `fdbserver/grvproxy/GrvTransactionRateInfo.h` | Token-bucket rate limiter driven by ratekeeper |
| `fdbserver/grvproxy/GrvProxyTagThrottler.h` | Per-tag throttling on proxy side |
| [`fdbserver/sequencer/masterserver.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/sequencer/masterserver.cpp) | Version assignment, wall-clock alignment |
| `fdbserver/sequencer/MasterData.h` | MasterData, version tracking |
| [`fdbserver/resolver/Resolver.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/Resolver.cpp) | Conflict detection, batch resolution |
| [`fdbserver/resolver/ConflictSet.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/ConflictSet.cpp) | SkipList-based conflict range tracking |
