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

### ProxyCommitData -- `ProxyCommitData.h:182-340`

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

**CommitBatchContext** (`CommitProxyServer.actor.cpp:487-586`):
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

Assigns read versions to client transactions.

### GrvProxyData (lines 203-264)

```
struct GrvProxyData {
    MasterInterface master;                    // version source
    GrvProxyTagThrottler tagThrottler;         // per-tag throttling
    VersionVector ssVersionVectorCache;        // storage server version tracking
    Version version;
};
```

### GetReadVersion Flow

1. **Queue management** (`queueGetReadVersionRequests`, lines 519-638):
   - Three priority queues: SYSTEM, DEFAULT, BATCH
   - Dynamic batching based on reply latency
   - Lower priorities dropped under pressure

2. **Version fetch** (`getLiveCommittedVersion`, lines 670-699):
   - Request to master: `master.getLiveCommittedVersion.getReply()`
   - Causal read check: `updateLastCommit()` to ensure epoch durability
   - Version vector integration if enabled

3. **Rate limiting**:
   - Communicates with Ratekeeper via `GetRateInfoRequest`
   - Enforces transaction rate limits by delaying responses
   - Per-tag throttling via `GrvProxyTagThrottler`

---

## Master/Sequencer -- [`masterserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/sequencer/masterserver.actor.cpp)

Assigns monotonically increasing commit versions.

### MasterData -- `MasterData.h:80-157`

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

### Version Assignment -- `getVersion()` (lines 130-232)

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

### figureVersion() -- Wall-Clock Alignment (lines 49-89)

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

## Resolver -- [`Resolver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/Resolver.actor.cpp), [`ConflictSet.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/ConflictSet.cpp)

Pure conflict detection. Maintains a sliding window of committed write ranges.

### Resolver Structure (lines 128-221)

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

### resolveBatch() (lines 261-519)

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
| [`fdbserver/grvproxy/GrvProxyServer.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/grvproxy/GrvProxyServer.cpp) | Read version assignment, rate limiting |
| [`fdbserver/sequencer/masterserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/sequencer/masterserver.actor.cpp) | Version assignment, wall-clock alignment |
| `fdbserver/sequencer/MasterData.h` | MasterData, version tracking |
| [`fdbserver/resolver/Resolver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/Resolver.actor.cpp) | Conflict detection, batch resolution |
| [`fdbserver/resolver/ConflictSet.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/ConflictSet.cpp) | SkipList-based conflict range tracking |
