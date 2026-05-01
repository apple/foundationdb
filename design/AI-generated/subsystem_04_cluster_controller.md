# Subsystem 4: Cluster Controller & Coordination

**[Diagrams](diagram_04_cluster_controller.md)**

**Location:** [`fdbserver/clustercontroller/`](https://github.com/apple/foundationdb/tree/main/fdbserver/clustercontroller), [`fdbserver/coordinator/`](https://github.com/apple/foundationdb/tree/main/fdbserver/coordinator), [`fdbserver/core/LeaderElection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/LeaderElection.actor.cpp)
**Size:** ~13K + coordination code  
**Role:** Leader election, role recruitment, process registration, cluster-wide decision making.

---

## Overview

One process is elected Cluster Controller (CC). It is the "brain" of the cluster: it recruits all server roles, monitors health, broadcasts configuration, and initiates recovery. Leadership is determined by a coordination quorum running on designated coordinator processes.

---

## Leader Election

### Coordinators

Typically 3 or 5 processes (addresses hardcoded in cluster file) that run a `leaderRegister` actor. They don't store user data -- only coordination metadata. Coordinators are not involved in committing transactions; they hold ~1KB of state that changes only during recovery.

### Generation Register (Paxos Acceptor) -- [`Coordination.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/coordinator/Coordination.cpp)`:121-173`

The fundamental primitive: a register with generation numbers that prevents stale writes. Each coordinator runs a `localGenerationReg()` actor that implements a single Paxos acceptor. The generation register protocol is isomorphic to single-decree Paxos:

| Paxos concept | FDB equivalent |
|---|---|
| Ballot number | `UniqueGeneration` (uint64 generation + UID tiebreaker) |
| Prepare(n) | `GenerationRegReadRequest(key, gen)` |
| Promise(n, v) | `GenerationRegReadReply(value, writeGen, readGen)` |
| Accept(n, v) | `GenerationRegWriteRequest(kv, gen)` |
| Quorum of acceptors | Majority of coordinators (n/2 + 1) |

```
struct GenerationRegVal {
    UniqueGeneration readGen;   // highest generation promised (Paxos: highest prepare seen)
    UniqueGeneration writeGen;  // highest generation accepted (Paxos: highest accept seen)
    Optional<Value> val;        // the accepted value
};
```

**Read protocol** (`GenerationRegReadRequest`) -- equivalent to Paxos **prepare**:
- If client's generation > stored readGen: update readGen (promise not to accept lower)
- Persist to disk (`store->commit()`)
- Return (value, writeGen, readGen)

**Write protocol** (`GenerationRegWriteRequest`) -- equivalent to Paxos **accept**:
- Check: `readGen <= requestGen AND writeGen < requestGen`
- If valid: update writeGen and value, persist, return writeGen
- If invalid: return `max(readGen, writeGen)` so client can retry with a higher generation

The core consensus logic in `localGenerationReg()` is ~50 lines of code.

### Election Algorithm -- [`LeaderElection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/LeaderElection.actor.cpp)

**`tryBecomeLeaderInternal()`** (lines 107-300):

1. **Nomination phase** (lines 136-227):
   - Generate `changeID` and `LeaderInfo` (encodes process class fitness in top bits)
   - Submit `CandidacyRequest` to all coordinators
   - Each coordinator nominates the candidate with best fitness
   - Loop: check if quorum nominates us
   - If quorum: become leader. If not: retry with updated info

2. **Heartbeat phase** (lines 233-294):
   - Send `LeaderHeartbeatRequest` to all coordinators at `HEARTBEAT_FREQUENCY`
   - Expect quorum to respond `true`
   - If quorum responds `false`: leadership lost, restart nomination
   - Update `changeID` when priority info changes

**LeaderInfo structure:**
- `serializedInfo` -- serialized cluster interface
- `changeID` -- unique ID for this leadership claim (top 7 bits = process class fitness)
- `forward` flag -- signals connection string update

### Coordinated State (Paxos Proposer) -- [`CoordinatedState.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/CoordinatedState.cpp)`:71-210`

Higher-level abstraction that implements the Paxos proposer role over generation registers. This is where the quorum logic lives -- individual coordinators are acceptors that know nothing about quorums; `CoordinatedStateImpl` drives the two-phase protocol across the quorum.

```
struct CoordinatedStateImpl {
    ServerCoordinators coordinators;
    UniqueGeneration gen;
    uint64_t conflictGen;
    bool doomed;
    bool initial;        // true if no prior value has been written
};
```

**`read()`** -- Paxos prepare phase (two rounds):
1. **Round 1**: Send `GenerationRegReadRequest(key, UniqueGeneration())` to all coordinators. Wait for majority. Learns the highest existing generation.
2. **Compute ballot**: `conflictGen = max(all seen generations) + 1`. Create `gen = UniqueGeneration(conflictGen, randomUID)`.
3. **Round 2**: Send `GenerationRegReadRequest(key, gen)` to all coordinators. Wait for majority. Each acceptor promises not to accept writes below `gen`. Returns the value with the highest `writeGen`.

**`setExclusive()`** -- Paxos accept phase:
- Send `GenerationRegWriteRequest(key, value, gen)` to all coordinators.
- For initial state (no prior writes): wait for **all** replicas. Otherwise: wait for **majority** (n/2 + 1).
- If any coordinator returns a generation > `gen`, throw `coordinated_state_conflict` (another proposer won).

**Quorum sizes** (from `replicatedRead` / `replicatedWrite`):
- Read quorum: `replicas.size() / 2 + 1` (majority)
- Write quorum: `replicas.size() / 2 + 1` (majority), except initial writes require all replicas

**`replicatedRead()`** has a subtlety: it races two sets of futures -- replies with a value vs. replies without. If a majority report empty (no value), it returns the empty reply with the highest `rgen`. Otherwise it returns the non-empty reply with the highest `writeGen`. This handles the bootstrap case where some coordinators have been written and others haven't.

Used by recovery to store/update `DBCoreState` (log system configuration). This is the only mutable state managed by coordinators.

### Coordinator Storage -- [`OnDemandStore`](https://github.com/apple/foundationdb/blob/main/fdbserver/coordinator/OnDemandStore.cpp), [`DiskQueue`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/DiskQueue.cpp)

Each coordinator's Paxos state (generation numbers and the coordinated value) is persisted via `KeyValueStoreMemory` -- an in-memory key-value store backed by a `DiskQueue` for durability.

**On-disk format:**
- Two files per coordinator: `coordination-0.fdq` and `coordination-1.fdq` in the process's data directory
- `.fdq` = **F**oundation**D**B **Q**ueue -- an append-only circular log that alternates between the two files
- Entries are checksummed (xxhash3 in DiskQueueVersion::V2)
- `KeyValueStoreMemory` periodically snapshots the full in-memory state into the log, after which older entries are reclaimed

**Access pattern:**
- `OnDemandStore` is a lazy wrapper: opens the `KeyValueStoreMemory` only on first access (coordinator processes that are never contacted don't create files)
- `localGenerationReg()` calls `store->readValue(key)` on each request and `store->set()` + `store->commit()` to persist generation updates
- `store->commit()` flushes to the `.fdq` log, making Paxos promises and accepts crash-safe
- The memory budget is 500MB (`keyValueStoreMemory(path, id, 500e6)`) -- vastly more than needed for the ~1KB of coordination state, but it's the standard `KeyValueStoreMemory` interface

This is the same storage engine used by transaction logs for their mutation queues and by `txnStateStore` during recovery.

---

## ClusterControllerData -- `ClusterController.h:122-3412`

The state maintained by the elected CC.

### DBInfo (lines 124-215)

```
struct DBInfo {
    Reference<AsyncVar<ClientDBInfo>> clientInfo;    // proxy list for clients
    Reference<AsyncVar<ServerDBInfo>> serverInfo;    // full cluster config for servers
    AsyncTrigger forceMasterFailure;
    DatabaseConfiguration config, fullyRecoveredConfig;
    int unfinishedRecoveries;
    Database db;
};
```

### Worker Pool

```
std::map<Optional<Standalone<StringRef>>, WorkerInfo> id_worker;  // processId → WorkerInfo
std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class; // processId → configured class
```

### Recruitment State

```
std::vector<Reference<RecruitWorkersInfo>> outstandingRecruitmentRequests;
std::vector<Reference<RecruitRemoteWorkersInfo>> outstandingRemoteRecruitmentRequests;
std::vector<std::pair<RecruitStorageRequest, double>> outstandingStorageRequests;

// Singleton role tracking
AsyncVar<bool> recruitDistributor, recruitRatekeeper, recruitConsistencyScan;
Optional<UID> recruitingDistributorID, recruitingRatekeeperID, recruitingConsistencyScanID;
```

### Health Monitoring

```
std::unordered_map<NetworkAddress, WorkerHealth> workerHealth;
struct WorkerHealth {
    std::unordered_map<NetworkAddress, DegradedTimes> degradedPeers;
    std::unordered_map<NetworkAddress, DegradedTimes> disconnectedPeers;
};
```

---

## Worker Registration -- [`ClusterController.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterController.actor.cpp)`:1252-1434`

### RegisterWorkerRequest

```
struct RegisterWorkerRequest {
    WorkerInterface wi;
    ProcessClass initialClass, processClass;
    ClusterControllerPriorityInfo priorityInfo;
    Generation generation;
    Optional<DataDistributorInterface> distributorInterf;
    Optional<RatekeeperInterface> ratekeeperInterf;
    Optional<ConsistencyScanInterface> consistencyScanInterf;
    bool degraded, recoveredDiskFiles;
};
```

### Registration Flow

1. **Cluster ID validation** -- reject workers from different clusters
2. **Priority calculation** -- compute CC fitness and DC fitness for this worker
3. **Process class resolution** -- use DB-configured class or initial class
4. **Worker add/update** -- create `WorkerInfo`, start watcher actor, add to `id_worker`
5. **Singleton handling** -- if worker reports DD/Ratekeeper/ConsistencyScan interface, update DBInfo
6. **Trigger recruitment check** -- call `checkOutstandingRequests()` to re-evaluate pending recruitment

---

## Role Recruitment

### Fitness Calculation -- `ClusterController.h:1443-1643`

**RoleFitness structure:**
```
struct RoleFitness {
    ProcessClass::Fitness bestFit, worstFit;
    ProcessClass::ClusterRole role;
    int count, worstUsed;
    bool degraded;
};
```

**Comparison priority** (for `operator<`):
1. Worst fitness (lower is better)
2. Worst used count (fewer concurrent roles is better)
3. Total count (more is better)
4. Prefer non-degraded
5. For non-TLog: compare best fitness

**Worker selection methods:**
- `getWorkerForRoleInDatacenter()` -- single best worker for role in DC
- `getWorkersForRoleInDatacenter()` -- multiple workers, prioritized by fitness
- `getWorkersForTlogsSimple()` -- TLog recruitment respecting zone distribution
- `getWorkersForTlogsComplex()` -- TLog recruitment for cross-zone policies

### Recruitment Functions -- `ClusterRecovery.cpp`

| Function | Role Recruited |
|----------|---------------|
| `recruitNewMaster()` | Master/Sequencer (must be same DC as CC) |
| `newCommitProxies()` | Commit proxies (via `InitializeCommitProxyRequest`) |
| `newGrvProxies()` | GRV proxies (via `InitializeGrvProxyRequest`) |
| `newResolvers()` | Resolvers (via `InitializeResolverRequest`) |
| `newTLogServers()` | TLogs, including remote region and log routers |
| `newSeedServers()` | Initial storage servers (version 0 only) |

---

## ServerDBInfo Broadcasting -- `ServerDBInfo.h:34-88`

### ServerDBInfo Contents

```
struct ServerDBInfo {
    UID id;
    ClusterControllerFullInterface clusterInterface;
    ClientDBInfo client;                          // proxy lists
    MasterInterface master;
    Optional<DataDistributorInterface> distributor;
    Optional<RatekeeperInterface> ratekeeper;
    std::vector<ResolverInterface> resolvers;
    DBRecoveryCount recoveryCount;
    RecoveryState recoveryState;
    LifetimeToken masterLifetime;
    LogSystemConfig logSystemConfig;
    int64_t infoGeneration;                       // increment on each change
};
```

### Broadcasting

`dbInfoUpdater()` actor:
1. Waits for `updateDBInfo` trigger or `serverInfo` change
2. Batches changes with `DBINFO_BATCH_DELAY`
3. Serializes and sends `UpdateServerDBInfoRequest` to all workers
4. Workers store in their local `AsyncVar<ServerDBInfo>` and react to changes

---

## ClusterController Main Loop -- `clusterControllerCore()`

**Core actors spawned:**
- `clusterWatchDatabase()` -- monitors master, triggers recovery
- `statusServer()` -- handles status requests from fdbcli
- `timeKeeper()` -- periodic version tracking
- `monitorProcessClasses()` -- watches DB for class changes
- `monitorDataDistributor()` / `monitorRatekeeper()` / `monitorConsistencyScan()` -- singleton lifecycles
- `dbInfoUpdater()` -- broadcasts ServerDBInfo
- `workerHealthMonitor()` -- optional health tracking

**Event loop handles:**
- `RegisterWorkerRequest` -- worker self-registration
- `RecruitStorageRequest` -- on-demand storage server recruitment
- `OpenDatabaseRequest` -- client database access
- `GetWorkersRequest` / `GetClientWorkersRequest` -- worker queries
- Coordination pings at `WORKER_COORDINATION_PING_DELAY` intervals
- Leader failure detection

---

## WorkerInterface -- [`WorkerInterface.actor.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/WorkerInterface.actor.h)`:45-126`

RPCs exposed by every worker process:

**Initialization:**
`tLog`, `master`, `commitProxy`, `grvProxy`, `dataDistributor`, `ratekeeper`, `consistencyScan`, `resolver`, `storage`, `logRouter`, `backup`

**Monitoring:**
`debugPing`, `coordinationPing`, `waitFailure`, `setMetricsRate`, `eventLogRequest`, `updateServerDBInfo`

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/clustercontroller/ClusterController.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterController.actor.cpp) | CC main loop, worker registration, event handling |
| `fdbserver/clustercontroller/ClusterController.h` | ClusterControllerData, fitness calculation, recruitment |
| [`fdbserver/coordinator/Coordination.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/coordinator/Coordination.cpp) | leaderRegister, generation register, coordination |
| [`fdbserver/core/LeaderElection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/LeaderElection.actor.cpp) | tryBecomeLeaderInternal, candidacy, heartbeat |
| [`fdbserver/core/CoordinatedState.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/CoordinatedState.cpp) | Replicated read/write over generation registers |
| [`fdbserver/core/include/fdbserver/core/WorkerInterface.actor.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/WorkerInterface.actor.h) | WorkerInterface, ClusterControllerFullInterface |
| `fdbserver/core/include/fdbserver/core/ServerDBInfo.h` | ServerDBInfo structure and broadcasting |
