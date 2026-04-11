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

Typically 3 or 5 processes (addresses hardcoded in cluster file) that run a `leaderRegister` actor. They don't store user data -- only coordination metadata.

### Generation Register -- [`Coordination.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/coordinator/Coordination.actor.cpp)`:67-131`

The fundamental primitive: a register with generation numbers that prevents stale writes.

```
struct GenerationRegVal {
    UniqueGeneration readGen;   // highest read generation
    UniqueGeneration writeGen;  // highest write generation
    Optional<Value> val;        // the coordinated value
};
```

**Read protocol** (`GenerationRegReadRequest`):
- If client's generation > stored readGen: update readGen
- Return (value, writeGen, readGen)

**Write protocol** (`GenerationRegWriteRequest`):
- Check: `readGen <= requestGen AND writeGen < requestGen`
- If valid: update writeGen and value, return writeGen
- If invalid: return `max(readGen, writeGen)` for client retry

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

### Coordinated State -- [`CoordinatedState.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/CoordinatedState.cpp)`:71-210`

Higher-level abstraction over generation registers for cluster-wide state:

```
struct CoordinatedStateImpl {
    ServerCoordinators coordinators;
    UniqueGeneration gen;
    uint64_t conflictGen;
    bool doomed;
};
```

**Read**: Replicated read from quorum of coordinators, returns highest-generation value.
**Write**: Replicated write with generation check. Throws `coordinated_state_conflict` if `wgen != gen`.

Used by recovery to store/update `DBCoreState` (log system configuration).

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

### Recruitment Functions -- `ClusterRecovery.actor.cpp`

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
| [`fdbserver/coordinator/Coordination.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/coordinator/Coordination.actor.cpp) | leaderRegister, generation register, coordination |
| [`fdbserver/core/LeaderElection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/LeaderElection.actor.cpp) | tryBecomeLeaderInternal, candidacy, heartbeat |
| [`fdbserver/core/CoordinatedState.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/CoordinatedState.cpp) | Replicated read/write over generation registers |
| [`fdbserver/core/include/fdbserver/core/WorkerInterface.actor.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/WorkerInterface.actor.h) | WorkerInterface, ClusterControllerFullInterface |
| `fdbserver/core/include/fdbserver/core/ServerDBInfo.h` | ServerDBInfo structure and broadcasting |
