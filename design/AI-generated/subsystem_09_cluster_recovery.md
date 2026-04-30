# Subsystem 9: Cluster Recovery

**[Diagrams](diagram_09_cluster_recovery.md)**

**Location:** [`fdbserver/clustercontroller/ClusterRecovery.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterRecovery.cpp), [`fdbserver/core/RecoveryState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/RecoveryState.h)
**Size:** Part of ClusterController files  
**Role:** 10-state recovery state machine to reconstitute the transaction system after failures.

---

## Overview

Recovery is triggered when the master (sequencer) fails, a network partition heals, or coordinators change. The Cluster Controller drives a 10-state state machine that locks old TLogs, recruits a new transaction system, replays uncommitted data, and resumes accepting commits.

---

## ClusterRecoveryData -- [`ClusterRecovery.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterRecovery.h)`:178-318`

```
struct ClusterRecoveryData {
    UID dbgid;
    Version lastEpochEnd;                        // last committed version in old epoch
    Version recoveryTransactionVersion;          // first version in new epoch
    Optional<int64_t> versionEpoch;
    
    DatabaseConfiguration configuration;
    std::vector<Optional<Key>> primaryDcId, remoteDcIds;
    
    Reference<LogSystem> logSystem;
    IKeyValueStore* txnStateStore;               // in-memory metadata store
    LogSystemDiskQueueAdapter* txnStateLogAdapter;
    
    std::vector<CommitProxyInterface> commitProxies;
    std::vector<GrvProxyInterface> grvProxies;
    std::vector<ResolverInterface> resolvers;
    
    ReusableCoordinatedState cstate;             // coordinated cluster state
    MasterInterface masterInterface;
    
    // Recovery signaling
    Promise<Void> recoveryReadyForCommits;
    Promise<Void> cstateUpdated;
};
```

---

## The 10 Recovery States -- [`RecoveryState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/RecoveryState.h)`:31-42`

```
enum class RecoveryState {
    UNINITIALIZED = 0,
    READING_CSTATE = 1,
    LOCKING_CSTATE = 2,
    RECRUITING = 3,
    RECOVERY_TRANSACTION = 4,
    WRITING_CSTATE = 5,
    ACCEPTING_COMMITS = 6,
    ALL_LOGS_RECRUITED = 7,
    STORAGE_RECOVERED = 8,
    FULLY_RECOVERED = 9
};
```

---

## clusterRecoveryCore() Walk-Through

### State 1: READING_CSTATE (line ~1504)

- Read coordinated state from coordinators
- Learn about previous epoch: `DBCoreState` with TLog configuration
- Validate protocol versions

### State 2: LOCKING_CSTATE (line ~1517)

- Lock coordinated state (increment recovery count)
- Prevents split-brain: old master cannot write new state
- Uses generation register for safe state transitions

### State 3: RECRUITING (line ~1584)

**`recruitEverything()`** (lines 999-1117):
1. Send `RecruitFromConfigurationRequest` to cluster controller
2. CC selects workers based on fitness, locality, exclusions
3. Initialize in parallel:
   - `newCommitProxies()` -- `InitializeCommitProxyRequest` to workers
   - `newGrvProxies()` -- `InitializeGrvProxyRequest`
   - `newResolvers()` -- `InitializeResolverRequest`
   - `newTLogServers()` -- creates new log epoch via `oldLogSystem->newEpoch()`
   - `newSeedServers()` -- seed storage servers (only at version 0)
4. `monitorInitializingTxnSystem()` provides timeout with exponential backoff

**`provisionalMaster()`** (line ~858):
- Provides minimal proxy implementation during recovery
- Serves clients with provisional (limited) responses
- Accepts emergency transactions (configuration mutations)

### State 4: RECOVERY_TRANSACTION (line ~1635)

**`readTransactionSystemState()`** (lines 1139-1270):
Peeks old TLogs to reconstruct metadata:
- `versionEpochKey` -- version epoch
- `minRequiredCommitVersionKey` -- for versionstamped operations
- Configuration keys -- database config
- Tag locality mappings -- datacenter localities
- Server tags and tag history

Calculates:
- `lastEpochEnd` -- last committed version in old epoch
- `recoveryTransactionVersion` -- first version in new epoch

Executes the recovery transaction: writes initial state to the new transaction system.

### State 5: WRITING_CSTATE (line ~1767)

- Write new epoch's `DBCoreState` to coordinators via coordinated state
- Contains: new TLog configuration, recovery count, log system state
- Must succeed at quorum of coordinators

### State 6: ACCEPTING_COMMITS (line ~1802)

- New transaction system is live
- CommitProxies, GrvProxies, Resolvers, TLogs all operational
- Client commits now flow through the new system
- Old TLog data still being consumed by storage servers

### State 7: ALL_LOGS_RECRUITED

Transition when all TLog sets (including remote region) are fully populated.

### State 8: STORAGE_RECOVERED

All old generation TLogs have been fully recruited into the system.

### State 9: FULLY_RECOVERED

- Old TLog data fully consumed by storage servers
- Old generations can be discarded
- `oldLogSystems->get()->stopRejoins()`
- Normal operation

---

## trackTlogRecovery() -- [`ClusterRecovery.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterRecovery.cpp)`:449-533`

Monitors TLog set completeness and drives state transitions:

```
loop {
    // Convert current logSystem to DBCoreState
    self->logSystem->toCoreState(newState);
    
    // Check completeness
    allLogs = newState.tLogs.size() == configuration.expectedLogSets(primaryDcId);
    finalUpdate = !newState.oldTLogData.size() && allLogs;
    
    // Write state durably
    co_await self->cstate.write(newState, finalUpdate);
    
    // Signal readiness
    if (self->recoveryReadyForCommits.canBeSet())
        self->recoveryReadyForCommits.send(Void());
    
    // Broadcast updated recovery state
    self->logSystem->coreStateWritten(newState);
    
    if (finalUpdate) {
        // FULLY_RECOVERED: stop rejoins, start rejoin handler
        co_return;
    }
    
    co_await changed;  // wait for next logSystem state change
}
```

---

## CoordinatedState Protocol

### Read

1. Replicated read from quorum of coordinators
2. Returns highest-generation value
3. If client's generation > stored readGen: updates readGen

### Write

1. Replicated write with generation check
2. `readGen <= requestGen AND writeGen < requestGen` must hold
3. If valid: update value and writeGen
4. If conflict: throws `coordinated_state_conflict`

---

## Recovery Timing

| Knob | Default | Purpose |
|------|---------|---------|
| `CC_RECOVERY_INIT_REQ_TIMEOUT` | 30s | Base timeout for recruitment |
| `CC_RECOVERY_INIT_REQ_TIMEOUT_GROWTH_FACTOR` | 2x | Exponential backoff per unfinished recovery |
| `CC_RECOVERY_INIT_REQ_MAX_TIMEOUT` | 300s | Maximum timeout cap |

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/clustercontroller/ClusterRecovery.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterRecovery.cpp) | clusterRecoveryCore, recruitment, state machine |
| [`fdbserver/clustercontroller/ClusterRecovery.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterRecovery.h) | ClusterRecoveryData struct |
| [`fdbserver/core/include/fdbserver/core/RecoveryState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/RecoveryState.h) | RecoveryState enum |
| [`fdbserver/core/include/fdbserver/core/DBCoreState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/DBCoreState.h) | DBCoreState (coordinated TLog config) |
| [`fdbserver/core/CoordinatedState.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/CoordinatedState.cpp) | Replicated read/write over generation registers |
| [`fdbserver/logsystem/LogSystem.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logsystem/LogSystem.cpp) | newEpoch() for creating new TLog generations |
