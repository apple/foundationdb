# Subsystem 8: Data Distribution

**[Diagrams](diagram_08_data_distribution.md)**

**Location:** [`fdbserver/datadistributor/`](https://github.com/apple/foundationdb/tree/main/fdbserver/datadistributor)
**Size:** ~22K  
**Role:** Shard management, team building, rebalancing, MoveKeys protocol.

---

## Overview

Data Distribution (DD) is a continuous background process that decides which storage servers hold which key ranges, and moves data when the assignment needs to change. The DD singleton is recruited by the Cluster Controller and runs for the lifetime of the cluster (re-recruited on failure).

---

## Main Actor -- [`DataDistribution.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DataDistribution.cpp)`:2602-2888`

The `dataDistribution()` actor spawns four major components:

1. **DataDistributionTracker** -- monitors shard metrics, triggers splits/merges/moves
2. **DDQueue** -- prioritized queue of shard relocations, executes moves
3. **DDTeamCollection (Primary)** -- builds teams, monitors health, recruits storage servers
4. **DDTeamCollection (Remote)** -- same for remote datacenter (if `usableRegions > 1`)

Plus optional bulk load/dump cores.

### Initialization Sequence

1. `initDDConfigWatch()` -- watch database configuration for changes
2. `DataDistributor::init()` -- load metadata from system keys
3. Resume relocations from previous run
4. Spawn tracker, queue, and team collections
5. `waitForAll(actors)` -- run until fatal error

---

## DDTeamCollection -- [`DDTeamCollection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDTeamCollection.actor.cpp)

### What is a Team?

A "team" is a set of storage servers that together hold replicas of a shard. For `triple` replication, a team has 3 servers in 3 different zones.

### Key Members (`DDTeamCollection.h:209+`)

```
std::vector<Reference<TCTeamInfo>> badTeams;
std::map<UID, Reference<TCServerInfo>> server_and_tss_info;
std::map<Key, int> lagging_zones;
Reference<StorageWiggler> storageWiggler;

int healthyTeamCount, optimalTeamCount;
AsyncVar<bool> zeroHealthyTeams;
Future<Void> teamBuilder;
PromiseStream<RelocateShard> output;          // relocation requests
```

### Team Building

`buildTeams()` creates new teams from healthy servers:
- Each team satisfies the replication policy (e.g., 3 replicas across 3 zones)
- All teams have exactly `teamSize` members
- Respects excluded/failed server lists
- **Machine teams** enforce datacenter locality constraints

### Health Monitoring

**`teamTracker()`** -- per-team actor:
- Watches for server failures (team < replication factor)
- Watches for server exclusion
- Emits `RelocateShard` when team health degrades

**`storageServerTracker()`** -- per-server actor:
- Monitors interface responsiveness, disk space, excluded status
- Marks failed on timeout or storage errors

**`storageRecruiter()`** -- continuous recruitment:
- Recruits new servers when existing ones fail
- Sends `RecruitStorageRequest` to cluster controller

---

## DDShardTracker -- [`DDShardTracker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDShardTracker.cpp)

Monitors shard sizes and operation rates, triggers splits/merges.

### Bandwidth Status (lines 42-58)

| Status | Condition |
|--------|-----------|
| `BandwidthStatusLow` | `bytesWrittenPerKSec < SHARD_MIN_BYTES_PER_KSEC` |
| `BandwidthStatusNormal` | Between min and max |
| `BandwidthStatusHigh` | `bytesWrittenPerKSec > SHARD_MAX_BYTES_PER_KSEC` |

### Shard Size Bounds (lines 85-151)

`calculateShardSizeBounds()` returns bounds that trigger splits/merges:

- **Byte bounds**: `max.bytes = max(currentBytes * 1.1, MIN_SHARD_BYTES)`, permits 10% deviation
- **Write bandwidth bounds**: Based on `SHARD_MIN/MAX_BYTES_PER_KSEC`
- **Read bandwidth bounds**: Based on `SHARD_MAX_READ_DENSITY_RATIO * bytes`
- **Read operation bounds**: Track `opsReadPerKSecond`, detect hot shards

### Tracking Logic (`trackShardMetrics`)

Continuously monitors each shard. When metrics exceed bounds:
- **Split** (high bytes/ops): Create two smaller shards, emit `RelocateShard` with `SPLIT_SHARD`
- **Merge** (low bytes/ops): Merge with adjacent shard, emit `RelocateShard` with `MERGE_SHARD`

---

## DDQueue / DDRelocationQueue -- [`DDRelocationQueue.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDRelocationQueue.actor.cpp)

### Key Members (`DDRelocationQueue.h:125+`)

```
std::set<RelocateData, std::greater<>> fetchingSourcesQueue;
std::map<UID, std::set<RelocateData>> queue;      // per-server queue
KeyRangeMap<RelocateData> inFlight;                // currently executing
KeyRangeActorMap inFlightActors;                   // actor per in-flight move
KeyRangeMap<DDDataMove> dataMoves;                 // move metadata
std::map<UID, Busyness> busymap, destBusymap;     // server utilization
```

### RelocateData (lines 74-122)

```
struct RelocateData {
    int priority;                   // urgency level
    RelocateReason reason;
    DataMovementReason dmReason;
    KeyRange keys;                  // range to move
    std::vector<UID> src;           // source server IDs
    int workFactor;                 // estimated cost
    UID dataMoveId;
    double startTime;
};
```

### Relocation Priorities

| Priority | Trigger |
|----------|---------|
| `PRIORITY_TEAM_REDUNDANT` | Team replicated on same machine |
| `PRIORITY_POPULATE_REGION` | Populate new region |
| `PRIORITY_TEAM_UNHEALTHY` | Unhealthy team |
| `PRIORITY_REBALANCE_*` | Disk/read balancing |
| `PRIORITY_SPLIT_SHARD` | Shard too large/hot |
| `PRIORITY_MERGE_SHARD` | Shard too small/cold |

Ordering: `(priority desc) → (startTime asc) → (randomId desc)`

### Core Operations

- **`queueRelocation()`**: Enqueue shard relocation, search for source team
- **`launchQueuedWork()`**: Check for overlapping in-flight moves, cancel lower priority, start MoveKeys
- **`completeSourceFetch()`**: Source servers identified, move to ready-to-launch

---

## MoveKeys Protocol -- [`fdbserver/core/MoveKeys.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/MoveKeys.cpp)

Atomic shard ownership transfer using system key transactions.

### Overall Flow

1. Destination SS starts `fetchKeys()` -- pulls data from source SS
2. During fetch, mutations forwarded to dest via TLog stream (same tag assignment)
3. Once dest reaches `fetchVersion`: ownership transfers atomically
4. Source removes shard

### System Keys Modified

| Key Range | Change |
|-----------|--------|
| `serverKeysPrefixFor(sourceId)` | Remove key range |
| `serverKeysPrefixFor(destId)` | Add key range |
| `keyServersKey(range)` | Update server list for range |
| `moveKeysLockOwnerKey` | DD process ownership |

### Key Functions

- **`takeMoveKeysLock()`**: Acquire exclusive lock (prevents concurrent moves by different DD instances)
- **`readMoveKeysLock()`**: Validate current DD still holds lock
- **`unassignServerKeys()`**: Remove server from key range in system keys, with coalescing
- **`deleteCheckpoints()`**: Clean up checkpoint metadata after physical shard move

### Conflict Resolution

- Read conflicts added at range boundaries to detect concurrent assignments
- Ensures no mutations lost during ownership transfer
- Transactions retry if conflicts detected

---

## Why Data Moves

| Trigger | Action |
|---------|--------|
| Shard grows too large | Split into smaller shards |
| Storage server fails | Re-replicate shards to healthy servers |
| Load imbalance | Move shards from hot to cold servers |
| Configuration change | Adjust replication factor |
| Server exclusion | Move all shards off excluded server |
| Shard too small | Merge with adjacent shard |

---

## Data Flow

```
DDShardTracker ──metrics exceed bounds──▶ RelocateShard request
                                              │
                                              ▼
DDQueue ──prioritize, dedup, find dest team──▶ launchQueuedWork()
                                              │
                                              ▼
MoveKeys protocol:
  1. Dest SS: fetchKeys() from source SS
  2. Dest SS: start consuming TLog for shard's tag
  3. Dest SS: catches up to current version
  4. Atomic system key transaction: transfer ownership
  5. Source SS: remove shard
```

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/datadistributor/DataDistribution.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DataDistribution.cpp) | Main DD actor, initialization |
| [`fdbserver/datadistributor/DDTeamCollection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDTeamCollection.actor.cpp) | Team building, health monitoring, SS recruitment |
| [`fdbserver/datadistributor/DDShardTracker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDShardTracker.cpp) | Shard metrics, split/merge decisions |
| [`fdbserver/datadistributor/DDRelocationQueue.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDRelocationQueue.actor.cpp) | Relocation queue, prioritization, execution |
| [`fdbserver/core/MoveKeys.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/MoveKeys.cpp) | Atomic shard transfer protocol |
| [`fdbserver/core/DataMovement.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/DataMovement.cpp) | Data move metadata |
| [`fdbserver/datadistributor/ShardsAffectedByTeamFailure.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/ShardsAffectedByTeamFailure.cpp) | Impact analysis for team failures |
