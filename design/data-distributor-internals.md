# Data Distribution Internals

This document discusses how data distribution works in FDB.

Data distribution manages the lifetime of storage servers, decides which storage server is responsible for which data range, and ensures data is evenly distributed across all storage servers (SS). This document discusses the internals of data distribution (DD) from three perspectives: components that are the data structures of DD; operations that are the actors changing the states of DD; and mechanisms that realize functionalities of DD.

## Components

**Storage server (`class TCServerInfo`):** DD creates a TCServerInfo object for each storage server (SS). The TCServerInfo includes: (1) the SS’ locality, which includes the processID that is unique to ip:port, the zoneId that specifies which rack the SS is on, and the dcId that specifies which DC the SS is in; (2) the server’s teams, which will be discussed in the following paragraph; (3) the tracker that monitor the status of the server; and (4) extra information related to the server’s interface and preference. A server is healthy if its storage engine on the process is the same with the configured storage engine, and it is marked as desired by DD.

**Machine (`class TCMachineInfo`)**: A machine in FDB is considered as a rack, because a typical FDB cluster will only use one physical host from each rack in the datacenter to reduce the impact of regular rack-maintenance events on the cluster. All servers on the same rack belong to the same machine. A machine is healthy if there exists a healthy server on the machine.

**Server team (`class TCTeamInfo`)**: A server team is a group of *k* servers that host the same key ranges, where *k* is the replication factor that is usually three. A server team is healthy if every server in the team is healthy and those servers’ localities satisfy the replication requirement. Servers are grouped into server teams to reduce the possibility of data unavailability events at the event of *k* server failures.

**Machine team (`class TCMachineTeamInfo`)**: A machine team is a group of k machines, where k is the replication factor. Each server team must be on a machine team, meaning that each server in the server team is on a machine in the machine team and that no two servers are on the same machine. Similar to the purpose of server teams, machine teams are used to reduce the possibility of data unavailability events at the event of *k* machine failures. A machine team is healthy if every machine on the team is healthy and machines’ localities satisfy the replication policy.

**`TeamCollection`**: It has a global view of all servers and server teams, machines and machine teams. With the information, it creates server teams and machine teams. It also maintains the configuration settings for DD, which is used to create teams and decide which type of storage servers to recruit.

**Shard (`struct DDShardInfo`)**: A shard is a key range. A shard is maintained by a server team. A server team is responsible for many shards. Each shard has a similar amount of data. When a shard has too much data or has too much write traffic, it will be split into multiple shards and redistributed to server teams. Likewise, when a shard has too little data, it can be merged with its neighbors.

**RelocateShard (`struct RelocateShard`)**: A `RelocateShard` records the key range that need to be moved among servers and the data movement’s priority. DD always move shards with higher priorities first.

**Data distribution queue (`struct DDQueue`)**: It receives shards to be relocated (i.e., RelocateShards), decides which shard should be moved to which server team, prioritizes the data movement based on relocate shard’s priority, and controls the progress of data movement based on servers’ workload.

**Special keys in the system keyspace**: DD saves its state in the system keyspace to recover from failure and to ensure every process (e.g., commit proxies, tLogs and storage servers) has a consistent view of which storage server is responsible for which key range.

*serverKeys* sub-space (`\xff/serverKeys/`): It records the start key of each shard a server is responsible for. The format is *\xff/serverKeys/[serverID]/[start_key]*. To get start keys of all shards for a server, DD can read the key range with prefix *\xff/serverKeys/[serverID]/* and decode the value of [start_key].

*keyServers* sub-space (`\xff/keyServers/`): It records each key’s source and destination server IDs. The format is *\xff/keyServers/[start_key]/[src_server][dst_server]*, where *[start_key]* is the start key of a shard, *[src_server]* are the servers responsible for the shard, *[dst_server]* are the new servers where the shard will be moved to when relocating shard request is initialized. To get all source and destination servers for the shard, DD can read the key range with the prefix `\xff/keyServers/[start_key]` and decode the value *[src_server][dst_server]*. To get each shard’s boundary, DD can read the key range with the prefix `\xff/keyServers/` and collect all *[start_key]*s. Two consecutive *[start_key]*, say *start_key1* and *start_key2*, construct the key range, say *[start_key1, start_key2)*, for a shard.

*`moveKeysLockOwnerKey`* (`\xff/moveKeysLock/Owner`) and *moveKeysLockWriteKey* (`\xff/moveKeysLock/Write`): When DD moves keys, it must grab the moveKeysLock, which consists of an owner key and a write key. The owner key (i.e., `moveKeysLockOwnerKey`) specifies which DD currently owns the lock. The write key (i.e., `moveKeysLockWriteKey`) specifies which DD is currently changing the mapping between keys and servers (i.e., operating on serverKeys and keyServers subspace). If DD finds it does not own both keys when it tries to move keys, it will kill itself by throwing an error. The cluster controller will recruit a new one.

When a new DD is initialized, it will set itself as the owner by setting its random UID to the `moveKeysLockOwnerKey`. Since the owner key has only one value, at most one DD can own the DD-related system subspace. This avoids the potential race condition between multiple DDs which may co-exist during DD recruitment.

**Transaction State Store (txnStateStore)**: It is a replica of the special keyspace that stores the cluster’s states, such as which SS is responsible for which shard. Because commit proxies use txnStateStore to decide which tLog and SS should receive a mutation, commit proxies must have a consistent view of txnStateStore. Therefore, changes to txnStateStore must be populated to all commit proxies in total order. To achieve that, we use the special transaction (`applyMetaMutations`) to update txnStateStore and use resolvers to ensure the total ordering (serializable snapshot isolation).

**Private mutation**: A private mutation is a mutation updating a special system key, such as keyServersKey (`\xff/keyServers/`) and serverKeysKey (`\xff/serverKeys/`). Like a normal mutation, a private mutation will be processed by the transaction systems (i.e., commit proxy, resolver and tLog) and be routed to a set of storage servers, based on the mutation’s tag, to update the key-value in the storage engine. Private mutations also keep the serializable snapshot isolation and consensus: The results of committed concurrent private mutations can be reproduced by sequentially executing the mutations, and all components in FDB have the same view of the mutations.


## Operations

Operations on the states (and data structure) of DD are done in actors. Each actor is responsible for only a specific task. We will describe the most important actors in this section.

**Storage server tracker (`storageServerTracker`)**: Whenever a storage server is created, a storage server tracker is created for the server. The tracker monitors the status (e.g., healthiness) of the server. When a server becomes unhealthy or the server’s process dies, the tracker issues the request to remove data on the server. Once all data are moved away from the server, the tracker remove the servers’ information from DD. When a server’s storage interface changes -- because the storage process reboots or moved -- the tracker updates the server’s information and changes the server’s teams accordingly to ensure the replication policy is always satisfied.

**Team tracker (`teamTracker`)**: Whenever a server team is created, a team tracker is created to monitor the healthiness of the team. When a healthy team becomes unhealthy, the team tracker will find all shards on the team, create the RelocateShard requests, and send the requests to the dataDistributionQueue.

**Team builder (`buildTeams`)**: Team builder is created when DD is initialized. It is invoked by the following events: (a) a new server is created and added to DD; (b) an existing server is removed from DD; (c) there is zero teams in the system.

Whenever the team builder is invoked, it aims to build the desired number of server teams. To ensure each server team belongs to a machine team, it first builds the desired number of machine teams; it then picks a machine team and picks a server from each machine in the machine team to form a server team.

**Data distribution queue server (`dataDistributionQueue` actor)**: It is created when DD is initialized. It behaves as a server to handle RelocateShard related requests. For example, it waits on the stream of RelocateShard. When a new RelocateShard is sent by teamTracker, it enqueues the new shard, and cancels the inflight shards that overlap with the new relocate shard.

**`applyMetaMutations`**: This is special logic to handle *private transactions* that modify txnStateStore and special system keys. Transaction systems (i.e., commit proxy, resolver and tLogs) and storage servers perform extra operations for the special transactions. For any update, it will be executed on all commit proxies in order so that all commit proxies have a consistent view of the txnStateStore. It will also send special keys to storage servers so that storage servers know the new keyspace they are now responsible for.

A storage server (SS) processes all requests sent to the server in its `storageServerCore` actor. When a (private) mutation request is sent to a SS, the server will call the `update()` function. Eventually, the `StorageUpdater` class will be invoked to apply the mutation in `applyMutation()` function, which handles private mutations `applyPrivateData()` function.

If a new key range is assigned to a storage server, the storage server will receive a private mutation that changes the *serverKeys* (`\xff/serverKeys/`) and *keyServers* (`\xff/keyServers/`). Then the server will create transactions, just like an FDB client, to read key-value pairs in the assigned key range and write the data into its local storage engine.

If a key range is removed from a storage server, similarly the storage server will receive a private mutation that changes the *serverKeys* and *keyServers*. Once the private mutation is processed by the SS, the SS removes data in its versioned data.


## Mechanisms

### How is data distribution initialized?

When a data distribution role is created, it recovers the states of the previous DD from the system keyspace. First, it sets itself as the owner of the `moveKeysLock`. Then it collects the information of servers and shards, the map between servers and shards, and the replication configuration by reading DD-related system keyspace (i.e., *serverKeys* sub-space). Based on the information, the new DD recreates its components (e.g., servers, teams, and trackers) that matches the states of the previous DD. Trackers will evaluate the healthiness of servers and teams based on the replication policy. Unhealthy servers and teams will be removed and new servers and teams will be created if the replication policy is changed.

### When to move keys?

Keys can be moved from a server to another for several reasons:
(1) DD moves keys from disk-overutilized servers to disk-underutilized servers, where a server’s disk-utilization is defined as the server’s disk space usage;
(2) DD moves keys from read-busy servers to read-cold servers if read-aware data distribution is enabled;
(3) DD splits or merges shards in order to rebalance the disk usage of servers;
(4) DD removes redundant teams when the team number is larger than the desired number;
(5) DD repairs the replication factor by duplicate shards from a server to another when servers in a team fail.

Actors are created to monitor the reasons of key movement:
(1) `MountainChopper` and `ValleyFiller` actors periodically measure a random server team’s utilization and rebalance the server’s keys among other servers;
(2) `shardMerger` and `shardSplitter` actors take a shard as input and respectively evaluates if the input shard can be merged with its neighboring shards without creating a too big shard and if the shard should be split. Once new shards are created, the actors create the shard’s tracker and send `RelocateShard` requests to DD’s queue;
(3) `serverTeamRemover` and `machineTeamRemover` actors periodically evaluate if the number of server teams and machine teams is larger than the desired number. If so, they respectively pick a server team or a machine team to remove based on predefined criteria;
(4) `teamTracker` actor monitors a team’s healthiness. When a server in the team becomes unhealthy, it issues the `RelocateShard` request to repair the replication factor. The less servers a team has, the higher priority the `RelocateShard` request will be.

#### Movement Priority
There are roughly 4 class of movement priorities
* Healthy priority. The movement is for maintain the cluster healthy status, and the priority is depended on the healthy status of the source team.
* Load balance priority. The movement is for balance cluster workload.
* Boundary change priority. The movement will change current shard boundaries.
* Others. Like resuming a in-flight movement.

Each shard movement has a priority associating with the move attempt,  The explanation of each priority knob (`PRIORITY_<XXX>`) is in `Knobs.h`.

In `status json` output, please look at field `.data.team_tracker.state` for team priority state.

### How to move keys?

A key range is a shard. A shard is the minimum unit of moving data. The storage server’s ownership of a shard -- which SS owns which shard -- is stored in the system keyspace *serverKeys* (`\xff/serverKeys/`) and *keyServers* (`\xff/keyServers/`). To simplify the explanation, we refer to the storage server’s ownership of a shard as a shard’s ownership.

A shard’s ownership is used in transaction systems (commit proxy and tLogs) to route mutations to tLogs and storage servers. When a commit proxy receives a mutation, it uses the shard’s ownership to decide which *k* tLogs receive the mutation, assuming *k* is the replicas factor. When a storage server pulls mutations from tLogs, it uses the shard’s ownership to decide which shards the SS is responsible for and which tLog the SS should pull the data from.

A shard’s ownership must be consistent across transaction systems and SSes, so that mutations can be correctly routed to SSes. Moving keys from a SS to another requires changing the shard’s ownership under ACID property. The ACID property is achieved by using FDB transactions to change the *serverKeys *(`\xff/serverKeys/`) and *keyServers* (`\xff/keyServers/`). The mutation on the *serverKeys *and* keyServers *will be categorized as private mutations in transaction system. Compared to normal mutation, the private mutations will change the transaction state store (txnStateStore) that maintains the *serverKeys* and *keyServers* for transaction systems (commit proxy and tLog) when it arrives on each transaction component (e.g., tLog). Because mutations are processed in total order with the ACID guarantees, the change to the txnStateStore will be executed in total order on each node and the change on the shard’s ownership will also be consistent.

The data movement from one server (called source server) to another (called destination server) has four steps:
(1) DD adds the destination server as the shard’s new owner;
(2) The destination server will issue transactions to read the shard range and write the key-value pairs back. The key-value will be routed to the destination server and saved in the server’s storage engine;
(3) DD removes the source server from the shard’s ownership by modifying the system keyspace;
(4) DD removes the shard’s information owned by the source server from the server’s team information (i.e., *shardsAffectedByTeamFailure*).

# Read-aware Data Distribution

## Motivation
Before FDB 7.2, when the data distributor wants to rebalance shard, it only considers write bandwidth when choosing source and destination team, and the moved shard is chosen randomly. There are several cases where uneven read distribution from users causes a small subset of servers to be busy with read requests. This motivates the data distributor considering read busyness to minimize the read load unevenness.

## When does read rebalance happen
The data distributor will periodically check whether the read rebalance is needed. The conditions of rebalancing are 
* the **worst CPU usage of source team >= 0.15** , which means the source team is somewhat busy;
* the ongoing relocation is less than the parallelism budget. `queuedRelocation[ priority ] < countLimit (default 50)`;
* the source team is not throttled to be a data movement source team. `( now() - The last time the source team was selected ) * time volume (default 20) > read sample interval (2 min default)`;
* the read load difference between source team and destination team is larger than 30% of the source team load;

## Metrics definition
* READ_LOAD = ceil(READ_BYTES_PER_KSECOND / PAGE_SIZE) 
* READ_IMBALANCE = ( MAX READ_LOAD / AVG READ_LOAD )
* MOVE_SCORE = READ_DENSITY = READ_BYTES_PER_KSECOND / SHARD_BYTE

The aim for read-aware data distributor is to minimize the IMBALANCE while not harm the disk utilization balance.

## Which shard to move
Basically, the MountainChopper will handle read-hot shards distribution with following steps:
1. The MountainChopper chooses **the source team** with the largest READ_LOAD while it satisfies HARD_CONSTRAINT, then check whether rebalance is needed; 
    * Hard constraint:
        * Team is healthy
        * The last time this team was source team is larger than (READ_SAMPLE_INTERVAL / MOVEMENT_PER_SAMPLE)
        * The worst CPU usage of source team >= 0.15
2. Choose the destination team for moving
    * Hard constraint:
        * Team is healthy
        * The team’s available space is larger than the median free space
    * Goals
        * The destination team has the least LOAD in a random team set while it satisfies HARD_CONSTRAINT;
3. Select K shards on the source team of which 
    a. `LOAD(shard) < (LOAD(src) - LOAD(dest)) * READ_REBALANCE_MAX_SHARD_FRAC `; 
    b. `LOAD(shard) > AVG(SourceShardLoad)`; 
    c. with the highest top-K `MOVE_SCORE`; 

    We use 3.a and 3.b to set a eligible shard bandwidth for read rebalance moving. If the upper bound is too large, it’ll just make the hot shard shift to another team but not even the read load. If the upper bound is small, we’ll just move some cold shards to other servers, which is also not helpful. The default value of READ_REBALANCE_MAX_SHARD_FRAC is 0.2 (up to 0.5) which is decided based on skewed workload test.
4. Issue relocation request to move a random shard in the top k set. If the maximum limit of read-balance movement is reached, give up this relocation.

Note: The ValleyFiller chooses a source team from a random set with the largest LOAD, and a destination team with the least LOAD.

## Performance Test and Summary
### Metrics to measure
1. StorageMetrics trace event report “FinishedQueries” which means the current storage server finishes how many read operations. The rate of FinishedQueries is what we measure first. The better the load balance is, the more similar the FinishedQueries rate across all storage servers.
CPU utilization. This metric is in a positive relationship with “FinishedQueries rate”. A even “FinishedQueries” generally means even CPU utilization in the read-only scenario.
2. Data movement size. We want to achieve load balance with as little movement as possible;
3. StandardDeviation(FinishedQueries). It indicates how much difference read load each storage server has.

### Typical Test Setup
120GB data, key=32B, value=200B; Single replica; 8 SS (20%) serves 80% read; 8 SS servers 60% write; 4 servers are both read and write hot;  TPS=100000, 7 read/txn + 1 write/txn; 

### Test Result Summary and Recommendation 
* With intersected sets of read-hot and write-hot servers, read-aware DD even out the read + write load on the double-hot (be both read and write hot) server, which means the converged write load is similar to disk rebalance only algorithm.
* Read-aware DD will balance the read workload under the read-skew scenario. Starting from an imbalance `STD(FinishedQueries per minute)=16k`,the best result it can achieve is `STD(FinishedQueries per minute) = 2k`.
* The typical movement size under a read-skew scenario is 100M ~ 600M under default KNOB value `READ_REBALANCE_MAX_SHARD_FRAC=0.2, READ_REBALANCE_SRC_PARALLELISM = 20`. Increasing those knobs may accelerate the converge speed with the risk of data movement churn, which overwhelms the destination and over-cold the source.
* The upper bound of `READ_REBALANCE_MAX_SHARD_FRAC` is 0.5. Any value larger than 0.5 can result in hot server switching.
* When needing a deeper diagnosis of the read aware DD, `BgDDMountainChopper`, and `BgDDValleyFiller` trace events are where to go.

## Data Distribution Diagnosis Q&A
* Why Read-aware DD hasn't been triggered when there's a read imbalance? 
  * Check `BgDDMountainChopper`, `BgDDValleyFiller` `SkipReason` field.
* The Read-aware DD is triggered, and some data movement happened, but it doesn't help the read balance. Why? 
  * Need to figure out which server is selected as the source and destination. The information is in `BgDDMountainChopper*`, `BgDDValleyFiller*`  `DestTeam` and `SourceTeam` field.
  * Also, the `DDQueueServerCounter` event tells how many times a server being a source or destination (defined in 
  ```c++
  enum CountType : uint8_t { ProposedSource = 0, QueuedSource, LaunchedSource, LaunchedDest };
  ```
  ) for different relocation reason (`Other`, `RebalanceDisk` and so on) in different phase within `DD_QUEUE_COUNTER_REFRESH_INTERVAL` (default 60) seconds. For example, 
  ```xml
  <Event Severity="10" Time="1659974950.984176" DateTime="2022-08-08T16:09:10Z" Type="DDQueueServerCounter" ID="0000000000000000" ServerId="0000000000000004" OtherPQSD="0 1 3 2" RebalanceDiskPQSD="0 0 1 4" RebalanceReadPQSD="2 0 0 5" MergeShardPQSD="0 0 1 0" SizeSplitPQSD="0 0 5 0" WriteSplitPQSD="1 0 0 0" ThreadID="9733255463206053180" Machine="0.0.0.0:0" LogGroup="default" Roles="TS" />
  ```
  `RebalanceReadPQSD="2 0 0 5"` means server `0000000000000004` has been selected as for read balancing for twice, but it's not queued and executed yet. This server also has been a destination for read balancing for 5 times in the past 1 min. Note that the field will be skipped if all 4 numbers are 0. To avoid spammy traces, if is enabled with knob `DD_QUEUE_COUNTER_SUMMARIZE = true`, event `DDQueueServerCounterTooMany` will summarize the unreported servers that involved in launched relocations (aka. `LaunchedSource`, `LaunchedDest` count are non-zero):
    ```xml
    <Event Severity="10" Time="1660095057.995837" DateTime="2022-08-10T01:30:57Z" Type="DDQueueServerCounterTooMany" ID="0000000000000000" RemainedLaunchedSources="000000000000007f,00000000000000d9,00000000000000e8,000000000000014c,0000000000000028,00000000000000d6,0000000000000067,000000000000003e,000000000000007d,000000000000000a,00000000000000cb,0000000000000106,00000000000000c1,000000000000003c,000000000000016e,00000000000000e4,000000000000013c,0000000000000016,0000000000000179,0000000000000061,00000000000000c2,000000000000005a,0000000000000001,00000000000000c9,000000000000012a,00000000000000fb,0000000000000146," RemainedLaunchedDestinations="0000000000000079,0000000000000115,000000000000018e,0000000000000167,0000000000000135,0000000000000139,0000000000000077,0000000000000118,00000000000000bb,0000000000000177,00000000000000c0,000000000000014d,000000000000017f,00000000000000c3,000000000000015c,00000000000000fb,0000000000000186,0000000000000157,00000000000000b6,0000000000000072,0000000000000144," ThreadID="1322639651557440362" Machine="0.0.0.0:0" LogGroup="default" Roles="TS" />
    ```
* How to track the lifecycle of a relocation attempt for balancing?
  * First find the TraceId fields in `BgDDMountainChopper*`, `BgDDValleyFiller*`, which indicates a relocation is triggered.
  * (Only when enabled) Find the `QueuedRelocation` event with the same `BeginPair` and `EndPair` as the original `TraceId`. This means the relocation request is queued.
  * Find the `RelocateShard` event whose `BeginPair`, `EndPair` field is the same as `TraceId`. This event means the relocation is ongoing.


# Alternative View -- AI-generated for FDB 7.3

The following was generated by an AI assistant with access to the FDB source repository, including the documentation above in this file.

## 1. Role in the Cluster

The Data Distributor (DD) is a singleton role recruited by the Cluster Controller. It is
the only component authorized to mutate the shard-to-server mapping stored in the system
keyspace. Its responsibilities:

1. **Shard lifecycle management** -- splitting shards that grow too large or too hot, merging
   shards that shrink below thresholds.
2. **Team formation** -- building and maintaining server teams (groups of `k` storage servers,
   where `k` = replication factor) and machine teams that satisfy the replication policy.
3. **Data movement** -- relocating shards to repair replication (after server failures), to
   rebalance disk/read/write load, or to carry out operational directives (exclusions, wiggle).
4. **Audit & bulk operations** -- storage audits and bulk load/dump coordination (7.3 additions).

DD is a single point of coordination -- if it crashes, the Cluster Controller recruits a new
one which recovers state from the system keyspace. During the gap, no new data movement occurs,
but existing storage servers continue to serve reads/writes normally.

### Singleton Guarantee: MoveKeysLock

Only one DD instance can modify the shard map at a time, enforced by `MoveKeysLock`:

```
moveKeysLockOwnerKey  (\xff/moveKeysLock/Owner)  -- which DD owns the lock
moveKeysLockWriteKey  (\xff/moveKeysLock/Write)   -- which DD is actively mutating
```

When a new DD starts, it sets its UID as the owner. If during a `moveKeys` operation it
discovers another DD has taken ownership, it throws `movekeys_conflict` and terminates. This
prevents split-brain corruption of the shard map.

Source: [`MoveKeys.actor.h:37`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/MoveKeys.actor.h#L37) (`MoveKeysLock`),
[`DataDistribution.actor.cpp:511`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DataDistribution.actor.cpp#L511) (`takeMoveKeysLock`).


## 2. Top-Level Actor Topology

```
dataDistribution()                               [DataDistribution.actor.cpp:889]
  ├─ DataDistributor::init()                     -- recover state from system keyspace
  ├─ resumeRelocations()                         -- resume in-flight data moves
  │
  ├─ DataDistributionTracker::run()              -- shard health monitoring
  │    ├─ shardTracker (per shard)               -- monitors size/bandwidth/read metrics
  │    ├─ shardSplitter                          -- splits oversized/overloaded shards
  │    ├─ shardMerger                            -- merges underutilized neighbors
  │    └─ readHotDetection                       -- identifies read-hot shards
  │
  ├─ DDQueue::run()                              -- relocation scheduling
  │    ├─ dataDistributionRelocator              -- per-relocation actor
  │    ├─ BgDDLoadRebalance (MC disk)            -- MountainChopper for disk
  │    ├─ BgDDLoadRebalance (VF disk)            -- ValleyFiller for disk
  │    ├─ BgDDLoadRebalance (MC read)            -- MountainChopper for read
  │    ├─ BgDDLoadRebalance (VF read)            -- ValleyFiller for read
  │    └─ periodicalRefreshCounter               -- refresh DDQueueServerCounter metrics
  │
  ├─ DDTeamCollection::run() [primary]           -- primary DC team management
  │    ├─ buildTeams                             -- create server/machine teams
  │    ├─ storageServerTracker (per SS)          -- monitor SS health
  │    ├─ teamTracker (per team)                 -- monitor team health, emit RelocateShards
  │    ├─ serverTeamRemover                      -- prune excess server teams
  │    ├─ machineTeamRemover                     -- prune excess machine teams
  │    ├─ storageRecruiter                       -- recruit new storage servers
  │    ├─ monitorPerpetualStorageWiggle          -- perpetual wiggle orchestration
  │    │    ├─ perpetualStorageWiggleIterator
  │    │    └─ perpetualStorageWiggler
  │    ├─ trackExcludedServers                   -- process exclusion list
  │    └─ monitorHealthyTeams                    -- track healthy team count
  │
  └─ DDTeamCollection::run() [remote]            -- remote DC (if usableRegions > 1)
       └─ (same actors as primary)
```

Source: [`dataDistribution()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DataDistribution.actor.cpp#L889).


## 3. Core Data Structures

### 3.1 Storage Server Info (`TCServerInfo`)

Defined in [`TCInfo.h`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/TCInfo.h). One per storage server tracked by DD. Contains:
- **Locality**: processID (unique to ip:port), zoneId (rack), dcId (datacenter)
- **Teams**: list of `TCTeamInfo` references this server belongs to
- **Metrics**: disk usage, CPU, storage queue length
- **Status**: [`ServerStatus`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDTeamCollection.h#L139) in `DDTeamCollection.h` tracks `isFailed`, `isUndesired`,
  `isWiggling`, `isWrongConfiguration`

A server is "healthy" if its storage engine matches the configured engine AND it is marked
as desired by DD. See `ServerStatus::isUnhealthy()` at [`DDTeamCollection.h:152`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDTeamCollection.h#L152).

### 3.2 Machine Info (`TCMachineInfo`)

A "machine" in FDB represents a **rack** -- the assumption is one physical host per rack.
All servers sharing a `zoneId` locality belong to the same machine. Defined in [`TCInfo.h`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/TCInfo.h).

### 3.3 Server Teams (`TCTeamInfo`)

A group of `k` servers (where `k` = `storageTeamSize`, typically 3) that replicate the same
shards. Implements `IDataDistributionTeam`. A team is healthy iff every server is healthy
AND the team satisfies the replication policy.

### 3.4 Machine Teams (`TCMachineTeamInfo`)

A group of `k` machines. Every server team must map onto a machine team (one server per
machine in the team). This ensures that server teams span distinct failure domains.

### 3.5 Team Collection (`DDTeamCollection`)

Defined in [`DDTeamCollection.h:205`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDTeamCollection.h#L205). The global view of all servers, server teams, machines,
and machine teams for one datacenter. Key data members:

```cpp
std::map<UID, Reference<TCServerInfo>> server_info;        // all servers
std::vector<Reference<TCTeamInfo>> teams;                  // all server teams
std::map<Standalone<StringRef>, Reference<TCMachineInfo>> machine_info;
std::vector<Reference<TCMachineTeamInfo>> machineTeams;
```

There is one `DDTeamCollection` per DC (primary + optionally remote).

### 3.6 Shards (`DDShardInfo`)

A shard is a contiguous key range assigned to a server team. Defined in
[`DataDistribution.actor.h:483`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DataDistribution.actor.h#L483):

```cpp
struct DDShardInfo {
    Key key;                      // start key of this shard
    std::vector<UID> primarySrc;  // current source servers (primary DC)
    std::vector<UID> remoteSrc;   // current source servers (remote DC)
    std::vector<UID> primaryDest; // destination servers during move
    std::vector<UID> remoteDest;
    bool hasDest;                 // true if a move is in progress
    UID srcId, destId;            // data move IDs
};
```

Shard boundaries are stored in the system keyspace:
- `\xff/keyServers/[start_key]` → source and destination server IDs
- `\xff/serverKeys/[serverID]/[start_key]` → which shards a server owns

### 3.7 Shard Size Bounds

Shard size is **workload-driven**, not fixed. The maximum shard size scales with database size:

```cpp
int64_t getMaxShardSize(double dbSizeEstimate) {
    return min((MIN_SHARD_BYTES + sqrt(max(dbSizeEstimate, 0)) * SHARD_BYTES_PER_SQRT_BYTES)
               * SHARD_BYTES_RATIO, MAX_SHARD_BYTES);
}
```

| DB Size Estimate | Max Shard Size |
|------------------|----------------|
| 0                | ~40 MB         |
| 1 GB             | ~46 MB         |
| 100 GB           | ~97 MB         |
| 1 TB             | ~220 MB        |
| >=6.5 TB         | 500 MB (cap)   |

Source: [`DataDistribution.actor.h:518`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DataDistribution.actor.h#L518) (`ShardSizeBounds`), documented in [`design/hotshard.md`](https://github.com/apple/foundationdb/blob/release-7.3/design/hotshard.md).

### 3.8 RelocateShard

The fundamental unit of work for data movement. Defined in [`DataDistribution.actor.h:147`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DataDistribution.actor.h#L147):

```cpp
struct RelocateShard {
    KeyRange keys;
    int priority;
    bool cancelled;
    std::shared_ptr<DataMove> dataMove;  // non-null if restoring a prior move
    UID dataMoveId;
    RelocateReason reason;
    DataMovementReason moveReason;
    UID traceId;                         // for lifecycle tracking
};
```

### 3.9 DDSharedContext

The shared state container connecting all DD sub-components. Defined in [`DDSharedContext.h:31`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDSharedContext.h#L31):

```cpp
class DDSharedContext {
    std::unique_ptr<DDEnabledState> ddEnabledState;
    DataDistributorInterface interface;
    UID ddId;
    MoveKeysLock lock;
    DatabaseConfiguration configuration;
    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
    Reference<DataDistributionTracker> tracker;
    Reference<DDQueue> ddQueue;
    Reference<DDTeamCollection> primaryTeamCollection, remoteTeamCollection;
};
```


## 4. Initialization Flow

When a new DD is recruited ([`DataDistribution.actor.cpp:889`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DataDistribution.actor.cpp#L889)):

1. **Open database connection** -- `openDBOnServer()` with `TaskPriority::DataDistributionLaunch`
2. **Initialize config watcher** -- `initDDConfigWatch()` establishes baseline
3. **`DataDistributor::init()`** ([line 931](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DataDistribution.actor.cpp#L931)) -- reads system keyspace to recover:
   - Current shard boundaries from `\xff/keyServers/`
   - Server-shard assignments from `\xff/serverKeys/`
   - Active storage servers and their interfaces from `\xff/serverList/`
   - Cluster configuration (replication mode, storage engine, etc.)
   - Takes ownership of `moveKeysLock`
4. **Resume relocations** -- [`resumeRelocations()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DataDistribution.actor.cpp#L806) reads persisted `DataMoveMetaData` to
   resume any in-flight data moves from the previous DD
5. **Start sub-components** in parallel:
   - `DataDistributionTracker::run()` -- begins shard monitoring
   - `DDQueue::run()` -- begins processing relocation requests
   - `DDTeamCollection::run()` (primary) -- team management for primary DC
   - `DDTeamCollection::run()` (remote) -- if `usableRegions > 1`
   - `pollMoveKeysLock()` -- periodic lock validation
   - Physical shard monitoring (if enabled)


## 5. Data Movement Priority System

Every relocation request has a priority that determines scheduling order. Higher values
take precedence. The full priority table from [`ServerKnobs.cpp:155-173`](https://github.com/apple/foundationdb/blob/release-7.3/fdbclient/ServerKnobs.cpp#L155):

| Priority | Value | DataMovementReason                    | Category      |
|----------|-------|---------------------------------------|---------------|
| `PRIORITY_RECOVER_MOVE`                | 110  | `RECOVER_MOVE`                 | Recovery      |
| `PRIORITY_REBALANCE_UNDERUTILIZED_TEAM`| 120  | `REBALANCE_UNDERUTILIZED_TEAM` | Load Balance  |
| `PRIORITY_REBALANCE_READ_UNDERUTIL_TEAM`| 121 | `REBALANCE_READ_UNDERUTIL_TEAM`| Load Balance  |
| `PRIORITY_REBALANCE_OVERUTILIZED_TEAM` | 122  | `REBALANCE_OVERUTILIZED_TEAM`  | Load Balance  |
| `PRIORITY_REBALANCE_READ_OVERUTIL_TEAM`| 123  | `REBALANCE_READ_OVERUTIL_TEAM` | Load Balance  |
| `PRIORITY_REBALANCE_STORAGE_QUEUE`     | 124  | `REBALANCE_STORAGE_QUEUE`      | Load Balance  |
| `PRIORITY_TEAM_HEALTHY`                | 140  | `TEAM_HEALTHY`                 | Health        |
| `PRIORITY_PERPETUAL_STORAGE_WIGGLE`    | 141  | `PERPETUAL_STORAGE_WIGGLE`     | Maintenance   |
| `PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER`| 150 | `TEAM_CONTAINS_UNDESIRED_SERVER`| Health       |
| `PRIORITY_TEAM_REDUNDANT`              | 200  | `TEAM_REDUNDANT`               | Health        |
| `PRIORITY_MERGE_SHARD`                 | 340  | `MERGE_SHARD`                  | Boundary      |
| `PRIORITY_POPULATE_REGION`             | 600  | `POPULATE_REGION`              | Health        |
| `PRIORITY_TEAM_UNHEALTHY`             | 700  | `TEAM_UNHEALTHY`               | Health        |
| `PRIORITY_TEAM_2_LEFT`                 | 709  | `TEAM_2_LEFT`                  | Health        |
| `PRIORITY_TEAM_1_LEFT`                 | 800  | `TEAM_1_LEFT`                  | Health        |
| `PRIORITY_TEAM_FAILED`                 | 805  | `TEAM_FAILED`                  | Health        |
| `PRIORITY_TEAM_0_LEFT`                 | 809  | `TEAM_0_LEFT`                  | Health        |
| `PRIORITY_SPLIT_SHARD`                 | 950  | `SPLIT_SHARD`                  | Boundary      |
| `PRIORITY_ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD` | 960 | Physical shard enforcement | Boundary |

Priority categories from [`DDRelocationQueue.h:41-86`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDRelocationQueue.h#L41) (`RelocateData` class):
- **Health priorities**: repairs to replication factor, team health
- **Boundary priorities**: shard splits and merges
- **Load balance priorities**: disk/read/write rebalancing (lowest urgency)

Source: [`DDRelocationQueue.actor.cpp:77-101`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDRelocationQueue.actor.cpp#L77) for the complete mapping.


## 6. Shard Tracker

The [`DataDistributionTracker`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDShardTracker.h#L54) (defined in `DDShardTracker.h`) monitors every shard's
metrics and initiates splits, merges, and relocations.

### 6.1 Shard Splitting

Triggered when a shard exceeds size or write bandwidth thresholds:
- **Size split**: `shard.bytes > maxShardSize` → `PRIORITY_SPLIT_SHARD (950)`
- **Write split**: `bytesWrittenPerKSecond > SHARD_MAX_BYTES_PER_KSEC (1 GB/s)`

The split process:
1. Tracker calls `splitStorageMetrics` on the storage servers hosting the shard
2. Storage servers use their sampled byte/write histograms to compute split points
3. `getSplitKey()` adds jitter to avoid picking the same boundary repeatedly
4. Tracker calls `executeShardSplit` to update the shard map and issue relocations

Split constraints:
- Each resulting piece must have bandwidth < `SHARD_SPLIT_BYTES_PER_KSEC (250 MB/s)`
- Minimum shard size enforced: `MIN_SHARD_BYTES (10 MB by default)`
- If the shard is too small to split (e.g., single hot key), no split occurs

Source: [`shardSplitter()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDShardTracker.actor.cpp#L852), [`StorageMetrics.actor.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/StorageMetrics.actor.cpp).

### 6.2 Shard Merging

Triggered when a shard's write bandwidth drops below `SHARD_MIN_BYTES_PER_KSEC (100 MB/s)`
and its size is small enough that merging with a neighbor stays under the max shard size.
Priority: `PRIORITY_MERGE_SHARD (340)`.

### 6.3 Read-Hot Shard Detection

When `READ_SAMPLING_ENABLED=true`, storage servers sample read operations. Shards exceeding
`SHARD_MAX_READ_OPS_PER_KSEC (45k ops/s)` or having high read density ratios are flagged.
The tracker emits `ReadHotRangeLog` trace events. Read-hot shards trigger **rebalance moves**
(not splits) -- DD moves the shard to a less-loaded team.

Source: [`StorageMetrics.actor.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/StorageMetrics.actor.cpp), [`design/hotshard.md`](https://github.com/apple/foundationdb/blob/release-7.3/design/hotshard.md).


## 7. Relocation Queue (DDQueue)

[`DDQueue`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDRelocationQueue.h#L113) (defined in `DDRelocationQueue.h`) is the scheduler for all data movements.
It receives `RelocateShard` requests from the tracker and team collection, prioritizes them,
selects destination teams, and launches the actual data transfers.

### 7.1 Queue Data Structures

```cpp
KeyRangeMap<RelocateData> queueMap;                               // all queued relocations
std::set<RelocateData, greater> fetchingSourcesQueue;             // fetching source info
std::set<RelocateData, greater> fetchKeysComplete;                // ready to launch
std::map<UID, std::set<RelocateData, greater>> queue;             // per-server queue
KeyRangeMap<RelocateData> inFlight;                               // actively moving
std::map<UID, Busyness> busymap;                                  // source server busyness
std::map<UID, Busyness> destBusymap;                              // dest server busyness
std::map<int, int> priority_relocations;                          // count by priority
```

Source: [`DDRelocationQueue.h:113-307`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDRelocationQueue.h#L113).

### 7.2 Throttling

The [`Busyness`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DDRelocationQueue.h#L86) struct tracks per-server load across 10 priority
buckets. Before launching a relocation, DDQueue checks:
- Source server busyness: `RELOCATION_PARALLELISM_PER_SOURCE_SERVER`
- Dest server busyness: `RELOCATION_PARALLELISM_PER_DEST_SERVER`
- Total move keys parallelism: `DD_MOVE_KEYS_PARALLELISM`

Inflight penalties increase apparent busyness based on relocation health status:
- `INFLIGHT_PENALTY_HEALTHY`, `INFLIGHT_PENALTY_REDUNDANT`, `INFLIGHT_PENALTY_UNHEALTHY`,
  `INFLIGHT_PENALTY_ONE_LEFT`

### 7.3 Relocation Lifecycle

Each relocation goes through:
1. **Queued** → `queueRelocation()` -- overlapping in-flight moves are cancelled
2. **Fetch sources** → `fetchSourceServersComplete` -- identify current shard owners
3. **Team selection** → `getSrcDestTeams()` → `GetTeamRequest` to `DDTeamCollection`
4. **Launch** → [`dataDistributionRelocator()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDRelocationQueue.actor.cpp#L1302)
5. **Move keys** → `moveKeys()` in [`MoveKeys.actor.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L3180) -- the actual data transfer:
   - Add destination as new shard owner (private mutation to `keyServers`/`serverKeys`)
   - Destination SS reads data from sources via FDB transactions
   - Remove source from shard ownership
   - Update `ShardsAffectedByTeamFailure`
6. **Complete** → `relocationComplete` stream


## 8. Load Rebalancing: MountainChopper & ValleyFiller

DD continuously rebalances load using two complementary algorithms implemented in a single
actor, [`BgDDLoadRebalance`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDRelocationQueue.actor.cpp#L2281):

### MountainChopper (Overutilized → Underutilized)

- **Source selection**: `TeamSelect::WANT_TRUE_BEST` -- picks the most loaded team
- **Dest selection**: `TeamSelect::ANY` with `PreferLowerDiskUtil::True`
- **DataMovementReason**: `REBALANCE_OVERUTILIZED_TEAM` (disk) or `REBALANCE_READ_OVERUTIL_TEAM` (read)
- For read rebalancing, calls `rebalanceReadLoad()` which selects top-K shards by read density

### ValleyFiller (Fill Underutilized Teams)

- **Source selection**: `TeamSelect::ANY` -- picks a random loaded team
- **Dest selection**: `TeamSelect::WANT_TRUE_BEST` with `PreferLowerDiskUtil::True` -- picks
  the least loaded team
- **DataMovementReason**: `REBALANCE_UNDERUTILIZED_TEAM` (disk) or `REBALANCE_READ_UNDERUTIL_TEAM` (read)

### Rebalancing Controls

- Polling interval: `BG_REBALANCE_POLLING_INTERVAL` (adaptive, backed off on repeated no-ops)
- Parallelism cap: `DD_REBALANCE_PARALLELISM` -- max concurrent rebalance moves per priority
- Can be disabled at runtime via special database key (checked via `getSkipRebalanceValue()`)
- Read rebalance CPU threshold: `READ_REBALANCE_CPU_THRESHOLD = 0.15` (source team worst CPU)
- Read rebalance shard selection: top-K by `READ_DENSITY = READ_BYTES_PER_KSEC / SHARD_BYTES`
  with constraint `LOAD(shard) < (LOAD(src) - LOAD(dest)) * READ_REBALANCE_MAX_SHARD_FRAC`

Source: [`BgDDLoadRebalance`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDRelocationQueue.actor.cpp#L2281),
[`isDataMovementForMountainChopper`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDRelocationQueue.actor.cpp#L58),
[`isDataMovementForValleyFiller`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDRelocationQueue.actor.cpp#L69).


## 9. Team Building

The team builder ([`buildTeams`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDTeamCollection.actor.cpp#L706) in `DDTeamCollection`) is invoked when:
- A new server is added
- A server is removed
- Zero teams exist

The algorithm:
1. **Build machine teams** using `addBestMachineTeams()`:
   - Uses `ReplicationPolicy::selectReplicas()` to find machine combinations
   - Target count: `DESIRED_TEAMS_PER_SERVER * serverCount / teamSize`
2. **Build server teams** using `addTeamsBestOf()`:
   - For each machine team, pick one server from each machine
   - Prefer the server with the fewest existing teams (`findOneLeastUsedServer`)
   - Verify the team satisfies the replication policy

### Team Health Monitoring

**[`teamTracker`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DDTeamCollection.actor.cpp#L860)** (per team): When a team's health changes, it evaluates:
- If all servers healthy → no action
- If a server is undesired → `PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER (150)`
- If team is redundant → `PRIORITY_TEAM_REDUNDANT (200)`
- If team is unhealthy → `PRIORITY_TEAM_UNHEALTHY (700)`
- If only 2 replicas left → `PRIORITY_TEAM_2_LEFT (709)`
- If only 1 replica left → `PRIORITY_TEAM_1_LEFT (800)`
- If team failed → `PRIORITY_TEAM_FAILED (805)`
- If 0 replicas left → `PRIORITY_TEAM_0_LEFT (809)`

The priority escalation drives DDQueue to process the most critical relocations first.

**`storageServerTracker`** (per SS): Monitors each server's health, storage engine type,
locality validity. When a server fails, it triggers data moves off all teams containing
that server.

### Team Pruning

- `serverTeamRemover`: periodically removes the server team whose members are on the most
  teams when total team count > desired. Threshold: `TR_REDUNDANT_TEAM_PERCENTAGE_THRESHOLD`.
- `machineTeamRemover`: removes machine teams with the most overlapping members when count
  exceeds desired.


## 10. Perpetual Storage Wiggle

The "wiggle" gradually cycles data off each storage server in turn, enabling rolling
maintenance and ensuring no server holds stale data indefinitely.

### Components

- **[`StorageWiggler`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DataDistribution.actor.h#L563)**: Maintains a min-heap priority queue
  of servers ordered by `StorageMetadataType` (creation time). Servers with the oldest metadata
  are wiggled first.
- **`perpetualStorageWiggleIterator`**: Selects the next server to wiggle and writes its ID to
  `perpetualStorageWiggleIDPrefix` in the system keyspace.
- **`perpetualStorageWiggler`**: Watches the wiggle ID key, excludes the target server (causing
  DD to move data off it), waits for completion, then includes it again and signals the iterator.
- **`clusterHealthCheckForPerpetualWiggle`**: Pauses wiggling if the cluster becomes unhealthy.

### Configuration

- Enable: `configure perpetual_storage_wiggle=1`
- Target specific locality: `configure perpetual_storage_wiggle_locality=<KEY>:<VALUE>`
- Wiggle pause threshold: `DD_STORAGE_WIGGLE_PAUSE_THRESHOLD`
- Wiggle stuck threshold: `DD_STORAGE_WIGGLE_STUCK_THRESHOLD`
- Min bytes balance ratio: `PERPETUAL_WIGGLE_MIN_BYTES_BALANCE_RATIO`
- Min SS age before wiggle: `DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC`

### Wiggle States

From `StorageWiggler::State`:
- `INVALID (0)`: Not initialized
- `RUN (1)`: Actively wiggling
- `PAUSE (2)`: Paused due to cluster health

The wiggler state and timestamp are queryable via `GetStorageWigglerStateRequest` RPC.


## 11. Physical Shard Collection (Experimental)

[`PhysicalShardCollection`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DataDistribution.actor.h#L277) introduces a higher-level grouping
where multiple logical shards (key ranges) share the same server team. This reduces metadata
overhead and enables more efficient bulk moves.

Key concepts:
- A **physical shard** contains one or more key ranges (logical shards) on the same team
- Two-step team selection: (1) select a physical shard for the primary team, (2) find the
  remote team sharing that physical shard
- Physical shards track aggregate metrics and can be oversized, triggering
  `PRIORITY_ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD (960)` moves

Enabled via: `SHARD_ENCODE_LOCATION_METADATA=true` + `ENABLE_DD_PHYSICAL_SHARD=true`

When DD restarts, all key ranges start in the "anonymous" physical shard and are gradually
transitioned out as data moves occur.


## 12. Multi-Datacenter Architecture

With `usableRegions > 1`, DD maintains two `DDTeamCollection` instances:

1. **Primary Team Collection** -- manages teams in `primaryDcId`
2. **Remote Team Collection** -- manages teams in `remoteDcIds`

The remote collection only starts after `remoteRecovered()` signals that the remote region
has completed recovery.

Every shard requires a team in both DCs. The DDQueue coordinates
relocations across both DCs -- when selecting destination teams, it requests teams from
both collections and ensures consistency.

`ShardsAffectedByTeamFailure` tracks teams with a `primary` flag to distinguish:
```cpp
struct Team {
    std::vector<UID> servers;
    bool primary;
};
```


## 13. Storage Server Recruitment

`storageRecruiter` in `DDTeamCollection` handles recruiting new storage servers:
- Sends `RecruitStorageRequest` to the Cluster Controller
- Validates the candidate's locality against the replication policy
- Optionally pairs with a Testing Storage Server (TSS) via `TSSPairState`
- Calls `initializeStorage()` to register the new SS

The recruiter is debounced via `restartRecruiting` to avoid thundering herd during
rapid server changes.


## 14. Shard Map Metadata Lifecycle: `keyServers` and `serverKeys`

The shard-to-server mapping is the most critical metadata in the cluster. It is stored as
two complementary indexes in the system keyspace and flows through every major component.

### 14.1 Schema

**`keyServers`** (`\xff/keyServers/` prefix, defined in [`SystemData.h`](https://github.com/apple/foundationdb/blob/release-7.3/fdbclient/include/fdbclient/SystemData.h)):

```
\xff/keyServers/[begin_key] → encoded(src_UIDs[], dest_UIDs[])
```

Maps each shard's start key to its current source servers (who own the data) and destination
servers (who are receiving the data during a move). When no move is in progress, `dest` is
empty. The encoding supports both UID-based and Tag-based formats, plus optional shard IDs
when `SHARD_ENCODE_LOCATION_METADATA` is enabled.

Key encoding/decoding functions in [`SystemData.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbclient/SystemData.cpp):
- `keyServersKey(k)` -- constructs the key
- `keyServersValue(UIDtoTagMap, src, dest)` -- encodes src/dest with tags
- `decodeKeyServersValue(serverTagMap, value, src, dest)` -- decodes

**`serverKeys`** (`\xff/serverKeys/` prefix, defined in [`SystemData.h`](https://github.com/apple/foundationdb/blob/release-7.3/fdbclient/include/fdbclient/SystemData.h)):

```
\xff/serverKeys/[serverID]/[begin_key] → serverKeysTrue | serverKeysFalse
```

The inverse index: for each storage server, records which shard ranges it owns.
`serverKeysTrue` means the server owns that range; `serverKeysFalse` means it doesn't.
When `SHARD_ENCODE_LOCATION_METADATA` is enabled, the value also encodes the `DataMoveType`,
`DataMoveId`, and `DataMovementReason`.

Key functions in [`SystemData.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbclient/SystemData.cpp):
- `serverKeysPrefixFor(serverID)` -- prefix for all shards on a server
- `serverKeysKey(serverID, key)` -- specific shard assignment key
- `decodeServerKeysValue(value, nowAssigned, emptyRange, type, id, reason)` -- decodes

### 14.2 Bootstrap: `seedShardServers`

At initial database creation, the master calls [`seedShardServers()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L3202)
to write the very first shard map:

```cpp
// 1. Set keyServers: all initial servers own the entire keyspace
auto ksValue = keyServersValue(serverTags, serverSrcUID);
tr.set(arena, keyServersKey(allKeys.begin), ksValue);     // \xff/keyServers/\x00
tr.set(arena, keyServersKey(allKeys.end), ...);            // \xff/keyServers/\xff

// 2. Set serverKeys: each server owns everything
for (auto& s : servers) {
    tr.set(arena, serverKeysKey(s.id(), allKeys.begin), serverKeysTrue);
}
```

This is committed as the very first transaction (`read_snapshot = 0`), establishing the
invariant that every key in the database is assigned to a team.

### 14.3 The moveKeys Protocol in Detail

The data transfer protocol is implemented in [`MoveKeys.actor.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp) and consists of three
coordinated phases. The [`moveKeys()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L3180) function orchestrates them:

```cpp
wait(rawStartMovement(occ, params, tssMapping));   // Phase 1: startMoveKeys
wait(rawCheckFetchingState(occ, params, ...));      // Phase 2: monitor data copy
wait(rawFinishMovement(occ, params, tssMapping));   // Phase 3: finishMoveKeys
```

#### Phase 1: `startMoveKeys` ([`MoveKeys.actor.cpp:924`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L924))

This phase sets up the destination servers and tells them to begin fetching data.

1. **Acquire lock**: `checkMoveKeysLock()` verifies this DD still owns the lock
2. **Read existing shard map**: `krmGetRanges(tr, keyServersPrefix, keys)` fetches all
   current shard assignments overlapping the move range
3. **Validate destinations**: confirms each destination server exists in `serverList`;
   throws `move_to_removed_server` if any are gone
4. **For each overlapping sub-shard**:
   - Decode existing `src` and `dest` from `keyServers`
   - **Write new `keyServers`**: [`krmSetPreviouslyEmptyRange`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L1044) sets `dest` to the new destination team while
     preserving `src`
   - Track old destinations that need cleanup
5. **Remove stale destination entries**: for old `dest` servers no longer needed,
   `removeOldDestinations()` sets their `serverKeys` to `serverKeysFalse`
6. **Write new `serverKeys` for destinations**: [`krmSetRangeCoalescing`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L1082) for each destination server
7. **Commit transaction**

This transaction may be split into multiple batches if there are many overlapping sub-shards
(`MOVE_KEYS_KRM_LIMIT` controls batch size). Each batch advances the `begin` cursor.

**Important**: at this point, `keyServers[range]` has both `src` (old team) and `dest`
(new team). Both teams serve reads for this shard. The commit of this transaction triggers
the private mutation propagation described in section 14.4.

#### Phase 2: Data Copy (Storage Server Fetch)

After the `startMoveKeys` transaction commits, it propagates as a private mutation through
the transaction system (see 14.4). The destination storage servers receive the mutation and
begin `fetchKeys` -- reading the shard data from source servers via standard FDB read
transactions. DD monitors this via `rawCheckFetchingState()` which polls the destination
servers until they report the shard is ready.

#### Phase 3: `finishMoveKeys` ([`MoveKeys.actor.cpp:1229`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L1229))

Once all destinations have the data, DD atomically swaps ownership:

1. **Read and validate**: reads `keyServers` for the range, decodes `src` and `dest`,
   verifies `dest` matches expected servers
2. **Check readiness**: for each destination server, checks if the shard is fetched and ready
3. **Atomic swap** ([line 1519-1524](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L1519)):
   ```cpp
   // Update keyServers: src = old_dest, dest = [] (move complete)
   krmSetRangeCoalescing(&tr, keyServersPrefix, currentKeys, keys,
                         keyServersValue(UIDtoTagMap, dest));  // dest becomes new src

   // Update serverKeys for ALL involved servers
   for (server in allServers) {
       bool destHasServer = (server in dest);
       krmSetRangeCoalescing(&tr, serverKeysPrefixFor(server), currentKeys,
                             allKeys, destHasServer ? serverKeysTrue : serverKeysFalse);
   }
   ```
4. **Commit**

After this commit, the old source servers are no longer owners. The private mutation
propagation tells them to drop the shard data.

**Retry handling**: on transaction conflict, `finishMoveKeys` retries with
exponential backoff. After every 10 retries ([line 1551](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L1551)) it emits a `RelocateShard_FinishMoveKeysRetrying`
warning; at 20 retries it escalates to `SevWarnAlways`.

### 14.4 Private Mutation Propagation

When `keyServers` or `serverKeys` mutations commit, they are classified as **private
mutations** and receive special treatment at every layer of the transaction system.

#### At the Commit Proxy ([`ApplyMetadataMutation.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/ApplyMetadataMutation.cpp))

**[`checkSetKeyServersPrefix`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/ApplyMetadataMutation.cpp#L207)** (line 207): When the proxy sees a mutation to
`\xff/keyServers/`:
1. Stores the mutation in the local `txnStateStore` -- this is the proxy's
   in-memory replica of the shard map
2. Decodes `src` and `dest` UIDs
3. Looks up each UID's `Tag` from the `storageCache`
4. Updates `keyInfo` (the `ServerCacheInfo` map) with the combined tag set
5. This updated `keyInfo` is used immediately for **routing subsequent mutations** -- any
   user transaction touching this key range will now be tagged for both `src` and `dest`
   servers

**[`checkSetServerKeysPrefix`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/ApplyMetadataMutation.cpp#L258)** (line 258): When the proxy sees a mutation to
`\xff/serverKeys/`:
1. Looks up the target server's `Tag` (line 263: [`if (toCommit)`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/ApplyMetadataMutation.cpp#L263) guard)
2. **Privatizes** the mutation by prepending `systemKeys.begin` to `param1`
3. Adds the server's tag to the commit batch: `toCommit->addTag(tag)` ([line 277](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/ApplyMetadataMutation.cpp#L277))
4. Writes the privatized mutation

This ensures the `serverKeys` change is streamed only to the specific storage server it
affects (via the tag-based routing through tLogs).

#### At the tLog

The tLog receives the tagged mutations and stores them in the appropriate tag-specific
queue. Storage servers pull mutations by their tag, so each SS only sees `serverKeys`
mutations addressed to it.

#### At the Storage Server ([`storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/storageserver.actor.cpp))

[`applyPrivateData()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/storageserver.actor.cpp#L10435) processes incoming `serverKeys` mutations in pairs (begin, end):

**First mutation**: Decodes the start key and metadata:
```cpp
decodeServerKeysValue(m.param2, nowAssigned, emptyRange, dataMoveType, dataMoveId, dataMoveReason);
```
This extracts: whether the server is now assigned this range, whether it's an empty-range
optimization, the `DataMoveType` (LOGICAL, PHYSICAL, PHYSICAL_BULKLOAD), and the move reason.

**Second mutation**: Forms the key range and applies the change:
- `setAssignedStatus(data, keys, nowAssigned)` -- updates the SS's in-memory shard map
- For shard-aware SS: [`changeServerKeysWithPhysicalShards()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/storageserver.actor.cpp#L8852) -- handles physical shard moves
- For non-shard-aware SS: [`changeServerKeys()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/storageserver.actor.cpp#L9803) -- triggers `fetchKeys` actor if newly assigned,
  or drops data if unassigned

If the SS is **newly assigned** this range (`nowAssigned = true`), `changeServerKeys` spawns
a `fetchKeys` actor that reads all data from the source servers and writes it into local
storage. Once complete, the shard transitions from FETCHING to READABLE.

If the SS is **unassigned** from this range (`nowAssigned = false`), the data is removed
from the SS's versioned data.

### 14.5 Recovery

During cluster recovery, the `txnStateStore` is reconstructed from tLog data before any
new transactions can proceed. This means `keyServers` and `serverKeys` are recovered to
their last committed state. The new commit proxies replay all metadata mutations, rebuilding
their `keyInfo` routing tables. When DD starts on the new cluster controller, `init()` reads
the recovered `keyServers` and `serverKeys` to reconstruct its in-memory shard map, teams,
and any in-flight data moves.

### 14.6 Consistency Model

The use of FDB's own transaction system for shard map mutations provides strong guarantees:

1. **Serializability**: all shard map changes are serialized through the resolver, so
   concurrent moves to overlapping ranges are detected and one is retried
2. **Atomicity**: `finishMoveKeys` atomically updates both `keyServers` and all affected
   `serverKeys` entries in a single transaction
3. **Consistency across proxies**: because metadata mutations update `txnStateStore` on
   every commit proxy in the same total order, all proxies have an identical view of which
   servers own which shards
4. **Durability**: mutations are persisted in tLogs before acknowledgment

The cost of this elegance is coupling: shard map mutations compete with user transactions
for commit proxy bandwidth and resolver cycles. During heavy data movement, this can
measurably impact user commit latencies.

### 14.7 Commented-Out Trace Events in moveKeys

Notably, `startMoveKeys` contains several **commented-out** trace events ([lines 1009, 1031, 1054, 1060](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L1009)) that would log per-sub-shard details during the move:

```cpp
// TraceEvent("StartMoveKeysOldRange", relocationIntervalId)
//     .detail("KeyBegin", rangeIntersectKeys.begin.toString())
//     .detail("KeyEnd", rangeIntersectKeys.end.toString())
//     .detail("OldSrc", describe(src))
//     .detail("OldDest", describe(dest))
//     .detail("ReadVersion", tr->getReadVersion().get());
```

These were likely disabled due to volume concerns, but their absence creates an observability
gap that we address in the recommendations document.


## 15. The High-Level moveKeys Orchestration

Wrapping the protocol above, [`moveKeys()`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/MoveKeys.actor.cpp#L3180) orchestrates
the three phases:

```cpp
ACTOR Future<Void> moveKeys(Database occ, MoveKeysParams params) {
    wait(rawStartMovement(occ, params, tssMapping));       // startMoveKeys
    state Future<Void> completionSignaller =
        rawCheckFetchingState(occ, params, tssMapping);    // monitor fetch progress
    wait(rawFinishMovement(occ, params, tssMapping));      // finishMoveKeys
    completionSignaller.cancel();
    if (!params.dataMovementComplete.isSet())
        params.dataMovementComplete.send(Void());
    return Void();
}
```

The `dataMovementComplete` promise signals the relocation queue that the move has finished,
allowing it to update `ShardsAffectedByTeamFailure` and complete the relocation lifecycle.


## 16. DD RPC Interface

[`DataDistributorInterface`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/include/fdbserver/DataDistributorInterface.h#L30) exposes these RPCs:

| RPC | Purpose |
|-----|---------|
| `waitFailure` | Cluster Controller liveness check |
| `haltDataDistributor` | Graceful shutdown |
| `distributorSnapReq` | Consistent snapshot coordination |
| `distributorExclCheckReq` | Safety check before server exclusion |
| `dataDistributorMetrics` | Query shard metrics |
| `distributorSplitRange` | External split point insertion |
| `storageWigglerState` | Query wiggle status |
| `triggerAudit` | Trigger/cancel storage audit |


## 17. Error Handling & Recovery

Normal DD errors that trigger re-recruitment (not cluster-wide failure):
```cpp
{error_code_movekeys_conflict, error_code_broken_promise,
 error_code_data_move_cancelled, error_code_data_move_dest_team_not_found}
```

Source: [`DataDistribution.actor.cpp:305`](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/DataDistribution.actor.cpp#L305).

When DD encounters a `movekeys_conflict` (another DD took the lock), it terminates and the
Cluster Controller recruits a fresh instance. The new DD re-reads all state from the system
keyspace, so no data is lost -- only in-flight moves may need to be re-evaluated.

`DDEnabledState` tracks the overall mode:
- `ENABLED` -- normal operation
- `SNAPSHOT` -- DD paused for consistent snapshot
- `BLOB_RESTORE_PREPARING` -- DD paused for blob restore preparation
