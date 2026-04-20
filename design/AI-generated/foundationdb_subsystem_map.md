# FoundationDB Codebase: Major Subsystem Map

This is a research document, not an implementation plan. It partitions the entire FoundationDB codebase into 12 major subsystems based on data flow and runtime relationships.

---

## The 12 Subsystems at a Glance

| # | Subsystem | Primary Location | ~LOC | One-line summary |
|---|-----------|-----------------|------|------------------|
| 1 | [**Flow Runtime**](subsystem_01_flow_runtime.md) | [`flow/`](https://github.com/apple/foundationdb/tree/main/flow) | 31K impl + headers | Async actor framework, event loop, deterministic time, memory arenas |
| 2 | [**RPC & Transport**](subsystem_02_rpc_transport.md) | [`fdbrpc/`](https://github.com/apple/foundationdb/tree/main/fdbrpc) | 28K impl + headers | Endpoint-addressed messaging, peer management, failure monitoring, simulation network |
| 3 | [**Client Library**](subsystem_03_client_library.md) | [`fdbclient/`](https://github.com/apple/foundationdb/tree/main/fdbclient) | 75K impl, 34K headers | Transaction API, read-your-writes, location caching, multi-version client |
| 4 | [**Cluster Controller & Coordination**](subsystem_04_cluster_controller.md) | [`fdbserver/clustercontroller/`](https://github.com/apple/foundationdb/tree/main/fdbserver/clustercontroller), [`fdbserver/coordinator/`](https://github.com/apple/foundationdb/tree/main/fdbserver/coordinator), [`fdbserver/core/LeaderElection`](https://github.com/apple/foundationdb/tree/main/fdbserver/core/LeaderElection) | 13K + coordination | Leader election, role recruitment, process registration, cluster-wide decision making |
| 5 | [**Transaction Commit Pipeline**](subsystem_05_commit_pipeline.md) | [`fdbserver/commitproxy/`](https://github.com/apple/foundationdb/tree/main/fdbserver/commitproxy), [`fdbserver/grvproxy/`](https://github.com/apple/foundationdb/tree/main/fdbserver/grvproxy), [`fdbserver/resolver/`](https://github.com/apple/foundationdb/tree/main/fdbserver/resolver), [`fdbserver/sequencer/`](https://github.com/apple/foundationdb/tree/main/fdbserver/sequencer) | ~12K | Version assignment, conflict detection, commit batching — the write-path orchestration |
| 6 | [**Transaction Log (TLog) & Log System**](subsystem_06_tlog_logsystem.md) | [`fdbserver/tlog/`](https://github.com/apple/foundationdb/tree/main/fdbserver/tlog), [`fdbserver/logsystem/`](https://github.com/apple/foundationdb/tree/main/fdbserver/logsystem), [`fdbserver/logrouter/`](https://github.com/apple/foundationdb/tree/main/fdbserver/logrouter) | 17K | Durable mutation logging, tag-partitioned replication, peek cursors for storage servers |
| 7 | [**Storage Server & Engines**](subsystem_07_storage_server.md) | [`fdbserver/storageserver/`](https://github.com/apple/foundationdb/tree/main/fdbserver/storageserver), [`fdbserver/kvstore/`](https://github.com/apple/foundationdb/tree/main/fdbserver/kvstore) | 63K | Serves reads, applies mutations from log, pluggable storage backends (RocksDB, SQLite, memory) |
| 8 | [**Data Distribution**](subsystem_08_data_distribution.md) | [`fdbserver/datadistributor/`](https://github.com/apple/foundationdb/tree/main/fdbserver/datadistributor) | 22K | Shard management, team building, rebalancing, MoveKeys protocol |
| 9 | [**Cluster Recovery**](subsystem_09_cluster_recovery.md) | [`fdbserver/clustercontroller/ClusterRecovery*`](https://github.com/apple/foundationdb/tree/main/fdbserver/clustercontroller), [`fdbserver/core/RecoveryState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/RecoveryState.h) | (part of #4 files) | 9-phase state machine to reconstitute the transaction system after failures |
| 10 | [**Rate Keeping & Throttling**](subsystem_10_ratekeeper.md) | [`fdbserver/ratekeeper/`](https://github.com/apple/foundationdb/tree/main/fdbserver/ratekeeper) | 4K | Back-pressure on commits and reads, tag-based throttling, throughput tracking |
| 11 | [**Backup, Restore & DR**](subsystem_11_backup_dr.md) | [`fdbserver/backupworker/`](https://github.com/apple/foundationdb/tree/main/fdbserver/backupworker), `fdbclient/FileBackupAgent*`, `fdbclient/BackupContainer*`, [`fdbbackup/`](https://github.com/apple/foundationdb/tree/main/fdbbackup) | ~10K server, ~20K client | Continuous backup to external storage, point-in-time restore, cross-cluster DR |
| 12 | [**Simulation & Testing**](subsystem_12_simulation_testing.md) | [`fdbrpc/sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp), [`fdbserver/workloads/`](https://github.com/apple/foundationdb/tree/main/fdbserver/workloads), [`fdbserver/tester/`](https://github.com/apple/foundationdb/tree/main/fdbserver/tester), [`tests/`](https://github.com/apple/foundationdb/tree/main/tests) | 51K workloads + sim | Deterministic simulation (Sim2), fault injection (Buggify), workload-based integration tests |

Plus supporting code: [`fdbserver/worker/`](https://github.com/apple/foundationdb/tree/main/fdbserver/worker) (process startup, ~4K), [`fdbserver/core/`](https://github.com/apple/foundationdb/tree/main/fdbserver/core) (shared interfaces/utilities, ~14K), [`fdbcli/`](https://github.com/apple/foundationdb/tree/main/fdbcli) (CLI, ~8K), [`bindings/`](https://github.com/apple/foundationdb/tree/main/bindings) (language FFI), [`fdbserver/consistencyscan/`](https://github.com/apple/foundationdb/tree/main/fdbserver/consistencyscan) (~2K).

---

## Detailed Subsystem Descriptions

### 1. [Flow Runtime](subsystem_01_flow_runtime.md) ([`flow/`](https://github.com/apple/foundationdb/tree/main/flow))

**What it is:** A custom async programming framework that underpins every line of FDB code. It is *not* a library you can opt out of — it *is* the execution model.

**Key abstractions:**
- **Future<T> / Promise<T>** — single-assignment async values. A Promise delivers once; a Future receives. Connected via reference-counted `SingleAssignmentVar` (SAV).
- **FutureStream<T> / PromiseStream<T>** — multi-value async channels. The primary request/reply pattern: send a struct containing a `ReplyPromise<T>` into a `RequestStream`, and the receiver sends the reply back through it (self-addressed envelope).
- **ACTOR functions** — compiled by a custom C# actor compiler (`actorcompiler.exe`) into state machines. `wait(expr)` suspends; `state` variables persist across suspensions. Being migrated to C++20 coroutines (`co_await`).
- **Arena / StringRef / Standalone<T>** — zero-copy memory management. `StringRef` is a non-owning pointer+length; `Arena` owns the backing memory; `Standalone<T>` bundles a T with its Arena.
- **Net2** ([`Net2.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Net2.cpp)) — the real event loop. Single-threaded, priority-based task scheduling over Boost.ASIO. Handles timers, yields, I/O multiplexing.
- **Deterministic random** ([`IRandom.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/IRandom.h)) — `deterministicRandom()` provides a seedable PRNG used everywhere, enabling reproducible simulation.
- **Tracing** ([`Trace.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Trace.cpp)) — structured event logging with `.detail()` chains, used for diagnostics and simulation analysis.

**Key dynamic behavior:** Every server-side component is a collection of ACTOR coroutines scheduled on Net2's run loop. There is no threading within a process (except for disk I/O thread pools). All concurrency is cooperative, driven by Future resolution.

**Principal files:** [`flow.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/flow.h), [`Arena.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/Arena.h), [`Error.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/Error.h), [`genericactors.actor.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/genericactors.actor.h), [`Net2.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Net2.cpp), [`Trace.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Trace.cpp), [`serialize.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/serialize.h), [`CoroutinesImpl.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/CoroutinesImpl.h)

---

### 2. [RPC & Transport](subsystem_02_rpc_transport.md) ([`fdbrpc/`](https://github.com/apple/foundationdb/tree/main/fdbrpc))

**What it is:** The layer that makes Flow's Future/Promise work *across process boundaries*. It maps the actor model onto a network.

**Key abstractions:**
- **Endpoint** = `(NetworkAddressList, Token)` — a globally-unique address for a message receiver. Tokens are 128-bit UIDs. Well-known tokens exist for system services.
- **FlowTransport** — singleton that owns all peer connections. `sendReliable()` guarantees delivery (retransmit on reconnect); `sendUnreliable()` is fire-and-forget.
- **Peer** — per-destination state: unsent queue, reliable packet list, connection future, latency histogram. Connection is lazy (established on first send).
- **EndpointMap** — maps Token → `NetworkMessageReceiver*`. When a packet arrives, its token is looked up and the receiver's `receive()` method is called to deserialize and deliver.
- **FailureMonitor** — tracks endpoint/process liveness. Components register interest via `onFailed(Endpoint)` futures.
- **Locality** ([`Locality.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/Locality.h)) — `LocalityData` (zone, machine, datacenter, data hall) and `ProcessClass` (fitness for roles). Drives replication policy enforcement.
- **Sim2** ([`sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp)) — *replaces* Net2 during simulation. Runs many virtual processes in one real thread with deterministic time, simulated network (clogging, latency, partitions), and fault injection (kill process/machine/zone/datacenter).

**Key dynamic behavior:** When Actor A on Process 1 sends a request to Actor B on Process 2, FlowTransport serializes the message, TCP delivers it, the receiver's EndpointMap dispatches it, and the reply flows back the same way. In simulation, this all happens in-process with virtual routing. The abstraction is seamless — actors don't know whether they're simulated or real.

**Principal files:** [`fdbrpc.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/fdbrpc.h), [`FlowTransport.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FlowTransport.h), [`FlowTransport.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/FlowTransport.cpp), [`FailureMonitor.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FailureMonitor.h), [`Locality.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/Locality.h), [`ReplicationPolicy.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/ReplicationPolicy.h), [`sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp), [`simulator.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/simulator.h)

---

### 3. [Client Library](subsystem_03_client_library.md) ([`fdbclient/`](https://github.com/apple/foundationdb/tree/main/fdbclient))

**What it is:** Everything an application (or internal FDB component acting as a client) uses to read and write data.

**Transaction layers (outside-in):**
1. **C API / Language Bindings** — FFI boundary. All external languages go through [`fdb_c.h`](https://github.com/apple/foundationdb/blob/main/bindings/c/foundationdb/fdb_c.h). Multi-version support loads different client `.so` files for protocol compatibility.
2. **MultiVersionTransaction** — version-negotiating wrapper.
3. **ReadYourWritesTransaction** (RYW) — the layer most internal code uses. Maintains a local write map and snapshot read cache. Reads check the write map first, then merge with storage server results. This provides read-your-own-writes semantics within a transaction.
4. **NativeAPI / Transaction** — the raw transaction. `getReadVersion()` contacts a GRV proxy; `get()`/`getRange()` contact storage servers; `commit()` sends mutations + conflict ranges to a commit proxy.

**Key dynamic behavior:**
- **Location cache**: `DatabaseContext` caches key-range → storage-server mappings. On cache miss or stale entry, re-resolves via a commit proxy's `getKeyServerLocations`.
- **Proxy load balancing**: `commitProxyLoadBalance()` / `grvProxyLoadBalance()` — round-robin with failover and automatic retry on proxy set changes.
- **Retry loop**: `Transaction::onError()` handles `not_committed`, `transaction_too_old`, etc. by resetting and retrying with backoff. The canonical usage pattern is `loop { try { ... tr.commit(); break; } catch(Error& e) { wait(tr.onError(e)); } }`.
- **Watches**: `tr.watch(key)` registers interest; the storage server notifies when the key changes.

**Principal files:** [`NativeAPI.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/NativeAPI.actor.cpp), [`ReadYourWrites.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/ReadYourWrites.actor.cpp), [`DatabaseContext.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/DatabaseContext.cpp), [`MultiVersionTransaction.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/MultiVersionTransaction.cpp), [`CommitTransaction.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/CommitTransaction.h), [`SystemData.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/SystemData.h), [`MonitorLeader.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/MonitorLeader.h)

---

### 4. [Cluster Controller & Coordination](subsystem_04_cluster_controller.md)

**What it is:** The "brain" that decides who does what. One process is elected Cluster Controller (CC); it recruits and monitors all server roles.

**Leader election:**
- Coordinators (typically 3 or 5, addresses in the cluster file) run a `leaderRegister` actor.
- CC candidates send `CandidacyRequest`s. Coordinators track candidates and nominate based on process class fitness (encoded in top bits of `LeaderInfo.changeID`).
- The winner becomes CC; losers detect this via `monitorLeader()` and defer.
- Uses a generation register (`localGenerationReg`) for safe state transitions — reads and writes carry generation numbers to prevent stale updates.

**Role recruitment:**
- Worker processes register with CC via `registrationClient()` ([`worker.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/worker/worker.actor.cpp)).
- CC maintains a pool of available workers with their `ProcessClass` and `LocalityData`.
- When the transaction system needs to be (re)constituted, CC recruits: Master/Sequencer, CommitProxies, GrvProxies, Resolvers, TLogs.
- Recruitment considers fitness (class match), locality (datacenter placement), and excludes failed/excluded processes.
- CC also recruits singleton roles: DataDistributor, Ratekeeper, ConsistencyScan.

**Cluster-wide state:**
- `ServerDBInfo` is the cluster-wide configuration broadcast. Contains: master interface, proxy lists, log system config, recovery state, latency band config.
- Updated by CC and distributed to all workers. Workers react to changes (e.g., new proxy set).

**Principal files:** [`ClusterController.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterController.actor.cpp), `ClusterControllerData.h`, [`Coordination.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/coordinator/Coordination.actor.cpp), [`LeaderElection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/LeaderElection.actor.cpp), [`CoordinatedState.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/CoordinatedState.cpp), [`WorkerInterface.actor.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/WorkerInterface.actor.h)

---

### 5. [Transaction Commit Pipeline](subsystem_05_commit_pipeline.md)

**What it is:** The assembly line that takes a client's commit request and makes it durable. Four server roles collaborate in a strict pipeline.

**The pipeline (a single commit's journey):**

```
Client ──CommitTransactionRequest──▶ CommitProxy
                                        │
Phase 1: GET VERSION                    │ GetCommitVersionRequest
                                        ▼
                                    Master/Sequencer
                                        │ assigns monotonic version
                                        │ (tracks prevVersion for ordering)
                                        ▼
Phase 2: RESOLVE                    CommitProxy
                                        │ ResolveTransactionBatchRequest
                                        ▼
                                    Resolver(s)  [parallel, sharded by key range]
                                        │ checks read-ranges vs committed write-ranges
                                        │ returns conflict/no-conflict per txn
                                        ▼
Phase 3: LOG                        CommitProxy
                                        │ pushes non-conflicting mutations
                                        │ via LogSystem::push()
                                        ▼
                                    TLog replicas (quorum write)
                                        │
Phase 4: REPLY                      CommitProxy
                                        │ CommitTransactionReply
                                        ▼
                                    Client (committed!)
```

**Master/Sequencer** ([`masterserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/sequencer/masterserver.actor.cpp)): Assigns commit versions via `getVersion()`. Ensures versions are monotonically increasing and roughly track wall-clock time. Also tracks the "live committed version" for GRV.

**GRV Proxy** ([`GrvProxyServer.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/grvproxy/GrvProxyServer.cpp)): Handles `GetReadVersionRequest` from clients. Assigns read versions that are guaranteed to see all previously committed transactions. Implements rate limiting (coordinated with Ratekeeper).

**Commit Proxy** ([`CommitProxyServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/commitproxy/CommitProxyServer.actor.cpp)): The workhorse. Batches commits (`commitBatcher`), drives the 4-phase pipeline, handles metadata mutations (system key writes), and updates the key-server location map.

**Resolver** ([`Resolver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/Resolver.actor.cpp), [`ConflictSet.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/ConflictSet.cpp)): Maintains a sliding window of committed write ranges. For each incoming batch, checks if any transaction's read ranges overlap with committed writes at versions newer than the transaction's read version. Pure conflict detection — no side effects.

**Principal files:** [`CommitProxyServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/commitproxy/CommitProxyServer.actor.cpp), [`GrvProxyServer.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/grvproxy/GrvProxyServer.cpp), [`masterserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/sequencer/masterserver.actor.cpp), [`Resolver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/Resolver.actor.cpp), [`ConflictSet.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/resolver/ConflictSet.cpp), [`LogSystem.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/LogSystem.h)

---

### 6. [Transaction Log (TLog) & Log System](subsystem_06_tlog_logsystem.md)

**What it is:** The durable write-ahead log. All committed mutations flow through TLogs before being applied to storage servers. This is FDB's durability guarantee.

**TLog** ([`TLogServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/tlog/TLogServer.actor.cpp)):
- Receives mutation batches from commit proxies via `push()`.
- Writes to a `DiskQueue` (append-only on-disk log) and an in-memory index.
- Mutations are **tagged** — each mutation carries one or more `Tag`s indicating which storage server(s) it's relevant to. Tags are assigned by the commit proxy based on the key-to-shard mapping.
- Provides `peek()` interface: storage servers pull mutations by tag and version range.
- Quorum replication: a mutation is committed when `f+1` of `2f+1` TLog replicas acknowledge.

**Log System** ([`TagPartitionedLogSystem.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logsystem/TagPartitionedLogSystem.actor.cpp)):
- Abstraction over the set of TLog replicas.
- `ILogSystem::push()` — sends mutations to all relevant TLog replicas, waits for quorum.
- `ILogSystem::peek()` → `IPeekCursor` — merges results from multiple TLogs for a given tag, handling multi-generation log sets during recovery.
- Manages log epochs: each recovery creates a new epoch of TLogs. Old epochs are kept until storage servers have consumed all their data.

**Log Router** ([`LogRouter.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logrouter/LogRouter.cpp)):
- In multi-region configurations, routes mutation data from the primary's TLogs to the remote region's TLogs.
- Bridges the tag-partitioned log system across datacenters.

**Key dynamic behavior:** Storage servers are *consumers* of the log. They continuously peek their assigned tag, pulling committed mutations and applying them to local storage. This decoupling means commits don't wait for storage servers — only for TLog quorum.

**Principal files:** [`TLogServer.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/tlog/TLogServer.actor.cpp), [`TagPartitionedLogSystem.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logsystem/TagPartitionedLogSystem.actor.cpp), [`LogRouter.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logrouter/LogRouter.cpp), [`LogSystem.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/LogSystem.h), [`LogSystemConfig.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/LogSystemConfig.h), [`IDiskQueue.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/IDiskQueue.h)

---

### 7. [Storage Server & Engines](subsystem_07_storage_server.md)

**What it is:** The read path and durable key-value store. Storage servers serve client reads and maintain the materialized state of their assigned key ranges (shards).

**Storage Server** ([`storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp)):
- Each SS owns a set of key ranges (shards) and a version.
- **Update loop**: continuously peeks its tag from the log system, deserializes mutations, applies them to the local KVStore, and advances its durable version.
- **Read serving**: handles `GetKeyValuesRequest`, `GetKeyRequest`, `GetValueRequest`. Checks that the requested version is available (between oldest readable version and current version), that the shard is owned, and reads from the KVStore.
- **Version management**: tracks `storageVersion` (latest durable), `durableVersion`, and `desiredOldestVersion`. Advertises its version to the cluster for GRV decisions.

**Storage Engines** ([`fdbserver/kvstore/`](https://github.com/apple/foundationdb/tree/main/fdbserver/kvstore)):
- **IKeyValueStore** interface: `readValue()`, `readRange()`, `set()`, `clear()`, `commit()`. All operations are on a versioned key-value store.
- **RocksDB** ([`KeyValueStoreRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp)) — primary production engine. Also a sharded variant ([`KeyValueStoreShardedRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreShardedRocksDB.actor.cpp)) that uses per-shard column families.
- **SQLite** ([`KeyValueStoreSQLite.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreSQLite.actor.cpp)) — legacy engine, still supported.
- **Memory** ([`KeyValueStoreMemory.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreMemory.actor.cpp)) — for testing and special uses (e.g., txnStateStore during recovery).

**Key dynamic behavior:** The storage server is *pull-based*. It pulls from the log system at its own pace. If a storage server falls behind, it catches up by reading more from TLogs. If it falls too far behind, it may be removed from its team and re-replicated. Reads at a given version are served from a snapshot — the SS maintains enough history to serve reads at any version between its oldest and current.

**Principal files:** [`storageserver.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/storageserver/storageserver.actor.cpp), [`KeyValueStoreRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreRocksDB.actor.cpp), [`KeyValueStoreShardedRocksDB.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreShardedRocksDB.actor.cpp), [`KeyValueStoreSQLite.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreSQLite.actor.cpp), [`KeyValueStoreMemory.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/kvstore/KeyValueStoreMemory.actor.cpp), [`IKeyValueStore.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/IKeyValueStore.h), [`StorageMetrics.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/StorageMetrics.cpp)

---

### 8. [Data Distribution](subsystem_08_data_distribution.md)

**What it is:** The subsystem that decides which storage servers hold which key ranges, and moves data when the assignment needs to change.

**Why data moves:**
- A shard grows too large → split
- A storage server fails → re-replicate its shards to healthy servers
- Load imbalance → move shards from hot to cold servers
- Configuration change (e.g., replication factor increase)
- Exclusion of a server/machine/datacenter

**Components:**
- **DDShardTracker** ([`DDShardTracker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDShardTracker.cpp)) — monitors shard sizes and read/write rates. Triggers splits, merges, and moves.
- **DDTeamCollection** ([`DDTeamCollection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDTeamCollection.actor.cpp)) — builds "teams" of storage servers that satisfy the replication policy (e.g., 3 servers in 3 different zones). Maintains healthy/unhealthy team lists.
- **DDRelocationQueue** ([`DDRelocationQueue.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDRelocationQueue.actor.cpp)) — prioritized queue of shard movements. Executes moves concurrently up to a parallelism limit.
- **MoveKeys** ([`fdbserver/core/MoveKeys.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/MoveKeys.cpp)) — the protocol for atomically transferring shard ownership. Uses a "move keys lock" to serialize concurrent moves. Updates the `keyServers` and `serverKeys` system key spaces.
- **DataMovement** ([`fdbserver/core/DataMovement.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/DataMovement.cpp)) — data move metadata and physical shard ID tracking.

**Key dynamic behavior:** Data distribution is a continuous background process. The DD singleton (recruited by CC) watches for changes in shard metrics, server health, and configuration. It produces `RelocateShard` requests that flow through the relocation queue, which executes the MoveKeys protocol. During a move, the destination SS fetches data from the source SS and begins consuming from the log system. When caught up, ownership transfers atomically via a system key transaction.

**Principal files:** [`DataDistribution.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DataDistribution.cpp), [`DDTeamCollection.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDTeamCollection.actor.cpp), [`DDRelocationQueue.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDRelocationQueue.actor.cpp), [`DDShardTracker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/DDShardTracker.cpp), [`MoveKeys.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/MoveKeys.cpp), [`DataMovement.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/DataMovement.cpp), [`ShardsAffectedByTeamFailure.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/datadistributor/ShardsAffectedByTeamFailure.cpp)

---

### 9. [Cluster Recovery](subsystem_09_cluster_recovery.md)

**What it is:** The state machine that reconstitutes the entire transaction system after a failure (master crash, network partition, coordinator change, etc.).

**The 9 recovery states** (from [`RecoveryState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/RecoveryState.h)):
1. **UNINITIALIZED** — starting
2. **READING_CSTATE** — read coordinated state from coordinators (learn about previous epoch)
3. **LOCKING_CSTATE** — lock old TLogs to prevent split-brain
4. **RECRUITING** — recruit new TLogs, CommitProxies, GrvProxies, Resolvers
5. **RECOVERY_TRANSACTION** — replay the last epoch's committed-but-unapplied mutations; establish the recovery version
6. **WRITING_CSTATE** — write the new epoch's coordinated state to coordinators
7. **ACCEPTING_COMMITS** — new transaction system is live, accepting commits
8. **ALL_LOGS_RECRUITED** — all TLog sets (including remote region) are populated
9. **FULLY_RECOVERED** — old TLog data fully consumed; old generations can be discarded

**Key operations during recovery:**
- `readTransactionSystemState()` — peeks old TLogs to reconstruct the txnStateStore (key-server mappings, configuration, etc.)
- `recruitEverything()` — recruits the full transaction system in parallel
- `trackTlogRecovery()` — monitors TLog set completeness and writes coordinated state updates
- `provisionalMaster()` — handles the gap between old and new transaction systems

**Principal files:** [`ClusterRecovery.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterRecovery.actor.cpp), [`ClusterRecovery.actor.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/clustercontroller/ClusterRecovery.actor.h), [`RecoveryState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/RecoveryState.h), [`DBCoreState.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/include/fdbserver/core/DBCoreState.h), [`CoordinatedState.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/core/CoordinatedState.cpp), [`TagPartitionedLogSystem.actor.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/logsystem/TagPartitionedLogSystem.actor.cpp) (newEpoch)

---

### 10. [Rate Keeping & Throttling](subsystem_10_ratekeeper.md) ([`fdbserver/ratekeeper/`](https://github.com/apple/foundationdb/tree/main/fdbserver/ratekeeper))

**What it is:** Back-pressure mechanism that prevents the cluster from being overwhelmed.

**What it monitors:**
- Storage server queue depths (how far behind are they from the log?)
- TLog queue depths
- Storage server disk throughput and IOPS
- Per-tag transaction rates (for tenant/workload isolation)

**What it controls:**
- Transaction rate limits sent to GRV proxies, which enforce them by delaying `GetReadVersion` responses
- Per-tag throttling via `GlobalTagThrottler` — can slow down or reject transactions from specific tags
- Batch priority vs. default priority differentiation

**Principal files:** [`Ratekeeper.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/Ratekeeper.cpp), [`Ratekeeper.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/Ratekeeper.h), [`GlobalTagThrottler.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/GlobalTagThrottler.cpp), [`RkTagThrottleCollection.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/RkTagThrottleCollection.cpp), [`ServerThroughputTracker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/ServerThroughputTracker.cpp)

---

### 11. [Backup, Restore & DR](subsystem_11_backup_dr.md)

**What it is:** Continuous backup of the mutation stream to external storage, with point-in-time restore capability.

**Backup architecture:**
- **BackupWorker** ([`fdbserver/backupworker/`](https://github.com/apple/foundationdb/tree/main/fdbserver/backupworker)) — server-side role that reads mutations from TLogs (like a storage server, but writes to backup storage instead of a local KVStore). Tag: `tagLocalityLogRouter`.
- **FileBackupAgent** ([`FileBackupAgent.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/FileBackupAgent.cpp)) — orchestrates backup lifecycle. Uses a TaskBucket pattern for reliable, resumable tasks.
- **BackupContainer** (`fdbclient/BackupContainer*`) — abstraction over backup storage destinations. Implementations: local directory, S3 blob store.
- **Two file types**: Range files (snapshot of key-value pairs at a version) and Log files (mutation stream between versions). Together they enable point-in-time restore.

**Restore:**
- Restore agent reads range files to reconstruct base state, then applies log files to reach target version.
- Supports key prefix transformation during restore (add/remove prefix).

**DR (Disaster Recovery):**
- `DatabaseBackupAgent` — streams mutations from one cluster to another in near-real-time.
- Enables warm standby clusters.

**Principal files:** [`BackupWorker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/backupworker/BackupWorker.cpp), [`FileBackupAgent.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/FileBackupAgent.cpp), [`DatabaseBackupAgent.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/DatabaseBackupAgent.cpp), [`BackupContainer.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/BackupContainer.h), [`BackupContainerFileSystem.cpp`](https://github.com/apple/foundationdb/blob/main/fdbclient/BackupContainerFileSystem.cpp), [`BackupAgent.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/BackupAgent.h), [`backup.cpp`](https://github.com/apple/foundationdb/blob/main/fdbbackup/backup.cpp)

---

### 12. [Simulation & Testing](subsystem_12_simulation_testing.md)

**What it is:** FDB's most distinctive engineering practice — deterministic simulation testing that can find bugs that would take years to manifest in production.

**Sim2** ([`sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp)):
- Replaces Net2 as the network implementation during tests.
- Runs the entire cluster (multiple "processes") in a single OS thread with virtual time.
- Deterministic: given the same random seed, produces identical execution.
- Simulates: network latency/clogging/partitions, disk I/O errors, process crashes, machine failures, datacenter failures.

**Buggify** (throughout the codebase):
- `BUGGIFY` / `BUGGIFY_WITH_PROB(p)` — randomly enables rare code paths in simulation.
- Examples: short timeouts, extra delays, buffer size changes, early returns.
- Dramatically increases code path coverage during testing.
- Disabled in production builds.

**Workloads** ([`fdbserver/workloads/`](https://github.com/apple/foundationdb/tree/main/fdbserver/workloads), ~51K LOC):
- `TestWorkload` base class with `setup()`, `start()`, `check()`, `getMetrics()`.
- ~60+ workload types covering: API correctness, backup/restore, bulk operations, configuration changes, failure injection, performance benchmarks.
- Configured via TOML files in [`tests/`](https://github.com/apple/foundationdb/tree/main/tests).
- Multiple workloads can run concurrently in a single simulation.

**Test runner** ([`fdbserver/tester/`](https://github.com/apple/foundationdb/tree/main/fdbserver/tester)):
- [`SimulatedCluster.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/SimulatedCluster.cpp) — sets up simulated cluster topology (machines, processes, datacenter layout).
- `TestConfig` — reads TOML test configuration.
- Orchestrates workload execution and result checking.

**Principal files:** [`sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp), [`simulator.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/simulator.h), [`SimulatedCluster.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/SimulatedCluster.cpp), [`fdbserver/workloads/`](https://github.com/apple/foundationdb/tree/main/fdbserver/workloads), [`tests/`](https://github.com/apple/foundationdb/tree/main/tests)

---

## Data Flow Summary

### Write Path (end to end)
```
App → C API → NativeAPI.commit() → CommitProxy → Master (version) → Resolver (conflicts)
  → CommitProxy → LogSystem.push() → TLog quorum (durable) → reply to client
  ~~async~~> StorageServer.peek() → apply mutations to KVStore
```

### Read Path (end to end)
```
App → C API → NativeAPI.getReadVersion() → GrvProxy (rate-limited version)
App → C API → NativeAPI.get()/getRange() → [location cache lookup] → StorageServer
  → KVStore.read() at requested version → reply to client
```

### Recovery Path
```
CC detects master failure → elect new CC (if needed) → read coordinated state
  → lock old TLogs → recruit new {Master, Proxies, Resolvers, TLogs}
  → replay old epoch → write new coordinated state → accept commits
```

### Data Movement Path
```
DDShardTracker detects imbalance → RelocateShard request → DDRelocationQueue
  → MoveKeys protocol: dest SS fetches data + starts consuming log
  → atomic ownership transfer via system key transaction
```
