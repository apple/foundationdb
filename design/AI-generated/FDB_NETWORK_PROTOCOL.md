# FoundationDB Network Protocol Message Reference

This document is a comprehensive enumeration of every serialized message type in the
FoundationDB network protocol. These types are not documented in any single place in
the source; they are defined implicitly by `serialize`/`serializer` methods on C++
structs throughout the codebase.

*NOTE: By design FDB protocols can change between minor versions. Information in
this file is very detailed and is subject to being out of date. However it is
probably straightforward to ask state of the art models to update the documentation
for you if needed.*

**Conventions in this document:**
- Every struct referenced in any message is fully defined with all serialized fields.
- Types are resolved recursively down to C/C++ base types.
- "→ base" annotations after a type show what it resolves to at the wire level.
- Fields marked "(not serialized)" are present in the C++ struct but do not appear on the wire.
- `Optional<T>` is serialized as: `bool present` + (if true) the value of type `T`.
- `std::vector<T>` / `VectorRef<T>` are serialized as: `int32_t count` + count × T.
- `Arena` fields provide memory backing for `Ref` types; they are serialized last.

---

## Table of Contents

- [1. Base Types and Aliases](#1-base-types-and-aliases)
- [2. Foundational Struct Definitions](#2-foundational-struct-definitions)
- [3. Coordination Protocol](#3-coordination-protocol)
- [4. Cluster Controller Protocol](#4-cluster-controller-protocol)
- [5. Master Protocol](#5-master-protocol)
- [6. GRV Proxy Protocol](#6-grv-proxy-protocol)
- [7. Commit Proxy Protocol](#7-commit-proxy-protocol)
- [8. Resolver Protocol](#8-resolver-protocol)
- [9. Transaction Log (TLog) Protocol](#9-transaction-log-tlog-protocol)
- [10. Storage Server Protocol](#10-storage-server-protocol)
- [11. Data Distributor Protocol](#11-data-distributor-protocol)
- [12. Ratekeeper Protocol](#12-ratekeeper-protocol)
- [13. Worker Protocol](#13-worker-protocol)
- [14. Backup / Consistency Scan / Test Protocols](#14-backup--consistency-scan--test-protocols)
- [15. Client Worker / Debug / Process Protocols](#15-client-worker--debug--process-protocols)
- [Appendix A: Log System Configuration Types](#appendix-a-log-system-configuration-types)
- [Appendix B: In-Band Log Messages](#appendix-b-in-band-log-messages)
- [Appendix C: Backpressure Protocol](#appendix-c-backpressure-protocol)
- [Appendix D: Client Library Protocol Flows](#appendix-d-client-library-protocol-flows)
- [Appendix E: fdbcli Protocol Usage](#appendix-e-fdbcli-protocol-usage)
- [Appendix F: Server-Side Protocol Flows](#appendix-f-server-side-protocol-flows)

---

## Architecture Overview

FDB uses an RPC framework built on its "Flow" actor model. Each server role exposes
an **interface** struct containing `RequestStream<T>` fields — typed endpoints.
Request messages carry a `ReplyPromise<R>` (or `ReplyPromiseStream<R>` for streaming)
that the receiver fulfills with the reply.

- **Endpoint multiplexing**: many interfaces serialize only one `RequestStream` and
  reconstruct the others via `getAdjustedEndpoint(offset)` during deserialization.
- **Wire format**: `ProtocolVersion` (uint64) header, then fields serialized
  sequentially via `serializer(ar, field1, field2, ...)` with no inter-field framing.

---

## 1. Base Types and Aliases

These are the leaf types that all struct fields ultimately resolve to.

| Wire Type | C++ Type | Size | Notes |
|-----------|----------|------|-------|
| `bool` | `bool` | 1 byte | |
| `int8_t` / `uint8_t` | integer | 1 byte | |
| `int16_t` / `uint16_t` | integer | 2 bytes | little-endian |
| `int32_t` / `uint32_t` / `int` | integer | 4 bytes | little-endian |
| `int64_t` / `uint64_t` | integer | 8 bytes | little-endian |
| `double` | IEEE 754 | 8 bytes | little-endian |
| `StringRef` | length-prefixed bytes | `int32_t` len + bytes | |
| `std::string` | length-prefixed bytes | `int32_t` len + bytes | |
| `ProtocolVersion` | `uint64_t` | 8 bytes | version with feature flags |
| enums | underlying integer type | varies | typically `uint8_t` or `uint32_t` |

**Type aliases** used throughout the protocol (all are simple typedefs):

| Alias | Resolves To | Purpose |
|-------|-------------|---------|
| `Version` | `int64_t` | MVCC version number |
| `LogEpoch` | `uint64_t` | TLog generation epoch |
| `Generation` | `int64_t` | Cluster controller generation |
| `Sequence` | `uint64_t` | Sequence number |
| `DBRecoveryCount` | `uint64_t` | Sequential recovery counter |
| `Key` | `Standalone<KeyRef>` → `StringRef` | Key bytes (arena-owned) |
| `KeyRef` | `StringRef` | Key bytes (ref) |
| `Value` | `Standalone<ValueRef>` → `StringRef` | Value bytes (arena-owned) |
| `ValueRef` | `StringRef` | Value bytes (ref) |
| `TransactionTag` | `Standalone<TransactionTagRef>` → `StringRef` | Transaction tag string |
| `TransactionTagRef` | `StringRef` | Transaction tag ref |
| `SpanID` | `UID` | Tracing span identifier |

**Generic containers** (serialization is count + elements):

| Container | Wire Format |
|-----------|-------------|
| `std::vector<T>` | `int32_t` count, then count × T |
| `VectorRef<T>` | `int32_t` count, then count × T |
| `std::set<T>` | `int32_t` count, then count × T |
| `std::map<K,V>` | `int32_t` count, then count × (K, V) pairs |
| `std::unordered_map<K,V>` | `int32_t` count, then count × (K, V) pairs |
| `std::deque<T>` | `int32_t` count, then count × T |
| `Optional<T>` | `bool` present, then (if true) T |
| `std::variant<Ts...>` | `uint8_t` index, then the active variant |
| `Standalone<T>` | same as T (Arena is implicit) |

---

## 2. Foundational Struct Definitions

Every struct used as a field in protocol messages is defined here, in dependency
order. Each definition shows all serialized fields and their fully-resolved types.

### UID
Universally unique identifier. Serialized as two `uint64_t` values.

| Field | Type | Purpose |
|-------|------|---------|
| part[0] | `uint64_t` | First 8 bytes |
| part[1] | `uint64_t` | Second 8 bytes |

### IPAddress
IP address supporting both IPv4 and IPv6.

Serialized as: `bool isV6`, then either `uint32_t` (v4) or `std::array<uint8_t, 16>` (v6).

### NetworkAddress
A network endpoint address.

| Field | Type | Purpose |
|-------|------|---------|
| ip | `IPAddress` | IP address (see above) |
| port | `uint16_t` | Port number |
| flags | `uint16_t` | Flags (TLS, public, etc.) |
| fromHostname | `bool` | Whether resolved from hostname (conditional on protocol version) |

### NetworkAddressList
A primary + optional secondary address for multi-homed processes.

| Field | Type | Purpose |
|-------|------|---------|
| address | `NetworkAddress` | Primary address |
| secondaryAddress | `Optional<NetworkAddress>` | Secondary address |

### Hostname

| Field | Type | Purpose |
|-------|------|---------|
| host | `std::string` | Hostname string |
| service | `std::string` | Service/port string |
| isTLS | `bool` | Whether TLS is enabled |

### AddressExclusion
An IP address + optional port for excluding servers.

| Field | Type | Purpose |
|-------|------|---------|
| ip | `IPAddress` | IP address |
| port | `int` (int32_t) | Port (0 = all ports on this IP) |

### Endpoint
A network-addressable RPC endpoint.

| Field | Type | Purpose |
|-------|------|---------|
| addresses | `NetworkAddressList` | Network addresses |
| token | `UID` | Endpoint token (two `uint64_t`) |

### Tag
Identifies which storage server(s) should receive a mutation.

| Field | Type | Purpose |
|-------|------|---------|
| locality | `int8_t` | Locality group (-1 = special, 0+ = DC index) |
| id | `uint16_t` | Tag ID within the locality |

### KeyRangeRef
A contiguous range of keys [begin, end).

| Field | Type | Purpose |
|-------|------|---------|
| begin | `KeyRef` → `StringRef` | Start key (inclusive) |
| end | `KeyRef` → `StringRef` | End key (exclusive) |

**Note:** `KeyRange` is `Standalone<KeyRangeRef>` (same wire format, arena-owned).

### KeyValueRef
A single key-value pair.

| Field | Type | Purpose |
|-------|------|---------|
| key | `KeyRef` → `StringRef` | Key bytes |
| value | `ValueRef` → `StringRef` | Value bytes |

**Note:** `KeyValue` is `Standalone<KeyValueRef>`.

### KeySelectorRef
A relative key reference: "the first/last key ≥/≤ `key`, plus `offset`."

| Field | Type | Purpose |
|-------|------|---------|
| key | `KeyRef` → `StringRef` | Anchor key |
| orEqual | `bool` | Whether to include the anchor key |
| offset | `int` (int32_t) | Offset from anchor |

### RangeResultRef
Result of a range read. Inherits from `VectorRef<KeyValueRef>` (vector of KV pairs).

| Field | Type | Purpose |
|-------|------|---------|
| (inherited) | `VectorRef<KeyValueRef>` | Key-value pairs |
| more | `bool` | Whether more results exist |
| readThrough | `Optional<KeyRef>` | Read-through key |
| readToBegin | `bool` | Read reached beginning |
| readThroughEnd | `bool` | Read reached end |

### MutationRef
A single database mutation.

| Field | Type | Purpose |
|-------|------|---------|
| type | `uint8_t` | Mutation type (SetValue=0, ClearRange=1, AddValue=2, ...) |
| param1 | `StringRef` | First parameter (key or range start) |
| param2 | `StringRef` | Second parameter (value or range end) |
| checksum | `Optional<uint32_t>` | Optional checksum (conditional on protocol version) |
| accumulativeChecksumIndex | `Optional<uint16_t>` | Checksum index (conditional on protocol version) |

### CommitTransactionRef
A complete transaction to be committed.

| Field | Type | Purpose |
|-------|------|---------|
| read_conflict_ranges | `VectorRef<KeyRangeRef>` | Read conflict ranges |
| write_conflict_ranges | `VectorRef<KeyRangeRef>` | Write conflict ranges |
| mutations | `VectorRef<MutationRef>` | Mutations to apply |
| read_snapshot | `Version` → `int64_t` | Read snapshot version |
| report_conflicting_keys | `bool` | Whether to report conflicts |
| lock_aware | `bool` | Whether lock-aware |
| spanContext | `Optional<SpanContext>` | Tracing context |
| tenantIds | `Optional<VectorRef<int64_t>>` | Tenant IDs (conditional on protocol version) |

### MutationsAndVersionRef
A set of mutations at a particular version.

| Field | Type | Purpose |
|-------|------|---------|
| mutations | `VectorRef<MutationRef>` | Mutations |
| version | `Version` → `int64_t` | Version |
| knownCommittedVersion | `Version` → `int64_t` | Known committed version |

### SpanContext
OpenTelemetry-compatible distributed tracing context.

| Field | Type | Purpose |
|-------|------|---------|
| traceID | `UID` (two `uint64_t`) | Trace identifier |
| spanID | `uint64_t` | Span identifier |
| m_Flags | `uint8_t` | Trace flags |

### FailureStatus

| Field | Type | Purpose |
|-------|------|---------|
| failed | `bool` | Whether the process is failed |

### SystemFailureStatus

| Field | Type | Purpose |
|-------|------|---------|
| addresses | `NetworkAddressList` | Process addresses |
| status | `FailureStatus` | Failure status (→ `bool failed`) |

### LocalityData
Locality key-value map for a process. Serialized as a `uint64_t` map size followed
by (key, value) pairs.

| Field | Type | Purpose |
|-------|------|---------|
| _data | `std::map<Standalone<StringRef>, Optional<Standalone<StringRef>>>` | Locality key-value pairs |

### ProcessClass
Classification of a process (role fitness).

| Field | Type | Purpose |
|-------|------|---------|
| _class | `int16_t` | Class type (unset, storage, transaction, resolution, ...) |
| _source | `int16_t` | Source (command line, configure, ...) |

### ClusterControllerPriorityInfo

| Field | Type | Purpose |
|-------|------|---------|
| processClassFitness | `uint8_t` | Fitness for cluster controller role |
| isExcluded | `bool` | Whether the process is excluded |
| dcFitness | `uint8_t` | Data center fitness |

### StorageMetrics
Load metrics for a storage range.

| Field | Type | Purpose |
|-------|------|---------|
| bytes | `int64_t` | Bytes stored |
| bytesWrittenPerKSecond | `int64_t` | Write rate |
| iosPerKSecond | `int64_t` | I/O rate |
| bytesReadPerKSecond | `int64_t` | Read rate |
| opsReadPerKSecond | `int64_t` | Read operation rate |

### StorageBytes
Disk space information.

| Field | Type | Purpose |
|-------|------|---------|
| free | `int64_t` | Free bytes |
| total | `int64_t` | Total bytes |
| used | `int64_t` | Used bytes |
| available | `int64_t` | Available bytes |

### HealthMetrics
Cluster-wide health metrics.

| Field | Type | Purpose |
|-------|------|---------|
| worstStorageQueue | `int64_t` | Worst SS queue size |
| limitingStorageQueue | `int64_t` | Limiting SS queue size |
| worstStorageDurabilityLag | `int64_t` | Worst SS durability lag |
| limitingStorageDurabilityLag | `int64_t` | Limiting SS durability lag |
| worstTLogQueue | `int64_t` | Worst TLog queue size |
| tpsLimit | `double` | Current TPS limit |
| batchLimited | `bool` | Whether batch is limited |
| storageStats | `std::map<UID, StorageStats>` | Per-SS stats |
| tLogQueue | `std::map<UID, int64_t>` | Per-TLog queue sizes |

#### HealthMetrics::StorageStats

| Field | Type | Purpose |
|-------|------|---------|
| storageQueue | `int64_t` | Queue size |
| storageDurabilityLag | `int64_t` | Durability lag |
| diskUsage | `double` | Disk usage fraction |
| cpuUsage | `double` | CPU usage fraction |

### BusyTagInfo
Information about a busy transaction tag on a storage server.

| Field | Type | Purpose |
|-------|------|---------|
| tag | `TransactionTag` → `StringRef` | Tag string |
| rate | `double` | Tag rate |
| fractionalBusyness | `double` | Fraction of busyness |

### DDMetricsRef
Data distribution shard metrics.

| Field | Type | Purpose |
|-------|------|---------|
| shardBytes | `int64_t` | Bytes in shard |
| shardBytesPerKSecond | `int64_t` | Write rate |
| beginKey | `KeyRef` → `StringRef` | Shard begin key |

### LifetimeToken
Identifies a master's lifetime for fencing.

| Field | Type | Purpose |
|-------|------|---------|
| ccID | `UID` (two `uint64_t`) | Cluster controller ID |
| count | `int64_t` | Monotonic counter |

### UniqueGeneration
Paxos generation identifier.

| Field | Type | Purpose |
|-------|------|---------|
| generation | `uint64_t` | Generation number |
| uid | `UID` (two `uint64_t`) | Unique ID for tie-breaking |

### LeaderInfo
Information about the current cluster controller leader.

| Field | Type | Purpose |
|-------|------|---------|
| changeID | `UID` (two `uint64_t`) | Unique ID for this leader epoch |
| serializedInfo | `Value` → `StringRef` | Serialized `ClusterControllerClientInterface` |
| forward | `bool` | If true, serializedInfo is a forwarding address |

### ClusterConnectionString

| Field | Type | Purpose |
|-------|------|---------|
| coords | `std::vector<NetworkAddress>` | Coordinator addresses |
| hostnames | `std::vector<Hostname>` | Coordinator hostnames |
| key | `Key` → `StringRef` | Cluster description key |
| keyDesc | `Key` → `StringRef` | Cluster description |

### ClientVersionRef

| Field | Type | Purpose |
|-------|------|---------|
| clientVersion | `StringRef` | Client version string |
| sourceVersion | `StringRef` | Source version hash |
| protocolVersion | `StringRef` | Protocol version string |

### VersionHistory
Global configuration change record.

| Field | Type | Purpose |
|-------|------|---------|
| version | `Version` → `int64_t` | Version of this config change |
| mutations | `Standalone<VectorRef<MutationRef>>` | Config mutations |

### ReadOptions
Options for read operations.

| Field | Type | Purpose |
|-------|------|---------|
| type | `ReadType` → `uint8_t` | Read type (NORMAL, EAGER, FETCH) |
| cacheResult | `bool` | Whether to cache the result |
| debugID | `Optional<UID>` | Debug trace ID |
| consistencyCheckStartVersion | `Optional<Version>` | Consistency check version |
| lockAware | `bool` | Whether lock-aware |

### TagSet
A set of transaction tags. Custom serialization.

Serialized as a vector of `TransactionTagRef` (`StringRef`) entries.

### ClientTagThrottleLimits

| Field | Type | Purpose |
|-------|------|---------|
| tpsRate | `double` | Transactions per second rate limit |
| duration | `double` | Duration of throttle |

### TransactionCommitCostEstimation

| Field | Type | Purpose |
|-------|------|---------|
| opsSum | `int` (int32_t) | Total operation count |
| costSum | `uint64_t` | Total cost |

### IdempotencyIdRef
A 16-byte idempotency identifier. Serialized as raw bytes with custom encoding.

| Field | Type | Purpose |
|-------|------|---------|
| first | `uint64_t` | First 8 bytes (or size indicator) |
| second | `uint64_t` | Second 8 bytes (or pointer to data) |

### KeyValueStoreType
Storage engine type enum.

| Field | Type | Purpose |
|-------|------|---------|
| type | `uint32_t` | Engine type (SSD=1, MEMORY=2, SSD_ROCKSDB=4, ...) |

### TLogVersion

| Field | Type | Purpose |
|-------|------|---------|
| version | `uint32_t` | TLog version (V2=2, ..., V8=8) |

### TLogSpillType

| Field | Type | Purpose |
|-------|------|---------|
| type | `uint32_t` | Spill type (DEFAULT=0, VALUE=1, REFERENCE=2) |

### EncryptionAtRestModeDeprecated

| Field | Type | Purpose |
|-------|------|---------|
| mode | `uint32_t` | Encryption mode (DISABLED=0, DOMAIN_AWARE=1, CLUSTER_AWARE=2) |

### MoveKeysLock

| Field | Type | Purpose |
|-------|------|---------|
| prevOwner | `UID` (two `uint64_t`) | Previous lock owner |
| myOwner | `UID` (two `uint64_t`) | Current lock owner |
| prevWrite | `UID` (two `uint64_t`) | Previous write |

### LogMessageVersion

| Field | Type | Purpose |
|-------|------|---------|
| version | `Version` → `int64_t` | Version |
| sub | `uint32_t` | Sub-version within the version |

### Versionstamp
Serialized in big-endian for ordered comparison.

| Field | Type | Purpose |
|-------|------|---------|
| version | `int64_t` | Version (big-endian on wire) |
| batchNumber | `int16_t` | Batch number (big-endian on wire) |

### StorageMetadataType

| Field | Type | Purpose |
|-------|------|---------|
| createdTime | `double` | Creation timestamp |
| storeType | `KeyValueStoreType` → `uint32_t` | Storage engine type |

### RecoveryState (enum)
Serialized as its underlying integer type.

| Value | Name | Meaning |
|-------|------|---------|
| 0 | UNINITIALIZED | Not started |
| 1 | READING_CSTATE | Reading coordinated state |
| 2 | LOCKING_CSTATE | Locking coordinated state |
| 3 | RECRUITING | Recruiting transaction system |
| 4 | RECOVERY_TRANSACTION | Writing recovery transaction |
| 5 | WRITING_CSTATE | Writing coordinated state |
| 6 | ACCEPTING_COMMITS | Accepting commits |
| 7 | ALL_LOGS_RECRUITED | All logs recruited |
| 8 | STORAGE_RECOVERED | Storage servers recovered |
| 9 | FULLY_RECOVERED | Fully recovered |

### ClusterType (enum)

| Value | Name |
|-------|------|
| 0 | STANDALONE |
| 1 | LEGACY_UNUSED_METACLUSTER_MANAGEMENT |
| 2 | LEGACY_UNUSED_METACLUSTER_DATA |

### LogSystemType (enum)

| Value | Name |
|-------|------|
| -1 | unset |
| 0 | empty |
| 2 | tagPartitioned |

### ResolverMoveRef
Describes a shard reassignment between resolvers.

| Field | Type | Purpose |
|-------|------|---------|
| range | `KeyRangeRef` | Key range being moved |
| dest | `int` (int32_t) | Destination resolver index |

### StateTransactionRef
A state transaction with its commit status.

| Field | Type | Purpose |
|-------|------|---------|
| committed | `bool` | Whether committed |
| mutations | `VectorRef<MutationRef>` | State mutations |

### UnknownCommittedVersions
Tracks version ranges whose commit status is unknown during recovery.

| Field | Type | Purpose |
|-------|------|---------|
| version | `Version` → `int64_t` | Version |
| prev | `Version` → `int64_t` | Previous version |
| tLogLocIds | `std::vector<uint16_t>` | TLog locality IDs |

### StorageServerShard
Physical shard metadata on a storage server.

| Field | Type | Purpose |
|-------|------|---------|
| range | `KeyRange` | Shard key range |
| version | `Version` → `int64_t` | Shard version |
| id | `uint64_t` | Shard ID |
| desiredId | `uint64_t` | Desired shard ID |
| shardState | `int8_t` | Shard state (ReadWrite, Adding, etc.) |
| moveInShardId | `Optional<UID>` | Move-in shard ID if being moved |

### CheckpointMetaData
Metadata describing a storage checkpoint.

| Field | Type | Purpose |
|-------|------|---------|
| version | `Version` → `int64_t` | Checkpoint version |
| ranges | `std::vector<KeyRange>` | Key ranges in checkpoint |
| format | `int16_t` | Checkpoint format |
| state | `int16_t` | Checkpoint state |
| checkpointID | `UID` (two `uint64_t`) | Checkpoint identifier |
| src | `std::vector<UID>` | Source servers |
| serializedCheckpoint | `Standalone<StringRef>` | Serialized checkpoint data |
| actionId | `Optional<UID>` | Action ID |
| bytesSampleFile | `Optional<std::string>` | Sample file path |
| dir | `std::string` | Directory path |

### OverlappingChangeFeedEntry
An entry describing a change feed that overlaps a key range.

| Field | Type | Purpose |
|-------|------|---------|
| feedId | `KeyRef` → `StringRef` | Change feed identifier |
| range | `KeyRangeRef` | Feed's key range |
| emptyVersion | `Version` → `int64_t` | Version at which feed was empty |
| stopVersion | `Version` → `int64_t` | Version at which feed was stopped |
| feedMetadataVersion | `Version` → `int64_t` | Feed metadata version |

### ReadHotRangeWithMetrics

| Field | Type | Purpose |
|-------|------|---------|
| keys | `KeyRangeRef` | Hot key range |
| density | `double` | Read density |
| readBandwidthSec | `double` | Read bandwidth (bytes/sec) |
| bytes | `int64_t` | Bytes in range |
| readOpsSec | `double` | Read operations per second |

### CheckSumMetaData

| Field | Type | Purpose |
|-------|------|---------|
| range | `KeyRange` | Key range |
| version | `Version` → `int64_t` | Version |
| checkSumValue | `StringRef` | Checksum bytes |

### BulkDumpState
State of a bulk dump job.

| Field | Type | Purpose |
|-------|------|---------|
| jobId | `UID` (two `uint64_t`) | Job identifier |
| jobRange | `KeyRange` | Key range being dumped |
| phase | `uint8_t` | Dump phase |
| taskId | `Optional<UID>` | Task identifier |
| manifest | `BulkLoadManifest` | Manifest data (complex, serialized as bytes) |

### AuditStorageState

| Field | Type | Purpose |
|-------|------|---------|
| id | `UID` (two `uint64_t`) | Audit ID |
| auditServerId | `UID` (two `uint64_t`) | Server performing audit |
| range | `KeyRange` | Audited key range |
| type | `uint8_t` | Audit type |
| phase | `uint8_t` | Audit phase |
| error | `std::string` | Error message |
| ddId | `UID` (two `uint64_t`) | Data distributor ID |
| engineType | `KeyValueStoreType` → `uint32_t` | Storage engine type |

### MappedKeyValueRef
A key-value pair with a secondary index lookup result.
Inherits from `KeyValueRef`.

| Field | Type | Purpose |
|-------|------|---------|
| key | `KeyRef` → `StringRef` | Primary key (inherited) |
| value | `ValueRef` → `StringRef` | Primary value (inherited) |
| reqAndResult | `std::variant<GetValueReqAndResultRef, GetRangeReqAndResultRef>` | Mapped result |

#### GetValueReqAndResultRef

| Field | Type | Purpose |
|-------|------|---------|
| key | `KeyRef` → `StringRef` | Looked-up key |
| result | `Optional<ValueRef>` | Looked-up value |

#### GetRangeReqAndResultRef

| Field | Type | Purpose |
|-------|------|---------|
| begin | `KeySelectorRef` | Range begin selector |
| end | `KeySelectorRef` | Range end selector |
| result | `RangeResultRef` | Range result |

### DatabaseConfiguration
Database configuration. Serialized as a key-value list.

| Field | Type | Purpose |
|-------|------|---------|
| rawConfiguration | `Standalone<VectorRef<KeyValueRef>>` | Configuration key-value pairs |

The key-value pairs encode redundancy mode, storage engine, proxy/resolver/log counts,
region configuration, and other tuning parameters.

### OpenDatabaseRequest::Samples
Sampling data for client issue/version tracking.

| Field | Type | Purpose |
|-------|------|---------|
| count | `int` (int32_t) | Total count |
| samples | `std::set<std::pair<NetworkAddress, Key>>` | Sampled (address, key) pairs |

### DiskFailureCommand (nested in SetFailureInjection)

| Field | Type | Purpose |
|-------|------|---------|
| stallInterval | `double` | Interval between stalls |
| stallPeriod | `double` | Duration of each stall |
| throttlePeriod | `double` | Throttle period |

### FlipBitsCommand (nested in SetFailureInjection)

| Field | Type | Purpose |
|-------|------|---------|
| percentBitFlips | `double` | Percentage of bits to flip |

### SerializedSample
An actor lineage sample.

| Field | Type | Purpose |
|-------|------|---------|
| time | `double` | Sample timestamp |
| data | `std::unordered_map<WaitState, std::string>` | Wait state → data map |

`WaitState` is serialized as its underlying integer type.

### PerfMetric

| Field | Type | Purpose |
|-------|------|---------|
| m_name | `std::string` | Metric name |
| m_format_code | `std::string` | Format code |
| m_value | `double` | Metric value |
| m_averaged | `bool` | Whether averaged |

---

## 3. Coordination Protocol

**Source:** `fdbclient/include/fdbclient/CoordinationInterface.h`,
`fdbserver/core/include/fdbserver/core/CoordinationInterface.h`

Coordinators run Paxos-based leader election and store the `DBCoreState`.

### ProtocolInfoRequest
Handshake message to discover a peer's protocol version.

| Field | Type | Purpose |
|-------|------|---------|
| reply | `ReplyPromise<ProtocolInfoReply>` → `UID` token | Where to send reply |

**Reply: ProtocolInfoReply**

| Field | Type | Purpose |
|-------|------|---------|
| version | `ProtocolVersion` → `uint64_t` | Protocol version of the responder |

### GetLeaderRequest
Ask a coordinator who the current leader (cluster controller) is.

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Cluster description key |
| knownLeader | `UID` | Last known leader change ID (long-poll) |
| reply | `ReplyPromise<Optional<LeaderInfo>>` | Where to send reply |

**Reply:** `Optional<LeaderInfo>` — see [LeaderInfo](#leaderinfo) definition.

### OpenDatabaseCoordRequest
Client requests current `ClientDBInfo` from coordinators.

| Field | Type | Purpose |
|-------|------|---------|
| traceLogGroup | `Key` → `StringRef` | Client's trace log group |
| issues | `Standalone<VectorRef<StringRef>>` | Client-reported issues |
| supportedVersions | `Standalone<VectorRef<ClientVersionRef>>` | Client library versions |
| knownClientInfoID | `UID` | Last known DB info ID (long-poll) |
| clusterKey | `Key` → `StringRef` | Cluster connection key |
| hostnames | `std::vector<Hostname>` | Coordinator hostnames |
| coordinators | `std::vector<NetworkAddress>` | Coordinator addresses |
| internal | `bool` | Whether this is an internal request |
| reply | `ReplyPromise<CachedSerialization<ClientDBInfo>>` | Where to send reply |

**Reply:** `ClientDBInfo` — see [Section 4](#4-cluster-controller-protocol).

### CheckDescriptorMutableRequest

| Field | Type | Purpose |
|-------|------|---------|
| reply | `ReplyPromise<CheckDescriptorMutableReply>` | Where to send reply |

**Reply: CheckDescriptorMutableReply** — `bool isMutable`.

### CandidacyRequest
Propose a candidate for cluster controller during leader election.

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Cluster description key |
| myInfo | `LeaderInfo` | Candidate's leader info |
| knownLeader | `UID` | Currently known leader |
| prevChangeID | `UID` | Previous change ID |
| reply | `ReplyPromise<Optional<LeaderInfo>>` | Election result |

### ElectionResultRequest
Wait for the result of an election.

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Cluster description key |
| hostnames | `std::vector<Hostname>` | Coordinator hostnames |
| coordinators | `std::vector<NetworkAddress>` | Coordinator addresses |
| knownLeader | `UID` | Currently known leader |
| reply | `ReplyPromise<Optional<LeaderInfo>>` | Election result |

### LeaderHeartbeatRequest
Leader sends heartbeats to maintain leadership.

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Cluster description key |
| myInfo | `LeaderInfo` | Leader's info |
| prevChangeID | `UID` | Previous change ID |
| reply | `ReplyPromise<LeaderHeartbeatReply>` | Heartbeat ack |

**Reply: LeaderHeartbeatReply** — `bool value` (accepted or not).

### ForwardRequest
Tell a coordinator to forward clients to a new connection string.

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Cluster description key |
| conn | `Value` → `StringRef` | New cluster connection string |
| reply | `ReplyPromise<Void>` | Acknowledgment |

### GenerationRegReadRequest
Read from the generation register (Paxos storage layer).

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Register key |
| gen | `UniqueGeneration` | Generation to read at |
| reply | `ReplyPromise<GenerationRegReadReply>` | Register value |

**Reply: GenerationRegReadReply**

| Field | Type | Purpose |
|-------|------|---------|
| value | `Optional<Value>` | Stored value |
| gen | `UniqueGeneration` | Write generation |
| rgen | `UniqueGeneration` | Read generation |

### GenerationRegWriteRequest
Write to the generation register.

| Field | Type | Purpose |
|-------|------|---------|
| kv | `KeyValue` (→ `StringRef` key + `StringRef` value) | Key-value to write |
| gen | `UniqueGeneration` | Generation for this write |
| reply | `ReplyPromise<UniqueGeneration>` | Actual generation after write |

---

## 4. Cluster Controller Protocol

**Source:** `fdbclient/include/fdbclient/ClusterInterface.h`

The cluster controller manages failure detection, role recruiting, and distributes
`ClientDBInfo` to clients.

### Interface: ClusterInterface

| Endpoint | Request Type |
|----------|-------------|
| openDatabase | `OpenDatabaseRequest` |
| failureMonitoring | `FailureMonitoringRequest` |
| databaseStatus | `StatusRequest` |
| ping | `ReplyPromise<Void>` |
| getClientWorkers | `GetClientWorkersRequest` |
| forceRecovery | `ForceRecoveryRequest` |
| moveShard | `MoveShardRequest` |
| repairSystemData | `RepairSystemDataRequest` |
| splitShard | `SplitShardRequest` |
| triggerAudit | `TriggerAuditRequest` |

### OpenDatabaseRequest

| Field | Type | Purpose |
|-------|------|---------|
| clientCount | `int` (int32_t) | Number of clients |
| issues | `std::map<Key, Samples>` | Client-reported issues |
| supportedVersions | `std::map<Standalone<ClientVersionRef>, Samples>` | Client versions |
| maxProtocolSupported | `std::map<Key, Samples>` | Max protocol per client |
| knownClientInfoID | `UID` | Last known info ID (long-poll) |
| reply | `ReplyPromise<ClientDBInfo>` | Current database info |

### ClientDBInfo
Returned to clients; contains everything needed to reach proxies.

| Field | Type | Purpose |
|-------|------|---------|
| grvProxies | `std::vector<GrvProxyInterface>` | GRV proxy interfaces |
| commitProxies | `std::vector<CommitProxyInterface>` | Commit proxy interfaces |
| id | `UID` | Unique ID for this info snapshot |
| forward | `Optional<Value>` → `Optional<StringRef>` | Forwarding connection string |
| history | `std::vector<VersionHistory>` | Configuration version history |
| clusterId | `UID` | Cluster ID |
| clusterType | `ClusterType` → enum (int) | Cluster type |

Note: the struct also contains `firstCommitProxy` (`Optional<CommitProxyInterface>`)
but this field is **not serialized** — it is reconstructed locally from `commitProxies`.

### FailureMonitoringRequest

| Field | Type | Purpose |
|-------|------|---------|
| senderStatus | `Optional<FailureStatus>` | Sender's own status |
| failureInformationVersion | `Version` → `int64_t` | Last known failure info version |
| addresses | `NetworkAddressList` | Sender's addresses |
| reply | `ReplyPromise<FailureMonitoringReply>` | Failure updates |

**Reply: FailureMonitoringReply**

| Field | Type | Purpose |
|-------|------|---------|
| changes | `VectorRef<SystemFailureStatus>` | Changed failure statuses |
| failureInformationVersion | `Version` → `int64_t` | Updated version |
| allOthersFailed | `bool` | Whether all other processes are failed |
| clientRequestIntervalMS | `int` (int32_t) | Suggested poll interval |
| considerServerFailedTimeoutMS | `int` (int32_t) | Failure timeout threshold |
| arena | `Arena` | Memory arena |

### StatusRequest

| Field | Type | Purpose |
|-------|------|---------|
| statusField | `std::string` | Optional field filter |
| reply | `ReplyPromise<StatusReply>` | JSON status |

**Reply: StatusReply** — serializes `statusStr` (`std::string`, JSON); `statusObj` is reconstructed on deserialization.

### TriggerAuditRequest

| Field | Type | Purpose |
|-------|------|---------|
| type | `uint8_t` | Audit type |
| range | `KeyRange` | Range to audit |
| id | `UID` | Audit ID |
| cancel | `bool` | Whether to cancel |
| engineType | `KeyValueStoreType` → `uint32_t` | Storage engine type |
| reply | `ReplyPromise<UID>` | Audit ID |

### Other Cluster Controller Requests
- **GetClientWorkersRequest**: `reply` → `std::vector<ClientWorkerInterface>`
- **ForceRecoveryRequest**: `Key dcId`, `reply` → `Void`
- **MoveShardRequest**: `KeyRange shard`, `std::vector<NetworkAddress> addresses`, `reply` → `Void`
- **RepairSystemDataRequest**: `reply` → `Void`
- **SplitShardRequest**: `KeyRange shard`, `int num`, `reply` → **SplitShardReply** {`std::vector<KeyRange> shards`}

---

## 5. Master Protocol

**Source:** `fdbserver/core/include/fdbserver/core/MasterInterface.h`

The master coordinates recovery and hands out commit versions to proxies.

### Interface: MasterInterface
Serializes `locality` and `waitFailure`; other endpoints reconstructed via offsets.

| Endpoint | Request Type |
|----------|-------------|
| waitFailure | `ReplyPromise<Void>` |
| getCommitVersion | `GetCommitVersionRequest` |
| getLiveCommittedVersion | `GetRawCommittedVersionRequest` |
| reportLiveCommittedVersion | `ReportRawCommittedVersionRequest` |
| updateRecoveryData | `UpdateRecoveryDataRequest` |

### GetCommitVersionRequest

| Field | Type | Purpose |
|-------|------|---------|
| spanContext | `SpanContext` | Tracing context |
| requestNum | `uint64_t` | Monotonic request number |
| mostRecentProcessedRequestNum | `uint64_t` | Most recent processed request |
| requestingProxy | `UID` | Proxy ID |
| reply | `ReplyPromise<GetCommitVersionReply>` | Assigned version |

**Reply: GetCommitVersionReply**

| Field | Type | Purpose |
|-------|------|---------|
| resolverChanges | `Standalone<VectorRef<ResolverMoveRef>>` | Resolver shard reassignments |
| resolverChangesVersion | `Version` → `int64_t` | Version of changes |
| version | `Version` → `int64_t` | Assigned commit version |
| prevVersion | `Version` → `int64_t` | Previous commit version |
| requestNum | `uint64_t` | Echoed request number |

### ReportRawCommittedVersionRequest

| Field | Type | Purpose |
|-------|------|---------|
| version | `Version` → `int64_t` | Committed version |
| locked | `bool` | Database locked |
| metadataVersion | `Optional<Value>` | Metadata version |
| minKnownCommittedVersion | `Version` → `int64_t` | Min known committed |
| prevVersion | `Optional<Version>` | Previous version |
| writtenTags | `Optional<std::set<Tag>>` | Tags written |
| reply | `ReplyPromise<Void>` | Acknowledgment |

### UpdateRecoveryDataRequest

| Field | Type | Purpose |
|-------|------|---------|
| recoveryTransactionVersion | `Version` → `int64_t` | Recovery txn version |
| lastEpochEnd | `Version` → `int64_t` | End of last epoch |
| commitProxies | `std::vector<CommitProxyInterface>` | Proxy interfaces |
| resolvers | `std::vector<ResolverInterface>` | Resolver interfaces |
| versionEpoch | `Optional<int64_t>` | Version epoch |
| primaryLocality | `int8_t` | Primary DC locality |
| reply | `ReplyPromise<Void>` | Acknowledgment |

### ChangeCoordinatorsRequest

| Field | Type | Purpose |
|-------|------|---------|
| newConnectionString | `Standalone<StringRef>` | New connection string |
| masterId | `UID` | Master ID |
| reply | `ReplyPromise<Void>` | Acknowledgment |

---

## 6. GRV Proxy Protocol

**Source:** `fdbclient/include/fdbclient/GrvProxyInterface.h`

### Interface: GrvProxyInterface
Serializes `processId` (`Optional<Key>`), `provisional` (`bool`), and
`getConsistentReadVersion`; other endpoints reconstructed via offsets.

### GetReadVersionRequest

| Field | Type | Purpose |
|-------|------|---------|
| spanContext | `SpanContext` | Tracing context |
| transactionCount | `uint32_t` | Transactions in batch |
| flags | `uint32_t` | Priority flags |
| tags | `TransactionTagMap<uint32_t>` → `std::unordered_map<TransactionTag, uint32_t>` | Tag counts |
| debugID | `Optional<UID>` | Debug trace ID |
| maxVersion | `Version` → `int64_t` | Max acceptable version |
| reply | `ReplyPromise<GetReadVersionReply>` | Read version |

**Reply: GetReadVersionReply**

| Field | Type | Purpose |
|-------|------|---------|
| processBusyTime | `double` | Proxy busy time |
| version | `Version` → `int64_t` | Assigned read version |
| locked | `bool` | Database locked |
| metadataVersion | `Optional<Value>` | Metadata version |
| tagThrottleInfo | `TransactionTagMap<ClientTagThrottleLimits>` | Per-tag throttle limits |
| midShardSize | `int64_t` | Median shard size |
| rkDefaultThrottled | `bool` | Ratekeeper throttling default |
| rkBatchThrottled | `bool` | Ratekeeper throttling batch |
| ssVersionVectorDelta | `VersionVector` | SS version vector delta (custom serialization) |
| proxyId | `UID` | Proxy ID |
| proxyTagThrottledDuration | `double` | Duration tag-throttled |

### GlobalConfigRefreshRequest

| Field | Type | Purpose |
|-------|------|---------|
| lastKnown | `Version` → `int64_t` | Last known config version |
| reply | `ReplyPromise<GlobalConfigRefreshReply>` | Updated config |

**Reply: GlobalConfigRefreshReply**: `Version version`, `RangeResultRef result`, `Arena arena`.

---

## 7. Commit Proxy Protocol

**Source:** `fdbclient/include/fdbclient/CommitProxyInterface.h`

### Interface: CommitProxyInterface
Serializes `processId` (`Optional<Key>`), `provisional` (`bool`), and `commit`;
11 other endpoints reconstructed via offsets 1–10 and 13 (offsets 11–12 are
reserved/unused).

### CommitTransactionRequest

| Field | Type | Purpose |
|-------|------|---------|
| spanContext | `SpanContext` | Tracing context |
| transaction | `CommitTransactionRef` | Transaction to commit |
| flags | `uint32_t` | Commit flags |
| debugID | `Optional<UID>` | Debug trace ID |
| commitCostEstimation | `Optional<ClientTrCommitCostEstimation>` | Cost estimate (see below) |
| tagSet | `Optional<TagSet>` | Transaction tags |
| idempotencyId | `IdempotencyIdRef` | Idempotency identifier |
| arena | `Arena` | Memory arena |
| reply | `ReplyPromise<CommitID>` | Commit result |

#### ClientTrCommitCostEstimation

| Field | Type | Purpose |
|-------|------|---------|
| opsCount | `int` (int32_t) | Operation count |
| writeCosts | `uint64_t` | Write cost estimate |
| clearIdxCosts | `std::deque<std::pair<int, uint64_t>>` | Clear index costs |
| expensiveCostEstCount | `uint32_t` | Expensive estimate count |

**Reply: CommitID**

| Field | Type | Purpose |
|-------|------|---------|
| version | `Version` → `int64_t` | Committed version |
| txnBatchId | `uint16_t` | Batch identifier |
| metadataVersion | `Optional<Value>` | Metadata version |
| conflictingKRIndices | `Optional<Standalone<VectorRef<int>>>` | Conflicting key range indices |

### GetKeyServerLocationsRequest

| Field | Type | Purpose |
|-------|------|---------|
| spanContext | `SpanContext` | Tracing context |
| begin | `KeyRef` → `StringRef` | Range begin key |
| end | `Optional<KeyRef>` | Range end key |
| limit | `int` (int32_t) | Max results |
| reverse | `bool` | Reverse iteration |
| legacyVersion | `Version` → `int64_t` | Legacy version |
| arena | `Arena` | Memory arena |
| reply | `ReplyPromise<GetKeyServerLocationsReply>` | Locations |

**Reply: GetKeyServerLocationsReply**

| Field | Type | Purpose |
|-------|------|---------|
| results | `std::vector<std::pair<KeyRangeRef, std::vector<StorageServerInterface>>>` | Range→SS mapping |
| resultsTssMapping | `std::vector<std::pair<UID, StorageServerInterface>>` | TSS pairs |
| resultsTagMapping | `std::vector<std::pair<UID, Tag>>` | SS→Tag mapping |
| arena | `Arena` | Memory arena |

### Other Commit Proxy Messages
- **GetRawCommittedVersionRequest**: `SpanContext`, `Optional<UID> debugID`, `Version maxVersion`, `reply` → **GetRawCommittedVersionReply** {`Optional<UID> debugID`, `Version version`, `bool locked`, `Optional<Value> metadataVersion`, `Version minKnownCommittedVersion`, `VersionVector ssVersionVectorDelta`}
- **GetStorageServerRejoinInfoRequest**: `UID id`, `Optional<Value> dcId`, `reply` → **GetStorageServerRejoinInfoReply** {`Version version`, `Tag tag`, `Optional<Tag> newTag`, `bool newLocality`, `std::vector<std::pair<Version, Tag>> history`, `EncryptionAtRestModeDeprecated encryptMode`}
- **TxnStateRequest**: `VectorRef<KeyValueRef> data`, `Sequence sequence` (uint64_t), `bool last`, `std::vector<Endpoint> broadcastInfo`, `Arena arena`, `reply` → `Void`
- **GetHealthMetricsRequest**: `bool detailed`, `reply` → **GetHealthMetricsReply** {`Standalone<StringRef> serialized`}
- **GetDDMetricsRequest**: `KeyRange keys`, `int shardLimit`, `reply` → **GetDDMetricsReply** {`Standalone<VectorRef<DDMetricsRef>> storageMetricsList`}
- **ProxySnapRequest**: `StringRef snapPayload`, `UID snapUID`, `Optional<UID> debugID`, `Arena arena`, `reply` → `Void`
- **ExclusionSafetyCheckRequest**: `std::vector<AddressExclusion> exclusions`, `reply` → **ExclusionSafetyCheckReply** {`bool safe`}
- **SetThrottledShardRequest**: `std::vector<KeyRange> throttledShards`, `double expirationTime`, `reply` → **SetThrottledShardReply** {}
- **ExpireIdempotencyIdRequest** (fire-and-forget): `Version commitVersion`, `uint8_t batchIndexHighByte`
- **GlobalConfigRefreshRequest**: `Version lastKnown`, `reply` → **GlobalConfigRefreshReply** {`Version version`, `RangeResultRef result`, `Arena arena`}

---

## 8. Resolver Protocol

**Source:** `fdbserver/core/include/fdbserver/core/ResolverInterface.h`

### Interface: ResolverInterface
All endpoints serialized directly: `uniqueID` (`UID`), `locality` (`LocalityData`),
`resolve`, `metrics`, `split`, `waitFailure`, `txnState`.

### ResolveTransactionBatchRequest

| Field | Type | Purpose |
|-------|------|---------|
| spanContext | `SpanContext` | Tracing context |
| prevVersion | `Version` → `int64_t` | Previous batch version |
| version | `Version` → `int64_t` | This batch's version |
| lastReceivedVersion | `Version` → `int64_t` | Last received by this resolver |
| transactions | `VectorRef<CommitTransactionRef>` | Transactions to check |
| txnStateTransactions | `VectorRef<int>` | Indices of state transactions |
| debugID | `Optional<UID>` | Debug trace ID |
| writtenTags | `std::set<Tag>` | Tags written |
| lastShardMove | `Version` → `int64_t` | Last shard move version |
| arena | `Arena` | Memory arena |
| reply | `ReplyPromise<ResolveTransactionBatchReply>` | Results |

**Reply: ResolveTransactionBatchReply**

| Field | Type | Purpose |
|-------|------|---------|
| committed | `VectorRef<uint8_t>` | Per-txn status (0=ok, 1=conflict, ...) |
| stateMutations | `VectorRef<VectorRef<StateTransactionRef>>` | State mutations |
| debugID | `Optional<UID>` | Debug trace ID |
| conflictingKeyRangeMap | `std::map<int, VectorRef<int>>` | Conflict details |
| privateMutations | `VectorRef<StringRef>` | Private mutations |
| privateMutationCount | `uint32_t` | Count |
| tpcvMap | `std::unordered_map<uint16_t, Version>` | Per-proxy committed versions |
| writtenTags | `std::set<Tag>` | Tags written |
| lastShardMove | `Version` → `int64_t` | Last shard move |
| arena | `Arena` | Memory arena |

### Other Resolver Messages
- **ResolutionMetricsRequest**: `reply` → **ResolutionMetricsReply** {`int64_t value`}
- **ResolutionSplitRequest**: `KeyRange range`, `int64_t offset`, `bool front`, `reply` → **ResolutionSplitReply** {`Key key`, `int64_t used`}

---

## 9. Transaction Log (TLog) Protocol

**Source:** `fdbserver/core/include/fdbserver/core/TLogInterface.h`

### Interface: TLogInterface
Serializes `uniqueID` (`UID`), `sharedTLogID` (`UID`), `filteredLocality` (`LocalityData`),
and `peekMessages`; 12 other endpoints reconstructed via offsets.

### TLogCommitRequest

| Field | Type | Purpose |
|-------|------|---------|
| spanContext | `SpanContext` | Tracing context |
| prevVersion | `Version` → `int64_t` | Previous version |
| version | `Version` → `int64_t` | Commit version |
| knownCommittedVersion | `Version` → `int64_t` | Known committed version |
| minKnownCommittedVersion | `Version` → `int64_t` | Min known committed |
| seqPrevVersion | `Version` → `int64_t` | Sequential previous version |
| messages | `StringRef` | Serialized mutation messages |
| tLogCount | `uint16_t` | Number of TLogs |
| tLogLocIds | `std::vector<uint16_t>` | TLog locality IDs |
| debugID | `Optional<UID>` | Debug trace ID |
| arena | `Arena` | Memory arena |
| reply | `ReplyPromise<TLogCommitReply>` | Acknowledgment |

**Reply: TLogCommitReply** — `Version version` (int64_t).

### TLogPeekRequest

| Field | Type | Purpose |
|-------|------|---------|
| begin | `Version` → `int64_t` | Start version |
| tag | `Tag` | Tag to peek |
| returnIfBlocked | `bool` | Return immediately if blocked |
| onlySpilled | `bool` | Only spilled data |
| sequence | `Optional<std::pair<UID, int>>` | Ordering sequence |
| end | `Optional<Version>` | End version |
| returnEmptyIfStopped | `Optional<bool>` | Return empty if stopped |
| reply | `ReplyPromise<TLogPeekReply>` | Mutations |

**Reply: TLogPeekReply**

| Field | Type | Purpose |
|-------|------|---------|
| messages | `StringRef` | Serialized mutations |
| end | `Version` → `int64_t` | End version |
| popped | `Optional<Version>` | Popped version |
| maxKnownVersion | `Version` → `int64_t` | Max known version |
| minKnownCommittedVersion | `Version` → `int64_t` | Min known committed |
| begin | `Optional<Version>` | Actual begin |
| onlySpilled | `bool` | Only from spill |
| arena | `Arena` | Memory arena |

### TLogPeekStreamRequest

| Field | Type | Purpose |
|-------|------|---------|
| begin | `Version` → `int64_t` | Start version |
| tag | `Tag` | Tag to peek |
| returnIfBlocked | `bool` | Return immediately |
| limitBytes | `int` (int32_t) | Byte limit per reply |
| end | `Optional<Version>` | End version |
| returnEmptyIfStopped | `Optional<bool>` | Return empty if stopped |
| reply | `ReplyPromiseStream<TLogPeekStreamReply>` | Streaming reply |

**Reply: TLogPeekStreamReply** (extends ReplyPromiseStreamReply)

| Field | Type | Purpose |
|-------|------|---------|
| acknowledgeToken | `Optional<UID>` | Backpressure token |
| sequence | `uint16_t` | Sequence number |
| rep | `TLogPeekReply` | Peek data (see above) |

### Other TLog Messages
- **TLogPopRequest**: `Version to`, `Version durableKnownCommittedVersion`, `Tag tag`, `reply` → `Void`
- **TLogQueuingMetricsRequest**: `reply` → **TLogQueuingMetricsReply** {`double localTime`, `int64_t instanceID`, `int64_t bytesDurable`, `int64_t bytesInput`, `StorageBytes storageBytes`, `Version v`}
- **TLogConfirmRunningRequest**: `Optional<UID> debugID`, `reply` → `Void`
- **TLogRecoveryFinishedRequest**: `reply` → `Void`
- **TLogDisablePopRequest**: `UID snapUID`, `Optional<UID> debugID`, `reply` → `Void`
- **TLogEnablePopRequest**: `UID snapUID`, `Optional<UID> debugID`, `reply` → `Void`
- **TLogSnapRequest**: `StringRef snapPayload`, `UID snapUID`, `StringRef role`, `Arena arena`, `reply` → `Void`
- **TrackTLogRecoveryRequest**: `Version oldestGenRecoverAtVersion`, `reply` → **TrackTLogRecoveryReply** {`Version oldestUnrecoveredStartVersion`}
- **TLogLockResult** (reply to `lock` endpoint): `Version end`, `Version knownCommittedVersion`, `std::deque<UnknownCommittedVersions> unknownCommittedVersions`, `UID id`, `UID logId`

---

## 10. Storage Server Protocol

**Source:** `fdbclient/include/fdbclient/StorageServerInterface.h`

### Interface: StorageServerInterface
Serializes `uniqueID` (`UID`), `locality` (`LocalityData`), `getValue`,
`tssPairID` (`Optional<UID>`), and `acceptingRequests` (`bool`); 30+ other
endpoints reconstructed via offsets.

### GetValueRequest

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Key to read |
| version | `Version` → `int64_t` | Read version |
| tags | `Optional<TagSet>` | Transaction tags |
| reply | `ReplyPromise<GetValueReply>` | Value |
| spanContext | `SpanContext` | Tracing context |
| options | `Optional<ReadOptions>` | Read options |
| ssLatestCommitVersions | `VersionVector` | SS version vector |

**Reply: GetValueReply**

| Field | Type | Purpose |
|-------|------|---------|
| penalty | `double` | Load balancing penalty |
| error | `Optional<Error>` | Error (serialized as error code) |
| value | `Optional<Value>` → `Optional<StringRef>` | The value |
| cached | `bool` | From cache |

### GetKeyRequest

| Field | Type | Purpose |
|-------|------|---------|
| sel | `KeySelectorRef` | Key selector |
| version | `Version` → `int64_t` | Read version |
| tags | `Optional<TagSet>` | Transaction tags |
| reply | `ReplyPromise<GetKeyReply>` | Resolved key |
| spanContext | `SpanContext` | Tracing context |
| options | `Optional<ReadOptions>` | Read options |
| ssLatestCommitVersions | `VersionVector` | SS version vector |
| arena | `Arena` | Memory arena |

**Reply: GetKeyReply** — `double penalty`, `Optional<Error> error`, `KeySelector sel`, `bool cached`.

### GetKeyValuesRequest

| Field | Type | Purpose |
|-------|------|---------|
| begin | `KeySelectorRef` | Range begin |
| end | `KeySelectorRef` | Range end |
| version | `Version` → `int64_t` | Read version |
| limit | `int` (int32_t) | Row limit |
| limitBytes | `int` (int32_t) | Byte limit |
| tags | `Optional<TagSet>` | Transaction tags |
| reply | `ReplyPromise<GetKeyValuesReply>` | Results |
| spanContext | `SpanContext` | Tracing context |
| options | `Optional<ReadOptions>` | Read options |
| ssLatestCommitVersions | `VersionVector` | SS version vector |
| taskID | `Optional<TaskPriority>` → `Optional<int>` | Task priority |
| arena | `Arena` | Memory arena |

**Reply: GetKeyValuesReply** — `double penalty`, `Optional<Error> error`, `VectorRef<KeyValueRef> data`, `Version version`, `bool more`, `bool cached`, `Arena arena`.

### GetMappedKeyValuesRequest
Same as `GetKeyValuesRequest` plus `KeyRef mapper`. Reply: `VectorRef<MappedKeyValueRef> data`, `Version version`, `bool more`, `bool cached`, `Arena arena`.

### GetKeyValuesStreamRequest
Same fields as `GetKeyValuesRequest` (without `taskID`) but with `ReplyPromiseStream<GetKeyValuesStreamReply> reply`.

**Reply: GetKeyValuesStreamReply** — `Optional<UID> acknowledgeToken`, `uint16_t sequence`, `VectorRef<KeyValueRef> data`, `Version version`, `bool more`, `bool cached`, `Arena arena`.

### WatchValueRequest

| Field | Type | Purpose |
|-------|------|---------|
| key | `Key` → `StringRef` | Key to watch |
| value | `Optional<Value>` | Expected current value |
| version | `Version` → `int64_t` | Version of expected value |
| tags | `Optional<TagSet>` | Transaction tags |
| debugID | `Optional<UID>` | Debug trace ID |
| reply | `ReplyPromise<WatchValueReply>` | Watch trigger |
| spanContext | `SpanContext` | Tracing context |

**Reply: WatchValueReply** — `Version version`, `bool cached`.

### ChangeFeedStreamRequest

| Field | Type | Purpose |
|-------|------|---------|
| rangeID | `Key` → `StringRef` | Change feed ID |
| begin | `Version` → `int64_t` | Start version |
| end | `Version` → `int64_t` | End version |
| range | `KeyRange` | Key range |
| reply | `ReplyPromiseStream<ChangeFeedStreamReply>` | Streaming reply |
| spanContext | `SpanContext` | Tracing context |
| replyBufferSize | `int` (int32_t) | Buffer size |
| canReadPopped | `bool` | Can read popped data |
| id | `UID` | Request ID |
| options | `Optional<ReadOptions>` | Read options |
| encrypted | `bool` | Whether to encrypt |
| arena | `Arena` | Memory arena |

**Reply: ChangeFeedStreamReply** — `Optional<UID> acknowledgeToken`, `uint16_t sequence`, `VectorRef<MutationsAndVersionRef> mutations`, `bool atLatestVersion`, `Version minStreamVersion`, `Version popVersion`, `Arena arena`.

### Other Storage Server Messages
- **GetShardStateRequest**: `KeyRange keys`, `int32_t mode`, `bool includePhysicalShard`, `reply` → **GetShardStateReply** {`Version first`, `Version second`, `std::vector<StorageServerShard> shards`}
- **WaitMetricsRequest**: `KeyRangeRef keys`, `StorageMetrics min`, `StorageMetrics max`, `Version legacyVersion`, `Arena arena`, `reply` → `StorageMetrics`
- **SplitMetricsRequest**: `KeyRangeRef keys`, `StorageMetrics limits/used/estimated`, `bool isLastShard`, `Optional<int> minSplitBytes`, `Arena arena`, `reply` → **SplitMetricsReply** {`Standalone<VectorRef<KeyRef>> splits`, `StorageMetrics used`, `bool more`}
- **GetStorageMetricsRequest**: `reply` → **GetStorageMetricsReply** {`StorageMetrics load/available/capacity`, `double bytesInputRate`, `int64_t versionLag`, `double lastUpdate`, `int64_t bytesDurable`, `int64_t bytesInput`, `int ongoingBulkLoadTaskCount`}
- **StorageQueuingMetricsRequest**: `reply` → **StorageQueuingMetricsReply** {`double localTime`, `int64_t instanceID/bytesDurable/bytesInput`, `StorageBytes storageBytes`, `Version version/durableVersion`, `double cpuUsage/diskUsage/localRateLimit`, `std::vector<BusyTagInfo> busiestTags`}
- **OverlappingChangeFeedsRequest**: `KeyRange range`, `Version minVersion`, `reply` → **OverlappingChangeFeedsReply** {`VectorRef<OverlappingChangeFeedEntry> feeds`, `Version feedMetadataVersion`, `Arena arena`}
- **ChangeFeedPopRequest**: `Key rangeID`, `Version version`, `KeyRange range`, `reply` → `Void`
- **ChangeFeedVersionUpdateRequest**: `Version minVersion`, `reply` → **ChangeFeedVersionUpdateReply** {`Version version`}
- **GetCheckpointRequest**: `Version version`, `std::vector<KeyRange> ranges`, `int16_t format`, `Optional<UID> actionId`, `reply` → `CheckpointMetaData`
- **FetchCheckpointRequest**: `UID checkpointID`, `Standalone<StringRef> token`, `reply` (stream) → **FetchCheckpointReply** {`Optional<UID> acknowledgeToken`, `uint16_t sequence`, `Standalone<StringRef> token`, `Standalone<StringRef> data`}
- **FetchCheckpointKeyValuesRequest**: `UID checkpointID`, `KeyRange range`, `reply` (stream) → **FetchCheckpointKeyValuesStreamReply** {`Optional<UID> acknowledgeToken`, `uint16_t sequence`, `VectorRef<KeyValueRef> data`, `Arena arena`}
- **ReadHotSubRangeRequest**: `KeyRangeRef keys`, `uint8_t type`, `int chunkCount`, `Arena arena`, `reply` → **ReadHotSubRangeReply** {`Standalone<VectorRef<ReadHotRangeWithMetrics>> readHotRanges`}
- **SplitRangeRequest**: `KeyRangeRef keys`, `int64_t chunkSize`, `Arena arena`, `reply` → **SplitRangeReply** {`Standalone<VectorRef<KeyRef>> splitPoints`}
- **UpdateCommitCostRequest**: `UID ratekeeperID`, `double postTime/elapsed`, `TransactionTag busiestTag`, `int opsSum`, `uint64_t costSum/totalWriteCosts`, `bool reported`, `reply` → `Void`
- **GetHotShardsRequest**: `reply` → **GetHotShardsReply** {`std::vector<KeyRange> hotShards`}
- **GetStorageCheckSumRequest**: `std::vector<std::pair<KeyRange, Optional<Version>>> ranges`, `Optional<UID> actionId`, `uint8_t checkSumMethod`, `reply` → **GetStorageCheckSumReply** {`std::vector<CheckSumMetaData> checkSums`, `uint8_t checkSumMethod`}
- **BulkDumpRequest**: `std::vector<UID> checksumServers`, `BulkDumpState bulkDumpState`, `reply` → `BulkDumpState`
- **AuditStorageRequest**: `UID id/ddId`, `KeyRange range`, `uint8_t type`, `std::vector<UID> targetServers`, `reply` → `Void`

---

## 11. Data Distributor Protocol

**Source:** `fdbserver/core/include/fdbserver/core/DataDistributorInterface.h`

### Interface: DataDistributorInterface
Serializes all endpoints directly: `waitFailure`, `haltDataDistributor`,
`locality` (`LocalityData`), `myId` (`UID`), and 7 additional `RequestStream`s.

- **HaltDataDistributorRequest**: `UID requesterID`, `reply` → `Void`
- **GetDataDistributorMetricsRequest**: `KeyRange keys`, `int shardLimit`, `bool midOnly`, `reply` → **GetDataDistributorMetricsReply** {`Standalone<VectorRef<DDMetricsRef>> storageMetricsList`, `Optional<int64_t> midShardSize`}
- **DistributorSnapRequest**: `StringRef snapPayload`, `UID snapUID`, `Optional<UID> debugID`, `Arena arena`, `reply` → `Void`
- **DistributorExclusionSafetyCheckRequest**: `std::vector<AddressExclusion> exclusions`, `reply` → **DistributorExclusionSafetyCheckReply** {`bool safe`}
- **DistributorSplitRangeRequest**: `std::vector<Key> splitPoints`, `reply` → **SplitShardReply** {`std::vector<KeyRange> shards`}
- **GetStorageWigglerStateRequest**: `reply` → **GetStorageWigglerStateReply** {`uint8_t primary`, `uint8_t remote`}
- **PrepareBlobRestoreRequest**: `UID requesterID`, `StorageServerInterface ssi`, `KeyRange keys`, `reply` → **PrepareBlobRestoreReply** {`int8_t res`}

---

## 12. Ratekeeper Protocol

**Source:** `fdbserver/core/include/fdbserver/core/RatekeeperInterface.h`

### Interface: RatekeeperInterface
Serializes all endpoints directly: `waitFailure`, `getRateInfo`, `haltRatekeeper`,
`reportCommitCostEstimation`, `getSSVersionLag`, `locality` (`LocalityData`), `myId` (`UID`).

### GetRateInfoRequest

| Field | Type | Purpose |
|-------|------|---------|
| requesterID | `UID` | Proxy ID |
| totalReleasedTransactions | `int64_t` | Total released |
| batchReleasedTransactions | `int64_t` | Batch released |
| version | `Version` → `int64_t` | Current version |
| throttledTagCounts | `TransactionTagMap<uint64_t>` | Throttled tag counts |
| detailed | `bool` | Include detailed metrics |
| reply | `ReplyPromise<GetRateInfoReply>` | Rate limits |

**Reply: GetRateInfoReply**

| Field | Type | Purpose |
|-------|------|---------|
| transactionRate | `double` | Default TPS limit |
| batchTransactionRate | `double` | Batch TPS limit |
| leaseDuration | `double` | Rate lease duration |
| healthMetrics | `HealthMetrics` | Cluster health |
| clientThrottledTags | `Optional<PrioritizedTransactionTagMap<ClientTagThrottleLimits>>` | Client throttles |
| proxyThrottledTags | `Optional<TransactionTagMap<double>>` | Proxy throttles |

`PrioritizedTransactionTagMap<T>` is `std::map<TransactionPriority, TransactionTagMap<T>>` where `TransactionPriority` is a `uint8_t` enum.

### Other Ratekeeper Messages
- **HaltRatekeeperRequest**: `UID requesterID`, `reply` → `Void`
- **ReportCommitCostEstimationRequest**: `UIDTransactionTagMap<TransactionCommitCostEstimation> ssTrTagCommitCost`, `reply` → `Void`
- **GetSSVersionLagRequest**: `reply` → **GetSSVersionLagReply** {`Version maxPrimarySSVersion`, `Version maxRemoteSSVersion`}

---

## 13. Worker Protocol

**Source:** `fdbserver/core/include/fdbserver/core/WorkerInterface.actor.h`

Workers host server roles. The cluster controller sends initialization requests.

### Interface: WorkerInterface
Serializes all 24 `RequestStream` endpoints and `TesterInterface` directly.

### RegisterWorkerRequest

| Field | Type | Purpose |
|-------|------|---------|
| wi | `WorkerInterface` | Worker's interface |
| initialClass | `ProcessClass` | Initial class |
| processClass | `ProcessClass` | Current class |
| priorityInfo | `ClusterControllerPriorityInfo` | Priority info |
| generation | `Generation` → `int64_t` | Cluster generation |
| distributorInterf | `Optional<DataDistributorInterface>` | DD interface |
| ratekeeperInterf | `Optional<RatekeeperInterface>` | RK interface |
| consistencyScanInterf | `Optional<ConsistencyScanInterface>` | CS interface |
| issues | `Standalone<VectorRef<StringRef>>` | Worker issues |
| incompatiblePeers | `std::vector<NetworkAddress>` | Incompatible peers |
| degraded | `bool` | Worker degraded |
| recoveredDiskFiles | `bool` | Disk files recovered |
| clusterId | `Optional<UID>` | Cluster ID |
| reply | `ReplyPromise<RegisterWorkerReply>` | Registration result |

**Reply: RegisterWorkerReply** — `ProcessClass processClass`, `ClusterControllerPriorityInfo priorityInfo`.

### Role Initialization Requests

All follow the pattern: fields describing the role configuration + `ReplyPromise<RoleInterface>`.

- **RecruitMasterRequest**: `LifetimeToken lifetime`, `bool forceRecovery`, `reply` → `MasterInterface`
- **InitializeTLogRequest**: `UID recruitmentID`, `LogSystemConfig recoverFrom`, `Version recoverAt/knownCommittedVersion`, `LogEpoch epoch`, `std::vector<Tag> recoverTags/allTags`, `TLogVersion logVersion`, `KeyValueStoreType storeType`, `TLogSpillType spillType`, `Tag remoteTag`, `int8_t locality`, `bool isPrimary`, `Version startVersion`, `int logRouterTags/txsTags`, `Version recoveryTransactionVersion`, `std::vector<Version> oldGenerationRecoverAtVersions`, `reply` → `TLogInterface`
- **InitializeCommitProxyRequest**: `MasterInterface master`, `LifetimeToken masterLifetime`, `uint64_t recoveryCount`, `Version recoveryTransactionVersion`, `bool firstProxy`, `EncryptionAtRestModeDeprecated encryptMode`, `uint16_t commitProxyIndex`, `reply` → `CommitProxyInterface`
- **InitializeGrvProxyRequest**: `MasterInterface master`, `LifetimeToken masterLifetime`, `uint64_t recoveryCount`, `reply` → `GrvProxyInterface`
- **InitializeResolverRequest**: `LifetimeToken masterLifetime`, `uint64_t recoveryCount`, `int commitProxyCount/resolverCount`, `UID masterId`, `EncryptionAtRestModeDeprecated encryptMode`, `reply` → `ResolverInterface`
- **InitializeStorageRequest**: `Tag seedTag`, `UID reqId/interfaceId`, `KeyValueStoreType storeType`, `Optional<std::pair<UID, Version>> tssPairIDAndVersion`, `Version initialClusterVersion`, `EncryptionAtRestModeDeprecated encryptMode`, `reply` → **InitializeStorageReply** {`StorageServerInterface interf`, `Version addedVersion`}
- **InitializeDataDistributorRequest**: `UID reqId`, `reply` → `DataDistributorInterface`
- **InitializeRatekeeperRequest**: `UID reqId`, `reply` → `RatekeeperInterface`
- **InitializeConsistencyScanRequest**: `UID reqId`, `reply` → `ConsistencyScanInterface`
- **InitializeLogRouterRequest**: `uint64_t recoveryCount`, `Tag routerTag`, `Version startVersion`, `std::vector<LocalityData> tLogLocalities`, `Reference<IReplicationPolicy> tLogPolicy`, `int8_t locality`, `Optional<Version> recoverAt`, `Optional<std::map<uint8_t, std::vector<uint16_t>>> knownLockedTLogIds`, `bool allowDropInSim/isReplacement`, `UID reqId`, `reply` → `TLogInterface`
- **InitializeBackupRequest**: `UID reqId`, `LogEpoch recruitedEpoch/backupEpoch`, `Tag routerTag`, `int totalTags`, `Version startVersion`, `Optional<Version> endVersion`, `reply` → **InitializeBackupReply** {`BackupInterface interf`, `LogEpoch backupEpoch`}

### Recruitment Requests
- **RecruitFromConfigurationRequest**: `DatabaseConfiguration configuration`, `bool recruitSeedServers`, `int maxOldLogRouters`, `reply` → **RecruitFromConfigurationReply** {`std::vector<WorkerInterface>` for: tLogs, satelliteTLogs, commitProxies, grvProxies, resolvers, storageServers, oldLogRouters, backupWorkers; `Optional<Key> dcId`, `bool satelliteFallback`}
- **RecruitRemoteFromConfigurationRequest**: `DatabaseConfiguration configuration`, `Optional<Key> dcId`, `int logRouterCount`, `std::vector<UID> exclusionWorkerIds`, `Optional<UID> dbgId`, `reply` → **RecruitRemoteFromConfigurationReply** {`std::vector<WorkerInterface> remoteTLogs/logRouters`, `Optional<UID> dbgId`}
- **RecruitStorageRequest**: `std::vector<Optional<Standalone<StringRef>>> excludeMachines/includeDCs`, `std::vector<AddressExclusion> excludeAddresses`, `bool criticalRecruitment`, `reply` → **RecruitStorageReply** {`WorkerInterface worker`, `ProcessClass processClass`}

### Other Worker Messages
- **RegisterMasterRequest**: `UID id`, `LocalityData mi`, `LogSystemConfig logSystemConfig`, `std::vector<CommitProxyInterface> commitProxies`, `std::vector<GrvProxyInterface> grvProxies`, `std::vector<ResolverInterface> resolvers`, `DBRecoveryCount recoveryCount`, `int64_t registrationCount`, `Optional<DatabaseConfiguration> configuration`, `std::vector<UID> priorCommittedLogServers`, `RecoveryState recoveryState`, `bool recoveryStalled` (no reply)
- **GetWorkersRequest**: `int flags`, `reply` → `std::vector<WorkerDetails>` where **WorkerDetails** = {`WorkerInterface interf`, `ProcessClass processClass`, `bool degraded`, `bool recoveredDiskFiles`}
- **TLogRejoinRequest**: `TLogInterface myInterface`, `reply` → **TLogRejoinReply** {`bool masterIsRecovered`}
- **BackupWorkerDoneRequest**: `UID workerUID`, `LogEpoch backupEpoch`, `reply` → `Void`
- **GetEncryptionAtRestModeRequest**: `UID tlogId`, `reply` → **GetEncryptionAtRestModeResponse** {`uint32_t mode`}
- **CoordinationPingMessage** (fire-and-forget): `UID clusterControllerId`, `int64_t timeStep`
- **SetMetricsLogRateRequest** (fire-and-forget): `uint32_t metricsLogsPerSecond`
- **UpdateWorkerHealthRequest** (fire-and-forget): `NetworkAddress address`, `std::vector<NetworkAddress> degradedPeers/disconnectedPeers/recoveredPeers`
- **LoadedPingRequest**: `UID id`, `bool loadReply`, `Standalone<StringRef> payload`, `reply` → **LoadedReply** {`Standalone<StringRef> payload`, `UID id`}
- **EventLogRequest**: `bool getLastError`, `Standalone<StringRef> eventName`, `reply` → `TraceEventFields`
- **TraceBatchDumpRequest**: `reply` → `Void`
- **ExecuteRequest**: `StringRef execPayload`, `Arena arena`, `reply` → `Void`
- **WorkerSnapRequest**: `StringRef snapPayload`, `UID snapUID`, `StringRef role`, `Arena arena`, `reply` → `Void`
- **DiskStoreRequest**: `bool includePartialStores`, `reply` → `Standalone<VectorRef<UID>>`

---

## 14. Backup / Consistency Scan / Test Protocols

### BackupInterface
`RequestStream<ReplyPromise<Void>> waitFailure`, `LocalityData locality`.

### ConsistencyScanInterface
`RequestStream<ReplyPromise<Void>> waitFailure`, `RequestStream<HaltConsistencyScanRequest> haltConsistencyScan`, `LocalityData locality`, `UID myId`.

**HaltConsistencyScanRequest**: `UID requesterID`, `reply` → `Void`.

### TesterInterface
`RequestStream<WorkloadRequest> recruitments`.

**WorkloadRequest**: `StringRef title`, `int timeout`, `double databasePingDelay`, `int64_t sharedRandomNumber`, `bool useDatabase/runFailureWorkloads`, `VectorRef<VectorRef<KeyValueRef>> options`, `std::vector<KeyRange> rangesToCheck`, `int clientId/clientCount`, `std::vector<std::string> disabledFailureInjectionWorkloads`, `Arena arena`, `reply` → **WorkloadInterface** {`RequestStream` endpoints for setup/start/check/metrics/stop}.

### NetworkTestRequest
`Key key`, `uint32_t replySize`, `reply` → **NetworkTestReply** {`Value value`}.

### NetworkTestStreamingRequest
`reply` (stream) → **NetworkTestStreamingReply** {`Optional<UID> acknowledgeToken`, `uint16_t sequence`, `int index`}.

---

## 15. Client Worker / Debug / Process Protocols

### ClientWorkerInterface
`RequestStream<RebootRequest> reboot`, `RequestStream<ProfilerRequest> profiler`, `RequestStream<SetFailureInjection> setFailureInjection`, `Optional<NetworkAddress> grpcAddress`.

- **RebootRequest** (fire-and-forget): `bool deleteData`, `bool checkData`, `uint32_t waitForDuration`
- **ProfilerRequest**: `Type` (enum/int), `Action` (enum/int), `int duration`, `Standalone<StringRef> outputFile`, `reply` → `Void`
- **SetFailureInjection**: `Optional<DiskFailureCommand> diskFailure`, `Optional<FlipBitsCommand> flipBits`, `reply` → `Void`

### ProcessInterface
`RequestStream<GetProcessInterfaceRequest> getInterface`, `RequestStream<ActorLineageRequest> actorLineage`.

- **GetProcessInterfaceRequest**: `reply` → `ProcessInterface`
- **ActorLineageRequest**: `WaitState waitStateStart/waitStateEnd` (enum/int), `time_t timeStart/timeEnd` (int64_t), `reply` → **ActorLineageReply** {`std::vector<SerializedSample> samples`}

---

## Appendix A: Log System Configuration Types

**Source:** `fdbserver/core/include/fdbserver/core/LogSystemConfig.h`,
`fdbserver/core/include/fdbserver/core/DBCoreState.h`,
`fdbserver/core/include/fdbserver/core/ServerDBInfo.h`

### OptionalInterface\<T\>
Conditionally serializes a full interface or just its UID.

| Field | Type | Purpose |
|-------|------|---------|
| iface | `Optional<T>` | Full interface (if present) |
| ident | `UID` | Interface ID (serialized only if iface is absent) |

### TLogSet

| Field | Type | Purpose |
|-------|------|---------|
| tLogs | `std::vector<OptionalInterface<TLogInterface>>` | TLog interfaces |
| logRouters | `std::vector<OptionalInterface<TLogInterface>>` | Log router interfaces |
| backupWorkers | `std::vector<OptionalInterface<BackupInterface>>` | Backup worker interfaces |
| tLogWriteAntiQuorum | `int32_t` | Write anti-quorum |
| tLogReplicationFactor | `int32_t` | Replication factor |
| tLogPolicy | `Reference<IReplicationPolicy>` | Replication policy (custom serialization) |
| tLogLocalities | `std::vector<LocalityData>` | TLog localities |
| isLocal | `bool` | Whether this is the local DC |
| locality | `int8_t` | DC locality |
| startVersion | `Version` → `int64_t` | Start version |
| satelliteTagLocations | `std::vector<std::vector<int>>` | Satellite tag locations |
| tLogVersion | `TLogVersion` → `uint32_t` | TLog version |

### OldTLogConf

| Field | Type | Purpose |
|-------|------|---------|
| tLogs | `std::vector<TLogSet>` | TLog sets |
| epochBegin | `Version` → `int64_t` | Epoch begin version |
| epochEnd | `Version` → `int64_t` | Epoch end version |
| recoverAt | `Version` → `int64_t` | Recovery version |
| logRouterTags | `int32_t` | Number of log router tags |
| txsTags | `int32_t` | Number of txs tags |
| pseudoLocalities | `std::set<int8_t>` | Pseudo-localities |
| epoch | `LogEpoch` → `uint64_t` | Epoch number |

### LogSystemConfig

| Field | Type | Purpose |
|-------|------|---------|
| logSystemType | `LogSystemType` → enum (int) | Always `tagPartitioned` (2) |
| tLogs | `std::vector<TLogSet>` | Current TLog sets |
| logRouterTags | `int32_t` | Log router tag count |
| txsTags | `int32_t` | Txs tag count |
| oldTLogs | `std::vector<OldTLogConf>` | Previous generation configs |
| expectedLogSets | `int32_t` | Expected log set count |
| recruitmentID | `UID` | Recruitment ID |
| stopped | `bool` | Whether stopped |
| recoveredAt | `Optional<Version>` | Recovery version |
| pseudoLocalities | `std::set<int8_t>` | Pseudo-localities |
| epoch | `LogEpoch` → `uint64_t` | Current epoch |
| oldestBackupEpoch | `LogEpoch` → `uint64_t` | Oldest backup epoch |
| knownLockedTLogIds | `std::map<uint8_t, std::vector<uint16_t>>` | Known locked TLog IDs |

### CoreTLogSet
Persisted on coordinators (UIDs only, no full interfaces).

| Field | Type | Purpose |
|-------|------|---------|
| tLogs | `std::vector<UID>` | TLog UIDs |
| tLogWriteAntiQuorum | `int32_t` | Write anti-quorum |
| tLogReplicationFactor | `int32_t` | Replication factor |
| tLogPolicy | `Reference<IReplicationPolicy>` | Replication policy |
| tLogLocalities | `std::vector<LocalityData>` | TLog localities |
| isLocal | `bool` | Local DC |
| locality | `int8_t` | DC locality |
| startVersion | `Version` → `int64_t` | Start version |
| satelliteTagLocations | `std::vector<std::vector<int>>` | Satellite locations |
| tLogVersion | `TLogVersion` → `uint32_t` | TLog version |

### OldTLogCoreData

| Field | Type | Purpose |
|-------|------|---------|
| tLogs | `std::vector<CoreTLogSet>` | Core TLog sets |
| logRouterTags | `int32_t` | Log router tags |
| txsTags | `int32_t` | Txs tags |
| epochBegin | `Version` → `int64_t` | Epoch begin |
| epochEnd | `Version` → `int64_t` | Epoch end |
| recoverAt | `Version` → `int64_t` | Recovery version (conditional on protocol version) |
| pseudoLocalities | `std::set<int8_t>` | Pseudo-localities |
| epoch | `LogEpoch` → `uint64_t` | Epoch |

### DBCoreState
Persisted on coordinators — the ground truth for recovery.

| Field | Type | Purpose |
|-------|------|---------|
| tLogs | `std::vector<CoreTLogSet>` | Current TLog sets |
| logRouterTags | `int32_t` | Log router tags |
| txsTags | `int32_t` | Txs tags |
| oldTLogData | `std::vector<OldTLogCoreData>` | Previous generations |
| recoveryCount | `DBRecoveryCount` → `uint64_t` | Recovery counter |
| logSystemType | `LogSystemType` → enum (int) | Log system type |
| pseudoLocalities | `std::set<int8_t>` | Pseudo-localities |
| newestProtocolVersion | `ProtocolVersion` → `uint64_t` | Newest protocol (conditional) |
| lowestCompatibleProtocolVersion | `ProtocolVersion` → `uint64_t` | Lowest compatible (conditional) |
| encryptionAtRestModeDeprecated | `EncryptionAtRestModeDeprecated` → `uint32_t` | Encryption mode (conditional) |

### ServerDBInfo
Distributed to all server processes.

| Field | Type | Purpose |
|-------|------|---------|
| id | `UID` | Info snapshot ID |
| clusterInterface | `ClusterControllerFullInterface` | CC interface |
| client | `ClientDBInfo` | Client-facing info |
| distributor | `Optional<DataDistributorInterface>` | DD interface |
| master | `MasterInterface` | Master interface |
| ratekeeper | `Optional<RatekeeperInterface>` | RK interface |
| consistencyScan | `Optional<ConsistencyScanInterface>` | CS interface |
| resolvers | `std::vector<ResolverInterface>` | Resolver interfaces |
| recoveryCount | `DBRecoveryCount` → `uint64_t` | Recovery counter |
| recoveryState | `RecoveryState` → enum (int) | Recovery state |
| masterLifetime | `LifetimeToken` | Master lifetime token |
| logSystemConfig | `LogSystemConfig` | Log system config |
| priorCommittedLogServers | `std::vector<UID>` | Prior committed log servers |
| latencyBandConfig | `Optional<LatencyBandConfig>` | Latency band config |
| infoGeneration | `int64_t` | Info generation counter |

### LatencyBandConfig

| Field | Type | Purpose |
|-------|------|---------|
| grvConfig | `GrvConfig` | GRV latency bands |
| readConfig | `ReadConfig` | Read latency bands |
| commitConfig | `CommitConfig` | Commit latency bands |

Each config inherits from **RequestConfig** {`std::set<double> bands`} and adds:
- **ReadConfig**: `Optional<int> maxReadBytes`, `Optional<int> maxKeySelectorOffset`
- **CommitConfig**: `Optional<int> maxCommitBytes`

### UpdateServerDBInfoRequest

| Field | Type | Purpose |
|-------|------|---------|
| serializedDbInfo | `Standalone<StringRef>` | Serialized `ServerDBInfo` |
| broadcastInfo | `std::vector<Endpoint>` | Endpoints to broadcast to |
| reply | `ReplyPromise<std::vector<Endpoint>>` | Updated endpoints |

### GetServerDBInfoRequest

| Field | Type | Purpose |
|-------|------|---------|
| knownServerInfoID | `UID` | Last known info ID (long-poll) |
| reply | `ReplyPromise<ServerDBInfo>` | Server DB info |

---

## Appendix B: In-Band Log Messages

**Source:** `fdbserver/core/include/fdbserver/core/LogProtocolMessage.h`,
`fdbserver/core/include/fdbserver/core/SpanContextMessage.h`,
`fdbserver/core/include/fdbserver/core/OTELSpanContextMessage.h`

These messages are embedded in the TLog mutation stream (`messages` field of
`TLogCommitRequest` / `TLogPeekReply`). They use reserved `MutationRef` type codes.

### LogProtocolMessage
Marks a protocol version change in the log stream.

| Field | Type | Purpose |
|-------|------|---------|
| (type code) | `uint8_t` | `MutationRef::Reserved_For_LogProtocolMessage` |
| (version) | `ProtocolVersion` → `uint64_t` | New protocol version |

### SpanContextMessage

| Field | Type | Purpose |
|-------|------|---------|
| (type code) | `uint8_t` | `MutationRef::Reserved_For_SpanContextMessage` |
| spanContext | `SpanID` → `UID` (two `uint64_t`) | Tracing span context |

### OTELSpanContextMessage

| Field | Type | Purpose |
|-------|------|---------|
| (type code) | `uint8_t` | `MutationRef::Reserved_For_OTELSpanContextMessage` |
| spanContext | `SpanContext` | OpenTelemetry span context |

---

## Appendix C: Backpressure Protocol

**Source:** `fdbrpc/include/fdbrpc/fdbrpc.h`

For streaming replies (`ReplyPromiseStream`), FDB implements flow control via
acknowledgments:

### AcknowledgementReply
Sent from client to server to acknowledge received bytes.

| Field | Type | Purpose |
|-------|------|---------|
| bytes | `int64_t` | Total bytes acknowledged |

The server tracks `bytesSent` vs `bytesAcknowledged` and pauses when the
difference exceeds `bytesLimit`. On cancellation, the client sends
`operation_obsolete` on the acknowledgment stream.

### ReplyPromiseStreamReply (base for all streaming replies)

| Field | Type | Purpose |
|-------|------|---------|
| acknowledgeToken | `Optional<UID>` | Ack endpoint token (sent in first reply) |
| sequence | `uint16_t` | Monotonic sequence number |

---

## Appendix D: Client Library Protocol Flows

**Source:** `fdbclient/NativeAPI.actor.cpp`, `fdbclient/MonitorLeader.cpp`,
`fdbclient/GlobalConfig.cpp`

This appendix describes the exact sequence of protocol messages the FDB client
library sends during each operation, as implemented in the NativeAPI layer.

### D.1 Cluster Discovery and Connection

The client's first task is to discover the cluster controller and obtain the
current proxy list. This happens continuously in the background.

**Step 1: Parse connection file.**
The client reads the cluster file (e.g., `fdb.cluster`) containing
`description:id@coordinator1:port,coordinator2:port,...`. This yields a
`ClusterConnectionString`.

**Step 2: Contact coordinators (monitorProxies loop).**
The client enters an infinite loop that monitors proxy changes. For each
coordinator, it creates a `ClientLeaderRegInterface` and sends:

```
Client → Coordinator:  OpenDatabaseCoordRequest
    clusterKey:          Key (from connection string)
    hostnames:           std::vector<Hostname>
    coordinators:        std::vector<NetworkAddress>
    knownClientInfoID:   UID (last known ClientDBInfo.id, for long-poll)
    supportedVersions:   std::vector<ClientVersionRef>
    traceLogGroup:       Key
    internal:            bool
```

The coordinator responds with `CachedSerialization<ClientDBInfo>`, which the
client deserializes to obtain:

```
Coordinator → Client:  ClientDBInfo  (serialization order)
    grvProxies:     std::vector<GrvProxyInterface>
    commitProxies:  std::vector<CommitProxyInterface>
    id:             UID
    forward:        Optional<Value>   (if set, reconnect to new coordinators)
    history:        std::vector<VersionHistory>
    clusterId:      UID
    clusterType:    ClusterType
```

Note: `firstCommitProxy` is a struct member but is NOT serialized — it is
reconstructed locally.

If `forward` is set, the client switches to the new connection string and
restarts discovery. Otherwise, it stores the proxy lists and long-polls
(blocks until `id` changes).

**Step 3: Proxy list management.**
The client randomly shuffles the proxy lists and truncates them to
`MAX_COMMIT_PROXY_CONNECTIONS` and `MAX_GRV_PROXY_CONNECTIONS` to limit
connection fan-out. It selects `firstCommitProxy` for operations that must
go to a single proxy.

### D.2 Get Read Version (GRV)

Every transaction begins by acquiring a read version.

**Step 1: Check GRV cache.**
If GRV caching is enabled and a cached version is recent enough (within
`MAX_VERSION_CACHE_LAG`), the client returns the cached version immediately
with no network round-trip.

**Step 2: Batch GRV requests.**
Multiple concurrent transactions' GRV needs are batched together by the
`readVersionBatcher` actor. It collects requests until `MAX_BATCH_SIZE` is
reached or `GRV_BATCH_TIMEOUT` expires, then sends a single request.

**Step 3: Send batched request to GRV proxy.**

```
Client → GrvProxy:  GetReadVersionRequest
    spanContext:       SpanContext
    transactionCount:  uint32_t  (number of transactions in batch)
    flags:             uint32_t  (PRIORITY_SYSTEM_IMMEDIATE | PRIORITY_DEFAULT | PRIORITY_BATCH)
    tags:              TransactionTagMap<uint32_t>  (tag counts for throttling)
    debugID:           Optional<UID>
    maxVersion:        Version  (max version in client's version vector cache)
```

Proxy selection: `basicLoadBalance()` across `grvProxies[]` with round-robin.

**Step 4: Process reply.**

```
GrvProxy → Client:  GetReadVersionReply
    processBusyTime:           double
    version:                   Version    (the assigned read version)
    locked:                    bool
    metadataVersion:           Optional<Value>
    midShardSize:              int64_t
    rkDefaultThrottled:        bool
    rkBatchThrottled:          bool
    tagThrottleInfo:           TransactionTagMap<ClientTagThrottleLimits>
    proxyTagThrottledDuration: double
    ssVersionVectorDelta:      VersionVector
    proxyId:                   UID
```

The client caches the version, updates its tag throttle table, and applies
the storage server version vector delta for read-your-writes consistency.

### D.3 Key Location Discovery

Before reading, the client must know which storage servers hold the target keys.

**Step 1: Check location cache.**
The client maintains a `KeyRangeMap` mapping key ranges → storage server
interfaces. If the target range is cached, skip to the read.

**Step 2: Request locations from commit proxy.**

```
Client → CommitProxy:  GetKeyServerLocationsRequest
    spanContext:    SpanContext
    begin:          KeyRef       (start of range)
    end:            Optional<KeyRef>  (end of range, if range query)
    limit:          int          (max shard locations, typically 100)
    reverse:        bool
    legacyVersion:  Version
    arena:          Arena
```

Proxy selection: `commitProxyLoadBalance()` across `commitProxies[]`.

**Step 3: Cache response.**

```
CommitProxy → Client:  GetKeyServerLocationsReply
    results:            std::vector<pair<KeyRangeRef, std::vector<StorageServerInterface>>>
    resultsTssMapping:  std::vector<pair<UID, StorageServerInterface>>
    resultsTagMapping:  std::vector<pair<UID, Tag>>
    arena:              Arena
```

Each result maps a key range to the set of storage server replicas hosting it.
The client caches these mappings. TSS (Testing Storage Server) pairs are stored
separately for read validation.

### D.4 Reading Data

#### Single Key Read (getValue)

```
Client → StorageServer:  GetValueRequest  (serialization order)
    key:                     Key
    version:                 Version       (transaction's read version)
    tags:                    Optional<TagSet>
    reply:                   ReplyPromise<GetValueReply>
    spanContext:             SpanContext
    options:                 Optional<ReadOptions>
    ssLatestCommitVersions:  VersionVector
```

Server selection: `loadBalance()` across replicas from the location cache,
with locality-aware preference for same-DC servers.

```
StorageServer → Client:  GetValueReply
    penalty:  double          (load balancing hint)
    error:    Optional<Error>
    value:    Optional<Value> (None if key not found)
    cached:   bool
```

#### Range Read (getRange / getKeyValues)

```
Client → StorageServer:  GetKeyValuesRequest  (serialization order)
    begin:                   KeySelectorRef
    end:                     KeySelectorRef
    version:                 Version
    limit:                   int     (row limit; negative for reverse)
    limitBytes:              int     (byte limit)
    tags:                    Optional<TagSet>
    reply:                   ReplyPromise<GetKeyValuesReply>
    spanContext:             SpanContext
    options:                 Optional<ReadOptions>
    ssLatestCommitVersions:  VersionVector
    taskID:                  Optional<TaskPriority>
    arena:                   Arena
```

```
StorageServer → Client:  GetKeyValuesReply
    penalty:  double
    error:    Optional<Error>
    data:     VectorRef<KeyValueRef>  (key-value pairs)
    version:  Version
    more:     bool    (true if more data available)
    cached:   bool
    arena:    Arena
```

If `more` is true, the client adjusts the key selectors and sends another
request. If the range spans multiple shards, the client sends parallel
requests to different storage servers and merges results.

**Error handling during reads:**
- `wrong_shard_server`: Shard moved; invalidate location cache, re-lookup, retry.
- `all_alternatives_failed`: All replicas failed; same as wrong_shard_server.
- `future_version`: Requested version not yet available; wait and retry.
- `transaction_too_old`: Version expired; restart transaction with new GRV.

#### Streaming Range Read (getKeyValuesStream)

For large range reads, the client can use the streaming variant:

```
Client → StorageServer:  GetKeyValuesStreamRequest
    (same fields as GetKeyValuesRequest, without taskID)
    reply:  ReplyPromiseStream<GetKeyValuesStreamReply>
```

This returns a stream of `GetKeyValuesStreamReply` messages with backpressure
(see Appendix C). Each reply carries `acknowledgeToken` and `sequence` for
flow control.

### D.5 Committing a Transaction

**Step 1: Build commit request.**
The client accumulates mutations (set, clear, atomic operations) and conflict
ranges during the transaction. When `commit()` is called:

```
Client → CommitProxy:  CommitTransactionRequest
    spanContext:           SpanContext
    transaction:           CommitTransactionRef
        read_conflict_ranges:   VectorRef<KeyRangeRef>
        write_conflict_ranges:  VectorRef<KeyRangeRef>
        mutations:              VectorRef<MutationRef>
        read_snapshot:          Version  (the read version)
        report_conflicting_keys: bool
        lock_aware:             bool
        spanContext:            Optional<SpanContext>
        tenantIds:              Optional<VectorRef<int64_t>>
    flags:                 uint32_t  (LOCK_AWARE, FIRST_IN_BATCH, etc.)
    debugID:               Optional<UID>
    commitCostEstimation:  Optional<ClientTrCommitCostEstimation>
    tagSet:                Optional<TagSet>
    idempotencyId:         IdempotencyIdRef
    arena:                 Arena
```

Proxy selection: `basicLoadBalance()` across `commitProxies[]`, or
`firstCommitProxy` if `COMMIT_ON_FIRST_PROXY` option is set.

**Step 2: Wait for reply.**

```
CommitProxy → Client:  CommitID
    version:                Version    (committed version, or invalidVersion on failure)
    txnBatchId:             uint16_t   (for constructing versionstamps)
    metadataVersion:        Optional<Value>
    conflictingKRIndices:   Optional<Standalone<VectorRef<int>>>
```

**Step 3: Handle result.**
- **Success** (`version != invalidVersion`): Transaction is committed. The
  client constructs a `Versionstamp` from `version` and `txnBatchId`, updates
  its committed version, and sends `ExpireIdempotencyIdRequest` if using
  idempotency.
- **Conflict** (`not_committed`): Transaction conflicted; throw to caller.
- **Unknown** (`commit_unknown_result`): Proxy may have crashed after committing.
  The client can retry with idempotency to determine the outcome.

### D.6 Watching a Key

```
Client → StorageServer:  WatchValueRequest
    spanContext:  SpanContext
    key:          Key
    value:        Optional<Value>  (expected current value)
    version:      Version          (version at which value was read)
    tags:         Optional<TagSet>
    debugID:      Optional<UID>
```

This is a long-poll: the storage server holds the request until the key's value
changes, then replies:

```
StorageServer → Client:  WatchValueReply
    version:  Version  (version at which value changed)
    cached:   bool
```

The client then waits for `waitForCommittedVersion(version)` to ensure the
change is durable before triggering the watch future. If the watch is cancelled
by a `watch_cancelled` error (too many concurrent watches), the client retries
with backoff.

### D.7 Change Feed Streaming

```
Client → StorageServer:  ChangeFeedStreamRequest
    spanContext:      SpanContext
    rangeID:          Key           (change feed identifier)
    begin:            Version       (start version)
    end:              Version       (end version)
    range:            KeyRange
    replyBufferSize:  int
    canReadPopped:    bool
    id:               UID
    options:          Optional<ReadOptions>
    encrypted:        bool
    arena:            Arena
    reply:            ReplyPromiseStream<ChangeFeedStreamReply>
```

Returns a stream of `ChangeFeedStreamReply` messages, each containing:
```
    mutations:        VectorRef<MutationsAndVersionRef>
    atLatestVersion:  bool
    minStreamVersion: Version
    popVersion:       Version
```

### D.8 Global Configuration Refresh

Runs periodically in the background:

```
Client → GrvProxy:  GlobalConfigRefreshRequest
    lastKnown:  Version  (last known config version)
```

```
GrvProxy → Client:  GlobalConfigRefreshReply
    version:  Version
    result:   RangeResultRef  (config key-value pairs)
    arena:    Arena
```

### D.9 Error Handling and Retry Strategy

| Error | Meaning | Client Action |
|-------|---------|---------------|
| `wrong_shard_server` | Key range relocated | Invalidate cache, retry |
| `all_alternatives_failed` | All replicas unreachable | Same as wrong_shard_server |
| `transaction_too_old` | Read version expired | Get new GRV, restart transaction |
| `future_version` | Version not yet available | Wait `FUTURE_VERSION_RETRY_DELAY`, retry |
| `not_committed` | Transaction conflicted | Throw conflict error to caller |
| `commit_unknown_result` | Proxy crash mid-commit | Verify via idempotency or retry |
| `commit_proxy_memory_limit_exceeded` | Proxy overloaded | Exponential backoff |
| `batch_transaction_throttled` | Batch priority throttled | Retry after delay |
| `proxy_tag_throttled` | Tag throttled by ratekeeper | Wait for throttle expiration |

Backoff: starts at `CLIENT_KNOBS->BACKOFF_DELAY`, multiplied by
`BACKOFF_GROWTH_RATE` on each failure, capped at
`RESOURCE_CONSTRAINED_MAX_BACKOFF`.

---

## Appendix E: fdbcli Protocol Usage

**Source:** `fdbcli/*.actor.cpp`, `fdbclient/include/fdbclient/ManagementAPI.h`

The `fdbcli` command-line tool uses three protocol layers:
1. **Normal transactions** via the NativeAPI (same as any client).
2. **Special Key Space writes** — transactions that write to `\xff\xff/management/`
   keys, which the commit proxy intercepts and translates into cluster operations.
3. **Direct RPC** to ClusterInterface or worker endpoints.

### E.1 Direct RPC Commands

These commands send protocol messages directly to cluster endpoints, bypassing
the transaction layer.

#### `status` Command
```
fdbcli → ClusterController:  StatusRequest
    statusField:  std::string  (optional filter like "json")
    reply:        ReplyPromise<StatusReply>
```
Reply: `StatusReply` containing `statusStr` (JSON string).

#### `force_recovery_with_data_loss <DCID>`
```
fdbcli → ClusterController:  ForceRecoveryRequest
    dcId:   Key  (datacenter ID)
    reply:  ReplyPromise<Void>
```

#### `kill` / `suspend` / `expensive_data_check`
First discovers workers via:
```
fdbcli → ClusterController:  GetClientWorkersRequest
    reply:  ReplyPromise<std::vector<ClientWorkerInterface>>
```

Then sends to each target worker:
```
fdbcli → Worker:  RebootRequest  (fire-and-forget)
    deleteData:       bool      (false for kill/suspend)
    checkData:        bool      (true for expensive_data_check)
    waitForDuration:  uint32_t  (0 for kill, >0 for suspend seconds)
```

#### `profile` Command (flow/heap profiling)
```
fdbcli → Worker:  ProfilerRequest
    type:       Type enum    (GPROF, FLOW, GPROF_HEAP)
    action:     Action enum  (ENABLE, DISABLE, RUN)
    duration:   int
    outputFile: Standalone<StringRef>
    reply:      ReplyPromise<Void>
```

#### `audit_storage` Command
```
fdbcli → ClusterController:  TriggerAuditRequest
    type:       uint8_t  (ValidateHA, ValidateReplica, ValidateLocationMetadata, ...)
    range:      KeyRange
    id:         UID
    cancel:     bool
    engineType: KeyValueStoreType (uint32_t)
    reply:      ReplyPromise<UID>
```

#### `snapshot` Command
Uses `IDatabase::createSnapshot()` which sends `ProxySnapRequest` to a commit
proxy. The proxy then forwards a `DistributorSnapRequest` to the data distributor,
which orchestrates the actual snapshot across TLogs and storage servers.

### E.2 Special Key Space Commands

These commands write to `\xff\xff/management/` keys within a transaction with
`SPECIAL_KEY_SPACE_ENABLE_WRITES` option. The commit proxy intercepts these
writes and performs the corresponding cluster operations.

#### `exclude` / `include` Commands
```
Write to: \xff\xff/management/excluded/<ADDRESS> = ""
Write to: \xff\xff/management/failed/<ADDRESS> = ""
Write to: \xff\xff/management/excluded_locality/<LOCALITY> = ""
Write to: \xff\xff/management/failed_locality/<LOCALITY> = ""

Options:
Write to: \xff\xff/management/options/excluded/force = ""   (skip safety check)

Read from: \xff\xff/management/in_progress_exclusion/<ADDRESS>  (check progress)
```

#### `configure` Command
Uses `ManagementAPI::changeConfig()` which:
1. Reads current configuration from `\xff/conf/` system keys.
2. Writes updated configuration via a normal transaction.
3. The commit proxy detects system key changes and triggers reconfiguration.

#### `coordinators` Command
```
Write to: \xff\xff/configuration/coordinators/processes = "<addr1>,<addr2>,..."
Write to: \xff\xff/configuration/coordinators/cluster_description = "<name>"

Read from: \xff\xff/management/auto_coordinators  (get auto-selected coordinators)
```

#### `lock` / `unlock` Commands
```
Write to: \xff\xff/management/db_locked = <UID>   (lock)
Clear:    \xff\xff/management/db_locked             (unlock, requires matching UID)
```

#### `maintenance` Command
```
Write to: \xff\xff/management/maintenance/<ZONEID> = <duration_seconds>
Write to: \xff\xff/management/maintenance/IgnoreSSFailures = ""  (disable DD for SS failures)
Clear:    \xff\xff/management/maintenance/<ZONEID>  (end maintenance)
```

#### `datadistribution` Command
```
Write to: \xff\xff/management/data_distribution/mode = "0" or "1"  (off/on)
Write to: \xff\xff/management/data_distribution/rebalance_ignored = <flags>
```

#### `advanceversion` Command
```
Write to: \xff\xff/management/min_required_commit_version = <version>
```

#### `throttle` Command
Uses `ThrottleApi` functions which read/write:
```
\xff\x02/throttledTags/tag/<tag>/<type>/<priority> = TagThrottleValue
\xff\x02/throttledTags/autoThrottledTags/<tag>/<type>/<priority> = TagThrottleValue
```

#### `setclass` Command
Uses `ManagementAPI::setClass()` which writes to system keys to change a
worker's process class assignment.

### E.3 Backup / Restore CLI Commands

Backup and restore commands use the `FileBackupAgent` and `DatabaseBackupAgent`
classes, which operate through normal transactions reading/writing system keys:

```
\xff\x02/fdbbackup/        — backup task state
\xff\x02/fdbrestore/       — restore task state
\xff\x02/backupProgress/   — backup progress markers
\xff\x02/backupStarted/    — backup start markers
```

These are standard key-value operations — the backup agents are implemented as
Taskbucket workflows that run as normal transactions within the cluster.

### E.4 Transaction Options Used by fdbcli

| Option | Purpose |
|--------|---------|
| `SPECIAL_KEY_SPACE_ENABLE_WRITES` | Enable writes to `\xff\xff/` keys |
| `PRIORITY_SYSTEM_IMMEDIATE` | Highest priority (bypass throttling) |
| `READ_SYSTEM_KEYS` | Allow reading `\xff/` system keys |
| `LOCK_AWARE` | Operate while database is locked |
| `ACCESS_SYSTEM_KEYS` | Full access to system keyspace |

---

## Appendix F: Server-Side Protocol Flows

**Source:** `fdbserver/commitproxy/CommitProxyServer.actor.cpp`,
`fdbserver/clustercontroller/ClusterRecovery.cpp`,
`fdbserver/storageserver/storageserver.actor.cpp`,
`fdbserver/sequencer/masterserver.cpp`

### F.1 Transaction Commit Path

This is the critical write path: how a client's `CommitTransactionRequest`
becomes durable. The commit proxy orchestrates a multi-phase protocol.

```
┌────────┐     ┌──────────────┐     ┌────────┐     ┌──────────┐     ┌───────┐
│ Client │────▶│ Commit Proxy │────▶│ Master │     │ Resolver │     │ TLog  │
└────────┘     └──────────────┘     └────────┘     └──────────┘     └───────┘
                      │                  │               │               │
                      │ GetCommitVersion │               │               │
                      │─────────────────▶│               │               │
                      │   Reply(version) │               │               │
                      │◀─────────────────│               │               │
                      │                                  │               │
                      │  ResolveTransactionBatchRequest   │               │
                      │─────────────────────────────────▶│               │
                      │  Reply(committed[], conflicts)   │               │
                      │◀─────────────────────────────────│               │
                      │                                                  │
                      │              TLogCommitRequest                   │
                      │─────────────────────────────────────────────────▶│
                      │              TLogCommitReply                     │
                      │◀─────────────────────────────────────────────────│
                      │                  │                               │
                      │ ReportRawCommittedVersion                        │
                      │─────────────────▶│                               │
```

**Phase 1: Get Commit Version**

The commit proxy batches incoming `CommitTransactionRequest` messages and
requests a commit version from the master:

```
CommitProxy → Master:  GetCommitVersionRequest
    spanContext:                      SpanContext
    requestNum:                      uint64_t  (monotonic per-proxy)
    mostRecentProcessedRequestNum:   uint64_t
    requestingProxy:                 UID
```

```
Master → CommitProxy:  GetCommitVersionReply
    resolverChanges:         Standalone<VectorRef<ResolverMoveRef>>
    resolverChangesVersion:  Version
    version:                 Version  (assigned commit version)
    prevVersion:             Version
    requestNum:              uint64_t
```

The master assigns monotonically increasing versions. If resolver topology has
changed, it includes `resolverChanges` so the proxy knows which resolver
handles which key ranges.

**Phase 2: Conflict Resolution**

The proxy sends the transaction batch to each resolver. Each resolver is
responsible for a range of keys. Transactions whose read/write conflict ranges
span multiple resolvers are sent to all relevant resolvers.

```
CommitProxy → Resolver[i]:  ResolveTransactionBatchRequest
    spanContext:           SpanContext
    prevVersion:           Version
    version:               Version
    lastReceivedVersion:   Version
    transactions:          VectorRef<CommitTransactionRef>
    txnStateTransactions:  VectorRef<int>
    debugID:               Optional<UID>
    writtenTags:           std::set<Tag>
    lastShardMove:         Version
    arena:                 Arena
```

```
Resolver[i] → CommitProxy:  ResolveTransactionBatchReply
    committed:               VectorRef<uint8_t>  (per-txn: 0=ok, 1=conflict)
    stateMutations:          VectorRef<VectorRef<StateTransactionRef>>
    conflictingKeyRangeMap:  std::map<int, VectorRef<int>>
    privateMutations:        VectorRef<StringRef>
    privateMutationCount:    uint32_t
    tpcvMap:                 std::unordered_map<uint16_t, Version>
    writtenTags:             std::set<Tag>
    lastShardMove:           Version
    arena:                   Arena
```

The proxy merges results from all resolvers. A transaction is committed only
if all resolvers agree it has no conflicts.

**Phase 3: TLog Push**

For committed transactions, the proxy serializes mutations into a `LogPushData`
object. Each mutation is tagged with the `Tag` of the storage server(s) that
should receive it. The proxy then pushes to all TLog replicas:

```
CommitProxy → TLog[j]:  TLogCommitRequest
    spanContext:              SpanContext
    prevVersion:              Version
    version:                  Version
    knownCommittedVersion:    Version
    minKnownCommittedVersion: Version
    seqPrevVersion:           Version
    messages:                 StringRef  (packed, tagged mutations)
    tLogCount:                uint16_t
    tLogLocIds:               std::vector<uint16_t>
    debugID:                  Optional<UID>
    arena:                    Arena
```

The commit is durable when a quorum of TLog replicas acknowledge:

```
TLog[j] → CommitProxy:  TLogCommitReply
    version:  Version  (durable version)
```

**Phase 4: Report Committed Version**

```
CommitProxy → Master:  ReportRawCommittedVersionRequest
    version:                  Version
    locked:                   bool
    metadataVersion:          Optional<Value>
    minKnownCommittedVersion: Version
    prevVersion:              Optional<Version>
    writtenTags:              Optional<std::set<Tag>>
```

The master updates its committed version. The proxy then replies to the
original client with `CommitID`.

### F.2 Recovery Protocol

Recovery happens when the cluster controller detects that the master has
failed, or during initial cluster startup. It is a complex multi-phase
protocol that establishes a new transaction system generation.

```
Phase 0:  CC recruits new roles
Phase 1:  Lock old TLogs to freeze previous generation
Phase 2:  Determine recovery point (last committed version)
Phase 3:  Recruit and initialize new TLogs
Phase 4:  Initialize resolvers and proxies with transaction state
Phase 5:  Write new DBCoreState to coordinators
Phase 6:  Begin accepting commits
```

**Phase 0: Role Recruitment**

```
ClusterController → Workers:  RecruitFromConfigurationRequest
    configuration:      DatabaseConfiguration
    recruitSeedServers: bool
    maxOldLogRouters:   int
```

```
Workers → ClusterController:  RecruitFromConfigurationReply
    tLogs:           std::vector<WorkerInterface>
    satelliteTLogs:  std::vector<WorkerInterface>
    commitProxies:   std::vector<WorkerInterface>
    grvProxies:      std::vector<WorkerInterface>
    resolvers:       std::vector<WorkerInterface>
    storageServers:  std::vector<WorkerInterface>
    oldLogRouters:   std::vector<WorkerInterface>
    backupWorkers:   std::vector<WorkerInterface>
    dcId:            Optional<Key>
    satelliteFallback: bool
```

**Phase 1: Lock Old TLogs**

The master sends a lock request to every TLog from the previous generation:

```
Master → OldTLog[i]:  TLogInterface::lock  (ReplyPromise<TLogLockResult>)
```

```
OldTLog[i] → Master:  TLogLockResult
    end:                       Version    (last version on this TLog)
    knownCommittedVersion:     Version
    unknownCommittedVersions:  std::deque<UnknownCommittedVersions>
    id:                        UID
    logId:                     UID
```

The master gathers all lock results to determine the recovery point: the
highest version that reached a quorum of TLogs.

**Phase 2: Initialize New TLogs**

```
ClusterController → NewTLog[j]:  InitializeTLogRequest
    recruitmentID:    UID
    recoverFrom:      LogSystemConfig  (previous generation's config)
    recoverAt:        Version
    knownCommittedVersion: Version
    epoch:            LogEpoch
    recoverTags:      std::vector<Tag>
    allTags:          std::vector<Tag>
    logVersion:       TLogVersion
    storeType:        KeyValueStoreType
    spillType:        TLogSpillType
    remoteTag:        Tag
    locality:         int8_t
    isPrimary:        bool
    startVersion:     Version
    logRouterTags:    int
    txsTags:          int
    recoveryTransactionVersion: Version
    oldGenerationRecoverAtVersions: std::vector<Version>
```

```
NewTLog[j] → ClusterController:  TLogInterface
    (the new TLog's interface, with all its RequestStream endpoints)
```

**Phase 3: Initialize Proxies and Resolvers**

```
CC → NewProxy:  InitializeCommitProxyRequest
    master:                     MasterInterface
    masterLifetime:             LifetimeToken
    recoveryCount:              uint64_t
    recoveryTransactionVersion: Version
    firstProxy:                 bool
    encryptMode:                EncryptionAtRestModeDeprecated
    commitProxyIndex:           uint16_t
```

```
CC → NewResolver:  InitializeResolverRequest
    masterLifetime:  LifetimeToken
    recoveryCount:   uint64_t
    commitProxyCount: int
    resolverCount:   int
    masterId:        UID
    encryptMode:     EncryptionAtRestModeDeprecated
```

**Phase 4: Broadcast Transaction State**

The master broadcasts the transaction system state (metadata about shards,
server assignments, etc.) to all resolvers:

```
Master → Resolver[i]:  TxnStateRequest  (sent in chunks)
    data:           VectorRef<KeyValueRef>  (transaction state KV pairs)
    sequence:       Sequence  (chunk sequence number)
    last:           bool      (true for final chunk)
    broadcastInfo:  std::vector<Endpoint>
```

**Phase 5: Write to Coordinators**

The master writes the new `DBCoreState` to the coordination service using
`GenerationRegWriteRequest`. This is the atomic commit point — once this
write succeeds, the new generation is official.

**Phase 6: Accept Commits**

The master updates `ServerDBInfo` and broadcasts it to all workers:

```
CC → Workers:  UpdateServerDBInfoRequest
    serializedDbInfo:  Standalone<StringRef>  (serialized ServerDBInfo)
    broadcastInfo:     std::vector<Endpoint>
```

The master then sets `recoveryReadyForCommits`, allowing commit proxies to
begin processing client transactions.

### F.3 Storage Server Data Flow

Storage servers pull mutations from TLogs using a continuous peek cursor.
The storage server calls `logSystem->peekSingle()` which creates a cursor
for its assigned tag. By default (knob `PEEK_USING_STREAMING = false`), this
uses regular `TLogPeekRequest` messages. If `PEEK_USING_STREAMING` is enabled,
it uses the streaming `TLogPeekStreamRequest` variant instead.

**Steady-State Mutation Pull (default, non-streaming):**

```
StorageServer → TLog:  TLogPeekRequest
    begin:              Version
    tag:                Tag     (this SS's tag)
    returnIfBlocked:    bool
    onlySpilled:        bool
    sequence:           Optional<std::pair<UID, int>>
    end:                Optional<Version>
    returnEmptyIfStopped: Optional<bool>
```

```
TLog → StorageServer:  TLogPeekReply
    messages:                  StringRef  (packed mutations)
    end:                       Version
    popped:                    Optional<Version>
    maxKnownVersion:           Version
    minKnownCommittedVersion:  Version
    begin:                     Optional<Version>
    onlySpilled:               bool
    arena:                     Arena
```

**Streaming variant (when `PEEK_USING_STREAMING = true`):**

```
StorageServer → TLog:  TLogPeekStreamRequest
    begin:              Version
    tag:                Tag
    returnIfBlocked:    bool
    limitBytes:         int
    end:                Optional<Version>
    returnEmptyIfStopped: Optional<bool>
    reply:              ReplyPromiseStream<TLogPeekStreamReply>
```

The storage server deserializes mutations from `messages`, applies them to its
in-memory versioned data structure, and periodically makes them durable.

**Pop (Garbage Collection):**

Once a storage server has durably written mutations, it tells the TLog to
discard them:

```
StorageServer → TLog:  TLogPopRequest
    to:                           Version  (pop up to this version)
    durableKnownCommittedVersion: Version
    tag:                          Tag
```

**Shard Movement (Data Distribution):**

When data distribution moves a shard to a new storage server:

1. The new SS begins peeking the TLog for the shard's tag at the transfer version.
2. The new SS uses `Transaction::getRange()` (or `getRangeStream()` if
   `FETCH_USING_STREAMING` is enabled) to read the shard's data. This goes through
   the normal client path: commit proxy for location lookup, then storage server
   reads via `GetKeyValuesRequest` (or `GetKeyValuesStreamRequest`). The reads
   target the old storage servers that still hold the shard.
3. Once caught up, the new SS transitions the shard to `ReadWrite` state.
4. Data distribution updates the shard map (via system key transactions through
   the commit proxy), and future `GetKeyServerLocationsReply` messages will
   direct clients to the new SS.
