# Subsystem 2: RPC & Transport

**[Diagrams](diagram_02_rpc_transport.md)**

**Location:** [`fdbrpc/`](https://github.com/apple/foundationdb/tree/main/fdbrpc)
**Size:** ~28K implementation + headers  
**Role:** Maps Flow's actor model onto a network -- makes Future/Promise work across process boundaries.

---

## Overview

FlowTransport is the layer that lets actors on different processes (or simulated processes) communicate transparently. It provides endpoint-addressed messaging with both reliable (at-least-once) and unreliable (at-most-once) delivery semantics. In simulation, the same code runs in-process with virtual routing via Sim2.

---

## Key Data Structures

### Endpoint -- [`FlowTransport.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FlowTransport.h)`:43-120`

A globally unique address for a message receiver:

```
class Endpoint {
    NetworkAddressList addresses;  // primary + optional secondary (TLS/non-TLS)
    Token token;                   // UID: 128-bit unique identifier
};
```

- **Token** = `UID` (pair of `uint64_t`). The low 32 bits of the second word are the endpoint's index into the `EndpointMap` — that is what `get()` looks up — and the map entry reuses that same field to hold the receiver's `TaskPriority`. The first word is a random base shared across an interface's contiguous block of endpoints.
- **Well-known tokens**: `wellKnownToken(int id)` returns `UID(-1, id)`. Reserved IDs: `WLTOKEN_ENDPOINT_NOT_FOUND(0)`, `WLTOKEN_PING_PACKET`, `WLTOKEN_UNAUTHORIZED_ENDPOINT`, plus system services (leader election, config transactions, etc.)
- **Address selection**: `choosePrimaryAddress()` swaps primary/secondary based on local TLS preference.

### Peer -- [`FlowTransport.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FlowTransport.h)`:149-193`

Per-destination connection state:

```
struct Peer : ReferenceCounted<Peer> {
    NetworkAddress destination;
    UnsentPacketQueue unsent;          // buffered outgoing packets
    ReliablePacketList reliable;       // retransmit list (circular linked)
    AsyncTrigger dataToSend;           // signal: unsent queue non-empty
    Future<Void> connect;              // connectionKeeper actor
    bool compatible, connected;
    double reconnectionDelay;          // exponential backoff
    int peerReferences;                // stream reference count (-1 = no streams)
    int outstandingReplies;
    DDSketch<double> pingLatencies;    // latency distribution
    AsyncVar<Optional<ProtocolVersion>> protocolVersion;
    Promise<Void> disconnect;          // notified on connection loss
};
```

**Connection lifecycle:**
1. First send to address creates Peer and starts `connectionKeeper()` actor
2. `connectionKeeper()` calls `INetworkConnections::connect()`, performs TLS handshake
3. Sends `ConnectPacket` (protocol version, local address, connection ID)
4. Spawns `connectionWriter()` (async write loop) and `connectionReader()` (async read loop)
5. `connectionMonitor()` sends periodic pings, detects timeouts
6. On failure: exponential backoff (INITIAL_RECONNECTION_TIME to MAX_RECONNECTION_TIME, growth factor 1.2x)
7. `discardUnreliablePackets()` on disconnect; reliable packets resent after reconnect

### EndpointMap -- [`FlowTransport.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/FlowTransport.cpp)`:90-230`

Maps tokens to local message receivers:

```
struct Entry {
    union { uint64_t uid[2]; uint32_t nextFree; };
    NetworkMessageReceiver* receiver;
};
```

- Pre-allocates slots for well-known endpoints (indices 0 to wellKnownEndpointCount-1)
- Dynamic endpoints allocated from free list; doubles table size when full
- `get(token)` -- O(1) lookup by token's lower 32 bits
- `insert()` -- allocates from free list (single endpoint) or a contiguous block (the `streams` overload, keyed off a fresh random base UID); stores the receiver's priority in the entry token's low 32 bits
- `remove()` -- returns slot to free list

### FlowTransport -- [`FlowTransport.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FlowTransport.h)`:199-315`

Singleton managing all network communication:

**Key methods:**
- `bind(publicAddress, listenAddress)` -- start listening
- `sendReliable(data, endpoint)` -- guaranteed delivery (retransmit on reconnect)
- `sendUnreliable(data, endpoint, openConnection)` -- fire-and-forget
- `addEndpoint(endpoint, receiver, priority)` -- register local endpoint
- `addWellKnownEndpoint(endpoint, receiver, priority)` -- register system service
- `removeEndpoint(endpoint, receiver)` -- unregister
- `resetConnection(address)` -- force reconnect
- `getDegraded()` -- `AsyncVar<bool>` for network degradation status

---

## Request/Reply Pattern -- [`fdbrpc.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/fdbrpc.h)

The fundamental communication pattern in FDB:

### RequestStream<T> ([`fdbrpc.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/fdbrpc.h)`:728-949`)

A typed channel for sending requests to a service. Wraps a `NetNotifiedQueue<T>`:

```cpp
RequestStream<GetValueRequest> getValue;  // on StorageServerInterface

// Sending a request:
GetValueRequest req;
req.key = myKey;
req.reply = ReplyPromise<GetValueReply>();  // self-addressed envelope
stream.send(req);
GetValueReply reply = wait(req.reply.getFuture());
```

### ReplyPromise<T> ([`fdbrpc.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/fdbrpc.h)`:131-204`)

Single-value reply channel embedded in requests:

- Wraps a `NetSAV<T>` (SAV + FlowReceiver + FastAllocated)
- Serialization only stores the endpoint token (16 bytes on wire)
- On deserialize at receiver: creates local endpoint to receive reply

### Complete Request-Reply Lifecycle

```
CLIENT:
1. Create request with embedded ReplyPromise<T>
2. stream.send(request) → serialize → FlowTransport.sendReliable()
3. Transport finds/creates Peer → queues in unsent buffer
4. connectionWriter() sends packet: [length][checksum][token][serialized data]
5. Client waits on reply.getFuture()

SERVER:
6. connectionReader() receives packet → scanPackets()
7. Extract token → EndpointMap lookup → find receiver
8. deliver() actor: yield to correct TaskPriority, call receiver.receive()
9. NetNotifiedQueue deserializes request, queues for application
10. Application calls request.reply.send(response)
11. sendUnreliable() back to client's reply endpoint

CLIENT:
12. connectionReader() receives reply packet
13. Token lookup → NetSAV::receive() → deserialize → SAV::send(value)
14. Client's wait() resolves
```

### ReplyPromiseStream<T> ([`fdbrpc.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/fdbrpc.h)`:460-640`)

Stream of replies with flow control:
- Server sends multiple responses
- Client acknowledges consumed bytes
- `setByteLimit()` configures backpressure threshold
- `onReady()` blocks server when too much data in-flight

---

## Interface Endpoint Layout

Service interfaces (e.g., `CommitProxyInterface`, `GrvProxyInterface`, `StorageServerInterface`) bundle many `RequestStream<T>` channels but ship a single endpoint over the wire. The rest are reconstructed locally by adding a fixed offset to that anchor.

### Convention

Each interface picks an "anchor" stream (typically the most-used one: `commit` for the commit proxy, `getConsistentReadVersion` for the GRV proxy, `getValue` for storage servers). It is the only `RequestStream` actually serialized. All other streams are reconstructed in the `if (Archive::isDeserializing)` branch via `anchor.getEndpoint().getAdjustedEndpoint(N)`, where N is the stream's position in `initEndpoints`'s `push_back` order.

```cpp
// CommitProxyInterface.h (excerpt)
template <class Archive>
void serialize(Archive& ar) {
    serializer(ar, processId, provisional, commit);  // commit is the anchor — the only RequestStream on the wire
    if (Archive::isDeserializing) {
        legacyGetConsistentReadVersion = ...(commit.getEndpoint().getAdjustedEndpoint(1));
        getKeyServersLocations         = ...(commit.getEndpoint().getAdjustedEndpoint(2));
        // ...
    }
}

void initEndpoints() {
    std::vector<...> streams;
    streams.push_back(commit.getReceiver(...));                         // index 0 — anchor
    streams.push_back(legacyGetConsistentReadVersion.getReceiver(...)); // 1
    streams.push_back(getKeyServersLocations.getReceiver(...));         // 2
    // ...
    FlowTransport::transport().addEndpoints(streams);
}
```

`EndpointMap::insert` allocates the registered receivers as a contiguous block keyed off a fresh random `base` UID. Stream `i` ends up at token offset `i` from the anchor, and `getAdjustedEndpoint(N)` produces the matching token. Client and server agree iff the client's deserialization index matches the server's registration order.

### The endpoint layout is a wire-compatibility contract

The offsets look like a private, per-build implementation detail. They are not. The offset of every stream within an interface is part of the wire protocol, and it must stay identical across **every binary that runs a compatible protocol version** — not merely across binaries built from the same source tree.

The binaries that actually differ are `fdbserver` and `fdbclient`. A cluster's server processes are upgraded together: an upgrade is a coordinated restart that triggers a recovery, so the cluster's `fdbserver` processes always run a single build, and server-to-server interface reconstruction — for example, Ratekeeper rebuilding a `CommitProxyInterface` from the broadcast `ServerDBInfo` — is effectively same-build. Client libraries are not: they are versioned and deployed independently of the cluster and are *not* upgraded in lockstep with it. An application links `fdb_c` libraries that may have been built long before, or after, the `fdbserver` it happens to connect to.

What lets a client talk to that server is *protocol compatibility*, not an identical build:

1. **FlowTransport connects compatible peers, not identical ones.** Two protocol versions are compatible when their high 48 bits match — `isCompatible` compares `version() & compatibleProtocolVersionMask`, where `compatibleProtocolVersionMask = 0xFFFFFFFFFFFF0000` (see [`ProtocolVersion.h`](https://github.com/apple/foundationdb/blob/main/flow/ProtocolVersion.h.cmake)). The low 16 bits never affect compatibility, and patch releases of an `x.y` line are *required* to keep the same protocol version (see `cmake/ProtocolVersions.cmake`: "This version impacts both communications and the deserialization of certain database and IKeyValueStore keys"). A single compatible protocol version therefore spans many distinct builds.
2. **The multi-version client selects a library by *normalized* (compatible) version.** `MultiVersionDatabase` indexes loaded client libraries by `protocolVersion.normalizedVersion()` and keeps the same connection when the cluster's protocol version changes but stays compatible (see [`subsystem_03_client_library.md`](subsystem_03_client_library.md)). The library it picks need only be *compatible* with the cluster, so it is routinely a different build than the `fdbserver` it connects to — and its compiled-in endpoint offsets must still match what that server registered.

A client reconstructs the *entire* interface (every stream in `commitProxies`/`grvProxies`) but only sends to the streams it actually uses. So a client-facing stream carries the cross-build `fdbserver`↔`fdbclient` contract, whereas a server-only stream (one no client sends to, such as `setThrottledShard`) is exercised only on the same-build server-to-server path — its realistic failure mode is the *local* `serialize`/`initEndpoints` misalignment described below, not a cross-build mismatch.

Two facts about tokens are true but do *not* license repacking: tokens are not persistent (every process gets a fresh random `base` UID, so no stale token survives a restart), and recovery reissues every interface via fresh `ServerDBInfo`/`ClientDBInfo` (see [`subsystem_09_cluster_recovery.md`](subsystem_09_cluster_recovery.md)). Both keep the *anchor* token fresh — but the offsets relative to that anchor are still reconstructed on the far side from the reader's compiled-in indices, so they remain a cross-binary contract whenever a client and server are different builds.

### Evolving an interface safely

There are two invariants, one local and one global:

- **Local (necessary).** Within a single build, the `getAdjustedEndpoint(N)` argument in `serialize()` must equal the `push_back` position of the same stream in `initEndpoints()`. If they differ, even two *identical* binaries mis-route: the reconstructed endpoint points at a slot the server never registered, and `EndpointMap::get` returns `nullptr`.
- **Global (the real contract).** The offset of each stream must be stable across all compatible binaries, per the section above.

From these, the only safe ways to change an interface are:

1. **Append new streams at the end.** Existing offsets are untouched, so an older client built against the previous layout keeps resolving every stream it knows about.
2. **When removing a stream within a compatible protocol version, leave a placeholder in its slot** so every successor keeps its offset. The retained `legacyGetConsistentReadVersion` field in [`CommitProxyInterface.h`](https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/CommitProxyInterface.h) is an example — a typed-but-unused `RequestStream` is enough to hold the slot. Removing a stream and letting successors shift down is a *silent* wire break for any compatible client still reconstructing the old layout.
3. **Repack or compact offsets only across an incompatible protocol-version change.** A new *major* version bumps the protocol version incompatibly, so builds on either side of the boundary refuse to connect rather than mis-route. A major version change is therefore exactly when it is safe to drop accumulated placeholders and renumber an interface's endpoints densely (as long as the `serialize()` offsets and `initEndpoints()` order are renumbered together — the local invariant still applies). Doing the same *within* a compatible protocol version — for example, a patch release — is a silent wire break.

A violation of this contract fails quietly. A send to a token the receiver never registered elicits a `WLTOKEN_ENDPOINT_NOT_FOUND` reply (surfacing as an `EndpointNotFound` trace event), but fire-and-forget sends (`RequestStream::send`) observe no application-level error, so the request is simply dropped. Guarding the layout is therefore best done proactively: a unit test that, for each interface, round-trips through `serialize`/`initEndpoints` and asserts every reconstructed stream resolves to a registered receiver catches the local invariant immediately, and pinning each stream's offset across *compatible* protocol versions catches a removal that forgets to reserve a placeholder (such a test is expected to be updated when a major version bump intentionally renumbers).

---

## Wire Protocol

### Packet Format

```
┌─────────────────┬────────────┬───────────────┬──────────────┐
│ Packet Length    │ Checksum   │ Token (UID)   │ Message Data │
│ (4 bytes)       │ (8 bytes)* │ (16 bytes)    │ (variable)   │
└─────────────────┴────────────┴───────────────┴──────────────┘
* Checksum only for non-TLS connections (XXH3_64bits)
```

### ConnectPacket (sent at connection start)

```cpp
struct ConnectPacket {
    uint32_t connectPacketLength;
    ProtocolVersion protocolVersion;
    uint16_t canonicalRemotePort;
    uint64_t connectionId;           // multi-version client ID
    uint32_t canonicalRemoteIp4;     // or 0 for IPv6
    uint16_t flags;                  // FLAG_IPV6
    uint8_t canonicalRemoteIp6[16];
};
```

---

## Failure Monitoring -- [`FailureMonitor.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FailureMonitor.h)

Tracks endpoint/process availability:

```
struct FailureStatus { bool failed; };

class SimpleFailureMonitor : public IFailureMonitor {
    std::unordered_map<NetworkAddress, FailureStatus> addressStatus;
    YieldedAsyncMap<Endpoint, bool> endpointKnownFailed;
    AsyncMap<NetworkAddress, bool> disconnectTriggers;
};
```

**Key methods:**
- `getState(endpoint)` -- current failure status
- `onStateChanged(endpoint)` -- future that fires on status change
- `onDisconnectOrFailure(endpoint)` -- wait for either event
- `onlyEndpointFailed(endpoint)` -- endpoint failed but address still up
- `setStatus(address, status)` -- update status

**Detection mechanism** (`connectionMonitor()` actor):
1. Sends periodic ping to `WLTOKEN_PING_PACKET`
2. Waits for reply with `CONNECTION_MONITOR_TIMEOUT` (2s default; 1.5s in simulation)
3. On timeout: increment count, eventually throw `connection_failed()`
4. Records ping latency in `DDSketch` histogram
5. After `FAILURE_DETECTION_DELAY` (4s): mark address as failed

---

## Locality & Replication -- [`Locality.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/Locality.h), [`ReplicationPolicy.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/ReplicationPolicy.h)

### LocalityData

Key-value map of location attributes for each process:

| Key | Example | Purpose |
|-----|---------|---------|
| `keyProcessId` | UUID | Unique process identifier |
| `keyZoneId` | "zone-a" | Failure zone |
| `keyMachineId` | "machine-1" | Physical machine |
| `keyDcId` | "dc-east" | Datacenter |
| `keyDataHallId` | "hall-1" | Data hall within DC |

### ProcessClass ([`Locality.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/Locality.h)`:27-154`)

Classifies process suitability for roles:
- **ClassType**: `StorageClass`, `TransactionClass`, `LogClass`, `CommitProxyClass`, `GrvProxyClass`, `MasterClass`, `ResolverClass`, etc.
- **Fitness**: `BestFit` > `GoodFit` > `UnsetFit` > `OkayFit` > `WorstFit` > `ExcludeFit` > `NeverAssign`

### Replication Policies ([`ReplicationPolicy.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/ReplicationPolicy.h))

Composable constraint system:
- **PolicyOne** -- select 1 replica anywhere
- **PolicyAcross(count, attribute, subpolicy)** -- "N replicas across distinct values of attribute"
  - Example: `PolicyAcross(3, "dcId", PolicyOne())` = 3 datacenters
- **PolicyAnd(policies...)** -- intersection of multiple policies

Used by team building (Data Distribution) to ensure failure domain separation.

### Load Balance Distance

```
enum LBDistance { SAME_MACHINE=0, SAME_DC=1, DISTANT=2 };
```

Computed from locality attributes; used for preferring nearby replicas.

---

## Sim2 -- Deterministic Simulator -- [`sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp), [`simulator.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/simulator.h)

Replaces Net2 during testing. Runs the entire cluster in a single OS thread with virtual time.

### Key Properties

- **Deterministic**: Given same random seed → identical execution
- **Virtual time**: No wall-clock dependency; time advances via task scheduling
- **Multi-process**: Many virtual processes in one real thread via `ProcessInfo`
- **Fault injection**: Kill processes, machines, zones, datacenters; inject network clogging, disconnection, disk errors

### ProcessInfo (`SimulatorProcessInfo.h:42-170`)

State per simulated process:
- `name`, `dataFolder`, `coordinationFolder`
- `machine` -- `MachineInfo*` for shared machine state
- `addresses` -- virtual network addresses
- `locality` -- zone/machine/DC placement
- `failed`, `excluded`, `rebooting`, `failedDisk` -- fault state
- `fault_injection_p1`, `fault_injection_p2` -- injection probabilities
- `globals` -- per-process global variables
- `shutdownSignal` -- Promise<KillType> for clean shutdown

### SimClogging ([`sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp)`:198-297`)

Simulates network impairment:
- `getSendDelay()` / `getRecvDelay()` -- adds latency between addresses
- `clogPairFor(addr1, addr2, seconds)` -- delay packets between pair
- `disconnectPairFor(addr1, addr2, seconds)` -- sever connection between pair
- Latency model: 99.9% fast (`FAST_NETWORK_LATENCY`), 0.1% slow (`SLOW_NETWORK_LATENCY`)

### Kill Operations

| Method | Scope | Effect |
|--------|-------|--------|
| `killProcess(pid, kt)` | Single process | Fail/fault/reboot process |
| `killMachine(machineId, kt)` | All processes on machine | Checks replication safety first |
| `killZone(zoneId, kt)` | All machines in zone | |
| `killDataCenter(dcId, kt)` | All machines in DC | |

**KillTypes**: `KillInstantly`, `InjectFaults`, `FailDisk`, `Reboot`, `RebootProcess`, `RebootAndDelete`, `RebootProcessAndDelete`

---

## Load Balancing -- `LoadBalance.actor.h`

Client-side request distribution:

- **QueueModel**: Estimates response latency based on pending requests per server
- **Round-robin**: Distributes across replicas
- **Failure routing**: Routes around failed/degraded servers
- **Locality-aware**: Prefers geographically proximate servers (SAME_MACHINE > SAME_DC > DISTANT)
- **TSS comparison**: Can shadow requests to Testing Storage Servers for correctness validation

---

## Key Constants

| Knob | Default | Purpose |
|------|---------|---------|
| `INITIAL_RECONNECTION_TIME` | 0.05s | First reconnect delay |
| `MAX_RECONNECTION_TIME` | 0.5s | Max backoff |
| `RECONNECTION_TIME_GROWTH_RATE` | 1.2x | Backoff multiplier |
| `CONNECTION_MONITOR_TIMEOUT` | 2s (1.5s in sim) | Ping timeout |
| `FAILURE_DETECTION_DELAY` | 4s | Before marking failed |
| `PACKET_LIMIT` | 100MB | Max packet size |
| `MIN_COALESCE_DELAY` | 10µs | Min buffer wait |
| `MAX_COALESCE_DELAY` | 20µs | Max buffer wait |

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbrpc/include/fdbrpc/fdbrpc.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/fdbrpc.h) | RequestStream, ReplyPromise, NetSAV, request/reply pattern |
| [`fdbrpc/include/fdbrpc/FlowTransport.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FlowTransport.h) | Endpoint, Peer, FlowTransport API |
| [`fdbrpc/FlowTransport.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/FlowTransport.cpp) | Transport implementation, EndpointMap, connection actors |
| [`fdbrpc/include/fdbrpc/FailureMonitor.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/FailureMonitor.h) | Failure detection interface and implementation |
| [`fdbrpc/include/fdbrpc/Locality.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/Locality.h) | LocalityData, ProcessClass, LBDistance |
| [`fdbrpc/include/fdbrpc/ReplicationPolicy.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/ReplicationPolicy.h) | PolicyOne, PolicyAcross, PolicyAnd |
| `fdbrpc/include/fdbrpc/LoadBalance.actor.h` | Client-side load balancing |
| [`fdbrpc/sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp) | Sim2 deterministic simulator |
| [`fdbrpc/include/fdbrpc/simulator.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/simulator.h) | ISimulator interface, ProcessInfo |
| `flow/include/flow/IConnection.h` | IConnection, IListener interfaces |
