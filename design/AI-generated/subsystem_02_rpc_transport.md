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

- **Token** = `UID` (pair of `uint64_t`). Lower 32 bits encode an index into the EndpointMap; upper 32 bits encode task priority.
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
6. On failure: exponential backoff (INITIAL_RECONNECTION_TIME to MAX_RECONNECTION_TIME, growth factor 1.5x)
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
- `insert()` -- allocates from free list, encodes priority in upper 32 bits
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
2. Waits for reply with `CONNECTION_MONITOR_TIMEOUT` (1s default)
3. On timeout: increment count, eventually throw `connection_failed()`
4. Records ping latency in `DDSketch` histogram
5. After `FAILURE_DETECTION_DELAY` (5s): mark address as failed

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
| `INITIAL_RECONNECTION_TIME` | 0.2s | First reconnect delay |
| `MAX_RECONNECTION_TIME` | 30s | Max backoff |
| `RECONNECTION_TIME_GROWTH_RATE` | 1.5x | Backoff multiplier |
| `CONNECTION_MONITOR_TIMEOUT` | 1s | Ping timeout |
| `FAILURE_DETECTION_DELAY` | 5s | Before marking failed |
| `PACKET_LIMIT` | 512MB | Max packet size |
| `MIN_COALESCE_DELAY` | 0.5ms | Min buffer wait |
| `MAX_COALESCE_DELAY` | 2ms | Max buffer wait |

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
