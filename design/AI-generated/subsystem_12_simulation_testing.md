# Subsystem 12: Simulation & Testing

**[Diagrams](diagram_12_simulation_testing.md)**

**Location:** [`fdbrpc/sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp), [`fdbserver/workloads/`](https://github.com/apple/foundationdb/tree/main/fdbserver/workloads), `fdbserver/tester/`, `tests/`
**Size:** ~51K workloads + simulator  
**Role:** Deterministic simulation (Sim2), fault injection (Buggify), workload-based integration tests.

---

## Overview

FDB's most distinctive engineering practice. The entire cluster -- multiple processes, network, disk -- runs deterministically in a single OS thread with virtual time. Combined with aggressive fault injection (Buggify), this finds bugs that would take years to manifest in production.

---

## Sim2 -- Deterministic Simulator -- [`fdbrpc/sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp)

### Sim2 Class (line 1025)

```
class Sim2 final : public ISimulator, public INetworkConnections
```

Implements `INetwork` (the event loop interface) and `ISimulator` (fault injection).

### Key Properties

- **Deterministic**: Same random seed → identical execution
- **Virtual time**: No wall-clock dependency; time advances via task scheduling
- **Single-threaded**: All virtual processes share one OS thread
- **Full cluster**: Multiple processes with separate globals, endpoints, listeners

### Virtual Event Loop

- `TaskQueue<PromiseTask> taskQueue` -- priority queue of pending tasks
- Time advances through task completion, not wall-clock
- `delay(seconds, taskId)` enqueues task at `currentTime + seconds`
- `timer()` can be up to 0.1s ahead of `now()` for timer jitter
- Buggified delays added via `FLOW_KNOBS->MAX_BUGGIFIED_DELAY`

---

## ProcessInfo -- `SimulatorProcessInfo.h:42-170`

State per simulated process:

```
struct ProcessInfo : NonCopyable {
    std::string name, dataFolder, coordinationFolder;
    MachineInfo* machine;
    NetworkAddressList addresses;
    LocalityData locality;
    ProcessClass startingClass;
    
    bool failed, excluded, cleared, rebooting, failedDisk;
    double fault_injection_p1, fault_injection_p2;
    
    std::vector<flowGlobalType> globals;     // per-process globals
    INetworkConnections* network;
    UID uid;
    ProtocolVersion protocolVersion;
    std::vector<ProcessInfo*> childs;
    Promise<KillType> shutdownSignal;
    
    bool isReliable();     // no faults injected
    bool isAvailable();    // !excluded && isReliable()
};
```

Each process has its own:
- Global variables (via `globals` vector)
- Endpoint registrations
- Listener map
- Fault injection parameters

---

## Fault Injection

### Kill Types -- `SimulatorKillType.h`

| KillType | Effect |
|----------|--------|
| `KillInstantly` | Immediate failure, process gone |
| `InjectFaults` | Enable probabilistic fault injection |
| `FailDisk` | Simulate disk failure |
| `Reboot` | Machine reboot, keep data |
| `RebootProcess` | Process reboot, keep data |
| `RebootAndDelete` | Machine reboot, delete data |
| `RebootProcessAndDelete` | Process reboot, delete data |
| `RebootProcessAndSwitch` | Reboot with different cluster file |

### Kill Hierarchy

| Method | Scope |
|--------|-------|
| `killProcess(pid, kt)` | Single process |
| `killMachine(machineId, kt)` | All processes on machine |
| `killZone(zoneId, kt)` | All machines in zone |
| `killDataCenter(dcId, kt)` | All machines in datacenter |
| `killDataHall(hallId, kt)` | All machines in data hall |

`killMachine()` checks replication policies before killing -- may upgrade kill type if processes are protected.

### Network Clogging -- `SimClogging` (sim2.cpp:198-297)

```
struct SimClogging {
    std::map<IPAddress, double> clogSendUntil, clogRecvUntil;
    std::map<pair<IP,IP>, double> clogPairUntil, clogPairLatency;
    std::map<pair<IP,IP>, double> disconnectPairUntil;
    
    double getSendDelay(from, to);
    double getRecvDelay(from, to);
    bool disconnected(from, to);
    void clogPairFor(from, to, seconds);
    void disconnectPairFor(from, to, seconds);
};
```

Latency model:
- 99.9% of packets: `MIN_NETWORK_LATENCY + FAST_NETWORK_LATENCY`
- 0.1% tail: `MIN_NETWORK_LATENCY + SLOW_NETWORK_LATENCY`
- Additional per-pair clogging latency
- Disconnection: throws `connection_failed()`

### Disk Fault Injection -- `AsyncFileNonDurable.h`

Simulates unreliable disk writes:

```
class AsyncFileNonDurable {
    Reference<IAsyncFile> file;           // real underlying file
    RangeMap<uint64_t, Future<Void>> pendingModifications;
    enum KillMode { NO_CORRUPTION, DROP_ONLY, FULL_CORRUPTION };
};
```

- Delays writes randomly (up to `NON_DURABLE_MAX_WRITE_DELAY`)
- On `kill()`: drops or corrupts pending writes based on `KillMode`
- `sync()` makes writes durable
- Simulates power failure: un-synced writes are lost

### I/O Error Injection

`simulator_should_inject_fault()` (sim2.cpp) -- called during I/O operations:
- Checks per-process `fault_injection_p1` and `fault_injection_p2`
- Probabilistically returns true → caller throws `io_timeout`, `io_error`, or `platform_error`

---

## Buggify System -- `flow/include/flow/Buggify.h`

Randomly enables rare code paths throughout the codebase.

### Mechanism

```cpp
#define BUGGIFY (getGeneralSBVar(__FILE__, __LINE__) && deterministicRandom()->random01() < P_FIRES)
#define BUGGIFY_WITH_PROB(x) (getGeneralSBVar(__FILE__, __LINE__) && deterministicRandom()->random01() < (x))
```

Two-level randomization:
1. **Activation** (once per code location): `P_BUGGIFIED_SECTION_ACTIVATED = 0.25` -- 25% of BUGGIFY sites are active
2. **Firing** (each execution): `P_BUGGIFIED_SECTION_FIRES = 0.25` -- active sites fire 25% of the time

Decisions cached in `SBVars` map keyed by `__FILE__ + __LINE__` for deterministic replay.

### Variants

| Macro | Scope |
|-------|-------|
| `BUGGIFY` | General server code |
| `BUGGIFY_WITH_PROB(x)` | Custom probability |
| `CLIENT_BUGGIFY` | Client-only code |
| `CLIENT_BUGGIFY_WITH_PROB(x)` | Client custom probability |

### Examples in Codebase

```cpp
if (BUGGIFY) delay(deterministicRandom()->random01());     // random extra delay
if (BUGGIFY) bufferSize = 1;                                // tiny buffer
if (BUGGIFY_WITH_PROB(0.001)) throw io_error();            // rare I/O error
```

---

## CODE_PROBE System -- `flow/include/flow/CodeProbe.h`

Tracks code path coverage during testing:

```cpp
CODE_PROBE(condition, "description of what happened");
```

### Implementation

```
struct ICodeProbe {
    const char* filePath(), line(), comment(), condition();
    bool wasHit() const;
    unsigned hitCount() const;
    void trace(bool condition) const;
};
```

- Static singleton per source location
- Atomic hit counter
- Trace event logged on first hit
- Annotations: `context::Sim2`, `decoration::Rare`, `assert::SimOnly`
- Used to verify rare code paths are exercised during simulation

---

## Workload System -- [`fdbserver/workloads/`](https://github.com/apple/foundationdb/tree/main/fdbserver/workloads)

### TestWorkload Base Class (`workloads.h:65-98`)

```
struct TestWorkload : NonCopyable, WorkloadContext, ReferenceCounted<TestWorkload> {
    int phases;  // SETUP | EXECUTION | CHECK | METRICS
    
    virtual std::string description() const = 0;
    virtual Future<Void> setup(Database const& cx);    // optional
    virtual Future<Void> start(Database const& cx) = 0;  // main logic
    virtual Future<bool> check(Database const& cx) = 0;  // verify results
    virtual Future<std::vector<PerfMetric>> getMetrics();
};
```

### WorkloadContext (lines 38-50)

- `clientId`, `clientCount` -- for distributed coordination
- `sharedRandomNumber` -- coordinated randomness across clients
- Reference to `ServerDBInfo`

### Representative Workloads

| Workload | Purpose |
|----------|---------|
| `ApiCorrectness` | Tests get/set/getRange/clear correctness |
| `ReadWrite` | Mixed read/write load with configurable patterns |
| `ConsistencyCheck` | Validates data consistency across replicas |
| `Cycle` | Tests cluster configuration changes |
| `BackupCorrectness` | Validates backup/restore integrity |
| `BulkLoad` / `BulkDumping` | Tests bulk operations |
| `AtomicOps` | Validates atomic operation correctness |
| `ChangeConfig` | Tests configuration changes under load |

~60+ workload types covering all aspects of the system.

---

## Test Configuration -- `tests/` directory

### TOML Format

```toml
[[knobs]]
enable_replica_consistency_check_on_reads = true

[[test]]
testTitle = 'ApiCorrectnessWithConsistencyCheck'
clearAfterTest = true
timeout = 2100

    [[test.workload]]
    testName = 'ApiCorrectness'
    numKeys = 5000
    numGets = 1000
    numGetRanges = 100
```

### Parameters

- `testTitle` -- test name
- `timeout` -- seconds before test times out
- `clearAfterTest` -- clear database after test
- Multiple `[[test.workload]]` sections for concurrent workloads
- `[[knobs]]` sections for server knob overrides

---

## SimulatedCluster -- [`fdbserver/SimulatedCluster.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/SimulatedCluster.cpp)

### TestConfig (lines 131-429)

```
class TestConfig : public BasicTestConfig {
    int minimumReplication, minimumRegions;
    Optional<int> datacenters, processesPerMachine;
    std::set<SimulationStorageEngine> storageEngineExcludeTypes;
    Optional<bool> generateFearless, buggify, faultInjection;
    bool simHTTPServerEnabled;
    bool injectTargetedSSRestart, injectSSDelay;
};
```

### Cluster Setup

1. Load test configuration from TOML/INI file
2. Create machines with virtual IP addresses
3. Create processes with locality data (zone, machine, DC)
4. Configure replication policies
5. Start workload execution

---

## Test Execution -- `fdbserver/tester/test.cpp`

### `runWorkload()` (lines 67-187)

Four phases:

1. **Setup** -- call `setup()` on each tester, initialize state
2. **Execution** -- call `start()` on each tester, run workload logic
3. **Check** -- call `check()` on each tester, verify results
4. **Metrics** -- call `getMetrics()`, aggregate performance data

### Distributed Coordination

Each tester receives `WorkloadRequest`:
- `clientId` -- unique client identifier
- `clientCount` -- total clients
- `sharedRandomNumber` -- coordinated RNG seed
- Results aggregated via `aggregateMetrics()`

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbrpc/sim2.cpp`](https://github.com/apple/foundationdb/blob/main/fdbrpc/sim2.cpp) | Sim2 deterministic simulator implementation |
| [`fdbrpc/include/fdbrpc/simulator.h`](https://github.com/apple/foundationdb/blob/main/fdbrpc/include/fdbrpc/simulator.h) | ISimulator interface |
| `fdbrpc/include/fdbrpc/SimulatorProcessInfo.h` | ProcessInfo, MachineInfo |
| `fdbrpc/include/fdbrpc/AsyncFileNonDurable.h` | Simulated unreliable disk |
| `flow/include/flow/Buggify.h` | BUGGIFY macros |
| `flow/include/flow/CodeProbe.h` | CODE_PROBE coverage tracking |
| [`fdbserver/SimulatedCluster.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/SimulatedCluster.cpp) | Simulated cluster setup, TestConfig |
| `fdbserver/tester/include/fdbserver/tester/workloads.h` | TestWorkload base class |
| `fdbserver/tester/test.cpp` | Test execution orchestration |
| `fdbserver/workloads/*.cpp` | 60+ workload implementations |
| `tests/**/*.toml` | Test configuration files |
