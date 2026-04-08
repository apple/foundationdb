# FDB Status: Return Partial Status When DB is Unavailable

## Context

When FoundationDB is down, `fdbcli --exec "status json"` times out entirely because `clusterGetStatus()` takes sequential database transactions that block. Much of the status data (processes, machines, metrics) comes from worker RPCs and could still be returned.

**Target**: PR to upstream [apple/foundationdb](https://github.com/apple/foundationdb) via [hxu/foundationdb](https://github.com/hxu/foundationdb) fork.

## Repo Setup

The upstream fork is cloned at `/Volumes/git/oss-foundationdb` with branch `hxu/async-status`.

```bash
cd /Volumes/git/oss-foundationdb
git checkout hxu/async-status
```

Build directory: `/Volumes/git/oss-foundationdb/cmake-build-debug`

## File to Modify

`fdbserver/clustercontroller/Status.actor.cpp` — the main status compilation file. Entry point is `clusterGetStatus()` at line 2847.

## The Bug

In `clusterGetStatus()`, transaction-dependent fetches are waited on **sequentially**, stacking timeouts:

| Line | Wait | Max Timeout | Needs DB Transaction? |
|------|------|-------------|----------------------|
| 2984 | `wait(waitForAll({recovery, idmpKey, versionEpoch}))` | ~5s each | Yes |
| 3033 | `wait(loadConfiguration(...))` | 5s | Yes |
| 3065 | `wait(latencyProbeFetcher(...))` | 5s × 5 probes = **25s** | Yes |
| 3115 | `wait(store(storageWiggler,...))` | varies | Mixed |
| 3137 | `wait(getAll(futures2))` — layer, locked, data, workload | 3-5s each | Partially |
| 3138 | `wait(success(primaryDCFO))` | 5s | Yes |
| 3141 | `wait(errorOr(timeoutError(resolveHostnames, 5.0)))` | 5s | No |
| 3246 | `wait(waitForAll(warningFutures))` | 5s each | Yes |
| 3341 | `wait(timeoutError(consistencyScanInfo, 2.0))` | 2s | Yes |

**Worst case sequential sum: ~52s**. Measured in simulation: **14.87s - 21s** worst latency per status call under attrition + clogging.

## Fix: `waitForAllReady` + graceful degradation

### Change 1: First transaction batch (line 2984)
```cpp
// Before:
wait(waitForAll<JsonBuilderObject>(
    { recoveryStateStatusFuture, idmpKeyStatusFuture, versionEpochStatusFuture }));
state JsonBuilderObject recoveryStateStatus = recoveryStateStatusFuture.get();
state JsonBuilderObject idmpKeyStatus = idmpKeyStatusFuture.get();
state JsonBuilderObject versionEpochStatus = versionEpochStatusFuture.get();

// After:
wait(waitForAllReady(std::vector<Future<JsonBuilderObject>>{
    recoveryStateStatusFuture, idmpKeyStatusFuture, versionEpochStatusFuture }));
state JsonBuilderObject recoveryStateStatus;
if (recoveryStateStatusFuture.isReady() && !recoveryStateStatusFuture.isError()) {
    recoveryStateStatus = recoveryStateStatusFuture.get();
} else {
    status_incomplete_reasons.insert("Unable to fetch recovery state status.");
}
state JsonBuilderObject idmpKeyStatus;
if (idmpKeyStatusFuture.isReady() && !idmpKeyStatusFuture.isError()) {
    idmpKeyStatus = idmpKeyStatusFuture.get();
} else {
    status_incomplete_reasons.insert("Unable to fetch idempotency key status.");
}
state JsonBuilderObject versionEpochStatus;
if (versionEpochStatusFuture.isReady() && !versionEpochStatusFuture.isError()) {
    versionEpochStatus = versionEpochStatusFuture.get();
} else {
    status_incomplete_reasons.insert("Unable to fetch version epoch status.");
}
```

### Change 2: loadConfiguration — already has internal timeout handling
`loadConfiguration()` already handles its own 5s timeout internally (lines 1626-1636, choose/when with `getConfTimeout`) and returns empty Optional on timeout. No change needed — it won't block forever.

### Change 3: latencyProbeFetcher — launch non-blocking (line 3065)
```cpp
// Before (blocking):
JsonBuilderObject latencyProbeResults =
    wait(latencyProbeFetcher(cx, &messages, &status_incomplete_reasons, &isAvailable));

// After: launch as future, collect later
state Future<JsonBuilderObject> latencyProbeFuture =
    latencyProbeFetcher(cx, &messages, &status_incomplete_reasons, &isAvailable);
// ... launch other work concurrently ...
// Later, after futures2 and other work:
wait(ready(latencyProbeFuture));
if (latencyProbeFuture.isReady() && !latencyProbeFuture.isError()) {
    JsonBuilderObject latencyProbeResults = latencyProbeFuture.get();
    statusObj["database_available"] = isAvailable;
    if (!latencyProbeResults.empty()) {
        statusObj["latency_probe"] = latencyProbeResults;
    }
}
```

### Change 4: futures2 batch — use waitForAllReady (line 3137)
```cpp
// Before:
state std::vector<JsonBuilderObject> workerStatuses = wait(getAll(futures2));

// After:
wait(waitForAllReady(futures2));
state std::vector<JsonBuilderObject> workerStatuses;
for (int i = 0; i < futures2.size(); i++) {
    if (futures2[i].isReady() && !futures2[i].isError()) {
        workerStatuses.push_back(futures2[i].get());
    } else {
        workerStatuses.push_back(JsonBuilderObject());
        status_incomplete_reasons.insert("Unable to fetch some status data.");
    }
}
```

### Change 5: Always assemble process/machine status
Ensure `processStatusFetcher()` (line 3256) and `machineStatusFetcher()` (line 3058) run and populate the response **regardless** of whether configuration was loaded. These use pre-fetched worker RPC data and don't need transactions.

## Test Changes (already done)

### StatusWorkload enhancement (`fdbserver/workloads/StatusWorkload.cpp`)

Added `maxStatusLatency` option and `worstLatency` tracking:
- New field: `double maxStatusLatency` — configurable per test (0 = disabled)
- New field: `double worstLatency` — tracks worst sim-time latency across all status calls
- `check()` now fails if `worstLatency > maxStatusLatency` (when threshold > 0)
- `fetcher()` tracks `worstLatency = std::max(worstLatency, latency)` per request
- New metric: "Worst Latency" reported in `getMetrics()`

### New test config (`tests/fast/StatusDuringOutage.toml`)

```toml
[[test]]
testTitle = 'StatusDuringOutage'

    [[test.workload]]
    testName = 'Status'
    testDuration = 60.0
    requestsPerSecond = 0.5
    maxStatusLatency = 10.0

    [[test.workload]]
    testName = 'Attrition'
    machinesToKill = 10
    machinesToLeave = 1
    reboot = false
    testDuration = 30.0

    [[test.workload]]
    testName = 'RandomClogging'
    testDuration = 60.0
    swizzle = 0
    scale = 1.0
    clogginess = 2.0
```

## Verification (Red-Green TDD)

All commands run from `/Volumes/git/oss-foundationdb/cmake-build-debug`.

### Step 1: GREEN — existing test passes (confirmed)
```bash
bin/fdbserver -r simulation -f ../tests/fast/SidebandWithStatus.toml \
  -s 1 -b on --locality-zoneid=test1 --locality-machineid=test1
```
Result: `1 tests passed; 0 tests failed.` (Worst Latency: 21.17s — but no threshold set)

### Step 2: RED — new test fails with unmodified Status.actor.cpp (confirmed)
```bash
bin/fdbserver -r simulation -f ../tests/fast/StatusDuringOutage.toml \
  -s 1 -b on --locality-zoneid=test1 --locality-machineid=test1
```
Result: `0 tests passed; 1 tests failed.` (Worst Latency: **14.87s**, threshold: 10s)

### Step 3: Make changes to `fdbserver/clustercontroller/Status.actor.cpp` (Changes 1-5)

### Step 4: Rebuild
```bash
cd /Volumes/git/oss-foundationdb/cmake-build-debug && ninja -j4 fdbserver
```

### Step 5: GREEN — new test passes after fix
```bash
bin/fdbserver -r simulation -f ../tests/fast/StatusDuringOutage.toml \
  -s 1 -b on --locality-zoneid=test1 --locality-machineid=test1
```
Expected: Worst latency under 10s, test passes.

### Step 6: GREEN — existing test still passes (no regression)
```bash
bin/fdbserver -r simulation -f ../tests/fast/SidebandWithStatus.toml \
  -s 1 -b on --locality-zoneid=test1 --locality-machineid=test1
```
Expected: Still passes, schema validation OK.

## Build Notes

CMake 4.x requires `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` propagated to ExternalProject subbuilds (toml11). The user's build directory is `cmake-build-debug` (IDE-generated).
