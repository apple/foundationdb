# Subsystem 10: Rate Keeping & Throttling

**[Diagrams](diagram_10_ratekeeper.md)**

**Location:** [`fdbserver/ratekeeper/`](https://github.com/apple/foundationdb/tree/main/fdbserver/ratekeeper)
**Size:** ~4K  
**Role:** Back-pressure on commits and reads, tag-based throttling, throughput tracking.

---

## Overview

The Ratekeeper is a singleton role (recruited by Cluster Controller) that monitors cluster health metrics and calculates transaction rate limits. These limits are sent to GRV proxies, which enforce them by delaying `GetReadVersion` responses. This prevents the cluster from being overwhelmed when storage servers or TLogs fall behind.

---

## Ratekeeper Structure -- [`Ratekeeper.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/Ratekeeper.h)`:129-232`

```
class Ratekeeper {
    Map<UID, StorageQueueInfo> storageQueueInfo;     // per-SS metrics
    Map<UID, TLogQueueInfo> tlogQueueInfo;           // per-TLog metrics
    std::map<UID, GrvProxyInfo> grvProxyInfo;
    
    Smoother smoothReleasedTransactions;              // actual TPS
    Smoother smoothBatchReleasedTransactions;
    Smoother smoothTotalDurableBytes;
    
    std::unique_ptr<ITagThrottler> tagThrottler;
    
    RatekeeperLimits normalLimits;                    // default priority limits
    RatekeeperLimits batchLimits;                     // batch priority limits
};
```

### StorageQueueInfo (lines 37-73)

```
class StorageQueueInfo {
    Smoother smoothFreeSpace, smoothTotalSpace;
    Smoother smoothDurableBytes, smoothInputBytes;
    Smoother smoothDurableVersion, smoothLatestVersion;
    StorageQueuingMetricsReply lastReply;
    limitReason_t limitReason;
    std::vector<BusyTagInfo> busiestReadTags, busiestWriteTags;
};
```

### TLogQueueInfo (lines 75-94)

```
class TLogQueueInfo {
    Smoother smoothDurableBytes, smoothInputBytes;
    Smoother smoothFreeSpace, smoothTotalSpace;
    TLogQueuingMetricsReply lastReply;
};
```

### RatekeeperLimits (lines 96-127)

```
struct RatekeeperLimits {
    double tpsLimit;
    int64_t storageTargetBytes, storageSpringBytes;
    int64_t logTargetBytes, logSpringBytes;
    double maxVersionDifference;
    int64_t durabilityLagTargetVersions;
    double durabilityLagLimit;
    TransactionPriority priority;
};
```

---

## Rate Calculation -- [`Ratekeeper.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/Ratekeeper.cpp)`:642-850`

### `updateRate(RatekeeperLimits* limits)`

Evaluates multiple metrics and takes the minimum rate:

### 1. Storage Queue Depth

```
storageQueue = ss.getStorageQueueBytes()
```

If queue between `springBytes` and `targetBytes`:
```
targetRateRatio = (queue - target + spring) / spring
tpsLimit = min(actualTps * maxBytesPerSecond / inputRate,
               maxBytesPerSecond * MAX_TRANSACTIONS_PER_BYTE)
```

### 2. Durability Lag

```
storageDurabilityLag = ss.getDurabilityLag()  // version gap between latest and durable
```

If lag exceeds `durabilityLagTargetVersions`: reduce rate proportionally.

### 3. Free Space

- Minimum free space enforcement with ratio factor
- Hard limit: `STORAGE_HARD_LIMIT_BYTES`
- Below hard limit: rate drops to 0

### 4. TLog Queue Depth

Similar to storage queue depth but for TLog queues:
- `logTargetBytes` / `logSpringBytes` thresholds
- Limits rate when TLogs fall behind

### 5. Write Bandwidth MVCC

```
maxBytesPerSecond = (targetBytes - springBytes) / mvcc_window
```

Prevents storage server from filling its MVCC queue faster than it can drain.

### Rate Selection

The final rate is the **minimum** across all metrics, with the `limitReason` tracking which factor is most constraining.

---

## Communication with GRV Proxies -- [`Ratekeeper.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/Ratekeeper.cpp)`:345-410`

### GetRateInfoRequest

```
struct GetRateInfoRequest {
    UID requesterID;
    int64_t totalReleasedTransactions;
    int64_t batchReleasedTransactions;
    Version version;
    TransactionTagMap<uint64_t> throttledTagCounts;
    bool detailed;
};
```

### GetRateInfoReply

```
struct GetRateInfoReply {
    double transactionRate;              // normal priority TPS limit
    double batchTransactionRate;         // batch priority TPS limit
    double leaseDuration;               // METRIC_UPDATE_RATE
    HealthMetrics healthMetrics;
    Optional<PrioritizedTransactionTagMap<ClientTagThrottleLimits>> clientThrottledTags;
    Optional<TransactionTagMap<double>> proxyThrottledTags;
};
```

### Flow

1. GRV proxy sends `GetRateInfoRequest` periodically
2. Ratekeeper updates released transaction counts
3. Calculates per-proxy limits (total limit / num proxies)
4. Includes per-tag throttle info if tags changed
5. Returns rate limits + health metrics

GRV proxy enforces limits by delaying `GetReadVersion` responses when rate exceeded.

---

## Tag Throttling -- [`GlobalTagThrottler.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/GlobalTagThrottler.cpp)

Per-tag (per-tenant/workload) transaction throttling.

### Per-Tag Statistics (lines 117-169)

```
class PerTagStatistics {
    Optional<ThrottleApi::TagQuotaValue> quota;  // reserved + total quotas
    HoltLinearSmoother transactionCounter;
    Smoother perClientRate;
    Smoother targetRate;
};
```

### Concepts

| Term | Meaning |
|------|---------|
| **Cost** | Bytes accessed per operation |
| **Total Quota** | Max cost/second cluster-wide for tag |
| **Reserved Quota** | Guaranteed cost/second (no throttling within) |
| **Desired TPS** | Transactions/sec assuming total quota |
| **Limiting TPS** | Based on storage server health |
| **Target TPS** | `max(reservedTps, min(desiredTps, limitingTps))` |

### Key Functions

- `getAverageTransactionCost()` -- estimate cost per transaction
- `getLimitingThrottlingRatio()` -- per-zone health factors
- `getTargetTps()` -- calculate target TPS for tag
- `getProxyRates()` -- per-proxy throttle limits for each tag

### Throttle Enforcement

1. Ratekeeper calculates per-tag rate limits
2. Sends to GRV proxies in `GetRateInfoReply.clientThrottledTags`
3. GRV proxies apply tag-specific delays to tagged transactions
4. Clients see `tag_throttled` error if rate exceeded

---

## Server Throughput Tracking -- [`ServerThroughputTracker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/ServerThroughputTracker.cpp)

Tracks per-storage-server throughput for load-aware throttling:
- Read IOPS and bandwidth per server
- Write IOPS and bandwidth per server
- Used by tag throttler to compute limiting ratios

---

## Principal Files

| File | Purpose |
|------|---------|
| [`fdbserver/ratekeeper/Ratekeeper.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/Ratekeeper.cpp) | Rate calculation, proxy communication |
| [`fdbserver/ratekeeper/Ratekeeper.h`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/Ratekeeper.h) | Ratekeeper, StorageQueueInfo, TLogQueueInfo, RatekeeperLimits |
| [`fdbserver/ratekeeper/GlobalTagThrottler.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/GlobalTagThrottler.cpp) | Per-tag throttling implementation |
| [`fdbserver/ratekeeper/RkTagThrottleCollection.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/RkTagThrottleCollection.cpp) | Tag throttle collection management |
| [`fdbserver/ratekeeper/ServerThroughputTracker.cpp`](https://github.com/apple/foundationdb/blob/main/fdbserver/ratekeeper/ServerThroughputTracker.cpp) | Per-server throughput monitoring |
| `fdbserver/ratekeeper/RatekeeperLimits.cpp` | Limit calculation utilities |
