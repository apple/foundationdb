# Hot Shard Management in FoundationDB

## Table of Contents

- [Overview](#overview)
- [What is a Hot Shard?](#what-is-a-hot-shard)
- [Normal Internal Shard Splitting Process](#normal-internal-shard-splitting-process)
- [Hot Shard Special Case](#hot-shard-special-case)
- [When a Hot Shard Occurs That Cannot Be Split](#when-a-hot-shard-occurs-that-cannot-be-split)
  - [Storage Server Impact](#storage-server-impact)
  - [Cluster-Wide Rate Limiting](#cluster-wide-rate-limiting)
- [Mitigation](#mitigation)
  - [Avoiding Hot Shards](#avoiding-hot-shards)
  - [Avoiding Read Hotspots](#avoiding-read-hotspots)
  - [Schema & Keyspace Design Patterns](#schema--keyspace-design-patterns)
  - [What to do when encountered in production](#what-to-do-when-encountered-in-production)
  - [Optional Hot Shard Throttling (Targeted Mitigation)](#optional-hot-shard-throttling-targeted-mitigation)
  - [Key Insight](#key-insight)
- [Server Knobs Related to Hot Shards](#server-knobs-related-to-hot-shards)
  - [Hot Shard Throttling Knobs](#hot-shard-throttling-knobs)
  - [Shard Bandwidth Splitting Knobs](#shard-bandwidth-splitting-knobs)
  - [Read Hot Shard Detection Knobs](#read-hot-shard-detection-knobs)
  - [Storage Queue Rebalancing Knobs](#storage-queue-rebalancing-knobs)
  - [Read Rebalancing Knobs](#read-rebalancing-knobs)
  - [Shard Split/Merge Priority Knobs](#shard-splitmerge-priority-knobs)
  - [Other Hot Shard Related Knobs](#other-hot-shard-related-knobs)
  - [Key Relationships](#key-relationships)
- [References](#references)

## Overview

FoundationDB partitions the keyspace into shards (contiguous key ranges) and assigns each shard to a storage-team replica set.

Shards are just key ranges; their size adapts continuously as Data Distribution monitors load and splits or merges ranges to stay within configured bounds [1][2]. Splits are triggered when metrics like bytes, read/write bandwidth, or hot-shard detection exceed thresholds (splitMetrics.bytes = shardBounds.max.bytes / 2, splitMetrics.bytesWrittenPerKSecond etc.). In other words, shard size is workload driven rather than fixed [1][3]. Storage servers and commit proxies provide ongoing measurements so DD can resize ranges dynamically to relieve load hotspots and maintain balance [4][5].

## What is a Hot Shard?

A shard is "hot" when it absorbs a disproportionate share of the cluster's read or write workload, driving CPU or bandwidth saturation on its replica servers [6].

Storage servers continually sample bytes-read, ops-read, and shard sizes; a shard whose read bandwidth density exceeds configured thresholds is tagged as read-hot so the distributor can identify the offending range [4].

## Normal Internal Shard Splitting Process

- The data distributor decides a shard should shrink and calls splitStorageMetrics, which in turn asks each storage server in the shard for split points via SplitMetricsRequest; the tracker sets targets like "half of the max shard size" and minimum bytes/throughput [1].

- The storage server handles that request in StorageServerMetrics::splitMetrics, pulling the live samples it already maintains for bytes, write bandwidth, and IOs (byteSample, bytesWriteSample, iopsSample) to compute balanced cut points [7][8].

- Inside splitMetrics, the helper getSplitKey converts the desired metric offset into an actual key using the sampled histogram, adds jitter so repeated splits don't choose exactly the same boundary, and enforces MIN_SHARD_BYTES plus optional write-traffic thresholds before accepting the split [8][9].

- It loops until the remaining range falls under all limits, emitting each chosen key into the reply; the tracker then uses those keys to call executeShardSplit, which updates the shard map and kicks off the relocation [10][11].

## Hot Shard Special Case

StorageServerMetrics::splitMetrics only emits a split point when it can carve the shard into two chunks that both satisfy the size/traffic limits—specifically the loop exits if remaining.bytes < 2 * MIN_SHARD_BYTES or the write bandwidth is below SHARD_SPLIT_BYTES_PER_KSEC, and getSplitKey() will return the shard end if the byte/IO samples can't find an interior key that meets the target [8][9].

Hot shards are often tiny (say, a single hot key), so they're already near the minimum size or lack sample resolution; splitting them would just violate those constraints and leave the hot key on the same team anyway.

## When a Hot Shard Occurs That Cannot Be Split

When a hot shard cannot be split (due to minimum size constraints or lack of sample resolution), the cluster experiences cascading effects:

### Storage Server Impact

- The storage servers hosting the hot shard become saturated with write traffic, causing their write queues to grow [13][14].

- Storage server metrics report high `bytesInput` and increasing queue depths, which are monitored by Ratekeeper [13].

- If the storage server's write queue size exceeds thresholds relative to available space and target ratios, it becomes the bottleneck for the entire cluster [14].

### Cluster-Wide Rate Limiting

- Ratekeeper continuously monitors storage server queues and bandwidth metrics to determine cluster-wide transaction rate limits [13][14].

- When a storage server's write queue becomes excessive, Ratekeeper sets the limit reason to `storage_server_write_queue_size` and reduces the cluster-wide transaction rate proportionally [14][15].

- This causes **all transactions across the cluster** to be throttled, even those not touching the hot shard, because the cluster cannot commit faster than its slowest storage server can durably persist data.

- The rate limit is calculated as: `limitTps = min(actualTps * maxBytesPerSecond / inputRate, maxBytesPerSecond * MAX_TRANSACTIONS_PER_BYTE)`, ensuring the cluster doesn't overwhelm the saturated storage server [13].

## Mitigation

### Avoiding Hot Shards

- Randomize key prefixes so consecutive writes land on different shard ranges; for example, hash user IDs or add a short random salt before the natural key. This way inserts will scatter instead of piling onto one shard.
- If you need to store a counter, consider sharding them across N disjoint keys (e.g., counter/<bucket>/…) and aggregate in clients or background jobs; this keeps the per-key mutation rate below the commit proxy’s hot-shard throttle.
- If you are storing append-only logs,  split them into multiple partitions (such as log/<partition>/<ts>), rotating partitions over time rather than funneling through a single key path.
- Avoid “read-modify-write” cycles. Use FDB's atomic operations (like ADD) when possible, and throttle/queue work in clients so they don’t stampede on that hot key.
- For multi-tenant schemas, assign each tenant a disjoint key subspace.

### Avoiding Read Hotspots

  - Cache hot objects (in the application or via a memory service) instead of repeatedly issuing get for the same key every transaction; read-hot shards
  often stem from the same value being fetched continuously.
  - For range scans, fetch only the window you truly need—store derived indexes or “chunked” materializations so transactions avoid reading a large
  contiguous range every time.
  - If one small document is hammered by many readers, replicate it into multiple keys (e.g., per region or per hash bucket) and route readers to
  different replicas based on a deterministic hash; background jobs can keep replicas in sync.
  - Stagger periodic polling across clients—add random jitter so metrics or heartbeat jobs don’t all read the same shard simultaneously.
  - Watch readBandwidth metrics and proactively split large ranges (via splitStorageMetrics) if access skew is predictable (e.g., time-series keys); pre-
  splitting keeps each shard small enough that DD can relocate it quickly.

### Schema & Keyspace Design Patterns

  - Choose the subspace layout so high-cardinality data (per user, per session, per order) groups together while low-cardinality shared data fans out. A
  common pattern is subspace.pack([prefix, hash(id), id, ...]), where the hash spreads load and the trailing id preserves locality for range scans within
  a user.
  - Reserve dedicated key ranges for “global” data that’s touched by admins or background coordinators; keep those ranges tiny and infrequently written so
  they never become hot.
  - For time-ordered data, interleave a bucket prefix (hour, minute) or a modulo partition to avoid writing the latest timestamp into a single ever-
  growing shard; readers can consult a manifest that lists active buckets.
  - If you must maintain a monotonic index (e.g., queue), combine it with a random shard ID so each enqueue writes to queue/<shard>/<increasing counter>;
  consumers merge shards client-side.

### What to do when encountered in production

TBD

### Optional Hot Shard Throttling (Targeted Mitigation)

**Note**: This feature is **experimental** and disabled by default, guarded by the `HOT_SHARD_THROTTLING_ENABLED` server knob [16].

- When enabled, write-hot shards are tracked by the commit proxies, which maintain a hot-shard table and reject incoming mutations against those ranges with the `transaction_throttled_hot_shard` error to keep them from overwhelming a single team [5].

- This targeted throttling attempts to throttle only transactions writing to the hot shard, rather than penalizing the entire cluster with global rate limits [5].

- Once the data distributor flags a hot shard, it can split the range into smaller pieces and/or relocate portions to other teams to spread the traffic while respecting health constraints and load targets [12].

### Key Insight

The fundamental problem with unsplittable hot shards is that they create a **single point of contention** that forces cluster-wide rate limiting. Even with targeted hot shard throttling enabled, the storage server write queues will still grow if the hot key receives sustained traffic, eventually triggering global rate limits that affect all clients.

---

## Server Knobs Related to Hot Shards

FoundationDB uses several server knobs to control hot shard detection, splitting, and throttling behavior.

### Hot Shard Throttling Knobs

These knobs control the experimental hot shard throttling feature (PR #10970):

**`HOT_SHARD_THROTTLING_ENABLED`** (bool, default: `false`)
- Master switch to enable/disable hot shard throttling at commit proxies
- When enabled, commit proxies track hot shards and reject transactions writing to them
- Disabled by default as the feature is experimental
- Location: fdbclient/ServerKnobs.cpp:999

**`HOT_SHARD_THROTTLING_EXPIRE_AFTER`** (double, default: `3.0` seconds)
- Duration after which a throttled hot shard expires and is removed from the throttle list
- Prevents indefinite throttling if load decreases
- Location: fdbclient/ServerKnobs.cpp:1000

**`HOT_SHARD_THROTTLING_TRACKED`** (int64_t, default: `1`)
- Maximum number of hot shards to track and throttle per storage server
- Limits the size of the hot shard list to prevent excessive memory usage
- Location: fdbclient/ServerKnobs.cpp:1001

**`HOT_SHARD_MONITOR_FREQUENCY`** (double, default: `5.0` seconds)
- How often Ratekeeper queries storage servers for hot shard information
- Lower values provide faster hot shard detection but increase RPC overhead
- Location: fdbclient/ServerKnobs.cpp:1002

### Shard Bandwidth Splitting Knobs

These knobs control when Data Distribution splits shards based on write bandwidth:

**`SHARD_MAX_BYTES_PER_KSEC`** (int64_t, default: `1,000,000,000` bytes/ksec = 1 GB/sec)
- Shards with write bandwidth exceeding this threshold are split immediately
- For a large shard (e.g., 100MB), it will be split into multiple pieces
- If set too low, causes excessive data movement for small bandwidth spikes
- If set too high, workload can remain concentrated on a single team indefinitely
- Location: fdbclient/ServerKnobs.cpp:244

**`SHARD_MIN_BYTES_PER_KSEC`** (int64_t, default: `100,000,000` bytes/ksec = 100 MB/sec)
- Shards with write bandwidth above this threshold will not be merged
- Must be significantly less than `SHARD_MAX_BYTES_PER_KSEC` to avoid merge/split cycles
- Controls the number of extra shards: max extra shards ≈ (total cluster bandwidth) / `SHARD_MIN_BYTES_PER_KSEC`
- Location: fdbclient/ServerKnobs.cpp:255

**`SHARD_SPLIT_BYTES_PER_KSEC`** (int64_t, default: `250,000,000` bytes/ksec = 250 MB/sec)
- When splitting a hot shard, each resulting piece will have less than this bandwidth
- Must be less than half of `SHARD_MAX_BYTES_PER_KSEC`
- Smaller values split hot shards into more pieces, distributing load faster across more servers
- If too small relative to `SHARD_MIN_BYTES_PER_KSEC`, generates immediate re-merging work
- Location: fdbclient/ServerKnobs.cpp:269

### Read Hot Shard Detection Knobs

These knobs control read-hot shard detection (primarily for read load balancing):

**`SHARD_MAX_READ_OPS_PER_KSEC`** (int64_t, default: `45,000,000` ops/ksec = 45k ops/sec)
- Read operation rate above which a shard is considered read-hot
- Assumption: at 45k reads/sec, a storage server becomes CPU-saturated
- Location: fdbclient/ServerKnobs.cpp:224

**`SHARD_READ_OPS_CHANGE_THRESHOLD`** (int64_t, default: `SHARD_MAX_READ_OPS_PER_KSEC / 4`)
- When sampled read ops change by more than this amount, shard metrics update immediately
- Enables faster response to sudden changes in read workload
- Location: fdbclient/ServerKnobs.cpp:225

**`READ_SAMPLING_ENABLED`** (bool, default: `false`)
- Master switch to enable/disable read sampling on storage servers
- When enabled, storage servers sample read operations to detect hot ranges
- Location: fdbclient/ServerKnobs.cpp:1018

**`ENABLE_WRITE_BASED_SHARD_SPLIT`** (bool)
- Controls whether write traffic (in addition to read traffic) can trigger shard splits
- When enabled, high write bandwidth marks shards as hot for splitting
- Location: fdbclient/include/fdbclient/ServerKnobs.h:260

**`SHARD_MAX_READ_DENSITY_RATIO`** (double)
- Maximum read density ratio before a shard is considered hot
- Used to detect hotspots based on read concentration rather than absolute rate
- Location: fdbclient/include/fdbclient/ServerKnobs.h:263

**`SHARD_READ_HOT_BANDWIDTH_MIN_PER_KSECONDS`** (int64_t)
- Minimum read bandwidth threshold (in bytes/ksec) for a shard to be marked read-hot
- Prevents low-traffic shards from being flagged as hot due to ratio calculations
- Location: fdbclient/include/fdbclient/ServerKnobs.h:264

**`SHARD_MAX_BYTES_READ_PER_KSEC_JITTER`** (int64_t)
- Jitter applied to read hot shard detection to avoid synchronization
- Adds randomness to prevent all shards from being evaluated simultaneously
- Location: fdbclient/include/fdbclient/ServerKnobs.h:265

### Storage Queue Rebalancing Knobs

These knobs control automatic shard movement when storage queues become unbalanced:

**`REBALANCE_STORAGE_QUEUE_SHARD_PER_KSEC_MIN`** (int64_t, default: `SHARD_MIN_BYTES_PER_KSEC`)
- Minimum write bandwidth for a shard to be considered for storage queue rebalancing
- Prevents moving tiny/idle shards during rebalancing
- Location: fdbclient/ServerKnobs.cpp:368

**`DD_ENABLE_REBALANCE_STORAGE_QUEUE_WITH_LIGHT_WRITE_SHARD`** (bool, default: `true`)
- Allows moving light-traffic shards out of overloaded storage servers
- Helps reduce queue buildup by redistributing low-write shards
- Location: fdbclient/ServerKnobs.cpp:369

### Read Rebalancing Knobs

These knobs control Data Distribution's ability to move shards based on read hotspots:

**`READ_REBALANCE_SHARD_TOPK`** (int)
- Number of top read-hot shards to consider for rebalancing moves
- Limits how many hot shards DD tracks simultaneously for read-aware relocations
- Location: fdbclient/include/fdbclient/ServerKnobs.h:242

**`READ_REBALANCE_MAX_SHARD_FRAC`** (double)
- Maximum fraction of the read imbalance gap that a moved shard can represent
- Prevents moving excessively large shards during read rebalancing
- Location: fdbclient/include/fdbclient/ServerKnobs.h:245

### Shard Split/Merge Priority Knobs

These knobs control the priority of different Data Distribution operations:

**`PRIORITY_SPLIT_SHARD`** (int)
- Priority level for shard split operations in DD's work queue
- Higher priority means splits happen before lower-priority operations
- Location: fdbclient/include/fdbclient/ServerKnobs.h:193

**`PRIORITY_MERGE_SHARD`** (int)
- Priority level for shard merge operations in DD's work queue
- Typically lower than split priority since merges are less urgent
- Location: fdbclient/include/fdbclient/ServerKnobs.h:177

### Other Hot Shard Related Knobs

**`WAIT_METRICS_WRONG_SHARD_CHANCE`** (double)
- Probability of injecting a delay when a storage server reports wrong_shard error
- Prevents rapid retry loops when shard location information is stale
- Location: fdbclient/include/fdbclient/ServerKnobs.h:1124

**`WRONG_SHARD_SERVER_DELAY`** (double, client-side)
- Backoff delay on client when receiving a wrong_shard_server error
- Prevents tight retry loops when client's location cache is stale
- Location: fdbclient/include/fdbclient/ClientKnobs.h:57

### Key Relationships

```
SHARD_MIN_BYTES_PER_KSEC (100 MB/s)
    ↓  (must be significantly less than)
SHARD_SPLIT_BYTES_PER_KSEC (250 MB/s)
    ↓  (must be less than half of)
SHARD_MAX_BYTES_PER_KSEC (1 GB/s)
```

When a shard's write bandwidth exceeds `SHARD_MAX_BYTES_PER_KSEC`, it is split into pieces each with bandwidth < `SHARD_SPLIT_BYTES_PER_KSEC`. Shards with bandwidth > `SHARD_MIN_BYTES_PER_KSEC` will not be merged back together.

**Read Hot Shard Detection Flow:**
1. If `READ_SAMPLING_ENABLED`, storage servers sample read operations
2. When reads exceed `SHARD_MAX_READ_OPS_PER_KSEC` or read density exceeds `SHARD_MAX_READ_DENSITY_RATIO`
3. And read bandwidth is above `SHARD_READ_HOT_BANDWIDTH_MIN_PER_KSECONDS`
4. The shard is marked as read-hot
5. DD considers top-K hot shards (`READ_REBALANCE_SHARD_TOPK`) for rebalancing

---

## References

[1] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/DDShardTracker.actor.cpp#L875
[2] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/DDShardTracker.actor.cpp#L884
[3] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/DDShardTracker.actor.cpp#L879
[4] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/StorageMetrics.actor.cpp#L472
[5] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/CommitProxyServer.actor.cpp#L742
[6] https://github.com/apple/foundationdb/blob/7.3.0/design/data-distributor-internals.md#L129
[7] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/storageserver.actor.cpp#L1973
[8] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/StorageMetrics.actor.cpp#L286
[9] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/StorageMetrics.actor.cpp#L302
[10] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/StorageMetrics.actor.cpp#L332
[11] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/DDShardTracker.actor.cpp#L890
[12] https://github.com/apple/foundationdb/blob/7.3.0/design/data-distributor-internals.md#L146
[13] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/Ratekeeper.actor.cpp#L933
[14] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/Ratekeeper.actor.cpp#L973
[15] https://github.com/apple/foundationdb/blob/7.3.0/fdbserver/Ratekeeper.actor.cpp#L1440
[16] https://github.com/apple/foundationdb/blob/7.3.0/fdbclient/ServerKnobs.cpp#L872