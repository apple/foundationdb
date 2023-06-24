# Throughput Quotas

Specific workloads can be throttled independently according to user-specified throughput quotas. This pages explains how to use this feature, and gives an overview of the implementation.

## Glossary

### Throttling IDs

A throttlingId refers to either a tenant group or a transaction tag. Transaction tags are specified via the `AUTO_THROTTLE_TAG` transaction option. Each transaction can specifify 0 or 1 transaction tags and belong to 0 or 1 tenant groups. If a transaction is tagged and belongs to a tenant group, the transaction tag is ignored.

### Quotas
For each throttlingId, a throughput quota can be specified. Throughput quotas contain two components:

* Reserved quota
* Total quota

The quota throttler cannot throttle throttlingIds to a throughput below the reserved quota, and it cannot allow throughput to exceed the total quota.

### Cost
The cost of an operation is the number of bytes accessed, rounded up to the nearest page size. Thus, the cost of a read operation is computed as:

```
readCost = ceiling(bytesRead / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
```

Writes are penalized further with a fungibility ratio (because writing some number of bytes is more expensive than reading the same number of bytes). The cost of a write operation is computed as:

```
writeCost = CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * ceiling(bytesWritten / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
```

Here `bytesWritten` includes cleared bytes. The size of range clears is estimated at commit time.

## API

### Tuple Layer
Per-tag throughput quotas are stored inside of the system keyspace (with prefix `\xff/tagQuota/`). Tenant group quotas are stored in the system keyspace as well, inside of the tenant metadata. They are stored using the tuple layer, in a tuple of form: `(reservedQuota, totalQuota)`. There is currently no custom code in the bindings for manipulating these system keys. However, in any language for which bindings are available, it is possible to use the tuple layer to manipulate throughput quotas.

For an example of how to manipulate tenant group quotas and per-tag quotas through the native API, see the `ThroughputQuotaWorkload` class.

### fdbcli
The easiest way for an external client to interact with per-tag quotas is through `fdbcli`. To get the quota (in bytes/second) of a particular tag, run the following command:

```
fdbcli> quota get <tag> [reserved_throughput|total_throughput]
```

To set the quota through `fdbcli`, run:

```
fdbcli> quota set <tag> [reserved_throughput|total_throughput] <bytes_per_second>
```

To clear a both reserved and total throughput quotas for a tag, run:

```
fdbcli> quota clear <tag>
```

Manipulating tenant group throughput quotas through `fdbcli` is not supported yet.

## Implementation

### Stat collection
The transaction rates and costs of all transactions must be visible to the quota throttler. Whenever a client tags a transaction, sampling is performed to determine whether to attach the transaction's throttlingId to messages sent to storage servers and commit proxies.

For read operations that are sampled (with probability `CLIENT_KNOBS->READ_TAG_SAMPLE_RATE`), read costs are aggregated on storage servers using the `ThrottlingCounter` class. This class tracks the busyness of the top-k throttlingIds affecting the storage server with read load (here `k` is determined by `SERVER_KNOBS->SS_THROTTLE_TAGS_TRACKED`). Storage servers periodically send per-throttlingId read cost statistics to the ratekeeper through `StorageQueuingMetricsReply` messages.

For write operations that are sampled (with probability determined by `COMMIT_SAMPLE_COST`), write costs are aggregated on commit proxies in the `ProxyCommitData::ssTrTagCommitCost` object. Per-storage, per-throttlingId write cost statistics are periodically sent from commit proxies to the ratekeeper through `ReportCommitCostEstimationRequest` messages.

The ratekeeper tracks per-storage, per-throttlingId cost statistics in the `QuotaThrottlerImpl::throughput` object.

The ratekeeper must also track the rate of transactions performed with each throttlingId. Each GRV proxy agreggates a per-throttlingId counter of transactions started (without sampling). These are sent to the ratekeeper through `GetRateInfoRequest` messages. The global tag throttler then tracks per tag transaction rates in the `QuotaThrottlerImpl::throttlingStatistics` object.

### Average Cost Calculation
Quotas are expressed in terms of cost, but because throttling is enforced at the beginning of transactions, budgets need to be calculated in terms of transactions per second. To make this conversion, it is necessary to track the average cost of transactions (per-tag, and per-tag on a particular storage server).

Both cost and transaction counters are exponentially smoothed over time, with knob-configurable smoothing intervals.

### Limit Calculation
The transaction budget that ratekeeper calculates and distributes to clients (via GRV proxies) for each throttlingId is calculated based on several intermediate rate calculations, outlined in this section.

* Reserved Rate: Based on reserved quota and the average transaction cost, a reserved TPS rate is computed for each throttlingId.

* Desired Rate: Based on total quota and the average transaction cost, a desired TPS rate is computed for each throttlingId.

* Limiting Rate: When a storage server is near saturation, throttlingIds contributing significantly to the workload on this storage server will receive a limiting TPS rate, computed to relieve the workload on the storage server.

* Target Rate: The target rate is the cluster-wide rate enforced by the quota throttler. This rate is computed as:

```
targetTps = max(reservedTps, min(desiredTps, limitingTps));
```

### Limiting Rate Calculation
In addition to throttlignId busyness statistics, the `StorageQueuingMetricsReply` messages sent from storage servers to the ratekeeper also contain metrics on the health of storage servers. The ratekeeper uses these metrics as part of its calculation of a global transaction rate (independent of tag throttling).

The quota throttler also makes use of these metrics to compute a "throttling ratio" for each storage server. This throttling ratio is computed in `StorageQueueInfo::getThrottlingRatio`. The quota throttler uses the throttling ratio for each tracked storage server to compute a "limiting transaction rate" for each combination of storage server and tag.

In the "healthy" case where no metrics are near saturation, the throttling ratio will be an empty `Optional<double>`, indicating that the storage server is not near saturation. If, on the other hand, the metrics indicate approaching saturation, the throttling ratio will indicate the ratio of current throughput the storage server can serve. In this case, the quota throttler looks at the current cost being served by the storage server, multiplies it by the throttling ratio, and computes a limiting cost for the storage server. Among all throttlingIds using significant resources on this storage server, this limiting cost is divided up according to the relative total quotas allocated to these throttlingIds. Next, a transaction limit is determined for each throttlingId, based on how much the average transaction for the given throttlingId affects the given storage server.

These per-throttlingId, per-storage limiting transaction rates are aggregated to compute per-throttlingId limiting transaction rates:

```
limitingTps(throttlingId) = min{limitingTps(throttlingId, storage) : all storage servers}
```

If the throttling ratio is empty for all storage servers affected by a throttlingId, then the per-throttlingId limiting TPS rate is also empty. In this case the target rate for this throttlingId is simply the desired rate.

If an individual zone is unhealthy, it may cause the throttling ratio for storage servers in that zone to shoot up. This should not be misinterpretted as a workload issue that requires active throttling. Therefore, the zone with the worst throttling ratios is ignored when computing the limiting transaction rate for a throttlingId (similar to the calculation of the global transaction limit in `RKRateUpdater::update`).

## Simulation Testing
`TagThroughputQuota.toml` and `TenantGroupThroughputQuota.toml` provide simple end-to-end tests using the quota throttler. Quotas are set using the internal throughput quota API in the `ThroughputQuota` workload. This is run with the `Cycle` workload, which randomly tags transactions or assigns them to tenant groups. 

In addition to this end-to-end test, there is a suite of unit tests with the `/QuotaThrottler/` prefix. These tests run in a mock environment, with mock storage servers providing simulated storage queue statistics and throttlingId busyness reports. Mock clients simulate workload on these mock storage servers, and get throttling feedback directly from a quota throttler which is monitoring the mock storage servers.

In each unit test, the `monitorActor` function is used to periodically check whether or not a desired equilibrium state has been reached. If the desired state is reached and maintained for a sufficient period of time, the test passes. If the unit test is unable to reach this desired equilibrium state before a timeout, the test will fail. Commonly, the desired state is for the quota throttler to report a client rate sufficiently close to the desired rate specified as an input to the `rateIsNear` function.

## Visibility

### Tracing
On the ratekeeper, every `SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL` seconds, the ratekeeper will call `QuotaThrottler::getProxyRates`. At the end of the rate calculation for each throttlingId, a trace event of type `QuotaThrottler_GotRate` is produced. This trace event reports the relevant inputs that went in to the rate calculation, and can be used for debugging.

On storage servers, every `SERVER_KNOBS->TAG_MEASUREMENT_INTERVAL` seconds, there are `BusyReader` events for every throttlingId that has sufficient read cost to be reported to the ratekeeper. Both cost and fractional busyness are reported.
