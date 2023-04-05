## Global Tag Throttling

When the `GLOBAL_TAG_THROTTLING` knob is enabled, the ratekeeper will use the [transaction tagging feature](https://apple.github.io/foundationdb/transaction-tagging.html) to throttle tags according to the global tag throttling algorithm. This page describes the implementation of this algorithm.

### Tag Quotas
The global tag throttler bases throttling decisions on "quotas" provided by clients through the tag quota API. Each tag quota has two components:

* Reserved quota
* Total quota

The global tag throttler cannot throttle tags to a throughput below the reserved quota, and it cannot allow throughput to exceed the total quota.

### Cost
Internally, the units for these quotas are bytes. The cost of an operation is rounded up to the nearest page size. The cost of a read operation is computed as:

```
readCost = ceiling(bytesRead / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
```

The cost of a write operation is computed as:

```
writeCost = CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * ceiling(bytesWritten / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
```

Here `bytesWritten` includes cleared bytes. The size of range clears is estimated at commit time.

### Tuple Layer
Tag quotas are stored inside of the system keyspace (with prefix `\xff/tagQuota/`). They are stored using the tuple layer, in a tuple of form: `(reservedQuota, totalQuota)`. There is currently no custom code in the bindings for manipulating these system keys. However, in any language for which bindings are available, it is possible to use the tuple layer to manipulate tag quotas.

### fdbcli
The easiest way for an external client to interact with tag quotas is through `fdbcli`. To get the quota (in bytes/second) of a particular tag, run the following command:

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

### Limit Calculation
The transaction budget that ratekeeper calculates and distributes to clients (via GRV proxies) for each tag is calculated based on several intermediate rate calculations, outlined in this section.

* Reserved Rate: Based on reserved quota and the average transaction cost, a reserved TPS rate is computed for each tag.

* Desired Rate: Based on total quota and the average transaction cost, a desired TPS rate is computed for each tag.

* Limiting Rate: When a storage server is near saturation, tags contributing notably to the workload on this storage server will receive a limiting TPS rate, computed to relieve the workload on the storage server.

* Target Rate: The target rate is the cluster-wide rate enforced by the global tag throttler. This rate is computed as:

```
targetTps = max(reservedTps, min(desiredTps, limitingTps));
```

* Per-Client Rate: While the target rate represents the cluster-wide desired throughput according to the global tag throttler, this budget must be shared across potentially many clients. Therefore, based on observed throughput from various clients, each client will receive an equal budget based on a per-client, per-tag rate computed by the global tag throttler. This rate is in the end what will be sent to clients to enforce throttling.

## Implementation

### Stat collection
The transaction rates and costs of all transactions must be visible to the global tag throttler. Whenever a client tags a transaction, sampling is performed to determine whether to attach the tag to messages sent to storage servers and commit proxies.

For read operations that are sampled (with probability `CLIENT_KNOBS->READ_TAG_SAMPLE_RATE`), read costs are aggregated on storage servers using the `TransactionTagCounter` class. This class tracks the busyness of the top-k tags affecting the storage server with read load (here `k` is determined by `SERVER_KNOBS->SS_THROTTLE_TAGS_TRACKED`). Storage servers periodically send per-tag read cost statistics to the ratekeeper through `StorageQueuingMetricsReply` messages.

For write operations that are sampled (with probability `COMMIT_SAMPLE_COST`), write costs are aggregated on commit proxies in the `ProxyCommitData::ssTrTagCommitCost` object. Per-storage, per-tag write cost statistics are periodically sent from commit proxies to the ratekeeper through `ReportCommitCostEstimationRequest` messages.

The ratekeeper tracks per-storage, per-tag cost statistics in the `GlobalTagThrottlerImpl::throughput` object.

The ratekeeper must also track the rate of transactions performed with each tag. Each GRV proxy agreggates a per-tag counter of transactions started (without sampling). These are sent to the ratekeeper through `GetRateInfoRequest` messages. The global tag throttler then tracks per tag transaction rates in the `GlobalTagThrottlerImpl::tagStatistics` object.

### Average Cost Calculation
Quotas are expressed in terms of cost, but because throttling is enforced at the beginning of transactions, budgets need to be calculated in terms of transactions per second. To make this conversion, it is necessary to track the average cost of transactions (per-tag, and per-tag on a particular storage server).

Both cost and transaction counters are smoothed using the `Smoother` class to provide stability over time. The "smoothing interval" can be modified through `SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME`.

### Reserved Rate Calculation
The global tag throttler periodically reads reserved quotas from the system keyspace. Using these reserved quotas and the average cost of transactions with the given tag, a reserved TPS rate is computed. Read and write rates are aggregated as follows:

```
reservedTps = max(reservedReadTps, reservedWriteTps);
```

### Desired Rate Calculation
Similar to reserved rate calculation, the total quota is read from the system key space. Then, using the average cost of transactions with the given tag, a desired TPS rate is computed. Read and write rates are aggregated as follows:

```
desiredTps = min(desiredReadTps, desiredWriteTps);
```

### Limiting Rate Calculation
In addition to tag busyness statistics, the `StorageQueuingMetricsReply` messages sent from storage servers to the ratekeeper also contain metrics on the health of storage servers. The ratekeeper uses these metrics as part of its calculation of a global transaction rate (independent of tag throttling).

The global tag throttler also makes use of these metrics to compute a "throttling ratio" for each storage server. This throttling ratio is computed in `StorageQueueInfo::getThrottlingRatio`. The global tag throttler uses the throttling ratio for each tracked storage server to compute a "limiting transaction rate" for each combination of storage server and tag.

In the "healthy" case where no metrics are near saturation, the throttling ratio will be an empty `Optional<double>`, indicating that the storage server is not near saturation. If, on the other hand, the metrics indicate approaching saturation, the throttling ratio will be a number between 0 and 2 indicating the ratio of current throughput the storage server can serve. In this case, the global tag throttler looks at the current cost being served by the storage server, multiplies it by the throttling ratio, and computes a limiting cost for the storage server. Among all tags using significant resources on this storage server, this limiting cost is divided up according to the relative total quotas allocated to these tags. Next, a transaction limit is determined for each tag, based on how much the average transaction for the given tag affects the given storage server.

These per-tag, per-storage limiting transaction rates are aggregated to compute per-tag limiting transaction rates:

```
limitingTps(tag) = min{limitingTps(tag, storage) : all storage servers}
```

If the throttling ratio is empty for all storage servers affected by a tag, then the per-tag, per-storage limiting TPS rate is also empty. In this case the target rate for this tag is simply the desired rate.

If an individual zone is unhealthy, it may cause the throttling ratio for storage servers in that zone to shoot up. This should not be misinterpretted as a workload issue that requires active throttling. Therefore, the zone with the worst throttling ratios is ignored when computing the limiting transaction rate for a tag (similar to the calculation of the global transaction limit in `Ratekeeper::updateRate`).

### Client Rate Calculation
The smoothed per-client rate for each tag is tracked within `GlobalTagThrottlerImpl::PerTagStatistics`. Once a target rate has been computed, this is passed to `GlobalTagThrotterImpl::PerTagStatistics::updateAndGetPerClientRate` which adjusts the per-client rate. The per-client rate is meant to limit the busiest clients, so that at equilibrium, the per-client rate will remain constant and the sum of throughput from all clients will match the target rate.

## Simulation Testing
The `ThroughputQuota.toml` test provides a simple end-to-end test using the global tag throttler. Quotas are set using the internal tag quota API in the `ThroughputQuota` workload. This is run with the `Cycle` workload, which randomly tags transactions. 

In addition to this end-to-end test, there is a suite of unit tests with the `/GlobalTagThrottler/` prefix. These tests run in a mock environment, with mock storage servers providing simulated storage queue statistics and tag busyness reports. Mock clients simulate workload on these mock storage servers, and get throttling feedback directly from a global tag throttler which is monitoring the mock storage servers.

In each unit test, the `GlobalTagThrottlerTesting::monitor` function is used to periodically check whether or not a desired equilibrium state has been reached. If the desired state is reached and maintained for a sufficient period of time, the test passes. If the unit test is unable to reach this desired equilibrium state before a timeout, the test will fail. Commonly, the desired state is for the global tag throttler to report a client rate sufficiently close to the desired rate specified as an input to the `GlobalTagThrottlerTesting::rateIsNear` function.

## Visibility

### Tracing
On the ratekeeper, every `SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL` seconds, the ratekeeper will call `GlobalTagThrottler::getClientRates`. At the end of the rate calculation for each tag, a trace event of type `GlobalTagThrottler_GotClientRate` is produced. This trace event reports the relevant inputs that went in to the rate calculation, and can be used for debugging.

On storage servers, every `SERVER_KNOBS->TAG_MEASUREMENT_INTERVAL` seconds, there are `BusyReadTag` events for every tag that has sufficient read cost to be reported to the ratekeeper. Both cost and fractional busyness are reported.
