# Cluster Health Metric

`cluster_health::Monitor` is a cluster controller background task that periodically evaluates a fixed set of health factors and emits a `ClusterHealthMetric` trace event.

The monitor is controlled by these server knobs:

| Knob | Default | Meaning |
| --- | --- | --- |
| `CLUSTER_HEALTH_METRIC_ENABLE` | `false` | Enables the cluster controller monitor. When disabled, the monitor exits immediately and does not emit `ClusterHealthMetric`. Simulation buggification can set this to `true`. |
| `CLUSTER_HEALTH_METRIC_POLL_INTERVAL` | `5.0` seconds | Time between monitor evaluations after the first evaluation. |
| `CLUSTER_HEALTH_METRIC_STORAGE_INTERVENTION_THRESHOLD` | `0.20` | Storage server free-space warning threshold. This is a ratio, not a percentage: `0.20` means 20% free space. |
| `CLUSTER_HEALTH_METRIC_STORAGE_CRITICAL_THRESHOLD` | `0.10` | Storage server free-space critical threshold. This should be less than or equal to the storage intervention threshold. |
| `CLUSTER_HEALTH_METRIC_TLOG_INTERVENTION_THRESHOLD` | `0.20` | TLog queue-disk free-space warning threshold. This is a ratio, not a percentage: `0.20` means 20% free space. |
| `CLUSTER_HEALTH_METRIC_TLOG_CRITICAL_THRESHOLD` | `0.10` | TLog queue-disk free-space critical threshold. This should be less than or equal to the TLog intervention threshold. |
| `CLUSTER_HEALTH_METRIC_RK_CRITICAL_RELEASED_TPS_RATIO_THRESHOLD` | `1.2` | Ratekeeper critical throttling threshold for `TPSLimit / ReleasedTPS`. With the default, ratekeeper is critical when the current TPS limit is less than 120% of recently released TPS. |

Threshold comparisons are strict: a value equal to a threshold does not trigger that threshold's health level.

## Health Levels

Each factor uses the following levels:

- `HEALTHY`: No issue is currently indicated by the factor.
- `SELF_HEALING`: The cluster is degraded, but automatic recovery or repair is already in progress.
- `INTERVENTION_REQUIRED`: The cluster is still functioning, but an operator likely needs to act to restore full health.
- `CRITICAL_INTERVENTION_REQUIRED`: The cluster is functioning, but in a severe state that likely requires immediate operator action.
- `OUTAGE`: The factor indicates loss of availability or loss of a required safety property.
- `METRICS_MISSING`: The factor could not evaluate because the required trace-event data was unavailable or malformed.

For aggregation, the implementation assigns an internal score to each level and keeps the lowest-scoring factor as the aggregate result:

- `HEALTHY` = 100
- `SELF_HEALING` = 80
- `INTERVENTION_REQUIRED` = 60
- `CRITICAL_INTERVENTION_REQUIRED` = 40
- `METRICS_MISSING` = 20
- `OUTAGE` = 0

These numeric values provide a scalar metric that can be emitted, and leave room for additional intermediate values to be added in the future.

## Factors

The monitor currently evaluates these factors:

### `StorageSpace`

Source event:
- `"<StorageServerInterface.id()>/StorageMetrics"`

Fields used:
- `KvstoreBytesAvailable`
- `KvstoreBytesTotal`

Behavior:
- Computes the minimum `KvstoreBytesAvailable / KvstoreBytesTotal` ratio across storage servers.
- Returns `CRITICAL_INTERVENTION_REQUIRED` when the ratio is below `CLUSTER_HEALTH_METRIC_STORAGE_CRITICAL_THRESHOLD`.
- Returns `INTERVENTION_REQUIRED` when the ratio is below `CLUSTER_HEALTH_METRIC_STORAGE_INTERVENTION_THRESHOLD` and at or above `CLUSTER_HEALTH_METRIC_STORAGE_CRITICAL_THRESHOLD`.
- Returns `HEALTHY` otherwise.

### `TLogSpace`

Source event:
- `"<TLogInterface.id()>/TLogMetrics"`

Fields used:
- `QueueDiskBytesAvailable`
- `QueueDiskBytesTotal`

Behavior:
- Computes the minimum `QueueDiskBytesAvailable / QueueDiskBytesTotal` ratio across tlogs.
- Returns `CRITICAL_INTERVENTION_REQUIRED` when the ratio is below `CLUSTER_HEALTH_METRIC_TLOG_CRITICAL_THRESHOLD`.
- Returns `INTERVENTION_REQUIRED` when the ratio is below `CLUSTER_HEALTH_METRIC_TLOG_INTERVENTION_THRESHOLD` and at or above `CLUSTER_HEALTH_METRIC_TLOG_CRITICAL_THRESHOLD`.
- Returns `HEALTHY` otherwise.

### `StorageReplication`

Source event:
- `MovingData`

Fields used:
- `InQueue`
- `InFlight`
- `PriorityTeamUnhealthy`
- `PriorityTeam2Left`
- `PriorityTeam1Left`
- `PriorityTeam0Left`

Behavior:
- Returns `OUTAGE` if any `PriorityTeam0Left > 0`.
- Returns `CRITICAL_INTERVENTION_REQUIRED` if any `PriorityTeam1Left > 0` and the configured storage team size is 3.
- Returns `SELF_HEALING` if data movement is queued or in flight to restore replication.
- Returns `HEALTHY` otherwise.

### `RecoveryState`

Source:
- `ServerDBInfo::recoveryState` from the cluster controller

Fields used:
- none; this factor reads the cluster controller's in-memory `RecoveryState`

Behavior:
- Returns `OUTAGE` if recovery state is below `RecoveryState::ACCEPTING_COMMITS`.
- Returns `SELF_HEALING` if recovery is at or above `ACCEPTING_COMMITS` but below `FULLY_RECOVERED`.
- Returns `HEALTHY` at `FULLY_RECOVERED`.

### `ProcessErrors`

Source event:
- latest worker error event, fetched with `EventLogRequest()`

Fields used:
- none are parsed directly; the factor only checks whether a non-empty latest-error trace event exists.

Behavior:
- Returns `CRITICAL_INTERVENTION_REQUIRED` if any worker reports a non-empty latest error.
- Returns `HEALTHY` if all non-failed latest-error records are empty and at least one latest-error request succeeded.
- Returns `METRICS_MISSING` only if every latest-error request fails.

### `RkThrottling`

Source event:
- `RkUpdate`

Fields used:
- `ReleasedTPS`: Ratekeeper's smoothed rate of transactions that GRV proxies have recently released from their start-transaction queues. A released transaction has been allowed to get a read version and begin; this is not commit throughput.
- `TPSLimit`: Ratekeeper's current transaction-per-second limit for normal transaction starts, computed from cluster pressure signals and sent back to GRV proxies to pace future releases. A value of `0` means ratekeeper is not allowing normal transaction starts.

Behavior:
- Returns `OUTAGE` if any `TPSLimit == 0`.
- Computes the minimum `TPSLimit / ReleasedTPS` ratio across samples with nonzero `ReleasedTPS`.
- Ignores samples with `ReleasedTPS == 0` for the ratio calculation to avoid dividing by zero.
- Returns `CRITICAL_INTERVENTION_REQUIRED` when the minimum ratio is below `CLUSTER_HEALTH_METRIC_RK_CRITICAL_RELEASED_TPS_RATIO_THRESHOLD`.
- Returns `HEALTHY` otherwise, including the case where every non-outage sample has `ReleasedTPS == 0`.

For `CLUSTER_HEALTH_METRIC_RK_CRITICAL_RELEASED_TPS_RATIO_THRESHOLD`, lower ratios are worse:

- `TPSLimit / ReleasedTPS == 2.0` means ratekeeper is allowing twice the recently released transaction rate, so this factor is healthy with the default threshold.
- `TPSLimit / ReleasedTPS == 1.0` means ratekeeper's current limit is equal to the recently released transaction rate, so this factor is critical with the default threshold of `1.2`.
- `TPSLimit / ReleasedTPS < 1.0` means ratekeeper's current limit is below the recently released transaction rate, which is also critical.

## Missing Metrics Semantics

Most factors treat missing or malformed inputs as `METRICS_MISSING`.

This can happen when:

- the underlying latest-event RPC fails
- no relevant worker emitted the requested trace event
- the trace event exists but does not contain the required fields
- a field cannot be parsed to the expected type

The implementation filters out empty `TraceEventFields` before interpreting a factor, so workers that never emit a role-specific event do not automatically degrade the whole metric.

## `ClusterHealthMetric` Trace Event Schema

The monitor emits a periodic trace event named `ClusterHealthMetric`.

Fields:

- `FactorStorageSpace`: string enum, one of `Outage`, `CriticalInterventionRequired`, `InterventionRequired`, `SelfHealing`, `MetricsMissing`, `Healthy`
- `FactorTLogSpace`: same enum
- `FactorStorageReplication`: same enum
- `FactorRecoveryState`: same enum
- `FactorProcessErrors`: same enum
- `FactorRkThrottling`: same enum
- `Aggregate`: same enum, computed from the most limiting factor
- `AggregateValue`: numeric aggregate score, currently one of `0`, `20`, `40`, `60`, `80`, or `100`
- `LimitingFactor`: factor name string, present when not healthy

The aggregate is determined by taking the lowest-scoring level across all factors.
