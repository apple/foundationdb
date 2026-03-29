# Cluster Health Metric

`cluster_health::Monitor` is a cluster controller background task that periodically evaluates a fixed set of health factors and emits a `ClusterHealthMetric` trace event.

The monitor is controlled by these server knobs:

- `CLUSTER_HEALTH_METRIC_ENABLE`
- `CLUSTER_HEALTH_METRIC_POLL_INTERVAL`
- `CLUSTER_HEALTH_METRIC_STORAGE_INTERVENTION_THRESHOLD`
- `CLUSTER_HEALTH_METRIC_STORAGE_CRITICAL_THRESHOLD`
- `CLUSTER_HEALTH_METRIC_TLOG_INTERVENTION_THRESHOLD`
- `CLUSTER_HEALTH_METRIC_TLOG_CRITICAL_THRESHOLD`
- `CLUSTER_HEALTH_METRIC_RK_CRITICAL_RELEASED_TPS_RATIO_THRESHOLD`

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
- `CRITICAL_INTERVENTION_REQUIRED` = 75
- `INTERVENTION_REQUIRED` = 50
- `SELF_HEALING` = 25
- `METRICS_MISSING` = 0
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
- Returns `INTERVENTION_REQUIRED` when the ratio is below `CLUSTER_HEALTH_METRIC_STORAGE_INTERVENTION_THRESHOLD`.
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
- Returns `INTERVENTION_REQUIRED` when the ratio is below `CLUSTER_HEALTH_METRIC_TLOG_INTERVENTION_THRESHOLD`.
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
- Returns `SELF_HEALING` if data movement is queued or in flight to restore replication.
- Returns `HEALTHY` otherwise.

### `RecoveryState`

Source event:
- `MasterRecoveryState`

Fields used:
- `StatusCode`

Behavior:
- Returns `OUTAGE` if any recovery state is below `RecoveryStatus::accepting_commits`.
- Returns `SELF_HEALING` if recovery is at or above `accepting_commits` but below `fully_recovered`.
- Returns `HEALTHY` at `fully_recovered`.

### `ProcessErrors`

Source event:
- latest worker error event, fetched with `EventLogRequest()`

Fields used:
- none are parsed directly; the factor only checks whether a non-empty latest-error trace event exists.

Behavior:
- Returns `CRITICAL_INTERVENTION_REQUIRED` if any worker reports a non-empty latest error.
- Returns `HEALTHY` if all latest-error records are empty.
- Returns `METRICS_MISSING` if the latest-error fetch fails.

### `RkThrottling`

Source event:
- `RkUpdate`

Fields used:
- `ReleasedTPS`
- `TPSLimit`

Behavior:
- Returns `OUTAGE` if any `TPSLimit == 0`.
- Computes the minimum `TPSLimit / ReleasedTPS` ratio across samples with nonzero `ReleasedTPS`.
- Returns `CRITICAL_INTERVENTION_REQUIRED` when that ratio is below `CLUSTER_HEALTH_METRIC_RK_CRITICAL_RELEASED_TPS_RATIO_THRESHOLD`.
- Returns `HEALTHY` otherwise.

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
- `LimitingFactor`: factor name string, present when not healthy

The aggregate is determined by taking the lowest-scoring level across all factors.
