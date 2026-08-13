---
name: analyze-fdb-trace
description: Analyze FoundationDB XML trace files and build interactive trace visualizations showing configuration history, worker and role lifetimes, recovery causes, active roles, workloads, and system behavior. Use when the user mentions FDB traces, trace.*.xml, simulation logs, recovery timelines, role placement, configuration changes, or asks to visualize a FoundationDB run.
---

# Analyze FoundationDB Traces

Use this workflow for FDB XML traces from simulation or production. The goal is an evidence-backed, reusable visualization rather than a raw event dump.

## Quick start

1. Run the extractor:

```bash
python3 .cursor/skills/analyze-fdb-trace/scripts/analyze_trace.py TRACE.xml --output /tmp/fdb-trace-analysis.json
```

2. Validate its output:

```bash
python3 .cursor/skills/analyze-fdb-trace/scripts/validate_analysis.py /tmp/fdb-trace-analysis.json
```

3. Read the normalized JSON.
4. If a visualization is requested, read and follow the available `canvas` skill before creating or editing a `.canvas.tsx` file.
5. Embed the normalized data in one Canvas file. Do not fetch the source trace at Canvas runtime.

## Investigation workflow

### 1. Establish run context

Report:

- Command line, FDB/source version, random seed, test file, fault-injection and Buggify state
- Event count, distinct event types, severity distribution, simulated time span, real elapsed time
- Unique emitting addresses, separating `0.0.0.0:0` from simulated or real processes
- Test outcome; a genuine severity-40 event fails simulation

Use `Time` as the causal timeline. `DateTime` is wall-clock time and may be coarse or distorted relative to simulation.

### 2. Reconstruct the high-signal story

Prioritize these event families:

1. Test/workload phases: `TestRunning`, `TestStarting`, `TestComplete`, `TestResults`
2. Configuration: `SimulatorConfig`, `ChangeConfig`, `MasterRecoveredConfig`, `ConfigurationMonitor`
3. Recovery: `ClusterRecovery`, `ClusterRecoveryRetrying`, `MasterRecoveryState`, `MasterRecoveryMetrics`
4. Topology: `Role`, `RoleAdd`, `RoleRemove`, `WorkerRegister`, `GotServerDBInfoChange`
5. Faults: `FailMachine`, `KillMachine`, `RebootingProcess`, `ProcessDestroyed`, `RestartingTxnSubsystem`
6. Behavior: selected `*Metrics`, rollback, data-distribution, queue, lag, and throttling events

Collapse startup knob dumps, periodic role refreshes, metrics, and shutdown code-coverage records by default. Preserve access to raw details.

### 3. Treat configuration as layered state

Keep these states distinct:

- **Desired/requested:** `SimulatorConfig` and `ChangeConfig`
- **Recovered/canonical:** JSON in `MasterRecoveredConfig.Conf` and recruiting `MasterRecoveryState.Conf`
- **Durable recovery state:** coordinator-held `DBCoreState`, exposed through recovery-generation events
- **Effective runtime view:** `ServerDBInfo`/`ClientDBInfo`, observed via `GotServerDBInfoChange` and `PublishNewClientInfo`

At every configuration set or recovered snapshot:

- Show simulated time, source event, state kind, and change from the prior comparable state
- Show the complete normalized key/value configuration
- Do not call `ConfigurationMonitor` a configuration mutation; it normally reports the recovery phase being watched
- Collapse `GotServerDBInfoChange` fan-out by `InfoGeneration`, retaining per-worker delivery times

### 4. Reconstruct role lifetimes

Use `Role` events:

- `Transition=Begin` opens a span keyed by `(ID, As)`
- `Transition=End` closes it
- `Transition=Refresh` is a heartbeat, not a new instance
- Open spans are right-censored at trace end and must be labeled “active at trace end”

Group by `Machine`, then role. If the same process hosts concurrent instances of one role, allocate visual sub-lanes. Labels such as `TL.2` are visualization-local sub-lane numbers, not FDB role names or IDs; explain this in the UI. Hover details should show the real role ID, process, start, end, and origination when available.

### 5. Explain every recovery

Anchor episodes on `ClusterRecovery BeginPair` and pair with `EndPair`. At each recovery start:

- Show the exact reason event and evidence
- Show recovery actor ID and recovery count
- Snapshot all roles active at that exact file position/time
- Show instance counts and process placements
- Preserve overlap between old and newly recruited generations
- Show phase progression and durations from `MasterRecoveryState`

Prefer explicit evidence:

1. `ClusterRecoveryRetrying.Error` and `ErrorDescription`
2. Immediately preceding role termination/error
3. Controller/process failure and restart events
4. Temporal inference only when no explicit link exists

Label temporal links as inferred and include the correlation window. Never turn proximity alone into a definitive root-cause claim.

Distinguish:

- `MasterRecoveryState.StatusCode`: detailed `RecoveryStatus`
- `MasterRecoveryMetrics.RecoveryState`: nine-phase operational `RecoveryState`

### 6. Build the visualization

The primary view should synchronize one time cursor across:

- High-level story timeline
- Configuration history and full snapshot/diff
- Worker/role lifetime Gantt chart
- Recovery reason and active-role snapshot
- Topology at the selected time
- Selected behavior metrics
- Raw event inspector

Minimum visualization requirements:

- Label time axes in simulated seconds
- Include source and time-range captions
- Include complete legends
- Provide filters for datacenter/process groups and event families
- Use process address plus parsed locality; do not treat `0.0.0.0:0` as a cluster node
- Explain visual-only labels and inferred causal links
- Avoid static architecture diagrams as the main view; FDB roles and log generations move over time

## Verification

Before handing off:

- Extractor exits successfully
- Validator reports `OK`
- Event count matches parsed XML events
- Every `Role Begin` appears in a closed or right-censored span
- Recovery intervals pair by `BeginPair`/`EndPair` when an end exists
- Active-role snapshots use `start <= recovery_time < end`
- Configuration JSON and configure strings are preserved even if normalization is partial
- Canvas TypeScript check and lint pass
- No event is claimed as causal without explicit evidence or an inference label

## Domain reference

Read [reference.md](reference.md) when event semantics, recovery phases, role abbreviations, or correlation keys need interpretation.
