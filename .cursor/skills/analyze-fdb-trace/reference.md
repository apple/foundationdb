# FoundationDB Trace Reference

## Common event envelope

Every XML event normally includes:

- `Severity`: 5 Debug, 10 Info, 20 Warn, 30 WarnAlways, 40 Error
- `Time`: network/simulation time in seconds; primary ordering axis
- `DateTime`: UTC wall-clock timestamp
- `Type`: event name
- `Machine`: emitting network address
- `ID`: contextual UID; it is not one universal entity key
- `Roles`: active role abbreviations on the emitting process
- `ThreadID`, `LogGroup`
- `TrackLatestType`: `Original` or log-roll `Rolled` for tracked state

Use file order to break equal-`Time` ties.

## Role abbreviations

| Abbreviation | Role |
|---|---|
| WK | Worker |
| CC | ClusterController |
| CD | Coordinator |
| MS | MasterServer |
| GP | GrvProxyServer |
| CP | CommitProxyServer |
| RV | Resolver |
| TL | TLog |
| SL | SharedTLog |
| SS | StorageServer |
| DD | DataDistributor |
| RK | Ratekeeper |
| CS | ConsistencyScan |
| TS | Tester |

The `Roles` field is an emitter snapshot, not necessarily the subject of the event.

## Recovery models

`RecoveryState` is the operational state broadcast through `ServerDBInfo`:

1. READING_CSTATE
2. LOCKING_CSTATE
3. RECRUITING
4. RECOVERY_TRANSACTION
5. WRITING_CSTATE
6. ACCEPTING_COMMITS
7. ALL_LOGS_RECRUITED
8. STORAGE_RECOVERED
9. FULLY_RECOVERED

`RecoveryStatus` is finer-grained and appears in `MasterRecoveryState.Status`, including:

- reading_coordinated_state
- locking_coordinated_state
- locking_old_transaction_servers
- reading_transaction_system_state
- configuration_missing / configuration_never_created / configuration_invalid
- recruiting_transaction_servers
- initializing_transaction_servers
- recovery_transaction
- writing_coordinated_state
- accepting_commits
- all_logs_recruited
- storage_recovered
- fully_recovered

The `Master` event prefix is historical/tooling compatibility; the Cluster Controller drives recovery.

## Configuration layers

1. `DatabaseConfiguration`: logical config in system keys
2. `txnStateStore`: transaction metadata reconstructed from TLogs
3. `DBCoreState`: durable log generations and recovery count held by coordinators
4. `ServerDBInfo` / `ClientDBInfo`: transient runtime broadcast

Useful events:

- `SimulatorConfig.ConfigString`
- `ChangeConfig.Mode`
- `MasterRecoveredConfig.Conf`
- `MasterRecoveryState.Conf`
- `ConfigurationMonitor.MasterRecoveryState`
- `GotServerDBInfoChange.ChangeID` and `InfoGeneration`
- `PublishNewClientInfo`

## Correlation keys

Use keys within their event family:

- Recovery interval: `BeginPair` / `EndPair`
- Role instance: `Role.ID` plus `As`
- Runtime config propagation: `InfoGeneration`, then `ChangeID`
- Recovery attempt: actor `ID`, `MyRecoveryCount`, or `RecoveryCount`
- Process: `Machine`
- Locality: parse `Locality` for `dcid`, `zoneid`, `machineid`, and `processid`

Do not assume all nonzero `ID` values share one namespace.

## Architecture mapping

- Coordinators hold durable coordination state.
- One Cluster Controller recruits roles and broadcasts `ServerDBInfo`.
- Write path: client → GRV Proxy → Commit Proxy → Resolver → TLog quorum.
- Storage Servers pull mutations from TLogs and serve reads.
- Data Distributor manages shard placement and storage teams.
- Ratekeeper converts storage/TLog pressure into transaction admission limits.
- Simulation workloads inject process, machine, network, storage, and rollback faults.

## Noise and integrity caveats

- Role refresh events and periodic metrics can dominate volume.
- Startup emits many `Knob` records.
- Shutdown may emit a large `CodeCoverage` block.
- A field over 495 bytes may be truncated.
- An event over 4000 bytes is dropped and replaced by `TraceEventOverflow`.
- Sampling and suppression mean absence is not proof that an action did not occur.
- Injected simulation faults may downgrade errors.
- `TrackLatestType=Rolled` can replay state across file rolls; use `OriginalTime` when present.
- One logical run may span multiple rolled trace files.

## Source references

- `flow/include/flow/Trace.h`
- `flow/Trace.cpp`
- `flow/XmlTraceLogFormatter.cpp`
- `fdbserver/core/WorkerSupport.cpp`
- `fdbrpc/include/fdbrpc/simulator.h`
- `fdbserver/clustercontroller/ClusterRecovery.cpp`
- `fdbserver/core/include/fdbserver/core/RecoveryState.h`
- `fdbserver/worker/worker.cpp`
- `fdbclient/include/fdbclient/DatabaseConfiguration.h`
- `fdbserver/core/include/fdbserver/core/ServerDBInfo.h`
- `design/recovery-internals.md`
- `design/transaction-state-store.md`
