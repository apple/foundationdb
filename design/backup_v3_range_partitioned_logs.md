# Range Partitioned Backup Mutation Logs (Backup v3) [Experimental]

> **Status: Experimental.** This feature is under active development and is not yet ready for general or critical workload usage. The design, on-disk layout, knobs, and command-line surface are subject to change.

## Objective

FoundationDB restore performance is currently bottlenecked by sequential replay of mutation logs. This design introduces range-partitioned mutation logs (Backup v3), which partition mutations by key range and version interval, enabling independent and parallel restore per key range. The goal is to reduce mutation-log replay time by 3x-5x compared to the current Backup v2 partitioned logs, while preserving correctness, reliability, and backward compatibility with existing backup files.

## Background

FDB has gone through two prior backup generations:

**Backup v1**: CommitProxies wrote a copy of every mutation into a system keyspace, and the backup agent read from there and uploaded mutation log files to blob storage. This doubled write bandwidth on the proxies and consumed significant CPU.

**Backup v2 (partitioned logs)**, experimental in 7.4: Introduced a new `BackupWorker` role that pulls mutations directly from transaction logs and uploads them, eliminating the duplicate write on CommitProxies. Mutations at a given version are partitioned across BackupWorker tags and written to separate log files. The detailed design lives in [`backup_v2_partitioned_logs.md`](backup_v2_partitioned_logs.md).

Both v1 and v2 use the same restore model: range files are restored directly, then mutation logs are replayed in version order to bring the cluster forward to the target version. The replay phase is the long pole. In v2 specifically, mutation log files are partitioned across backup workers by an opaque tag — not by key range — so the restore engine must merge all partitions into a single version-ordered stream and apply mutations sequentially. This limits restore parallelism: each version's worth of mutations can only be applied after the previous version is done, regardless of how independent the underlying key ranges are.

Range files (key-value snapshot data) remain unchanged across v2 and v3; the change is concentrated entirely in mutation-log layout and the restore engine's ability to consume them in parallel.

## Requirements

### Functional Requirements

- **Correctness**: Restored data must exactly match original data at the target version.
- **Backward compatibility**: Restore tooling must detect the mutation log format on disk and handle both v2 partitioned logs and v3 range-partitioned logs. Existing v1/v2 backups remain restorable.
- **Fault tolerance**: The backup system must tolerate node failures in the FDB cluster — including BackupWorker crashes, CommitProxy recoveries, and cluster recoveries — without losing or duplicating mutations in the backup.
- **Restorability from S3 alone**: Restore must be self-contained, depending only on data in S3. No state on the original cluster is required to restore.
- **Reliability**: Meets FDB continuous backup & restore reliability standards. No loss of mutation logs.

### Non-Functional Requirements

- **Performance**: Mutation-log replay throughput during restore should improve from the current ~25 MB/s average to a target of 75–125 MB/s — a 3x-5x improvement. Overall restore should complete meaningfully faster than today, dominated by the mutation-log replay phase.
- **Security**: Encryption, authentication, and access controls must match existing backup data guarantees.
- **Observability**: Maintain existing metrics, dashboards, and alerts. Add new metrics where the design introduces new state (PartitionMap version, per-partition upload progress, partition-tag distribution).

## Design Overview

The user keyspace is divided into a configurable number of non-overlapping, contiguous key-range partitions (default `KNOB_BACKUP_NUM_OF_PARTITIONS ≈ 100`). Each partition is owned by exactly one BackupWorker at any point in time. BackupWorkers buffer mutations per partition and upload **one mutation log file per (partition, version interval) pair**. Restore reads only the files for the partitions it needs, and processes each partition's stream independently.

### New concepts

- **Partition**: a contiguous, non-overlapping range of user keys. The full user keyspace is the union of all partitions, with no gaps.
- **PartitionMap**: a mapping `BackupTag ↔ list<Partition>` that assigns partitions to BackupWorkers. Maintained by CommitProxy and persisted in `txnStateStore`.
- **PartitionMap History**: a versioned log of `(epoch, version, PartitionMap)` tuples persisted on Storage Servers under `\xff\x02/backupPartitionMap`. Required so BackupWorkers processing the tail end of an old epoch after recovery can route mutations correctly.

### Where changes are concentrated

1. **Partitioning and metadata management**: DataDistributor computes the partition vector; CommitProxy builds and broadcasts the PartitionMap; Storage Servers persist its history.
2. **Mutation ingestion and storage**: CommitProxy routes user mutations to BackupWorkers based on key range. Each BackupWorker writes one log file per (partition, version interval) to S3.
3. **Restore parallelism**: The restore engine processes mutation logs per key range concurrently with minimal cross-range synchronization.

### What does not change

Range files (snapshot key-value data) — file naming, layout, and writing path are identical to v2. Only the mutation-log directory structure and writing path are new.

## Detailed Design

### Initialization flow

When a backup with range-partitioned logs is started:

1. BackupAgent sets the special key `backupPartitionRequired` to request partitioning.
2. DataDistributor watches that key. On detection, it:
   - Queries all shards and uses `StorageMetricSample.getEstimate()` to compute per-shard byte sizes.
   - Divides the user keyspace into `KNOB_BACKUP_NUM_OF_PARTITIONS` contiguous, balanced partitions.
   - Writes the partition vector as a system metadata key at some version `n`.
   - Clears the `backupPartitionRequired` key.
3. CommitProxy receives the partition-vector mutation at version `n`, builds a `PartitionMap` (BackupTag ↔ list<Partition>), and persists it in `txnStateStore`.
4. CommitProxy broadcasts the PartitionMap to all BackupWorker and relevant StorageServer tags at the same version, ensuring every BackupWorker has the same map.
5. BackupWorkers receive the PartitionMap at version `n`, cache their assigned partitions in memory, create a new S3 directory for version `n+1`, and upload the `partitionId_keyRange_Map` metadata file.
6. From version `n+1` onward, CommitProxy routes every user mutation to the BackupWorker(s) whose partitions contain the mutation's key.
7. BackupWorkers buffer mutations per partition and periodically upload immutable mutation log files to S3.

The system is now operating with range-partitioned backup mutation logs.

### S3 file layout

Only the mutation-log directory changes. Range files, snapshots, and properties directories remain identical to v2.

**Existing v2 partitioned mutation logs** live under `plogs/`.

**New v3 range-partitioned mutation logs** live under `rlogs/`:

```
rlogs/
  <PartitionMapUpdateVersion>/
    partitionId_keyRange_Map           # written once per PartitionMap update
    <partitionId>/
      log,<beginVersion>,<endVersion>,<partitionId>,<randomUID>,<blockSize>
      log,<beginVersion>,<endVersion>,<partitionId>,<randomUID>,<blockSize>
      ...
```

#### Version directories

A new version directory is created when:

- The PartitionMap changes (initial partition, re-partition, re-distribute).
- A cluster recovery occurs.
- `Version % 100,000,000,000 == 0` — i.e., approximately every 100,000 seconds (~28 hours) of versions. This caps the number of log files per directory (~5,000) and keeps the directory tree balanced.

All BackupWorkers write to the same version directory because the directory choice is synchronized with the `txnStateStore` update through CommitProxy.

#### `partitionId_keyRange_Map` file

Written immediately on every PartitionMap update. Contains the `partitionId → keyRange` map for that version directory. All BackupWorkers attempt to write it but skip if it already exists, so concurrent writers do not overwrite each other.

#### Mutation log file

- **Path**: `rlogs/<PartitionMapUpdateVersion>/<partitionId>/`
- **Name**: `log,<beginVersion>,<endVersion>,<partitionId>,<randomUID>,<blockSize>`
  - `randomUID` and `blockSize` follow existing backup file naming conventions.
- **Contents**: `<partitionId>,<keyRange>` header followed by one or more data blocks of mutations in version order.

### Persistence

| Location | What | When cleaned |
|---|---|---|
| `txnStateStore` | Latest `PartitionMap` | When backup is stopped |
| Storage Server on-disk (`\xff\x02/backupPartitionMap`) | `PartitionMap` history `{epoch, version, PartitionMap}` | DD cleanup actor weekly; entries older than 30 days deleted (except the latest) |

All BackupWorkers attempt to write each PartitionMap-history entry, conditional on the entry not already existing — only one succeeds.

### Partitioning algorithm

DataDistributor performs partitioning because it already has authoritative shard metadata. The algorithm:

1. Query all shards from DD.
2. Use `StorageMetricSample.getEstimate()` for per-shard byte sizes.
3. Build `KNOB_BACKUP_NUM_OF_PARTITIONS` contiguous partitions, balancing total bytes across partitions.

Goals: balanced data per BackupWorker, balanced mutation log file sizes, avoid hotspots.

### Re-partitioning vs. re-distributing

Two operations to rebalance load:

| Operation | Effect | Issued by |
|---|---|---|
| **Re-partitioning** | Recompute the partition vector for the entire user keyspace | DD |
| **Re-distributing** | Move 1 or more partitions from an overloaded BackupWorker to a less loaded one (no keyspace recomputation) | CommitProxy |

Both can be triggered:

- **Manually**: via `fdbbackup modify --repartition` or `fdbcli`.
- **Dynamically**: BackupWorker 0, DD, or CommitProxy detect hotspots or imbalance and issue a request. Rate-limited; competing requests must be serialized.

Special keys used for coordination:

- `backupPartitionRequired`
  - Polled or watched by DD.
  - `1` = initial partition or manual/adaptive re-partition.
  - `2` = cleanup PartitionMap (issued by backup abort/stop).
  - `0` = cleared; ready for next request.
- `backupPartitionRequiredTag`
  - Used by CommitProxy to generate a new PartitionMap by moving partitions from the named BackupWorker tag to a random or underloaded one.

### CommitProxy routing of user mutations

For each mutation, CommitProxy looks up the relevant partition(s) from the PartitionMap and assigns the matching BackupWorker tag(s).

**ClearRange mutations spanning multiple partitions**:

- All relevant BackupWorker partition tags are attached (analogous to how SS tags are attached today).
- The mutation is **not split** at partition boundaries by CommitProxy. Splitting would generate `n` smaller mutations per original ClearRange, with memory, network, and CPU overhead on the proxies.
- The partition-boundary fix-up happens at restore time: each log file carries its PartitionMap, so restore can clip ClearRanges to the partition's key range when applying them.

### Recovery

On every cluster recovery:

- `txnStateStore` and BackupWorkers processing the current epoch receive the PartitionMap update as their first mutation.
- If the set of BackupWorker tags differs from the previous epoch, a new PartitionMap is generated. Otherwise, the existing PartitionMap from `txnStateStore` is re-broadcast.
- A new version directory is created in S3.

For BackupWorkers processing the **old** epoch's tail of mutations:

- Read the PartitionMap history from Storage Servers for the relevant version range.
- Apply the PartitionMap that was active when those mutations were generated.
- Each mutation batch is processed using the historically correct partition routing.

If there is a recovery between "uploading log file to S3" and "updating progress to DB", duplicate log files may exist. These are filtered by restore.

The PartitionMap update is also added to the recovery transaction (~1-3 MB). The headroom in the recovery transaction must be validated.

**Scenario 1**: Recovery without an intervening re-partition

- v=1: epoch 1 starts with PartitionMap PM(epoch=1, startVersion=1).
- v=100: recovery — epoch 2 starts with PM(epoch=2, startVersion=100).
- BackupWorkers lagging at v=80 must finish [80, 100) using PM(epoch=1, startVersion=1) read from SS history.

**Scenario 2**: Recovery after an intra-epoch re-partition

- v=1: PM(epoch=1, startVersion=1).
- v=90: re-partition — PM(epoch=1, startVersion=90).
- v=100: recovery — PM(epoch=2, startVersion=100).
- Lagging BackupWorker at v=80 processes:
  - `[80, 90)` using PM(epoch=1, startVersion=1)
  - `[90, 100)` using PM(epoch=1, startVersion=90)
- Each mutation batch uses the PartitionMap active at its generation time.

### Configuration: starting a v3 backup

Required:

- Worker processes with `class=backup`.
- `backup_worker_enabled := 1`.
- A start-backup request flagged for range-partitioned logs.

**Happy path**: BackupAgent flips `backup_worker_enabled := 1` if needed, posts `BackupCreateRangePartitions`, ClusterController triggers recovery, BackupWorkers are recruited post-recovery, and BackupAgent writes `backupPartitionRequired`, which kicks off the initialization flow.

**Scenario 1 (workers already running)**: BackupAgent skips the recovery step and goes straight to writing `backupPartitionRequired`. BackupWorkers idle on `peek(TLog)` until the first mutation arrives.

**Scenario 2 (no `class=backup` workers exist)**: ClusterController fails to recruit and asserts. BackupAgent's task fails.

**Scenario 3 (abort)**: `fdbbackup abort/stop` posts `BackupAbortRangePartitions`. BackupAgent writes `backupPartitionRequired = 2`. CommitProxy clears the PartitionMap in `txnStateStore` and broadcasts an empty PartitionMap. BackupWorkers close their in-progress log files, upload them, and idle on `peek(TLog)` until the next backup starts. CommitProxy stops routing mutations to BackupWorkers until a new partition request arrives.

### Restore comparison (v2 vs v3)

| Step | Backup v2 (partitioned logs) | Backup v3 (range-partitioned logs) |
|---|---|---|
| Find files | Scan `plogs/`, filter by version range | Scan only relevant key-range directories under `rlogs/` |
| Remove duplicates | Sort by tag, remove overlapping files | Sort by version per keyRange, remove overlapping files |
| Verify continuity | Check no version gaps across all tags | Check no version gaps within each key range |
| Create iterators | One per tag, 1 MB buffer | One per target key range, independent |
| Merge / order | Find min version across all tags **(bottleneck)** | Process each key range independently (no synchronization) |
| Apply | Write to `alog` keyspace, then to DB | Optionally write directly to DB, halving write amplification |
| Partial restore | Must read all data | Reads only relevant key-range data |

### Backup comparison (v2 vs v3)

| Aspect | Backup v2 | Backup v3 |
|---|---|---|
| LogRouter ↔ BackupWorker | 1:1, tightly coupled | Decoupled — independent scaling |
| Concept of Partition | None | Contiguous key range |
| Concept of PartitionMap | None | `BackupTag → vector<Partition>` |
| BackupWorker input | Random mutations matching its tag | n partitions worth of mutations, buffered per partition |
| Files in flight per worker | One log file appended at a time | Multiple — one per partition |
| TLog popping | Up to the latest version uploaded | `min(latest version uploaded across all partitions)` |
| CommitProxy mutation routing | Random LogRouter + BackupWorker tags | Random LogRouter; BackupWorker by PartitionMap |
| Backup size | Smaller (no ClearRange duplication) | Slightly larger (ClearRanges may duplicate across partitions) |
| Idle behavior | Peek-and-discard TLog when backup off | Wait on TLog peek until backup starts |
| Coordination key | `\xff\x02/backupStarted` (per-worker, different versions) | `\xff\x02/backupPartitionRequired` (synchronized at the same version via CommitProxy) |
| `BackupProgress` saved | `[epoch][tag] = savedVersion` | `[epoch][tag] = min(savedVersion across all partitions)` |
| Mutation log file content | Multiple data blocks | `partitionId, keyRange` header + data blocks |
| Mutation log file name | `log,<bv>,<ev>,<tagId>,<UID>,[N-of-M],<blockSize>` | `log,<bv>,<ev>,<partitionId>,<UID>,<blockSize>` |

### Capacity sizing

Worked example (illustrative; final values determined by experimentation):

| Parameter | Value |
|---|---|
| Cluster write rate | 30 MB/s |
| `KNOB_BACKUP_NUM_OF_PARTITIONS` | 100 |
| BackupWorker count | 10 |
| Partitions per BackupWorker | 10 |
| Per-BackupWorker write rate | 3 MB/s |
| Per-partition write rate | 0.3 MB/s |
| With 10s mutation buffering | ~3 MB per log file (avg) |

### Knob constraints

- `KNOB_BACKUP_NUM_OF_PARTITIONS` default ~100. Max ~500 (limited by the 10 MB max txn size; the PartitionMap update transaction must fit).
- Changes to the knob do not take effect until the next re-partition request.
- 100 partitions with max-size keys: ~1 MB for begin keys + ~1 MB for end keys ≈ 2 MB for the list, plus overhead. Recovery transaction headroom must accommodate this.

## Alternatives Considered

### S3 Layout Option 2: no PartitionMap history on SS

The chosen design (Layout Option 1) persists `PartitionMap` history on Storage Servers so BackupWorkers processing an old epoch can recover the correct routing for the lagging version range.

An alternative (Layout Option 2) avoids persisting PartitionMap history on SS entirely. Instead, BackupWorkers processing the old epoch during recovery write their data to an `EpochXXX/recovery_data/` directory without keyRange information:

```
rlogs/
  <Epoch>/
    <PartitionMapUpdateVersion>/
      <partitionId>_<tagId>/
        log,<bv>,<ev>,<partitionId>,<UID>,<blockSize>
    recovery_data/
      <EpochStartVersion>_<tagId>/
        log,<bv>,<ev>,<UID>,<blockSize>   # no keyRange — partition unknown
```

| Dimension | Layout 1 (chosen) | Layout 2 (alternative) |
|---|---|---|
| Extra `recovery_data/` directory | No | Yes (data without keyRange info) |
| File naming | Every file has version + keyRange | Most files do; `recovery_data/` does not |
| PartitionMap history persistence | On SS | Not persisted |
| Cleanup overhead | Yes (PartitionMap history) | No |
| Recovery time impact | Potential increase (TBD via experimentation) | No impact |
| Duplicate files | Possible, easily filtered | Possible + `recovery_data/` may overlap version directories |
| Restore complexity | Simpler | Needs additional synchronization to filter overlap |
| ClearRange mutation handling at CP | No split needed; restore handles boundaries | Must split at partition boundaries (CP must know partitions; recovery-time workers cannot, so restore cannot fix up) — adds CP memory, network, and CPU overhead |

**Decision: Layout 1.** Reasons:

- Cleaner layout in S3.
- No extra synchronization required during restore for `recovery_data` (a path that, while rare, would otherwise need careful overlap handling).
- ClearRange mutations remain unmodified on the CommitProxy hot path.

The PartitionMap history persistence cost (cleanup actor, on-disk bytes, recovery overhead) is judged acceptable in exchange. If experimentation shows unacceptable recovery-time impact, Layout 2 remains a fallback.

### Possible improvements not targeted for v3

Out of scope for this release but tracked as follow-ups:

- **Sub-partition indexing per mutation log file**: Each partition can be further subdivided into sub-partitions (e.g., 5-10). Mutations for each are buffered independently. Two encodings considered:
  - **Header index**: Each file starts with a sub-partition `keyRange → byte offset` map, followed by mutations.
  - **Per-partition metadata files**: Metadata `(sub-partitionKeyRange, versionRange) → (file, offset, length)` kept in memory and periodically flushed. Restore can reconstruct from log files alone if metadata is lost.
  - **Bitmask per part**: One bit per sub-partition indicates whether data exists. Enables quick filtering during restore with minimal IO.
- **Mutation log file checksums**.
- **Mutation log compression**.

## Testing Considerations

### Functional

- Partition integrity (no gaps, no overlaps).
- Mutation consistency end-to-end (round-trip a known dataset through backup + restore and diff).
- Partition boundary conditions (ClearRange across boundaries, single-key partitions, empty partitions).
- Re-partition correctness (data continuity across PartitionMap changes within an epoch).

### Fault injection and reliability

- BackupWorker failures during upload.
- Partial S3 uploads (network drop mid-write).
- TLog replay failures.
- Cluster recoveries at various points in the upload pipeline.
- Recovery scenarios 1 and 2 from the Recovery section above.
- Concurrent re-partition + recovery.

### Test surfaces

- New simulation tests for partition assignment, recovery, and re-partition flows.
- CTest updates for end-to-end backup-restore with v3.
- Performance tests on large clusters for cluster-scale validation.

## Observability/Supportability Considerations

### Metrics

Extend existing backup metrics and add new metrics specific to v3:

- Active PartitionMap version per BackupWorker.
- Per-partition upload lag (version and bytes).
- Per-BackupWorker partition count and assigned key-range size.
- Re-partition / re-distribute event rate, latency, and reason (manual vs. dynamic, source workload).
- Recovery PartitionMap history read latency.
- PartitionMap history size on Storage Servers.
- `recovery_data`-style duplicate file count seen during restore (should be near zero in Layout 1).

### Alerts

- New alert: any BackupWorker lagging > N versions behind its peers (indicates partition imbalance or worker degradation).
- New alert: PartitionMap history size on SS exceeds budget.

### Tooling

- `fdbbackup`, `fdbrestore` commands updated for v3.
- `fdbdecode` updated to parse v3 mutation log files.
- New CLI to dump partition metadata (PartitionMap, history, per-partition file counts) for debugging.

## Rollout/Migration Considerations

### Backward compatibility

- v3 is selected per-backup via a start flag. Existing backups continue to use v2 layout (`plogs/`) and v2 restore path.
- Restore tooling auto-detects layout by presence of `plogs/` vs. `rlogs/` and uses the corresponding restore engine.
- v1 and v2 backups remain restorable indefinitely — no on-disk data is migrated or rewritten.

### Rollout plan

1. Deploy code with v3 paths gated by feature flag / per-backup opt-in.
2. Enable v3 on a small canary cluster; validate restore round-trip.
3. Tune `KNOB_BACKUP_NUM_OF_PARTITIONS` and partitions-per-worker via performance tests on large clusters.
4. Expand to a larger cluster for endurance testing (multi-day backup with recoveries and re-partitions).
5. General rollout cluster-by-cluster.

### Rollback

A v3 backup cannot be rolled back to v2 mid-flight (the on-disk layout is different). Rollback procedure: stop the v3 backup, start a new v2 backup. Restorability of the partially completed v3 backup remains intact as long as a snapshot exists.

### Experimental decisions

The following parameters are interdependent and require tuning on real clusters before general rollout:

- Number of BackupWorkers.
- `KNOB_BACKUP_NUM_OF_PARTITIONS`.
- Partitions per BackupWorker.
- Mutation log file size target.
- BackupWorker version lag budget.
- BackupWorker memory budget.
- BackupWorker upload delay.

The partitioning scheme must produce balanced load. Imbalance creates hotspots and unbalanced mutation log file sizes, which directly degrade restore throughput.

### Success criteria

- **Correctness**: Restored data matches original at the target version (bit-for-bit via round-trip test).
- **Performance**: Mutation-log replay completes meaningfully faster than the current implementation — targeting a 3x-5x improvement in replay throughput.
- **Reliability**: Meets FDB continuous backup & restore reliability standards — no loss of mutation logs across induced failures.
