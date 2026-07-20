# SHARD_ENCODE_LOCATION_METADATA Feature Description

## What It Is

A feature flag (`SHARD_ENCODE_LOCATION_METADATA`) that enriches FDB's internal
location metadata with persistent shard identities. It transitions the Data
Distributor from reasoning about "which servers own which key ranges" to reasoning
about "named shards with persistent identity."

Default: `false` in production. Enabled 75% of the time in simulation.

## Two Parallel Data Move Paths

| Aspect | Old (`SHARD_ENCODE=false`) | New (`SHARD_ENCODE=true`) |
|--------|---------------------------|--------------------------|
| Entry point | `startMoveKeys` / `finishMoveKeys` | `startMoveShards` / `finishMoveShards` |
| keyServers value | `{srcTag[], destTag[]}` | `{src[], dest[], srcId, destId}` |
| serverKeys value | `serverKeysTrue` / `serverKeysFalse` | `serverKeysValue(shardId)` — actual shard UID |
| Persistent move state | None | `\xff/dataMoves/<id>` = DataMoveMetaData |
| Resume after DD restart | No (moves restart from scratch) | Yes (reads DataMoveMetaData) |
| Checkpoint support | No | Yes (checkpoint IDs stored in DataMoveMetaData) |
| Bulk load support | No | Yes (dataMoveId links to separate bulkLoadTask key space) |
| Physical shard moves | No | Yes (via ENABLE_DD_PHYSICAL_SHARD) |
| Data move auditing | No | Yes (AUDIT_DATAMOVE_PRE/POST_CHECK) |
| Per-range replication (large teams) | Yes (DD_MAX_SHARDS_ON_LARGE_TEAMS) | Disabled |
| Dispatch | `rawStartMovement()` → old path | `rawStartMovement()` → new path |

All three dispatch points (`rawStartMovement`, `rawCheckFetchingState`,
`rawFinishMovement`) branch on the knob to select old vs new path.

Both paths use the same KRM primitives (`krmSetPreviouslyEmptyRange` for
keyServers, `krmSetRangeCoalescing` for serverKeys). The difference is in the
value encoding and the existence of DataMoveMetaData.

**Coalescing behavior differs between paths.** In the old path, `serverKeys`
values are boolean constants (`serverKeysTrue` / `serverKeysFalse`). Adjacent
shards on the same SS with the same `true` value coalesce into a single KRM
entry. In the new path, `serverKeys` values encode unique `dataMoveId` UIDs,
and adjacent shards almost always have different UIDs (from different moves).
This prevents KRM coalescing, keeping each shard as a separate KRM entry — see
`unassignServerKeys()` at `MoveKeys.cpp:74` which explicitly works around
`krmSetRangeCoalescing`'s inability to handle per-shard unique values.

**This boundary enforcement is critical for bulkload.** Bulkload maps physical
files to specific shard ranges (`DataMoveType::LOGICAL_BULKLOAD`). Without
enforced boundaries, a coalesced KRM entry could span multiple file sets,
making it impossible to determine which files belong to which shard. The fetch
key data movement path does not require this — it fetches data per logical
range regardless of KRM entry boundaries — but bulkload needs exact per-shard
boundaries because it loads data from physical file sets rather than from
source servers.

**Downstream effect of no coalescing:** In the old path any code reading
`serverKeys` saw a compact KRM (few entries per SS), so small reads with
`krmGetRanges` were sufficient. In the new path the KRM has one entry per
shard per SS, so reads iterate more entries and move operations write more
boundaries per transaction — reflected in the 10× higher KRM row limit
(`MOVE_SHARD_KRM_ROW_LIMIT=20000` vs `MOVE_KEYS_KRM_LIMIT=2000`).

### keyServers encoding change

The old format stores **Tags** — small TLog stream identifiers (locality + id).
To find the actual server UID for a Tag, you must join against `\xff/serverTag/`
(the UIDtoTagMap). Every read of keyServers in the old path requires this extra
range read to resolve Tags to UIDs.

The new format stores **UIDs directly** — server identifiers are self-contained
in the value. No join required. Additionally, the new format includes `srcId` and
`destId` (shard/move identifiers) that link the entry to its DataMoveMetaData.

## Architecture (New Path)

```
+-----------------------------------------------------------+
|                  Data Distributor (DD)                    |
|                                                           |
|  +---------------+       +---------------+                |
|  | DD Scheduler  |------>| DD Relocator  |                |
|  | (picks what   |       | (executes     |                |
|  |  to move)     |       |  the move)    |                |
|  +---------------+       +-------+-------+                |
+-----------------------------------|-----------------------+
                                    |
                                    v
+-----------------------------------------------------------+
|                    MoveKeys Layer                         |
|                                                           |
|  +------------------+          +-------------------+      |
|  | startMoveShards  |--------->| finishMoveShards  |      |
|  |                  |          |                   |      |
|  | Writes (1 txn):  |          | Writes (1 txn):  |       |
|  |  - DM metadata   |          |  - keyServers:    |      |
|  |  - keyServers:   |          |    merge dest->src|      |
|  |    set destId    |          |    clear destId   |      |
|  |  - serverKeys:   |          |  - serverKeys:    |      |
|  |    assign range  |          |    unassign old   |      |
|  |    to dest SS    |          |  - delete DM meta |      |
|  +------------------+          +-------------------+      |
|                                                           |
|  All writes are system keys in the FDB database.          |
|  SS learns about moves by processing serverKeys           |
|   mutations from the mutation log (push-based).           |
|                                                           |
|  DM metadata = DataMoveMetaData in \xff/dataMoves/<id>    |
+-----------------------------------------------------------+
                                    |
                                    v
+-----------------------------------------------------------+
|              Storage Servers (SS)                         |
|                                                           |
|  Source SS                         Dest SS                |
|  +----------+                      +----------+           |
|  | Has data |------ fetchKeys ---->| Fetching |           |
|  | (in src) |      (or checkpoint) | (in dest)|           |
|  +----------+                      +----------+           |
+-----------------------------------------------------------+
```

## Key Spaces

```
\xff/keyServers/<key>      = { src[], dest[], srcId, destId }
\xff/serverKeys/<ssId>/<k> = { assigned, shardId, moveType }
\xff/dataMoves/<moveId>    = DataMoveMetaData
\xff/bulkLoadTask/<range>  = BulkLoadTaskState (for bulk load moves)
```

## DataMoveMetaData

Serialized with `ObjectWriter` + `IncludeVersion()` (versioned, evolvable).

```
+-----------------------------------+
| DataMoveMetaData                  |
+-----------------------------------+
| id:             UID               |
| ranges:         vector<KeyRange>  |
| phase:          Running|Deleting  |
| priority:       int               |
| src:            set<UID>          |
| dest:           set<UID>          |
| checkpoints:    set<UID>          |
| mode:           int8              |
| bulkLoadTaskState: Optional<...>  |
+-----------------------------------+
```

## dataMoveId Structure

Generated by `newDataMoveId()` in DDRelocationQueue (upstream of startMoveShards):

```
+----------------------------------+----------------------------------+
|         First 64 bits            |         Second 64 bits           |
+----------------------------------+----------------------------------+
|       physicalShardId            | [63:16] random                   |
|       (random)                   | [15:8]  DataMovementReason       |
|                                  | [7:0]   DataMoveType             |
+----------------------------------+----------------------------------+

DataMoveType: LOGICAL, LOGICAL_BULKLOAD, PHYSICAL_BULKLOAD
Sentinel: anonymousShardId = UID(0x666666, 0x88888888)
```

SS decodes the DataMoveType from the dataMoveId to determine fetch behavior.

## Move Lifecycle

```
  startMoveShards                              finishMoveShards
       |                                             |
       v                                             v
+------------------+    SS fetches data    +------------------+
| In Progress      |---------------------> | Complete         |
|                  |                       |                  |
| keyServers:      |                       | keyServers:      |
|   src  = [A, B]  |                       |  src  = [A,B,C,D]|
|   dest = [C, D]  |                       |  dest = []       |
|   destId = X     |                       |  destId = 0      |
|                  |                       |                  |
| dataMoves/X:     |                       | dataMoves/X:     |
|   phase=Running  |                       |   (deleted)      |
|   dest=[C,D]     |                       |                  |
+------------------+                       +------------------+
```

## startMoveShards Steps

1. Receives `dataMoveId` from DD relocator (already generated upstream)
2. Write DataMoveMetaData to `\xff/dataMoves/<dataMoveId>`
   - Records range, src servers, dest servers, phase=Running
3. Update keyServers for each shard in range:
   - Set `dest = [new servers]`, `destId = dataMoveId`
   - Keeps `src` unchanged (still serving reads)
4. Update serverKeys: write `\xff/serverKeys/<destSS>/<range> = serverKeysValue(dataMoveId)`
   - This is a normal DB write in the same transaction
   - SS picks it up later via mutation log
5. Commit (single atomic transaction)

## finishMoveShards Steps

1. Read DataMoveMetaData from `\xff/dataMoves/<dataMoveId>`
   - Gets dest servers from here (NOT from keyServers)
2. Update keyServers for each shard:
   - Merge: `src = src + dest`
   - Clear: `dest = []`, `destId = 0`
3. Update serverKeys: mark old src-only servers for removal
4. Delete DataMoveMetaData entry
5. Commit

## How Storage Servers Work in Each Path

SS involvement is entirely **push-based via the mutation log** in both paths.
The SS never polls DD. The mechanism is identical; only the serverKeys value
encoding differs.

### Old path: SS behavior

serverKeys values are boolean constants (`serverKeysTrue` / `serverKeysFalse`).

- SS sees `serverKeys/<myId>/<range> = serverKeysTrue` → start `fetchKeys`
- SS sees `serverKeys/<myId>/<range> = serverKeysFalse` → cancel fetch, drop range
- If SS already has the range and sees `true` again → no-op (idempotent)

There is no move identity. Every write is idempotent. DD doesn't "retry" — it
just overwrites. If DD restarts and picks the same dest, the `serverKeysTrue`
write is already there (no-op). If it picks a different dest, old dest gets
`false` (cancel), new dest gets `true` (start).

The SS cannot distinguish "continue existing fetch" from "start a new fetch for
the same range." It doesn't need to — without bulk load, all fetches pull the
same data from src servers regardless of which DD lifetime initiated them.

### New path: SS behavior

serverKeys values encode a UID containing the dataMoveId (which carries the
DataMoveType in its lower bits).

- SS sees `serverKeys/<myId>/<range> = UID` → decode type from UID bits
  - If type is LOGICAL: start normal `fetchKeys` from src
  - If type is LOGICAL_BULKLOAD: look up bulk load task, download from S3
  - If type is PHYSICAL_BULKLOAD: look up checkpoint, restore from SST files
- SS sees `serverKeys/<myId>/<range> = serverKeysFalse` → cancel, same as old path

The dataMoveId lets the SS distinguish:
- **Same ID as current fetch** → retry/continuation, keep going
- **Different ID** → new task, restart fetch from scratch

This distinction is critical for bulk load. The S3 path lives in the
`BulkLoadTaskState` persisted at `\xff/bulkLoadTask/<range>` and is
fixed for the lifetime of a given dataMoveId — DD writes it once when
the move is created, SS reads it. So:

- DD restart that re-issues the **same** dataMoveId → SS sees the same
  task, continues from the same path (no restart).
- DD issuing a **different** dataMoveId for the same range (a new bulk
  load) → new `BulkLoadTaskState`, possibly with a different S3 path.
  The change in dataMoveId is the signal to the SS to restart the
  fetch from the new path.

## Bulk Load Integration

Bulk load piggybacks on the data move machinery:

1. DD relocator generates a `dataMoveId` with `DataMoveType::LOGICAL_BULKLOAD`
2. `startMoveShards` writes DataMoveMetaData and bulk load task state to
   `\xff/bulkLoadTask/<range>` with the dataMoveId embedded
3. The serverKeys mutation carries the dataMoveId to the dest SS
4. Dest SS decodes the dataMoveId, sees LOGICAL_BULKLOAD type
5. SS reads DataMoveMetaData to confirm, then looks up bulk load task
6. SS downloads data from the bulk load source (e.g. S3)

The dataMoveId links all three: keyServers entry, DataMoveMetaData, and bulk
load task state.


## Physical Shard Moves (ENABLE_DD_PHYSICAL_SHARD)

Instead of SS fetching data key-by-key:

1. `startMoveShards` creates a `CheckpointMetaData` entry per shard
   - Stores checkpoint ID in `dataMove.checkpoints` (inside DataMoveMetaData)
2. Source SS creates a RocksDB checkpoint (SST files)
3. Dest SS reads DataMoveMetaData to find checkpoint IDs
4. Dest SS restores from checkpoint (orders of magnitude faster than fetchKeys)
5. Requires `RESUME_DATA_MOVES_ON_RESTART=true` (checkpoints must survive DD restart)

## Encoding Details

keyServers values are encoded with `BinaryWriter` using
`ProtocolVersion::withShardEncodeLocationMetaData()` (`0x0FDB00B072000000`).
The protocol version is embedded in each serialized value, so the decoder
detects format per-value (not per-knob-setting). This enables mixed-format
coexistence during rollout.

At database creation, `SeedShardServers.cpp` generates initial shard IDs and
writes them into both `\xff/keyServers/` and `\xff/serverKeys/` in the new format.

## Features Gated on This Knob

Requires `SHARD_ENCODE_LOCATION_METADATA=true`:
- `ENABLE_DD_PHYSICAL_SHARD` (physical shard moves)
- BulkLoad / BulkDump
- `AUDIT_DATAMOVE_PRE_CHECK` / `AUDIT_DATAMOVE_POST_CHECK`
- Sharded RocksDB storage engine

Disabled when `SHARD_ENCODE_LOCATION_METADATA=true`:
- Per-range replication ("large teams"): allows specific key ranges to have a higher
  replication factor than the cluster default (e.g. range X gets 5 replicas when
  cluster is configured for 3). DD builds oversized teams for those ranges. Not
  supported by the shard-encoded path which assumes uniform team size.

Enabling on an existing cluster works incrementally — new moves write new-format
entries, old entries remain readable. A **storage wiggle** rewrites all entries
into new format, which is needed only for full coverage of features that require
shard IDs on every entry (auditing, bulk load). Basic DD operation works without it.

## Rollout and Rollback

### Rollout (enable new format)

Safe — the new binary reads both formats. Mixed-format coexistence works because
the decoder checks the protocol version embedded in each value. Enable via
`configure shard_metadata_format=encoded` (or, on a cluster that has not set
the config, the `SHARD_ENCODE_LOCATION_METADATA=true` knob fallback).

### Rollback within same binary (config-driven)

Safe — tested and verified in simulation (`ShardEncodeRollback.toml` for the
knob-fallback path, `ShardEncodeRollbackConfig.toml` for the config-driven
path).

**Migration is optional.** Setting the target back to old format
(`configure shard_metadata_format=original`, no restart) is a
zero-additional-work operation from the cluster's perspective:

- The cluster keeps running fine after the target change. DD's decoders
  handle both old and new formats; SS's `applyPrivateData` accepts
  both.
- DD writes old-format for every new shard move going forward.
- Existing new-format entries linger. `audit_storage
  metadata_encoding` reports `MIGRATION IN PROGRESS` indefinitely.

The only thing this mixed state prevents is **downgrading the FDB
binary**. Old FDB versions cannot decode new-format `serverKeys`
values and will misbehave. To make downgrade safe, all entries must
be drained to old format — reflected in `audit_storage
metadata_encoding` returning `ROLLBACK COMPLETE — safe to downgrade
binary`.

Operators pick from three paths to reach `ROLLBACK COMPLETE`:

1. **Do nothing beyond setting the target.** Natural DD moves gradually
   drain entries over time. Timeframe unbounded — could be days or
   weeks on a quiescent cluster. Fine if you don't need to downgrade
   on a schedule.

2. **Active rewrite (this doc's remaining subsections).** Opt in via
   the two DatabaseConfiguration options. Bounded — scales with cluster
   size (the serverKeys drain is serial per SS): minutes on typical
   clusters, potentially hours on a very large (~250k-shard) cluster.
   These are estimates. Progress is observable via DD trace events and
   `audit_storage metadata_encoding` (see the serverKeys subsection).

3. **Storage wiggle.** Enable perpetual storage wiggle. Every SS
   rotates through the cluster, causing every shard to move at least
   once. Slow (hours-to-days) but exercises only pre-existing code
   paths. Recommended when active rewrite is too aggressive for
   safety envelope.

The active rewrite path described below is **opt-in per operator event**
via two database configuration options that mirror the
`storage_engine` + `perpetual_storage_wiggle` pattern:

```
fdbcli> configure shard_metadata_format=original
fdbcli> configure shard_metadata_migration=enabled
```

- `shard_metadata_format` = the target format DD converges existing
  entries toward. When unset, DD falls back to the
  `SHARD_ENCODE_LOCATION_METADATA` knob (`true → encoded`,
  `false → original`).
- `shard_metadata_migration` = whether DD actively runs the rewrite
  pass at init. When unset or `disabled` (default), DD does nothing at
  init and both directions converge via natural DD moves or a manual
  storage wiggle (unbounded timescale).

Deploying a binary with this feature onto an existing cluster is a
byte-identical no-op unless the operator explicitly enables migration.

When migration is enabled, the safety properties below apply:

1. **Decoders handle both formats.** SS processes both old-format and shard-encoded
   serverKeys mutations correctly regardless of its persistent `shardAware` flag.
   No data loss from format mismatch.

2. **DD re-inits on a config change.** A `configure shard_metadata_format=...`
   (or `shard_metadata_migration=...`) change triggers a recovery that
   re-elects DD; the new DD resolves the effective target and runs the
   startup rewrite. No knob change or process restart is required (the knob
   is only the fallback when the config is unset). In-flight actors from the
   old DD are cancelled.

3. **DD rewrites keyServers on startup.** When DD re-inits with the effective
   target = `original` and migration=enabled, it scans keyServers entries and
   rewrites shard-encoded ones to old tag-based format. DataMoveMetaData
   entries are cleared.

4. **serverKeys are rewritten on startup.** DD scans each SS's serverKeys
   KRM and rewrites shard-encoded ranges back to old-format constants
   using `krmSetRangeCoalescing` — the same primitive natural moves
   use. This preserves the two KRM invariants (no fragmentation because
   coalescing merges adjacent same-value entries; paired `[begin, end)`
   mutations because that is what `krmSetRangeCoalescing` emits, matching
   what `applyPrivateData` on the SS expects). Unlike the keyServers
   rewrite (step 3), the serverKeys rewrite is not paginated across
   DD-init re-invocations: a single invocation drains every SS's
   serverKeys KRM (looping over the serverList until it stabilizes),
   then writes the completion sentinel. It blocks DD init for the
   duration of the drain, and the serverKeys rewrite processes storage
   servers **serially** (``co_await`` per SS, each SS re-scanned until a
   pass rewrites nothing), so the time scales with cluster size:
   (# SSes) × (shard-encoded ranges per SS). On typical clusters this is
   minutes (e.g. a 12-SS live-cluster test drained within minutes); on a
   very large (~250k-shard) cluster it could be substantially longer —
   order of hours. These are estimates, not measured bounds. It is a
   rare, opt-in, per-rollback (``shard_metadata_migration=enabled``)
   event.

   Progress is observable while it runs: DD emits ``DDShardEncodeRewriteBegin``,
   per-SS ``DDShardEncodeRollbackPhase3SSStart`` / ``...Phase3SSDone``, and
   ``DDShardEncodeRewriteComplete`` (with ``Phase3Rewrites`` / ``Phase3Passes``
   / ``Phase3ElapsedSec``) trace events; operators can also poll
   ``fdbcli> audit_storage metadata_encoding`` (new-format counts trending to
   zero) and ``fdbcli> location_metadata physicalshards`` (physical shards
   trending to zero).

   Direct
   `tr.set()` at each entry — an earlier design that this section used
   to warn against — would produce fragmentation and violate the SS
   pair-processing invariant; the current implementation avoids both
   by routing through the same KRM primitive natural moves use.

5. **Safety net for rolling restarts.** During a rolling restart, an old DD
   instance (not yet killed) may find its DataMoveMetaData cleared by a new DD
   instance that already ran the startup rewrite. In this case, MoveKeys functions
   throw `dd_config_changed` instead of asserting — causing the old DD to restart
   once more and pick up the new knob value. This is at most one extra restart
   per DD instance during the transition, not per-shard.

6. **Sentinel-based idempotency.** DD writes a completion sentinel key
   (`\xff/dd/shard_encode_migration_complete = "old"`) at the end of a
   successful rewrite pass. Subsequent DD inits read the sentinel and
   fast-path skip — steady-state cost when migration is enabled is one
   key read per DD init. On forward-direction knob flips
   (`shard_metadata_format=encoded` or knob=true), DD clears the
   stale sentinel so subsequent rollback events don't fast-path skip
   incorrectly. If DD dies mid-rewrite, the sentinel remains absent →
   the next DD does a full scan and converges.

The rollback procedure (config-driven, no knob flip, no restart):
1. `fdbcli> configure shard_metadata_format=original shard_metadata_migration=enabled`
2. Force DD to re-init so it picks up the new configuration
   immediately (the configure in step 1 already triggers a
   recovery; this only expedites it):

       fdbcli> datadistribution off
       fdbcli> datadistribution on

   The newly-elected DD resolves `effective = original` from the
   config and runs the rewrite pass. No process restart and no knob
   flip is needed — DD's write paths follow the config target.
3. On the DD's next init, keyServers entries and serverKeys entries
   are both rewritten to old format, and DataMoveMetaData is
   cleared. The keyServers rewrite is paginated (up to 1000 entries
   per DD-init pass, re-invoking until the prefix is covered); the
   serverKeys rewrite then drains all storage servers within a
   single DD-init invocation before the completion sentinel is
   written.
4. DD proceeds with old path for all new moves.
5. Verify via `fdbcli> audit_storage metadata_encoding` returning
   `ROLLBACK COMPLETE`.

Alternative rollback procedure (drain via storage wiggle, no
configure needed):
1. Set `SHARD_ENCODE_LOCATION_METADATA=false` and restart processes.
2. Leave `shard_metadata_migration` unset or `disabled`.
3. Enable perpetual storage wiggle. Every SS rotates through,
   causing DD to rewrite every shard's metadata in the current
   (old) format as part of natural moves.
4. Verify via `audit_storage metadata_encoding`. Reaches
   `ROLLBACK COMPLETE` over hours-to-days depending on cluster
   size and wiggle throttling.

Because DD rewrites serverKeys directly on init when migration is
enabled (using the same KRM primitive natural moves use — see step 4
in the previous section), storage wiggle is not required to reach
`ROLLBACK COMPLETE` on a quiescent cluster. Both paths are
supported; operators pick based on how quickly they need
`ROLLBACK COMPLETE`.

### Config is the source of truth; the knob is the fallback

DD resolves its effective encoding target once at init:

```
effective = shard_metadata_format.present()
                ? shard_metadata_format
                : SHARD_ENCODE_LOCATION_METADATA   // knob fallback
```

`shard_metadata_format` (configure option) is authoritative for **all** of
DD's decisions — both the migration decision at init and the write paths
(MoveKeys, natural moves, failed-server / dataMove cleanup). The
`SHARD_ENCODE_LOCATION_METADATA` knob is consulted **only as the fallback**
when `shard_metadata_format` is UNSET.

DD does **not** write the config. It re-resolves the effective target on every
init, and a `configure shard_metadata_format=...` change triggers a recovery
that re-elects a DD which reads the new value. So nothing needs to be "kept in
sync" on a running cluster and there is no self-seeding write (which would
force an extra recovery on upgrade): just set the config and DD converges.
Deploying the binary is a byte-identical no-op until an operator sets the
config.

**Two DD features stay gated on the raw knob, not the config target — a
known limitation.** Physical shard moves (`ENABLE_DD_PHYSICAL_SHARD`) and
per-range replication / "large teams" (`ddLargeTeamEnabled`, i.e.
`DD_MAX_SHARDS_ON_LARGE_TEAMS > 0 && !SHARD_ENCODE_LOCATION_METADATA`) are
**mutually exclusive with shard-encoded metadata** — they operate only in the
old (tag-based) format. They still read the `SHARD_ENCODE_LOCATION_METADATA`
knob directly rather than the resolved `shard_metadata_format` target. That is
a deliberate trade-off, not a full solution:

- Gating them on `shard_metadata_format` would be *worse*: the config flips
  instantly but the metadata drains asynchronously, so they would switch on
  the moment `shard_metadata_format=original` is set — while encoded entries
  still exist — violating the mutual exclusion during the active rollback.
  Gating on the knob (which only changes via a process restart) avoids that.
- But the knob gating is not fully safe either, once config and knob can
  disagree. Two consequences:
  - After a **config-only** rollback (config=old, knob still `true`), large
    teams / physical shard stay **disabled** even at `ROLLBACK COMPLETE`. To
    re-enable them, redeploy with `SHARD_ENCODE_LOCATION_METADATA=false` (the
    same knob=false deploy a binary downgrade needs — see the downgrade
    contract below).
  - Conversely, a contradictory `SHARD_ENCODE_LOCATION_METADATA=false` flip
    while `shard_metadata_format=encoded` would **wrongly enable** large
    teams / physical shard on encoded metadata.
- Impact is low in practice: large teams is rarely used, physical shard is
  experimental (off by default), and the bad case requires an operator
  deliberately contradicting the config with the knob. The correct fix (gate
  these on the fully-drained old-format state) is deferred.

**Operational rule:** once you set `shard_metadata_format`, do not move the
`SHARD_ENCODE_LOCATION_METADATA` knob except as part of a downgrade
(knob=false on a fully rolled-back cluster). Config drives the *encoding*; the
knob gates the mutually-exclusive *old-format-only features*.

Neither of these affects the keyServers/serverKeys encoding or the
rollback/downgrade contract.

### Rollback to old binary (downgrade)

NOT safe if shard-encoded values exist. Old binary cannot parse new-format
serverKeys values. Requires DD's rewrite to complete first — verify via
`audit_storage metadata_encoding` reporting `ROLLBACK COMPLETE`.

**Downgrade contract (config-only clusters).** A pre-config binary cannot read
`shard_metadata_format`; it decides encoding **solely from the
`SHARD_ENCODE_LOCATION_METADATA` knob**. Config-only convergence leaves the
knob untouched (it may still be `true`). So a binary downgrade is safe only
when **both**:

1. the cluster is already in unencoded (old) metadata (`ROLLBACK COMPLETE`), and
2. the downgrade-target binary is deployed with `SHARD_ENCODE_LOCATION_METADATA=false`
   in its command-line / knob config.

Condition 2 is natural — a binary downgrade is itself a redeploy/restart, so
you set the knob on the target binary at that moment; no live knob change on
the running config-only binary is needed. If you skip it, the old binary boots
with `knob=true` and resumes new-format writes, re-corrupting the metadata the
config-driven rollback just drained. (Downgrades *among* config-aware binaries
are unaffected — they all read `shard_metadata_format`.)

### Migration for downgrade

1. Converge to old format via config, no restart:
   `fdbcli> configure shard_metadata_format=original shard_metadata_migration=enabled`
   (optionally `datadistribution off; on` to expedite). DD rewrites both
   keyServers and serverKeys to old format on init; DataMoveMetaData is cleared.
2. Verify completion: `fdbcli> audit_storage metadata_encoding` reports
   `ROLLBACK COMPLETE — safe to downgrade binary`. Large clusters may
   require several DD init passes to fully drain; the tool's status
   line shows progress.
3. Downgrade the binary, deploying it with `SHARD_ENCODE_LOCATION_METADATA=false`
   (see the downgrade contract above).

### Verifying migration state

Two complementary tools:

**`fdbcli> location_metadata physicalshards`** — quick check. Shows total shards
vs physical (shard-encoded) shards. During forward migration, physical shards
increase toward total. During rollback, they decrease toward zero. Fast,
lightweight, no scanning required.

**`fdbcli> audit_storage metadata_encoding`** — definitive check. Scans all
keyServers and serverKeys entries, classifies each by protocol version header,
counts dataMoves entries. Reports one of four states:

- **`FORWARD COMPLETE`** — every keyServers and serverKeys entry is in the
  new (UID-encoded) format. Forward migration has finished; the cluster is
  fully on shard-encoded location metadata.
- **`MIGRATION IN PROGRESS`** — both formats coexist (mixed). The
  cluster is mid-migration but the snapshot can't tell direction from
  the keyspace data alone — the operator who flipped the knob already
  knows whether it's forward (toward shard-encoded) or back (toward
  old format). Status line shows new-format counts so progress is
  visible in either direction.
- **`ROLLBACK COMPLETE`** — every entry is in the old format and
  `\xff/dataMoves/` is empty. Safe to downgrade the binary. Also the
  state for a cluster that has never enabled the knob.
- **`NOT STARTED`** — every entry is old format but `\xff/dataMoves/`
  is non-empty. Transient state seen briefly right after the knob is
  flipped on (moves in flight before any keyServers entries have been
  rewritten).

Use `location_metadata physicalshards` for routine monitoring (is migration
progressing?). Use `audit_storage metadata_encoding` for the final gate decision
(is it safe to downgrade?).

### Rollout/rollback summary

```
                     SAFE                         REQUIRES MIGRATION
                      |                                  |
  Old binary ----[upgrade]----> New binary ----[enable knob]----> Shard-encoded
                      |              |                                  |
                      |         [flip knob off]                   [drain + verify]
                      |              |                                  |
                      |              v                                  v
                      |         New binary (old path)              New binary (old path)
                      |              |                                  |
                      |              |                            [downgrade]
                      |              |                                  |
                      |              v                                  v
                      +--------> Old binary <--------------------------+
```

## Verifying Migration State: ValidateMetadataEncoding Audit

Extends the existing `audit_storage` fdbcli command with a new AuditType that
reports encoding format state across keyServers and serverKeys. Used to confirm
both forward migration completion and rollback readiness. Runs client-side
(immediate scan, not a distributed audit).

### Usage

```
fdbcli> audit_storage metadata_encoding
keyServers: 45021 entries
  Original format (tag-based): 0
  Encoded format (UID-based): 45021
serverKeys: 123004 entries
  Original format (constants): 0
  Encoded format (UID-encoded): 123004
dataMoves: 3 entries (in-progress moves)

Migration status: FORWARD COMPLETE
```

After rollback (target set to `original`, drain in progress):

```
fdbcli> audit_storage metadata_encoding
keyServers: 45021 entries
  Original format (tag-based): 44800
  Encoded format (UID-based): 221
serverKeys: 123004 entries
  Original format (constants): 122500
  Encoded format (UID-encoded): 504
dataMoves: 0 entries

Migration status: MIGRATION IN PROGRESS (mixed format: 221 encoded keyServers, 504 encoded serverKeys)
```

When all zeros:

```
Migration status: ROLLBACK COMPLETE — safe to downgrade binary
```
