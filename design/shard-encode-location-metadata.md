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
| version:        Version           |
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

This distinction is critical for bulk load: if DD restarts and re-issues the same
bulk load, SS must continue (not restart). If DD issues a DIFFERENT bulk load for
the same range, SS must restart with the new S3 path.

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

### Rollout (enable knob)

Safe — the new binary reads both formats. Mixed-format coexistence works because
the decoder checks the protocol version embedded in each value.

### Rollback within same binary (flip knob false)

Safe — tested and verified in simulation (`ShardEncodeRollback.toml`):

1. **Decoders handle both formats.** SS processes both old-format and shard-encoded
   serverKeys mutations correctly regardless of its persistent `shardAware` flag.
   No data loss from format mismatch.

2. **DD restarts on knob change.** In production, changing the knob requires a
   process restart. On restart, DD initializes with the new value and runs the
   startup rewrite. In-flight actors from the old DD are cancelled.

3. **DD rewrites keyServers on startup.** When DD restarts with knob=false, it
   scans keyServers entries and rewrites shard-encoded ones to old tag-based format.
   DataMoveMetaData entries are cleared.

4. **serverKeys drains naturally.** Shard-encoded serverKeys entries are NOT
   rewritten on startup (doing so causes KRM fragmentation and SS pair-processing
   issues). They remain readable in both formats and drain to old format as DD
   moves shards using the old path.

5. **Safety net for rolling restarts.** During a rolling restart, an old DD
   instance (not yet killed) may find its DataMoveMetaData cleared by a new DD
   instance that already ran the startup rewrite. In this case, MoveKeys functions
   throw `dd_config_changed` instead of asserting — causing the old DD to restart
   once more and pick up the new knob value. This is at most one extra restart
   per DD instance during the transition, not per-shard.

The rollback procedure:
1. Set `SHARD_ENCODE_LOCATION_METADATA=false`
2. Restart the `fdbserver` processes (knob change requires restart)
3. On DD init, keyServers entries are rewritten to old format and DataMoveMetaData is cleared
4. DD proceeds with old path for all new moves
5. serverKeys entries drain to old format as DD touches shards over time

No need to stop writes or trigger wiggle.

### Rollback to old binary (downgrade)

NOT safe if shard-encoded values exist. Old binary cannot parse new-format
serverKeys values. Requires a migration step (drain all entries to old format)
before downgrade.

### Migration for downgrade

1. Set knob false, restart (DD uses old path, writes old format)
2. Run background rewriter or trigger storage wiggle to touch all shards
3. Verify completion: scan keyServers/serverKeys, confirm zero shard-encoded entries
4. Clear `\xff/dataMoves/` range
5. Safe to downgrade binary

### Verifying migration state

Two complementary tools:

**`fdbcli> location_metadata physicalshards`** — quick check. Shows total shards
vs physical (shard-encoded) shards. During forward migration, physical shards
increase toward total. During rollback, they decrease toward zero. Fast,
lightweight, no scanning required.

**`fdbcli> audit_storage metadata_encoding`** — definitive check. Scans all
keyServers and serverKeys entries, classifies each by protocol version header,
counts dataMoves entries. Reports explicit migration status:
FORWARD COMPLETE / ROLLBACK IN PROGRESS / ROLLBACK COMPLETE.

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
  Old format (tag-based): 0
  New format (UID-based): 45021
serverKeys: 123004 entries
  Old format (constants): 0
  New format (UID-encoded): 123004
dataMoves: 3 entries (in-progress moves)

Migration status: FORWARD COMPLETE
```

After rollback (knob flipped false, drain in progress):

```
fdbcli> audit_storage metadata_encoding
keyServers: 45021 entries
  Old format (tag-based): 44800
  New format (UID-based): 221
serverKeys: 123004 entries
  Old format (constants): 122500
  New format (UID-encoded): 504
dataMoves: 0 entries

Migration status: ROLLBACK IN PROGRESS (221 keyServers + 504 serverKeys remaining)
```

When all zeros:

```
Migration status: ROLLBACK COMPLETE — safe to downgrade binary
```
