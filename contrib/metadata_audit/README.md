# FDB Metadata Audit Tools

Tools for diagnosing and recovering from FoundationDB metadata corruption
(serverList, keyServers, serverKeys).

**You should never need these tools under normal operation.** They exist to
address rare metadata inconsistencies found in older versions of FDB (7.3)
due to bugs in shard movement that should now be fixed. If you find yourself
needing them on a current release, something unexpected has happened — please
be sure to file a bug.

## Quick Start

```bash
# Check for corruption (read-only diagnostics)
./metadata-audit.sh check -C /path/to/fdb.cluster

# Backup metadata before any repair
./metadata-audit.sh backup -C /path/to/fdb.cluster --output-dir /tmp/meta_backup

# Restore from backup (after failed repair)
./metadata-audit.sh restore --backup-dir /tmp/meta_backup_20260216_120000 \
    --dry-run -C /path/to/fdb.cluster
```

## Prerequisites

1. **libfdb_c** — The FDB C client library (`libfdb_c.so` or `libfdb_c.dylib`).

2. **Python 3.6+**

The `fdb` Python bindings are bundled in this directory — no `pip install` needed.

## Finding libfdb_c and the Cluster File

On a machine running FDB, use `ps` to find the running `fdbserver` process:

```bash
ps auxw | grep fdbserver
```

This shows something like:

```
fdb  12345  ... /usr/local/foundationdb/bin/fdbserver \
    -C /etc/foundationdb/fdb.cluster ...
```

From this you can extract:

- **Cluster file**: the `-C` argument (e.g., `/etc/foundationdb/fdb.cluster`)
- **libfdb_c location**: replace `bin/fdbserver` with `lib/` in the binary path
  (e.g., `/usr/local/foundationdb/lib/`)

Then run:

```bash
FDB_LIB_PATH=/usr/local/foundationdb/lib \
    ./metadata-audit.sh check -C /etc/foundationdb/fdb.cluster
```

If `libfdb_c` is already on `LD_LIBRARY_PATH` (common on hosts with the client
package installed), you only need the cluster file:

```bash
./metadata-audit.sh check -C /data/v8/fdb/daily_test/fdb.cluster
```

## Commands

### `check` — Corruption Diagnostics

Reads all three metadata spaces and cross-references them to detect:
- Uncoalesced adjacent entries (keyServers and serverKeys)
- Dead server references (servers not in serverList)
- keyServers/serverKeys disagreements (wrong_shard_server)
- Orphaned serverKeys entries (server not in serverList)
- Empty source servers / ranges assigned to zero servers
- SS shard probing (actual reads to detect missing data)

```bash
./metadata-audit.sh check -C fdb.cluster
./metadata-audit.sh check -C fdb.cluster --output report.txt
```

**Note:** The check output is verbose — it dumps everything it finds.
Focus on the FINDINGS and EXECUTIVE SUMMARY sections at the end.

### `backup` — Snapshot Metadata to JSON

Creates a timestamped backup of all metadata with verification.

```bash
./metadata-audit.sh backup -C fdb.cluster --output-dir /safe/location
```

Output: `<output-dir>_<timestamp>/` containing JSON files and a manifest.

### `restore` — Restore from Backup

Restores metadata from a previous backup. Primary use case: rolling back
after a failed repair attempt.

```bash
# Preview what would be restored
./metadata-audit.sh restore --backup-dir backup_20260216_120000 --dry-run -C fdb.cluster

# Actually restore (requires confirmation flag)
./metadata-audit.sh restore --backup-dir backup_20260216_120000 --yes-i-am-sure -C fdb.cluster

# Restore only one metadata type
./metadata-audit.sh restore --backup-dir backup_20260216_120000 \
    --restore-only keyServers --yes-i-am-sure -C fdb.cluster
```

**Important limitations:** Restoring `serverKeys` only works if the same
storage servers are still running (same UIDs). See comments in
`restore_metadata.py` for details.

### `repair-coalesce` — Fix Uncoalesced Entries

**This is a rare condition.** Under normal operation, FDB's Data Distributor
coalesces KRM entries automatically. Uncoalesced entries can occur after bugs
in shard movement, interrupted data moves, or metadata corruption.

**When you need this:** DD crash-loops on an assertion in `MoveKeys.cpp`
(function `unassignServerKeys`) while attempting a shard move, because it
encounters adjacent serverKeys entries with the same value. The `check` command
will report uncoalesced entries. This tool deletes the redundant entries so DD
can operate normally.

Removes redundant adjacent KRM entries with the same value. This is safe and
idempotent — running it on a healthy cluster finds nothing to delete, and
running it multiple times is harmless.

**Background:** KRM (Key Range Map) entries define where a value starts. If
two adjacent entries have the same value, the second is redundant and wastes
space. Uncoalesced entries on live servers can trigger assertions during active
shard moves; on dead servers they are inert garbage.

```bash
# Dry run — read-only, shows what would be deleted
./metadata-audit.sh repair-coalesce --type serverKeys --dry-run -C fdb.cluster

# Dry run for a single server
./metadata-audit.sh repair-coalesce --type serverKeys --server <UID_HEX> --dry-run -C fdb.cluster

# Actually fix (requires confirmation flag)
./metadata-audit.sh repair-coalesce --type serverKeys --yes-i-am-sure -C fdb.cluster

# Fix a single server only (smallest blast radius for first test)
./metadata-audit.sh repair-coalesce --type serverKeys --server <UID_HEX> --yes-i-am-sure -C fdb.cluster

# Fix keyServers entries
./metadata-audit.sh repair-coalesce --type keyServers --dry-run -C fdb.cluster
./metadata-audit.sh repair-coalesce --type keyServers --yes-i-am-sure -C fdb.cluster
```

**Options:**
- `--type serverKeys|keyServers` — which metadata to coalesce (required)
- `--server UID` — limit to a specific server (hex UID, serverKeys only)
- `--key-prefix HEX` — limit to keys with this prefix (keyServers only)
- `--dry-run` — show what would be deleted without making changes
- `--yes-i-am-sure` — required to actually write; ignored if `--dry-run` is also set
- `--keep-dd-disabled` — don't re-enable DD after repair (use when chaining multiple repairs; remember to re-enable DD manually when done)

**What it does:**
1. Disables Data Distribution and takes the MoveKeysLock
2. Reads all entries in the target range
3. Identifies runs of adjacent entries with the same value
4. Deletes redundant entries in batches (keeps the first entry in each run)
5. Re-enables DD and releases the lock

**Safety:**
- DD is disabled during writes — no conflicts with shard moves
- Only deletes redundant boundaries — KRM semantics are unchanged
- Idempotent — running again after repair finds 0 entries to delete
- If interrupted (ctrl-C, crash), DD remains disabled. Re-enable manually:
  `fdbcli --exec "option on ACCESS_SYSTEM_KEYS; writemode on; set \xff/dataDistributionMode \x01\x00\x00\x00"`
- Verify with `status details` after repair — DD should reinitialize and show "Healthy"

**After running repair**, confirm DD is healthy:
```bash
fdbcli -C fdb.cluster --exec "status details"
```
Look for `Replication health - Healthy` and `Moving data` showing a value (not "unknown").

## Environment Setup (Manual)

If not using the wrapper script:

```bash
export LD_LIBRARY_PATH=/path/to/lib/containing/libfdb_c:$LD_LIBRARY_PATH
export PYTHONPATH=/path/to/fdb/python/bindings:$PYTHONPATH
python3 check_krm_corruption.py -C fdb.cluster
```

## How It Works

FoundationDB stores shard-to-server mapping in three system key spaces:

| Key Space | Purpose |
|-----------|---------|
| `\xff/serverList/` | Maps server UID → network address |
| `\xff/keyServers/` | Maps key range → set of server UIDs (KRM format) |
| `\xff/serverKeys/` | Maps (server UID, key range) → assignment status |

These must be consistent with each other. The `check` command reads all three
and reports any inconsistencies.

Writing to `serverKeys` or `keyServers` requires taking the MoveKeysLock
(disabling Data Distributor). The backup/restore tools handle this
automatically.
