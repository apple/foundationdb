# FDB Metadata Audit Tools

Tools for diagnosing and recovering from FoundationDB metadata corruption
(serverList, keyServers, serverKeys).

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
   Build from source or install the `foundationdb-clients` package.

2. **fdb Python module** — The FoundationDB Python bindings.
   Either `pip install foundationdb` or use the build output from
   `bindings/python/` in your FDB build directory.

3. **Python 3.6+**

The wrapper script (`metadata-audit.sh`) automatically searches for these in
common locations. Use `--fdb-lib` and `--fdb-python` flags to override.

## Commands

### `check` — Corruption Diagnostics

Reads all three metadata spaces and cross-references them to detect:
- Orphaned serverKeys entries (server not in serverList)
- Missing serverKeys entries (keyServers references server with no serverKeys)
- keyServers/serverKeys disagreements
- Non-contiguous keyServers ranges (gaps or overlaps)
- Ranges assigned to zero servers

```bash
./metadata-audit.sh check -C fdb.cluster
./metadata-audit.sh check -C fdb.cluster --output report.txt
```

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
