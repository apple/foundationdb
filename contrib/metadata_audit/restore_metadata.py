#!/usr/bin/env python3
"""
Restore FDB metadata from backup created by backup_metadata.py.

WARNING: This overwrites current metadata with backup contents!

IMPORTANT LIMITATIONS:

  | Metadata    | Restore Works? | Why                                      |
  |-------------|----------------|------------------------------------------|
  | serverList  | Yes            | No privatization, just metadata          |
  | keyServers  | Yes            | No privatization, just updates proxy     |
  | serverKeys  | PARTIAL        | Privatization issues (see below)         |

  serverKeys restore problems:

  1. For dead/non-existent servers: SET fails (Tag lookup crashes in
     ApplyMetadataMutation.cpp - server must exist in serverTag)

  2. For live servers: SET works but DESTABILIZES the cluster. The SS
     receives the private mutation and changes its local shard state,
     potentially conflicting with actual data on disk.

  What backup is useful for:
  - Diagnostic: See what metadata looked like before corruption
  - Reference: Know what values to manually restore via fdbcli
  - serverList/keyServers restore: These work (no privatization)
  - Rollback after failed repair: If servers still running, metadata-only fix

  What you CANNOT do:
  - Restore serverKeys to cluster with different server UIDs
  - Restore serverKeys without destabilizing live servers
  - Use backup as disaster recovery for completely lost cluster

  WHEN RESTORE WORKS:

  Primary use case: Rollback after failed repair attempt.
    1. Backup metadata (cluster in state A)
    2. Attempt repair
    3. Repair makes things worse
    4. Restore backup -> back to state A

  This works because same servers are still running (Tag lookup succeeds)
  and you're restoring to a state the cluster was already in.

  | Scenario                          | Works? | Notes                        |
  |-----------------------------------|--------|------------------------------|
  | Rollback failed repair            | Yes    | Primary use case             |
  | Restore serverList only           | Yes    | No privatization             |
  | Restore keyServers only           | Yes    | No privatization             |
  | Dev/test environment reset        | Yes    | Acceptable to destabilize    |
  | Disaster recovery (new cluster)   | NO     | Different server UIDs        |
  | After servers got new UIDs        | NO     | Tag lookup fails             |

  Bottom line: Backup/restore is a safety net for repair operations, not
  disaster recovery. Always backup before editing metadata.

To write to serverKeys/keyServers, this script must:
1. Disable Data Distributor
2. Take ownership of MoveKeysLock
3. Update MoveKeysLock write key with each transaction
This is exactly what FDB's internal code does (see MoveKeys.actor.cpp).

Usage:
    python3 restore_metadata.py --backup-dir DIR [--cluster-file PATH] --yes-i-am-sure

Options:
    --dry-run         Show what would be restored without writing
    --restore-only X  Only restore specific type: serverList, keyServers, serverKeys
"""

import fdb
import json
import argparse
import os
import sys

from fdb_metadata_utils import (
    SERVER_KEYS_PREFIX, SERVER_LIST_PREFIX, KEY_SERVERS_PREFIX,
    strinc, take_movekeys_lock, release_movekeys_lock,
    set_write_transaction_options, set_read_transaction_options,
    update_movekeys_lock_write,
)


def restore_entries(db, entries, name, prefix, end, dry_run=False, batch_size=100):
    """
    Restore entries to FDB.
    Each entry is {'key': hex_string, 'value': hex_string}.

    Validates all keys fall within [prefix, end) before writing.
    Uses MoveKeysLock for serverKeys/keyServers writes.
    """
    print(f"\nRestoring {name} ({len(entries)} entries)...", flush=True)

    # Validate all keys are within the expected range
    out_of_range = 0
    for entry in entries:
        key = bytes.fromhex(entry['key'])
        if key < prefix or key >= end:
            out_of_range += 1
            if out_of_range <= 3:
                print(f"  ERROR: key {entry['key'][:40]}... outside {name} range", flush=True)

    if out_of_range > 0:
        print(f"  ERROR: {out_of_range} keys outside expected range, aborting restore of {name}", flush=True)
        return -1

    if dry_run:
        print(f"  DRY RUN - would write {len(entries)} entries", flush=True)
        return 0

    written = 0
    batch = []

    for entry in entries:
        key = bytes.fromhex(entry['key'])
        value = bytes.fromhex(entry['value'])
        batch.append((key, value))

        if len(batch) >= batch_size:
            # Capture batch in closure
            batch_to_write = batch[:]

            @fdb.transactional
            def write_batch(tr):
                set_write_transaction_options(tr)
                update_movekeys_lock_write(tr)
                for k, v in batch_to_write:
                    tr[k] = v

            write_batch(db)
            written += len(batch)
            print(f"  ... {written} entries written", flush=True)
            batch = []

    # Write remaining
    if batch:
        batch_to_write = batch[:]

        @fdb.transactional
        def write_batch(tr):
            set_write_transaction_options(tr)
            update_movekeys_lock_write(tr)
            for k, v in batch_to_write:
                tr[k] = v

        write_batch(db)
        written += len(batch)

    print(f"  Total: {written} entries written", flush=True)
    return written


def clear_range(db, prefix, end, name, dry_run=False):
    """Clear all entries in a range before restoring."""
    print(f"\nClearing existing {name}...", flush=True)

    if dry_run:
        print(f"  DRY RUN - would clear range", flush=True)
        return

    @fdb.transactional
    def do_clear(tr):
        set_write_transaction_options(tr)
        update_movekeys_lock_write(tr)
        tr.clear_range(prefix, end)

    do_clear(db)
    print(f"  Cleared", flush=True)


def verify_backup(args, manifest, db):
    """Verify backup is readable and optionally compare against current FDB."""

    # Connect to FDB if not already connected
    if db is None:
        if args.cluster_file:
            db = fdb.open(args.cluster_file)
        else:
            db = fdb.open()
        print("\nConnected to FoundationDB")

    print("\n" + "=" * 60)
    print("VERIFYING BACKUP")
    print("=" * 60)

    all_ok = True

    for name in ['serverList', 'keyServers', 'serverKeys']:
        file_path = os.path.join(args.backup_dir, f'{name}.json')
        if not os.path.exists(file_path):
            print(f"\n[{name}] File not found: {file_path}")
            all_ok = False
            continue

        print(f"\n[{name}]")

        # Load and verify JSON
        try:
            with open(file_path) as f:
                entries = json.load(f)
            print(f"  Loaded {len(entries)} entries from JSON")
        except Exception as e:
            print(f"  ERROR loading JSON: {e}")
            all_ok = False
            continue

        # Verify entry structure
        bad_entries = 0
        for entry in entries:
            try:
                if 'key' not in entry or 'value' not in entry:
                    bad_entries += 1
                else:
                    bytes.fromhex(entry['key'])
                    bytes.fromhex(entry['value'])
            except:
                bad_entries += 1

        if bad_entries > 0:
            print(f"  ERROR: {bad_entries} entries have invalid structure")
            all_ok = False
        else:
            print(f"  Entry structure: OK")

        # Spot-check against FDB
        @fdb.transactional
        def check_entry(tr, key_hex, expected_value_hex):
            set_read_transaction_options(tr, timeout_ms=10000)
            key = bytes.fromhex(key_hex)
            actual = tr[key].wait()
            if actual is None:
                return "missing"
            if actual.hex() != expected_value_hex:
                return "changed"
            return "match"

        # Check sample entries
        if len(entries) == 0:
            print(f"  Spot-check: skipped (no entries)")
            continue

        sample_size = min(20, len(entries))
        indices = [0] + [len(entries) * i // sample_size for i in range(1, sample_size)]
        indices = sorted(set(indices))[:sample_size]

        matches = 0
        missing = 0
        changed = 0

        for i in indices:
            entry = entries[i]
            result = check_entry(db, entry['key'], entry['value'])
            if result == "match":
                matches += 1
            elif result == "missing":
                missing += 1
            elif result == "changed":
                changed += 1

        print(f"  Spot-check ({len(indices)} entries): {matches} match, {missing} missing, {changed} changed")

        if missing > 0 or changed > 0:
            print(f"  NOTE: Differences detected - backup may be from different point in time")

    print("\n" + "=" * 60)
    if all_ok:
        print("BACKUP VERIFICATION PASSED")
        print("Backup files are readable and entries are valid hex.")
    else:
        print("BACKUP VERIFICATION FAILED")
        print("Review errors above.")
    print("=" * 60)

    return 0 if all_ok else 1


def main():
    parser = argparse.ArgumentParser(description='Restore FDB metadata from backup')
    parser.add_argument('--backup-dir', '-b', required=True, help='Backup directory')
    parser.add_argument('--cluster-file', '-C', help='Path to fdb.cluster file')
    parser.add_argument('--yes-i-am-sure', action='store_true', help='Confirm restore')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--restore-only', choices=['serverList', 'keyServers', 'serverKeys'],
                        help='Only restore specific metadata type')
    parser.add_argument('--verify', action='store_true',
                        help='Verify backup is readable and compare against current FDB')
    args = parser.parse_args()

    # Check backup exists
    manifest_path = os.path.join(args.backup_dir, 'backup_manifest.json')
    if not os.path.exists(manifest_path):
        print(f"ERROR: No backup_manifest.json found in {args.backup_dir}")
        return 1

    with open(manifest_path) as f:
        manifest = json.load(f)

    print("=" * 60)
    print("RESTORE FDB METADATA")
    print("=" * 60)
    print(f"Backup directory: {args.backup_dir}")
    print(f"Backup timestamp: {manifest.get('datetime', 'unknown')}")
    print(f"Backup contains:")
    for name, count in manifest.get('counts', {}).items():
        print(f"  {name}: {count} entries")

    if args.verify:
        print("\n*** VERIFY MODE - comparing backup against current FDB ***")
        return verify_backup(args, manifest, None)

    if not args.yes_i_am_sure and not args.dry_run:
        print("\nERROR: This will OVERWRITE current metadata!")
        print("Add --yes-i-am-sure to confirm, or --dry-run to preview, or --verify to check backup")
        return 1

    if args.dry_run:
        print("\n*** DRY RUN MODE - no changes will be made ***")

    # Connect to FDB
    if args.cluster_file:
        db = fdb.open(args.cluster_file)
    else:
        db = fdb.open()

    print("\nConnected to FoundationDB")

    restore_targets = []
    if args.restore_only:
        restore_targets = [args.restore_only]
    else:
        restore_targets = ['serverList', 'keyServers', 'serverKeys']

    # Take MoveKeysLock before writing (required for serverKeys/keyServers)
    prev_dd_mode = None
    if not args.dry_run:
        print("\nDisabling DD and taking MoveKeysLock...")
        try:
            our_uid, prev_dd_mode = take_movekeys_lock(db)
            print(f"  Lock acquired (owner: {our_uid.hex()[:16]}...)")
        except Exception as e:
            print(f"  ERROR taking lock: {e}")
            print("  Cannot proceed without lock - DD may interfere with writes")
            return 1

    try:
        # Restore each type
        for name in restore_targets:
            file_path = os.path.join(args.backup_dir, f'{name}.json')
            if not os.path.exists(file_path):
                print(f"\nWARNING: {file_path} not found, skipping")
                continue

            with open(file_path) as f:
                entries = json.load(f)

            print(f"\nLoaded {len(entries)} entries from {name}.json")

            # Determine prefix/end for clearing
            if name == 'serverList':
                prefix, end = SERVER_LIST_PREFIX, strinc(SERVER_LIST_PREFIX)
            elif name == 'keyServers':
                prefix, end = KEY_SERVERS_PREFIX, strinc(KEY_SERVERS_PREFIX)
            elif name == 'serverKeys':
                prefix, end = SERVER_KEYS_PREFIX, strinc(SERVER_KEYS_PREFIX)

            # Clear existing, then restore
            clear_range(db, prefix, end, name, dry_run=args.dry_run)
            result = restore_entries(db, entries, name, prefix, end, dry_run=args.dry_run)
            if result < 0:
                print(f"\n  Skipping {name} due to validation errors")
                continue

    finally:
        # Release MoveKeysLock and re-enable DD
        if not args.dry_run:
            print("\nRe-enabling DD and releasing MoveKeysLock...")
            try:
                release_movekeys_lock(db, prev_dd_mode)
                print("  Lock released, DD re-enabled")
            except Exception as e:
                print(f"  WARNING: Error releasing lock: {e}")
                print("  DD may need to be manually re-enabled")

    # Summary
    print(f"\n" + "=" * 60)
    if args.dry_run:
        print("DRY RUN COMPLETE - no changes made")
    else:
        print("RESTORE COMPLETE")
    print("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
