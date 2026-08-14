#!/usr/bin/env python3
"""
Coalesce uncoalesced serverKeys or keyServers entries.

This script is IDEMPOTENT - running it on an already-healthy KRM does nothing.
Running it multiple times is safe.

The algorithm:
1. Scan entries in key order
2. Find runs of adjacent entries with equivalent values
3. Delete all but the FIRST entry in each run (it defines where the range starts)

For serverKeys:
  Adjacent entries with same value (TRUE or FALSE) are redundant.

For keyServers:
  Adjacent entries pointing to the same set of servers are redundant.

Usage:
    python3 repair_coalesce.py --type serverKeys [--dry-run] [--cluster-file PATH]
    python3 repair_coalesce.py --type keyServers [--dry-run] [--cluster-file PATH]

Options:
    --type TYPE       Required: 'serverKeys' or 'keyServers'
    --dry-run         Show what would be deleted without actually deleting
    --yes-i-am-sure   Required to actually make changes (without --dry-run)
    --server UID      For serverKeys: only process specific server (hex UID)
    --key-prefix HEX  For keyServers: only process keys with this prefix (hex)
"""

import fdb
import argparse
import sys
import struct

from fdb_metadata_utils import (
    SERVER_KEYS_PREFIX, SERVER_LIST_PREFIX, KEY_SERVERS_PREFIX,
    SERVER_LIST_END, KEY_SERVERS_END,
    strinc, take_movekeys_lock, release_movekeys_lock,
    set_read_transaction_options, set_write_transaction_options,
    update_movekeys_lock_write,
)


def decode_key_servers_value(v):
    """
    Decode keyServers value to extract server UIDs.
    Returns tuple of (src_uids, dest_uids) as frozensets for comparison.
    """
    if not v or len(v) < 12:
        return (frozenset(), frozenset())

    try:
        pos = 8  # Skip protocol version

        # src vector size
        if len(v) < pos + 4:
            return (frozenset(), frozenset())
        src_count = struct.unpack('<I', v[pos:pos + 4])[0]
        pos += 4

        # src UIDs
        src_uids = set()
        for _ in range(src_count):
            if len(v) < pos + 16:
                return (frozenset(src_uids), frozenset())
            src_uids.add(v[pos:pos + 16])
            pos += 16

        # dest vector size
        if len(v) < pos + 4:
            return (frozenset(src_uids), frozenset())
        dest_count = struct.unpack('<I', v[pos:pos + 4])[0]
        pos += 4

        # dest UIDs
        dest_uids = set()
        for _ in range(dest_count):
            if len(v) < pos + 16:
                return (frozenset(src_uids), frozenset())
            dest_uids.add(v[pos:pos + 16])
            pos += 16

        return (frozenset(src_uids), frozenset(dest_uids))

    except Exception:
        return (frozenset(), frozenset())


def get_value_key(entry_type, value):
    """
    Get a comparable key for the value.
    For serverKeys: the raw bytes (TRUE/FALSE)
    For keyServers: tuple of (src_set, dest_set)
    """
    if entry_type == 'serverKeys':
        return value  # Direct byte comparison
    else:
        return decode_key_servers_value(value)


def get_live_server_uids(db):
    """Get set of server UIDs from serverList."""
    @fdb.transactional
    def read_serverlist(tr):
        set_read_transaction_options(tr)
        uids = set()
        for k, v in tr.get_range(SERVER_LIST_PREFIX, SERVER_LIST_END):
            uid = k[len(SERVER_LIST_PREFIX):]
            uids.add(uid)
        return uids
    return read_serverlist(db)


def read_entries_batched(db, start, end, prefix_len, batch_size=10000):
    """Read all entries in a range, batched to avoid transaction limits."""
    all_entries = []
    batch_start = start

    while True:
        @fdb.transactional
        def read_batch(tr):
            set_read_transaction_options(tr, timeout_ms=60000)
            batch = []
            for k, v in tr.get_range(batch_start, end, limit=batch_size):
                batch.append((k, v))
            return batch

        batch = read_batch(db)
        if not batch:
            break

        all_entries.extend(batch)

        if len(batch) < batch_size:
            break

        batch_start = batch[-1][0] + b'\x00'

    return all_entries


def find_redundant_entries(entries, entry_type):
    """
    Find redundant entries in a list of (key, value) tuples.
    Returns list of keys to delete.

    KRM semantics: each key marks where a value STARTS.
    So [key1=TRUE, key2=TRUE, key3=FALSE] means:
      - [key1, key2) = TRUE
      - [key2, key3) = TRUE  <- redundant, same as previous
      - [key3, ...) = FALSE

    We keep the FIRST entry in each run (it defines where the range starts)
    and delete subsequent entries with the same value.

    IDEMPOTENT: If no adjacent duplicates, returns empty list.
    """
    if len(entries) < 2:
        return []

    to_delete = []
    i = 0

    while i < len(entries):
        # Start of a potential run - KEEP this entry (it's the first)
        run_start = i
        run_value_key = get_value_key(entry_type, entries[i][1])

        # Find extent of run (adjacent entries with same value)
        i += 1
        while i < len(entries):
            current_value_key = get_value_key(entry_type, entries[i][1])
            if current_value_key != run_value_key:
                break
            i += 1

        run_end = i
        run_length = run_end - run_start

        # If run has more than 1 entry, delete all but the FIRST
        # The first entry defines where the range starts
        if run_length > 1:
            for j in range(run_start + 1, run_end):
                to_delete.append(entries[j][0])

    return to_delete


def delete_keys_batched(db, keys_to_delete, dry_run=False, batch_size=100):
    """Delete keys in batches. Returns count deleted."""
    if dry_run or not keys_to_delete:
        return 0

    deleted = 0
    for i in range(0, len(keys_to_delete), batch_size):
        batch = keys_to_delete[i:i + batch_size]

        @fdb.transactional
        def do_delete(tr):
            set_write_transaction_options(tr, timeout_ms=30000)
            update_movekeys_lock_write(tr)
            for key in batch:
                tr.clear(key)

        do_delete(db)
        deleted += len(batch)

    return deleted


def coalesce_serverkeys(db, server_uid, dry_run=False):
    """Coalesce serverKeys for a single server. Returns (found, deleted)."""
    prefix = SERVER_KEYS_PREFIX + server_uid
    entries = read_entries_batched(db, prefix, strinc(prefix), len(prefix))

    to_delete = find_redundant_entries(entries, 'serverKeys')

    if to_delete and not dry_run:
        delete_keys_batched(db, to_delete)

    found = len(to_delete)
    deleted = found if not dry_run else 0
    return found, deleted


def coalesce_keyservers(db, key_prefix=None, dry_run=False):
    """Coalesce all keyServers entries. Returns (found, deleted)."""
    start = KEY_SERVERS_PREFIX + (key_prefix or b'')
    end = KEY_SERVERS_END

    print("    Reading keyServers entries...", flush=True)
    entries = read_entries_batched(db, start, end, len(KEY_SERVERS_PREFIX))
    print(f"    Found {len(entries)} entries", flush=True)

    to_delete = find_redundant_entries(entries, 'keyServers')

    if to_delete and not dry_run:
        print(f"    Deleting {len(to_delete)} redundant entries...", flush=True)
        delete_keys_batched(db, to_delete)

    found = len(to_delete)
    deleted = found if not dry_run else 0
    return found, deleted


def main():
    parser = argparse.ArgumentParser(
        description='Coalesce uncoalesced serverKeys or keyServers entries (IDEMPOTENT)')
    parser.add_argument('--type', '-t', required=True, choices=['serverKeys', 'keyServers'],
                        help='Type of entries to coalesce')
    parser.add_argument('--cluster-file', '-C', help='Path to fdb.cluster file')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--yes-i-am-sure', action='store_true', help='Confirm changes')
    parser.add_argument('--server', help='For serverKeys: specific server UID (hex)')
    parser.add_argument('--key-prefix', help='For keyServers: key prefix (hex)')
    parser.add_argument('--keep-dd-disabled', action='store_true',
                        help='Do not re-enable DD after repair (use when running multiple repairs)')
    args = parser.parse_args()

    print("=" * 60)
    print(f"COALESCE {args.type.upper()} ENTRIES")
    print("=" * 60)
    print("\nThis operation is IDEMPOTENT - safe to run multiple times.")
    print("Running on a healthy KRM will find nothing to delete.")

    if not args.dry_run and not args.yes_i_am_sure:
        print("\nERROR: Must specify --dry-run or --yes-i-am-sure")
        return 1

    if args.dry_run:
        print("\n*** DRY RUN MODE - no changes will be made ***")

    # Connect to FDB
    if args.cluster_file:
        db = fdb.open(args.cluster_file)
    else:
        db = fdb.open()

    print("\nConnected to FoundationDB")

    # Take MoveKeysLock before modifying
    prev_dd_mode = None
    if not args.dry_run:
        print("\n[1] Disabling DD and taking MoveKeysLock...")
        try:
            our_uid, prev_dd_mode = take_movekeys_lock(db)
            print(f"    Lock acquired (owner: {our_uid.hex()[:16]}...)")
        except Exception as e:
            print(f"    ERROR taking lock: {e}")
            return 1

    total_found = 0
    total_deleted = 0

    try:
        if args.type == 'serverKeys':
            # Get live servers
            print("\n[2] Reading serverList (live servers)...")
            live_uids = get_live_server_uids(db)
            print(f"    Found {len(live_uids)} live servers")

            # Filter to specific server if requested
            if args.server:
                target_uid = bytes.fromhex(args.server)
                if target_uid not in live_uids:
                    print(f"    WARNING: Server {args.server} not in serverList")
                live_uids = {target_uid}
                print(f"    Filtering to server: {args.server}")

            print(f"\n[3] Scanning serverKeys for uncoalesced entries...")
            servers_with_issues = 0

            for i, uid in enumerate(sorted(live_uids)):
                found, deleted = coalesce_serverkeys(db, uid, dry_run=args.dry_run)

                if found > 0:
                    servers_with_issues += 1
                    total_found += found
                    total_deleted += deleted
                    print(f"    Server {uid.hex()}: {found} redundant entries")

                if (i + 1) % 20 == 0:
                    print(f"    ... processed {i + 1}/{len(live_uids)} servers", flush=True)

            print(f"\n    Servers with uncoalesced entries: {servers_with_issues}")

        else:  # keyServers
            key_prefix = bytes.fromhex(args.key_prefix) if args.key_prefix else None

            print(f"\n[2] Scanning keyServers for uncoalesced entries...")
            total_found, total_deleted = coalesce_keyservers(db, key_prefix, dry_run=args.dry_run)

    finally:
        # Release MoveKeysLock (optionally keep DD disabled)
        if not args.dry_run:
            if args.keep_dd_disabled:
                print("\n[4] Keeping DD disabled (--keep-dd-disabled flag)")
                print("    DD remains disabled for subsequent repairs")
                print("    Re-enable manually with: fdbcli --exec \"option on ACCESS_SYSTEM_KEYS; writemode on; set \\xff/dataDistributionMode \\x01\\x00\\x00\\x00\"")
            else:
                print("\n[4] Re-enabling DD and releasing MoveKeysLock...")
                try:
                    release_movekeys_lock(db, prev_dd_mode)
                    print("    Lock released, DD re-enabled")
                except Exception as e:
                    print(f"    WARNING: Error releasing lock: {e}")

    # Summary
    print(f"\n" + "=" * 60)
    if total_found == 0:
        print("NO REDUNDANT ENTRIES FOUND")
        print(f"The {args.type} are already properly coalesced.")
    elif args.dry_run:
        print("DRY RUN COMPLETE")
        print(f"Would delete {total_found} redundant {args.type} entries")
    else:
        print("COALESCE COMPLETE")
        print(f"Deleted {total_deleted} redundant {args.type} entries")
    print("=" * 60)

    # Idempotency note
    print("\nIDEMPOTENCY CHECK: Running again should find 0 redundant entries.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
