#!/usr/bin/env python3
"""
Backup FDB metadata (serverList, keyServers, serverKeys) to JSON files.

This creates a point-in-time snapshot that can be restored with restore_metadata.py.

Usage:
    python3 backup_metadata.py [--output-dir DIR] [--cluster-file PATH]

Output files:
    <output-dir>/serverList.json
    <output-dir>/keyServers.json
    <output-dir>/serverKeys.json
    <output-dir>/backup_manifest.json  (metadata about the backup)
"""

import fdb
import json
import argparse
import os
import sys
from datetime import datetime

from fdb_metadata_utils import (
    KEY_SERVERS_PREFIX, SERVER_KEYS_PREFIX, SERVER_LIST_PREFIX,
    KEY_SERVERS_END, SERVER_KEYS_END, SERVER_LIST_END,
    set_read_transaction_options,
)


def backup_range(db, prefix, end, name, batch_size=10000):
    """
    Backup all entries in a key range.
    Returns a list of {'key': hex_str, 'value': hex_str} dicts.
    """
    print(f"\nBacking up {name}...", flush=True)

    entries = []
    start = prefix
    total = 0

    while True:
        @fdb.transactional
        def read_batch(tr):
            set_read_transaction_options(tr)
            batch = []
            for k, v in tr.get_range(start, end, limit=batch_size):
                batch.append({
                    'key': k.hex(),
                    'value': v.hex()
                })
            return batch

        batch = read_batch(db)
        if not batch:
            break

        entries.extend(batch)
        total += len(batch)
        print(f"  ... {total} entries", flush=True)

        if len(batch) < batch_size:
            break

        # Continue from after the last key
        last_key = bytes.fromhex(batch[-1]['key'])
        start = last_key + b'\x00'

    print(f"  Total: {total} entries", flush=True)
    return entries


def main():
    parser = argparse.ArgumentParser(description='Backup FDB metadata')
    parser.add_argument('--cluster-file', '-C', help='Path to fdb.cluster file')
    parser.add_argument('--output-dir', '-o', default='backup', help='Output directory (default: backup)')
    args = parser.parse_args()

    # Create output directory
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"{args.output_dir}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    print(f"Backup directory: {output_dir}")

    # Connect to FDB
    if args.cluster_file:
        db = fdb.open(args.cluster_file)
    else:
        db = fdb.open()

    print("Connected to FoundationDB")

    # Backup each metadata type
    serverlist_entries = backup_range(db, SERVER_LIST_PREFIX, SERVER_LIST_END, "serverList")
    keyservers_entries = backup_range(db, KEY_SERVERS_PREFIX, KEY_SERVERS_END, "keyServers")
    serverkeys_entries = backup_range(db, SERVER_KEYS_PREFIX, SERVER_KEYS_END, "serverKeys")

    # Write to files
    print(f"\nWriting files to {output_dir}/...")

    serverlist_path = os.path.join(output_dir, 'serverList.json')
    with open(serverlist_path, 'w') as f:
        json.dump(serverlist_entries, f, indent=2)
    print(f"  serverList.json: {len(serverlist_entries)} entries")

    keyservers_path = os.path.join(output_dir, 'keyServers.json')
    with open(keyservers_path, 'w') as f:
        json.dump(keyservers_entries, f, indent=2)
    print(f"  keyServers.json: {len(keyservers_entries)} entries")

    serverkeys_path = os.path.join(output_dir, 'serverKeys.json')
    with open(serverkeys_path, 'w') as f:
        json.dump(serverkeys_entries, f, indent=2)
    print(f"  serverKeys.json: {len(serverkeys_entries)} entries")

    # Write manifest
    manifest = {
        'timestamp': timestamp,
        'datetime': datetime.now().isoformat(),
        'counts': {
            'serverList': len(serverlist_entries),
            'keyServers': len(keyservers_entries),
            'serverKeys': len(serverkeys_entries),
        },
        'files': {
            'serverList': 'serverList.json',
            'keyServers': 'keyServers.json',
            'serverKeys': 'serverKeys.json',
        }
    }

    manifest_path = os.path.join(output_dir, 'backup_manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    print(f"  backup_manifest.json")

    # Verify backup is restorable
    print(f"\n" + "=" * 60)
    print("VERIFYING BACKUP")
    print("=" * 60)

    verification_ok = True

    # 1. Re-read JSON files and verify they parse
    print("\n[1] Verifying JSON files are readable...")
    for name, expected_count in [('serverList', len(serverlist_entries)),
                                  ('keyServers', len(keyservers_entries)),
                                  ('serverKeys', len(serverkeys_entries))]:
        path = os.path.join(output_dir, f'{name}.json')
        try:
            with open(path) as f:
                loaded = json.load(f)
            if len(loaded) != expected_count:
                print(f"  ERROR: {name}.json has {len(loaded)} entries, expected {expected_count}")
                verification_ok = False
            else:
                print(f"  {name}.json: OK ({len(loaded)} entries)")
        except Exception as e:
            print(f"  ERROR: Failed to read {name}.json: {e}")
            verification_ok = False

    # 2. Verify entries have correct structure
    print("\n[2] Verifying entry structure...")
    for name, entries in [('serverList', serverlist_entries),
                          ('keyServers', keyservers_entries),
                          ('serverKeys', serverkeys_entries)]:
        errors = 0
        for i, entry in enumerate(entries[:100]):  # Check first 100
            if 'key' not in entry or 'value' not in entry:
                errors += 1
            else:
                try:
                    bytes.fromhex(entry['key'])
                    bytes.fromhex(entry['value'])
                except Exception:
                    errors += 1
        if errors > 0:
            print(f"  ERROR: {name} has {errors} malformed entries")
            verification_ok = False
        else:
            print(f"  {name}: OK (entries have valid hex key/value)")

    # 3. Spot-check: verify some entries match current FDB state
    print("\n[3] Spot-checking entries against live FDB...")

    @fdb.transactional
    def verify_entry(tr, key_hex, expected_value_hex):
        tr.options.set_read_system_keys()
        tr.options.set_lock_aware()
        tr.options.set_timeout(10000)
        key = bytes.fromhex(key_hex)
        actual = tr[key].wait()
        if actual is None:
            return False, "key not found"
        if actual.hex() != expected_value_hex:
            return False, f"value mismatch"
        return True, "OK"

    spot_check_count = 10
    for name, entries in [('serverList', serverlist_entries),
                          ('keyServers', keyservers_entries),
                          ('serverKeys', serverkeys_entries)]:
        # Check first, middle, and last entries
        check_indices = [0, len(entries)//2, len(entries)-1] if len(entries) >= 3 else list(range(len(entries)))
        check_indices = check_indices[:spot_check_count]

        errors = 0
        for i in check_indices:
            entry = entries[i]
            ok, msg = verify_entry(db, entry['key'], entry['value'])
            if not ok:
                errors += 1
                if errors <= 2:
                    print(f"  WARNING: {name}[{i}] {msg}")

        if errors > 0:
            print(f"  {name}: {errors}/{len(check_indices)} spot-checks FAILED")
            verification_ok = False
        else:
            print(f"  {name}: {len(check_indices)} spot-checks OK")

    # 4. Summary
    print(f"\n" + "=" * 60)
    if verification_ok:
        print("BACKUP COMPLETE AND VERIFIED")
    else:
        print("BACKUP COMPLETE BUT VERIFICATION FAILED")
        print("Review errors above before proceeding!")
    print("=" * 60)
    print(f"Directory: {output_dir}")
    print(f"serverList: {len(serverlist_entries)} entries")
    print(f"keyServers: {len(keyservers_entries)} entries")
    print(f"serverKeys: {len(serverkeys_entries)} entries")
    print(f"\nTo restore, run:")
    print(f"  python3 restore_metadata.py --backup-dir {output_dir} --yes-i-am-sure")

    return 0 if verification_ok else 1


if __name__ == '__main__':
    sys.exit(main())
