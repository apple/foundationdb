#!/usr/bin/env python3
"""
Check and repair FoundationDB keyServers and serverKeys for KRM corruption.

WORKFLOW FOR METADATA CORRUPTION DIAGNOSIS AND REPAIR:

  Step 1: Run C++ audit tool to get authoritative SS state
    ./audit_ss_shards -C fdb.cluster --json > audit_output.json

  Step 2: Generate comparison report (DIAGNOSTIC ONLY - no changes)
    python3 check_krm_corruption.py -c fdb.cluster \\
        --report-audit audit_output.json

    This shows how SS actual state compares to metadata:
    - Servers with matching state (SS agrees with metadata)
    - Servers missing shards (metadata claims SS has them, SS doesn't)
    - Servers with extra shards (SS has them, metadata doesn't know)
    - Unreachable servers

  Step 3: Dry-run repair to see what would be fixed
    python3 check_krm_corruption.py -c fdb.cluster \\
        --repair-from-audit audit_output.json --repair-serverkeys

  Step 4: Execute repair (after careful review!)
    python3 check_krm_corruption.py -c fdb.cluster \\
        --repair-from-audit audit_output.json --repair-serverkeys --execute-repair

This script detects:
1. Adjacent KRM entries with the same value (should be coalesced)
   - Triggers ASSERT at KeyRangeMap.actor.cpp:297 / MoveKeys.actor.cpp:158
2. Entries pointing to servers not in serverList (dead server references)
3. wrong_shard_server scenario (THE "DD STUCK" PROBLEM):
   - keyServers says server owns a shard
   - But serverKeys for that server doesn't claim that range
   - DD will spin getting wrong_shard_server errors from those servers
4. Orphan serverKeys - servers claiming ranges not routed to by keyServers
5. serverList vs running servers mismatch (stale or missing entries)
6. SS missing shards (via actual read probes or C++ audit tool)

SS LOCAL STATE VERIFICATION (C++ TOOL):
  The audit_ss_shards C++ tool directly queries each SS via getShardState RPC.
  This is authoritative - it asks each SS what shards it thinks it has.

  Build: ninja audit_ss_shards
  Run:   ./audit_ss_shards -C fdb.cluster --json > audit.json

  Benefits over Python probing:
  - Python probing infers missing shards from read timeouts (all replicas missing)
  - C++ tool directly queries each SS, detects when ANY single SS is missing a shard
  - C++ tool also detects orphan shards (SS has data metadata doesn't know about)

REPAIR OPTIONS:
  --repair-from-audit FILE  Load audit_ss_shards JSON output
  --repair-serverkeys       Delete stale serverKeys entries for missing shards
  --execute-repair          Actually make changes (default is dry-run)

Usage:
    python3 check_krm_corruption.py [options]

Options:
    -C, --cluster-file PATH   Path to fdb.cluster file
    -k, --key-prefix HEX      Only check keyServers with this prefix (hex)
    -l, --limit N             Max entries to scan (default: 10M)
    -v, --verbose             Verbose output
    --audit-ss-shards PATH    Run C++ audit tool and integrate results
    --report-audit FILE       Generate comparison report from audit JSON (diagnostic only)

Examples:
    # Diagnostic only (Python checks)
    python3 check_krm_corruption.py -c fdb.cluster

    # Run with C++ audit tool (direct SS queries)
    python3 check_krm_corruption.py -c fdb.cluster --audit-ss-shards ./audit_ss_shards

    # Generate detailed report comparing SS state vs metadata (RECOMMENDED)
    ./audit_ss_shards -C fdb.cluster --json > audit.json
    python3 check_krm_corruption.py -c fdb.cluster --report-audit audit.json

    # Verbose report showing all ranges
    python3 check_krm_corruption.py -c fdb.cluster --report-audit audit.json -v

    # Dry-run repair from saved audit
    python3 check_krm_corruption.py -c fdb.cluster \\
        --repair-from-audit audit.json --repair-serverkeys

    # Execute repair
    python3 check_krm_corruption.py -c fdb.cluster \\
        --repair-from-audit audit.json --repair-serverkeys --execute-repair
"""

import fdb
import struct
import argparse
import sys
import json
import subprocess
import os
from collections import defaultdict

from fdb_metadata_utils import (
    KEY_SERVERS_PREFIX, SERVER_KEYS_PREFIX, SERVER_LIST_PREFIX,
    KEY_SERVERS_END, SERVER_KEYS_END, SERVER_LIST_END,
    strinc, set_read_transaction_options,
    update_movekeys_lock_write, take_movekeys_lock, release_movekeys_lock,
)

# End keys are imported from fdb_metadata_utils

def decode_uid(uid_bytes):
    """Convert 16-byte UID to hex string for display."""
    return uid_bytes.hex() if uid_bytes else "<empty>"

def short_uid(uid_bytes):
    """Short display form of UID."""
    return uid_bytes[:4].hex() + "..." if len(uid_bytes) >= 4 else uid_bytes.hex()

def decode_compressed_int(data):
    """Decode FDB's compressed integer format."""
    if not data:
        return 0, 0

    first_byte = data[0]
    if first_byte < 254:
        return first_byte, 1
    elif first_byte == 254:
        if len(data) < 3:
            return 0, 1
        return struct.unpack('<H', data[1:3])[0], 3
    else:  # 255
        if len(data) < 5:
            return 0, 1
        return struct.unpack('<I', data[1:5])[0], 5

def decode_key_servers_value(value):
    """
    Decode keyServers value to extract source and destination server UIDs.

    FDB 6.2+ format:
    - Protocol version (8 bytes)
    - src vector size (4 bytes little-endian)
    - src UIDs (16 bytes each)
    - dest vector size (4 bytes little-endian)
    - dest UIDs (16 bytes each)

    FDB 7.2+ additionally includes (ShardEncodeLocationMetaData):
    - srcID (shard ID - 16 bytes UID)
    - destID (shard ID - 16 bytes UID) - only if dest is non-empty

    Returns: (src_servers, dest_servers, src_shard_id, dest_shard_id, proto_version)
    proto_version is the original protocol version (needed for re-encoding)
    """
    if not value:
        return [], [], None, None, None

    try:
        pos = 0

        # Check if this looks like a versioned value (starts with 0x0FDB...)
        # The protocol version is 8 bytes
        if len(value) >= 8:
            # Read protocol version (little-endian uint64)
            proto_version = struct.unpack('<Q', value[0:8])[0]
            # Check if it's a valid FDB protocol version (starts with 0x0FDB00...)
            # The version with objectSerializerFlag might have high bit set
            proto_version_clean = proto_version & 0x0FFFFFFFFFFFFFFF
            if (proto_version_clean >> 48) == 0x0FDB:
                # This is a versioned value
                pos = 8

                # Check for ShardEncodeLocationMetaData (7.2+)
                # 0x0FDB00B072000000 = 7.2, 0x0FDB00B073000000 = 7.3
                has_shard_metadata = proto_version_clean >= 0x0FDB00B072000000

                # Read src vector size (4 bytes little-endian)
                if pos + 4 > len(value):
                    return None, None, None, None, None
                src_count = struct.unpack('<I', value[pos:pos+4])[0]
                pos += 4

                # Read src UIDs
                src_servers = []
                for _ in range(src_count):
                    if pos + 16 > len(value):
                        return None, None, None, None, None
                    src_servers.append(value[pos:pos+16])
                    pos += 16

                # Read dest vector size (4 bytes little-endian)
                if pos + 4 > len(value):
                    return src_servers, [], None, None, proto_version
                dest_count = struct.unpack('<I', value[pos:pos+4])[0]
                pos += 4

                # Read dest UIDs
                dest_servers = []
                for _ in range(dest_count):
                    if pos + 16 > len(value):
                        return src_servers, None, None, None, proto_version
                    dest_servers.append(value[pos:pos+16])
                    pos += 16

                # Read shard IDs if present (ShardEncodeLocationMetaData)
                src_shard_id = None
                dest_shard_id = None
                if has_shard_metadata and pos + 16 <= len(value):
                    src_shard_id = value[pos:pos+16]
                    pos += 16

                    # destID only present if dest is non-empty
                    if dest_count > 0 and pos + 16 <= len(value):
                        dest_shard_id = value[pos:pos+16]
                        pos += 16

                return src_servers, dest_servers, src_shard_id, dest_shard_id, proto_version

        # Fallback: Try old compressed int format (pre-6.2?)
        src_count, bytes_read = decode_compressed_int(value[pos:])
        pos += bytes_read

        src_servers = []
        for _ in range(src_count):
            if pos + 16 > len(value):
                return None, None, None, None, None
            src_servers.append(value[pos:pos+16])
            pos += 16

        dest_servers = []
        if pos < len(value):
            dest_count, bytes_read = decode_compressed_int(value[pos:])
            pos += bytes_read

            for _ in range(dest_count):
                if pos + 16 > len(value):
                    return src_servers, None, None, None, None
                dest_servers.append(value[pos:pos+16])
                pos += 16

        return src_servers, dest_servers, None, None, None
    except Exception as e:
        return None, None, None, None, None


# Backwards compatible wrapper
def decode_key_servers_value_simple(value):
    """Wrapper that returns just (src, dest) for backwards compatibility."""
    result = decode_key_servers_value(value)
    if result[0] is None:
        return None, None
    return result[0], result[1]


def encode_compressed_int(n):
    """Encode an integer using FDB's compressed integer format."""
    if n < 254:
        return bytes([n])
    elif n < 65536:
        return bytes([254]) + struct.pack('<H', n)
    else:
        return bytes([255]) + struct.pack('<I', n)


# FDB protocol versions
# 0x0FDB00B062010001 = 6.2 (KeyServerValue feature)
# 0x0FDB00B072000000 = 7.2 (ShardEncodeLocationMetaData feature)
# 0x0FDB00B073000000 = 7.3
FDB_PROTOCOL_VERSION_62 = 0x0FDB00B062010001
FDB_PROTOCOL_VERSION_72 = 0x0FDB00B072000000
FDB_PROTOCOL_VERSION_73 = 0x0FDB00B073000000

# Anonymous shard ID (used when no specific shard ID is assigned)
# This is UID(0x666666, 0x88888888) - see SystemData.cpp
# UID serializes as two little-endian uint64_t values
ANONYMOUS_SHARD_ID = struct.pack('<Q', 0x666666) + struct.pack('<Q', 0x88888888)


def encode_key_servers_value(src_servers, dest_servers=None, src_shard_id=None, dest_shard_id=None, proto_version=None):
    """
    Encode keyServers value.

    Format for FDB 6.2+:
    - Protocol version (8 bytes)
    - src vector size (4 bytes little-endian)
    - src UIDs (16 bytes each)
    - dest vector size (4 bytes little-endian)
    - dest UIDs (16 bytes each)

    FDB 7.2+ additionally includes:
    - srcID (shard ID - 16 bytes)
    - destID (shard ID - 16 bytes) - only if dest is non-empty

    If proto_version is provided, use that version and only include shard IDs
    if the version supports them (>= 7.2).
    """
    result = bytearray()

    # Use provided protocol version or default to 7.3
    if proto_version is None:
        proto_version = FDB_PROTOCOL_VERSION_73

    # Check if this version supports shard metadata
    proto_version_clean = proto_version & 0x0FFFFFFFFFFFFFFF
    has_shard_metadata = proto_version_clean >= FDB_PROTOCOL_VERSION_72

    # Protocol version (8 bytes, little-endian)
    result.extend(struct.pack('<Q', proto_version))

    # src vector size (4 bytes, little-endian)
    result.extend(struct.pack('<I', len(src_servers)))

    # src UIDs
    for uid in src_servers:
        if len(uid) != 16:
            raise ValueError(f"Server UID must be 16 bytes, got {len(uid)}")
        result.extend(uid)

    # dest vector size (4 bytes, little-endian)
    dest_list = dest_servers if dest_servers else []
    result.extend(struct.pack('<I', len(dest_list)))

    # dest UIDs
    for uid in dest_list:
        if len(uid) != 16:
            raise ValueError(f"Server UID must be 16 bytes, got {len(uid)}")
        result.extend(uid)

    # Only add shard IDs for 7.2+ format
    if has_shard_metadata:
        # srcID (shard ID)
        shard_id = src_shard_id if src_shard_id else ANONYMOUS_SHARD_ID
        if len(shard_id) != 16:
            raise ValueError(f"Shard ID must be 16 bytes, got {len(shard_id)}")
        result.extend(shard_id)

        # destID only if dest is non-empty
        if dest_list:
            dest_shard = dest_shard_id if dest_shard_id else ANONYMOUS_SHARD_ID
            if len(dest_shard) != 16:
                raise ValueError(f"Dest shard ID must be 16 bytes, got {len(dest_shard)}")
            result.extend(dest_shard)

    return bytes(result)


def hex_dump(data, prefix=""):
    """Print hex dump of data in readable format."""
    hex_str = data.hex()
    # Print in groups of 2 (1 byte) with space every 16 bytes
    parts = [hex_str[i:i+2] for i in range(0, len(hex_str), 2)]
    lines = []
    for i in range(0, len(parts), 16):
        chunk = parts[i:i+16]
        lines.append(prefix + " ".join(chunk))
    return "\n".join(lines)


def analyze_keyservers_value_format(v):
    """Analyze the structure of a keyServers value."""
    if not v:
        return "empty"

    analysis = []
    pos = 0

    analysis.append(f"Total length: {len(v)} bytes")

    # Check for versioned format (FDB 7.x+)
    if len(v) >= 8:
        proto_version = struct.unpack('<Q', v[0:8])[0]
        proto_clean = proto_version & 0x0FFFFFFFFFFFFFFF
        has_obj_flag = (proto_version & 0x1000000000000000) != 0

        analysis.append(f"Protocol version: 0x{proto_version:016x}")
        analysis.append(f"  Clean version: 0x{proto_clean:016x}")
        analysis.append(f"  Has objectSerializerFlag: {has_obj_flag}")

        if (proto_clean >> 48) == 0x0FDB:
            # Extract version info
            major = (proto_clean >> 32) & 0xFF
            minor = (proto_clean >> 24) & 0xFF
            analysis.append(f"  FDB version: {major}.{minor}")

            has_shard_metadata = proto_clean >= 0x0FDB00B072000000
            analysis.append(f"  Has ShardEncodeLocationMetaData: {has_shard_metadata}")

            pos = 8

            # src vector size (4 bytes)
            if pos + 4 <= len(v):
                src_count = struct.unpack('<I', v[pos:pos+4])[0]
                analysis.append(f"src_count: {src_count} (at offset {pos})")
                pos += 4

                # src UIDs
                for i in range(src_count):
                    if pos + 16 <= len(v):
                        uid = v[pos:pos+16]
                        analysis.append(f"  src[{i}]: {short_uid(uid)}")
                        pos += 16

                # dest vector size
                if pos + 4 <= len(v):
                    dest_count = struct.unpack('<I', v[pos:pos+4])[0]
                    analysis.append(f"dest_count: {dest_count} (at offset {pos})")
                    pos += 4

                    # dest UIDs
                    for i in range(dest_count):
                        if pos + 16 <= len(v):
                            uid = v[pos:pos+16]
                            analysis.append(f"  dest[{i}]: {short_uid(uid)}")
                            pos += 16

                    # Shard IDs
                    if has_shard_metadata and pos + 16 <= len(v):
                        src_shard = v[pos:pos+16]
                        analysis.append(f"srcShardID: {src_shard.hex()} (at offset {pos})")
                        pos += 16

                        if dest_count > 0 and pos + 16 <= len(v):
                            dest_shard = v[pos:pos+16]
                            analysis.append(f"destShardID: {dest_shard.hex()} (at offset {pos})")
                            pos += 16

                    if pos < len(v):
                        analysis.append(f"UNEXPECTED EXTRA: {len(v)-pos} bytes at offset {pos}")
                        analysis.append(f"  Extra: {v[pos:].hex()}")
                    else:
                        analysis.append(f"Fully parsed (pos={pos}, len={len(v)})")

            return "\n    ".join(analysis)

    # Fallback: try compressed int format
    analysis.append("Trying compressed int format...")
    try:
        src_count, bytes_read = decode_compressed_int(v[pos:])
        analysis.append(f"src_count: {src_count} (used {bytes_read} bytes)")
        pos += bytes_read

        expected_src_bytes = src_count * 16
        if pos + expected_src_bytes <= len(v):
            pos += expected_src_bytes
            analysis.append(f"After src UIDs: pos={pos}")

            if pos < len(v):
                dest_count, bytes_read = decode_compressed_int(v[pos:])
                analysis.append(f"dest_count: {dest_count}")
                pos += bytes_read + dest_count * 16
                analysis.append(f"After dest UIDs: pos={pos}, remaining={len(v)-pos}")
    except Exception as e:
        analysis.append(f"Decode error: {e}")

    return "\n    ".join(analysis)


def test_keyservers_encoding_roundtrip(tr, limit=100):
    """
    Test that we can correctly decode and re-encode keyServers values.
    Returns True if all tested entries round-trip correctly.
    """
    print("\n[Encoding Round-Trip Test: keyServers]", flush=True)

    # First, analyze a few values to understand the format
    print("\n  Analyzing keyServers value format:", flush=True)
    start = KEY_SERVERS_PREFIX
    end = KEY_SERVERS_END

    for i, (k, v) in enumerate(tr.get_range(start, end, limit=3)):
        print(f"\n  Sample {i+1}:", flush=True)
        print(f"    Value length: {len(v)} bytes", flush=True)
        print(f"    Hex dump:", flush=True)
        print(hex_dump(v, "      "), flush=True)
        print(f"    Analysis:", flush=True)
        print(f"    {analyze_keyservers_value_format(v)}", flush=True)

    print("\n  Running round-trip test...", flush=True)

    tested = 0
    passed = 0
    failed = 0
    failed_samples = []

    for k, v in tr.get_range(start, end, limit=limit):
        tested += 1

        # Decode (new format returns 5 values including protocol version)
        result = decode_key_servers_value(v)
        src_servers, dest_servers, src_shard_id, dest_shard_id, proto_version = result

        if src_servers is None:
            failed += 1
            if len(failed_samples) < 5:
                failed_samples.append({'key': k, 'reason': 'decode failed'})
            continue

        # Re-encode with same protocol version and shard IDs
        try:
            re_encoded = encode_key_servers_value(
                src_servers, dest_servers, src_shard_id, dest_shard_id, proto_version
            )
        except Exception as e:
            failed += 1
            if len(failed_samples) < 5:
                failed_samples.append({'key': k, 'reason': f'encode failed: {e}'})
            continue

        # Compare
        if re_encoded == v:
            passed += 1
        else:
            failed += 1
            if len(failed_samples) < 5:
                proto_clean = (proto_version & 0x0FFFFFFFFFFFFFFF) if proto_version else 0
                failed_samples.append({
                    'key': k,
                    'reason': 'mismatch',
                    'original': v.hex(),
                    'original_len': len(v),
                    'reencoded': re_encoded.hex(),
                    'reencoded_len': len(re_encoded),
                    'src_count': len(src_servers),
                    'dest_count': len(dest_servers) if dest_servers else 0,
                    'src_shard_id': src_shard_id.hex() if src_shard_id else None,
                    'dest_shard_id': dest_shard_id.hex() if dest_shard_id else None,
                    'proto_version': f'0x{proto_clean:016x}' if proto_version else None,
                })

    print(f"  Tested: {tested}", flush=True)
    print(f"  Passed: {passed}", flush=True)
    print(f"  Failed: {failed}", flush=True)

    if failed_samples:
        print(f"\n  Failed samples:", flush=True)
        for f in failed_samples:
            print(f"    Key: {f['key'][len(KEY_SERVERS_PREFIX):][:30]!r}...", flush=True)
            print(f"      Reason: {f['reason']}", flush=True)
            if 'original' in f:
                print(f"      Protocol version: {f.get('proto_version', 'unknown')}", flush=True)
                print(f"      Original len:  {f['original_len']} bytes", flush=True)
                print(f"      Reencoded len: {f['reencoded_len']} bytes", flush=True)
                print(f"      Decoded: {f['src_count']} src servers, {f['dest_count']} dest servers", flush=True)
                print(f"      src_shard_id: {f['src_shard_id']}", flush=True)
                print(f"      dest_shard_id: {f['dest_shard_id']}", flush=True)
                print(f"      Original:  {f['original'][:100]}...", flush=True)
                print(f"      Reencoded: {f['reencoded'][:100]}...", flush=True)

    if failed == 0:
        print(f"\n  SUCCESS: All {tested} entries round-trip correctly!", flush=True)
        return True
    else:
        print(f"\n  FAILED: {failed}/{tested} entries did not round-trip", flush=True)
        return False


def test_serverkeys_encoding_roundtrip(tr, limit=100):
    """
    Test serverKeys encoding.

    serverKeys values can be:
    - "" (empty) or "0" = serverKeysFalse - does not own
    - "1" = serverKeysTrue - owns range
    - "3" = serverKeysTrueEmptyRange - owns range but treats as empty
    - Or: protocol version (8 bytes) + UID (16 bytes) for new format with shard ID
    """
    print("\n[Encoding Round-Trip Test: serverKeys]", flush=True)

    start = SERVER_KEYS_PREFIX
    end = SERVER_KEYS_END

    tested = 0
    values_seen = {}

    for k, v in tr.get_range(start, end, limit=limit):
        tested += 1
        v_hex = v.hex()
        values_seen[v_hex] = values_seen.get(v_hex, 0) + 1

    print(f"  Tested: {tested} entries", flush=True)
    print(f"  Unique values seen:", flush=True)
    for v_hex, count in sorted(values_seen.items(), key=lambda x: -x[1]):
        # Interpret the value
        v_bytes = bytes.fromhex(v_hex) if v_hex else b''
        if v_bytes == b'1' or v_bytes == b'\x01':
            meaning = "owns range (serverKeysTrue)"
        elif v_bytes == b'3' or v_bytes == b'\x03':
            meaning = "owns empty range (serverKeysTrueEmptyRange)"
        elif v_bytes == b'0' or v_bytes == b'\x00' or v_bytes == b'':
            meaning = "does not own (serverKeysFalse)"
        elif len(v_bytes) == 24:
            # New format: protocol version (8) + shard ID (16)
            meaning = "shard ID format (24 bytes)"
        else:
            meaning = f"unknown ({len(v_bytes)} bytes)"
        print(f"    '{v_hex}' ({meaning}): {count} entries", flush=True)

    print(f"\n  serverKeys value meanings:", flush=True)
    print(f"    '' or '30' (0x30='0') = does not own", flush=True)
    print(f"    '31' (0x31='1') = owns range", flush=True)
    print(f"    '33' (0x33='3') = owns empty range (assigned but empty)", flush=True)
    print(f"    24 bytes = protocol version + shard ID", flush=True)
    return True

def get_server_list(tr):
    """Get all server IDs from serverList."""
    servers = {}
    start = SERVER_LIST_PREFIX
    end = SERVER_LIST_END

    for k, v in tr.get_range(start, end):
        server_id = k[len(SERVER_LIST_PREFIX):]
        servers[server_id] = v

    return servers


def run_audit_ss_shards(binary_path, cluster_file=None, timeout_seconds=60):
    """
    Run the C++ audit_ss_shards tool to directly query each SS for its shard state.

    This provides authoritative information about what each SS thinks it has,
    which can be compared against metadata. Unlike the Python probe approach
    (which infers missing shards from read timeouts), this directly asks each SS.

    Returns dict with audit results, or None on error.
    """
    print("\n[C++ SS Shard Audit - Direct SS Queries]", flush=True)
    print(f"  Running: {binary_path} --json", flush=True)

    cmd = [binary_path, '--json']
    if cluster_file:
        cmd.extend(['-C', cluster_file])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds * 2)

        if result.returncode != 0:
            print(f"  ERROR: audit_ss_shards failed with code {result.returncode}", flush=True)
            if result.stderr:
                print(f"  stderr: {result.stderr[:500]}", flush=True)
            return None

        # Parse JSON output
        try:
            audit_result = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            print(f"  ERROR: Failed to parse JSON output: {e}", flush=True)
            print(f"  stdout: {result.stdout[:500]}", flush=True)
            return None

        # Check for error in JSON
        if 'error' in audit_result:
            print(f"  ERROR from tool: {audit_result['error']}", flush=True)
            return None

        # Print summary
        total = audit_result.get('total_servers', 0)
        reachable = audit_result.get('reachable_servers', 0)
        unreachable = audit_result.get('unreachable_servers', 0)
        mismatches = audit_result.get('servers_with_mismatches', 0)
        missing = audit_result.get('total_missing_from_ss', 0)
        extra = audit_result.get('total_extra_in_ss', 0)

        print(f"\n  C++ Audit Results:", flush=True)
        print(f"    Total servers: {total}", flush=True)
        print(f"    Reachable: {reachable}", flush=True)
        print(f"    Unreachable: {unreachable}", flush=True)
        print(f"    Servers with mismatches: {mismatches}", flush=True)

        if missing > 0 or extra > 0:
            print(f"\n  CRITICAL (from direct SS queries):", flush=True)
            print(f"    Ranges metadata says SS should have, but SS doesn't: {missing}", flush=True)
            print(f"    Ranges SS has but metadata doesn't claim: {extra}", flush=True)

            # Show details for each server with issues
            for server in audit_result.get('servers', []):
                missing_ranges = server.get('missing_from_ss', [])
                extra_ranges = server.get('extra_in_ss', [])

                if missing_ranges or extra_ranges:
                    print(f"\n    Server {server['server_id']} @ {server['address']}:", flush=True)

                    if missing_ranges:
                        print(f"      Missing from SS ({len(missing_ranges)} ranges):", flush=True)
                        for r in missing_ranges[:5]:  # Show first 5
                            print(f"        [{r['begin'][:40]}..., {r['end'][:40]}...)", flush=True)
                        if len(missing_ranges) > 5:
                            print(f"        ... and {len(missing_ranges) - 5} more", flush=True)

                    if extra_ranges:
                        print(f"      Extra in SS ({len(extra_ranges)} ranges):", flush=True)
                        for r in extra_ranges[:5]:
                            print(f"        [{r['begin'][:40]}..., {r['end'][:40]}...)", flush=True)
                        if len(extra_ranges) > 5:
                            print(f"        ... and {len(extra_ranges) - 5} more", flush=True)
        else:
            print(f"\n  All reachable SSs match metadata (no mismatches from direct queries).", flush=True)

        # List unreachable servers
        unreachable_servers = [s for s in audit_result.get('servers', []) if not s.get('reachable', False)]
        if unreachable_servers:
            print(f"\n  Unreachable servers:", flush=True)
            for s in unreachable_servers:
                print(f"    {s['server_id']} @ {s['address']} - {s.get('error', 'unknown error')}", flush=True)

        # Report metrics test results (DD waitStorageMetrics path)
        metrics = audit_result.get('metrics')
        if metrics:
            print(f"\n  Metrics Path Test (DD waitStorageMetrics):", flush=True)
            print(f"    Total ranges:       {metrics.get('total_ranges', 0)}", flush=True)
            print(f"    Metrics OK:         {metrics.get('ok_count', 0)}", flush=True)
            print(f"    wrong_shard_server: {metrics.get('wrong_shard_count', 0)}", flush=True)
            print(f"    Timeout:            {metrics.get('timeout_count', 0)}", flush=True)
            print(f"    Total data size:    {metrics.get('total_bytes', 0):,} bytes", flush=True)

            wrong_shard_count = metrics.get('wrong_shard_count', 0)
            if wrong_shard_count > 0:
                print(f"\n  CRITICAL: {wrong_shard_count} ranges cause DD wrong_shard_server!", flush=True)
                failed_shards = metrics.get('failed_shards', [])
                for shard in failed_shards[:5]:
                    print(f"    [{shard.get('begin', '')[:40]}...]", flush=True)
                if len(failed_shards) > 5:
                    print(f"    ... and {len(failed_shards) - 5} more", flush=True)
            else:
                print(f"\n  All ranges returned metrics OK - DD should work.", flush=True)

        return audit_result

    except subprocess.TimeoutExpired:
        print(f"  ERROR: audit_ss_shards timed out after {timeout_seconds * 2}s", flush=True)
        return None
    except FileNotFoundError:
        print(f"  ERROR: Binary not found: {binary_path}", flush=True)
        return None
    except Exception as e:
        print(f"  ERROR: {e}", flush=True)
        return None


def get_cluster_status(cluster_file=None, fdbcli_path=None):
    """Get cluster status via fdbcli and parse storage server info."""
    fdbcli = fdbcli_path or os.environ.get('FDBCLI_PATH', 'fdbcli')
    cmd = [fdbcli, '--exec', 'status json']
    if cluster_file:
        cmd.extend(['-C', cluster_file])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"    fdbcli error: {result.stderr}")
            return None

        # Parse JSON from output (skip any non-JSON lines)
        output = result.stdout
        json_start = output.find('{')
        if json_start == -1:
            print(f"    No JSON found in fdbcli output")
            return None

        return json.loads(output[json_start:])
    except subprocess.TimeoutExpired:
        print(f"    fdbcli timed out")
        return None
    except json.JSONDecodeError as e:
        print(f"    JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"    Error getting status: {e}")
        return None


def status_id_to_serverlist_prefix(status_id_hex):
    """
    Convert status JSON storage server ID to serverList UID prefix format.

    Status JSON shows UIDs in a different byte order than serverList keys:
    - Status ID is 8 bytes (16 hex chars): bytes4-7_reversed + bytes0-3_reversed
    - serverList UID is 16 bytes, we can only compare the first 8 bytes

    Example:
    - Status ID: aed3b039325af704
    - serverList prefix: 04f75a3239b0d3ae (first 8 bytes of 16-byte UID)

    The conversion:
    - Split status ID into two 4-byte halves
    - Reverse each half's bytes
    - Swap the halves
    """
    if len(status_id_hex) != 16:  # 8 bytes = 16 hex chars
        return None

    status_bytes = bytes.fromhex(status_id_hex)
    # Status shows: bytes4-7_reversed + bytes0-3_reversed
    # To get original: reverse each half and swap
    first_half_reversed = status_bytes[0:4]   # was bytes 4-7 reversed
    second_half_reversed = status_bytes[4:8]  # was bytes 0-3 reversed

    # Reverse back to get original byte order
    original_bytes_4_7 = first_half_reversed[::-1]
    original_bytes_0_3 = second_half_reversed[::-1]

    # Combine in original order: bytes 0-3, then bytes 4-7
    return original_bytes_0_3 + original_bytes_4_7


def serverlist_uid_to_status_format(uid_bytes):
    """
    Convert serverList UID (first 8 bytes) to status JSON format.
    This is the inverse of status_id_to_serverlist_prefix.
    """
    if len(uid_bytes) < 8:
        return uid_bytes.hex()

    # Take first 8 bytes of serverList UID
    first_8 = uid_bytes[:8]

    # Reverse each 4-byte half and swap
    bytes_0_3 = first_8[0:4]
    bytes_4_7 = first_8[4:8]

    # Status shows: bytes4-7_reversed + bytes0-3_reversed
    status_bytes = bytes_4_7[::-1] + bytes_0_3[::-1]
    return status_bytes.hex()


def extract_storage_servers_from_status(status, debug=False):
    """Extract storage server IDs from cluster status JSON.

    NOTE: Status JSON shows UIDs in a different format than serverList.
    We convert to serverList format for consistent comparison.
    """
    servers = {}

    if not status or 'cluster' not in status:
        return servers

    cluster = status['cluster']

    # Get processes
    processes = cluster.get('processes', {})
    for proc_id, proc_info in processes.items():
        roles = proc_info.get('roles', [])
        for role in roles:
            if role.get('role') == 'storage':
                # Storage server ID is in the role info
                ss_id = role.get('id')
                if ss_id:
                    if debug and len(servers) < 3:
                        print(f"    DEBUG: fdbcli status SS id = '{ss_id}' (len={len(ss_id)})", flush=True)

                    # Convert from status format to serverList format
                    try:
                        # Convert status ID to serverList UID prefix (first 8 bytes)
                        uid_prefix = status_id_to_serverlist_prefix(ss_id)
                        if uid_prefix is None:
                            if debug:
                                print(f"    DEBUG: Could not convert status ID '{ss_id}'", flush=True)
                            continue

                        if debug and len(servers) < 3:
                            print(f"    DEBUG:   -> serverList prefix = {uid_prefix.hex()} (len={len(uid_prefix)})", flush=True)

                        servers[uid_prefix] = {
                            'process_id': proc_id,
                            'address': proc_info.get('address', 'unknown'),
                            'data_bytes': role.get('stored_bytes', 0),
                            'kvstore_bytes': role.get('kvstore_used_bytes', 0),
                            'raw_id': ss_id,  # Keep the original status format string
                        }
                    except ValueError as e:
                        if debug:
                            print(f"    DEBUG: Failed to parse SS id '{ss_id}': {e}", flush=True)
                        pass

    return servers


def get_ss_shard_info_from_special_keys(db):
    """Try to get storage server shard info from FDB special key spaces."""
    info = {}
    print("    Querying special keys (5 second timeout)...", flush=True)

    @fdb.transactional
    def read_special_keys(tr):
        tr.options.set_read_system_keys()
        tr.options.set_lock_aware()
        tr.options.set_timeout(5000)  # 5 second timeout

        results = {}

        # Check for storage server metrics - be very limited
        try:
            # Just check if anything exists, don't scan
            metrics_prefix = b'\xff\xff/metrics/'
            result = tr.get_range(metrics_prefix, strinc(metrics_prefix), limit=5)
            for k, v in result:
                results[k] = v
            print(f"    Found {len(results)} metric entries", flush=True)
        except fdb.FDBError as e:
            results['metrics_error'] = f"FDB error: {e}"
            print(f"    Metrics query failed: {e}", flush=True)
        except Exception as e:
            results['metrics_error'] = str(e)
            print(f"    Metrics query failed: {e}", flush=True)

        return results

    try:
        info = read_special_keys(db)
    except fdb.FDBError as e:
        info['error'] = f"FDB error: {e}"
        print(f"    Special keys query failed: {e}", flush=True)
    except Exception as e:
        info['error'] = str(e)
        print(f"    Special keys query failed: {e}", flush=True)

    return info


def check_serverlist_vs_running(db, cluster_file=None, fdbcli_path=None):
    """
    Compare serverList UIDs with running storage server UIDs from fdbcli status.

    This answers the critical question: Is serverList correct, or is it also stale?

    Returns:
        dict with:
        - serverlist_uids: set of UIDs in serverList
        - running_uids: set of UIDs from fdbcli status (running SSs)
        - in_both: UIDs present in both (correct)
        - only_in_serverlist: UIDs in serverList but not running (stale entries)
        - only_running: UIDs running but not in serverList (missing entries)
        - match: True if they match exactly
    """
    print("\n" + "=" * 60, flush=True)
    print("SERVERLIST vs RUNNING SERVERS CHECK", flush=True)
    print("=" * 60, flush=True)

    result = {
        'serverlist_uids': set(),
        'serverlist_details': {},
        'running_uids': set(),
        'running_details': {},
        'in_both': set(),
        'only_in_serverlist': set(),
        'only_running': set(),
        'match': False,
    }

    # Step 1: Get serverList entries from FDB
    print("\n[1] Reading serverList from FDB...", flush=True)

    def read_serverlist_with_timeout(db, timeout_ms):
        @fdb.transactional
        def read_serverlist(tr):
            tr.options.set_read_system_keys()
            tr.options.set_lock_aware()
            tr.options.set_timeout(timeout_ms)
            return get_server_list(tr)
        return read_serverlist(db)

    # Retry with increasing timeouts
    serverlist = None
    for attempt, timeout_ms in enumerate([30000, 60000, 120000], 1):
        try:
            print(f"    Attempt {attempt}/3 (timeout={timeout_ms}ms)...", flush=True)
            serverlist = read_serverlist_with_timeout(db, timeout_ms)
            print(f"    Found {len(serverlist)} servers in serverList", flush=True)
            break
        except fdb.FDBError as e:
            if e.code == 1031 and attempt < 3:  # transaction_timed_out
                print(f"    Timeout, retrying with longer timeout...", flush=True)
                import time
                time.sleep(2)
                continue
            raise

    if serverlist is None:
        print("    ERROR: Could not read serverList after retries", flush=True)
        return result

    for uid_bytes, interface_data in serverlist.items():
        result['serverlist_uids'].add(uid_bytes)
        # Try to extract address from the StorageServerInterface
        # The interface is serialized, but we can try to find IP:port patterns
        result['serverlist_details'][uid_bytes] = {
            'interface_size': len(interface_data),
            'interface_hex': interface_data[:64].hex() if len(interface_data) > 64 else interface_data.hex(),
        }
        # Try to extract address (look for IP pattern in the data)
        try:
            interface_str = interface_data.decode('latin-1')
            # Look for IP:port pattern in string form
            import re
            ip_match = re.search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+)', interface_str)
            if ip_match:
                result['serverlist_details'][uid_bytes]['address'] = ip_match.group(1)
        except:
            pass

        # Also try to find IP addresses as 4 consecutive bytes that look like 100.103.x.x
        # (based on the running server IPs we saw)
        try:
            for i in range(len(interface_data) - 6):
                # Look for 100.103.x.x pattern (0x64 0x67 ...)
                if interface_data[i] == 100 and interface_data[i+1] == 103:
                    ip = f"{interface_data[i]}.{interface_data[i+1]}.{interface_data[i+2]}.{interface_data[i+3]}"
                    # Look for port after IP (usually 2 bytes, little-endian)
                    if i + 6 <= len(interface_data):
                        port = struct.unpack('<H', interface_data[i+4:i+6])[0]
                        if 4500 <= port <= 4600:  # Reasonable FDB port range
                            result['serverlist_details'][uid_bytes]['address'] = f"{ip}:{port}"
                            result['serverlist_details'][uid_bytes]['address_offset'] = i
                            break
        except:
            pass

    # Step 2: Get running storage servers from fdbcli status
    print("\n[2] Getting running servers from fdbcli status...", flush=True)

    status = get_cluster_status(cluster_file, fdbcli_path)
    if status:
        running_servers = extract_storage_servers_from_status(status, debug=True)
        print(f"    Found {len(running_servers)} running storage servers", flush=True)

        for uid_bytes, info in running_servers.items():
            result['running_uids'].add(uid_bytes)
            result['running_details'][uid_bytes] = info
    else:
        print("    WARNING: Could not get cluster status", flush=True)

    # Debug: Show serverList key format
    print("\n    DEBUG: Sample serverList keys (raw):", flush=True)
    for uid in sorted(result['serverlist_uids'], key=lambda x: x.hex())[:3]:
        print(f"      Key suffix: {uid.hex()} (len={len(uid)})", flush=True)

    # Step 3: Compare
    print("\n[3] Comparing serverList vs running servers...", flush=True)

    result['in_both'] = result['serverlist_uids'] & result['running_uids']
    result['only_in_serverlist'] = result['serverlist_uids'] - result['running_uids']
    result['only_running'] = result['running_uids'] - result['serverlist_uids']
    result['match'] = (result['serverlist_uids'] == result['running_uids'])

    # Check if mismatch is due to UID length difference (8 vs 16 bytes)
    sl_lens = set(len(uid) for uid in result['serverlist_uids']) if result['serverlist_uids'] else set()
    run_lens = set(len(uid) for uid in result['running_uids']) if result['running_uids'] else set()
    result['length_mismatch'] = sl_lens != run_lens and len(sl_lens) > 0 and len(run_lens) > 0

    # Print results
    print(f"\n    serverList UIDs:  {len(result['serverlist_uids'])}", flush=True)
    print(f"    Running UIDs:     {len(result['running_uids'])}", flush=True)
    print(f"    In both:          {len(result['in_both'])}", flush=True)
    print(f"    Only serverList:  {len(result['only_in_serverlist'])} (stale entries)", flush=True)
    print(f"    Only running:     {len(result['only_running'])} (missing from serverList)", flush=True)

    # Debug: show sample UIDs to verify format
    print(f"\n    Sample serverList UIDs (raw hex):", flush=True)
    for uid in sorted(result['serverlist_uids'], key=lambda x: x.hex())[:3]:
        print(f"      {uid.hex()} (len={len(uid)})", flush=True)

    print(f"\n    Sample running UIDs (raw hex):", flush=True)
    for uid in sorted(result['running_uids'], key=lambda x: x.hex())[:3]:
        print(f"      {uid.hex()} (len={len(uid)})", flush=True)

    if result['match']:
        print(f"\n    ✓ MATCH: serverList contains exactly the running servers", flush=True)
        print(f"    serverList is CORRECT - no repair needed", flush=True)
    else:
        if result['length_mismatch']:
            print(f"\n    Note: UID lengths differ (serverList: {sl_lens}, running: {run_lens})", flush=True)
            print(f"    Will attempt prefix matching in step [4]...", flush=True)
        else:
            print(f"\n    ✗ MISMATCH: serverList does NOT match running servers", flush=True)

            # Count how many serverList entries we found addresses for
            serverlist_with_addr = sum(1 for uid in result['serverlist_uids']
                                       if result['serverlist_details'].get(uid, {}).get('address'))
            print(f"\n    Extracted addresses from {serverlist_with_addr}/{len(result['serverlist_uids'])} serverList entries", flush=True)

            if result['only_in_serverlist']:
                print(f"\n    STALE serverList entries (not running):", flush=True)
                for uid in sorted(result['only_in_serverlist'], key=lambda x: x.hex())[:10]:
                    details = result['serverlist_details'].get(uid, {})
                    addr = details.get('address', 'unknown')
                    size = details.get('interface_size', 0)
                    print(f"      {short_uid(uid)} -> {addr} (size={size})", flush=True)
                if len(result['only_in_serverlist']) > 10:
                    print(f"      ... and {len(result['only_in_serverlist']) - 10} more", flush=True)

            if result['only_running']:
                print(f"\n    MISSING from serverList (running but not registered):", flush=True)
                for uid in sorted(result['only_running'], key=lambda x: x.hex())[:10]:
                    details = result['running_details'].get(uid, {})
                    addr = details.get('address', 'unknown')
                    print(f"      {short_uid(uid)} -> {addr}", flush=True)
                if len(result['only_running']) > 10:
                    print(f"      ... and {len(result['only_running']) - 10} more", flush=True)

        # Show a hex dump of one serverList entry for debugging (skip if length mismatch - step 4 handles it)
        if result['serverlist_uids'] and not result['length_mismatch']:
            sample_uid = sorted(result['serverlist_uids'], key=lambda x: x.hex())[0]
            sample_value = serverlist.get(sample_uid, b'')
            details = result['serverlist_details'].get(sample_uid, {})
            print(f"\n    Sample serverList value (first entry):", flush=True)
            print(f"      UID: {sample_uid.hex()}", flush=True)
            print(f"      Size: {details.get('interface_size', 0)} bytes", flush=True)
            print(f"      First 64 bytes: {details.get('interface_hex', 'N/A')}", flush=True)

            # Search for IP addresses in serverList values - be more precise
            print(f"\n    Searching for IP addresses in serverList values...", flush=True)

            # First, extract the IP prefixes from running servers to know what to look for
            running_addrs = set()
            running_ip_prefixes = set()  # First two octets
            for uid in result['running_uids']:
                addr = result['running_details'].get(uid, {}).get('address', '')
                if addr:
                    running_addrs.add(addr)
                    parts = addr.split(':')[0].split('.')
                    if len(parts) >= 2:
                        running_ip_prefixes.add((int(parts[0]), int(parts[1])))

            print(f"      Running servers use IP prefixes: {sorted(running_ip_prefixes)}", flush=True)
            print(f"      Running servers use port: 4501", flush=True)

            # Search for IPs with those specific prefixes AND port 4501
            all_ips_found = {}  # ip -> list of UIDs that have it
            port_4501_ips = {}  # IPs found with port 4501 specifically

            for uid, value in serverlist.items():
                # Method 1: Look for running server IP prefixes (e.g., 100.103 = 0x64 0x67)
                for prefix in running_ip_prefixes:
                    b0, b1 = prefix
                    for i in range(len(value) - 5):
                        if value[i] == b0 and value[i+1] == b1:
                            b2, b3 = value[i+2], value[i+3]
                            if b2 < 256 and b3 < 256:  # Valid IP octets
                                if i + 6 <= len(value):
                                    port = struct.unpack('<H', value[i+4:i+6])[0]
                                    ip_str = f"{b0}.{b1}.{b2}.{b3}:{port}"
                                    if ip_str not in all_ips_found:
                                        all_ips_found[ip_str] = []
                                    all_ips_found[ip_str].append(uid)
                                    if port == 4501:
                                        if ip_str not in port_4501_ips:
                                            port_4501_ips[ip_str] = []
                                        port_4501_ips[ip_str].append(uid)

                # Method 2: Search for port 4501 (0x95 0x11 in little-endian) and look backwards for IP
                port_bytes = struct.pack('<H', 4501)  # b'\x95\x11'
                idx = 0
                while True:
                    idx = value.find(port_bytes, idx)
                    if idx == -1 or idx < 4:
                        break
                    # Check if 4 bytes before could be an IP
                    b0, b1, b2, b3 = value[idx-4], value[idx-3], value[idx-2], value[idx-1]
                    # Check if it looks like a valid private/CGNAT IP
                    if (b0 == 10 or (b0 == 100 and 64 <= b1 <= 127) or
                        (b0 == 172 and 16 <= b1 <= 31) or (b0 == 192 and b1 == 168)):
                        ip_str = f"{b0}.{b1}.{b2}.{b3}:4501"
                        if ip_str not in port_4501_ips:
                            port_4501_ips[ip_str] = []
                        port_4501_ips[ip_str].append(uid)
                        if ip_str not in all_ips_found:
                            all_ips_found[ip_str] = []
                        all_ips_found[ip_str].append(uid)
                    idx += 1

            # Report findings
            if port_4501_ips:
                print(f"\n      Found {len(port_4501_ips)} IPs with port 4501 in serverList:", flush=True)
                for ip in sorted(port_4501_ips.keys())[:15]:
                    count = len(port_4501_ips[ip])
                    match = "✓ MATCH" if ip in running_addrs else ""
                    print(f"        {ip} ({count} entries) {match}", flush=True)
                if len(port_4501_ips) > 15:
                    print(f"        ... and {len(port_4501_ips) - 15} more", flush=True)

                # Check matches
                matching = set(port_4501_ips.keys()) & running_addrs
                if matching:
                    print(f"\n      ✓ {len(matching)} serverList IPs MATCH running servers!", flush=True)
                    print(f"        Matching IPs:", flush=True)
                    for ip in sorted(matching)[:10]:
                        print(f"          {ip}", flush=True)
                    print(f"\n        This confirms servers restarted with NEW UIDs on SAME IPs", flush=True)
                    result['serverlist_ips'] = set(port_4501_ips.keys())
                    result['matching_ips'] = matching
                else:
                    print(f"\n      ✗ NO serverList IPs match running servers", flush=True)

                    # Show what's different
                    serverlist_ip_bases = set(ip.split(':')[0] for ip in port_4501_ips.keys())
                    running_ip_bases = set(ip.split(':')[0] for ip in running_addrs)
                    print(f"        serverList IP range: {sorted(serverlist_ip_bases)[:5]}...", flush=True)
                    print(f"        Running IP range: {sorted(running_ip_bases)[:5]}...", flush=True)
            else:
                print(f"      No IPs with port 4501 found in serverList", flush=True)

                # Search for ANY FDB-like ports (4500-4510)
                print(f"\n      Searching for any FDB ports (4500-4510)...", flush=True)
                fdb_port_ips = {}
                for uid, value in serverlist.items():
                    for port in range(4500, 4511):
                        port_bytes = struct.pack('<H', port)
                        idx = 0
                        while True:
                            idx = value.find(port_bytes, idx)
                            if idx == -1 or idx < 4:
                                break
                            b0, b1, b2, b3 = value[idx-4], value[idx-3], value[idx-2], value[idx-1]
                            # Check for plausible private/CGNAT IP (filter out false positives like x.0.0.0)
                            # Must be private/CGNAT range AND have non-zero octets
                            is_valid_ip = False
                            if b1 != 0 or b2 != 0 or b3 != 0:  # Filter out x.0.0.0 patterns
                                if b0 == 10:  # 10.x.x.x
                                    is_valid_ip = True
                                elif b0 == 100 and 64 <= b1 <= 127:  # CGNAT 100.64-127.x.x
                                    is_valid_ip = True
                                elif b0 == 172 and 16 <= b1 <= 31:  # 172.16-31.x.x
                                    is_valid_ip = True
                                elif b0 == 192 and b1 == 168:  # 192.168.x.x
                                    is_valid_ip = True
                            if is_valid_ip:
                                ip_str = f"{b0}.{b1}.{b2}.{b3}:{port}"
                                if ip_str not in fdb_port_ips:
                                    fdb_port_ips[ip_str] = []
                                fdb_port_ips[ip_str].append(uid)
                            idx += 1

                if fdb_port_ips:
                    print(f"      Found {len(fdb_port_ips)} IPs with FDB ports:", flush=True)
                    for ip in sorted(fdb_port_ips.keys())[:20]:
                        count = len(fdb_port_ips[ip])
                        print(f"        {ip} ({count} entries)", flush=True)
                    if len(fdb_port_ips) > 20:
                        print(f"        ... and {len(fdb_port_ips) - 20} more", flush=True)

                    # Extract unique subnets
                    subnets = set()
                    for ip in fdb_port_ips.keys():
                        parts = ip.split(':')[0].split('.')
                        if len(parts) == 4:
                            subnets.add(f"{parts[0]}.{parts[1]}.x.x")
                    print(f"\n      Subnets in serverList: {sorted(subnets)}", flush=True)
                    print(f"      Running servers subnet: 100.103.x.x", flush=True)

                    if '100.103.x.x' in subnets:
                        print(f"      ✓ SAME SUBNET - servers may have restarted with new UIDs", flush=True)
                    else:
                        print(f"      ✗ DIFFERENT SUBNET - serverList is for different machines", flush=True)
                else:
                    print(f"      No FDB ports (4500-4510) found in serverList", flush=True)

                    # Last resort: dump raw bytes around typical address offsets
                    print(f"\n      Raw search for common IP patterns...", flush=True)
                    found_any = False
                    for uid, value in list(serverlist.items())[:3]:
                        # Try to find any sequence that looks like IP:port
                        # FDB often stores NetworkAddress with IP then port
                        for i in range(min(200, len(value) - 6)):
                            b0, b1, b2, b3 = value[i], value[i+1], value[i+2], value[i+3]
                            port = struct.unpack('<H', value[i+4:i+6])[0]
                            # Look for IPs starting with 10, 100, 172, 192 AND reasonable FDB ports
                            if b0 in (10, 100, 172, 192) and 4500 <= port <= 4510:
                                print(f"        UID {short_uid(uid)} offset {i}: {b0}.{b1}.{b2}.{b3}:{port}", flush=True)
                                found_any = True

                    if not found_any:
                        print(f"      No IP:port patterns found in first 200 bytes", flush=True)
                        print(f"\n      serverList values may use a different serialization format", flush=True)
                        print(f"      or contain addresses for a completely different network", flush=True)

    # Step 4: Try to match by first 8 bytes (fdbcli only shows first half of UID)
    if not result['match']:
        print(f"\n[4] Checking if fdbcli shows truncated UIDs (first 8 bytes only)...", flush=True)

        # Build map of first 8 bytes -> full UID for serverList
        serverlist_by_prefix = {}
        for uid in result['serverlist_uids']:
            prefix = uid[:8]
            serverlist_by_prefix[prefix] = uid

        # Check if running UIDs (8 bytes) match the first 8 bytes of serverList UIDs
        matches_by_prefix = {}
        for running_uid in result['running_uids']:
            if running_uid in serverlist_by_prefix:
                full_uid = serverlist_by_prefix[running_uid]
                matches_by_prefix[running_uid] = full_uid

        if matches_by_prefix:
            print(f"    FOUND {len(matches_by_prefix)} matches by UID prefix!", flush=True)
            print(f"    fdbcli is showing truncated UIDs (first 8 bytes only)", flush=True)
            print(f"\n    Matches:", flush=True)
            for partial_uid, full_uid in list(matches_by_prefix.items())[:5]:
                addr = result['running_details'].get(partial_uid, {}).get('address', 'unknown')
                print(f"      {partial_uid.hex()} -> {full_uid.hex()} ({addr})", flush=True)
            if len(matches_by_prefix) > 5:
                print(f"      ... and {len(matches_by_prefix) - 5} more", flush=True)

            # Update result
            result['matches_by_prefix'] = matches_by_prefix
            result['partial_match'] = len(matches_by_prefix) == len(result['running_uids'])

            if result['partial_match']:
                print(f"\n    ✓ ALL {len(matches_by_prefix)} running servers found in serverList by prefix match!", flush=True)
                print(f"    serverList appears CORRECT (fdbcli just shows truncated UIDs)", flush=True)
                print(f"\n    NOTE: Any earlier 'DIFFERENT SUBNET' warnings can be ignored.", flush=True)
                print(f"    The IP extraction from binary data may produce false positives.", flush=True)
        else:
            print(f"    No prefix matches found - UIDs are genuinely different", flush=True)

    # Step 5: Try to match by address (only if prefix match failed)
    if not result['match'] and not result.get('partial_match') and result['only_in_serverlist'] and result['only_running']:
        print(f"\n[5] Attempting to match stale->running by address...", flush=True)

        # Build address -> UID maps
        serverlist_by_addr = {}
        for uid in result['only_in_serverlist']:
            details = result['serverlist_details'].get(uid, {})
            addr = details.get('address')
            if addr:
                # Normalize address (remove port for comparison)
                base_addr = addr.split(':')[0] if ':' in addr else addr
                serverlist_by_addr[base_addr] = uid

        running_by_addr = {}
        for uid in result['only_running']:
            details = result['running_details'].get(uid, {})
            addr = details.get('address', '')
            if addr:
                base_addr = addr.split(':')[0] if ':' in addr else addr
                running_by_addr[base_addr] = uid

        # Find matches
        addr_matches = {}
        for addr, serverlist_uid in serverlist_by_addr.items():
            if addr in running_by_addr:
                running_uid = running_by_addr[addr]
                addr_matches[serverlist_uid] = {
                    'running_uid': running_uid,
                    'address': addr,
                }

        if addr_matches:
            print(f"    Found {len(addr_matches)} potential matches by address:", flush=True)
            for old_uid, match in list(addr_matches.items())[:5]:
                new_uid = match['running_uid']
                addr = match['address']
                print(f"      {short_uid(old_uid)} -> {short_uid(new_uid)} (same IP: {addr})", flush=True)
            if len(addr_matches) > 5:
                print(f"      ... and {len(addr_matches) - 5} more", flush=True)

            print(f"\n    This suggests servers restarted with NEW UIDs.", flush=True)
            print(f"    serverList may need to be updated (or SS re-registration failed).", flush=True)
        else:
            print(f"    No address matches found - servers may be on different IPs.", flush=True)
    elif result.get('partial_match'):
        print(f"\n[5] Skipping address match - prefix match already succeeded", flush=True)

    # Step 6: Scan serverKeys to find full UIDs
    print(f"\n[6] Scanning serverKeys for full 16-byte UIDs...", flush=True)

    @fdb.transactional
    def scan_serverkeys_uids(tr):
        tr.options.set_read_system_keys()
        tr.options.set_lock_aware()
        tr.options.set_timeout(60000)

        serverkeys_uids = {}  # uid -> count of entries
        start = SERVER_KEYS_PREFIX
        end = SERVER_KEYS_END

        count = 0
        for k, v in tr.get_range(start, end, limit=10000000):  # Scan up to 10M entries
            count += 1
            if count % 100000 == 0:
                print(f"      ... scanned {count} entries", flush=True)
            remainder = k[len(SERVER_KEYS_PREFIX):]
            if len(remainder) >= 16:
                server_uid = remainder[:16]
                if server_uid not in serverkeys_uids:
                    serverkeys_uids[server_uid] = 0
                serverkeys_uids[server_uid] += 1

        return serverkeys_uids, count

    try:
        serverkeys_uids, total_entries = scan_serverkeys_uids(db)
        print(f"    Scanned {total_entries} serverKeys entries", flush=True)
        print(f"    Found {len(serverkeys_uids)} unique server UIDs in serverKeys", flush=True)

        result['serverkeys_uids'] = set(serverkeys_uids.keys())

        # Show sample UIDs
        print(f"\n    Sample serverKeys UIDs (full 16-byte):", flush=True)
        for uid in sorted(serverkeys_uids.keys(), key=lambda x: -serverkeys_uids[x])[:5]:
            count = serverkeys_uids[uid]
            print(f"      {uid.hex()} ({count} entries)", flush=True)

        # Check if any serverKeys UIDs match running servers (by 8-byte prefix)
        print(f"\n    Checking if serverKeys UIDs match running servers (by 8-byte prefix)...", flush=True)
        matches_by_prefix = {}
        for sk_uid in serverkeys_uids.keys():
            prefix = sk_uid[:8]
            for running_uid in result['running_uids']:
                if running_uid == prefix:
                    matches_by_prefix[running_uid] = {
                        'full_uid': sk_uid,
                        'entries': serverkeys_uids[sk_uid],
                        'address': result['running_details'].get(running_uid, {}).get('address', 'unknown'),
                    }

        if matches_by_prefix:
            print(f"\n    ✓ FOUND {len(matches_by_prefix)} matches!", flush=True)
            print(f"    Running servers' full 16-byte UIDs:", flush=True)
            for partial_uid, info in sorted(matches_by_prefix.items(), key=lambda x: x[0].hex()):
                full_uid = info['full_uid']
                addr = info['address']
                entries = info['entries']
                print(f"      {partial_uid.hex()} -> {full_uid.hex()} @ {addr} ({entries} serverKeys entries)", flush=True)

            result['running_full_uids'] = {v['full_uid']: k for k, v in matches_by_prefix.items()}
            result['prefix_to_full'] = {k: v['full_uid'] for k, v in matches_by_prefix.items()}

            # Check overlap with serverList
            serverkeys_set = set(serverkeys_uids.keys())
            serverlist_set = result['serverlist_uids']
            overlap = serverkeys_set & serverlist_set

            print(f"\n    serverKeys UIDs: {len(serverkeys_set)}", flush=True)
            print(f"    serverList UIDs: {len(serverlist_set)}", flush=True)
            print(f"    Overlap: {len(overlap)}", flush=True)

            if overlap:
                print(f"    Some serverKeys UIDs ARE in serverList", flush=True)
            else:
                print(f"    ✗ NO overlap between serverKeys and serverList UIDs", flush=True)
                print(f"    serverList is completely stale", flush=True)

        else:
            print(f"    ✗ No prefix matches found", flush=True)
            print(f"    serverKeys UIDs don't match running servers either", flush=True)

            # Check if serverKeys UIDs match serverList UIDs
            serverkeys_set = set(serverkeys_uids.keys())
            serverlist_set = result['serverlist_uids']
            overlap = serverkeys_set & serverlist_set

            print(f"\n    Checking serverKeys vs serverList UIDs:", flush=True)
            print(f"      serverKeys UIDs: {len(serverkeys_set)}", flush=True)
            print(f"      serverList UIDs: {len(serverlist_set)}", flush=True)
            print(f"      Overlap: {len(overlap)}", flush=True)

            if overlap:
                print(f"      serverKeys and serverList share {len(overlap)} UIDs", flush=True)
            else:
                print(f"      ✗ NO overlap - three completely disjoint sets!", flush=True)

    except fdb.FDBError as e:
        print(f"    ERROR scanning serverKeys: {e}", flush=True)
    except Exception as e:
        print(f"    ERROR: {e}", flush=True)

    print("\n" + "=" * 60, flush=True)

    return result


def verify_ss_ownership(db, ranges_by_old_server, old_to_new_mapping, running_servers, sample_count=0):
    """
    Verify that running SSs actually have the data by using get_addresses_for_key().

    This queries FDB's live routing to see which SS holds each key,
    then compares against our expected mapping.

    sample_count: 0 = verify all ranges, >0 = sample that many
    """
    print("\n[SS Ownership Verification]", flush=True)
    print(f"  Verifying that running SSs actually have the expected data...", flush=True)
    print(f"  Using get_addresses_for_key() to query live routing", flush=True)

    # Quick test to verify the API works
    print(f"  Testing fdb.locality.get_addresses_for_key() with a sample key...", flush=True)
    try:
        test_tr = db.create_transaction()
        test_tr.options.set_timeout(10000)
        test_key = b'\x00'  # A simple key
        test_future = fdb.locality.get_addresses_for_key(test_tr, test_key)
        test_addrs = test_future.wait()  # Must wait on the future!
        print(f"    Test succeeded: key {test_key!r} -> {list(test_addrs)}", flush=True)
    except fdb.FDBError as e:
        print(f"    Test FAILED: FDBError {e.code}: {e.description}", flush=True)
        print(f"    get_addresses_for_key() may not work in this cluster state", flush=True)
    except AttributeError as e:
        print(f"    Test FAILED: fdb.locality not available in this FDB version", flush=True)
        print(f"    Error: {e}", flush=True)
    except Exception as e:
        print(f"    Test FAILED: {e}", flush=True)

    # Build reverse mapping: new server ID -> address
    new_id_to_addr = {}
    for new_id, info in running_servers.items():
        addr = info.get('address', '')
        if addr:
            new_id_to_addr[new_id] = addr

    # Build address -> new server ID mapping
    addr_to_new_id = {v: k for k, v in new_id_to_addr.items()}

    # Collect all ranges with their expected owners
    all_ranges = []
    for old_id, ranges in ranges_by_old_server.items():
        new_id = old_to_new_mapping.get(old_id)
        if new_id:
            for start, end in ranges:
                all_ranges.append((start, end, old_id, new_id))

    print(f"  Total ranges to verify: {len(all_ranges)}", flush=True)

    # Sample or use all
    import random
    if sample_count > 0 and len(all_ranges) > sample_count:
        check_ranges = random.sample(all_ranges, sample_count)
        print(f"  Sampling {len(check_ranges)} ranges...", flush=True)
    else:
        check_ranges = all_ranges
        print(f"  Checking ALL {len(check_ranges)} ranges...", flush=True)

    verified = 0
    mismatched = 0
    errors = 0
    mismatch_details = []
    error_samples = []  # Capture first few errors for debugging

    # Categorize mismatches
    mismatch_blog = 0
    mismatch_userspace = 0
    mismatch_system = 0
    mismatch_by_expected_server = {}  # expected_addr -> count

    def check_key_ownership_direct(db, key):
        """Query get_addresses_for_key using fdb.locality API."""
        tr = db.create_transaction()
        tr.options.set_timeout(10000)  # 10 second timeout
        try:
            # Use fdb.locality.get_addresses_for_key() - returns a Future
            future = fdb.locality.get_addresses_for_key(tr, key)
            addresses = future.wait()  # Must wait on the future!
            return list(addresses), None  # Convert to list
        except fdb.FDBError as e:
            return None, f"FDBError {e.code}: {e.description}"
        except AttributeError as e:
            return None, f"fdb.locality not available: {e}"
        except Exception as e:
            return None, str(e)

    total_to_check = len(check_ranges)
    progress_interval = max(1, total_to_check // 50)  # Report ~50 times

    for i, (start, end, old_id, expected_new_id) in enumerate(check_ranges):
        if i % progress_interval == 0:
            print(f"    ... verified {i}/{total_to_check} ranges ({100*i//total_to_check}%)", flush=True)

        # Pick a key in the middle of the range
        if start and end and start < end:
            # Simple midpoint - just use start key + a byte
            sample_key = start + b'\x00' if start else b'\x00'
        else:
            sample_key = start if start else b'\x00'

        try:
            addresses, err = check_key_ownership_direct(db, sample_key)

            if addresses is None:
                errors += 1
                if len(error_samples) < 5:
                    error_samples.append({'key': sample_key, 'error': err})
                continue

            # Check if expected server is in the list
            expected_addr = new_id_to_addr.get(expected_new_id, '')
            expected_ip = expected_addr.split(':')[0] if ':' in expected_addr else expected_addr

            found_expected = False
            actual_addrs = []
            # addresses may be a list of strings or bytes depending on FDB version
            if isinstance(addresses, (list, tuple)):
                for addr in addresses:
                    addr_str = addr.decode() if isinstance(addr, bytes) else str(addr)
                    actual_addrs.append(addr_str)
                    addr_ip = addr_str.split(':')[0] if ':' in addr_str else addr_str
                    if addr_ip == expected_ip:
                        found_expected = True
            else:
                # Single address or string
                addr_str = addresses.decode() if isinstance(addresses, bytes) else str(addresses)
                actual_addrs.append(addr_str)
                addr_ip = addr_str.split(':')[0] if ':' in addr_str else addr_str
                if addr_ip == expected_ip:
                    found_expected = True

            if found_expected:
                verified += 1
            else:
                mismatched += 1

                # Categorize the mismatch
                if sample_key.startswith(b'\xff\x02/blog'):
                    mismatch_blog += 1
                elif sample_key.startswith(b'\xff'):
                    mismatch_system += 1
                else:
                    mismatch_userspace += 1

                # Track by expected server
                if expected_addr not in mismatch_by_expected_server:
                    mismatch_by_expected_server[expected_addr] = {'count': 0, 'samples': []}
                mismatch_by_expected_server[expected_addr]['count'] += 1
                if len(mismatch_by_expected_server[expected_addr]['samples']) < 3:
                    mismatch_by_expected_server[expected_addr]['samples'].append({
                        'key': sample_key,
                        'actual_addrs': actual_addrs,
                    })

                if len(mismatch_details) < 20:
                    mismatch_details.append({
                        'key': sample_key,
                        'expected_id': expected_new_id,
                        'expected_addr': expected_addr,
                        'actual_addrs': actual_addrs,
                        'is_blog': sample_key.startswith(b'\xff\x02/blog'),
                        'is_system': sample_key.startswith(b'\xff'),
                    })
        except Exception as e:
            errors += 1

    print(f"\n  Results:", flush=True)
    print(f"    Verified (SS has data): {verified}", flush=True)
    print(f"    Mismatched (different SS): {mismatched}", flush=True)
    print(f"    Errors: {errors}", flush=True)

    if mismatched > 0:
        print(f"\n  Mismatch breakdown:", flush=True)
        print(f"    Blog shards (\\xff\\x02/blog): {mismatch_blog}", flush=True)
        print(f"    User-space shards: {mismatch_userspace}", flush=True)
        print(f"    Other system shards (\\xff): {mismatch_system}", flush=True)

        print(f"\n  Mismatches by expected server:", flush=True)
        sorted_servers = sorted(mismatch_by_expected_server.items(), key=lambda x: -x[1]['count'])
        for addr, info in sorted_servers[:10]:
            print(f"    {addr}: {info['count']} mismatches", flush=True)
        if len(sorted_servers) > 10:
            print(f"    ... and {len(sorted_servers) - 10} more servers", flush=True)

    if error_samples:
        print(f"\n  Error samples:", flush=True)
        for e in error_samples:
            print(f"    Key: {e['key'][:30]!r}...", flush=True)
            print(f"      Error: {e['error']}", flush=True)

    if mismatch_details:
        print(f"\n  Mismatch sample details:", flush=True)
        # Show blog mismatches
        blog_mismatches = [m for m in mismatch_details if m.get('is_blog')]
        userspace_mismatches = [m for m in mismatch_details if not m.get('is_system')]
        system_mismatches = [m for m in mismatch_details if m.get('is_system') and not m.get('is_blog')]

        if userspace_mismatches:
            print(f"\n    User-space mismatches:", flush=True)
            for m in userspace_mismatches[:5]:
                print(f"      Key: {m['key'][:40]!r}...", flush=True)
                print(f"        Expected: {m['expected_addr']}", flush=True)
                print(f"        Actual: {m['actual_addrs']}", flush=True)

        if blog_mismatches:
            print(f"\n    Blog mismatches:", flush=True)
            for m in blog_mismatches[:5]:
                print(f"      Key: {m['key'][:40]!r}...", flush=True)
                print(f"        Expected: {m['expected_addr']}", flush=True)
                print(f"        Actual: {m['actual_addrs']}", flush=True)

        if system_mismatches:
            print(f"\n    Other system mismatches:", flush=True)
            for m in system_mismatches[:3]:
                print(f"      Key: {m['key'][:40]!r}...", flush=True)
                print(f"        Expected: {m['expected_addr']}", flush=True)
                print(f"        Actual: {m['actual_addrs']}", flush=True)

    if verified > 0 and mismatched == 0:
        print(f"\n  VERIFICATION PASSED: Running SSs have the expected data!", flush=True)
        return {'verified': verified, 'mismatched': 0, 'errors': errors}
    elif mismatched > 0:
        print(f"\n  PARTIAL VERIFICATION: {100*verified//(verified+mismatched)}% of ranges on expected SS", flush=True)

        # Provide repair recommendations
        print(f"\n  Repair recommendations:", flush=True)
        if mismatch_blog > 0 and mismatch_userspace == 0:
            print(f"    ALL mismatches are blog shards ({mismatch_blog} ranges).", flush=True)
            print(f"    Option 1: Rebuild keyServers without these blog ranges (drop them)", flush=True)
            print(f"    Option 2: Query actual SSs for these ranges and update mapping", flush=True)
        elif mismatch_userspace > 0:
            print(f"    {mismatch_userspace} USER-SPACE mismatches - these need careful handling!", flush=True)
            print(f"    The actual SS addresses are known (from get_addresses_for_key).", flush=True)
            print(f"    Option: Use actual addresses instead of expected for these ranges", flush=True)

        return {
            'verified': verified,
            'mismatched': mismatched,
            'errors': errors,
            'mismatch_blog': mismatch_blog,
            'mismatch_userspace': mismatch_userspace,
            'mismatch_system': mismatch_system,
            'mismatch_by_server': mismatch_by_expected_server,
            'mismatch_details': mismatch_details,
        }
    else:
        print(f"\n  Could not verify (too many errors).", flush=True)
        return {'verified': 0, 'mismatched': 0, 'errors': errors}


def probe_ss_for_shards(db, sample_ranges, sample_count=100):
    """
    Actually probe storage servers by reading keys to detect wrong_shard_server.

    This detects cases where:
    - Metadata (keyServers + serverKeys) says SS should have a shard
    - But SS doesn't actually have it locally (returns wrong_shard_server error 1037)

    sample_ranges: list of (range_start, range_end) tuples to probe
    sample_count: max number of ranges to probe (0 = all)

    Returns dict with probe results.
    """
    print("\n[SS Shard Probe - Actual Read Test]", flush=True)
    print("  Probing SSs with actual reads to detect missing shards...", flush=True)
    print("  (This detects when metadata says SS has shard but SS doesn't have it)", flush=True)

    import random

    if not sample_ranges:
        print("  No ranges to probe.", flush=True)
        return {'probed': 0, 'ok': 0, 'wrong_shard': 0, 'errors': 0}

    # Sample if requested
    if sample_count > 0 and len(sample_ranges) > sample_count:
        ranges_to_probe = random.sample(sample_ranges, sample_count)
        print(f"  Sampling {len(ranges_to_probe)} of {len(sample_ranges)} ranges...", flush=True)
    else:
        ranges_to_probe = sample_ranges
        print(f"  Probing all {len(ranges_to_probe)} ranges...", flush=True)

    probed = 0
    ok_count = 0
    wrong_shard_count = 0
    error_count = 0
    wrong_shard_details = []
    error_by_code = {}  # error_code -> count
    error_samples = []  # sample of errors for debugging

    progress_interval = max(1, min(500, len(ranges_to_probe) // 20))  # Report every 500 or ~20 times

    for start, end in ranges_to_probe:
        probed += 1

        # Pick a key in the middle of the range
        if start and end and start < end:
            # Use start key + some offset
            sample_key = start + b'\x00'
        else:
            sample_key = start if start else b'\x00'

        # Try to read the key - retry on timeout since all-replica-missing manifests as timeout
        last_error = None
        success = False
        for attempt in range(2):  # Up to 2 attempts (reduced from 3)
            try:
                tr = db.create_transaction()
                tr.options.set_timeout(10000)  # 10 second timeout
                # Enable system key access for \xff prefixed ranges
                if sample_key.startswith(b'\xff'):
                    tr.options.set_read_system_keys()

                # Do a simple get - if SS doesn't have the shard, we'll get wrong_shard_server
                result = tr.get(sample_key).wait()

                # If we got here, SS has the shard (even if key doesn't exist)
                ok_count += 1
                success = True
                break

            except fdb.FDBError as e:
                last_error = e
                if e.code == 1037:  # wrong_shard_server - no point retrying
                    break
                elif e.code in (1007, 1009, 1031):  # timeout errors - retry
                    continue
                else:
                    break  # other errors - don't retry
            except Exception as e:
                last_error = e
                break

        if not success and last_error is not None:
            if isinstance(last_error, fdb.FDBError):
                if last_error.code == 1037:  # wrong_shard_server
                    wrong_shard_count += 1
                    if len(wrong_shard_details) < 20:
                        wrong_shard_details.append({
                            'range_start': start,
                            'range_end': end,
                            'probe_key': sample_key,
                            'error': 'wrong_shard_server',
                        })
                elif last_error.code in (1007, 1009, 1031):  # persistent timeout after retries
                    # Persistent timeout likely means all replicas are missing the shard
                    wrong_shard_count += 1
                    if len(wrong_shard_details) < 20:
                        wrong_shard_details.append({
                            'range_start': start,
                            'range_end': end,
                            'probe_key': sample_key,
                            'error': f'persistent_timeout_{last_error.code} (likely all replicas missing)',
                        })
                else:
                    error_count += 1
                    error_by_code[last_error.code] = error_by_code.get(last_error.code, 0) + 1
                    if len(error_samples) < 10:
                        error_samples.append({
                            'code': last_error.code,
                            'description': str(last_error),
                            'key': sample_key,
                        })
            else:
                error_count += 1
                error_by_code[-1] = error_by_code.get(-1, 0) + 1
                if len(error_samples) < 10:
                    error_samples.append({
                        'code': -1,
                        'description': str(last_error),
                        'key': sample_key,
                    })

        if probed % progress_interval == 0:
            print(f"    ... probed {probed}/{len(ranges_to_probe)} ranges ({100*probed//len(ranges_to_probe)}%)", flush=True)

    # Count actual wrong_shard_server vs timeouts
    actual_wrong_shard = sum(1 for d in wrong_shard_details if d.get('error') == 'wrong_shard_server')
    timeout_count = sum(1 for d in wrong_shard_details if 'persistent_timeout' in d.get('error', ''))

    print(f"\n  Probe Results:", flush=True)
    print(f"    Total probed: {probed}", flush=True)
    print(f"    SS has shard: {ok_count}", flush=True)
    print(f"    wrong_shard_server (SS explicitly missing): {actual_wrong_shard}", flush=True)
    print(f"    Timeout (SS slow or overloaded): {timeout_count}", flush=True)
    print(f"    Other errors: {error_count}", flush=True)

    if error_count > 0 and error_by_code:
        print(f"\n  Error breakdown by code:", flush=True)
        for code, count in sorted(error_by_code.items(), key=lambda x: -x[1]):
            # Common FDB error codes
            code_names = {
                1007: 'transaction_timed_out',
                1009: 'request_timed_out',
                1031: 'transaction_timed_out',
                1034: 'accessed_unreadable',
                1037: 'wrong_shard_server',
                1039: 'storage_server_overloaded',
                2012: 'commit_proxy_memory_limit_exceeded',
                -1: 'non_fdb_exception',
            }
            name = code_names.get(code, f'error_{code}')
            print(f"      {name} ({code}): {count}", flush=True)

        if error_samples:
            print(f"\n  Error samples:", flush=True)
            for e in error_samples[:5]:
                print(f"      Code {e['code']}: {e['description'][:60]}", flush=True)
                print(f"        Key: {e['key'][:30]!r}...", flush=True)

    if wrong_shard_count > 0:
        # Count direct wrong_shard vs timeout-inferred
        direct_wrong_shard = sum(1 for d in wrong_shard_details if d.get('error') == 'wrong_shard_server')
        timeout_inferred = sum(1 for d in wrong_shard_details if 'persistent_timeout' in d.get('error', ''))

        if direct_wrong_shard > 0:
            print(f"\n  CRITICAL: {direct_wrong_shard} shards MISSING (SS explicitly returned wrong_shard_server)!", flush=True)
            print(f"    This indicates DATA LOSS or metadata corruption.", flush=True)
        if timeout_inferred > 0:
            print(f"\n  WARNING: {timeout_inferred} shards TIMED OUT (SS didn't respond in time)", flush=True)
            print(f"    Could be: slow SS, large shard, network issue, or missing data.", flush=True)
            print(f"    Re-run with longer timeout to confirm.", flush=True)

        print(f"\n  Sample missing shard ranges:", flush=True)
        for detail in wrong_shard_details[:10]:
            err_type = detail.get('error', 'wrong_shard_server')
            print(f"    [{detail['range_start'][:30]!r}..., {detail['range_end'][:30]!r}...) - {err_type}", flush=True)
    elif error_count == 0:
        print(f"\n  All probed SSs have the expected shards (no wrong_shard_server).", flush=True)
    else:
        print(f"\n  No wrong_shard_server errors, but {error_count} other errors occurred.", flush=True)
        print(f"    These may be transient (timeouts) or indicate cluster health issues.", flush=True)

    return {
        'probed': probed,
        'ok': ok_count,
        'wrong_shard': wrong_shard_count,
        'errors': error_count,
        'error_by_code': error_by_code,
        'wrong_shard_details': wrong_shard_details,
    }


def probe_with_metrics(db, sample_ranges, sample_count=0):
    """
    Probe ranges using get_estimated_range_size_bytes() - the same metrics path DD uses.

    This tests the actual waitStorageMetrics code path that DD uses, not just point reads.
    If DD gets wrong_shard_server on metrics, this should show the same errors.
    """
    print("\n[Metrics Probe - Same path as DD waitStorageMetrics]", flush=True)
    print("  Using get_estimated_range_size_bytes() to test metrics path...", flush=True)

    import random

    if not sample_ranges:
        print("  No ranges to probe.", flush=True)
        return {'probed': 0, 'ok': 0, 'wrong_shard': 0, 'timeout': 0, 'errors': 0}

    # Sample if requested
    if sample_count > 0 and len(sample_ranges) > sample_count:
        ranges_to_probe = random.sample(sample_ranges, sample_count)
        print(f"  Sampling {len(ranges_to_probe)} of {len(sample_ranges)} ranges...", flush=True)
    else:
        ranges_to_probe = sample_ranges
        print(f"  Probing all {len(ranges_to_probe)} ranges...", flush=True)

    probed = 0
    ok_count = 0
    wrong_shard_count = 0
    timeout_count = 0
    error_count = 0
    error_by_code = {}
    wrong_shard_details = []

    progress_interval = max(1, min(500, len(ranges_to_probe) // 20))

    # Track system key ranges separately - clients can't probe them
    skipped_system_keys = 0
    skipped_blog_ranges = 0
    skipped_backup_ranges = 0

    for start, end in ranges_to_probe:
        probed += 1

        # Track system key ranges that clients can't access via get_estimated_range_size_bytes
        # These MUST be checked via metadata analysis (check_wrong_shard_server) instead
        if start and start[:1] == b'\xff':
            skipped_system_keys += 1
            if start.startswith(b'\xff\x02/blog/'):
                skipped_blog_ranges += 1
            elif start.startswith(b'\xff\x02/backup') or start.startswith(b'\xff\x02/fdbbackup'):
                skipped_backup_ranges += 1
            continue

        last_error = None
        success = False

        for attempt in range(2):  # 2 attempts
            try:
                tr = db.create_transaction()
                tr.options.set_timeout(15000)  # 15 second timeout for metrics

                # This is the key test - get_estimated_range_size_bytes uses
                # the same internal path as DD's waitStorageMetrics
                size = tr.get_estimated_range_size_bytes(start, end).wait()

                # If we get here, SS has the shard and returned metrics
                ok_count += 1
                success = True
                break

            except fdb.FDBError as e:
                last_error = e
                if e.code == 1037:  # wrong_shard_server - no point retrying
                    break
                elif e.code in (1007, 1009, 1031):  # timeout errors - retry once
                    continue
                else:
                    break
            except Exception as e:
                last_error = e
                break

        if not success and last_error is not None:
            if isinstance(last_error, fdb.FDBError):
                if last_error.code == 1037:
                    wrong_shard_count += 1
                    if len(wrong_shard_details) < 20:
                        wrong_shard_details.append({
                            'range_start': start,
                            'range_end': end,
                            'error': 'wrong_shard_server',
                        })
                elif last_error.code in (1007, 1009, 1031):
                    timeout_count += 1
                    if len(wrong_shard_details) < 20:
                        wrong_shard_details.append({
                            'range_start': start,
                            'range_end': end,
                            'error': f'timeout_{last_error.code}',
                        })
                else:
                    error_count += 1
                    error_by_code[last_error.code] = error_by_code.get(last_error.code, 0) + 1
            else:
                error_count += 1
                error_by_code[-1] = error_by_code.get(-1, 0) + 1

        if probed % progress_interval == 0:
            print(f"    ... probed {probed}/{len(ranges_to_probe)} (ok={ok_count}, wrong_shard={wrong_shard_count}, timeout={timeout_count})", flush=True)

    print(f"\n  Metrics Probe Results:", flush=True)
    print(f"    Total probed: {probed}", flush=True)
    print(f"    Metrics OK: {ok_count}", flush=True)
    print(f"    wrong_shard_server: {wrong_shard_count}", flush=True)
    print(f"    Timeout: {timeout_count}", flush=True)
    print(f"    Other errors: {error_count}", flush=True)

    if skipped_system_keys > 0:
        print(f"\n  WARNING: Skipped {skipped_system_keys} system key ranges (clients can't probe \\xff...)", flush=True)
        print(f"    Blog ranges skipped:   {skipped_blog_ranges}", flush=True)
        print(f"    Backup ranges skipped: {skipped_backup_ranges}", flush=True)
        print(f"    Other system ranges:   {skipped_system_keys - skipped_blog_ranges - skipped_backup_ranges}", flush=True)
        print(f"    --> Use metadata analysis (check_wrong_shard_server) for system key issues", flush=True)

    if wrong_shard_count > 0:
        print(f"\n  CRITICAL: {wrong_shard_count} ranges returned wrong_shard_server on metrics!", flush=True)
        print(f"    This is what DD experiences - these shards cause DD to loop.", flush=True)
        print(f"\n  Sample ranges with wrong_shard_server:", flush=True)
        for detail in wrong_shard_details[:10]:
            if detail.get('error') == 'wrong_shard_server':
                print(f"    [{detail['range_start'][:30]!r}..., {detail['range_end'][:30]!r}...)", flush=True)

    if timeout_count > 0:
        print(f"\n  WARNING: {timeout_count} ranges timed out on metrics request.", flush=True)
        print(f"    These may be slow SSs or overloaded - not necessarily missing data.", flush=True)

    return {
        'probed': probed,
        'ok': ok_count,
        'wrong_shard': wrong_shard_count,
        'timeout': timeout_count,
        'errors': error_count,
        'error_by_code': error_by_code,
        'wrong_shard_details': wrong_shard_details,
        'skipped_system_keys': skipped_system_keys,
        'skipped_blog_ranges': skipped_blog_ranges,
        'skipped_backup_ranges': skipped_backup_ranges,
    }


def build_serverkeys_map_for_servers(tr, server_ids, limit=1000000):
    """
    Build a map of server_id -> list of (start_key, end_key) ranges
    for the specified server IDs.
    """
    server_ranges = {sid: [] for sid in server_ids}

    for server_id in server_ids:
        prefix = SERVER_KEYS_PREFIX + server_id + b'/'
        prev_key = None
        prev_owns = False

        for k, v in tr.get_range(prefix, strinc(prefix), limit=limit):
            user_key = k[len(SERVER_KEYS_PREFIX) + 17:]  # skip prefix + 16-byte ID + /
            owns = (v == b'1' or v == b'\x01')

            if prev_key is not None and prev_owns:
                # Previous entry started a range, this entry ends it
                server_ranges[server_id].append((prev_key, user_key))

            prev_key = user_key
            prev_owns = owns

    return server_ranges


def analyze_serverlist_vs_running(tr, serverlist_servers, running_servers):
    """
    Analyze the relationship between serverList servers and running servers.
    Try to find a mapping based on patterns in the data.
    """
    print("\n[ServerList vs Running Analysis]")

    # Get serverList server details
    sl_details = {}
    for sid, value in serverlist_servers.items():
        # The serverList value contains the StorageServerInterface
        # which includes the address
        sl_details[sid] = {'value_len': len(value), 'value_prefix': value[:20].hex() if value else ''}

    # Get running server details
    run_details = {}
    for sid, info in running_servers.items():
        run_details[sid] = info

    print(f"  serverList servers: {len(sl_details)}")
    print(f"  Running servers: {len(run_details)}")

    # Try to find address matches
    # Running servers have addresses like "100.103.8.50:4501"
    # serverList values contain serialized StorageServerInterface which includes address

    # For now, just report what we have
    print("\n  Running server addresses:")
    for sid, info in list(run_details.items())[:5]:
        print(f"    {short_uid(sid)}: {info.get('address', 'unknown')}")
    if len(run_details) > 5:
        print(f"    ... and {len(run_details) - 5} more")

    return sl_details, run_details


def try_extract_address_from_serverlist_value(value):
    """
    Try to extract IP address from serverList value.
    The value is a serialized StorageServerInterface.
    """
    # StorageServerInterface serialization includes the NetworkAddress
    # which contains IP and port. Look for IP-like patterns.
    import re

    # Convert to string and look for IP patterns
    try:
        # Look for IPv4 pattern in the bytes
        text = value.decode('latin-1')
        ip_pattern = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        matches = re.findall(ip_pattern, text)
        return matches[0] if matches else None
    except:
        return None


def attempt_server_id_mapping(serverlist_servers, running_servers):
    """
    Try to map old server IDs (in serverList) to new server IDs (running).
    Uses address matching if possible.
    """
    print("\n[Server ID Mapping Attempt]", flush=True)

    # Build address -> running server ID map
    print("    Building running server address map...", flush=True)
    addr_to_running = {}
    for sid, info in running_servers.items():
        addr = info.get('address', '')
        if addr:
            # Extract just IP (without port) for matching
            ip = addr.split(':')[0] if ':' in addr else addr
            addr_to_running[ip] = sid

    print(f"    Running servers with addresses: {len(addr_to_running)}", flush=True)

    # Try to extract addresses from serverList values
    print("    Extracting addresses from serverList values...", flush=True)
    mapping = {}
    unmatched_serverlist = []
    unmatched_running = set(running_servers.keys())

    for i, (old_sid, value) in enumerate(serverlist_servers.items()):
        if i % 10 == 0 and i > 0:
            print(f"    ... processed {i}/{len(serverlist_servers)} serverList entries", flush=True)

        addr = try_extract_address_from_serverlist_value(value)
        if addr and addr in addr_to_running:
            new_sid = addr_to_running[addr]
            mapping[old_sid] = new_sid
            unmatched_running.discard(new_sid)
        else:
            unmatched_serverlist.append(old_sid)

    print(f"  Successful mappings (old -> new): {len(mapping)}", flush=True)
    print(f"  Unmatched serverList servers: {len(unmatched_serverlist)}", flush=True)
    print(f"  Unmatched running servers: {len(unmatched_running)}", flush=True)

    if mapping:
        print("\n  Sample mappings:", flush=True)
        for old_sid, new_sid in list(mapping.items())[:5]:
            new_info = running_servers.get(new_sid, {})
            print(f"    {short_uid(old_sid)} -> {short_uid(new_sid)} @ {new_info.get('address', '?')}", flush=True)

    return mapping, unmatched_serverlist, list(unmatched_running)


def get_serverkeys_ranges_for_orphan_servers(tr, orphan_server_ids, limit=100000):
    """
    Get the shard ranges claimed by the 'orphan' servers (those in serverList
    but not in keyServers). These might be the correct ranges, just with stale IDs.
    """
    print("\n[Orphan Server Ranges]", flush=True)
    print("  (These servers are in serverList but not referenced in keyServers)", flush=True)
    print(f"  Scanning serverKeys for {len(orphan_server_ids)} orphan servers...", flush=True)

    all_ranges = {}
    total_ranges = 0
    server_list = list(orphan_server_ids)

    for i, server_id in enumerate(server_list):
        if i % 10 == 0:
            print(f"    ... processing server {i+1}/{len(server_list)}", flush=True)

        prefix = SERVER_KEYS_PREFIX + server_id + b'/'
        ranges = []
        prev_key = None
        prev_owns = False

        for k, v in tr.get_range(prefix, strinc(prefix), limit=limit):
            user_key = k[len(prefix):]
            owns = (v == b'1' or v == b'\x01')

            if prev_key is not None and prev_owns:
                ranges.append((prev_key, user_key))

            prev_key = user_key
            prev_owns = owns

        if ranges:
            all_ranges[server_id] = ranges
            total_ranges += len(ranges)

    print(f"  Found {total_ranges} ranges across {len(all_ranges)} orphan servers", flush=True)

    # Show sample
    for sid in list(all_ranges.keys())[:3]:
        ranges = all_ranges[sid]
        print(f"\n    Server {short_uid(sid)}: {len(ranges)} ranges", flush=True)
        for r in ranges[:2]:
            print(f"      [{r[0][:25]!r}..., {r[1][:25]!r}...)", flush=True)
        if len(ranges) > 2:
            print(f"      ... and {len(ranges) - 2} more", flush=True)

    return all_ranges


def compare_server_sets(keyservers_servers, serverlist_servers, serverkeys_servers, running_servers):
    """Compare server IDs across different sources.

    NOTE: running_servers uses 8-byte UID prefixes (from status JSON conversion),
    while other sources use full 16-byte UIDs. We compare by 8-byte prefix.
    """
    print("\n[Server ID Comparison]")

    ks_set = set(keyservers_servers) if keyservers_servers else set()
    sl_set = set(serverlist_servers.keys()) if serverlist_servers else set()
    sk_set = set(serverkeys_servers) if serverkeys_servers else set()
    run_set = set(running_servers.keys()) if running_servers else set()

    # Build 8-byte prefix sets for comparison with running_servers (which has 8-byte keys)
    sl_prefix_set = set(sid[:8] for sid in sl_set if len(sid) >= 8)
    ks_prefix_set = set(sid[:8] for sid in ks_set if len(sid) >= 8)
    sk_prefix_set = set(sid[:8] for sid in sk_set if len(sid) >= 8)

    print(f"  Servers in keyServers:  {len(ks_set)}")
    print(f"  Servers in serverList:  {len(sl_set)}")
    print(f"  Servers in serverKeys:  {len(sk_set)}")
    print(f"  Servers actually running: {len(run_set)}")

    # Key comparisons (using 8-byte prefixes for running_servers comparison)
    print("\n  Comparisons (using 8-byte UID prefixes):")

    # Running vs serverList (should match)
    run_not_in_sl = run_set - sl_prefix_set
    sl_not_in_run = sl_prefix_set - run_set
    if run_not_in_sl:
        print(f"    Running but NOT in serverList: {len(run_not_in_sl)}")
        for s in list(run_not_in_sl)[:3]:
            print(f"      - {short_uid(s)}")
    if sl_not_in_run:
        print(f"    In serverList but NOT running: {len(sl_not_in_run)}")
        for s in list(sl_not_in_run)[:3]:
            print(f"      - {short_uid(s)}")
    if not run_not_in_sl and not sl_not_in_run:
        print(f"    serverList matches running servers: YES")

    # Running vs keyServers (the key question!)
    run_in_ks = run_set & ks_prefix_set
    run_not_in_ks = run_set - ks_prefix_set
    ks_not_running = ks_prefix_set - run_set

    print(f"\n    Running servers referenced in keyServers: {len(run_in_ks)}")
    print(f"    Running servers NOT in keyServers: {len(run_not_in_ks)}")
    if run_not_in_ks:
        print(f"      (These servers have data but DD doesn't know about them!)")
        for s in list(run_not_in_ks)[:5]:
            info = running_servers.get(s, {})
            print(f"      - {short_uid(s)} @ {info.get('address', '?')} ({info.get('data_bytes', 0) / 1e9:.2f} GB)")

    print(f"    keyServers references NOT running: {len(ks_not_running)}")
    if ks_not_running:
        print(f"      (These are the 'dead' servers in keyServers)")

    # Running vs serverKeys
    run_in_sk = run_set & sk_prefix_set
    run_not_in_sk = run_set - sk_prefix_set

    print(f"\n    Running servers with serverKeys entries: {len(run_in_sk)}")
    print(f"    Running servers WITHOUT serverKeys: {len(run_not_in_sk)}")

    return {
        'keyservers_servers': ks_set,
        'serverlist_servers': sl_set,
        'serverkeys_servers': sk_set,
        'running_servers': run_set,
        'running_not_in_keyservers': run_not_in_ks,
        'keyservers_not_running': ks_not_running,
    }

def check_key_servers(tr, live_servers, key_prefix=None, limit=100000):
    """
    Check keyServers for corruption.
    Returns dict with issues found.

    Checks for issues that would trigger DD asserts:
    - ASSERT(src.size()) - empty source servers
    - ASSERT(servers.size() > 0) - no servers for range
    - ASSERT_GT(keyServers.size(), 0) - keyServers empty
    - KRM uncoalesced assertion - adjacent entries with same value
    """
    issues = {
        'uncoalesced': [],      # Adjacent entries with same value (KRM assertion)
        'dead_servers': [],      # References to servers not in serverList
        'decode_errors': [],     # Values that couldn't be decoded
        'empty_sources': [],     # Entries with no source servers (ASSERT(src.size()))
        'keyservers_empty': False,  # keyServers has no entries (ASSERT_GT)
        'coverage_gaps': [],    # Gaps in key range coverage
        # Shard statistics
        'total_shards': 0,
        'shards_all_dead': 0,      # Shards where ALL source servers are dead
        'shards_some_dead': 0,     # Shards where SOME source servers are dead
        'shards_all_live': 0,      # Shards where ALL source servers are live
        'blog_shards': 0,          # Shards in blog keyspace
        'servers_referenced': set(),  # All unique servers referenced
    }

    start = KEY_SERVERS_PREFIX + (key_prefix or b'')
    end = KEY_SERVERS_END

    prev_key = None
    prev_value = None
    prev_decoded = None
    count = 0

    for k, v in tr.get_range(start, end, limit=limit):
        count += 1
        user_key = k[len(KEY_SERVERS_PREFIX):]

        src_servers, dest_servers = decode_key_servers_value_simple(v)

        if src_servers is None:
            issues['decode_errors'].append({
                'key': user_key,
                'value_hex': v.hex(),
            })
            prev_key, prev_value, prev_decoded = k, v, None
            continue

        if not src_servers:
            # \xff\xff is the end sentinel key - having no source servers is expected
            if user_key != b'\xff\xff':
                issues['empty_sources'].append({
                    'key': user_key,
                })

        # Track shard statistics
        issues['total_shards'] += 1

        # Check if this is a blog shard
        if user_key.startswith(b'\xff\x02/blog/') or user_key.startswith(b'\xff\x02/blog'):
            issues['blog_shards'] += 1

        # Count live vs dead source servers for this shard
        live_count = 0
        dead_count = 0
        for srv in (src_servers or []):
            issues['servers_referenced'].add(srv)
            if srv in live_servers:
                live_count += 1
            else:
                dead_count += 1
                issues['dead_servers'].append({
                    'key': user_key,
                    'dead_server': srv,
                    'type': 'source',
                })

        # Categorize shard by server liveness
        if dead_count == 0 and live_count > 0:
            issues['shards_all_live'] += 1
        elif live_count == 0 and dead_count > 0:
            issues['shards_all_dead'] += 1
        elif live_count > 0 and dead_count > 0:
            issues['shards_some_dead'] += 1

        for srv in (dest_servers or []):
            issues['servers_referenced'].add(srv)
            if srv not in live_servers:
                issues['dead_servers'].append({
                    'key': user_key,
                    'dead_server': srv,
                    'type': 'destination',
                })

        # Check for uncoalesced adjacent entries
        if prev_value is not None and prev_value == v:
            issues['uncoalesced'].append({
                'key1': prev_key[len(KEY_SERVERS_PREFIX):],
                'key2': user_key,
                'value_hex': v.hex(),
                'src_servers': [short_uid(s) for s in (src_servers or [])],
            })

        prev_key = k
        prev_value = v
        prev_decoded = (src_servers, dest_servers)

    # Check if keyServers is completely empty
    if count == 0:
        issues['keyservers_empty'] = True

    return issues, count

def check_server_keys(tr, live_servers, limit=100000):
    """
    Check serverKeys for corruption.
    Returns dict with issues found.
    """
    issues = {
        'uncoalesced': [],      # Adjacent entries for same server with same value
        'dead_servers': [],      # serverKeys entries for servers not in serverList
        'malformed_uid': [],     # Server IDs that aren't 16 bytes
        # Statistics
        'total_ranges_claimed': 0,  # Total shard ranges claimed by all servers
        'blog_ranges': 0,           # Ranges in blog keyspace
        'live_server_ranges': 0,    # Ranges claimed by live servers
        'dead_server_ranges': 0,    # Ranges claimed by dead servers
    }

    start = SERVER_KEYS_PREFIX
    end = SERVER_KEYS_END

    # Track per-server for uncoalesced check
    prev_by_server = {}  # server_id -> (key, value)
    count = 0
    servers_seen = set()

    for k, v in tr.get_range(start, end, limit=limit):
        count += 1

        # Parse: \xff/serverKeys/<serverID>/<userKey>
        remainder = k[len(SERVER_KEYS_PREFIX):]

        # Server ID should be 16 bytes
        if len(remainder) < 16:
            issues['malformed_uid'].append({
                'key': k,
                'remainder_len': len(remainder),
            })
            continue

        server_id = remainder[:16]
        user_key = remainder[17:]  # Skip 16-byte UID + 1-byte "/" separator

        servers_seen.add(server_id)
        is_live = server_id in live_servers

        # Check if server exists
        if not is_live:
            issues['dead_servers'].append({
                'server_id': server_id,
                'user_key': user_key,
            })

        # Track ranges claimed - serverKeys value "1" or "\x01" means server owns starting here
        owns = (v == b'1' or v == b'\x01')

        # Count range transitions (when a server starts owning a range)
        if server_id in prev_by_server:
            prev_user_key, prev_value, prev_owns = prev_by_server[server_id]

            # If previous entry was "owns" and this entry is anything, we completed a range
            if prev_owns:
                issues['total_ranges_claimed'] += 1
                if is_live:
                    issues['live_server_ranges'] += 1
                else:
                    issues['dead_server_ranges'] += 1
                # Check if it's a blog range
                if prev_user_key.startswith(b'\xff\x02/blog'):
                    issues['blog_ranges'] += 1

            # Check for uncoalesced adjacent entries
            if prev_value == v:
                # Check if this spans the full keyspace (very wrong!)
                is_full_keyspace = (prev_user_key == b'' and user_key == b'\xff\xff')

                issues['uncoalesced'].append({
                    'server_id': server_id,
                    'key1': prev_user_key,
                    'key2': user_key,
                    'value': v,
                    'is_live': is_live,
                    'is_full_keyspace': is_full_keyspace,
                })

        prev_by_server[server_id] = (user_key, v, owns)

    return issues, count, servers_seen

def check_key_servers_for_range(tr, live_servers, key_start, key_end, verbose=False):
    """Check keyServers for a specific key range."""
    start = KEY_SERVERS_PREFIX + key_start
    end = KEY_SERVERS_PREFIX + key_end

    entries = []
    for k, v in tr.get_range(start, end, limit=10000):
        user_key = k[len(KEY_SERVERS_PREFIX):]
        src, dest = decode_key_servers_value_simple(v)
        entries.append({
            'key': user_key,
            'value': v,
            'src': src,
            'dest': dest,
        })

    if verbose:
        print(f"\nkeyServers entries for range [{key_start!r}, {key_end!r}):")
        print(f"Found {len(entries)} entries\n")

        for i, e in enumerate(entries):
            src_str = ', '.join(short_uid(s) for s in (e['src'] or []))
            dest_str = ', '.join(short_uid(s) for s in (e['dest'] or [])) if e['dest'] else ""
            print(f"  [{i}] key={e['key'][:40]!r}...")
            print(f"      src=[{src_str}] dest=[{dest_str}]")

            # Check if same as previous
            if i > 0 and entries[i-1]['value'] == e['value']:
                print(f"      *** UNCOALESCED - same value as previous entry!")

    return entries

def check_keyservers_serverkeys_mismatch(tr, live_servers, key_prefix=None, limit=100000):
    """
    Check for mismatches between keyServers and serverKeys.

    keyServers says: range [K1, K2) is owned by servers {S1, S2, S3}
    serverKeys should have: S1, S2, S3 all claiming that range

    Returns list of mismatches.
    """
    mismatches = []

    # Step 1: Build map from keyServers - what ranges are assigned to which servers
    print("    Building keyServers map...")
    ks_ranges = []  # list of (start_key, end_key, set of server_ids)

    start = KEY_SERVERS_PREFIX + (key_prefix or b'')
    end = KEY_SERVERS_END

    prev_key = None
    prev_servers = None
    count = 0

    for k, v in tr.get_range(start, end, limit=limit):
        count += 1
        user_key = k[len(KEY_SERVERS_PREFIX):]
        src_servers, dest_servers = decode_key_servers_value_simple(v)

        if prev_key is not None and prev_servers is not None:
            # Range [prev_key, user_key) is owned by prev_servers
            ks_ranges.append((prev_key, user_key, set(prev_servers)))

        prev_key = user_key
        prev_servers = src_servers if src_servers else []

    print(f"    Found {len(ks_ranges)} keyServers ranges from {count} entries")

    # Step 2: Build map from serverKeys - what ranges each server claims
    print("    Building serverKeys map...")
    sk_by_server = {}  # server_id -> list of (start_key, end_key)

    start = SERVER_KEYS_PREFIX
    end = SERVER_KEYS_END

    prev_by_server = {}  # server_id -> (prev_key, prev_value)
    sk_count = 0

    for k, v in tr.get_range(start, end, limit=limit):
        sk_count += 1
        remainder = k[len(SERVER_KEYS_PREFIX):]

        if len(remainder) < 16:
            continue

        server_id = remainder[:16]
        user_key = remainder[17:]  # Skip 16-byte UID + 1-byte "/" separator

        if server_id not in sk_by_server:
            sk_by_server[server_id] = []

        # serverKeys value: "1" means server owns this range, "" means it doesn't
        owns = (v == b'1' or v == b'\x01')

        if server_id in prev_by_server:
            prev_key, prev_owns = prev_by_server[server_id]
            if prev_owns:
                # Range [prev_key, user_key) is owned by this server
                sk_by_server[server_id].append((prev_key, user_key))

        prev_by_server[server_id] = (user_key, owns)

    print(f"    Processed {sk_count} serverKeys entries for {len(sk_by_server)} servers")

    # Step 3: Compare - for each keyServers range, check serverKeys agrees
    print("    Comparing keyServers vs serverKeys...")

    # Sample check - full comparison is expensive, so check a sample
    sample_size = min(100, len(ks_ranges))
    import random
    if len(ks_ranges) > sample_size:
        sample_ranges = random.sample(ks_ranges, sample_size)
    else:
        sample_ranges = ks_ranges

    for start_key, end_key, expected_servers in sample_ranges:
        # For each server that should own this range, check serverKeys
        for server_id in expected_servers:
            if server_id not in sk_by_server:
                mismatches.append({
                    'type': 'missing_server_in_serverkeys',
                    'range_start': start_key,
                    'range_end': end_key,
                    'server_id': server_id,
                    'detail': 'keyServers says server owns range, but server has no serverKeys entries',
                })
                continue

            # Check if any of this server's ranges overlap with [start_key, end_key)
            server_ranges = sk_by_server[server_id]
            has_overlap = False
            for sk_start, sk_end in server_ranges:
                # Check for overlap: ranges overlap if start1 < end2 and start2 < end1
                if sk_start < end_key and start_key < sk_end:
                    has_overlap = True
                    break

            if not has_overlap:
                mismatches.append({
                    'type': 'range_not_in_serverkeys',
                    'range_start': start_key,
                    'range_end': end_key,
                    'server_id': server_id,
                    'detail': 'keyServers says server owns range, but serverKeys disagrees',
                })

    # Also check reverse: servers claiming ranges not in keyServers
    # (This is expensive, so just check for servers not in keyServers at all)
    ks_all_servers = set()
    for _, _, servers in ks_ranges:
        ks_all_servers.update(servers)

    for server_id in sk_by_server:
        if server_id not in ks_all_servers and server_id in live_servers:
            if sk_by_server[server_id]:  # Has claimed ranges
                mismatches.append({
                    'type': 'server_not_in_keyservers',
                    'server_id': server_id,
                    'num_ranges': len(sk_by_server[server_id]),
                    'detail': 'Server has serverKeys entries but not referenced in keyServers',
                })

    return mismatches


def check_wrong_shard_server(tr, live_servers, key_prefix=None, limit=100000):
    """
    Check for the "wrong_shard_server" scenario from the DD Stuck story.

    This happens when:
    - keyServers says range [K1, K2) is owned by servers {S1, S2, S3}
    - But when DD asks S1/S2/S3 for that shard, they respond "wrong_shard_server"
    - This occurs because serverKeys for those servers doesn't claim that range

    This check finds keyServers entries where the listed servers don't have
    corresponding serverKeys entries claiming they own those ranges.

    Returns dict with:
      - wrong_shard_ranges: list of {range_start, range_end, servers_missing}
      - orphan_serverkeys: servers with serverKeys entries but not in any keyServers
    """
    issues = {
        'wrong_shard_ranges': [],   # Ranges where servers would respond wrong_shard_server
        'orphan_serverkeys': [],     # Servers with serverKeys but not referenced
    }

    # Step 1: Build complete map of what each server claims to own from serverKeys
    print("    [Step 1] Building serverKeys ownership map...")
    server_owns = {}  # server_id -> set of (start, end) tuples

    start = SERVER_KEYS_PREFIX
    end = SERVER_KEYS_END

    prev_by_server = {}  # server_id -> (prev_key, prev_owns)
    sk_count = 0

    for k, v in tr.get_range(start, end, limit=limit):
        sk_count += 1
        if sk_count % 100000 == 0:
            print(f"        ... processed {sk_count} serverKeys entries")

        remainder = k[len(SERVER_KEYS_PREFIX):]
        if len(remainder) < 16:
            continue

        server_id = remainder[:16]
        user_key = remainder[17:]  # Skip 16-byte UID + 1-byte "/" separator

        if server_id not in server_owns:
            server_owns[server_id] = []

        # serverKeys value: "1" or "\x01" means server owns this range
        owns = (v == b'1' or v == b'\x01')

        if server_id in prev_by_server:
            prev_key, prev_owns = prev_by_server[server_id]
            if prev_owns:
                # Server owns range [prev_key, user_key)
                server_owns[server_id].append((prev_key, user_key))

        prev_by_server[server_id] = (user_key, owns)

    print(f"    Processed {sk_count} serverKeys entries for {len(server_owns)} servers")

    # Step 2: For each keyServers range, check that all listed servers claim it
    print("    [Step 2] Checking each keyServers range against serverKeys...")

    start = KEY_SERVERS_PREFIX + (key_prefix or b'')
    end = KEY_SERVERS_END

    prev_key = None
    prev_servers = None
    ks_count = 0
    ranges_checked = 0
    all_ks_servers = set()  # All servers referenced in keyServers

    for k, v in tr.get_range(start, end, limit=limit):
        ks_count += 1
        if ks_count % 100000 == 0:
            print(f"        ... processed {ks_count} keyServers entries")

        user_key = k[len(KEY_SERVERS_PREFIX):]
        src_servers, dest_servers = decode_key_servers_value_simple(v)

        if prev_key is not None and prev_servers is not None and prev_servers:
            # Check range [prev_key, user_key) owned by prev_servers
            ranges_checked += 1

            for srv in prev_servers:
                all_ks_servers.add(srv)

                # Skip dead servers - they're a different issue
                if srv not in live_servers:
                    continue

                # Check if this server claims to own this range in serverKeys
                if srv not in server_owns:
                    # Server has NO serverKeys entries at all!
                    issues['wrong_shard_ranges'].append({
                        'range_start': prev_key,
                        'range_end': user_key,
                        'server_id': srv,
                        'reason': 'server_has_no_serverkeys_entries',
                    })
                    continue

                # Check if any of the server's ranges overlap with [prev_key, user_key)
                claimed_ranges = server_owns[srv]
                has_overlap = False

                for sk_start, sk_end in claimed_ranges:
                    # Ranges overlap if start1 < end2 AND start2 < end1
                    if sk_start < user_key and prev_key < sk_end:
                        has_overlap = True
                        break

                if not has_overlap:
                    issues['wrong_shard_ranges'].append({
                        'range_start': prev_key,
                        'range_end': user_key,
                        'server_id': srv,
                        'reason': 'server_does_not_claim_range_in_serverkeys',
                    })

        prev_key = user_key
        prev_servers = src_servers if src_servers else []

    print(f"    Checked {ranges_checked} ranges from {ks_count} keyServers entries")

    # Step 3: Find orphan serverKeys - servers that claim ranges but aren't in keyServers
    print("    [Step 3] Checking for orphan serverKeys entries...")

    for srv in server_owns:
        if srv not in all_ks_servers and srv in live_servers:
            if server_owns[srv]:  # Has claimed ranges
                issues['orphan_serverkeys'].append({
                    'server_id': srv,
                    'num_ranges': len(server_owns[srv]),
                    'sample_ranges': server_owns[srv][:3],  # Show first 3
                })

    return issues

# =============================================================================
# REPAIR FUNCTIONALITY - COMMENTED OUT UNTIL WE UNDERSTAND THE FULL PICTURE
# =============================================================================
#
# For uncoalesced KRM entries, we would delete the redundant entries:
#
#    If we have a chain like:
#      K1: V
#      K2: V  <- uncoalesced (delete this)
#      K3: V  <- uncoalesced (delete this)
#      K4: W
#
#    Deleting K2 and K3 gives:
#      K1: V  <- now covers [K1, K4)
#      K4: W
#
# BUT: We need to understand the full corruption picture before repairing.
#      Multiple issues may be related:
#      - Original issue: 50 stale blog keyServers entries (wrong_shard_server)
#      - Uncoalesced entries in itms keyspace
#      - Dead server references
#
# =============================================================================

def load_audit_json(file_path):
    """Load and parse the C++ audit_ss_shards JSON output."""
    with open(file_path, 'r') as f:
        return json.load(f)


def is_keyservers_audit(audit_data):
    """Check if this is a keyServers-based audit (vs serverKeys-based)."""
    return audit_data.get('audit_mode') == 'keyServers'


def lookup_phantom_shard_servers(db, phantom_shards, server_list=None):
    """
    Look up which servers the keyServers entries point to for phantom shards.

    Args:
        db: FDB database handle
        phantom_shards: List of phantom shard dicts from audit (with 'begin' hex key)
        server_list: Optional dict of server_uid -> server_info for address lookup

    Returns:
        List of dicts with phantom shard info including server UIDs
    """
    results = []

    def lookup_servers(db, shard_key_hex):
        """Look up keyServers entry for a shard and decode server UIDs."""
        try:
            # Convert hex key to bytes
            shard_key = bytes.fromhex(shard_key_hex)

            # Build keyServers key
            ks_key = KEY_SERVERS_PREFIX + shard_key

            # Create transaction with system key access
            tr = db.create_transaction()
            tr.options.set_read_system_keys()
            tr.options.set_lock_aware()

            val = tr.get(ks_key).wait()
            if not val:
                return None, None, "no_keyservers_entry"

            # Decode the value to get server UIDs
            src_servers, dest_servers, src_shard_id, dest_shard_id, proto = decode_key_servers_value(bytes(val))

            if src_servers is None:
                return None, None, "decode_failed"

            return src_servers, dest_servers, None
        except Exception as e:
            return None, None, str(e)

    # Build server UID to address map if server_list provided
    uid_to_address = {}
    if server_list:
        for uid, info in server_list.items():
            # Try to extract address from server info
            # The value contains serialized LocalityData with address
            try:
                # Simple heuristic: look for IP:port pattern in the value
                info_str = info.decode('latin-1') if isinstance(info, bytes) else str(info)
                import re
                match = re.search(r'(\d+\.\d+\.\d+\.\d+:\d+)', info_str)
                if match:
                    uid_to_address[uid] = match.group(1)
            except:
                pass

    for shard in phantom_shards:
        begin_hex = shard.get('begin', '')
        begin_printable = shard.get('begin_printable', '')
        end_printable = shard.get('end_printable', '')

        src_servers, dest_servers, error = lookup_servers(db, begin_hex)

        shard_info = {
            'begin': begin_hex,
            'begin_printable': begin_printable,
            'end_printable': end_printable,
            'error': error,
        }

        if src_servers:
            shard_info['src_servers'] = []
            for uid in src_servers:
                uid_hex = uid.hex()
                server_info = {'uid': uid_hex}
                # Try to find address
                if uid in uid_to_address:
                    server_info['address'] = uid_to_address[uid]
                else:
                    # Try prefix match (first 8 bytes)
                    uid_prefix = uid[:8]
                    for list_uid in uid_to_address:
                        if list_uid.startswith(uid_prefix) or uid_prefix == list_uid[:8]:
                            server_info['address'] = uid_to_address[list_uid]
                            break
                shard_info['src_servers'].append(server_info)

        if dest_servers:
            shard_info['dest_servers'] = []
            for uid in dest_servers:
                uid_hex = uid.hex()
                server_info = {'uid': uid_hex}
                if uid in uid_to_address:
                    server_info['address'] = uid_to_address[uid]
                shard_info['dest_servers'].append(server_info)

        results.append(shard_info)

    return results


def process_keyservers_audit(db, audit_data, verbose=False):
    """
    Process keyServers-based audit results.

    This audit mode queries servers based on what keyServers claims they should have,
    and identifies "phantom shards" - ranges where keyServers says data exists but
    no storage server actually has it.

    Args:
        db: FDB database handle
        audit_data: Parsed JSON from audit_ss_shards --json --use-keyservers
        verbose: Show detailed output

    Returns:
        dict with analysis results
    """
    print("\n" + "=" * 70)
    print("KEYSERVERS AUDIT REPORT: Phantom Shard Detection")
    print("=" * 70)
    print("This audit queries servers based on keyServers entries to find")
    print("ranges where keyServers claims data exists but no SS has it.")
    print()

    total = audit_data.get('total_ranges', 0)
    confirmed = audit_data.get('confirmed_ranges', 0)
    wrong_shard = audit_data.get('wrong_shard_count', 0)
    timeout = audit_data.get('timeout_count', 0)
    errors = audit_data.get('error_count', 0)
    phantom_shards = audit_data.get('phantom_shards', [])

    print(f"  Total keyServers ranges checked: {total}")
    print(f"  Confirmed (SS has data):         {confirmed}")
    print(f"  Phantom shards (NO SS has data): {wrong_shard}")
    print(f"  Inconclusive (timeout/error):    {timeout + errors}")
    print()

    if wrong_shard == 0:
        print("  SUCCESS: All keyServers entries point to valid data.")
        return {'phantom_count': 0, 'phantom_shards': []}

    print(f"  CRITICAL: {wrong_shard} PHANTOM SHARDS detected!")
    print(f"  These are keyServers entries where NO storage server has the data.")
    print(f"  This is DEFINITIVE PROOF the data doesn't exist.")
    print()

    # Categorize phantom shards
    blog_shards = []
    user_shards = []
    other_system_shards = []

    for shard in phantom_shards:
        begin_hex = shard.get('begin', '')
        begin_printable = shard.get('begin_printable', '')

        # Check if it's a blog shard (\xff\x02)
        if begin_hex.startswith('ff02') or begin_printable.startswith('\\xff\\x02'):
            blog_shards.append(shard)
        elif begin_hex.startswith('ff') or begin_printable.startswith('\\xff'):
            other_system_shards.append(shard)
        else:
            user_shards.append(shard)

    print(f"  Phantom shard breakdown:")
    print(f"    Blog ranges (\\xff\\x02/blog/):  {len(blog_shards)}")
    print(f"    Other system ranges:           {len(other_system_shards)}")
    print(f"    User data ranges:              {len(user_shards)}")
    print()

    if blog_shards:
        print(f"  Blog phantom shards (first 10):")
        for shard in blog_shards[:10]:
            begin = shard.get('begin_printable', shard.get('begin', ''))
            end = shard.get('end_printable', shard.get('end', ''))
            print(f"    [{begin[:60]}...")
        if len(blog_shards) > 10:
            print(f"    ... and {len(blog_shards) - 10} more")
        print()

    if user_shards:
        print(f"  WARNING: User data phantom shards detected!")
        print(f"  User phantom shards (first 10):")
        for shard in user_shards[:10]:
            begin = shard.get('begin_printable', shard.get('begin', ''))
            end = shard.get('end_printable', shard.get('end', ''))
            print(f"    [{begin[:60]}...")
        if len(user_shards) > 10:
            print(f"    ... and {len(user_shards) - 10} more")
        print()

    # Look up which servers the phantom shard keyServers entries point to
    print("  Looking up server assignments for phantom shards...")
    try:
        # Get server list for address mapping (needs system key access)
        def get_servers_with_system_access(db):
            tr = db.create_transaction()
            tr.options.set_read_system_keys()
            tr.options.set_lock_aware()
            servers = {}
            start = SERVER_LIST_PREFIX
            end = SERVER_LIST_END
            for k, v in tr.get_range(start, end):
                server_id = k[len(SERVER_LIST_PREFIX):]
                servers[server_id] = v
            return servers

        server_list = get_servers_with_system_access(db)
        phantom_server_info = lookup_phantom_shard_servers(db, phantom_shards, server_list)

        print()
        print("  PHANTOM SHARD SERVER ASSIGNMENTS:")
        print("  " + "-" * 60)

        # Collect all unique server UIDs across all phantom shards
        all_src_servers = {}  # uid -> {'address': ..., 'count': N}

        for i, shard_info in enumerate(phantom_server_info):
            begin = shard_info.get('begin_printable', shard_info.get('begin', ''))[:50]
            print(f"\n  Phantom shard {i+1}: [{begin}...")

            if shard_info.get('error'):
                print(f"    ERROR: {shard_info['error']}")
                continue

            src_servers = shard_info.get('src_servers', [])
            dest_servers = shard_info.get('dest_servers', [])

            if src_servers:
                print(f"    Source servers ({len(src_servers)}):")
                for srv in src_servers:
                    uid = srv.get('uid', 'unknown')
                    addr = srv.get('address', 'unknown')
                    print(f"      - {uid[:16]}... @ {addr}")

                    # Track for summary
                    if uid not in all_src_servers:
                        all_src_servers[uid] = {'address': addr, 'count': 0}
                    all_src_servers[uid]['count'] += 1
            else:
                print(f"    Source servers: NONE (empty)")

            if dest_servers:
                print(f"    Dest servers ({len(dest_servers)}):")
                for srv in dest_servers:
                    uid = srv.get('uid', 'unknown')
                    addr = srv.get('address', 'unknown')
                    print(f"      - {uid[:16]}... @ {addr}")

        # Print summary of all servers involved
        print()
        print("  " + "-" * 60)
        print(f"  SUMMARY: {len(all_src_servers)} unique servers assigned to phantom shards:")
        for uid, info in sorted(all_src_servers.items(), key=lambda x: -x[1]['count']):
            print(f"    {uid[:16]}... @ {info['address']:20} ({info['count']} phantom shards)")

    except Exception as e:
        print(f"  ERROR looking up phantom shard servers: {e}")
        import traceback
        traceback.print_exc()

    return {
        'phantom_count': wrong_shard,
        'phantom_shards': phantom_shards,
        'blog_shards': blog_shards,
        'user_shards': user_shards,
        'other_system_shards': other_system_shards,
    }


def repair_keyservers_phantom_shards(db, audit_data, dry_run=True):
    """
    Delete keyServers entries for phantom shards (ranges where no SS has data).

    This removes keyServers entries that point to non-existent data, which
    unblocks DD from trying to move/serve these ranges.

    Args:
        db: FDB database handle
        audit_data: Parsed JSON from audit_ss_shards --json --use-keyservers
        dry_run: If True, only show what would be deleted

    Returns:
        dict with repair results
    """
    print("\n" + "=" * 70)
    if dry_run:
        print("REPAIR PREVIEW: Delete keyServers entries for phantom shards")
    else:
        print("REPAIR EXECUTE: Deleting keyServers entries for phantom shards")
    print("=" * 70)

    phantom_shards = audit_data.get('phantom_shards', [])
    if not phantom_shards:
        print("  No phantom shards to repair.")
        return {'deleted': 0}

    print(f"  Phantom shards to delete: {len(phantom_shards)}")
    print()

    # keyServers prefix
    keyservers_prefix = b'\xff/keyServers/'

    deleted = 0
    errors = 0

    # Process in batches
    batch_size = 100
    for i in range(0, len(phantom_shards), batch_size):
        batch = phantom_shards[i:i+batch_size]

        if dry_run:
            for shard in batch:
                begin_hex = shard.get('begin', '')
                begin_printable = shard.get('begin_printable', begin_hex)
                print(f"  Would delete: {begin_printable[:70]}...")
                deleted += 1
        else:
            try:
                @fdb.transactional
                def delete_batch(tr):
                    tr.options.set_access_system_keys()
                    tr.options.set_lock_aware()
                    tr.options.set_timeout(60000)
                    tr.options.set_priority_system_immediate()
                    update_movekeys_lock_write(tr)
                    count = 0
                    for shard in batch:
                        begin_hex = shard.get('begin', '')
                        try:
                            begin_key = bytes.fromhex(begin_hex)
                            ks_key = keyservers_prefix + begin_key
                            tr.clear(ks_key)
                            count += 1
                        except Exception as e:
                            print(f"  ERROR parsing key {begin_hex}: {e}")
                    return count

                deleted += delete_batch(db)
                print(f"  Deleted batch {i//batch_size + 1}: {len(batch)} entries")
            except Exception as e:
                print(f"  ERROR deleting batch: {e}")
                errors += len(batch)

    print()
    if dry_run:
        print(f"  DRY RUN: Would delete {deleted} keyServers entries")
        print()
        print("  To execute this repair, run with --execute-repair:")
        print("    python3 check_krm_corruption.py -C cluster.file \\")
        print("        --repair-from-audit audit.json --repair-keyservers --execute-repair")
    else:
        print(f"  DELETED {deleted} keyServers entries")
        if errors:
            print(f"  ERRORS: {errors} entries failed to delete")

    return {'deleted': deleted, 'errors': errors}


def hex_to_bytes(hex_str):
    """Convert hex string to bytes."""
    return bytes.fromhex(hex_str)


def generate_audit_comparison_report(db, audit_data, verbose=False):
    """
    Generate a detailed report comparing SS actual state (from audit) vs metadata.

    This is a DIAGNOSTIC ONLY function - no repairs are made.

    The report shows:
    1. Summary statistics
    2. Per-server comparison: what metadata claims vs what SS actually has
    3. Categorization of discrepancies

    Args:
        db: FDB database handle
        audit_data: Parsed JSON from audit_ss_shards --json
        verbose: If True, show all ranges; if False, show samples

    Returns:
        dict with report data
    """
    print("\n" + "=" * 70)
    print("AUDIT COMPARISON REPORT: Metadata vs Storage Server State")
    print("=" * 70)
    print("MODE: DIAGNOSTIC ONLY (no changes)")
    print()

    report = {
        'total_servers': 0,
        'servers_with_issues': 0,
        'total_missing_from_ss': 0,
        'total_extra_in_ss': 0,
        'server_details': [],
        'metadata_vs_ss': {},  # server_id -> {metadata_ranges, ss_ranges, missing, extra}
    }

    # Get audit summary
    report['total_servers'] = audit_data.get('total_servers', 0)
    report['reachable_servers'] = audit_data.get('reachable_servers', 0)
    report['unreachable_servers'] = audit_data.get('unreachable_servers', 0)
    report['servers_with_mismatches'] = audit_data.get('servers_with_mismatches', 0)
    report['total_missing_from_ss'] = audit_data.get('total_missing_from_ss', 0)
    report['total_extra_in_ss'] = audit_data.get('total_extra_in_ss', 0)

    print(f"[Summary from C++ Audit Tool]")
    print(f"  Total servers in serverList:    {report['total_servers']}")
    print(f"  Reachable (responded to RPC):   {report['reachable_servers']}")
    print(f"  Unreachable (no response):      {report['unreachable_servers']}")
    print(f"  Servers with mismatches:        {report['servers_with_mismatches']}")
    print()

    # Categorize servers
    servers_perfect = []      # SS state matches metadata exactly
    servers_missing = []      # Metadata says SS has shards, but SS doesn't have them
    servers_extra = []        # SS has shards that metadata doesn't know about
    servers_unreachable = []  # Couldn't contact SS

    for server in audit_data.get('servers', []):
        server_id = server.get('server_id', '')
        address = server.get('address', '')
        reachable = server.get('reachable', False)
        missing = server.get('missing_from_ss', [])
        extra = server.get('extra_in_ss', [])
        # C++ tool outputs 'metadata_ranges' and 'ss_shards'
        metadata_ranges_count = server.get('metadata_ranges', 0)
        ss_reported_count = server.get('ss_shards', 0)

        server_info = {
            'server_id': server_id,
            'address': address,
            'reachable': reachable,
            'metadata_ranges_count': metadata_ranges_count,
            'ss_reported_count': ss_reported_count,
            'missing_count': len(missing),
            'extra_count': len(extra),
            'missing_ranges': missing,
            'extra_ranges': extra,
        }
        report['server_details'].append(server_info)

        if not reachable:
            servers_unreachable.append(server_info)
        elif len(missing) == 0 and len(extra) == 0:
            servers_perfect.append(server_info)
        else:
            if len(missing) > 0:
                servers_missing.append(server_info)
            if len(extra) > 0:
                servers_extra.append(server_info)

    # Print categorized results
    print(f"[Server Categories]")
    print(f"  Servers with matching state:  {len(servers_perfect)}")
    print(f"  Servers missing shards:       {len(servers_missing)} (metadata says they should have shards they don't)")
    print(f"  Servers with extra shards:    {len(servers_extra)} (SS has shards metadata doesn't know about)")
    print(f"  Unreachable servers:          {len(servers_unreachable)}")
    print()

    # Detailed per-server report
    if servers_missing:
        print("=" * 70)
        print("SERVERS WITH MISSING SHARDS (Metadata claims SS has shards it doesn't)")
        print("=" * 70)
        print()
        print("  These are STALE serverKeys entries. The metadata (serverKeys) claims")
        print("  these storage servers own certain key ranges, but when we directly")
        print("  queried the SS via getShardState RPC, it reported it doesn't have them.")
        print()
        print("  This is likely the cause of 'wrong_shard_server' errors and DD looping.")
        print()

        total_missing_ranges = 0
        for s in servers_missing:
            print(f"  Server: {s['server_id']}")
            print(f"    Address: {s['address']}")
            print(f"    Metadata says: {s['metadata_ranges_count']} ranges")
            print(f"    SS reports:    {s['ss_reported_count']} ranges")
            print(f"    MISSING:       {s['missing_count']} ranges (metadata has, SS doesn't)")
            total_missing_ranges += s['missing_count']

            if verbose:
                # Show all missing ranges
                for r in s['missing_ranges']:
                    begin = r.get('begin', '')
                    end = r.get('end', '')
                    print(f"      Missing: [{begin[:60]}, {end[:60]})")
            else:
                # Show first 3 samples
                for r in s['missing_ranges'][:3]:
                    begin = r.get('begin', '')
                    end = r.get('end', '')
                    print(f"      Missing: [{begin[:60]}, {end[:60]})")
                if len(s['missing_ranges']) > 3:
                    print(f"      ... and {len(s['missing_ranges']) - 3} more missing ranges")
            print()

        print(f"  TOTAL MISSING RANGES: {total_missing_ranges}")
        print()

    if servers_extra:
        print("=" * 70)
        print("SERVERS WITH EXTRA SHARDS (SS has shards metadata doesn't claim)")
        print("=" * 70)
        print()
        print("  These are ORPHANED shards. The storage server has data that metadata")
        print("  (serverKeys) doesn't know about. This could be leftover from incomplete")
        print("  moves or other issues.")
        print()

        total_extra_ranges = 0
        for s in servers_extra:
            print(f"  Server: {s['server_id']}")
            print(f"    Address: {s['address']}")
            print(f"    Metadata says: {s['metadata_ranges_count']} ranges")
            print(f"    SS reports:    {s['ss_reported_count']} ranges")
            print(f"    EXTRA:         {s['extra_count']} ranges (SS has, metadata doesn't)")
            total_extra_ranges += s['extra_count']

            if verbose:
                for r in s['extra_ranges']:
                    begin = r.get('begin', '')
                    end = r.get('end', '')
                    print(f"      Extra: [{begin[:60]}, {end[:60]})")
            else:
                for r in s['extra_ranges'][:3]:
                    begin = r.get('begin', '')
                    end = r.get('end', '')
                    print(f"      Extra: [{begin[:60]}, {end[:60]})")
                if len(s['extra_ranges']) > 3:
                    print(f"      ... and {len(s['extra_ranges']) - 3} more extra ranges")
            print()

        print(f"  TOTAL EXTRA RANGES: {total_extra_ranges}")
        print()

    if servers_unreachable:
        print("=" * 70)
        print("UNREACHABLE SERVERS")
        print("=" * 70)
        print()
        print("  These servers are in serverList but could not be contacted.")
        print("  They may be down, or network issues prevented communication.")
        print()

        for s in servers_unreachable:
            print(f"  Server: {s['server_id']}")
            print(f"    Address: {s['address']}")
            print()

    if servers_perfect:
        print("=" * 70)
        print("SERVERS WITH MATCHING STATE")
        print("=" * 70)
        print()
        print(f"  {len(servers_perfect)} servers have SS state that matches metadata exactly.")
        if verbose:
            for s in servers_perfect:
                print(f"    {s['server_id']} @ {s['address']} - {s['metadata_ranges_count']} ranges")
        else:
            for s in servers_perfect[:5]:
                print(f"    {s['server_id']} @ {s['address']} - {s['metadata_ranges_count']} ranges")
            if len(servers_perfect) > 5:
                print(f"    ... and {len(servers_perfect) - 5} more")
        print()

    # Analysis summary
    print("=" * 70)
    print("ANALYSIS SUMMARY")
    print("=" * 70)
    print()

    if report['total_missing_from_ss'] == 0 and report['total_extra_in_ss'] == 0:
        print("  All reachable storage servers match metadata exactly.")
        print("  No discrepancies detected between SS state and serverKeys metadata.")
    else:
        print(f"  DISCREPANCIES FOUND:")
        print()
        if report['total_missing_from_ss'] > 0:
            print(f"    {report['total_missing_from_ss']} ranges: metadata claims SS has shards, but SS doesn't")
            print(f"      Impact: These cause 'wrong_shard_server' errors. DD will loop forever")
            print(f"              trying to route to SSs that don't have the data.")
            print(f"      Root cause: Likely stale serverKeys entries from interrupted data moves.")
            print(f"      Fix: Delete stale serverKeys entries (--repair-serverkeys)")
            print()

        if report['total_extra_in_ss'] > 0:
            print(f"    {report['total_extra_in_ss']} ranges: SS has shards that metadata doesn't claim")
            print(f"      Impact: Orphaned data consuming storage. Generally less critical.")
            print(f"      Root cause: Incomplete cleanup after data moves.")
            print(f"      Fix: SS garbage collection should eventually clean these up.")
            print()

    # Report on metrics test results (DD waitStorageMetrics path)
    metrics = audit_data.get('metrics')
    if metrics:
        print("=" * 70)
        print("METRICS PATH TEST (Same as DD waitStorageMetrics)")
        print("=" * 70)
        print()
        print("  This tests the exact code path DD uses to get shard sizes.")
        print("  If a shard returns wrong_shard_server here, DD will loop forever.")
        print()

        total_ranges = metrics.get('total_ranges', 0)
        ok_count = metrics.get('ok_count', 0)
        wrong_shard_count = metrics.get('wrong_shard_count', 0)
        timeout_count = metrics.get('timeout_count', 0)
        error_count = metrics.get('error_count', 0)
        total_bytes = metrics.get('total_bytes', 0)

        print(f"  Total ranges tested:     {total_ranges}")
        print(f"  Metrics OK:              {ok_count}")
        print(f"  wrong_shard_server:      {wrong_shard_count}")
        print(f"  Timeout:                 {timeout_count}")
        print(f"  Other errors:            {error_count}")
        print(f"  Total data size:         {total_bytes:,} bytes ({total_bytes / (1024**3):.2f} GB)")
        print()

        # Add metrics to report
        report['metrics'] = {
            'total_ranges': total_ranges,
            'ok_count': ok_count,
            'wrong_shard_count': wrong_shard_count,
            'timeout_count': timeout_count,
            'error_count': error_count,
            'total_bytes': total_bytes,
        }

        if wrong_shard_count > 0:
            print(f"  CRITICAL: {wrong_shard_count} ranges returned wrong_shard_server!")
            print(f"  DD cannot get metrics for these ranges and will loop forever.")
            print()

            failed_shards = metrics.get('failed_shards', [])
            if failed_shards:
                print("  Failing ranges (first 20):")
                for shard in failed_shards[:20]:
                    begin = shard.get('begin', '')[:40]
                    end = shard.get('end', '')[:40]
                    print(f"    [{begin}..., {end}...)")
                if len(failed_shards) > 20:
                    print(f"    ... and {len(failed_shards) - 20} more")
                print()

                report['metrics']['failed_shards'] = failed_shards

        elif timeout_count > 0:
            print(f"  WARNING: {timeout_count} ranges timed out.")
            print(f"  These may or may not cause DD issues - storage servers may be slow.")
            print()

        else:
            print("  SUCCESS: All ranges returned metrics successfully.")
            print("  DD should be able to get metrics for all shards.")
            print()

        # Show shard size distribution if we have shards data
        shards = metrics.get('shards', [])
        if shards and verbose:
            print("  Shard size distribution:")
            # Calculate size buckets
            size_buckets = {
                '< 1MB': 0,
                '1-10MB': 0,
                '10-100MB': 0,
                '100MB-1GB': 0,
                '> 1GB': 0,
            }
            for shard in shards:
                if not shard.get('ok', False):
                    continue
                bytes_size = shard.get('bytes', 0)
                if bytes_size < 1024 * 1024:
                    size_buckets['< 1MB'] += 1
                elif bytes_size < 10 * 1024 * 1024:
                    size_buckets['1-10MB'] += 1
                elif bytes_size < 100 * 1024 * 1024:
                    size_buckets['10-100MB'] += 1
                elif bytes_size < 1024 * 1024 * 1024:
                    size_buckets['100MB-1GB'] += 1
                else:
                    size_buckets['> 1GB'] += 1

            for bucket, count in size_buckets.items():
                if count > 0:
                    print(f"    {bucket}: {count} shards")
            print()

    # Check for correlation with serverKeys
    if servers_missing:
        print("=" * 70)
        print("NEXT STEPS FOR REPAIR")
        print("=" * 70)
        print()
        print("  To repair the stale serverKeys entries (remove metadata claims for")
        print("  shards the SS doesn't actually have):")
        print()
        print("  1. Save the audit output to a file:")
        print("     ./audit_ss_shards -C fdb.cluster --json > audit.json")
        print()
        print("  2. Dry-run to see what would be repaired:")
        print("     python3 check_krm_corruption.py -c fdb.cluster \\")
        print("         --repair-from-audit audit.json --repair-serverkeys")
        print()
        print("  3. Execute repair (CAREFUL!):")
        print("     python3 check_krm_corruption.py -c fdb.cluster \\")
        print("         --repair-from-audit audit.json --repair-serverkeys --execute-repair")
        print()

    return report


def uid_cpp_to_raw(cpp_hex):
    """
    Convert C++ UID::toString() format to raw bytes format.

    FDB UID::toString() outputs two little-endian uint64_t values as hex.
    Raw bytes are the actual byte sequence stored in keys.

    Example:
        C++ format: aed3b039325af7048594d1c449f765db
        Raw bytes:  04f75a3239b0d3aedb65f749c4d19485

    The conversion reverses the byte order within each 8-byte half.
    """
    if len(cpp_hex) != 32:
        return None

    # Split into two 16-char (8-byte) halves
    first_hex = cpp_hex[:16]   # First uint64 in LE format
    second_hex = cpp_hex[16:]  # Second uint64 in LE format

    # Reverse byte order within each half (LE uint64 to raw bytes)
    first_bytes = bytes.fromhex(first_hex)
    second_bytes = bytes.fromhex(second_hex)

    # Reverse to get raw byte order
    raw_bytes = first_bytes[::-1] + second_bytes[::-1]
    return raw_bytes


def uid_raw_to_cpp(raw_bytes):
    """
    Convert raw UID bytes to C++ UID::toString() format.
    """
    if len(raw_bytes) != 16:
        return None

    # Split into two 8-byte halves and reverse each
    first_raw = raw_bytes[:8][::-1]
    second_raw = raw_bytes[8:][::-1]

    return first_raw.hex() + second_raw.hex()


def correlate_audit_with_metadata(audit_data, serverkeys_by_server, keyservers_data=None):
    """
    Correlate audit findings with metadata analysis.

    This provides a unified view showing how audit (SS actual state) relates
    to the metadata (serverKeys/keyServers) analysis.

    Args:
        audit_data: Parsed JSON from audit_ss_shards
        serverkeys_by_server: Dict of server_id_bytes -> list of ranges from serverKeys
        keyservers_data: Optional dict with keyServers analysis results

    Returns:
        dict with correlation results
    """
    print("\n" + "=" * 70)
    print("CORRELATION: Audit Results vs Metadata Analysis")
    print("=" * 70)
    print()

    correlation = {
        'servers_in_audit': set(),
        'servers_in_serverkeys': set(),
        'servers_in_both': set(),
        'audit_only': set(),
        'serverkeys_only': set(),
        'stale_serverkeys_confirmed': 0,
        'stale_by_server': {},
    }

    # Build set of server IDs from audit
    # NOTE: Audit uses C++ UID::toString() format, serverKeys uses raw bytes
    # We need to convert audit IDs to raw bytes format to match
    audit_cpp_to_raw = {}  # Maps C++ format -> raw bytes
    for server in audit_data.get('servers', []):
        server_id_hex = server.get('server_id', '')
        if server_id_hex:
            try:
                # Convert from C++ UID::toString() format to raw bytes
                raw_bytes = uid_cpp_to_raw(server_id_hex)
                if raw_bytes:
                    correlation['servers_in_audit'].add(raw_bytes)
                    audit_cpp_to_raw[server_id_hex] = raw_bytes
            except:
                pass

    # Build set of server IDs from serverKeys (already in raw bytes format)
    correlation['servers_in_serverkeys'] = set(serverkeys_by_server.keys())

    # Calculate overlaps
    correlation['servers_in_both'] = correlation['servers_in_audit'] & correlation['servers_in_serverkeys']
    correlation['audit_only'] = correlation['servers_in_audit'] - correlation['servers_in_serverkeys']
    correlation['serverkeys_only'] = correlation['servers_in_serverkeys'] - correlation['servers_in_audit']

    print(f"[Server Coverage]")
    print(f"  Servers in audit (from serverList):  {len(correlation['servers_in_audit'])}")
    print(f"  Servers with serverKeys entries:     {len(correlation['servers_in_serverkeys'])}")
    print(f"  Servers in both:                     {len(correlation['servers_in_both'])}")
    print()

    if correlation['audit_only']:
        print(f"  Servers in audit but no serverKeys:  {len(correlation['audit_only'])}")
        print(f"    (These servers are in serverList but have no ownership claims)")
        for sid in list(correlation['audit_only'])[:3]:
            print(f"      {sid.hex()}")
        if len(correlation['audit_only']) > 3:
            print(f"      ... and {len(correlation['audit_only']) - 3} more")
        print()

    if correlation['serverkeys_only']:
        print(f"  Servers with serverKeys but not in audit: {len(correlation['serverkeys_only'])}")
        print(f"    (These may be stale/dead server entries in serverKeys)")
        for sid in list(correlation['serverkeys_only'])[:3]:
            print(f"      {sid.hex()}")
        if len(correlation['serverkeys_only']) > 3:
            print(f"      ... and {len(correlation['serverkeys_only']) - 3} more")
        print()

    # Correlate missing ranges from audit with serverKeys
    print(f"[Stale serverKeys Confirmation]")
    print(f"  The audit directly queries each SS to see what shards it has.")
    print(f"  Ranges where metadata claims SS owns but SS doesn't have = STALE serverKeys")
    print()

    total_stale_confirmed = 0
    servers_with_stale = []

    for server in audit_data.get('servers', []):
        server_id_hex = server.get('server_id', '')
        address = server.get('address', '')
        missing_ranges = server.get('missing_from_ss', [])

        if missing_ranges:
            servers_with_stale.append({
                'server_id': server_id_hex,
                'address': address,
                'stale_count': len(missing_ranges),
            })
            total_stale_confirmed += len(missing_ranges)
            correlation['stale_by_server'][server_id_hex] = len(missing_ranges)

    print(f"  Total CONFIRMED stale serverKeys entries: {total_stale_confirmed}")
    print(f"  Servers with stale entries: {len(servers_with_stale)}")
    print()

    if servers_with_stale:
        # Sort by stale count descending
        servers_with_stale.sort(key=lambda x: x['stale_count'], reverse=True)

        print(f"  Top servers by stale entry count:")
        for s in servers_with_stale[:10]:
            print(f"    {s['server_id'][:16]}... @ {s['address']}: {s['stale_count']} stale entries")
        if len(servers_with_stale) > 10:
            print(f"    ... and {len(servers_with_stale) - 10} more servers")
        print()

    correlation['stale_serverkeys_confirmed'] = total_stale_confirmed

    # Summary
    print(f"[Correlation Summary]")
    if total_stale_confirmed > 0:
        print(f"  The audit CONFIRMS {total_stale_confirmed} serverKeys entries are STALE.")
        print(f"  These entries claim servers own shards they don't actually have.")
        print(f"  This is the root cause of wrong_shard_server errors and DD looping.")
        print()
        print(f"  RECOMMENDED ACTION: Delete these stale serverKeys entries")
        print(f"    python3 check_krm_corruption.py -c fdb.cluster \\")
        print(f"        --repair-from-audit audit.json --repair-serverkeys --execute-repair")
    else:
        print(f"  No stale serverKeys entries confirmed by audit.")
        print(f"  All servers have the shards that metadata claims they have.")

    print()
    return correlation


def three_way_metadata_correlation(db, wrong_shard_issues, audit_data, live_servers, verbose=False):
    """
    Three-way correlation between keyServers, serverKeys, and SS actual state.

    This clarifies the relationship between the different discrepancy types:
    1. Missing serverKeys (148,929): keyServers says server owns range, serverKeys has no entry
    2. Stale serverKeys (14,178): serverKeys claims server owns range, SS doesn't have it

    For each live server, we compute:
    - keyServers claims: ranges keyServers says this server should own
    - serverKeys claims: ranges serverKeys says this server owns
    - SS actual: ranges SS confirmed it has (via audit getShardState)

    The math:
    - Missing from serverKeys = keyServers claims - serverKeys claims
    - Stale in serverKeys = serverKeys claims - SS actual
    - Correct serverKeys = serverKeys claims ∩ SS actual
    """
    print("\n" + "=" * 70)
    print("THREE-WAY CORRELATION: keyServers vs serverKeys vs SS Actual")
    print("=" * 70)
    print()

    # Build per-server data from wrong_shard_server issues (missing serverKeys)
    # This tells us: keyServers says server owns range, but serverKeys missing
    missing_by_server = {}  # server_id -> count of missing ranges
    for issue in wrong_shard_issues.get('wrong_shard_ranges', []):
        srv = issue['server_id']
        if srv in live_servers:
            if srv not in missing_by_server:
                missing_by_server[srv] = 0
            missing_by_server[srv] += 1

    # Build per-server data from audit (stale serverKeys)
    # This tells us: serverKeys says server owns range, but SS doesn't have it
    stale_by_server = {}  # server_id_bytes -> count of stale ranges
    confirmed_by_server = {}  # server_id_bytes -> count of confirmed ranges
    serverkeys_count_by_server = {}  # server_id_bytes -> total serverKeys entries

    for server in audit_data.get('servers', []):
        server_id_hex = server.get('server_id', '')
        try:
            server_id_bytes = uid_cpp_to_raw(server_id_hex)
            if server_id_bytes and server_id_bytes in live_servers:
                metadata_ranges = server.get('metadata_ranges', 0)
                ss_shards = server.get('ss_shards', 0)
                missing_from_ss = len(server.get('missing_from_ss', []))

                serverkeys_count_by_server[server_id_bytes] = metadata_ranges
                stale_by_server[server_id_bytes] = missing_from_ss
                confirmed_by_server[server_id_bytes] = ss_shards
        except:
            pass

    # Compute totals
    total_missing_serverkeys = sum(missing_by_server.values())
    total_stale_serverkeys = sum(stale_by_server.values())
    total_confirmed = sum(confirmed_by_server.values())
    total_serverkeys_entries = sum(serverkeys_count_by_server.values())

    print(f"[Summary for {len(live_servers)} Live Servers]")
    print()
    print(f"  serverKeys entries (claims):           {total_serverkeys_entries:,}")
    print(f"    - Confirmed by SS (correct):         {total_confirmed:,}")
    print(f"    - Stale (SS doesn't have):           {total_stale_serverkeys:,}")
    print()
    print(f"  Missing from serverKeys:               {total_missing_serverkeys:,}")
    print(f"    (keyServers says server should own, but serverKeys has no entry)")
    print()

    # Key insight: what does this mean for DD?
    print("=" * 70)
    print("[Impact Analysis]")
    print("=" * 70)
    print()

    if total_stale_serverkeys > 0 and total_confirmed == 0:
        print(f"  OBSERVATION: ALL serverKeys entries for live servers are STALE!")
        print(f"    - serverKeys claims: {total_serverkeys_entries:,} entries")
        print(f"    - SS confirmed:      {total_confirmed:,} entries")
        print(f"    - ALL {total_stale_serverkeys:,} are stale (SS doesn't have these ranges)")
        print()
        print(f"  This means serverKeys is COMPLETELY WRONG for live servers.")
        print(f"  The SS has DIFFERENT data than what serverKeys claims.")
        print()
        print(f"  Why does data still exist?")
        print(f"    → SS loads shard assignments from keyServers (correct)")
        print(f"    → serverKeys is stale/never updated")
        print(f"    → SS has data for keyServers ranges, NOT serverKeys ranges")
        print()

    print(f"[Discrepancy Breakdown]")
    print()
    print(f"  1. MISSING serverKeys ({total_missing_serverkeys:,} entries):")
    print(f"     - keyServers says server should own range")
    print(f"     - serverKeys has NO entry for that (server, range)")
    print(f"     - Impact: serverKeys incomplete, but SS has data via keyServers")
    print(f"     - DD uses serverKeys for shard metrics - may fail to get metrics")
    print()
    print(f"  2. STALE serverKeys ({total_stale_serverkeys:,} entries):")
    print(f"     - serverKeys says server owns range")
    print(f"     - SS doesn't actually have that data (getShardState = wrong_shard_server)")
    print(f"     - Impact: Garbage metadata pointing to nonexistent data")
    print()

    if total_missing_serverkeys > 0 and total_stale_serverkeys > 0:
        print(f"  COMBINED PICTURE:")
        print(f"     serverKeys has {total_stale_serverkeys:,} WRONG entries (stale)")
        print(f"     serverKeys is missing {total_missing_serverkeys:,} CORRECT entries")
        print(f"     The serverKeys metadata is severely corrupted.")
        print()
        print(f"  The {total_missing_serverkeys:,} 'missing' entries represent where")
        print(f"  serverKeys SHOULD have entries but doesn't. The SS actually has")
        print(f"  this data (loaded from keyServers), just not recorded in serverKeys.")
        print()

    # Per-server breakdown if verbose
    if verbose and live_servers:
        print(f"[Per-Server Breakdown]")
        print()
        for srv in list(live_servers)[:10]:
            missing = missing_by_server.get(srv, 0)
            stale = stale_by_server.get(srv, 0)
            confirmed = confirmed_by_server.get(srv, 0)
            sk_count = serverkeys_count_by_server.get(srv, 0)
            print(f"  Server {srv.hex()[:16]}...:")
            print(f"    serverKeys claims:    {sk_count}")
            print(f"    SS confirmed:         {confirmed}")
            print(f"    Stale:                {stale}")
            print(f"    Missing (from wss):   {missing}")
        if len(live_servers) > 10:
            print(f"  ... and {len(live_servers) - 10} more servers")
        print()

    return {
        'total_missing_serverkeys': total_missing_serverkeys,
        'total_stale_serverkeys': total_stale_serverkeys,
        'total_confirmed': total_confirmed,
        'total_serverkeys_entries': total_serverkeys_entries,
        'missing_by_server': missing_by_server,
        'stale_by_server': stale_by_server,
    }


def correlate_stale_serverkeys_with_keyservers(db, audit_data, verbose=False):
    """
    For stale serverKeys entries (SS doesn't have the range), check if keyServers
    also points to those servers for those ranges.

    This answers: "Will DD actually hit wrong_shard_server for these ranges?"

    Categories:
    1. ALL_MISSING: keyServers points ONLY to servers that don't have the range
       → Data unreachable, DD will loop forever
    2. SOME_MISSING: keyServers includes servers that don't have the range,
       but other servers DO have it → DD might hit wrong_shard_server but can recover
    3. NOT_IN_KEYSERVERS: Stale serverKeys but keyServers doesn't point to that server
       → Just garbage to clean up, not causing wrong_shard_server

    Args:
        db: FDB database handle
        audit_data: Parsed JSON from audit_ss_shards
        verbose: If True, show detailed output

    Returns:
        dict with categorized results
    """
    print("\n" + "=" * 70)
    print("KEYSERVERS CORRELATION: Will stale serverKeys cause wrong_shard_server?")
    print("=" * 70)
    print()

    # If we have metrics data from the audit, show that first - it's the definitive answer
    metrics = audit_data.get('metrics')
    if metrics:
        wrong_shard_count = metrics.get('wrong_shard_count', 0)
        total_ranges = metrics.get('total_ranges', 0)
        ok_count = metrics.get('ok_count', 0)
        total_bytes = metrics.get('total_bytes', 0)

        print("[METRICS TEST RESULTS - Definitive Answer]")
        print(f"  The audit tool tested {total_ranges} ranges using DD's exact waitStorageMetrics path.")
        print()

        if wrong_shard_count > 0:
            print(f"  CRITICAL: {wrong_shard_count} ranges returned wrong_shard_server!")
            print(f"  These are the EXACT ranges causing DD to loop.")
            print()

            failed_shards = metrics.get('failed_shards', [])
            if failed_shards:
                print(f"  Failing ranges (from metrics test):")
                for shard in failed_shards[:10]:
                    begin = shard.get('begin', '')[:50]
                    print(f"    [{begin}...]")
                if len(failed_shards) > 10:
                    print(f"    ... and {len(failed_shards) - 10} more")
            print()
        else:
            print(f"  SUCCESS: All {ok_count} ranges returned metrics successfully.")
            print(f"  Total data: {total_bytes:,} bytes ({total_bytes / (1024**3):.2f} GB)")
            print(f"  DD should be able to get metrics for all shards.")
            print()
            print("  The correlation analysis below is for diagnostic purposes only.")
            print()

    # Build map: server_id_bytes -> set of (begin, end) ranges the SS is MISSING
    missing_by_server = {}
    server_addresses = {}  # server_id_hex -> address

    for server in audit_data.get('servers', []):
        server_id_hex = server.get('server_id', '')
        address = server.get('address', '')
        missing_ranges = server.get('missing_from_ss', [])

        if not missing_ranges:
            continue

        server_addresses[server_id_hex] = address

        try:
            # Convert C++ UID format to raw bytes
            server_id_bytes = uid_cpp_to_raw(server_id_hex)
            if server_id_bytes:
                missing_by_server[server_id_bytes] = []
                for r in missing_ranges:
                    begin = bytes.fromhex(r['begin'])
                    end = bytes.fromhex(r['end'])
                    missing_by_server[server_id_bytes].append((begin, end))
        except Exception as e:
            print(f"  Warning: Could not parse server {server_id_hex}: {e}")

    print(f"  Servers with missing ranges: {len(missing_by_server)}")
    total_missing = sum(len(ranges) for ranges in missing_by_server.values())
    print(f"  Total missing ranges: {total_missing}")
    print()

    if not missing_by_server:
        print("  No stale serverKeys entries to correlate.")
        return {'all_missing': [], 'some_missing': [], 'not_in_keyservers': []}

    # Read keyServers to build map: range -> set of server IDs
    print("  Reading keyServers entries...")

    @fdb.transactional
    def read_keyservers(tr):
        set_read_transaction_options(tr)
        entries = []
        prev_key = None
        prev_servers = None

        for k, v in tr.get_range(KEY_SERVERS_PREFIX, KEY_SERVERS_END, limit=100000):
            user_key = k[len(KEY_SERVERS_PREFIX):]
            src_servers, dest_servers, _, _, _ = decode_key_servers_value(v)

            if prev_key is not None:
                entries.append({
                    'begin': prev_key,
                    'end': user_key,
                    'servers': prev_servers,
                })

            prev_key = user_key
            prev_servers = set(src_servers)  # Use source servers

        return entries

    try:
        ks_entries = read_keyservers(db)
        print(f"  Read {len(ks_entries)} keyServers ranges")
    except Exception as e:
        print(f"  ERROR reading keyServers: {e}")
        return {'error': str(e)}

    # Now correlate: for each missing range, find overlapping keyServers entry
    # and check if that keyServers entry points to the server with missing range

    results = {
        'all_missing': [],      # keyServers only has servers that don't have the range
        'some_missing': [],     # keyServers includes some servers missing, some have it
        'not_in_keyservers': [],  # Stale serverKeys but server not in keyServers for that range
    }

    print("  Correlating stale serverKeys with keyServers...")

    checked = 0
    for server_id_bytes, missing_ranges in missing_by_server.items():
        for begin, end in missing_ranges:
            checked += 1
            if checked % 1000 == 0:
                print(f"    ... checked {checked} ranges")

            # Find keyServers entry that contains this range
            # (Simplified: find entry where begin <= range_begin < end)
            matching_ks = None
            for ks in ks_entries:
                if ks['begin'] <= begin < ks['end']:
                    matching_ks = ks
                    break

            if matching_ks is None:
                # No keyServers entry for this range - might be system range or deleted
                results['not_in_keyservers'].append({
                    'server': server_id_bytes.hex(),
                    'range_begin': begin.hex(),
                    'range_end': end.hex(),
                    'reason': 'no_keyservers_entry',
                })
                continue

            # Check if this server is in the keyServers entry
            if server_id_bytes not in matching_ks['servers']:
                # Server not in keyServers for this range - stale serverKeys is just garbage
                results['not_in_keyservers'].append({
                    'server': server_id_bytes.hex(),
                    'range_begin': begin.hex(),
                    'range_end': end.hex(),
                    'reason': 'server_not_in_keyservers',
                })
                continue

            # Server IS in keyServers - check how many other servers have the range
            servers_with_range = []
            servers_missing_range = []

            for srv in matching_ks['servers']:
                if srv in missing_by_server:
                    # Check if this server is missing THIS specific range
                    is_missing = False
                    for mb, me in missing_by_server[srv]:
                        if mb <= begin < me:
                            is_missing = True
                            break
                    if is_missing:
                        servers_missing_range.append(srv)
                    else:
                        servers_with_range.append(srv)
                else:
                    # Server not in audit missing list - assume it has the range
                    servers_with_range.append(srv)

            if len(servers_with_range) == 0:
                # ALL servers in keyServers are missing this range!
                results['all_missing'].append({
                    'range_begin': begin.hex(),
                    'range_end': end.hex(),
                    'servers_missing': [s.hex() for s in servers_missing_range],
                    'total_servers': len(matching_ks['servers']),
                })
            else:
                # SOME servers missing, some have it
                results['some_missing'].append({
                    'range_begin': begin.hex(),
                    'range_end': end.hex(),
                    'servers_missing': [s.hex() for s in servers_missing_range],
                    'servers_have': [s.hex() for s in servers_with_range],
                })

    # Report results
    print()
    print("=" * 70)
    print("CORRELATION RESULTS")
    print("=" * 70)
    print()

    # Check how many ALL_MISSING are blog ranges
    blog_prefix = b'\xff\x02/blog/'.hex()
    all_missing_blog = sum(1 for r in results['all_missing'] if r['range_begin'].startswith(blog_prefix))
    all_missing_user = len(results['all_missing']) - all_missing_blog

    print(f"[Category 1: ALL_MISSING - Data Unreachable]")
    print(f"  Count: {len(results['all_missing'])}")
    if results['all_missing']:
        print(f"    Blog ranges: {all_missing_blog}")
        print(f"    User data:   {all_missing_user}")
        if all_missing_blog > 0 and all_missing_user == 0:
            print(f"  NOTE: All ALL_MISSING ranges are blog ranges (backup logs).")
            print(f"  Blog ranges are managed differently - this may be expected.")
        elif all_missing_user > 0:
            print(f"  CRITICAL: {all_missing_user} USER DATA ranges have NO servers with the data!")
            print(f"  DD will loop forever getting wrong_shard_server from all replicas.")
        # Always show sample ranges
        print(f"\n  Sample ALL_MISSING ranges:")
        for r in results['all_missing'][:5]:
            begin_hex = r['range_begin']
            # Try to decode as printable
            try:
                begin_bytes = bytes.fromhex(begin_hex)
                printable = begin_bytes.decode('latin-1')[:40]
            except:
                printable = begin_hex[:40]
            print(f"    [{begin_hex[:50]}...)")
            print(f"      Decoded: {printable!r}")
            print(f"      Servers missing: {r['total_servers']}")
        if len(results['all_missing']) > 5:
            print(f"    ... and {len(results['all_missing']) - 5} more")
        if verbose:
            for r in results['all_missing'][:10]:
                print(f"    Range: [{r['range_begin'][:30]}..., {r['range_end'][:30]}...)")
                print(f"      Servers (all missing): {len(r['servers_missing'])}")
            if len(results['all_missing']) > 10:
                print(f"    ... and {len(results['all_missing']) - 10} more")
    print()

    print(f"[Category 2: SOME_MISSING - Partial Availability]")
    print(f"  Count: {len(results['some_missing'])}")
    if results['some_missing']:
        print(f"  WARNING: These ranges have some servers missing, but others have the data.")
        print(f"  DD might hit wrong_shard_server but can retry with other replicas.")
        if verbose:
            for r in results['some_missing'][:5]:
                print(f"    Range: [{r['range_begin'][:30]}..., {r['range_end'][:30]}...)")
                print(f"      Missing: {len(r['servers_missing'])}, Have: {len(r['servers_have'])}")
            if len(results['some_missing']) > 5:
                print(f"    ... and {len(results['some_missing']) - 5} more")
    print()

    print(f"[Category 3: NOT_IN_KEYSERVERS - Garbage to Clean]")
    print(f"  Count: {len(results['not_in_keyservers'])}")
    if results['not_in_keyservers']:
        print(f"  INFO: Stale serverKeys entries, but keyServers doesn't point to these servers.")
        print(f"  These don't cause wrong_shard_server - just garbage to clean up.")
    print()

    # Summary
    print("=" * 70)
    print("IMPACT SUMMARY")
    print("=" * 70)
    if results['all_missing']:
        if all_missing_user > 0:
            print(f"  CRITICAL: {all_missing_user} USER DATA ranges are UNREACHABLE (data loss?)")
            print(f"    Action: Investigate - data may be lost or on servers not in audit")
        if all_missing_blog > 0:
            print(f"  INFO: {all_missing_blog} blog ranges are ALL_MISSING (may be expected)")
            print(f"    Blog ranges are managed by backup system, not normal DD")
    if results['some_missing']:
        print(f"  WARNING: {len(results['some_missing'])} ranges have partial availability")
        print(f"    Action: Clean stale serverKeys, DD can use remaining replicas")
    if results['not_in_keyservers']:
        print(f"  INFO: {len(results['not_in_keyservers'])} stale serverKeys (no DD impact)")
        print(f"    Action: Clean stale serverKeys as garbage collection")

    if not results['all_missing'] and not results['some_missing']:
        print(f"  GOOD: No stale serverKeys entries are causing wrong_shard_server!")
        print(f"  The {len(results['not_in_keyservers'])} stale entries are just garbage.")

    # Verify ALL_MISSING by actually probing them with get_range()
    # This matches DD's waitStorageMetrics behavior and detects gaps in SS shard map
    verification = None
    if results['all_missing']:
        print("=" * 70)
        print("VERIFICATION: Probing ALL_MISSING ranges (using get_range)")
        print("=" * 70)
        print()
        verification = verify_all_missing_ranges(db, results['all_missing'])
        results['verification'] = verification

        # Update impact based on actual verification
        total_probed = verification.get('success', 0) + verification.get('wrong_shard_server', 0) + verification.get('key_outside_range', 0) + verification.get('timeout', 0) + verification.get('other_error', 0)
        if verification and verification.get('wrong_shard_server', 0) == 0 and verification.get('success', 0) > 0:
            print()
            print("=" * 70)
            print("VERIFIED: No gaps in SS shard map")
            print("=" * 70)
            print(f"  All {len(results['all_missing'])} 'ALL_MISSING' ranges are fully readable.")
            print(f"  These stale serverKeys entries will NOT cause DD wrong_shard_server.")
            print(f"  Safe to clean up as garbage collection.")
            results['verified_data_exists'] = True
        elif verification and verification.get('wrong_shard_server', 0) > 0:
            print()
            print("=" * 70)
            print("CONFIRMED: Gaps in SS shard map causing DD failures!")
            print("=" * 70)
            print(f"  {verification['wrong_shard_server']} ranges have gaps that cause wrong_shard_server.")
            print(f"  This is why DD's waitStorageMetrics fails.")
            results['verified_data_exists'] = False

    print()
    return results


def verify_all_missing_ranges(db, all_missing_ranges, sample_size=None):
    """
    Probe ALL_MISSING ranges to detect shard ownership issues.

    Probes both the FIRST and LAST key of each range. This catches cases where:
    - The beginning of the range is not readable
    - The end of the range is not readable

    Note: This may miss gaps in the MIDDLE of ranges. For thorough checking,
    use --range-probe-keyservers which does full get_range() traversal.

    Args:
        sample_size: Max ranges to probe. None = probe all (default).
    """
    if sample_size is None:
        to_probe = all_missing_ranges
    else:
        to_probe = all_missing_ranges[:sample_size] if len(all_missing_ranges) > sample_size else all_missing_ranges

    print(f"  Probing {len(to_probe)} of {len(all_missing_ranges)} ALL_MISSING ranges...")
    print(f"  (checking both first and last key of each range)")
    print()

    results = {
        'wrong_shard_server': 0,
        'key_outside_range': 0,
        'success': 0,
        'timeout': 0,
        'other_error': 0,
        'failed_ranges': [],
    }

    for i, r in enumerate(to_probe):
        if i % 50 == 0:
            print(f"    ... probed {i}/{len(to_probe)} (success={results['success']}, wrong_shard={results['wrong_shard_server']}, timeout={results['timeout']})")
            sys.stdout.flush()

        try:
            begin = bytes.fromhex(r['range_begin'])
            end = bytes.fromhex(r['range_end'])

            # Probe both ends of the range to catch gaps
            # - get(begin) checks first shard
            # - get(end-1) checks last shard
            # If there's a gap in the middle, we might miss it, but this catches most cases
            tr = db.create_transaction()
            tr.options.set_timeout(2000)  # 2 second timeout
            # Enable system key access for \xff prefixed ranges
            if begin.startswith(b'\xff'):
                tr.options.set_read_system_keys()

            try:
                # Check beginning
                _ = tr.get(begin).wait()

                # Check near end (one byte before end key)
                if len(end) > 0 and end > begin:
                    last_key = end[:-1] + bytes([max(0, end[-1] - 1)]) if end[-1] > 0 else end[:-1]
                    if last_key > begin:
                        _ = tr.get(last_key).wait()

                results['success'] += 1
            except fdb.FDBError as e:
                if e.code == 1037:  # wrong_shard_server
                    results['wrong_shard_server'] += 1
                    if len(results['failed_ranges']) < 20:
                        results['failed_ranges'].append({
                            'begin': begin.hex(),
                            'end': end.hex(),
                        })
                elif e.code == 2004:  # key_outside_legal_range
                    results['key_outside_range'] += 1
                elif e.code in (1007, 1009, 1031):  # timeouts
                    results['timeout'] += 1
                else:
                    results['other_error'] += 1
        except Exception as e:
            results['other_error'] += 1

    print(f"  Range probe results:")
    print(f"    wrong_shard_server:     {results['wrong_shard_server']}")
    print(f"    key_outside_range:      {results['key_outside_range']}")
    print(f"    success (fully readable): {results['success']}")
    print(f"    timeout:                {results['timeout']}")
    print(f"    other_error:            {results['other_error']}")
    print()

    if results['wrong_shard_server'] > 0:
        print(f"  CONFIRMED: {results['wrong_shard_server']} ranges have GAPS in SS shard map!")
        print(f"  These WILL cause DD's waitStorageMetrics to fail.")
        print()
        if results['failed_ranges']:
            print(f"  Sample failed ranges:")
            for fr in results['failed_ranges'][:5]:
                print(f"    [{fr['begin'][:32]}..., {fr['end'][:32]}...)")
            if len(results['failed_ranges']) > 5:
                print(f"    ... and {len(results['failed_ranges']) - 5} more")
    elif results['key_outside_range'] == len(to_probe):
        print(f"  ALL ranges return 'key_outside_legal_range' (error 2004)")
        print(f"  These are system/restricted ranges that clients cannot access.")
        print(f"  DD may handle these differently than user data.")
    elif results['success'] == len(to_probe):
        print(f"  ALL {results['success']} probed ranges are FULLY READABLE!")
        print(f"  No gaps in SS shard map for these ranges.")
        print(f"  These stale serverKeys entries are safe to clean up.")
    elif results['success'] > 0:
        print(f"  Mixed: {results['success']} fully readable, {results['wrong_shard_server']} have gaps")

    return results


def verify_ranges_with_get_range(db, ranges_to_probe, sample_size=None):
    """
    Probe ranges using get_range() to detect gaps in SS's internal shard map.

    Unlike single-key probes, get_range() must traverse ALL shards in the range.
    If there's a gap (SS doesn't have part of the range), we'll get wrong_shard_server.

    This mimics how DD's waitStorageMetrics() works - it queries the entire range,
    so any gap causes failure even if point queries at the endpoints succeed.

    Args:
        db: FDB database handle
        ranges_to_probe: List of dicts with 'range_begin' and 'range_end' (hex strings)
        sample_size: Max ranges to probe. None = probe all.

    Returns:
        dict with probe results
    """
    if sample_size is None:
        to_probe = ranges_to_probe
    else:
        to_probe = ranges_to_probe[:sample_size] if len(ranges_to_probe) > sample_size else ranges_to_probe

    print(f"  Range-probing {len(to_probe)} ranges using get_range()...")
    print(f"  (This detects gaps that single-key probes miss)")
    print()

    results = {
        'wrong_shard_server': 0,
        'key_outside_range': 0,
        'success': 0,
        'timeout': 0,
        'other_error': 0,
        'failed_ranges': [],
    }

    for i, r in enumerate(to_probe):
        if i > 0 and i % 20 == 0:
            print(f"    ... probed {i}/{len(to_probe)}")

        try:
            begin = bytes.fromhex(r['range_begin'])
            end = bytes.fromhex(r['range_end'])

            tr = db.create_transaction()
            tr.options.set_timeout(5000)  # 5 second timeout
            # Enable system key access for \xff prefixed ranges
            if begin.startswith(b'\xff'):
                tr.options.set_read_system_keys()

            try:
                # Use get_range with limit=1 - we just need to traverse the range,
                # not actually read all the data. The routing/shard check happens
                # for the full range even with limit=1.
                # Actually, limit=1 might only check the first shard. Let's use
                # a streaming read that touches the whole range.
                result = tr.get_range(begin, end, limit=0, streaming_mode=fdb.StreamingMode.want_all)
                # Force evaluation by iterating (this traverses all shards)
                count = 0
                for kv in result:
                    count += 1
                    if count > 100:  # Don't read too much data
                        break
                results['success'] += 1
            except fdb.FDBError as e:
                if e.code == 1037:  # wrong_shard_server
                    results['wrong_shard_server'] += 1
                    if len(results['failed_ranges']) < 10:
                        results['failed_ranges'].append({
                            'begin': begin.hex(),
                            'end': end.hex(),
                            'error': 'wrong_shard_server'
                        })
                elif e.code == 2004:  # key_outside_legal_range
                    results['key_outside_range'] += 1
                elif e.code in (1007, 1009, 1031):  # timeouts
                    results['timeout'] += 1
                else:
                    results['other_error'] += 1
        except Exception as e:
            results['other_error'] += 1

    print(f"  Range probe results:")
    print(f"    wrong_shard_server:     {results['wrong_shard_server']}")
    print(f"    key_outside_range:      {results['key_outside_range']}")
    print(f"    success:                {results['success']}")
    print(f"    timeout:                {results['timeout']}")
    print(f"    other_error:            {results['other_error']}")
    print()

    if results['wrong_shard_server'] > 0:
        print(f"  FOUND GAPS: {results['wrong_shard_server']} ranges have gaps in SS shard map!")
        print(f"  These ranges cause wrong_shard_server for DD's waitStorageMetrics.")
        print()
        print(f"  Sample failed ranges:")
        for fr in results['failed_ranges'][:5]:
            print(f"    [{fr['begin'][:20]}... , {fr['end'][:20]}...)")
    elif results['success'] == len(to_probe):
        print(f"  All {results['success']} ranges are fully readable (no gaps).")

    return results


def repair_serverkeys_from_audit(db, audit_data, dry_run=True):
    """
    Repair serverKeys based on C++ audit results.

    For each server with 'missing_from_ss' ranges, delete the corresponding
    serverKeys entries since the SS doesn't actually have those shards.

    Args:
        db: FDB database handle
        audit_data: Parsed JSON from audit_ss_shards --json
        dry_run: If True, only report what would be done without making changes

    Returns:
        dict with repair statistics
    """
    print("\n" + "=" * 60)
    print("REPAIR: Fixing serverKeys based on C++ audit results")
    print("=" * 60)

    if dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    else:
        print("MODE: LIVE REPAIR (changes will be committed)")

    stats = {
        'servers_processed': 0,
        'ranges_to_delete': 0,
        'ranges_deleted': 0,
        'errors': 0,
    }

    servers = audit_data.get('servers', [])

    for server in servers:
        server_id_hex = server.get('server_id', '')
        address = server.get('address', '')
        missing_ranges = server.get('missing_from_ss', [])

        if not missing_ranges:
            continue

        stats['servers_processed'] += 1
        stats['ranges_to_delete'] += len(missing_ranges)

        print(f"\n  Server {server_id_hex} @ {address}:")
        print(f"    {len(missing_ranges)} ranges to delete from serverKeys")

        # Convert server_id hex to bytes (16 bytes)
        try:
            server_id_bytes = hex_to_bytes(server_id_hex)
        except Exception as e:
            print(f"    ERROR: Invalid server_id hex: {e}")
            stats['errors'] += 1
            continue

        # Process each range
        for i, range_info in enumerate(missing_ranges):
            begin_hex = range_info.get('begin', '')
            end_hex = range_info.get('end', '')

            try:
                begin_key = hex_to_bytes(begin_hex)
                end_key = hex_to_bytes(end_hex)
            except Exception as e:
                print(f"    ERROR: Invalid range hex: {e}")
                stats['errors'] += 1
                continue

            # The serverKeys key format is: \xff/serverKeys/[16-byte server_id]/[user_key]
            # We need to delete entries for this range
            serverkeys_prefix = SERVER_KEYS_PREFIX + server_id_bytes + b'/'
            sk_begin = serverkeys_prefix + begin_key
            sk_end = serverkeys_prefix + end_key

            if dry_run:
                if i < 3:  # Show first 3 examples
                    print(f"      Would delete serverKeys range: [{begin_hex[:30]}..., {end_hex[:30]}...)")
                elif i == 3:
                    print(f"      ... and {len(missing_ranges) - 3} more ranges")
            else:
                try:
                    @fdb.transactional
                    def delete_serverkeys_range(tr):
                        tr.options.set_access_system_keys()
                        tr.options.set_lock_aware()
                        tr.options.set_timeout(60000)
                        tr.options.set_priority_system_immediate()
                        update_movekeys_lock_write(tr)
                        # Clear the range in serverKeys
                        tr.clear_range(sk_begin, sk_end)
                        # Also clear the begin boundary key itself
                        tr.clear(sk_begin)

                    delete_serverkeys_range(db)
                    stats['ranges_deleted'] += 1

                    if i < 3:
                        print(f"      Deleted serverKeys range: [{begin_hex[:30]}..., {end_hex[:30]}...)")
                    elif i == 3:
                        print(f"      ... deleting {len(missing_ranges) - 3} more ranges")

                except Exception as e:
                    print(f"      ERROR deleting range: {e}")
                    stats['errors'] += 1

    print(f"\n  Repair Summary:")
    print(f"    Servers processed: {stats['servers_processed']}")
    print(f"    Ranges to delete: {stats['ranges_to_delete']}")
    if not dry_run:
        print(f"    Ranges deleted: {stats['ranges_deleted']}")
    print(f"    Errors: {stats['errors']}")

    return stats


def repair_keyservers_from_audit(db, audit_data, dry_run=True):
    """
    Repair keyServers based on C++ audit results.

    For ranges where ALL assigned servers are missing the shard, we need to
    update keyServers to remove those servers from the assignment.

    This is more complex than serverKeys repair because:
    1. keyServers entries list multiple servers per shard
    2. We need to check if ANY server has the shard before removing
    3. If no servers have the shard, the data may be lost

    Args:
        db: FDB database handle
        audit_data: Parsed JSON from audit_ss_shards --json
        dry_run: If True, only report what would be done

    Returns:
        dict with repair statistics
    """
    print("\n" + "=" * 60)
    print("REPAIR: Analyzing keyServers based on C++ audit results")
    print("=" * 60)

    if dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    else:
        print("MODE: LIVE REPAIR (changes will be committed)")

    # Build a map of server_id -> set of missing ranges
    missing_by_server = {}
    for server in audit_data.get('servers', []):
        server_id_hex = server.get('server_id', '')
        missing_ranges = server.get('missing_from_ss', [])

        if missing_ranges:
            try:
                server_id_bytes = hex_to_bytes(server_id_hex)
                missing_by_server[server_id_bytes] = set()
                for r in missing_ranges:
                    begin = hex_to_bytes(r['begin'])
                    missing_by_server[server_id_bytes].add(begin)
            except:
                pass

    print(f"\n  Servers with missing shards: {len(missing_by_server)}")

    # This analysis is complex - for now just report which ranges are problematic
    # A full repair would need to:
    # 1. Read all keyServers entries
    # 2. For each entry, check if assigned servers actually have the shard
    # 3. If no server has it -> DATA LOSS, flag for investigation
    # 4. If some servers have it -> update keyServers to only list those servers

    print("\n  NOTE: keyServers repair is complex and requires careful analysis.")
    print("  The serverKeys repair above removes stale ownership claims.")
    print("  After serverKeys repair, DD should be able to rebuild keyServers.")

    return {'status': 'analysis_only'}


def main():
    parser = argparse.ArgumentParser(description='Check FDB KRM for corruption')
    parser.add_argument('--cluster-file', '-C', help='Path to fdb.cluster file')
    parser.add_argument('--fdbcli', help='Path to fdbcli binary (default: fdbcli in PATH or FDBCLI_PATH env var)')
    parser.add_argument('--key-prefix', '-k', help='Only check keyServers with this prefix (hex)', default=None)
    parser.add_argument('--check-blog', action='store_true', help='Check \\xff\\x02/blog/ range specifically')
    parser.add_argument('--limit', '-l', type=int, default=10000000, help='Max entries to scan (default: 10M)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--check-mismatch', action='store_true', help='Check for mismatches between keyServers and serverKeys (sample)')
    parser.add_argument('--skip-wrong-shard', action='store_true', help='Skip the wrong_shard_server check (runs by default)')
    parser.add_argument('--skip-analyze-orphans', action='store_true', help='Skip orphan server analysis (runs by default)')
    parser.add_argument('--skip-discover-ss', action='store_true', help='Skip SS discovery (runs by default)')
    parser.add_argument('--skip-verify-ss', action='store_true', help='Skip SS verification (runs by default)')
    parser.add_argument('--skip-check-serverlist', action='store_true', help='Skip serverList vs running servers check (runs by default)')
    parser.add_argument('--skip-probe-ss', action='store_true', help='Skip SS shard probe with actual reads (runs by default)')
    parser.add_argument('--verify-samples', type=int, default=0, help='Number of ranges to sample for verification (default: 0 = all)')
    parser.add_argument('--probe-samples', type=int, default=0, help='Number of ranges to probe with actual reads (default: 0 = all)')
    parser.add_argument('--test-encoding', action='store_true', help='Test that we can round-trip encode/decode keyServers values')
    parser.add_argument('--audit-ss-shards', help='Path to audit_ss_shards binary (C++ tool for direct SS queries)')
    parser.add_argument('--skip-audit-ss-shards', action='store_true', help='Skip C++ SS shard audit even if binary is available')
    # Audit report option (runs full diagnostics + audit comparison)
    parser.add_argument('--report-audit', help='Load audit JSON and run full diagnostics with integrated SS state comparison')
    # Repair options
    parser.add_argument('--repair-from-audit', help='Path to audit_ss_shards JSON output file to use for repair')
    parser.add_argument('--repair-serverkeys', action='store_true', help='Delete stale serverKeys entries based on audit (requires --repair-from-audit)')
    parser.add_argument('--repair-keyservers', action='store_true', help='Delete phantom keyServers entries based on keyServers audit (requires --repair-from-audit with --use-keyservers audit)')
    parser.add_argument('--execute-repair', action='store_true', help='Actually execute the repair (default is dry-run)')
    # Range probe ALL keyServers entries (not just the correlation subset)
    parser.add_argument('--range-probe-keyservers', action='store_true', help='Range-probe ALL keyServers entries to find which cause wrong_shard_server')
    # Legacy flags (kept for backwards compatibility, now run by default)
    parser.add_argument('--analyze-orphans', action='store_true', help='(deprecated - now runs by default)')
    parser.add_argument('--discover-ss', action='store_true', help='(deprecated - now runs by default)')
    parser.add_argument('--verify-ss', action='store_true', help='(deprecated - now runs by default)')
    parser.add_argument('--check-serverlist', action='store_true', help='(deprecated - now runs by default)')
    # REPAIR DISABLED - need to understand full corruption picture first
    # parser.add_argument('--repair', action='store_true', help='Repair uncoalesced entries')
    args = parser.parse_args()

    # Load audit data if provided (will be integrated into full diagnostic run)
    preloaded_audit_data = None
    if args.report_audit:
        print(f"Loading audit data from: {args.report_audit}")
        try:
            preloaded_audit_data = load_audit_json(args.report_audit)
            if is_keyservers_audit(preloaded_audit_data):
                print(f"  Audit mode: keyServers-based (phantom shard detection)")
                print(f"  Total ranges checked: {preloaded_audit_data.get('total_ranges', 0)}")
                print(f"  Confirmed (SS has data): {preloaded_audit_data.get('confirmed_ranges', 0)}")
                print(f"  Phantom shards: {preloaded_audit_data.get('wrong_shard_count', 0)}")
            else:
                print(f"  Audit mode: serverKeys-based (standard)")
                print(f"  Loaded audit with {preloaded_audit_data.get('total_servers', 0)} servers")
                print(f"  Total missing from SS: {preloaded_audit_data.get('total_missing_from_ss', 0)}")
                print(f"  Total extra in SS: {preloaded_audit_data.get('total_extra_in_ss', 0)}")
        except Exception as e:
            print(f"ERROR: Failed to load audit data: {e}")
            sys.exit(1)
        # Continue to full diagnostic run below (don't exit here)

    # Handle repair mode
    if args.repair_from_audit:
        if not args.repair_serverkeys and not args.repair_keyservers:
            print("ERROR: --repair-from-audit requires --repair-serverkeys or --repair-keyservers")
            print("Usage: python3 check_krm_corruption.py -C cluster.file \\")
            print("         --repair-from-audit audit_output.json --repair-serverkeys")
            print("  OR:  python3 check_krm_corruption.py -C cluster.file \\")
            print("         --repair-from-audit audit_output.json --repair-keyservers")
            sys.exit(1)

        # Load audit data
        print(f"Loading audit data from: {args.repair_from_audit}")
        try:
            audit_data = load_audit_json(args.repair_from_audit)
        except Exception as e:
            print(f"ERROR: Failed to load audit data: {e}")
            sys.exit(1)

        # Detect audit mode
        if is_keyservers_audit(audit_data):
            print(f"  Audit mode: keyServers-based (phantom shard detection)")
            print(f"  Total ranges checked: {audit_data.get('total_ranges', 0)}")
            print(f"  Confirmed (SS has data): {audit_data.get('confirmed_ranges', 0)}")
            print(f"  Phantom shards: {audit_data.get('wrong_shard_count', 0)}")

            if args.repair_serverkeys:
                print("ERROR: --repair-serverkeys requires serverKeys-based audit (without --use-keyservers)")
                print("       Use --repair-keyservers for keyServers-based audit")
                sys.exit(1)
        else:
            print(f"  Audit mode: serverKeys-based (standard)")
            print(f"  Total servers in audit: {audit_data.get('total_servers', 0)}")
            print(f"  Servers with mismatches: {audit_data.get('servers_with_mismatches', 0)}")
            print(f"  Total missing from SS: {audit_data.get('total_missing_from_ss', 0)}")

            if args.repair_keyservers:
                print("ERROR: --repair-keyservers requires keyServers-based audit (with --use-keyservers)")
                print("       Use --repair-serverkeys for serverKeys-based audit")
                sys.exit(1)

        # Connect to FDB
        if args.cluster_file:
            db = fdb.open(args.cluster_file)
        else:
            db = fdb.open()

        print("\nConnected to FoundationDB")

        # Determine dry-run mode
        dry_run = not args.execute_repair
        if dry_run:
            print("MODE: DRY RUN (use --execute-repair to make changes)")
        else:
            print("MODE: LIVE REPAIR - CHANGES WILL BE MADE!")
            # Safety confirmation
            confirm = input("Type 'yes' to confirm repair: ")
            if confirm.lower() != 'yes':
                print("Repair cancelled.")
                sys.exit(0)

        # Run repair based on audit type
        if not dry_run:
            print("\nDisabling DD and taking MoveKeysLock...")
            our_uid, prev_dd_mode = take_movekeys_lock(db)
            print(f"  Lock acquired (owner: {our_uid.hex()[:16]}...)")

        try:
            if args.repair_serverkeys:
                repair_serverkeys_from_audit(db, audit_data, dry_run=dry_run)
            elif args.repair_keyservers:
                # First show analysis
                process_keyservers_audit(db, audit_data, verbose=args.verbose)
                # Then repair
                repair_keyservers_phantom_shards(db, audit_data, dry_run=dry_run)
        finally:
            if not dry_run:
                print("\nRe-enabling DD and releasing MoveKeysLock...")
                release_movekeys_lock(db, prev_dd_mode)
                print("  Lock released, DD re-enabled")

        sys.exit(0)

    # Handle range-probe-keyservers mode
    if args.range_probe_keyservers:
        print("\n" + "=" * 70)
        print("RANGE PROBE: Testing all keyServers ranges with get_range()")
        print("=" * 70)
        print()
        print("This detects gaps in SS internal shard maps that cause DD's")
        print("waitStorageMetrics to fail with wrong_shard_server, even when")
        print("point queries succeed.")
        print()

        # Connect to FDB
        if args.cluster_file:
            db = fdb.open(args.cluster_file)
        else:
            db = fdb.open()

        print("Connected to FoundationDB")
        print()

        # Read all keyServers entries
        print("Reading keyServers entries...")
        tr = db.create_transaction()
        tr.options.set_read_system_keys()

        ranges_to_probe = []
        prev_key = None
        for k, v in tr.get_range(KEY_SERVERS_PREFIX, KEY_SERVERS_END, limit=args.limit):
            user_key = k[len(KEY_SERVERS_PREFIX):]
            if prev_key is not None:
                ranges_to_probe.append({
                    'range_begin': prev_key.hex(),
                    'range_end': user_key.hex()
                })
            prev_key = user_key

        print(f"Found {len(ranges_to_probe)} keyServers ranges to probe")
        print()

        # Filter out system key ranges (start with \xff) - these may have special handling
        user_ranges = [r for r in ranges_to_probe if not bytes.fromhex(r['range_begin']).startswith(b'\xff')]
        system_ranges = [r for r in ranges_to_probe if bytes.fromhex(r['range_begin']).startswith(b'\xff')]

        print(f"  User data ranges: {len(user_ranges)}")
        print(f"  System ranges (\\xff...): {len(system_ranges)}")
        print()

        if user_ranges:
            print("Probing USER DATA ranges...")
            print()
            results = verify_ranges_with_get_range(db, user_ranges)

            if results['wrong_shard_server'] > 0:
                print()
                print("=" * 70)
                print("DIAGNOSIS: Found ranges causing DD wrong_shard_server!")
                print("=" * 70)
                print()
                print(f"  {results['wrong_shard_server']} keyServers ranges have GAPS in SS shard map.")
                print()
                print("  This explains why DD's waitStorageMetrics fails while point queries succeed:")
                print("  - Point query hits a readable part of the range -> success")
                print("  - Range query (waitStorageMetrics) spans a gap -> wrong_shard_server")
                print()
                print("  Root cause: SS's internal shard boundaries don't match keyServers")
                print("  due to corrupted serverKeys mutations.")
        else:
            print("No user data ranges to probe.")

        sys.exit(0)

    # Connect to FDB
    if args.cluster_file:
        db = fdb.open(args.cluster_file)
    else:
        db = fdb.open()

    print("Connected to FoundationDB")
    print("MODE: READ-ONLY (diagnostic only, no modifications)")
    print("=" * 60)

    # Get running servers from fdbcli status (outside transaction)
    print("\n[0] Getting cluster status via fdbcli...")
    cluster_status = get_cluster_status(args.cluster_file, args.fdbcli)
    running_servers = {}
    if cluster_status:
        running_servers = extract_storage_servers_from_status(cluster_status)
        print(f"    Found {len(running_servers)} running storage servers")

        # Show cluster health summary
        client = cluster_status.get('client', {})
        cluster = cluster_status.get('cluster', {})
        print(f"    Cluster healthy: {cluster.get('database_status', {}).get('healthy', 'unknown')}")
        print(f"    Data state: {cluster.get('data', {}).get('state', {}).get('name', 'unknown')}")
    else:
        print("    Could not get cluster status (fdbcli may not be available)")

    # Check serverList vs running servers (runs by default)
    if not args.skip_check_serverlist:
        serverlist_result = check_serverlist_vs_running(db, args.cluster_file, args.fdbcli)
        if not serverlist_result['match'] and not serverlist_result.get('partial_match'):
            print("\n*** WARNING: serverList does not match running servers! ***")
            print("This may require serverList repair in addition to keyServers/serverKeys.")
        elif serverlist_result.get('partial_match'):
            print("\n*** serverList is CORRECT (all running servers found by prefix match) ***")

    @fdb.transactional
    def run_checks(tr):
        tr.options.set_read_system_keys()
        tr.options.set_lock_aware()

        # Get live servers
        print("\n[1] Getting serverList...")
        live_servers = get_server_list(tr)
        print(f"    Found {len(live_servers)} live servers")

        # Check keyServers
        print(f"\n[2] Checking keyServers (limit={args.limit})...")
        key_prefix = bytes.fromhex(args.key_prefix) if args.key_prefix else None

        if args.check_blog:
            key_prefix = b'\xff\x02/blog/'
            print(f"    Focusing on blog range: {key_prefix!r}")
        else:
            print(f"    Scanning ALL keyServers entries")

        ks_issues, ks_count = check_key_servers(tr, live_servers, key_prefix, args.limit)
        print(f"    Scanned {ks_count} keyServers entries")

        # Check serverKeys
        print(f"\n[3] Checking serverKeys (limit={args.limit})...")
        sk_issues, sk_count, servers_seen = check_server_keys(tr, live_servers, args.limit)
        print(f"    Scanned {sk_count} serverKeys entries")
        print(f"    Found entries for {len(servers_seen)} unique servers")

        # Dedicated blog check
        print(f"\n[3b] Counting blog entries specifically...")
        blog_ks_prefix = KEY_SERVERS_PREFIX + b'\xff\x02/blog'

        blog_ks_count = 0
        for k, v in tr.get_range(blog_ks_prefix, strinc(blog_ks_prefix), limit=args.limit):
            blog_ks_count += 1
        print(f"    Blog keyServers entries: {blog_ks_count}")

        # Count blog serverKeys entries (entries where the user_key starts with blog prefix)
        blog_sk_count = 0
        for k, v in tr.get_range(SERVER_KEYS_PREFIX, SERVER_KEYS_END, limit=args.limit):
            remainder = k[len(SERVER_KEYS_PREFIX):]
            if len(remainder) >= 17:  # Need 16 bytes UID + 1 byte "/" separator
                user_key = remainder[17:]  # Skip 16-byte UID + 1-byte "/" separator
                if user_key.startswith(b'\xff\x02/blog'):
                    blog_sk_count += 1
        print(f"    Blog serverKeys entries: {blog_sk_count}")

        # Report findings
        print("\n" + "=" * 60)
        print("FINDINGS")
        print("=" * 60)

        total_issues = 0

        # Shard statistics
        print("\n[Shard Statistics]")
        print(f"  Total shards (keyServers entries): {ks_issues.get('total_shards', ks_count)}")
        print(f"  Blog keyServers entries: {blog_ks_count}")
        print(f"  Blog serverKeys entries: {blog_sk_count}")
        print(f"  Unique servers referenced in keyServers: {len(ks_issues.get('servers_referenced', set()))}")
        print(f"  Shards with ALL live servers: {ks_issues.get('shards_all_live', 0)}")
        print(f"  Shards with SOME dead servers: {ks_issues.get('shards_some_dead', 0)}")
        print(f"  Shards with ALL dead servers: {ks_issues.get('shards_all_dead', 0)} <- DD can't route these!")

        if blog_ks_count == 0 and blog_sk_count > 0:
            print(f"\n  WARNING: Blog data exists in serverKeys ({blog_sk_count}) but NOT in keyServers!")
            print(f"           This means DD has no routing info for blog shards.")
        elif blog_sk_count == 0 and blog_ks_count > 0:
            print(f"\n  WARNING: Blog keyServers exist ({blog_ks_count}) but no blog serverKeys!")
            print(f"           This means keyServers references data no server claims to own.")

        # keyServers issues
        print("\n[keyServers Issues]")

        if ks_issues['uncoalesced']:
            print(f"\n  CRITICAL: {len(ks_issues['uncoalesced'])} uncoalesced adjacent entries!")
            print(f"            (This triggers ASSERT at KeyRangeMap.actor.cpp:297)")
            total_issues += len(ks_issues['uncoalesced'])
            for issue in ks_issues['uncoalesced'][:10]:
                print(f"    - keys: {issue['key1'][:30]!r}... -> {issue['key2'][:30]!r}...")
                print(f"      servers: {issue['src_servers']}")
            if len(ks_issues['uncoalesced']) > 10:
                print(f"    ... and {len(ks_issues['uncoalesced']) - 10} more")
        else:
            print("  No uncoalesced keyServers entries found")

        if ks_issues['dead_servers']:
            dead_unique = set(i['dead_server'] for i in ks_issues['dead_servers'])
            print(f"\n  WARNING: {len(ks_issues['dead_servers'])} references to {len(dead_unique)} dead servers")
            total_issues += len(ks_issues['dead_servers'])
            for srv in list(dead_unique)[:5]:
                print(f"    - {decode_uid(srv)}")
            if len(dead_unique) > 5:
                print(f"    ... and {len(dead_unique) - 5} more dead servers")
        else:
            print("  No dead server references found")

        if ks_issues['decode_errors']:
            print(f"\n  ERROR: {len(ks_issues['decode_errors'])} entries failed to decode")
            total_issues += len(ks_issues['decode_errors'])
        else:
            print("  No decode errors (values all parseable)")

        if ks_issues['empty_sources']:
            print(f"\n  CRITICAL: {len(ks_issues['empty_sources'])} entries with no source servers")
            print(f"            (This triggers ASSERT(src.size()) in DD - DDTxnProcessor.actor.cpp:72)")
            total_issues += len(ks_issues['empty_sources'])
            for issue in ks_issues['empty_sources'][:5]:
                print(f"    - key: {issue['key'][:40]!r}...")
            if len(ks_issues['empty_sources']) > 5:
                print(f"    ... and {len(ks_issues['empty_sources']) - 5} more")
        else:
            print("  No empty source servers (ASSERT(src.size()) OK - DDTxnProcessor.actor.cpp:72)")

        if ks_issues.get('keyservers_empty'):
            print(f"\n  CRITICAL: keyServers is completely empty!")
            print(f"            (This triggers ASSERT_GT(keyServers.size(), 0) in DD)")
            total_issues += 1
        else:
            print("  keyServers is not empty (ASSERT_GT(keyServers.size(), 0) OK)")

        # serverKeys statistics
        print("\n[serverKeys Statistics]")
        print(f"  Total ranges claimed by servers: {sk_issues.get('total_ranges_claimed', 0)}")
        print(f"  Blog ranges: {sk_issues.get('blog_ranges', 0)}")
        print(f"  Ranges claimed by live servers: {sk_issues.get('live_server_ranges', 0)}")
        print(f"  Ranges claimed by dead servers: {sk_issues.get('dead_server_ranges', 0)}")

        # serverKeys issues
        print("\n[serverKeys Issues]")

        if sk_issues['uncoalesced']:
            # Categorize by live/dead and full-keyspace
            live_uncoalesced = [u for u in sk_issues['uncoalesced'] if u.get('is_live', False)]
            dead_uncoalesced = [u for u in sk_issues['uncoalesced'] if not u.get('is_live', False)]
            full_keyspace = [u for u in sk_issues['uncoalesced'] if u.get('is_full_keyspace', False)]
            full_ks_live = [u for u in full_keyspace if u.get('is_live', False)]
            full_ks_dead = [u for u in full_keyspace if not u.get('is_live', False)]

            # Count unique servers
            live_servers_affected = set(u['server_id'] for u in live_uncoalesced)
            dead_servers_affected = set(u['server_id'] for u in dead_uncoalesced)

            severity = "CRITICAL" if live_uncoalesced else "WARNING"
            print(f"\n  {severity}: {len(sk_issues['uncoalesced'])} uncoalesced adjacent entries!")
            if live_uncoalesced:
                print(f"            Live server entries may trigger ASSERT during shard moves")
                print(f"            (MoveKeys.cpp unassignServerKeys asserts no adjacent duplicates when writing)")
            else:
                print(f"            All on dead servers — will NOT crash DD (dead servers are never read)")
            print(f"\n    Breakdown:")
            print(f"      Live servers: {len(live_uncoalesced)} entries across {len(live_servers_affected)} servers")
            print(f"      Dead servers: {len(dead_uncoalesced)} entries across {len(dead_servers_affected)} servers")
            if full_keyspace:
                print(f"\n    FULL KEYSPACE entries (b'' -> b'\\xff\\xff') — orphaned dead server state:")
                print(f"      Live servers: {len(full_ks_live)}")
                print(f"      Dead servers: {len(full_ks_dead)}")
            print(f"\n    NOTE: DD only reads serverKeys for servers in serverList (live servers).")
            print(f"          Dead server entries are inert garbage — safe to clean up but not urgent.")

            total_issues += len(sk_issues['uncoalesced'])

            # Show live server issues first (more concerning)
            if live_uncoalesced:
                print(f"\n    Live server uncoalesced entries:")
                for issue in live_uncoalesced[:10]:
                    marker = " [FULL KEYSPACE!]" if issue.get('is_full_keyspace') else ""
                    print(f"      - server: {short_uid(issue['server_id'])}{marker}")
                    print(f"        keys: {issue['key1'][:30]!r}... -> {issue['key2'][:30]!r}...")
                if len(live_uncoalesced) > 10:
                    print(f"      ... and {len(live_uncoalesced) - 10} more live server entries")

            # Show dead server issues
            if dead_uncoalesced:
                print(f"\n    Dead server uncoalesced entries:")
                for issue in dead_uncoalesced[:5]:
                    marker = " [FULL KEYSPACE!]" if issue.get('is_full_keyspace') else ""
                    print(f"      - server: {short_uid(issue['server_id'])}{marker}")
                    print(f"        keys: {issue['key1'][:30]!r}... -> {issue['key2'][:30]!r}...")
                if len(dead_uncoalesced) > 5:
                    print(f"      ... and {len(dead_uncoalesced) - 5} more dead server entries")
        else:
            print("  No uncoalesced serverKeys entries found")

        if sk_issues['dead_servers']:
            dead_unique = set(i['server_id'] for i in sk_issues['dead_servers'])
            print(f"\n  WARNING: {len(sk_issues['dead_servers'])} entries for {len(dead_unique)} dead servers")
            print(f"           (Dead server refs are inert — DD never reads serverKeys for servers not in serverList)")
            total_issues += len(sk_issues['dead_servers'])
            for srv in list(dead_unique)[:5]:
                print(f"    - {decode_uid(srv)}")
        else:
            print("  No dead server entries found (all servers in serverList)")

        if sk_issues['malformed_uid']:
            print(f"\n  ERROR: {len(sk_issues['malformed_uid'])} malformed server UIDs")
            total_issues += len(sk_issues['malformed_uid'])
        else:
            print("  No malformed server UIDs (all UIDs are 16 bytes)")

        # Verbose: show blog range details
        if args.verbose and args.check_blog:
            check_key_servers_for_range(tr, live_servers,
                                        b'\xff\x02/blog/', b'\xff\x02/blog0',
                                        verbose=True)

        # Check for mismatches between keyServers and serverKeys (sample check)
        mismatches = []
        if args.check_mismatch:
            print(f"\n[4] Checking keyServers vs serverKeys consistency (sample)...")
            mismatches = check_keyservers_serverkeys_mismatch(tr, live_servers, key_prefix, args.limit)

            if mismatches:
                print(f"\n  WARNING: {len(mismatches)} mismatches found!")
                total_issues += len(mismatches)

                by_type = {}
                for m in mismatches:
                    t = m['type']
                    by_type[t] = by_type.get(t, 0) + 1

                for t, count in by_type.items():
                    print(f"    - {t}: {count}")

                # Show some examples
                for m in mismatches[:5]:
                    if 'range_start' in m:
                        print(f"    Example: {m['type']}")
                        print(f"      range: [{m['range_start'][:20]!r}..., {m['range_end'][:20]!r}...)")
                        print(f"      server: {short_uid(m['server_id'])}")
            else:
                print("  No mismatches found (sampled check)")

        # Check for wrong_shard_server scenario - this is the original DD Stuck problem
        # Runs by default because it's the key check for the production issue
        wrong_shard_issues = {'wrong_shard_ranges': [], 'orphan_serverkeys': []}
        if not args.skip_wrong_shard:
            step_num = 5 if args.check_mismatch else 4
            print(f"\n[{step_num}] Checking for wrong_shard_server scenario...")
            print("    (This detects: keyServers says server owns shard, but serverKeys disagrees)")
            wrong_shard_issues = check_wrong_shard_server(tr, live_servers, key_prefix, args.limit)

            print("\n[wrong_shard_server Issues]")

            if wrong_shard_issues['wrong_shard_ranges']:
                wsr = wrong_shard_issues['wrong_shard_ranges']
                # Group by server
                by_server = {}
                for issue in wsr:
                    srv = issue['server_id']
                    if srv not in by_server:
                        by_server[srv] = []
                    by_server[srv].append(issue)

                # Also group by RANGE to identify ranges where ALL servers are missing
                # This answers: which ranges will cause DD to loop forever?
                by_range = {}  # (range_start, range_end) -> list of missing server_ids
                for issue in wsr:
                    range_key = (issue['range_start'], issue['range_end'])
                    if range_key not in by_range:
                        by_range[range_key] = []
                    by_range[range_key].append(issue['server_id'])

                # We need to know total servers per range from keyServers to determine
                # if ALL or SOME are missing. Re-read keyServers to get this info.
                @fdb.transactional
                def get_servers_for_ranges(tr, ranges_to_check):
                    """Get the server count from keyServers for specific ranges."""
                    set_read_transaction_options(tr)
                    range_servers = {}
                    for range_start, range_end in ranges_to_check:
                        # Find keyServers entry covering this range
                        range_start_bytes = range_start if isinstance(range_start, bytes) else range_start.encode('latin-1')
                        ks_key = KEY_SERVERS_PREFIX + range_start_bytes
                        # Read the keyServers entry at or before this key
                        for k, v in tr.get_range(KEY_SERVERS_PREFIX, ks_key + b'\xff', limit=1, reverse=True):
                            src_servers, dest_servers = decode_key_servers_value_simple(v)
                            if src_servers:
                                range_servers[(range_start, range_end)] = len(src_servers)
                            break
                    return range_servers

                try:
                    range_server_counts = get_servers_for_ranges(db, list(by_range.keys())[:1000])
                except:
                    range_server_counts = {}

                all_missing_ranges = []  # Ranges where ALL servers are missing
                some_missing_ranges = []  # Ranges where SOME servers are missing

                for (range_start, range_end), missing_servers in by_range.items():
                    total_servers = range_server_counts.get((range_start, range_end), len(missing_servers))
                    if len(missing_servers) >= total_servers:
                        all_missing_ranges.append({
                            'range_start': range_start,
                            'range_end': range_end,
                            'missing_count': len(missing_servers),
                            'total_servers': total_servers
                        })
                    else:
                        some_missing_ranges.append({
                            'range_start': range_start,
                            'range_end': range_end,
                            'missing_count': len(missing_servers),
                            'total_servers': total_servers
                        })

                print(f"\n  CRITICAL: {len(wsr)} wrong_shard_server issues across {len(by_server)} servers!")
                print(f"            Unique ranges affected: {len(by_range)}")
                print()

                # Report by range category - this is what answers the DD stuck question
                if all_missing_ranges:
                    # Categorize ALL_MISSING ranges by type
                    blog_ranges = []
                    backup_ranges = []
                    system_ranges = []
                    user_ranges = []

                    for r in all_missing_ranges:
                        rs = r['range_start']
                        if rs.startswith(b'\xff\x02/blog/'):
                            blog_ranges.append(r)
                        elif rs.startswith(b'\xff\x02/backup') or rs.startswith(b'\xff\x02/fdbbackup'):
                            backup_ranges.append(r)
                        elif rs.startswith(b'\xff'):
                            system_ranges.append(r)
                        else:
                            user_ranges.append(r)

                    print(f"    RANGES WHERE *ALL* SERVERS MISSING: {len(all_missing_ranges)}")
                    print("    --> DD WILL LOOP FOREVER on these ranges until 15-min timeout!")
                    print()
                    print(f"    BREAKDOWN BY TYPE:")
                    print(f"      Blog ranges (\\xff\\x02/blog/):     {len(blog_ranges)}")
                    print(f"      Backup ranges:                     {len(backup_ranges)}")
                    print(f"      Other system ranges (\\xff...):    {len(system_ranges)}")
                    print(f"      User data ranges:                  {len(user_ranges)}")
                    print()

                    # Show most critical first (user data, then blog)
                    if user_ranges:
                        print(f"    CRITICAL - USER DATA RANGES ({len(user_ranges)}):")
                        for r in user_ranges[:5]:
                            print(f"      [{r['range_start'][:40]!r}...)")
                            print(f"        All {r['total_servers']} servers missing from serverKeys")
                        if len(user_ranges) > 5:
                            print(f"      ... and {len(user_ranges) - 5} more user data ranges")
                        print()

                    if blog_ranges:
                        print(f"    BLOG RANGES ({len(blog_ranges)}) - backup log data:")
                        for r in blog_ranges[:3]:
                            print(f"      [{r['range_start'][:40]!r}...)")
                        if len(blog_ranges) > 3:
                            print(f"      ... and {len(blog_ranges) - 3} more blog ranges")
                        print()

                    if backup_ranges:
                        print(f"    BACKUP RANGES ({len(backup_ranges)}):")
                        for r in backup_ranges[:3]:
                            print(f"      [{r['range_start'][:40]!r}...)")
                        if len(backup_ranges) > 3:
                            print(f"      ... and {len(backup_ranges) - 3} more backup ranges")
                        print()

                if some_missing_ranges:
                    print(f"    RANGES WHERE *SOME* SERVERS MISSING: {len(some_missing_ranges)}")
                    print("    --> DD can retry with other replicas (random selection)")
                    for r in some_missing_ranges[:3]:
                        print(f"      [{r['range_start'][:30]!r}...)")
                        print(f"        {r['missing_count']}/{r['total_servers']} servers missing")
                    if len(some_missing_ranges) > 3:
                        print(f"      ... and {len(some_missing_ranges) - 3} more")
                    print()

                total_issues += len(wsr)

                # Show by server
                for srv in list(by_server.keys())[:5]:
                    issues_for_srv = by_server[srv]
                    print(f"\n    Server {short_uid(srv)}: {len(issues_for_srv)} ranges")
                    for issue in issues_for_srv[:3]:
                        print(f"      - [{issue['range_start'][:25]!r}..., {issue['range_end'][:25]!r}...)")
                        print(f"        reason: {issue['reason']}")
                    if len(issues_for_srv) > 3:
                        print(f"      ... and {len(issues_for_srv) - 3} more ranges")

                if len(by_server) > 5:
                    print(f"\n    ... and {len(by_server) - 5} more servers with issues")
            else:
                print("  No wrong_shard_server issues found (keyServers and serverKeys are consistent)")

            if wrong_shard_issues['orphan_serverkeys']:
                orphans = wrong_shard_issues['orphan_serverkeys']
                print(f"\n  WARNING: {len(orphans)} servers have serverKeys entries but not in keyServers")
                print("           (These servers claim to own data but DD doesn't route to them)")
                total_issues += len(orphans)

                for orphan in orphans[:5]:
                    print(f"    - Server {short_uid(orphan['server_id'])}: claims {orphan['num_ranges']} ranges")
                    for r in orphan['sample_ranges'][:2]:
                        print(f"        [{r[0][:25]!r}..., {r[1][:25]!r}...)")
                if len(orphans) > 5:
                    print(f"    ... and {len(orphans) - 5} more orphan servers")
            else:
                print("  No orphan serverKeys entries found")
        else:
            print("\n[--skip-wrong-shard] Skipping wrong_shard_server check")

        print("\n" + "=" * 60)
        if total_issues > 0:
            print(f"TOTAL: {total_issues} issues found")
        else:
            print("NO CORRUPTION DETECTED")

        return ks_issues, sk_issues, wrong_shard_issues, total_issues, live_servers, servers_seen

    try:
        ks_issues, sk_issues, wrong_shard_issues, total_issues, serverlist_servers, serverkeys_servers = run_checks(db)

        # Compare server sets
        if running_servers:
            keyservers_servers = ks_issues.get('servers_referenced', set())
            comparison = compare_server_sets(
                keyservers_servers,
                serverlist_servers,
                serverkeys_servers,
                running_servers
            )

        # Test encoding round-trip if requested
        if args.test_encoding:
            @fdb.transactional
            def run_encoding_tests(tr):
                tr.options.set_read_system_keys()
                tr.options.set_lock_aware()
                tr.options.set_timeout(30000)

                ks_ok = test_keyservers_encoding_roundtrip(tr, limit=1000)
                sk_ok = test_serverkeys_encoding_roundtrip(tr, limit=1000)
                return ks_ok and sk_ok

            encoding_ok = run_encoding_tests(db)
            if encoding_ok:
                print("\n  ENCODING TEST PASSED: Safe to proceed with repair", flush=True)
            else:
                print("\n  ENCODING TEST FAILED: Do not proceed with repair!", flush=True)

        # Initialize orphan_ranges for later use
        orphan_ranges = None

        # Analyze orphan servers (runs by default)
        if not args.skip_analyze_orphans:
            @fdb.transactional
            def analyze_orphans(tr):
                tr.options.set_read_system_keys()
                tr.options.set_lock_aware()
                tr.options.set_timeout(60000)  # 60 second timeout

                # Get servers that are in serverList but not in keyServers
                sl_set = set(serverlist_servers.keys())
                ks_set = ks_issues.get('servers_referenced', set())
                orphan_ids = sl_set - ks_set

                if orphan_ids:
                    orphan_ranges = get_serverkeys_ranges_for_orphan_servers(tr, orphan_ids)
                    return orphan_ranges
                else:
                    print("\n[Orphan Server Ranges]")
                    print("  No orphan servers found (all serverList servers are in keyServers)")
                    return {}

            orphan_ranges = analyze_orphans(db)

            # Check if orphan ranges cover the keyspace
            if orphan_ranges:
                all_ranges = []
                for ranges in orphan_ranges.values():
                    all_ranges.extend(ranges)
                all_ranges.sort(key=lambda r: r[0])

                print(f"\n  Total ranges from orphan servers: {len(all_ranges)}", flush=True)
                if all_ranges:
                    print(f"  First range starts at: {all_ranges[0][0][:30]!r}...", flush=True)
                    print(f"  Last range ends at: {all_ranges[-1][1][:30]!r}...", flush=True)

        # Try to discover SS shard ownership (runs by default)
        if not args.skip_discover_ss:
            print("\n[Storage Server Discovery]", flush=True)
            print("  Attempting to discover what shards running SSs have...", flush=True)

            # The FDB client can't directly query SS shard ownership
            # But we can try a few approaches:

            # 1. Check if there's useful info in special keys
            special_info = get_ss_shard_info_from_special_keys(db)
            if special_info and 'error' not in special_info and 'metrics_error' not in special_info:
                print(f"  Found {len(special_info)} special key entries", flush=True)
                for k, v in list(special_info.items())[:5]:
                    print(f"    {k}: {v[:50] if isinstance(v, bytes) else v}...", flush=True)
            else:
                print("  No useful info from special keys", flush=True)

            # 2. Try to map old server IDs to new ones by address
            print("\n  Attempting to map old server IDs to new (by address)...")
            mapping, unmatched_old, unmatched_new = attempt_server_id_mapping(
                serverlist_servers, running_servers
            )

            if mapping:
                print(f"\n  SUCCESS: Found {len(mapping)} ID mappings!", flush=True)
                print("  This means we can potentially rebuild keyServers by:", flush=True)
                print("    1. Take serverKeys ranges from old (serverList) server IDs", flush=True)
                print("    2. Map to new (running) server IDs", flush=True)
                print("    3. Write new keyServers entries pointing to running servers", flush=True)

                # Verify SS ownership (runs by default)
                if not args.skip_verify_ss and orphan_ranges:
                    verify_ss_ownership(
                        db,
                        orphan_ranges,
                        mapping,
                        running_servers,
                        sample_count=args.verify_samples
                    )
                elif not args.skip_verify_ss and not orphan_ranges:
                    print("\n  NOTE: No orphan ranges to verify (orphan analysis may be skipped or no orphans found)", flush=True)
            else:
                print("\n  Could not map server IDs by address.", flush=True)
                print("  Alternative approaches to discover SS shards:", flush=True)
                print("    a) fdbcli: 'datadistribution status' (if DD were running)", flush=True)
                print("    b) Query SS directly via internal RPC (requires C++ tool)", flush=True)
                print("    c) Examine SS local storage on the pods", flush=True)
                print("    d) Manual mapping based on PVC/pod names", flush=True)

        # Probe SSs with actual reads to detect missing shards (runs by default)
        if not args.skip_probe_ss:
            # Build list of ranges to probe from keyServers (use same limit as main scan)
            @fdb.transactional
            def get_keyservers_ranges_for_probe(tr, limit):
                tr.options.set_read_system_keys()
                tr.options.set_lock_aware()
                tr.options.set_timeout(120000)

                ranges = []
                prev_key = None

                for k, v in tr.get_range(KEY_SERVERS_PREFIX, KEY_SERVERS_END, limit=limit):
                    user_key = k[len(KEY_SERVERS_PREFIX):]

                    if prev_key is not None:
                        # Range is [prev_key, user_key)
                        ranges.append((prev_key, user_key))

                    prev_key = user_key

                return ranges

            probe_ranges = get_keyservers_ranges_for_probe(db, args.limit)
            if probe_ranges:
                probe_result = probe_ss_for_shards(db, probe_ranges, sample_count=args.probe_samples)
                if probe_result.get('wrong_shard', 0) > 0:
                    total_issues += probe_result['wrong_shard']

        # Run C++ audit_ss_shards if binary is provided, OR use preloaded audit data
        cpp_audit_result = None
        audit_correlation_results = None  # Store audit correlation for final summary
        keyservers_audit_results = None  # For keyServers-based audit
        if preloaded_audit_data:
            # Use preloaded audit data from --report-audit
            cpp_audit_result = preloaded_audit_data

            if is_keyservers_audit(cpp_audit_result):
                # KeyServers-based audit (phantom shard detection)
                print("\n[KeyServers Audit - Using preloaded audit data]", flush=True)
                keyservers_audit_results = process_keyservers_audit(db, cpp_audit_result, verbose=args.verbose)
                phantom_count = keyservers_audit_results.get('phantom_count', 0)
                if phantom_count > 0:
                    total_issues += phantom_count
            else:
                # Standard serverKeys-based audit
                print("\n[C++ SS Shard Audit - Using preloaded audit data]", flush=True)
                cpp_missing = cpp_audit_result.get('total_missing_from_ss', 0)
                cpp_extra = cpp_audit_result.get('total_extra_in_ss', 0)
                print(f"  Total servers: {cpp_audit_result.get('total_servers', 0)}", flush=True)
                print(f"  Reachable: {cpp_audit_result.get('reachable_servers', 0)}", flush=True)
                print(f"  Servers with mismatches: {cpp_audit_result.get('servers_with_mismatches', 0)}", flush=True)
                print(f"  Ranges missing from SS: {cpp_missing}", flush=True)
                print(f"  Ranges extra in SS: {cpp_extra}", flush=True)
                if cpp_missing > 0:
                    total_issues += cpp_missing
                # Generate detailed comparison report
                generate_audit_comparison_report(db, cpp_audit_result, verbose=args.verbose)
                # Correlate audit findings with metadata analysis
                serverkeys_by_server = {sid: [] for sid in serverkeys_servers}  # Just need the server IDs
                correlate_audit_with_metadata(cpp_audit_result, serverkeys_by_server)
                # Check if stale serverKeys entries will cause wrong_shard_server
                audit_correlation_results = correlate_stale_serverkeys_with_keyservers(db, cpp_audit_result, verbose=args.verbose)
        elif args.audit_ss_shards and not args.skip_audit_ss_shards:
            cpp_audit_result = run_audit_ss_shards(args.audit_ss_shards, args.cluster_file)
            if cpp_audit_result:
                cpp_missing = cpp_audit_result.get('total_missing_from_ss', 0)
                cpp_extra = cpp_audit_result.get('total_extra_in_ss', 0)
                if cpp_missing > 0:
                    # This is authoritative - SS directly says it doesn't have these shards
                    total_issues += cpp_missing
                # Correlate audit findings with metadata analysis
                serverkeys_by_server = {sid: [] for sid in serverkeys_servers}
                correlate_audit_with_metadata(cpp_audit_result, serverkeys_by_server)
                # Check if stale serverKeys entries will cause wrong_shard_server
                audit_correlation_results = correlate_stale_serverkeys_with_keyservers(db, cpp_audit_result, verbose=args.verbose)

        # Three-way correlation if we have both wrong_shard_server data and audit data
        three_way_results = None
        if cpp_audit_result and wrong_shard_issues.get('wrong_shard_ranges'):
            three_way_results = three_way_metadata_correlation(
                db, wrong_shard_issues, cpp_audit_result, serverlist_servers, verbose=args.verbose
            )

        if total_issues > 0:
            # ================================================================
            # EXECUTIVE SUMMARY - What DD Will Experience
            # ================================================================
            print("\n" + "=" * 70)
            print("EXECUTIVE SUMMARY - DD IMPACT PREDICTION")
            print("=" * 70)

            wsr = wrong_shard_issues.get('wrong_shard_ranges', [])
            if wsr:
                # Group by range and count blog vs user data
                by_range = {}
                for issue in wsr:
                    range_key = (issue['range_start'], issue['range_end'])
                    if range_key not in by_range:
                        by_range[range_key] = []
                    by_range[range_key].append(issue['server_id'])

                # Categorize ranges
                blog_count = 0
                backup_count = 0
                system_count = 0
                user_count = 0

                for (rs, re), servers in by_range.items():
                    if rs.startswith(b'\xff\x02/blog/'):
                        blog_count += 1
                    elif rs.startswith(b'\xff\x02/backup') or rs.startswith(b'\xff\x02/fdbbackup'):
                        backup_count += 1
                    elif rs.startswith(b'\xff'):
                        system_count += 1
                    else:
                        user_count += 1

                total_all_missing = len(by_range)

                print()
                print(f"  RANGES WHERE DD WILL LOOP (until 15-min timeout):")
                print(f"  ================================================")
                print(f"    Total unique ranges:     {total_all_missing:,}")
                print()
                print(f"    Blog ranges:             {blog_count:,}  (backup mutation logs)")
                print(f"    Backup/agent ranges:     {backup_count:,}")
                print(f"    Other system ranges:     {system_count:,}")
                print(f"    USER DATA ranges:        {user_count:,}  {'<-- CRITICAL' if user_count > 0 else ''}")
                print()

                if user_count > 0:
                    print(f"  !! WARNING: {user_count} USER DATA ranges will timeout !!")
                    print(f"     DD will return estimated metrics after 15 minutes per range.")
                    print()

                # Estimate DD startup time impact
                estimated_timeout_minutes = total_all_missing * 15
                print(f"  ESTIMATED DD IMPACT:")
                print(f"    If DD hits all {total_all_missing:,} ranges sequentially:")
                print(f"    Worst case: {estimated_timeout_minutes:,} minutes ({estimated_timeout_minutes / 60:.1f} hours)")
                print(f"    (In practice, DD may skip some or process in parallel)")
                print()
            else:
                print()
                print("  No wrong_shard_server issues detected.")
                print("  DD should not loop on shard metrics.")
                print()

            print("=" * 70)
            print()

            print("=" * 60)
            print("ANALYSIS")
            print("=" * 60)

            # Categorize issues
            has_wrong_shard = bool(wrong_shard_issues.get('wrong_shard_ranges'))
            has_uncoalesced = bool(ks_issues.get('uncoalesced') or sk_issues.get('uncoalesced'))
            has_dead_refs = bool(ks_issues.get('dead_servers') or sk_issues.get('dead_servers'))

            if has_wrong_shard:
                print("\nWRONG_SHARD_SERVER ISSUES DETECTED:")
                wsr = wrong_shard_issues.get('wrong_shard_ranges', [])
                # Compute unique ranges with ALL servers missing
                by_range = {}
                for issue in wsr:
                    range_key = (issue['range_start'], issue['range_end'])
                    if range_key not in by_range:
                        by_range[range_key] = 0
                    by_range[range_key] += 1

                print(f"  Metadata check: {len(wsr)} (server, range) pairs where")
                print(f"    keyServers says server owns range, but serverKeys disagrees")
                print(f"  Unique ranges affected: {len(by_range)}")

                # Cross-reference with audit if available
                if audit_correlation_results and isinstance(audit_correlation_results, dict):
                    acr = audit_correlation_results
                    all_missing = len(acr.get('all_missing', []))
                    some_missing = len(acr.get('some_missing', []))
                    not_in_ks = len(acr.get('not_in_keyservers', []))
                    verified_data_exists = acr.get('verified_data_exists', None)

                    print(f"\n  AUDIT CORRELATION (stale serverKeys vs keyServers):")
                    print(f"    ALL_MISSING (stale serverKeys in keyServers): {all_missing} ranges")
                    print(f"    SOME_MISSING (partial stale):                 {some_missing} ranges")
                    print(f"    NOT_IN_KEYSERVERS (pure garbage):             {not_in_ks} entries")

                    # Check verification results
                    if verified_data_exists is True:
                        print(f"\n  VERIFIED: All {all_missing} 'ALL_MISSING' ranges have data!")
                        print(f"            Probing via client API found data on other replicas.")
                        print(f"            These are stale serverKeys entries - garbage to clean.")
                        print(f"            NO data loss. NO DD loops (client routing works).")
                    elif verified_data_exists is False:
                        verification = acr.get('verification', {})
                        wss_count = verification.get('wrong_shard_server', 0)
                        print(f"\n  CRITICAL: {wss_count} ranges return wrong_shard_server!")
                        print(f"            DD WILL loop forever on these until 15-min timeout.")
                    elif all_missing > 0:
                        print(f"\n  WARNING: {all_missing} ranges where ALL stale servers listed.")
                        print(f"           Verification not yet run - run full report to confirm.")
                    elif all_missing == 0 and (some_missing > 0 or not_in_ks > 0):
                        print(f"\n  GOOD NEWS: No ranges where ALL servers are missing.")
                        print(f"             Stale entries are garbage to clean, not causing DD loops.")
                else:
                    print(f"\n  NOTE: Run with --report-audit or --audit-ss-shards to get SS confirmation")
                    print(f"        Audit directly queries each SS to confirm if it has the data.")

                # Add three-way correlation summary if available
                if three_way_results and isinstance(three_way_results, dict):
                    twr = three_way_results
                    print(f"\n  THREE-WAY CORRELATION SUMMARY:")
                    print(f"    Missing serverKeys:  {twr.get('total_missing_serverkeys', 0):,} entries")
                    print(f"      (keyServers claims, serverKeys has no entry, but SS has data)")
                    print(f"    Stale serverKeys:    {twr.get('total_stale_serverkeys', 0):,} entries")
                    print(f"      (serverKeys claims, but SS doesn't have data)")
                    print(f"    Correct serverKeys:  {twr.get('total_confirmed', 0):,} entries")
                    print(f"      (serverKeys claims, SS confirmed it has data)")

                print("\n  Potential fixes:")
                print("  1. Delete stale serverKeys entries (cleanup)")
                print("  2. Coalesce uncoalesced entries if present")

            if has_uncoalesced:
                print("\nUNCOALESCED ENTRIES DETECTED:")
                print("  Adjacent KRM entries with the same value should be merged.")
                print("  Impact:")
                print("    - keyServers: Wastes memory in DD shard map (no crash)")
                print("    - serverKeys (live): May trigger ASSERT during active shard moves")
                print("      (MoveKeys.cpp unassignServerKeys asserts no adjacent duplicates when writing)")
                print("    - serverKeys (dead): Inert garbage, never read by DD")
                print("\n  Fix: Run ./metadata-audit.sh repair-coalesce to delete redundant entries (idempotent)")

            if has_dead_refs:
                print("\nDEAD SERVER REFERENCES DETECTED:")
                print("  Entries reference servers not in serverList.")
                print("  This is usually less critical but may cause issues.")

            print("\n" + "=" * 60)
            print("Repair is disabled until we understand the full corruption picture.")
            sys.exit(1)
        else:
            sys.exit(0)

    except fdb.FDBError as e:
        print(f"FDB Error: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)

if __name__ == '__main__':
    main()
