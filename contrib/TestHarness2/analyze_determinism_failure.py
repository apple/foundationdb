#!/usr/bin/env python3
"""
Script to analyze FoundationDB determinism check failures.

This script compares trace files from two runs of the same test to identify
where determinism broke down. It focuses on S3 operations, bulk dump events,
and other timing-sensitive operations that commonly cause determinism failures.

Usage: python3 analyze_determinism_failure.py <initial_run_dir> <determinism_check_dir>
"""

import json
import sys
import os
import re
from pathlib import Path
from collections import defaultdict

def parse_trace_file(file_path):
    """Parse a trace file and extract relevant events."""
    events = []
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    event = json.loads(line)
                    events.append(event)
                except json.JSONDecodeError:
                    # Skip non-JSON lines
                    continue
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
        return []
    
    return events

def extract_key_events(events):
    """Extract events that are relevant for determinism analysis."""
    key_events = []
    
    # Events we care about for determinism
    relevant_types = {
        'BulkDumpDeterministicVersion',
        'BulkDumpingWorkLoadTransportMethod', 
        'S3ClientCopyUpFileEnd',
        'S3ClientCopyDownFileEnd',
        'S3ClientDeleteResourceEnd',
        'BulkDumpingWorkload',
        'WorkloadComplete',
        'TestResults',
        'ParseS3XMLResponse',
        'BulkDumpJobSpawnRange',
        'BulkDumpDoTaskStart',
        'BulkDumpDoTaskComplete',
        'BulkDumpDoTaskError'
    }
    
    for event in events:
        event_type = event.get('Type', '')
        
        # Include relevant event types
        if event_type in relevant_types:
            key_events.append(event)
            continue
            
        # Include events with S3-related details
        if any(key in event for key in ['S3URL', 'S3Error', 'Bucket', 'Object']):
            key_events.append(event)
            continue
            
        # Include events with version information
        if any(key in event for key in ['Version', 'DeterministicVersion', 'BaseVersion']):
            key_events.append(event)
            continue
    
    return key_events

def normalize_event(event):
    """Normalize an event for comparison by removing timing-specific fields."""
    normalized = event.copy()
    
    # Remove fields that are expected to differ between runs
    fields_to_remove = [
        'Time', 'DateTime', 'ActualTime', 'Timestamp',
        'Machine', 'LogGroup', 'ID', 'ThreadID',
        'Duration'  # We'll handle this specially
    ]
    
    for field in fields_to_remove:
        normalized.pop(field, None)
    
    # Keep Duration but round it to reduce noise
    if 'Duration' in event:
        try:
            duration = float(event['Duration'])
            normalized['Duration'] = round(duration, 3)  # Round to milliseconds
        except (ValueError, TypeError):
            pass
    
    return normalized

def compare_event_sequences(events1, events2, label1="Run 1", label2="Run 2"):
    """Compare two sequences of events and identify differences."""
    print(f"\n=== Comparing {label1} vs {label2} ===")
    
    # Group events by type
    events1_by_type = defaultdict(list)
    events2_by_type = defaultdict(list)
    
    for event in events1:
        events1_by_type[event.get('Type', 'Unknown')].append(normalize_event(event))
    
    for event in events2:
        events2_by_type[event.get('Type', 'Unknown')].append(normalize_event(event))
    
    all_types = set(events1_by_type.keys()) | set(events2_by_type.keys())
    
    differences_found = False
    
    for event_type in sorted(all_types):
        events1_type = events1_by_type[event_type]
        events2_type = events2_by_type[event_type]
        
        if len(events1_type) != len(events2_type):
            print(f"\n❌ {event_type}: Different counts - {label1}: {len(events1_type)}, {label2}: {len(events2_type)}")
            differences_found = True
            continue
        
        # Compare each event of this type
        for i, (e1, e2) in enumerate(zip(events1_type, events2_type)):
            if e1 != e2:
                print(f"\n❌ {event_type} #{i+1}: Events differ")
                
                # Show specific differences
                all_keys = set(e1.keys()) | set(e2.keys())
                for key in sorted(all_keys):
                    val1 = e1.get(key, '<missing>')
                    val2 = e2.get(key, '<missing>')
                    if val1 != val2:
                        print(f"  {key}: {label1}={val1}, {label2}={val2}")
                
                differences_found = True
            else:
                print(f"✅ {event_type} #{i+1}: Match")
    
    if not differences_found:
        print(f"\n✅ All events match between {label1} and {label2}!")
    
    return not differences_found

def extract_s3_operations(events):
    """Extract S3 operations and their file names."""
    s3_ops = []
    
    for event in events:
        # Look for S3 file operations
        if 'S3URL' in event or 'Object' in event:
            op_info = {
                'Type': event.get('Type', 'Unknown'),
                'Time': event.get('Time', 0),
                'Object': event.get('Object', ''),
                'S3URL': event.get('S3URL', ''),
                'Duration': event.get('Duration', 0)
            }
            
            # Extract filename from URL or Object
            filename = ''
            if op_info['Object']:
                filename = op_info['Object']
            elif op_info['S3URL']:
                # Extract filename from URL
                match = re.search(r'/([^/]+)$', op_info['S3URL'])
                if match:
                    filename = match.group(1)
            
            op_info['Filename'] = filename
            s3_ops.append(op_info)
    
    return s3_ops

def analyze_s3_operations(events1, events2):
    """Analyze S3 operations for determinism issues."""
    print("\n=== S3 Operations Analysis ===")
    
    s3_ops1 = extract_s3_operations(events1)
    s3_ops2 = extract_s3_operations(events2)
    
    print(f"Run 1: {len(s3_ops1)} S3 operations")
    print(f"Run 2: {len(s3_ops2)} S3 operations")
    
    # Extract unique filenames
    files1 = set(op['Filename'] for op in s3_ops1 if op['Filename'])
    files2 = set(op['Filename'] for op in s3_ops2 if op['Filename'])
    
    print(f"\nFiles in Run 1: {sorted(files1)}")
    print(f"Files in Run 2: {sorted(files2)}")
    
    if files1 == files2:
        print("✅ Same files used in both runs")
    else:
        print("❌ Different files used!")
        print(f"Only in Run 1: {files1 - files2}")
        print(f"Only in Run 2: {files2 - files1}")
    
    return files1 == files2

def compare_early_events(initial_events, determinism_events, num_events=200):
    """Compare the first N events from each run to see if they start identically."""
    print(f"\n=== Comparing First {num_events} Events ===")
    
    if len(initial_events) < num_events:
        print(f"Warning: Initial run only has {len(initial_events)} events")
        num_events = min(num_events, len(initial_events))
    
    if len(determinism_events) < num_events:
        print(f"Warning: Determinism check only has {len(determinism_events)} events")
        num_events = min(num_events, len(determinism_events))
    
    differences = []
    
    for i in range(num_events):
        event1 = initial_events[i]
        event2 = determinism_events[i]
        
        # Compare event types
        type1 = event1.get('Type', 'Unknown')
        type2 = event2.get('Type', 'Unknown')
        
        if type1 != type2:
            differences.append(f"Event {i}: Type differs - Run1: {type1}, Run2: {type2}")
            continue
            
        # For timing-sensitive events, compare key fields
        if type1 in ['BulkDumpingWorkLoadSetKey', 'BulkDumpingWorkLoadTransportMethod', 'S3ClientCopyUpFileEnd']:
            # Compare all fields except timing-related ones
            fields_to_compare = set(event1.keys()) | set(event2.keys())
            timing_fields = {'Time', 'DateTime', 'Machine', 'LogGroup', 'Roles'}
            
            for field in fields_to_compare - timing_fields:
                val1 = event1.get(field)
                val2 = event2.get(field)
                if val1 != val2:
                    differences.append(f"Event {i} ({type1}): {field} differs - Run1: {val1}, Run2: {val2}")
    
    if differences:
        print(f"❌ Found {len(differences)} differences in first {num_events} events:")
        for diff in differences[:10]:  # Show first 10 differences
            print(f"  {diff}")
        if len(differences) > 10:
            print(f"  ... and {len(differences) - 10} more differences")
        return False
    else:
        print(f"✅ First {num_events} events are identical!")
        return True

def find_divergence_point(initial_events, determinism_events, max_events=None):
    """Find the exact point where the two runs diverge."""
    if max_events is None:
        max_events = max(len(initial_events), len(determinism_events))
    
    print(f"\n=== Finding Exact Divergence Point (up to {max_events} events) ===")
    
    max_compare = min(len(initial_events), len(determinism_events), max_events)
    
    for i in range(max_compare):
        event1 = initial_events[i]
        event2 = determinism_events[i]
        
        # Compare event types first
        type1 = event1.get('Type', 'Unknown')
        type2 = event2.get('Type', 'Unknown')
        
        if type1 != type2:
            print(f"🎯 FIRST DIVERGENCE at event {i}: Type differs")
            print(f"  Run 1: {type1}")
            print(f"  Run 2: {type2}")
            
            # Show context around divergence
            print(f"\n📋 Context around divergence (events {max(0, i-3)} to {min(max_compare, i+3)}):")
            for j in range(max(0, i-3), min(max_compare, i+4)):
                marker = " 🔥" if j == i else "   "
                if j < len(initial_events):
                    print(f"  {j:3d}{marker} Run1: {initial_events[j].get('Type', 'Unknown')}")
                if j < len(determinism_events):
                    print(f"      {marker} Run2: {determinism_events[j].get('Type', 'Unknown')}")
                if j < len(initial_events) and j < len(determinism_events):
                    print()
            
            return i
            
        # For same event types, compare key fields (excluding timing)
        if type1 in ['BulkDumpingWorkLoadSetKey', 'BulkDumpingWorkLoadTransportMethod', 'S3ClientCopyUpFileEnd', 'S3BlobStoreBadRequest']:
            fields_to_compare = set(event1.keys()) | set(event2.keys())
            timing_fields = {'Time', 'DateTime', 'Machine', 'LogGroup', 'Roles'}
            
            for field in fields_to_compare - timing_fields:
                val1 = event1.get(field)
                val2 = event2.get(field)
                if val1 != val2:
                    print(f"🎯 FIRST DIVERGENCE at event {i}: {type1}.{field} differs")
                    print(f"  Run 1: {val1}")
                    print(f"  Run 2: {val2}")
                    return i
    
    print(f"✅ All {max_compare} events are identical!")
    if len(initial_events) != len(determinism_events):
        print(f"⚠️  However, total event counts differ: {len(initial_events)} vs {len(determinism_events)}")
    
    return -1  # No divergence found

def analyze_duplication_pattern(initial_events, determinism_events):
    """Analyze where the duplication pattern starts."""
    print(f"\n=== Analyzing Duplication Pattern ===")
    
    # Look for where Run 2 starts having "extra" events
    len1, len2 = len(initial_events), len(determinism_events)
    print(f"Total events: Run 1 = {len1}, Run 2 = {len2}")
    
    if len2 < len1:
        print("⚠️ Run 2 has fewer events than Run 1 - unusual pattern")
        return
    
    # Check if Run 2 has roughly 2x the events
    ratio = len2 / len1 if len1 > 0 else 0
    print(f"Event count ratio: {ratio:.2f}x")
    
    if 1.8 <= ratio <= 2.2:
        print("🎯 Perfect ~2x duplication pattern detected!")
        
        # Estimate where duplication starts
        duplication_start = len1
        print(f"💡 Hypothesis: Duplication likely starts around event {duplication_start}")
        print(f"   Run 1 ends at event {len1}")
        print(f"   Run 2 continues with {len2 - len1} additional events")
        
        # Look at the events right at the boundary
        if duplication_start < len2:
            print(f"\n📋 Events around the duplication boundary:")
            start_idx = max(0, duplication_start - 5)
            end_idx = min(len2, duplication_start + 10)
            
            for i in range(start_idx, end_idx):
                marker = " 🔥" if i == duplication_start else "   "
                if i < len1:
                    print(f"  {i:4d}{marker} Run1&2: {initial_events[i].get('Type', 'Unknown')}")
                else:
                    print(f"  {i:4d}{marker} Run2 only: {determinism_events[i].get('Type', 'Unknown')}")
    else:
        print(f"⚠️ Not a simple 2x pattern - ratio is {ratio:.2f}x")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 analyze_determinism_failure.py <initial_run_trace_dir> <determinism_check_trace_dir>")
        print("Example: python3 analyze_determinism_failure.py trace_initial_run trace_determinism_check")
        sys.exit(1)
    
    initial_dir = Path(sys.argv[1])
    determinism_dir = Path(sys.argv[2])
    
    print(f"Analyzing determinism logs:")
    print(f"  Initial run: {initial_dir}")
    print(f"  Determinism check: {determinism_dir}")
    
    # Find trace files
    initial_traces = list(initial_dir.glob("*.json"))
    determinism_traces = list(determinism_dir.glob("*.json"))
    
    if not initial_traces:
        print(f"No trace files found in {initial_dir}")
        sys.exit(1)
    
    if not determinism_traces:
        print(f"No trace files found in {determinism_dir}")
        sys.exit(1)
    
    print(f"Found {len(initial_traces)} initial trace files")
    print(f"Found {len(determinism_traces)} determinism check trace files")
    
    # Parse all events
    initial_events = []
    for trace_file in initial_traces:
        initial_events.extend(parse_trace_file(trace_file))
    
    determinism_events = []
    for trace_file in determinism_traces:
        determinism_events.extend(parse_trace_file(trace_file))
    
    print(f"Parsed {len(initial_events)} events from initial run")
    print(f"Parsed {len(determinism_events)} events from determinism check")
    
    # Compare early events to see if runs start identically
    early_match = compare_early_events(initial_events, determinism_events)
    
    # Find the exact divergence point
    divergence_point = find_divergence_point(initial_events, determinism_events)
    
    # Analyze duplication pattern
    analyze_duplication_pattern(initial_events, determinism_events)
    
    # Extract key events
    key_initial = extract_key_events(initial_events)
    key_determinism = extract_key_events(determinism_events)
    
    print(f"Extracted {len(key_initial)} key events from initial run")
    print(f"Extracted {len(key_determinism)} key events from determinism check")
    
    # Analyze S3 operations
    s3_match = analyze_s3_operations(key_initial, key_determinism)
    
    # Compare event sequences
    events_match = compare_event_sequences(key_initial, key_determinism, "Initial Run", "Determinism Check")
    
    # Summary
    print("\n=== SUMMARY ===")
    if early_match and s3_match and events_match:
        print("✅ DETERMINISM CHECK PASSED: All events match!")
    else:
        print("❌ DETERMINISM CHECK FAILED: Differences found")
        if not early_match:
            print("  - Early events differ (runs diverge from the start)")
        if divergence_point >= 0:
            print(f"  - First divergence at event {divergence_point}")
        if not s3_match:
            print("  - S3 operations differ")
        if not events_match:
            print("  - Event sequences differ")

if __name__ == "__main__":
    main()
