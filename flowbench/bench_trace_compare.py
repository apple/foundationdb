#!/usr/bin/env python3
"""Before/after comparison of the TraceEvent flowbench benchmarks (flowbench/BenchTrace.cpp).

Build flowbench (`ninja flowbench`) in two trees, one without the change you want to measure and one with it, both
with BenchTrace.cpp. Then:

    python3 bench_trace_compare.py --before <before tree>/bin/flowbench --after <after tree>/bin/flowbench

Runs every benchmark on both binaries, ROUNDS times, interleaved (the before/after order is shuffled each round) and
pinned to the same cores, each run in its own process. One sample is the median CPU time of a run's REPS repetitions.
Prints a markdown table of the median sample per binary to stdout; progress goes to stderr.
"""
import argparse
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile

BENCHES = [
    # (name, label, needs a trace file)
    ("bench_trace_event_file_open", "event, trace file open", True),
    ("bench_trace_event_no_file", "event, no trace file", False),
    ("bench_trace_event_disabled", "disabled event", False),
    ("bench_flush_trace_file", "flush, nothing buffered", True),
]

p = argparse.ArgumentParser()
p.add_argument("--before", required=True, help="flowbench built without the change")
p.add_argument("--after", required=True, help="flowbench built with the change")
p.add_argument("--rounds", type=int, default=8)
p.add_argument("--reps", type=int, default=10)
p.add_argument("--min-time", default="0.5")
p.add_argument("--cpus", default="8-11", help="cores to pin to with taskset, same for every run")
a = p.parse_args()

for b in (a.before, a.after):
    if not os.access(b, os.X_OK):
        sys.exit("not an executable: " + b)


def run(binary, bench, needs_dir):
    tmp = tempfile.mkdtemp(prefix="bench_compare_")
    try:
        out = os.path.join(tmp, "out.json")
        env = dict(os.environ)
        env.pop("BENCH_TRACE_DIR", None)
        if needs_dir:
            os.mkdir(os.path.join(tmp, "trace"))
            env["BENCH_TRACE_DIR"] = os.path.join(tmp, "trace")
        cmd = ["taskset", "-c", a.cpus, binary, "--benchmark_filter=^%s$" % bench,
               "--benchmark_repetitions=%d" % a.reps, "--benchmark_min_time=%s" % a.min_time,
               "--benchmark_out=" + out, "--benchmark_out_format=json"]
        subprocess.run(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=900)
        rows = json.load(open(out))["benchmarks"]
        errors = [r for r in rows if r.get("error_occurred")]
        if errors:
            sys.exit("%s %s: %s" % (binary, bench, errors[0].get("error_message")))
        med = [r["cpu_time"] for r in rows if r.get("run_type") == "aggregate" and r.get("aggregate_name") == "median"]
        if len(med) != 1:
            sys.exit("%s %s: no median in output" % (binary, bench))
        return med[0]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


samples = {(b, w): [] for b, _, _ in BENCHES for w in ("before", "after")}
for r in range(a.rounds):
    for bench, _, needs_dir in BENCHES:
        order = [("before", a.before), ("after", a.after)]
        random.shuffle(order)
        for which, binary in order:
            samples[(bench, which)].append(run(binary, bench, needs_dir))
    print("round %d/%d done" % (r + 1, a.rounds), file=sys.stderr, flush=True)

print("before: %s\nafter:  %s\n%d rounds x %d repetitions x %ss, pinned to cores %s, median CPU time\n" % (
    a.before, a.after, a.rounds, a.reps, a.min_time, a.cpus))
print("| benchmark | before | after | diff |")
print("| --- | --- | --- | --- |")
for bench, label, _ in BENCHES:
    before = statistics.median(samples[(bench, "before")])
    after = statistics.median(samples[(bench, "after")])
    fmt = "%.1f ns" if before < 100 else "%.0f ns"
    print("| %s | %s | %s | %+.2f%% |" % (label, fmt % before, fmt % after, 100.0 * (after - before) / before))
