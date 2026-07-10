#!/usr/bin/env python3
#
# mako_ab_binaries.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
A/B benchmark across two *different fdbserver binaries*:

  A = vanilla main (a build with no memory-tracker code at all)
  B = this PR's build, with memory tracking turned OFF (sample_inverse=0)

The point is to isolate the cost of the always-compiled memory-tracker hooks
when they are disabled — i.e. to check the design's claim that the off-state
overhead is <=1%. Unlike mako_ab_memtracker.py (one binary, two knob settings),
each arm here runs a different --build.

Only arm B is passed the memory-tracking knob; arm A (main) does not have it and
would reject an unknown knob. Both arms get the RocksDB direct-I/O-off knobs for
the rocksdb engine (main has those long-standing knobs too) since the data dir
is on tmpfs.

Reuses contrib/mako_storage_bench.sh and the chart.js report from
mako_ab_memtracker.py. Run on the dev pod, e.g.:

  python3 contrib/mako_ab_binaries.py \
      --build-a /root/build_output4 --build-b /root/build_output5 \
      --engines redwood rocksdb --warmup 60 --seconds 240 \
      --outdir /mnt/ram/binab
"""

import argparse
import json
import os
import re
import shutil
import subprocess

from mako_ab_memtracker import parse_run, line_ds, PCTS

BENCH_DEFAULT = "/root/src/fdb5/foundationdb/contrib/mako_storage_bench.sh"

# A = baseline blue, B = warm orange.
COLOR = {"a": "#4477CC", "b": "#EE7733"}


def rocksdb_tmpfs_knobs(engine):
    """RocksDB opens its DB with O_DIRECT, which tmpfs does not support; both
    binaries need direct I/O off to run rocksdb on /mnt/ram. redwood needs
    nothing."""
    if engine == "rocksdb":
        return ["--knob_rocksdb_use_direct_reads=0",
                "--knob_rocksdb_use_direct_io_flush_compaction=0"]
    return []


def run_arm(bench, build, engine, armkey, extra_knobs, warmup, seconds, rows, outbase):
    """Run one (engine, arm) via mako_storage_bench.sh with the arm's build."""
    workdir = os.path.join(outbase, armkey)
    env = dict(os.environ)
    env["WORKDIR"] = workdir
    env["WARMUP_SECONDS"] = str(warmup)
    env["SECONDS_RUN"] = str(seconds)
    env["ROWS"] = str(rows)
    env["KNOBS"] = " ".join(extra_knobs + rocksdb_tmpfs_knobs(engine))
    print(f"\n=== {engine} / {armkey}  build={build} ===", flush=True)
    print(f"    WORKDIR={workdir}  KNOBS={env['KNOBS']}", flush=True)
    subprocess.run(["bash", bench, build, engine], env=env, check=False)
    return os.path.join(workdir, engine)


def source_version(build):
    """The git source version baked into the binary (fdbserver --version). Used
    to prove A and B are genuinely different builds, and to record provenance."""
    fdbserver = os.path.join(build, "bin", "fdbserver")
    try:
        out = subprocess.run([fdbserver, "--version"], capture_output=True,
                             text=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    m = re.search(r"source version (\w+)", out)
    return m.group(1) if m else None


# --------------------------------------------------------------------------

def clobber_ramdisk(ram_mount):
    """Clear all scratch under the tmpfs so every run starts with an empty
    ramdisk. /mnt/ram is only ~24 GB and fills fast across runs. We do this at
    startup rather than at teardown for two reasons: a killed run can't be
    trusted to have cleaned up after itself, and leaving the last run's data in
    place until the next run means it's still there to inspect when something
    fails."""
    if not os.path.isdir(ram_mount):
        return
    for name in os.listdir(ram_mount):
        subprocess.run(["rm", "-rf", os.path.join(ram_mount, name)], check=False)
    print(f"clobbered ramdisk contents under {ram_mount}", flush=True)


def _harvest(rundir, dst):
    """Copy the small result files off tmpfs to persistent storage before the
    bulky SS/cluster data is wiped."""
    os.makedirs(dst, exist_ok=True)
    for name in ("mako.json", "mako-run.txt"):
        src = os.path.join(rundir, name)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(dst, name))


def collect(bench, build_a, build_b, engines, warmup, seconds, rows, ramdir, outdir):
    b_knobs = ["--knob_memory_tracking_sample_inverse=0"]
    data = {}
    for engine in engines:
        data[engine] = {}
        for armkey, build, knobs in (("a", build_a, []), ("b", build_b, b_knobs)):
            rundir = run_arm(bench, build, engine, armkey, knobs,
                             warmup, seconds, rows, ramdir)
            dst = os.path.join(outdir, armkey, engine)
            _harvest(rundir, dst)                 # save results to /root for persistence + debugging
            metrics = parse_run(dst)
            print(f"    -> overallTPS={metrics['overallTPS']}", flush=True)
            data[engine][armkey] = metrics
    return data


def generate_html(data, outpath, meta, warmup, seconds, rows):
    engines = list(data.keys())
    la, lb = meta["label_a"], meta["label_b"]

    rows_html = []
    for eng in engines:
        a = data[eng].get("a", {})
        b = data[eng].get("b", {})
        ta, tb = a.get("overallTPS"), b.get("overallTPS")
        # B relative to A: negative = B slower (overhead).
        d = (100.0 * (tb - ta) / ta) if (ta and tb) else None
        p99a = a.get("latency", {}).get("p99")
        p99b = b.get("latency", {}).get("p99")
        dp = (100.0 * (p99b - p99a) / p99a) if (p99a and p99b) else None

        def fmt(v, s=""):
            return f"{v:,.0f}{s}" if isinstance(v, (int, float)) else "—"

        def dfmt(v):
            if v is None:
                return "—"
            return f"{'+' if v >= 0 else ''}{v:.2f}%"

        rows_html.append(
            f"<tr><td>{eng}</td><td>{fmt(ta)}</td><td>{fmt(tb)}</td>"
            f"<td class='delta'>{dfmt(d)}</td>"
            f"<td>{fmt(p99a,' µs')}</td><td>{fmt(p99b,' µs')}</td>"
            f"<td class='delta'>{dfmt(dp)}</td></tr>")

    tps_a = [data[e].get("a", {}).get("overallTPS") or 0 for e in engines]
    tps_b = [data[e].get("b", {}).get("overallTPS") or 0 for e in engines]
    tps_datasets = json.dumps([
        {"label": la, "data": tps_a, "backgroundColor": COLOR["a"]},
        {"label": lb, "data": tps_b, "backgroundColor": COLOR["b"]},
    ])

    blocks = []
    for i, eng in enumerate(engines):
        a = data[eng].get("a", {})
        b = data[eng].get("b", {})
        ps_datasets = json.dumps([
            line_ds(f"{eng} {la}", a.get("persec", []), COLOR["a"]),
            line_ds(f"{eng} {lb}", b.get("persec", []), COLOR["b"], dashed=True),
        ])
        lat_labels = json.dumps([s for _, s in PCTS])
        lat_a = [a.get("latency", {}).get(s) for _, s in PCTS]
        lat_b = [b.get("latency", {}).get(s) for _, s in PCTS]
        lat_datasets = json.dumps([
            {"label": la, "data": lat_a, "backgroundColor": COLOR["a"]},
            {"label": lb, "data": lat_b, "backgroundColor": COLOR["b"]},
        ])
        n = max(len(a.get('persec', [])), len(b.get('persec', [])), 1)
        blocks.append(f"""
  <h2>{eng}</h2>
  <div class="chart-box"><canvas id="ps{i}"></canvas></div>
  <div class="chart-box"><canvas id="lat{i}"></canvas></div>
  <script>
    new Chart(document.getElementById('ps{i}'), {{
      type: 'line',
      data: {{ labels: [...Array({n}).keys()], datasets: {ps_datasets} }},
      options: {{ animation:false, responsive:true,
        scales: {{ x: {{ title:{{display:true,text:'sample (~1 s each, incl. warmup)'}} }},
                   y: {{ beginAtZero:true, title:{{display:true,text:'TPS'}} }} }},
        plugins: {{ title:{{display:true,text:'{eng}: per-second throughput'}} }} }}
    }});
    new Chart(document.getElementById('lat{i}'), {{
      type: 'bar',
      data: {{ labels: {lat_labels}, datasets: {lat_datasets} }},
      options: {{ animation:false, responsive:true,
        scales: {{ y: {{ beginAtZero:true, title:{{display:true,text:'transaction latency (µs)'}} }} }},
        plugins: {{ title:{{display:true,text:'{eng}: transaction latency percentiles'}} }} }}
    }});
  </script>""")

    same = (meta["ver_a"] and meta["ver_a"] == meta["ver_b"])
    guard = ""
    if same:
        guard = ("<p class='warn'><b>WARNING:</b> both builds report the same source "
                 "version — A and B may be the same binary; results are not meaningful.</p>")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>FDB Memory Tracker A/B: vanilla main vs PR (tracking off)</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <style>
    body {{ font-family: sans-serif; max-width: 960px; margin: 40px auto; padding: 0 20px; background:#fafafa; }}
    h1 {{ font-size:1.5em; margin-bottom:.1em; }} h2 {{ margin-top:2em; }}
    p.sub {{ color:#555; }} p.warn {{ color:#b00; }}
    table {{ border-collapse:collapse; width:100%; margin:1em 0; background:#fff; }}
    th,td {{ border:1px solid #ddd; padding:6px 10px; text-align:right; }}
    th:first-child, td:first-child {{ text-align:left; }}
    td.delta {{ font-weight:bold; }}
    .chart-box {{ background:#fff; border:1px solid #ddd; border-radius:6px; padding:20px; margin-bottom:24px; }}
  </style>
</head>
<body>
  <h1>FDB Memory Tracker A/B &mdash; vanilla main vs PR (tracking OFF)</h1>
  <p class="sub">Two different fdbserver binaries on the same host / workload, isolating the
     cost of the always-compiled memory-tracker code when it is <b>disabled</b>.
     <b>A</b> = {la} (build <code>{meta['build_a']}</code>, source <code>{meta['ver_a']}</code>);
     <b>B</b> = {lb} (build <code>{meta['build_b']}</code>, source <code>{meta['ver_b']}</code>,
     <code>memory_tracking_sample_inverse=0</code>).
     Single-host 1/1/1 loopback cluster on /mnt/ram (storage isolated, CPU-bound) via
     <code>contrib/mako_storage_bench.sh</code>. rows={rows:,}, warmup {warmup}s, run {seconds}s.
     &Delta; is B relative to A: a small negative &Delta; is the off-state overhead; the design
     target is &le;1%.</p>
  {guard}
  <p class="sub"><b>Caveat:</b> {meta['drift_note']}</p>

  <h2>Summary</h2>
  <table>
    <tr><th>engine</th><th>TPS A ({la})</th><th>TPS B ({lb})</th><th>&Delta; TPS (B vs A)</th>
        <th>p99 lat A</th><th>p99 lat B</th><th>&Delta; p99</th></tr>
    {''.join(rows_html)}
  </table>

  <div class="chart-box"><canvas id="tps"></canvas></div>
  <script>
    new Chart(document.getElementById('tps'), {{
      type: 'bar',
      data: {{ labels: {json.dumps(engines)}, datasets: {tps_datasets} }},
      options: {{ animation:false, responsive:true,
        scales: {{ y: {{ beginAtZero:true, title:{{display:true,text:'Overall TPS'}} }} }},
        plugins: {{ title:{{display:true,text:'Overall throughput (A vs B)'}} }} }}
    }});
  </script>
  {''.join(blocks)}
</body>
</html>
"""
    with open(outpath, "w") as f:
        f.write(html)
    return outpath


def main():
    ap = argparse.ArgumentParser(description="Vanilla-main vs PR-tracking-off A/B via mako_storage_bench.sh")
    ap.add_argument("--build-a", default="/root/build_output4", help="vanilla main build (arm A)")
    ap.add_argument("--build-b", default="/root/build_output5", help="this-PR build (arm B)")
    ap.add_argument("--label-a", default="main (no tracker)")
    ap.add_argument("--label-b", default="PR, tracking off")
    ap.add_argument("--bench", default=BENCH_DEFAULT)
    ap.add_argument("--engines", nargs="+", default=["redwood", "rocksdb"])
    ap.add_argument("--warmup", type=int, default=60)
    ap.add_argument("--seconds", type=int, default=240)
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--ramdir", default="/mnt/ram/binab",
                    help="tmpfs scratch for SS/cluster data (only SS data lives on /mnt/ram)")
    ap.add_argument("--ram-mount", default="/mnt/ram",
                    help="tmpfs mount clobbered clean at startup")
    ap.add_argument("--outdir", default="/root/binab_results",
                    help="persistent results dir on /root (~1 TB); harvested off tmpfs per arm")
    ap.add_argument("--report", default="/root/src/mako_binab.html",
                    help="HTML output path (syncs to ~/src on the Mac)")
    ap.add_argument("--drift-note", default="A and B may build from slightly different main "
                    "revisions; the delta bundles the PR's off-state cost with any main drift.")
    ap.add_argument("--report-only", action="store_true")
    args = ap.parse_args()

    meta = {
        "label_a": args.label_a, "label_b": args.label_b,
        "build_a": args.build_a, "build_b": args.build_b,
        "ver_a": source_version(args.build_a), "ver_b": source_version(args.build_b),
        "drift_note": args.drift_note,
    }
    print(f"A: {args.build_a} source={meta['ver_a']}")
    print(f"B: {args.build_b} source={meta['ver_b']}")

    if args.report_only:
        data = {}
        for eng in args.engines:
            data[eng] = {"a": parse_run(os.path.join(args.outdir, "a", eng)),
                         "b": parse_run(os.path.join(args.outdir, "b", eng))}
    else:
        clobber_ramdisk(args.ram_mount)          # clean slate up front; no end-of-run cleanup
        os.makedirs(args.ramdir, exist_ok=True)
        os.makedirs(args.outdir, exist_ok=True)
        data = collect(args.bench, args.build_a, args.build_b, args.engines,
                       args.warmup, args.seconds, args.rows, args.ramdir, args.outdir)

    generate_html(data, args.report, meta, args.warmup, args.seconds, args.rows)
    print(f"\nReport written: {args.report}")


if __name__ == "__main__":
    main()
