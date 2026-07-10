#!/usr/bin/env python3
#
# mako_ab_memtracker.py
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
A/B benchmark: FDB per-call-site memory tracker OFF vs ON (1:100 sampling).

Uses contrib/mako_storage_bench.sh (single-host 1/1/1 loopback cluster, storage
role isolated on ~one core, CPU-bound on /mnt/ram) to run each engine under both
arms back-to-back on the same host, then generates a self-contained chart.js
report (mako_memtracker_ab.html) viewable in any browser.

Both arms are the SAME build; only runtime knobs differ:
  OFF: --knob_memory_tracking_sample_inverse=0
  ON : --knob_memory_tracking_sample_inverse=100
Both also force --knob_memory_tracking_report_interval=30 so the tracker's
periodic dump fires within the run (equal cadence in both arms -> fair) and so
we can verify the knob took by reading MemoryTrackerSummary's SampleInverse out
of the storage process trace.

Run on the dev pod, e.g.:
  python3 /root/src/fdb5/foundationdb/contrib/mako_ab_memtracker.py --build /root/build_output5 \
      --engines redwood rocksdb --warmup 60 --seconds 240 \
      --outdir /mnt/ram/memtracker_ab
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys

BENCH_DEFAULT = "/root/src/fdb5/foundationdb/contrib/mako_storage_bench.sh"
BUILD_DEFAULT = "/root/build_output5"

# OFF = baseline blue, ON = warm orange.
COLOR = {"off": "#4477CC", "on": "#EE7733"}
ARMS = [("off", 0), ("on", 100)]  # (label, sample_inverse)
PCTS = [("medianLatency", "p50"), ("p95Latency", "p95"),
        ("p99Latency", "p99"), ("p99.9Latency", "p99.9")]


def run_arm(bench, build, engine, arm, inverse, warmup, seconds, rows, outbase):
    """Run one (engine, arm) via mako_storage_bench.sh; return its output dir."""
    workdir = os.path.join(outbase, arm)
    env = dict(os.environ)
    env["WORKDIR"] = workdir
    env["WARMUP_SECONDS"] = str(warmup)
    env["SECONDS_RUN"] = str(seconds)
    env["ROWS"] = str(rows)
    env["KNOBS"] = (f"--knob_memory_tracking_sample_inverse={inverse} "
                    f"--knob_memory_tracking_report_interval=30")
    print(f"\n=== {engine} / {arm} (sample_inverse={inverse}) ===", flush=True)
    print(f"    WORKDIR={workdir}  KNOBS={env['KNOBS']}", flush=True)
    subprocess.run(["bash", bench, build, engine], env=env, check=False)
    return os.path.join(workdir, engine)


def verify_sample_inverse(rundir):
    """Read MemoryTrackerSummary's SampleInverse from the storage trace.
    Returns the observed int, or None if no summary event was found."""
    lc = os.path.join(rundir, "loopback-cluster")
    observed = None
    if not os.path.isdir(lc):
        return None
    for root, _, files in os.walk(lc):
        for fn in files:
            if "trace" not in fn:
                continue
            try:
                with open(os.path.join(root, fn), errors="ignore") as fh:
                    for line in fh:
                        if "MemoryTrackerSummary" in line:
                            m = re.search(r'SampleInverse["\s:=]+(-?\d+)', line)
                            if m:
                                observed = int(m.group(1))
            except OSError:
                pass
    return observed


def alloc_rate_from_trace(rundir):
    """Estimate the storage process's allocation rate from the ON arm's
    MemoryTrackerSummary events (emitted every report interval, 30s here):
    rate = delta(EstCumulativeAllocs)/delta(time). Only meaningful when
    sampling is on. Returns {alloc_per_sec, mb_per_sec, samples_per_sec} or
    None."""
    lc = os.path.join(rundir, "loopback-cluster")
    if not os.path.isdir(lc):
        return None

    def field(line, key, cast=float):
        m = re.search(key + r'="?(-?[\d.]+)"?', line)
        return cast(m.group(1)) if m else None

    rows = []
    for root, _, files in os.walk(lc):
        for fn in files:
            if "trace" not in fn:
                continue
            try:
                for line in open(os.path.join(root, fn), errors="ignore"):
                    if "MemoryTrackerSummary" not in line:
                        continue
                    t = field(line, "Time")
                    ea = field(line, "EstCumulativeAllocs", int)
                    eb = field(line, "EstCumulativeBytes", int)
                    se = field(line, "SamplesEmitted", int)
                    mm = re.search(r'Machine="([^"]+)"', line)
                    rm = re.search(r'Roles="([^"]*)"', line)
                    if t is not None and ea is not None:
                        rows.append((mm.group(1) if mm else "?",
                                     rm.group(1) if rm else "", t, ea,
                                     eb or 0, se or 0))
            except OSError:
                pass
    if not rows:
        return None
    # Pick the storage process: the machine with the highest peak
    # EstCumulativeAllocs (storage allocates far more than stateless/log). Use
    # all of that machine's summaries (early role-less ones included) so even
    # short runs yield >=2 points to difference.
    by_mach = {}
    for r in rows:
        by_mach.setdefault(r[0], []).append(r)
    best = max(by_mach.values(), key=lambda rs: max(x[3] for x in rs))
    best.sort(key=lambda r: r[2])
    if len(best) < 2:
        return None
    ar, br, sr = [], [], []
    for a, b in zip(best, best[1:]):
        dt = b[2] - a[2]
        if dt <= 0:
            continue
        ar.append((b[3] - a[3]) / dt)
        br.append((b[4] - a[4]) / dt)
        sr.append((b[5] - a[5]) / dt)
    if len(ar) >= 3:  # drop the first interval (startup ramp)
        ar, br, sr = ar[1:], br[1:], sr[1:]
    if not ar:
        return None
    return {"alloc_per_sec": statistics.median(ar),
            "mb_per_sec": statistics.median(br) / 1e6,
            "samples_per_sec": statistics.median(sr)}


def parse_run(rundir):
    """Extract metrics from a completed run dir."""
    out = {"overallTPS": None, "persec": [], "latency": {}, "ok": False}
    mj = os.path.join(rundir, "mako.json")
    if os.path.exists(mj):
        try:
            d = json.load(open(mj))
        except (OSError, json.JSONDecodeError):
            return out
        res = d.get("results", {})
        out["overallTPS"] = res.get("overallTPS")
        out["persec"] = [s["tps"] for s in d.get("samples", []) if "tps" in s]
        for key, short in PCTS:
            blk = res.get(key, {})
            if isinstance(blk, dict) and "TRANSACTION" in blk:
                out["latency"][short] = blk["TRANSACTION"]  # microseconds
        out["ok"] = out["overallTPS"] is not None
    # Fallback: Overall TPS from the teed text report.
    if out["overallTPS"] is None:
        mt = os.path.join(rundir, "mako-run.txt")
        if os.path.exists(mt):
            for line in open(mt, errors="ignore"):
                m = re.search(r'Overall TPS:\s*([\d.]+)', line)
                if m:
                    out["overallTPS"] = float(m.group(1))
                    out["ok"] = True
    out["rate"] = alloc_rate_from_trace(rundir)
    return out


def collect(bench, build, engines, warmup, seconds, rows, outbase):
    data = {}  # engine -> arm -> {metrics, verified_inverse}
    for engine in engines:
        data[engine] = {}
        for arm, inverse in ARMS:
            rundir = run_arm(bench, build, engine, arm, inverse,
                             warmup, seconds, rows, outbase)
            metrics = parse_run(rundir)
            observed = verify_sample_inverse(rundir)
            metrics["verified_inverse"] = observed
            metrics["expected_inverse"] = inverse
            ok = "OK" if observed == inverse else f"MISMATCH (saw {observed})"
            print(f"    -> overallTPS={metrics['overallTPS']} "
                  f"knob-verify SampleInverse={observed} expected={inverse} [{ok}]",
                  flush=True)
            data[engine][arm] = metrics
    return data


# --------------------------------------------------------------------------
# HTML / chart.js generation
# --------------------------------------------------------------------------

def line_ds(label, series, color, dashed=False):
    d = {"label": label, "data": series, "borderColor": color,
         "backgroundColor": color, "pointRadius": 0, "borderWidth": 2,
         "tension": 0.2, "fill": False}
    if dashed:
        d["borderDash"] = [6, 3]
    return d


def generate_html(data, outpath, warmup, seconds, rows):
    engines = list(data.keys())

    # ---- Summary rows ----
    rows_html = []
    for eng in engines:
        off = data[eng].get("off", {})
        on = data[eng].get("on", {})
        t_off, t_on = off.get("overallTPS"), on.get("overallTPS")
        dtps = (100.0 * (t_on - t_off) / t_off) if (t_off and t_on) else None
        p99_off = off.get("latency", {}).get("p99")
        p99_on = on.get("latency", {}).get("p99")
        dp99 = (100.0 * (p99_on - p99_off) / p99_off) if (p99_off and p99_on) else None

        def fmt(v, s=""):
            return f"{v:,.0f}{s}" if isinstance(v, (int, float)) else "—"

        def dfmt(v):
            if v is None:
                return "—"
            sign = "+" if v >= 0 else ""
            return f"{sign}{v:.2f}%"

        rows_html.append(
            f"<tr><td>{eng}</td><td>{fmt(t_off)}</td><td>{fmt(t_on)}</td>"
            f"<td class='delta'>{dfmt(dtps)}</td>"
            f"<td>{fmt(p99_off,' µs')}</td><td>{fmt(p99_on,' µs')}</td>"
            f"<td class='delta'>{dfmt(dp99)}</td>"
            f"<td>off={off.get('verified_inverse')} / on={on.get('verified_inverse')}</td></tr>")

    # ---- Per-engine charts ----
    tps_labels = engines
    tps_off = [data[e].get("off", {}).get("overallTPS") or 0 for e in engines]
    tps_on = [data[e].get("on", {}).get("overallTPS") or 0 for e in engines]
    tps_datasets = json.dumps([
        {"label": "off (inverse=0)", "data": tps_off, "backgroundColor": COLOR["off"]},
        {"label": "on (inverse=100)", "data": tps_on, "backgroundColor": COLOR["on"]},
    ])

    blocks = []
    for i, eng in enumerate(engines):
        off = data[eng].get("off", {})
        on = data[eng].get("on", {})
        # per-second time series
        ps_datasets = json.dumps([
            line_ds(f"{eng} off", off.get("persec", []), COLOR["off"]),
            line_ds(f"{eng} on", on.get("persec", []), COLOR["on"], dashed=True),
        ])
        # latency percentiles
        lat_labels = json.dumps([s for _, s in PCTS])
        lat_off = [off.get("latency", {}).get(s) for _, s in PCTS]
        lat_on = [on.get("latency", {}).get(s) for _, s in PCTS]
        lat_datasets = json.dumps([
            {"label": "off", "data": lat_off, "backgroundColor": COLOR["off"]},
            {"label": "on", "data": lat_on, "backgroundColor": COLOR["on"]},
        ])
        blocks.append(f"""
  <h2>{eng}</h2>
  <div class="chart-box"><canvas id="ps{i}"></canvas></div>
  <div class="chart-box"><canvas id="lat{i}"></canvas></div>
  <script>
    new Chart(document.getElementById('ps{i}'), {{
      type: 'line',
      data: {{ labels: [...Array({max(len(off.get('persec',[])), len(on.get('persec',[])), 1)}).keys()],
               datasets: {ps_datasets} }},
      options: {{ animation:false, responsive:true,
        scales: {{ x: {{ title:{{display:true,text:'sample (~1 s each, incl. warmup)'}} }},
                   y: {{ beginAtZero:true, title:{{display:true,text:'TPS'}} }} }},
        plugins: {{ title:{{display:true,text:'{eng}: per-second throughput (off vs on)'}} }} }}
    }});
    new Chart(document.getElementById('lat{i}'), {{
      type: 'bar',
      data: {{ labels: {lat_labels}, datasets: {lat_datasets} }},
      options: {{ animation:false, responsive:true,
        scales: {{ y: {{ beginAtZero:true, title:{{display:true,text:'transaction latency (µs)'}} }} }},
        plugins: {{ title:{{display:true,text:'{eng}: transaction latency percentiles (off vs on)'}} }} }}
    }});
  </script>""")

    rate_bits = []
    for eng in engines:
        r = data[eng].get("on", {}).get("rate")
        if r:
            rate_bits.append(
                f"{eng} &approx; {r['alloc_per_sec'] / 1e6:.2f} M allocs/s "
                f"({r['mb_per_sec']:.0f} MB/s, {r['samples_per_sec'] / 1e3:.0f}K samples/s)")
    rate_note = ("<p class='sub'><b>Observed storage-process allocation rate (on arm)</b>, "
                 "from the tracker's own <code>MemoryTrackerSummary</code> "
                 "(&Delta;EstCumulativeAllocs / report interval): " + "; ".join(rate_bits) +
                 ". This is the rate the per-free global-lock cost scales with &mdash; far above "
                 "a single-threaded microbenchmark's assumed 100K/s, which (with cross-thread lock "
                 "contention) is why the end-to-end overhead here exceeds the &mu;bench estimate.</p>"
                 ) if rate_bits else ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>FDB Memory Tracker A/B: off vs 1:100 sampling</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <style>
    body {{ font-family: sans-serif; max-width: 960px; margin: 40px auto; padding: 0 20px; background:#fafafa; }}
    h1 {{ font-size:1.5em; margin-bottom:.1em; }} h2 {{ margin-top:2em; }}
    p.sub {{ color:#555; }}
    table {{ border-collapse:collapse; width:100%; margin:1em 0; background:#fff; }}
    th,td {{ border:1px solid #ddd; padding:6px 10px; text-align:right; }}
    th:first-child, td:first-child {{ text-align:left; }}
    td.delta {{ font-weight:bold; }}
    .chart-box {{ background:#fff; border:1px solid #ddd; border-radius:6px; padding:20px; margin-bottom:24px; }}
  </style>
</head>
<body>
  <h1>FDB Memory Tracker A/B &mdash; off vs 1:100 sampling</h1>
  <p class="sub">Same build; only the runtime knob differs
     (<code>memory_tracking_sample_inverse</code> 0 vs 100, report interval forced to 30&nbsp;s in both).
     Single-host 1/1/1 loopback cluster on /mnt/ram (storage isolated, CPU-bound) via
     <code>contrib/mako_storage_bench.sh</code>. Workload g18ui,
     rows={rows:,}, warmup {warmup}s, run {seconds}s. A TPS drop in the "on" arm means the
     tracker added CPU cost on the storage hot path; near-parity means no regression.</p>

  <h2>Summary</h2>
  <table>
    <tr><th>engine</th><th>TPS off</th><th>TPS on</th><th>&Delta; TPS</th>
        <th>p99 lat off</th><th>p99 lat on</th><th>&Delta; p99</th>
        <th>knob verify (SampleInverse)</th></tr>
    {''.join(rows_html)}
  </table>
  {rate_note}

  <div class="chart-box"><canvas id="tps"></canvas></div>
  <script>
    new Chart(document.getElementById('tps'), {{
      type: 'bar',
      data: {{ labels: {json.dumps(tps_labels)}, datasets: {tps_datasets} }},
      options: {{ animation:false, responsive:true,
        scales: {{ y: {{ beginAtZero:true, title:{{display:true,text:'Overall TPS'}} }} }},
        plugins: {{ title:{{display:true,text:'Overall throughput (off vs on)'}} }} }}
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
    ap = argparse.ArgumentParser(description="Memory-tracker off/on A/B via mako_storage_bench.sh")
    ap.add_argument("--build", default=BUILD_DEFAULT)
    ap.add_argument("--bench", default=BENCH_DEFAULT)
    ap.add_argument("--engines", nargs="+", default=["redwood", "rocksdb"])
    ap.add_argument("--warmup", type=int, default=60)
    ap.add_argument("--seconds", type=int, default=240)
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--outdir", default="/mnt/ram/memtracker_ab")
    ap.add_argument("--report", default=None,
                    help="HTML output path (default: /root/src/mako_memtracker_ab.html, "
                         "which syncs to ~/src on the Mac)")
    ap.add_argument("--report-only", action="store_true",
                    help="Skip running; regenerate HTML from an existing outdir")
    args = ap.parse_args()

    # Default the report into the okteto sync root (~/src <-> /root/src) so it
    # lands on the Mac for viewing without a separate copy step. The bulky run
    # data stays in --outdir (tmpfs /mnt/ram), which is not synced.
    report = args.report or "/root/src/mako_memtracker_ab.html"

    if args.report_only:
        data = {}
        for eng in args.engines:
            data[eng] = {}
            for arm, inverse in ARMS:
                rundir = os.path.join(args.outdir, arm, eng)
                m = parse_run(rundir)
                m["verified_inverse"] = verify_sample_inverse(rundir)
                m["expected_inverse"] = inverse
                data[eng][arm] = m
    else:
        os.makedirs(args.outdir, exist_ok=True)
        data = collect(args.bench, args.build, args.engines,
                       args.warmup, args.seconds, args.rows, args.outdir)

    generate_html(data, report, args.warmup, args.seconds, args.rows)
    print(f"\nReport written: {report}")


if __name__ == "__main__":
    main()
