#!/usr/bin/env python3
"""Stream a FoundationDB XML trace into visualization-oriented JSON."""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any


ROLE_ABBREVIATIONS = {
    "Worker": "WK",
    "ClusterController": "CC",
    "Coordinator": "CD",
    "MasterServer": "MS",
    "GrvProxyServer": "GP",
    "CommitProxyServer": "CP",
    "Resolver": "RV",
    "TLog": "TL",
    "SharedTLog": "SL",
    "StorageServer": "SS",
    "DataDistributor": "DD",
    "Ratekeeper": "RK",
    "ConsistencyScan": "CS",
    "Tester": "TS",
}

SIGNAL_TYPES = {
    "ClusterRecoveryRetrying",
    "CommitProxyTerminated",
    "MasterTerminated",
    "RestartingTxnSubsystem",
    "FailMachine",
    "KillMachine",
    "KillMachineProcess",
    "RebootingProcess",
    "ProcessDestroyed",
    "WorkerKill",
}

FAULT_TYPES = {
    "FailMachine",
    "KillMachine",
    "KillMachineProcess",
    "RebootingProcess",
    "ProcessDestroyed",
    "WorkerKill",
    "StorageServerReboot",
    "Rollback",
    "RestartingTxnSubsystem",
}

WORKLOAD_TYPES = {
    "TestRunning",
    "TestStarting",
    "TestComplete",
    "TestResults",
    "WorkloadRunStatus",
    "WorkloadCheckStatus",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path, help="FDB XML trace file")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output JSON path; stdout when omitted",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation (default: 2)",
    )
    return parser.parse_args()


def split_mode_tokens(text: str) -> list[str]:
    """Split configure text on whitespace outside JSON/brackets and quotes."""
    tokens: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    escaped = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\" and quote:
            current.append(char)
            escaped = True
            continue
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in {'"', "'"}:
            current.append(char)
            quote = char
        elif char in "[{(":
            current.append(char)
            depth += 1
        elif char in "]})":
            current.append(char)
            depth = max(0, depth - 1)
        elif char.isspace() and depth == 0:
            if current:
                tokens.append("".join(current))
                current = []
        else:
            current.append(char)
    if current:
        tokens.append("".join(current))
    return tokens


def parse_config_mode(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    redundancy_aliases = {
        "single",
        "double",
        "triple",
        "three_data_hall",
        "three_datacenter",
        "one_satellite_single",
        "one_satellite_double",
        "remote_single",
        "remote_double",
    }
    for token in split_mode_tokens(text):
        if token == "new":
            values["_operation"] = "new"
            continue
        if token in redundancy_aliases:
            values["redundancy_mode"] = token
            continue
        delimiter = ":=" if ":=" in token else "=" if "=" in token else None
        if delimiter:
            key, value = token.split(delimiter, 1)
            values[key] = value
        else:
            values.setdefault("_flags", "")
            values["_flags"] = " ".join(filter(None, [values["_flags"], token]))
    return values


def parse_recovered_config(text: str) -> tuple[dict[str, Any], str | None]:
    try:
        value = json.loads(text)
        if isinstance(value, dict):
            return value, None
        return {"_value": value}, None
    except json.JSONDecodeError as exc:
        return {"_raw": text}, str(exc)


def config_diff(
    previous: dict[str, Any] | None, current: dict[str, Any]
) -> list[dict[str, Any]]:
    if previous is None:
        return [{"key": key, "old": None, "new": value} for key, value in current.items()]
    changes = []
    for key in sorted(set(previous) | set(current)):
        old = previous.get(key)
        new = current.get(key)
        if old != new:
            changes.append({"key": key, "old": old, "new": new})
    return changes


def event_record(seq: int, attrs: dict[str, str]) -> dict[str, Any]:
    common = {
        "seq": seq,
        "time": float(attrs.get("Time", 0)),
        "type": attrs.get("Type", ""),
        "machine": attrs.get("Machine", ""),
        "id": attrs.get("ID", ""),
        "severity": int(attrs.get("Severity", 10)),
    }
    excluded = {
        "Time",
        "Type",
        "Machine",
        "ID",
        "Severity",
        "DateTime",
        "ThreadID",
        "LogGroup",
        "Roles",
    }
    common["attrs"] = {key: value for key, value in attrs.items() if key not in excluded}
    if "Roles" in attrs:
        common["roles"] = [role for role in attrs["Roles"].split(",") if role]
    return common


def classify_recovery_reason(
    start_time: float,
    prior_recovery_count: int,
    recent_signals: deque[dict[str, Any]],
) -> dict[str, Any]:
    candidates = [
        signal
        for signal in recent_signals
        if 0 <= start_time - signal["time"] <= 30
    ]
    retries = [
        signal for signal in candidates if signal["type"] == "ClusterRecoveryRetrying"
    ]
    evidence = retries[-1] if retries else candidates[-1] if candidates else None
    if prior_recovery_count == 0:
        label = "Initial cluster bootstrap"
        confidence = "explicit-bootstrap-context"
    elif evidence and evidence["attrs"].get("Error") == "commit_proxy_failed":
        label = "Commit proxy failure"
        confidence = "explicit"
    elif evidence and evidence["attrs"].get("Error") == "no_more_servers":
        label = "Not enough physical servers available"
        confidence = "explicit"
    elif evidence:
        label = evidence["attrs"].get(
            "ErrorDescription", evidence["attrs"].get("Error", evidence["type"])
        )
        confidence = "nearest-signal"
    else:
        label = "No explicit trigger found in the preceding 30 seconds"
        confidence = "unknown"
    return {
        "label": label,
        "confidence": confidence,
        "evidence": evidence,
        "correlation_window_seconds": 30,
    }


def analyze(trace_path: Path) -> dict[str, Any]:
    severity_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    machines: set[str] = set()
    thread_ids: set[str] = set()
    event_count = 0
    start_time: float | None = None
    end_time = 0.0
    run: dict[str, Any] = {}
    warnings: list[str] = []

    config_snapshots: list[dict[str, Any]] = []
    previous_config_by_kind: dict[str, dict[str, Any]] = {}
    config_monitors: list[dict[str, Any]] = []
    role_open: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
    role_spans: list[dict[str, Any]] = []
    recoveries: list[dict[str, Any]] = []
    recovery_by_pair: dict[str, dict[str, Any]] = {}
    latest_recovery_by_actor: dict[str, dict[str, Any]] = {}
    recent_signals: deque[dict[str, Any]] = deque()
    workload_events: list[dict[str, Any]] = []
    faults: list[dict[str, Any]] = []
    dbinfo_generations: dict[int, dict[str, Any]] = {}

    try:
        iterator = ET.iterparse(trace_path, events=("end",))
        for _, element in iterator:
            if element.tag != "Event":
                element.clear()
                continue
            attrs = dict(element.attrib)
            seq = event_count
            event_count += 1
            event_type = attrs.get("Type", "")
            event_time = float(attrs.get("Time", 0))
            start_time = event_time if start_time is None else min(start_time, event_time)
            end_time = max(end_time, event_time)
            severity_counts[attrs.get("Severity", "10")] += 1
            type_counts[event_type] += 1
            machines.add(attrs.get("Machine", ""))
            if attrs.get("ThreadID"):
                thread_ids.add(attrs["ThreadID"])

            if event_type == "ProgramStart" and not run:
                run = {
                    key: attrs.get(key)
                    for key in (
                        "CommandLine",
                        "RandomSeed",
                        "SourceVersion",
                        "Version",
                        "PackageName",
                        "ProtocolVersion",
                        "BuggifyEnabled",
                        "FaultInjectionEnabled",
                        "WorkingDirectory",
                    )
                    if attrs.get(key) is not None
                }
            elif event_type == "Simulation":
                run["TestFile"] = attrs.get("TestFile")
            elif event_type == "ElapsedTime":
                run["SimTime"] = attrs.get("SimTime")
                run["RealTime"] = attrs.get("RealTime")
                run["RandomUnseed"] = attrs.get("RandomUnseed")

            if event_type in {
                "SimulatorConfig",
                "ChangeConfig",
                "MasterRecoveredConfig",
            } or (event_type == "MasterRecoveryState" and "Conf" in attrs):
                if event_type == "SimulatorConfig":
                    raw = attrs.get("ConfigString", "")
                    values = parse_config_mode(raw)
                    kind = "desired"
                elif event_type == "ChangeConfig":
                    raw = attrs.get("Mode", "")
                    values = parse_config_mode(raw)
                    kind = "requested"
                else:
                    raw = attrs.get("Conf", "")
                    values, parse_error = parse_recovered_config(raw)
                    kind = (
                        "recovery-use"
                        if event_type == "MasterRecoveryState"
                        else "recovered"
                    )
                    if parse_error:
                        warnings.append(
                            f"{event_type} at {event_time:.6f}: invalid Conf JSON: {parse_error}"
                        )
                previous = previous_config_by_kind.get(kind)
                snapshot = {
                    "seq": seq,
                    "time": event_time,
                    "source": event_type,
                    "kind": kind,
                    "machine": attrs.get("Machine"),
                    "values": values,
                    "changes_from_prior_same_kind": config_diff(previous, values),
                    "raw": raw,
                }
                config_snapshots.append(snapshot)
                previous_config_by_kind[kind] = values

            if event_type == "ConfigurationMonitor":
                config_monitors.append(event_record(seq, attrs))

            if event_type == "Role":
                transition = attrs.get("Transition")
                role = attrs.get("As", "")
                role_id = attrs.get("ID", "")
                key = (role_id, role)
                if transition == "Begin":
                    role_open[key].append(
                        {
                            "role": role,
                            "abbreviation": ROLE_ABBREVIATIONS.get(role, role),
                            "id": role_id,
                            "machine": attrs.get("Machine", ""),
                            "start": event_time,
                            "start_seq": seq,
                            "origination": attrs.get("Origination"),
                            "on_worker": attrs.get("OnWorker"),
                            "locality": attrs.get("Locality"),
                        }
                    )
                elif transition == "End":
                    if role_open[key]:
                        span = role_open[key].popleft()
                        span.update(
                            {
                                "end": event_time,
                                "end_seq": seq,
                                "right_censored": False,
                                "end_reason": attrs.get("Reason"),
                                "end_error": attrs.get("Error"),
                            }
                        )
                        role_spans.append(span)
                    else:
                        warnings.append(
                            f"Unmatched Role End for ({role_id}, {role}) at seq {seq}"
                        )

            if event_type in SIGNAL_TYPES:
                signal = event_record(seq, attrs)
                recent_signals.append(signal)
                while recent_signals and event_time - recent_signals[0]["time"] > 60:
                    recent_signals.popleft()

            if event_type == "ClusterRecovery" and attrs.get("BeginPair"):
                reason = classify_recovery_reason(
                    event_time, len(recoveries), recent_signals
                )
                episode = {
                    "index": len(recoveries) + 1,
                    "actor_id": attrs.get("ID", ""),
                    "machine": attrs.get("Machine", ""),
                    "pair_id": attrs["BeginPair"],
                    "start": event_time,
                    "start_seq": seq,
                    "reason": reason,
                    "states": [],
                }
                recoveries.append(episode)
                recovery_by_pair[attrs["BeginPair"]] = episode
                latest_recovery_by_actor[attrs.get("ID", "")] = episode
            elif event_type == "ClusterRecovery" and attrs.get("EndPair"):
                episode = recovery_by_pair.get(attrs["EndPair"])
                if episode:
                    episode["transaction_recovery_end"] = event_time
                    episode["end_seq"] = seq
                    episode["recovery_transaction_version"] = attrs.get(
                        "RecoveryTransactionVersion"
                    )
                else:
                    warnings.append(
                        f"Unmatched ClusterRecovery EndPair {attrs['EndPair']} at seq {seq}"
                    )

            if event_type == "MasterRecoveryState":
                episode = latest_recovery_by_actor.get(attrs.get("ID", ""))
                if episode and event_time >= episode["start"]:
                    state = {
                        "seq": seq,
                        "time": event_time,
                        "status": attrs.get("Status"),
                        "status_code": attrs.get("StatusCode"),
                    }
                    for key in (
                        "MyRecoveryCount",
                        "ActiveGenerations",
                        "TLogs",
                        "RequiredCommitProxies",
                        "RequiredGrvProxies",
                        "RequiredResolvers",
                        "RecoveryDuration",
                    ):
                        if key in attrs:
                            state[key] = attrs[key]
                    episode["states"].append(state)
                    if attrs.get("MyRecoveryCount"):
                        episode["recovery_count"] = attrs["MyRecoveryCount"]
                    if attrs.get("Status") == "fully_recovered":
                        episode["fully_recovered_at"] = event_time

            if event_type in WORKLOAD_TYPES:
                workload_events.append(event_record(seq, attrs))
            if event_type in FAULT_TYPES:
                faults.append(event_record(seq, attrs))

            if event_type == "GotServerDBInfoChange" and attrs.get("InfoGeneration"):
                generation = int(attrs["InfoGeneration"])
                record = dbinfo_generations.setdefault(
                    generation,
                    {
                        "info_generation": generation,
                        "first_time": event_time,
                        "last_time": event_time,
                        "observation_count": 0,
                        "machines": set(),
                        "change_ids": set(),
                        "master_ids": set(),
                    },
                )
                record["first_time"] = min(record["first_time"], event_time)
                record["last_time"] = max(record["last_time"], event_time)
                record["observation_count"] += 1
                record["machines"].add(attrs.get("Machine", ""))
                if attrs.get("ChangeID"):
                    record["change_ids"].add(attrs["ChangeID"])
                if attrs.get("MasterID"):
                    record["master_ids"].add(attrs["MasterID"])

            element.clear()
    except (ET.ParseError, OSError, ValueError) as exc:
        raise RuntimeError(f"Failed to parse {trace_path}: {exc}") from exc

    for entries in role_open.values():
        while entries:
            span = entries.popleft()
            span.update(
                {
                    "end": end_time,
                    "end_seq": None,
                    "right_censored": True,
                    "end_reason": "trace_end",
                    "end_error": None,
                }
            )
            role_spans.append(span)

    role_spans.sort(key=lambda span: (span["machine"], span["start"], span["role"]))
    for episode in recoveries:
        active = [
            span
            for span in role_spans
            if span["start"] <= episode["start"] < span["end"]
        ]
        by_role: dict[str, dict[str, Any]] = {}
        for span in active:
            role = span["role"]
            role_record = by_role.setdefault(
                role,
                {
                    "role": role,
                    "abbreviation": ROLE_ABBREVIATIONS.get(role, role),
                    "instance_count": 0,
                    "placements": defaultdict(int),
                    "instances": [],
                },
            )
            role_record["instance_count"] += 1
            role_record["placements"][span["machine"]] += 1
            role_record["instances"].append(
                {"id": span["id"], "machine": span["machine"]}
            )
        normalized_roles = []
        for role_record in by_role.values():
            role_record["placements"] = dict(role_record["placements"])
            normalized_roles.append(role_record)
        normalized_roles.sort(
            key=lambda item: list(ROLE_ABBREVIATIONS).index(item["role"])
            if item["role"] in ROLE_ABBREVIATIONS
            else 999
        )
        episode["active_roles_at_start"] = normalized_roles

    generations = []
    for generation in sorted(dbinfo_generations):
        record = dbinfo_generations[generation]
        for key in ("machines", "change_ids", "master_ids"):
            record[key] = sorted(record[key])
        generations.append(record)

    return {
        "schema_version": 1,
        "source": {"path": str(trace_path.resolve()), "format": "fdb-xml-trace"},
        "run": run,
        "summary": {
            "event_count": event_count,
            "distinct_event_types": len(type_counts),
            "event_type_counts": dict(type_counts.most_common()),
            "severity_counts": dict(sorted(severity_counts.items())),
            "start_time": start_time or 0,
            "end_time": end_time,
            "simulated_duration_seconds": end_time - (start_time or 0),
            "machine_addresses": sorted(machines),
            "cluster_machine_addresses": sorted(
                machine for machine in machines if machine != "0.0.0.0:0"
            ),
            "thread_ids": sorted(thread_ids),
            "severity_40_count": severity_counts.get("40", 0),
        },
        "configuration": {
            "snapshots": sorted(
                config_snapshots, key=lambda item: (item["time"], item["seq"])
            ),
            "monitor_events": config_monitors,
            "server_db_info_generations": generations,
        },
        "roles": {
            "abbreviations": ROLE_ABBREVIATIONS,
            "spans": role_spans,
            "span_count": len(role_spans),
            "right_censored_count": sum(
                1 for span in role_spans if span["right_censored"]
            ),
        },
        "recoveries": recoveries,
        "workload_events": workload_events,
        "fault_events": faults,
        "warnings": warnings,
    }


def main() -> int:
    args = parse_args()
    if not args.trace.is_file():
        print(f"error: trace file does not exist: {args.trace}", file=sys.stderr)
        return 2
    try:
        result = analyze(args.trace)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    rendered = json.dumps(result, indent=args.indent, sort_keys=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(
            f"Wrote {args.output}: {result['summary']['event_count']} events, "
            f"{result['roles']['span_count']} role spans, "
            f"{len(result['recoveries'])} recoveries"
        )
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
