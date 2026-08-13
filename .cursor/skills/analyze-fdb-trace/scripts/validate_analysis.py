#!/usr/bin/env python3
"""Validate normalized output from analyze_trace.py."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def fail(message: str, errors: list[str]) -> None:
    errors.append(message)


def require(
    condition: bool, message: str, errors: list[str]
) -> None:
    if not condition:
        fail(message, errors)


def validate(data: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    require(data.get("schema_version") == 1, "schema_version must be 1", errors)

    summary = data.get("summary", {})
    event_count = summary.get("event_count")
    require(isinstance(event_count, int) and event_count > 0, "event_count must be positive", errors)
    require(
        summary.get("start_time", 0) <= summary.get("end_time", -1),
        "start_time must not exceed end_time",
        errors,
    )
    severity_counts = summary.get("severity_counts", {})
    if isinstance(event_count, int):
        require(
            sum(int(value) for value in severity_counts.values()) == event_count,
            "severity counts must sum to event_count",
            errors,
        )
    type_counts = summary.get("event_type_counts", {})
    if isinstance(event_count, int):
        require(
            sum(int(value) for value in type_counts.values()) == event_count,
            "event type counts must sum to event_count",
            errors,
        )

    roles = data.get("roles", {})
    spans = roles.get("spans", [])
    require(roles.get("span_count") == len(spans), "role span_count mismatch", errors)
    for index, span in enumerate(spans):
        prefix = f"role span {index}"
        require(bool(span.get("role")), f"{prefix}: missing role", errors)
        require(bool(span.get("machine")), f"{prefix}: missing machine", errors)
        require(span.get("start") <= span.get("end"), f"{prefix}: start exceeds end", errors)
        if span.get("right_censored"):
            require(
                span.get("end") == summary.get("end_time"),
                f"{prefix}: right-censored span must end at trace end",
                errors,
            )

    recoveries = data.get("recoveries", [])
    pair_ids: set[str] = set()
    for index, recovery in enumerate(recoveries):
        prefix = f"recovery {index + 1}"
        pair_id = recovery.get("pair_id")
        require(bool(pair_id), f"{prefix}: missing pair_id", errors)
        if pair_id:
            require(pair_id not in pair_ids, f"{prefix}: duplicate pair_id {pair_id}", errors)
            pair_ids.add(pair_id)
        require(
            recovery.get("start", -1) >= summary.get("start_time", 0),
            f"{prefix}: starts before trace",
            errors,
        )
        if "transaction_recovery_end" in recovery:
            require(
                recovery["transaction_recovery_end"] >= recovery["start"],
                f"{prefix}: end precedes start",
                errors,
            )
        for role in recovery.get("active_roles_at_start", []):
            placements = role.get("placements", {})
            require(
                role.get("instance_count") == sum(placements.values()),
                f"{prefix}: role placement count mismatch for {role.get('role')}",
                errors,
            )

    snapshots = data.get("configuration", {}).get("snapshots", [])
    for index, snapshot in enumerate(snapshots):
        prefix = f"configuration snapshot {index}"
        require(bool(snapshot.get("source")), f"{prefix}: missing source", errors)
        require(isinstance(snapshot.get("values"), dict), f"{prefix}: values must be an object", errors)
        require(
            summary.get("start_time", 0)
            <= snapshot.get("time", -1)
            <= summary.get("end_time", -1),
            f"{prefix}: time outside trace",
            errors,
        )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("analysis", type=Path, help="JSON produced by analyze_trace.py")
    args = parser.parse_args()
    try:
        data = json.loads(args.analysis.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"INVALID: unable to read {args.analysis}: {exc}", file=sys.stderr)
        return 2
    if not isinstance(data, dict):
        print("INVALID: top-level JSON must be an object", file=sys.stderr)
        return 2
    errors = validate(data)
    if errors:
        print("INVALID")
        for error in errors:
            print(f"- {error}")
        return 1
    print(
        "OK: "
        f"{data['summary']['event_count']} events, "
        f"{data['roles']['span_count']} role spans, "
        f"{len(data['recoveries'])} recoveries, "
        f"{len(data['configuration']['snapshots'])} configuration snapshots"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
