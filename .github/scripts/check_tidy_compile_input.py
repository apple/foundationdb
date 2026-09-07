#!/usr/bin/env python3
"""Require a real compile command, and prove a mapped header is included by it."""

import json
from pathlib import Path
import shlex
import subprocess
import sys


def source_path(entry):
    return Path(entry["directory"], entry["file"]).resolve()


def dependencies(entry):
    args = entry.get("arguments") or shlex.split(entry["command"])
    preprocess = []
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
        elif arg in ("-o", "-MF", "-MT", "-MQ"):
            skip_next = True
        elif arg in ("-c", "-MD", "-MMD", "-MP"):
            continue
        else:
            preprocess.append(arg)
    preprocess.extend(("-M", "-MT", "tidy-input"))
    result = subprocess.run(
        preprocess, cwd=entry["directory"], capture_output=True, text=True, timeout=120
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "preprocessor failed")
    output = result.stdout.replace("\\\n", " ")
    if not output.startswith("tidy-input:"):
        raise RuntimeError("unexpected compiler dependency output")
    return {
        Path(entry["directory"], name).resolve()
        for name in shlex.split(output[len("tidy-input:") :])
    }


def main():
    if len(sys.argv) not in (3, 4):
        raise ValueError("usage: check_tidy_compile_input.py compile_commands.json consumer [header]")
    entries = json.loads(Path(sys.argv[1]).read_text())
    consumer = Path(sys.argv[2]).resolve()
    commands = [entry for entry in entries if source_path(entry) == consumer]
    if not commands:
        raise RuntimeError(f"no compile command for {consumer}")
    if len(sys.argv) == 4:
        header = Path(sys.argv[3]).resolve()
        errors = []
        for entry in commands:
            try:
                if header in dependencies(entry):
                    return
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                errors.append(str(error))
        raise RuntimeError(
            f"{header} is not an active dependency of {consumer}"
            + (f"; preprocessor errors: {'; '.join(errors)}" if errors else "")
        )


if __name__ == "__main__":
    try:
        main()
    except (OSError, KeyError, ValueError, RuntimeError) as error:
        sys.exit(f"clang-tidy input check failed: {error}")
