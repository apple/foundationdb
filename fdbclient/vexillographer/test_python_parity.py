#!/usr/bin/env python3
#
# test_python_parity.py
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

"""Check that vexillographer.py generates the same bindings as the C# vexillographer.

Usage: test_python_parity.py OPTIONS_FILE LANG[,LANG...] -- REFERENCE_COMMAND...

REFERENCE_COMMAND runs the C# vexillographer (e.g. `mono vexillographer.exe`).
CRLF is normalized to LF before comparing, since C# verbatim strings take on the line
endings of the checked-out .cs sources (e.g. CRLF with git autocrlf).
"""

import difflib
import pathlib
import subprocess
import sys
import tempfile

PYTHON_GENERATOR = pathlib.Path(__file__).resolve().parent / "vexillographer.py"


def output_target(out_dir: pathlib.Path, lang: str) -> pathlib.Path:
    out_dir.mkdir()
    if lang == "java":
        return out_dir
    # cpp treats the target as a base name and appends .h and .cpp.
    return out_dir / ("fdboptions.g" if lang == "cpp" else f"fdboptions.{lang}")


def generate(command, options_file: str, lang: str, out_dir: pathlib.Path) -> dict:
    target = output_target(out_dir, lang)
    subprocess.run([*command, options_file, lang, str(target)], check=True)
    return {
        p.relative_to(out_dir).as_posix(): p.read_bytes().decode().replace("\r\n", "\n")
        for p in sorted(out_dir.rglob("*"))
        if p.is_file()
    }


def compare(lang: str, expected: dict, actual: dict) -> bool:
    ok = True
    if not expected:
        print(f"[{lang}] reference generator produced no files")
        ok = False
    for name in sorted(expected.keys() - actual.keys()):
        print(f"[{lang}] {name}: generated only by the C# vexillographer")
        ok = False
    for name in sorted(actual.keys() - expected.keys()):
        print(f"[{lang}] {name}: generated only by vexillographer.py")
        ok = False
    for name in sorted(expected.keys() & actual.keys()):
        if expected[name] == actual[name]:
            continue
        ok = False
        diff = list(
            difflib.unified_diff(
                expected[name].splitlines(keepends=True),
                actual[name].splitlines(keepends=True),
                f"csharp/{name}",
                f"python/{name}",
            )
        )
        print(f"[{lang}] {name}: outputs differ ({len(diff)} diff lines)")
        sys.stdout.writelines(diff[:200])
        if len(diff) > 200:
            print("... (diff truncated)")
    return ok


def main(argv) -> int:
    if "--" not in argv or argv.index("--") != 2 or len(argv) < 4:
        print(__doc__, file=sys.stderr)
        return 2
    options_file, langs = argv[0], [lang for lang in argv[1].split(",") if lang]
    reference_command = argv[3:]
    python_command = [sys.executable, str(PYTHON_GENERATOR)]

    ok = True
    with tempfile.TemporaryDirectory() as tmp:
        for lang in langs:
            base = pathlib.Path(tmp) / lang
            base.mkdir()
            expected = generate(reference_command, options_file, lang, base / "csharp")
            actual = generate(python_command, options_file, lang, base / "python")
            if compare(lang, expected, actual):
                print(f"[{lang}] OK ({len(expected)} files)")
            else:
                ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
