#!/usr/bin/env python3
"""Parse FoundationDB XML trace events, resolve backtraces via addr2line, and
emit the original log interleaved with symbolicated stack traces."""

import sys
import re
import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Resolve FDB trace backtraces inline using addr2line")
    parser.add_argument("file", nargs="?", help="Input file (default: stdin)")
    parser.add_argument("--addr2line", default="llvm-addr2line",
                        help="addr2line binary to use (default: llvm-addr2line)")
    parser.add_argument("--debug-binary", default="7.3.77-fdbserver.debug.x86_64",
                        help="Debug binary path (default: 7.3.77-fdbserver.debug.x86_64)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands instead of executing them")
    args = parser.parse_args()

    pattern = re.compile(r'addr2line -e \S+ -p -C -f -i (0x[0-9a-f]+(?:\s+0x[0-9a-f]+)*)')

    if args.file:
        f = open(args.file)
    else:
        f = sys.stdin

    for line in f:
        sys.stdout.write(line)
        for match in pattern.finditer(line):
            addrs = match.group(1)
            cmd = [args.addr2line, "-e", args.debug_binary, "-p", "-C", "-f", "-i"] + addrs.split()
            if args.dry_run:
                print(f"---------- BACKTRACE BEGIN ----------")
                print(f"[Running: {' '.join(cmd)}]")
                print(f"---------- BACKTRACE END ----------")
            else:
                try:
                    print(f"---------- BACKTRACE BEGIN ----------")
                    print(f"[Running: {' '.join(cmd)}]")
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                    if result.stdout:
                        sys.stdout.write(result.stdout)
                    if result.returncode != 0 and result.stderr:
                        print(f"# addr2line error: {result.stderr.strip()}", file=sys.stderr)
                except FileNotFoundError:
                    print(f"# ERROR: {args.addr2line} not found", file=sys.stderr)
                    sys.exit(1)
                except subprocess.TimeoutExpired:
                    print(f"# ERROR: addr2line timed out", file=sys.stderr)
                print(f"---------- BACKTRACE END ----------")

    if args.file:
        f.close()


if __name__ == "__main__":
    main()
