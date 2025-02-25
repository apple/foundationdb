#!/usr/bin/env python3
#
# test_argument_parsing.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import argparse
import subprocess

last_command_output = None


def check(condition):
    global last_command_output
    assert condition, "Command output:\n" + last_command_output


def run_command(command, args):
    global last_command_output
    last_command_output = (
        subprocess.run(command + args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        .stdout.decode("utf-8")
        .strip()
    )
    return last_command_output


def is_unknown_option(output):
    return output.startswith("ERROR: unknown option")


def is_unknown_knob(output):
    return output.startswith("ERROR: Invalid knob option")

def is_invalid_knob_value(output):
    return output.startswith("ERROR: Invalid value")

def is_cli_usage(output):
    return output.startswith("FoundationDB CLI")


def test_fdbserver(build_dir):
    command = [args.build_dir + "/bin/fdbserver", "-r", "unittests"]

    check(is_unknown_option(run_command(command, ["--unknown-option"])))

    check(not is_unknown_option(run_command(command, ["--cluster-file", "foo"])))
    check(not is_unknown_option(run_command(command, ["--cluster_file", "foo"])))

    check(is_unknown_knob(run_command(command, ["--knob-fake-knob", "foo"])))
    check(is_invalid_knob_value(run_command(command, ["--knob_commit_batches_mem_bytes_hard_limit", "4GiB"])))

    check(not is_unknown_knob(run_command(command, ["--knob-min-trace-severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob-min_trace_severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob_min_trace_severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob_min-trace-severity", "5"])))


def test_fdbcli(build_dir):
    command = [args.build_dir + "/bin/fdbcli", "--exec", "begin"]

    check(is_cli_usage(run_command(command, ["--unknown-option"])))

    check(not is_cli_usage(run_command(command, ["--api-version", "700"])))
    check(not is_cli_usage(run_command(command, ["--api_version", "700"])))

    check(is_unknown_knob(run_command(command, ["--knob-fake-knob", "foo"])))

    check(not is_unknown_knob(run_command(command, ["--knob-min-trace-severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob-min_trace_severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob_min_trace_severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob_min-trace-severity", "5"])))


def test_fdbbackup(build_dir):
    command = [args.build_dir + "/bin/fdbbackup", "list"]

    check(is_unknown_option(run_command(command, ["--unknown-option"])))

    check(not is_unknown_option(run_command(command, ["--trace-format", "foo"])))
    check(not is_unknown_option(run_command(command, ["--trace_format", "foo"])))

    check(is_unknown_knob(run_command(command, ["--knob-fake-knob", "foo"])))

    check(not is_unknown_knob(run_command(command, ["--knob-min-trace-severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob-min_trace_severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob_min_trace_severity", "5"])))
    check(not is_unknown_knob(run_command(command, ["--knob_min-trace-severity", "5"])))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="This test checks for proper command line argument parsing."
    )
    parser.add_argument(
        "build_dir", metavar="BUILD_DIRECTORY", help="FDB build directory"
    )
    args = parser.parse_args()

    test_fdbserver(args.build_dir)
    test_fdbcli(args.build_dir)
    test_fdbbackup(args.build_dir)
