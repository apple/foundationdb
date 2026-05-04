# build_logs_to_json.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

import argparse
import glob
import json
import os
import sys
import xmltodict


def convert_ninja_log(args):
    with open(os.path.join(args.build_dir, ".ninja_log"), "r") as ninja_log:
        result = []
        firstLine = next(ninja_log)
        assert firstLine == "# ninja log v5\n"
        for l in ninja_log:
            try:
                (start_ms, end_ms, _, name, cmd_hash) = l.rstrip().split("\t")
                result.append(
                    dict(
                        start_ms=int(start_ms),
                        end_ms=int(end_ms),
                        name=name,
                        cmd_hash=cmd_hash,
                    )
                )
            except ValueError:  # malformed line?
                pass
    return (os.path.join(args.build_dir, "packages", "ninja_log.json"), result)


def convert_test_xml(args):
    result = []
    for xml_file_name in glob.iglob(
        os.path.join(args.build_dir, "Testing", "**/*.xml"), recursive=True
    ):
        with open(xml_file_name) as xml_file:
            result.append({xml_file_name: xmltodict.parse(xml_file.read())})
    return (os.path.join(args.build_dir, "packages", "test_log.json"), result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
    Convert certain interesting build logs to json files, for easier ingestion.
    """,
    )
    parser.add_argument(
        "--build-dir",
        "-b",
        metavar="BUILD_DIRECTORY",
        help="FDB build directory",
        required=True,
    )
    args = parser.parse_args()
    for f in [convert_ninja_log, convert_test_xml]:
        try:
            (filename, result) = f(args)
            with open(filename, "w") as file_obj:
                json.dump(result, file_obj)
            print("Wrote {}".format(filename))
        except FileNotFoundError as e:
            print("File {} not found. Skipping {} step".format(e.filename, f.__name__))
