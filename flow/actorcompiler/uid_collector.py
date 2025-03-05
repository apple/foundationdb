#! /usr/bin/env python3

import dataclasses
import glob
import io
import logging
import os.path
import pathlib
import secrets
import sys

from typing import List, Tuple

logger = logging.getLogger("uid_collector")


LICENSE: str = r"""/*
 * FoundationDB ACTOR UID data
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Do not include this file directly.
 */
"""


@dataclasses.dataclass
class Identifier:
    guid1: int
    guid2: int
    path: pathlib.Path
    line: int
    type_: str
    name: str

    def parse(line: str) -> "Identifier":
        nsplit = line.strip().split("|")
        assert len(nsplit) == 6, "Expecting 6 terms"

        guid1_s, guid2_s, path, line_s, type_, name = nsplit

        return Identifier(int(guid1_s), int(guid2_s), path, int(line_s), type_, name)


def parse(stream: io.TextIOWrapper) -> List[Identifier]:
    return [Identifier.parse(line) for line in stream.readlines()]


def collect(path: pathlib.Path) -> List[Identifier]:
    result = []

    for item in glob.glob("**/*.uid", root_dir=path, recursive=True):
        logger.info(f"Parsing {path}...")
        full_path = os.path.join(path, item)
        with open(full_path, "r") as stream:
            r = parse(stream)
            logger.info(f"Found {len(r)} items")
            result.extend(r)

    return result


def generate_binary_version() -> Tuple[int, int]:
    """Generates two 64-bit random numbers as the unique binary verison of fdbserver.
    The ACTOR identifiers/block identifiers mapping are versioned using this.
    """
    return (
        int.from_bytes(secrets.token_bytes(8)),
        int.from_bytes(secrets.token_bytes(8)),
    )


def render(
    stream: io.TextIOWrapper, binary_version: Tuple[int, int], items: List[Identifier]
):
    stream.write(LICENSE)
    stream.write(
        r"""
#ifndef __ACTOR_UID_DATA_H_INCLUDE
#error This file should be included by ActorUID.cpp only
#endif

#include "flow/ActorUID.h"

#include <string_view>

namespace ActorMonitoring {
"""
    )
    stream.write(
        f"""
const GUID BINARY_GUID = GUID({binary_version[0]}ULL, {binary_version[1]}ULL);
"""
    )
    stream.write(
        r"""
namespace {

std::unordered_map<GUID, ActorDebuggingData> initializeActorDebuggingData() {
#if ACTOR_MONITORING == ACTOR_MONITORING_DISABLED
    return {};
#else   // ACTOR_MONITORING == ACTOR_MONITORING_DISABLED
    // FIXME: For unknown reasons string literals ""sv is not working with error
    // no matching literal operator for call to 'operator""sv' with arguments of types 'const char *' and 'unsigned long', and no matching literal operator template
    return {
"""
    )

    for item in items:
        # FIXME: Escape path for item.path in string
        template = '        < <{0}ULL, {1}ULL>, <std::string_view("{2}"), {3}, std::string_view("{4}"), std::string_view("{5}")> >,\n'.format(
            item.guid1, item.guid2, item.path, item.line, item.name, item.type_
        )
        stream.write(template.replace("<", "{").replace(">", "}"))

    stream.write(
        r"""
    };  // return
#endif  // ACTOR_MONITORING == ACTOR_MONITORING_DISABLED
}   // initializeActorDebuggingData

}   // namespace

const std::unordered_map<GUID, ActorDebuggingData> ACTOR_DEBUGGING_DATA = initializeActorDebuggingData();

}   // namespace ActorMonitoring
"""
    )


def main(argv: List[str]):
    argc = len(argv)
    logging.basicConfig(level=logging.WARN)

    if argc not in [2, 3]:
        print(
            "uid_collector.py [FoundationDB build path] [Output File]", file=sys.stderr
        )
        sys.exit(-1)

    stream = None
    if argc == 2:
        stream = sys.stdout
        render(sys.stdout, generate_binary_version(), collect(os.path.abspath(argv[1])))
    if argc == 3:
        stream = open(argv[2], "w")

    render(stream, generate_binary_version(), collect(os.path.abspath(argv[1])))


if __name__ == "__main__":
    main(sys.argv)
