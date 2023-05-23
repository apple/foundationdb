#! /usr/bin/env python3

import argparse
import collections
import io
import logging
import json
import multiprocessing
import os
import os.path
import pathlib
import subprocess
import sys

from typing import List, Union

logger = logging.getLogger()

PWD = os.getcwd()
DEFAULT_SOURCE_SCANNER_PATH = pathlib.Path(os.path.join(PWD, "source_scanner"))
DEFAULT_COMPILATION_DATABASE_PATH = pathlib.Path(
    os.path.join(PWD, "build/compile_commands.json")
)


def _setup_args():
    parser = argparse.ArgumentParser("renormalize.py")

    parser.add_argument(
        "--source-scanner", type=str, default=None, help="Path to source_scanner"
    )
    parser.add_argument(
        "--extra-arg",
        type=str,
        default=None,
        help="Additional flags for source_scanner",
    )
    parser.add_argument(
        "--compilation-database",
        type=str,
        default=None,
        help="Path to compilation database",
    )
    parser.add_argument(
        "--num-workers", type=int, default=8, help="Number of workers in parallel"
    )

    return parser.parse_args()


class CompilationDatabase:
    def __init__(self, path: pathlib.Path):
        self._path = path
        self._compile_database = {}
        self._base_directory = None

        self._load_database()

    @property
    def path(self):
        return self._path

    @classmethod
    def _get_options(cls, command: str) -> str:
        return command.split(" ", 1)[1]

    def _load_database(self):
        with open(self._path, "r") as stream:
            data = json.load(stream)

        self._base_directory = pathlib.Path(
            os.path.commonpath([item["file"] for item in data])
        )
        for item in data:
            source = item["file"]
            compile_flags = CompilationDatabase._get_options(item["command"])
            source_relative_path = os.path.relpath(source, self._base_directory)
            self._compile_database[source_relative_path] = {"flags": compile_flags}

        logger.debug(
            f"Total {len(self._compile_database)} items found in compilation database"
        )

    @property
    def base_directory(self) -> Union[pathlib.Path, None]:
        return self._base_directory

    def iterate_files(self):
        for key in self._compile_database.keys():
            if not any(
                (prefix in key)
                for prefix in [
                    "flow/",
                    "fdbcli/",
                    "fdbserver/",
                    "fdbclient/",
                    "fdbrpc/",
                ]
            ):
                continue
            if os.path.splitext(key)[1] != ".cpp":
                continue
            yield key


class SerializableObjectLibrary:
    def __init__(self):
        self._library = collections.defaultdict(dict)

    def accept(
        self, path: str, class_name: str, fields: List[str], serialize_code: str
    ):
        if path in self._library and class_name in self._library[path]:
            return
        logger.debug(f"Add class {path}:{class_name}")
        self._library[path][class_name] = {
            "fields": fields,
            "raw_serialize_code": serialize_code,
        }

    def generate_report(self, stream: io.TextIOWrapper):
        paths = sorted(list(self._library.keys()))
        for path in paths:
            stream.write(f"## `{path}`\n")
            classes = sorted(list(self._library[path].keys()))
            for class_ in classes:
                stream.write(f"### `{class_}`\n")
                stream.write("#### Member variables\n")
                for index, item in enumerate(self._library[path][class_]["fields"]):
                    # NOTE index starts with 0
                    stream.write(f"{index + 1}. `{item['name']}`: `{item['type']}`\n")
                stream.write("#### Serialize code\n")
                stream.write("```c++\n")
                # This additional `\t` formats the source better.
                stream.write("\t" + self._library[path][class_]["raw_serialize_code"])
                stream.write("\n```\n")


class SourceScanner:
    def __init__(
        self,
        source_scanner_path: pathlib.Path,
        extra_arg: str,
        compilation_database: CompilationDatabase,
    ):
        self._source_scanner_path = source_scanner_path
        self._extra_arg = extra_arg
        self._project_base_directory = compilation_database.base_directory
        self._compiliation_database_path = compilation_database.path

    def scan(self, source_path: pathlib.Path):
        logger.debug(f"Scan file {source_path}")
        command = [
            self._source_scanner_path,
            source_path,
            "-p",
            self._compiliation_database_path,
        ]
        if self._extra_arg:
            command.extend(["--extra-arg", self._extra_arg])

        parsed = subprocess.run(
            command,
            capture_output=True,
        )
        stderr = parsed.stderr.decode("utf-8")
        if len(stderr) > 0:
            logger.warning(f"Error when parsing {source_path}: {stderr}")

        result = []
        for row in parsed.stdout.splitlines():
            item = json.loads(row)
            item["sourceFilePath"] = os.path.relpath(
                os.path.abspath(item["sourceFilePath"]), self._project_base_directory
            )
            result.append(item)

        return result


def _main():
    args = _setup_args()
    logging.basicConfig(level=logging.DEBUG)

    source_scanner = args.source_scanner or DEFAULT_SOURCE_SCANNER_PATH
    compilation_database = (
        args.compilation_database or DEFAULT_COMPILATION_DATABASE_PATH
    )

    logger.debug(f"Using source_scanner: {source_scanner}")
    logger.debug(f"Using compilation database: {compilation_database}")

    compilation_database = CompilationDatabase(compilation_database)
    assert compilation_database.base_directory is not None
    library = SerializableObjectLibrary()
    source_scanner = SourceScanner(source_scanner, args.extra_arg, compilation_database)
    paths = compilation_database.iterate_files()

    with multiprocessing.Pool(args.num_workers) as pool:
        for items in pool.map(source_scanner.scan, paths):
            for item in items:
                library.accept(
                    item["sourceFilePath"],
                    item["className"],
                    item["variables"],
                    item["raw"],
                )

    with open("SerialzedObjects.md", "w") as stream:
        library.generate_report(stream)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
