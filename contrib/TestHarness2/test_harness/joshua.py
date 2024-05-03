from __future__ import annotations

import collections
import io
import re
import sys
import xml.sax
import xml.sax.handler
from pathlib import Path
from typing import List, OrderedDict, Set

from joshua import joshua_model

import test_harness.run
from test_harness.config import config
from test_harness.summarize import SummaryTree


class ToSummaryTree(xml.sax.handler.ContentHandler):
    def __init__(self):
        super().__init__()
        self.root: SummaryTree | None = None
        self.stack: List[SummaryTree] = []

    def result(self) -> SummaryTree:
        assert len(self.stack) == 0 and self.root is not None, "Parse Error"
        return self.root

    def startElement(self, name, attrs):
        new_child = SummaryTree(name)
        for k, v in attrs.items():
            new_child.attributes[k] = v
        self.stack.append(new_child)

    def endElement(self, name):
        closed = self.stack.pop()
        assert closed.name == name
        if len(self.stack) == 0:
            self.root = closed
        else:
            self.stack[-1].children.append(closed)


def _print_summary(summary: SummaryTree, commands: Set[str]):
    cmd = []
    is_valgrind_run = False
    if config.reproduce_prefix is not None:
        cmd.append(config.reproduce_prefix)
    cmd.append("bin/fdbserver")
    if "TestFile" in summary.attributes:
        file_name = summary.attributes["TestFile"]
        role = "test" if test_harness.run.is_no_sim(Path(file_name)) else "simulation"
        cmd += ["-r", role, "-f", file_name]
        if re.search(r"restarting\/.*-2\.", file_name):
            cmd += ["--restarting"]
    else:
        cmd += ["-r", "simulation", "-f", "<ERROR>"]
    if "RandomSeed" in summary.attributes:
        cmd += ["-s", summary.attributes["RandomSeed"]]
    else:
        cmd += ["-s", "<Error>"]
    if "BuggifyEnabled" in summary.attributes:
        arg = "on"
        if summary.attributes["BuggifyEnabled"].lower() in ["0", "off", "false"]:
            arg = "off"
        cmd += ["-b", arg]
    else:
        cmd += ["b", "<ERROR>"]
    cmd += ["--crash", "--trace_format", config.trace_format]
    # we want the command as the first attribute
    attributes = {"Command": " ".join(cmd)}
    for k, v in summary.attributes.items():
        if k == "Errors":
            attributes["ErrorCount"] = v
        else:
            attributes[k] = v
    summary.attributes = attributes
    error_count = 0
    warning_count = 0
    small_summary = SummaryTree("Test")
    small_summary.attributes = attributes
    errors = SummaryTree("Errors")
    warnings = SummaryTree("Warnings")
    buggifies: OrderedDict[str, List[int]] = collections.OrderedDict()
    for child in summary.children:
        if (
            "Severity" in child.attributes
            and child.attributes["Severity"] == "40"
            and error_count < config.max_errors
        ):
            error_count += 1
            if errors.name == "ValgrindError":
                is_valgrind_run = True
            errors.append(child)
        if (
            "Severity" in child.attributes
            and child.attributes["Severity"] == "30"
            and warning_count < config.max_warnings
        ):
            warning_count += 1
            warnings.append(child)
        if child.name == "BuggifySection":
            file = child.attributes["File"]
            line = int(child.attributes["Line"])
            buggifies.setdefault(file, []).append(line)
    buggifies_elem = SummaryTree("Buggifies")
    for file, lines in buggifies.items():
        lines.sort()
        if config.output_format == "json":
            buggifies_elem.attributes[file] = " ".join(str(line) for line in lines)
        else:
            child = SummaryTree("Buggify")
            child.attributes["File"] = file
            child.attributes["Lines"] = " ".join(str(line) for line in lines)
            small_summary.append(child)
    small_summary.children.append(buggifies_elem)
    if len(errors.children) > 0:
        small_summary.children.append(errors)
    if len(warnings.children) > 0:
        small_summary.children.append(warnings)
    if is_valgrind_run:
        idx = 0 if config.reproduce_prefix is None else 1
        cmd.insert(idx, "valgrind")
    key = " ".join(cmd)
    count = 1
    while key in commands:
        key = "{} # {}".format(" ".join(cmd), count)
        count += 1
    if config.details:
        key = str(len(commands))
        str_io = io.StringIO()
        summary.dump(str_io, prefix=("  " if config.pretty_print else ""))
        if config.output_format == "json":
            sys.stdout.write(
                '{}"Test{}": {}'.format(
                    "  " if config.pretty_print else "", key, str_io.getvalue()
                )
            )
        else:
            sys.stdout.write(str_io.getvalue())
        if config.pretty_print:
            sys.stdout.write("\n" if config.output_format == "xml" else ",\n")
        return key
    output = io.StringIO()
    small_summary.dump(output, prefix=("  " if config.pretty_print else ""))
    if config.output_format == "json":
        sys.stdout.write(
            '{}"{}": {}'.format(
                "  " if config.pretty_print else "", key, output.getvalue().strip()
            )
        )
    else:
        sys.stdout.write(
            "{}{}".format(
                "  " if config.pretty_print else "", output.getvalue().strip()
            )
        )
    sys.stdout.write("\n" if config.output_format == "xml" else ",\n")


def print_errors(ensemble_id: str):
    joshua_model.open(config.cluster_file)
    properties = joshua_model.get_ensemble_properties(ensemble_id)
    compressed = properties["compressed"] if "compressed" in properties else False
    for rec in joshua_model.tail_results(
        ensemble_id, errors_only=(not config.success), compressed=compressed
    ):
        if len(rec) == 5:
            versionstamp, result_code, host, seed, output = rec
        elif len(rec) == 4:
            versionstamp, result_code, host, output = rec
        elif len(rec) == 3:
            versionstamp, result_code, output = rec
        elif len(rec) == 2:
            versionstamp, seed = rec
            output = str(joshua_model.fdb.tuple.unpack(seed)[0]) + "\n"
        else:
            raise Exception("Unknown result format")
        lines = output.splitlines()
        commands: Set[str] = set()
        for line in lines:
            summary = ToSummaryTree()
            xml.sax.parseString(line, summary)
            commands.add(_print_summary(summary.result(), commands))
