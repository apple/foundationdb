from __future__ import annotations

import sys
import xml.sax
import xml.sax.handler
from pathlib import Path
from typing import List

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
        assert len(self.stack) == 0 and self.root is not None, 'Parse Error'
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


def print_errors(ensemble_id: str):
    joshua_model.open(config.cluster_file)
    properties = joshua_model.get_ensemble_properties(ensemble_id)
    compressed = properties["compressed"] if "compressed" in properties else False
    for rec in joshua_model.tail_results(ensemble_id, errors_only=(not config.details), compressed=compressed):
        if len(rec) == 5:
            version_stamp, result_code, host, seed, output = rec
        elif len(rec) == 4:
            version_stamp, result_code, host, output = rec
            seed = None
        elif len(rec) == 3:
            version_stamp, result_code, output = rec
            host = None
            seed = None
        elif len(rec) == 2:
            version_stamp, seed = rec
            output = str(joshua_model.fdb.tuple.unpack(seed)[0]) + "\n"
            result_code = None
            host = None
            seed = None
        else:
            raise Exception("Unknown result format")
        summary = ToSummaryTree()
        xml.sax.parseString(output, summary)
        res = summary.result()
        cmd = []
        if config.reproduce_prefix is not None:
            cmd.append(config.reproduce_prefix)
        cmd.append('fdbserver')
        if 'TestFile' in res.attributes:
            file_name = res.attributes['TestFile']
            role = 'test' if test_harness.run.is_no_sim(Path(file_name)) else 'simulation'
            cmd += ['-r', role, '-f', file_name]
        else:
            cmd += ['-r', 'simulation', '-f', '<ERROR>']
        if 'BuggifyEnabled' in res.attributes:
            arg = 'on'
            if res.attributes['BuggifyEnabled'].lower() in ['0', 'off', 'false']:
                arg = 'off'
            cmd += ['-b', arg]
        else:
            cmd += ['b', '<ERROR>']
        cmd += ['--crash', '--trace_format', 'json']
        # we want the command as the first attribute
        attributes = {'Command': ' '.join(cmd)}
        for k, v in res.attributes.items():
            attributes[k] = v
        res.attributes = attributes
        res.dump(sys.stdout, prefix=('  ' if config.pretty_print else ''), new_line=config.pretty_print)
