from __future__ import annotations

import argparse
import io
import json
import re
import sys
import test_harness.fdb

from typing import List, Tuple, OrderedDict
from test_harness.summarize import SummaryTree, Coverage
from test_harness.config import config
from xml.sax.saxutils import quoteattr


class GlobalStatistics:
    def __init__(self):
        self.total_probes_hit: int = 0
        self.total_cpu_time: int = 0
        self.total_test_runs: int = 0
        self.total_missed_probes: int = 0


class EnsembleResults:
    def __init__(self, cluster_file: str | None, ensemble_id: str):
        self.global_statistics = GlobalStatistics()
        self.fdb_path = ('joshua', 'ensembles', 'results', 'application', ensemble_id)
        self.coverage_path = self.fdb_path + ('coverage',)
        self.statistics = test_harness.fdb.Statistics(cluster_file, self.fdb_path)
        coverage_dict: OrderedDict[Coverage, int] = test_harness.fdb.read_coverage(cluster_file, self.coverage_path)
        self.coverage: List[Tuple[Coverage, int]] = []
        self.min_coverage_hit: int | None = None
        self.ratio = self.global_statistics.total_test_runs / config.hit_per_runs_ratio
        for cov, count in coverage_dict.items():
            if re.search(config.cov_include_files, cov.file) is None:
                continue
            if re.search(config.cov_exclude_files, cov.file) is not None:
                continue
            self.global_statistics.total_probes_hit += count
            self.coverage.append((cov, count))
            if count <= self.ratio:
                self.global_statistics.total_missed_probes += 1
            if self.min_coverage_hit is None or self.min_coverage_hit > count:
                self.min_coverage_hit = count
        self.coverage.sort(key=lambda x: (x[1], x[0].file, x[0].line))
        self.stats: List[Tuple[str, int, int]] = []
        for k, v in self.statistics.stats.items():
            self.global_statistics.total_test_runs += v.run_count
            self.global_statistics.total_cpu_time += v.runtime
            self.stats.append((k, v.runtime, v.run_count))
        self.stats.sort(key=lambda x: x[1], reverse=True)
        if self.min_coverage_hit is not None:
            self.coverage_ok = self.min_coverage_hit > self.ratio
        else:
            self.coverage_ok = False

    def dump(self, prefix: str):
        errors = 0
        out = SummaryTree('EnsembleResults')
        out.attributes['TotalRuntime'] = str(self.global_statistics.total_cpu_time)
        out.attributes['TotalTestRuns'] = str(self.global_statistics.total_test_runs)
        out.attributes['TotalProbesHit'] = str(self.global_statistics.total_probes_hit)
        out.attributes['MinProbeHit'] = str(self.min_coverage_hit)
        out.attributes['TotalProbes'] = str(len(self.coverage))
        out.attributes['MissedProbes'] = str(self.global_statistics.total_missed_probes)

        for cov, count in self.coverage:
            severity = 10 if count > self.ratio else 40
            if severity == 40:
                errors += 1
            if (severity == 40 and errors <= config.max_errors) or config.details:
                child = SummaryTree('CodeProbe')
                child.attributes['Severity'] = str(severity)
                child.attributes['File'] = cov.file
                child.attributes['Line'] = str(cov.line)
                child.attributes['Comment'] = '' if cov.comment is None else cov.comment
                child.attributes['HitCount'] = str(count)
                child.attributes['Rare'] = str(cov.rare)
                out.append(child)

        if config.details:
            for k, runtime, run_count in self.stats:
                child = SummaryTree('Test')
                child.attributes['Name'] = k
                child.attributes['Runtime'] = str(runtime)
                child.attributes['RunCount'] = str(run_count)
                out.append(child)
        if errors > 0:
            out.attributes['Errors'] = str(errors)
        str_io = io.StringIO()
        out.dump(str_io, prefix=prefix, new_line=config.pretty_print)
        if config.output_format == 'xml':
            sys.stdout.write(str_io.getvalue())
        else:
            sys.stdout.write('{}"EnsembleResults":{}{}'.format('  ' if config.pretty_print else '',
                                                               '\n' if config.pretty_print else ' ',
                                                               str_io.getvalue()))


def write_header(ensemble_id: str):
    if config.output_format == 'json':
        if config.pretty_print:
            print('{')
            print('  "{}": {},\n'.format('ID', json.dumps(ensemble_id.strip())))
        else:
            sys.stdout.write('{{{}: {},'.format('ID', json.dumps(ensemble_id.strip())))
    elif config.output_format == 'xml':
        sys.stdout.write('<Ensemble ID={}>'.format(quoteattr(ensemble_id.strip())))
        if config.pretty_print:
            sys.stdout.write('\n')
    else:
        assert False, 'unknown output format {}'.format(config.output_format)


def write_footer():
    if config.output_format == 'xml':
        sys.stdout.write('</Ensemble>\n')
    elif config.output_format == 'json':
        sys.stdout.write('}\n')
    else:
        assert False, 'unknown output format {}'.format(config.output_format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('TestHarness Results', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    config.change_default('pretty_print', True)
    config.change_default('max_warnings', 0)
    config.build_arguments(parser)
    parser.add_argument('ensemble_id', type=str, help='The ensemble to fetch the result for')
    args = parser.parse_args()
    config.extract_args(args)
    config.output_format = args.output_format
    write_header(args.ensemble_id)
    try:
        import test_harness.joshua
        test_harness.joshua.print_errors(args.ensemble_id)
    except ModuleNotFoundError:
        child = SummaryTree('JoshuaNotFound')
        child.attributes['Severity'] = '30'
        child.attributes['Message'] = 'Could not import Joshua -- set PYTHONPATH to joshua checkout dir'
        child.dump(sys.stdout, prefix=('  ' if config.pretty_print else ''), new_line=config.pretty_print)
    results = EnsembleResults(config.cluster_file, args.ensemble_id)
    results.dump('  ' if config.pretty_print else '')
    write_footer()
    exit(0 if results.coverage_ok else 1)
