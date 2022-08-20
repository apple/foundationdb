from __future__ import annotations

import sys
from typing import List, Tuple

from test_harness.summarize import SummaryTree
from test_harness.config import config

import argparse
import test_harness.fdb


class EnsembleResults:
    def __init__(self, cluster_file: str | None, ensemble_id: str):
        self.fdb_path = ('joshua', 'ensembles', 'results', 'application', ensemble_id)
        self.statistics = test_harness.fdb.Statistics(cluster_file, self.fdb_path)
        self.out = SummaryTree('EnsembleResults')
        stats: List[Tuple[str, int, int]] = []
        for k, v in self.statistics.stats.items():
            stats.append((k, v.runtime, v.run_count))
        stats.sort(key=lambda x: x[1], reverse=True)
        for k, runtime, run_count in stats:
            child = SummaryTree('Test')
            child.attributes['Name'] = k
            child.attributes['Runtime'] = str(runtime)
            child.attributes['RunCount'] = str(run_count)
            self.out.append(child)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('TestHarness Results', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-C', '--cluster-file', required=False, help='Path to cluster file')
    parser.add_argument('-o', '--output-format', default='json', choices=['json', 'xml'], help='Format of the output')
    parser.add_argument('ensemble_id', type=str, help='The ensemble to fetch the result for')
    args = parser.parse_args()
    config.pretty_print = True
    config.output_format = args.output_format
    results = EnsembleResults(args.cluster_file, args.ensemble_id)
    results.out.dump(sys.stdout)
