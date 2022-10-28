import argparse
import re
import sys

from pathlib import Path
from test_harness.config import config
from test_harness.summarize import Summary, TraceFiles
from typing import Pattern, List


def files_matching(path: Path, pattern: Pattern, recurse: bool = True) -> List[Path]:
    res: List[Path] = []
    for file in path.iterdir():
        if file.is_file() and pattern.match(file.name) is not None:
            res.append(file)
        elif file.is_dir() and recurse:
            res += files_matching(file, pattern, recurse)
    return res


def dirs_with_files_matching(path: Path, pattern: Pattern, recurse: bool = True) -> List[Path]:
    res: List[Path] = []
    sub_directories: List[Path] = []
    has_file = False
    for file in path.iterdir():
        if file.is_file() and pattern.match(file.name) is not None:
            has_file = True
        elif file.is_dir() and recurse:
            sub_directories.append(file)
    if has_file:
        res.append(path)
    if recurse:
        for file in sub_directories:
            res += dirs_with_files_matching(file, pattern, recurse=True)
    res.sort()
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser('TestHarness Timeout', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    config.build_arguments(parser)
    args = parser.parse_args()
    config.extract_args(args)
    valgrind_files: List[Path] = []
    if config.use_valgrind:
        valgrind_files = files_matching(Path.cwd(), re.compile(r'valgrind.*\.xml'))

    for directory in dirs_with_files_matching(Path.cwd(), re.compile(r'trace.*\.(json|xml)'), recurse=True):
        trace_files = TraceFiles(directory)
        for files in trace_files.items():
            if config.use_valgrind:
                for valgrind_file in valgrind_files:
                    summary = Summary(Path('bin/fdbserver'), was_killed=True)
                    summary.valgrind_out_file = valgrind_file
                    summary.summarize_files(files)
                    summary.out.dump(sys.stdout)
            elif config.long_running:
                summary = Summary(Path('bin/fdbserver'), was_killed=True, long_running=True)
                summary.summarize_files(files)
                summary.out.dump(sys.stdout)
            else:
                summary = Summary(Path('bin/fdbserver'), was_killed=True)
                summary.summarize_files(files)
                summary.out.dump(sys.stdout)
