import argparse
import random

from test_harness.config import config
from test_harness.run import TestRunner


if __name__ == '__main__':
    # seed the random number generator
    parser = argparse.ArgumentParser('TestHarness')
    config.build_arguments(parser)
    test_runner = TestRunner()
    # initialize arguments
    parser.add_argument('--joshua-dir', type=str, help='Where to write FDB data to', required=False)
    parser.add_argument('-C', '--cluster-file', type=str, help='Path to fdb cluster file', required=False)
    parser.add_argument('--stats', type=str,
                        help='A base64 encoded list of statistics (used to reproduce runs)',
                        required=False)
    args = parser.parse_args()
    config.extract_args(args)
    random.seed(config.joshua_seed)
    if not test_runner.run(args.stats):
        exit(1)
