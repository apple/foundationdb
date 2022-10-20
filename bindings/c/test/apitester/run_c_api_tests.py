#!/usr/bin/env python3
#
# run_c_api_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

import sys
import argparse
import os
from subprocess import Popen, TimeoutExpired
import logging
import signal
from pathlib import Path
import glob
import random
import string

TESTER_STATS_INTERVAL_SEC = 5


def random_string(len):
    return ''.join(random.choice(string.ascii_letters + string.digits) for i in range(len))


def get_logger():
    return logging.getLogger('foundationdb.run_c_api_tests')


def initialize_logger_level(logging_level):
    logger = get_logger()

    assert logging_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR']

    logging.basicConfig(format='%(message)s')
    if logging_level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif logging_level == 'INFO':
        logger.setLevel(logging.INFO)
    elif logging_level == 'WARNING':
        logger.setLevel(logging.WARNING)
    elif logging_level == 'ERROR':
        logger.setLevel(logging.ERROR)


def dump_client_logs(log_dir):
    for log_file in glob.glob(os.path.join(log_dir, "*")):
        print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
        with open(log_file, "r") as f:
            print(f.read())
        print(">>>>>>>>>>>>>>>>>>>> End of {}:".format(log_file))


def run_tester(args, test_file):
    cmd = [args.tester_binary,
           "--cluster-file", args.cluster_file,
           "--test-file", test_file,
           "--stats-interval", str(TESTER_STATS_INTERVAL_SEC*1000)]
    if args.external_client_library is not None:
        cmd += ["--external-client-library", args.external_client_library]
    if args.tmp_dir is not None:
        cmd += ["--tmp-dir", args.tmp_dir]
    log_dir = None
    if args.log_dir is not None:
        log_dir = Path(args.log_dir).joinpath(random_string(8))
        log_dir.mkdir(exist_ok=True)
        cmd += ['--log', "--log-dir", str(log_dir)]

    if args.blob_granule_local_file_path is not None:
        cmd += ["--blob-granule-local-file-path",
                args.blob_granule_local_file_path]

    if args.tls_ca_file is not None:
        cmd += ["--tls-ca-file", args.tls_ca_file]

    if args.tls_key_file is not None:
        cmd += ["--tls-key-file", args.tls_key_file]

    if args.tls_cert_file is not None:
        cmd += ["--tls-cert-file", args.tls_cert_file]

    for knob in args.knobs:
        knob_name, knob_value = knob.split("=")
        cmd += ["--knob-" + knob_name, knob_value]

    get_logger().info('\nRunning tester \'%s\'...' % ' '.join(cmd))
    proc = Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
    timed_out = False
    ret_code = 1
    try:
        ret_code = proc.wait(args.timeout)
    except TimeoutExpired:
        proc.kill()
        timed_out = True
    except Exception as e:
        raise Exception('Unable to run tester (%s)' % e)

    if ret_code != 0:
        if timed_out:
            reason = 'timed out after %d seconds' % args.timeout
        elif ret_code < 0:
            reason = signal.Signals(-ret_code).name
        else:
            reason = 'exit code: %d' % ret_code
        get_logger().error('\n\'%s\' did not complete succesfully (%s)' %
                           (cmd[0], reason))
        if (log_dir is not None):
            dump_client_logs(log_dir)

    get_logger().info('')
    return ret_code


def run_tests(args):
    num_failed = 0
    test_files = [f for f in os.listdir(args.test_dir) if os.path.isfile(
        os.path.join(args.test_dir, f)) and f.endswith(".toml")]

    for test_file in test_files:
        get_logger().info('=========================================================')
        get_logger().info('Running test %s' % test_file)
        get_logger().info('=========================================================')
        ret_code = run_tester(args, os.path.join(args.test_dir, test_file))
        if ret_code != 0:
            num_failed += 1

    return num_failed


def parse_args(argv):
    parser = argparse.ArgumentParser(description='FoundationDB C API Tester')

    parser.add_argument('--cluster-file', type=str, default="fdb.cluster",
                        help='The cluster file for the cluster being connected to. (default: fdb.cluster)')
    parser.add_argument('--tester-binary', type=str, default="fdb_c_api_tester",
                        help='Path to the fdb_c_api_tester executable. (default: fdb_c_api_tester)')
    parser.add_argument('--external-client-library', type=str, default=None,
                        help='Path to the external client library. (default: None)')
    parser.add_argument('--test-dir', type=str, default="./",
                        help='Path to a directory with test definitions. (default: ./)')
    parser.add_argument('--timeout', type=int, default=300,
                        help='The timeout in seconds for running each individual test. (default 300)')
    parser.add_argument('--log-dir', type=str, default=None,
                        help='The directory for storing logs (default: None)')
    parser.add_argument('--logging-level', type=str, default='INFO',
                        choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'], help='Specifies the level of detail in the tester output (default=\'INFO\').')
    parser.add_argument('--tmp-dir', type=str, default=None,
                        help='The directory for storing temporary files (default: None)')
    parser.add_argument('--blob-granule-local-file-path', type=str, default=None,
                        help='Enable blob granule tests if set, value is path to local blob granule files')
    parser.add_argument('--tls-ca-file', type=str, default=None,
                        help='Path to client\'s TLS CA file: i.e. certificate of CA that signed the server certificate')
    parser.add_argument('--tls-cert-file', type=str, default=None,
                        help='Path to client\'s TLS certificate file')
    parser.add_argument('--tls-key-file', type=str, default=None,
                        help='Path to client\'s TLS private key file')
    parser.add_argument('--knob', type=str, default=[], action="append", dest="knobs",
                        help='[lowercase-knob-name]=[knob-value] (there may be multiple --knob options)')

    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    initialize_logger_level(args.logging_level)
    return run_tests(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
