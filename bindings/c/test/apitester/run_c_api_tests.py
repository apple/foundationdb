#!/usr/bin/env python3
#
# run_c_api_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
import toml

# fmt: off
from tmp_cluster import TempCluster
from local_cluster import TLSConfig
# fmt: on

sys.path[:0] = [
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "tests", "TestRunner"
    )
]

TESTER_STATS_INTERVAL_SEC = 5


def random_string(len):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for i in range(len)
    )


def get_logger():
    return logging.getLogger("foundationdb.run_c_api_tests")


def initialize_logger_level(logging_level):
    logger = get_logger()

    assert logging_level in ["DEBUG", "INFO", "WARNING", "ERROR"]

    logging.basicConfig(format="%(message)s")
    if logging_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif logging_level == "INFO":
        logger.setLevel(logging.INFO)
    elif logging_level == "WARNING":
        logger.setLevel(logging.WARNING)
    elif logging_level == "ERROR":
        logger.setLevel(logging.ERROR)


def dump_client_logs(log_dir):
    for log_file in glob.glob(os.path.join(log_dir, "*")):
        print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
        with open(log_file, "r") as f:
            print(f.read())
        print(">>>>>>>>>>>>>>>>>>>> End of {}:".format(log_file))


def run_tester(args, cluster, test_file):
    build_dir = Path(args.build_dir).resolve()
    tester_binary = Path(args.api_tester_bin).resolve()
    external_client_library = build_dir.joinpath(
        "bindings", "c", "libfdb_c_external.so"
    )
    log_dir = Path(cluster.log).joinpath("client")
    log_dir.mkdir(exist_ok=True)
    cmd = [
        tester_binary,
        "--cluster-file",
        cluster.cluster_file,
        "--test-file",
        test_file,
        "--stats-interval",
        str(TESTER_STATS_INTERVAL_SEC * 1000),
        "--tmp-dir",
        cluster.tmp_dir,
        "--log",
        "--log-dir",
        str(log_dir),
    ]

    if args.external_client_library is not None:
        external_client_library = Path(args.external_client_library).resolve()
        cmd += ["--external-client-library", external_client_library]

    if args.retain_client_lib_copies:
        cmd += ["--retain-client-lib-copies"]

    if cluster.tls_config is not None:
        cmd += [
            "--tls-ca-file",
            cluster.server_ca_file,
            "--tls-key-file",
            cluster.client_key_file,
            "--tls-cert-file",
            cluster.client_cert_file,
        ]

    for knob in args.knobs:
        knob_name, knob_value = knob.split("=")
        cmd += ["--knob-" + knob_name, knob_value]

    get_logger().info("\nRunning tester '%s'..." % " ".join(map(str, cmd)))
    proc = Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
    timed_out = False
    ret_code = 1
    try:
        ret_code = proc.wait(args.timeout)
    except TimeoutExpired:
        proc.kill()
        timed_out = True
    except Exception as e:
        raise Exception("Unable to run tester (%s)" % e)

    if ret_code != 0:
        if timed_out:
            reason = "timed out after %d seconds" % args.timeout
        elif ret_code < 0:
            reason = signal.Signals(-ret_code).name
        else:
            reason = "exit code: %d" % ret_code
        get_logger().error(
            "\n'%s' did not complete successfully (%s)" % (cmd[0], reason)
        )
        if log_dir is not None and not args.disable_log_dump:
            dump_client_logs(log_dir)

    get_logger().info("")
    return ret_code


class TestConfig:
    def __init__(self, test_file):
        config = toml.load(test_file)
        server_config = config.get("server", [{}])[0]
        self.tenants_enabled = server_config.get("tenants_enabled", True)
        self.enable_encryption_at_rest = server_config.get(
            "enable_encryption_at_rest", False
        )
        self.tls_enabled = server_config.get("tls_enabled", False)
        self.client_chain_len = server_config.get("tls_client_chain_len", 2)
        self.server_chain_len = server_config.get("tls_server_chain_len", 3)
        self.min_num_processes = server_config.get("min_num_processes", 1)
        self.max_num_processes = server_config.get("max_num_processes", 3)
        self.num_processes = random.randint(
            self.min_num_processes, self.max_num_processes
        )


def run_test(args, test_file):
    config = TestConfig(test_file)

    tls_config = None
    if config.tls_enabled:
        tls_config = TLSConfig(
            server_chain_len=config.client_chain_len,
            client_chain_len=config.server_chain_len,
        )

    with TempCluster(
        args.build_dir,
        config.num_processes,
        enable_tenants=config.tenants_enabled,
        enable_encryption_at_rest=config.enable_encryption_at_rest,
        tls_config=tls_config,
    ) as cluster:
        ret_code = run_tester(args, cluster, test_file)
        if not cluster.check_cluster_logs():
            ret_code = 1 if ret_code == 0 else ret_code
        return ret_code


def run_tests(args):
    num_failed = 0
    if args.test_file is not None:
        test_files = [Path(args.test_file).resolve()]
    else:
        test_files = [
            f
            for f in os.listdir(args.test_dir)
            if os.path.isfile(os.path.join(args.test_dir, f)) and f.endswith(".toml")
        ]

    for test_file in test_files:
        get_logger().info("=========================================================")
        get_logger().info("Running test %s" % test_file)
        get_logger().info("=========================================================")
        ret_code = run_test(args, os.path.join(args.test_dir, test_file))
        if ret_code != 0:
            num_failed += 1

    return num_failed


def parse_args(argv):
    parser = argparse.ArgumentParser(description="FoundationDB C API Tester")
    parser.add_argument(
        "--build-dir", "-b", type=str, required=True, help="FDB build directory"
    )
    parser.add_argument(
        "--api-tester-bin",
        type=str,
        help="Path to the fdb_c_api_tester executable.",
        required=True,
    )
    parser.add_argument(
        "--external-client-library",
        type=str,
        help="Path to the external client library.",
    )
    parser.add_argument(
        "--retain-client-lib-copies",
        action="store_true",
        default=False,
        help="Retain temporary external client library copies.",
    )
    parser.add_argument(
        "--cluster-file",
        type=str,
        default="fdb.cluster",
        help="The cluster file for the cluster being connected to. (default: fdb.cluster)",
    )
    parser.add_argument(
        "--test-dir",
        type=str,
        default="./",
        help="Path to a directory with test definitions. (default: ./)",
    )
    parser.add_argument(
        "--test-file",
        type=str,
        default=None,
        help="Path to a single test definition to be executed, overrides --test-dir if set.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="The timeout in seconds for running each individual test. (default 300)",
    )
    parser.add_argument(
        "--logging-level",
        type=str,
        default="INFO",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        help="Specifies the level of detail in the tester output (default='INFO').",
    )
    parser.add_argument(
        "--knob",
        type=str,
        default=[],
        action="append",
        dest="knobs",
        help="[lowercase-knob-name]=[knob-value] (there may be multiple --knob options)",
    )
    parser.add_argument(
        "--disable-log-dump",
        help="Do not dump logs on error",
        action="store_true",
    )

    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    initialize_logger_level(args.logging_level)
    return run_tests(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
