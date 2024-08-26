#!/usr/bin/env python3

import os
import shutil
import subprocess
import sys
from test_util import random_alphanum_string
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path


class ClusterFileGenerator:
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir).resolve()
        assert self.output_dir.exists(), "{} does not exist".format(output_dir)
        assert self.output_dir.is_dir(), "{} is not a directory".format(output_dir)
        self.tmp_dir = self.output_dir.joinpath("tmp", random_alphanum_string(16))
        self.tmp_dir.mkdir(parents=True)
        self.cluster_file_path = self.tmp_dir.joinpath("fdb.cluster")

    def __enter__(self):
        with open(self.cluster_file_path, "x") as f:
            f.write("foo:bar@1.1.1.1:5678\n")

        return self

    def __exit__(self, xc_type, exc_value, traceback):
        try:
            shutil.rmtree(self.tmp_dir)
        except FileNotFoundError:
            pass

    def close(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="""
    This script generates a cluster file that can be used to run a test and that will
    be cleaned up when the test is over. The cluster file will not correspond to a real
    cluster.

    Before the command is executed, the following arguments will be preprocessed:
    - All occurrences of @CLUSTER_FILE@ will be replaced with the path to the generated cluster file.

    The environment variable FDB_CLUSTER_FILE is set to the generated cluster file for the command if
    it is not set already.
    """,
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        metavar="OUTPUT_DIRECTORY",
        help="Directory where output files are written",
        required=True,
    )
    parser.add_argument("cmd", metavar="COMMAND", nargs="+", help="The command to run")
    args = parser.parse_args()
    errcode = 1

    with ClusterFileGenerator(args.output_dir) as generator:
        print("cluster-file: {}".format(generator.cluster_file_path))
        cmd_args = []
        for cmd in args.cmd:
            if cmd == "@CLUSTER_FILE@":
                cmd_args.append(str(generator.cluster_file_path))
            else:
                cmd_args.append(cmd)

        env = dict(**os.environ)
        env["FDB_CLUSTER_FILE"] = env.get(
            "FDB_CLUSTER_FILE", generator.cluster_file_path
        )
        errcode = subprocess.run(
            cmd_args, stdout=sys.stdout, stderr=sys.stderr, env=env
        ).returncode

    sys.exit(errcode)
