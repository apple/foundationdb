#!/usr/bin/env python3

import glob
import os
import shutil
import subprocess
import sys
from local_cluster import LocalCluster
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from random import choice
from pathlib import Path


class TempCluster:
    def __init__(self, build_dir: str, process_number: int = 1, port: str = None):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        tmp_dir = self.build_dir.joinpath(
            "tmp",
            "".join(choice(LocalCluster.valid_letters_for_secret)
                    for i in range(16)),
        )
        tmp_dir.mkdir(parents=True)
        self.cluster = LocalCluster(
            tmp_dir,
            self.build_dir.joinpath("bin", "fdbserver"),
            self.build_dir.joinpath("bin", "fdbmonitor"),
            self.build_dir.joinpath("bin", "fdbcli"),
            process_number,
            port=port,
        )
        self.log = self.cluster.log
        self.etc = self.cluster.etc
        self.data = self.cluster.data
        self.tmp_dir = tmp_dir

    def __enter__(self):
        self.cluster.__enter__()
        self.cluster.create_database()
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        self.cluster.__exit__(xc_type, exc_value, traceback)
        shutil.rmtree(self.tmp_dir)

    def close(self):
        self.cluster.__exit__(None, None, None)
        shutil.rmtree(self.tmp_dir)


if __name__ == "__main__":
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="""
    This script automatically configures a temporary local cluster on the machine
    and then calls a command while this cluster is running. As soon as the command
    returns, the configured cluster is killed and all generated data is deleted.
    This is useful for testing: if a test needs access to a fresh fdb cluster, one
    can simply pass the test command to this script.

    The command to run after the cluster started. Before the command is executed,
    the following arguments will be preprocessed:
    - All occurrences of @CLUSTER_FILE@ will be replaced with the path to the generated cluster file.
    - All occurrences of @DATA_DIR@ will be replaced with the path to the data directory.
    - All occurrences of @LOG_DIR@ will be replaced with the path to the log directory.
    - All occurrences of @ETC_DIR@ will be replaced with the path to the configuration directory.

    The environment variable FDB_CLUSTER_FILE is set to the generated cluster for the command if it is not set already.
    """,
    )
    parser.add_argument(
        "--build-dir",
        "-b",
        metavar="BUILD_DIRECTORY",
        help="FDB build directory",
        required=True,
    )
    parser.add_argument("cmd", metavar="COMMAND",
                        nargs="+", help="The command to run")
    parser.add_argument(
        "--process-number",
        "-p",
        help="Number of fdb processes running",
        type=int,
        default=1,
    )
    parser.add_argument(
        '--disable-log-dump',
        help='Do not dump cluster log on error',
        action="store_true"
    )
    args = parser.parse_args()
    errcode = 1
    with TempCluster(args.build_dir, args.process_number) as cluster:
        print("log-dir: {}".format(cluster.log))
        print("etc-dir: {}".format(cluster.etc))
        print("data-dir: {}".format(cluster.data))
        print("cluster-file: {}".format(cluster.etc.joinpath("fdb.cluster")))
        cmd_args = []
        for cmd in args.cmd:
            if cmd == "@CLUSTER_FILE@":
                cmd_args.append(str(cluster.etc.joinpath("fdb.cluster")))
            elif cmd == "@DATA_DIR@":
                cmd_args.append(str(cluster.data))
            elif cmd == "@LOG_DIR@":
                cmd_args.append(str(cluster.log))
            elif cmd == "@ETC_DIR@":
                cmd_args.append(str(cluster.etc))
            else:
                cmd_args.append(cmd)
        env = dict(**os.environ)
        env["FDB_CLUSTER_FILE"] = env.get(
            "FDB_CLUSTER_FILE", cluster.etc.joinpath("fdb.cluster")
        )
        errcode = subprocess.run(
            cmd_args, stdout=sys.stdout, stderr=sys.stderr, env=env
        ).returncode

        sev40s = (
            subprocess.getoutput(
                "grep -r 'Severity=\"40\"' {}".format(cluster.log.as_posix())
            )
            .rstrip()
            .splitlines()
        )

        for line in sev40s:
            # When running ASAN we expect to see this message. Boost coroutine should be using the correct asan annotations so that it shouldn't produce any false positives.
            if line.endswith(
                "WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!"
            ):
                continue
            print(">>>>>>>>>>>>>>>>>>>> Found severity 40 events - the test fails")
            errcode = 1
            break

        if errcode and not args.disable_log_dump:
            for etc_file in glob.glob(os.path.join(cluster.etc, "*")):
                print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(etc_file))
                with open(etc_file, "r") as f:
                    print(f.read())
            for log_file in glob.glob(os.path.join(cluster.log, "*")):
                print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
                with open(log_file, "r") as f:
                    print(f.read())

    sys.exit(errcode)
