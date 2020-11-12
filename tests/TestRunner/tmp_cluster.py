#!/usr/bin/env python3

import shutil
import subprocess
import sys
import socket
from local_cluster import LocalCluster
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from random import choice
from pathlib import Path

class TempCluster:
    def __init__(self, build_dir: str):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        tmp_dir = self.build_dir.joinpath(
            'tmp',
            ''.join(choice(LocalCluster.valid_letters_for_secret) for i in range(16)))
        tmp_dir.mkdir(parents=True)
        self.cluster = LocalCluster(tmp_dir,
                                    self.build_dir.joinpath('bin', 'fdbserver'),
                                    self.build_dir.joinpath('bin', 'fdbmonitor'),
                                    self.build_dir.joinpath('bin', 'fdbcli'))
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


if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
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
    """)
    parser.add_argument('--build-dir', '-b', metavar='BUILD_DIRECTORY', help='FDB build directory', required=True)
    parser.add_argument('cmd', metavar="COMMAND", nargs="+", help="The command to run")
    args = parser.parse_args()
    errcode = 1
    with TempCluster(args.build_dir) as cluster:
        print("log-dir: {}".format(cluster.log))
        print("etc-dir: {}".format(cluster.etc))
        print("data-dir: {}".format(cluster.data))
        print("cluster-file: {}".format(cluster.etc.joinpath('fdb.cluster')))
        cmd_args = []
        for cmd in args.cmd:
            if cmd == '@CLUSTER_FILE@':
                cmd_args.append(str(cluster.etc.joinpath('fdb.cluster')))
            elif cmd == '@DATA_DIR@':
                cmd_args.append(str(cluster.data))
            elif cmd == '@LOG_DIR@':
                cmd_args.append(str(cluster.log))
            elif cmd == '@ETC_DIR@':
                cmd_args.append(str(cluster.etc))
            else:
                cmd_args.append(cmd)
        errcode = subprocess.run(cmd_args, stdout=sys.stdout, stderr=sys.stderr).returncode
    sys.exit(errcode)
