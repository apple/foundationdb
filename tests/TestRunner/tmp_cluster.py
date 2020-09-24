#!/usr/bin/env python3

from local_cluster import LocalCluster
from argparse import ArgumentParser
from random import choice
from pathlib import Path
import shutil
import subprocess
import sys

class TempCluster:
    def __init__(self, build_dir: str):
        self.build_dir = Path(build_dir)
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
    parser = ArgumentParser()
    parser.add_argument('--build-dir', metavar='BUILD_DIRECTORY', help='FDB build directory', required=True)
    parser.add_argument('cmd', metavar="COMMAND", help="Command to run", nargs='+')
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
