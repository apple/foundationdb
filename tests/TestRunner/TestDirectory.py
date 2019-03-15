#!/usr/bin/env python3
import os
from datetime import datetime
from argparse import ArgumentParser


class TestDirectory:
    def __init__(self, builddir):
        self.builddir = builddir

    def get_test_root(self):
        root = os.path.join(self.builddir, 'test_runs')
        if not os.path.exists(root):
            os.mkdir(root)
        return root

    def create_new_test_dir(self):
        t = self.get_test_root()
        ts = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
        r = os.path.join(t, ts)
        os.mkdir(r)
        return r

    def get_current_test_dir(self):
        r = self.get_test_root()
        dirs = list(filter(lambda x: os.path.isdir(os.path.join(r, x)),
                           os.listdir(r)))
        dirs.sort()
        return os.path.join(r, dirs[-1])


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('builddir')
    args = parser.parse_args()
    td = TestDirectory(args.builddir)
    td.create_new_test_dir()
