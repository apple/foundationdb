#!/usr/bin/env python3
import argparse
import os
import subprocess

from argparse import ArgumentParser, RawDescriptionHelpFormatter

def run_fdbcli_command(cluster_file, *args):
    command_template = [fdbcli_bin, '-C', "{}".format(cluster_file), '--exec']
    commands = command_template + ["{}".format(' '.join(args))]
    print(commands)
    try:
        # if the fdbcli command is stuck for more than 20 seconds, the database is definitely unavailable
        process = subprocess.run(commands, stdout=subprocess.PIPE, env=fdbcli_env, timeout=20)
        return process.stdout.decode('utf-8').strip()
    except subprocess.TimeoutExpired:
        raise Exception('The fdbcli command is stuck, database is unavailable')

if __name__ == "__main__":
    print("metacluster_fdbcli_tests")
    script_desc = """
    This script executes a series of commands on multiple clusters within an FDB metacluster.
    """

    parser = argparse.ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
                                     description=script_desc)

    parser.add_argument('build_dir', metavar='BUILD_DIRECTORY', help='FDB build directory')
    args = parser.parse_args()

    # keep current environment variables
    fdbcli_env = os.environ.copy()
    cluster_files = fdbcli_env.get("FDB_CLUSTERS").split(';')
    assert len(cluster_files) > 0

    fdbcli_bin = args.build_dir + '/bin/fdbcli'

    for cf in cluster_files:
        output = run_fdbcli_command(cf, "metacluster status")
        print("{}".format(output))
