#!/usr/bin/env python3
import argparse
import os
import subprocess

from argparse import RawDescriptionHelpFormatter


def run_command(*args):
    commands = ["{}".format(args)]
    print(commands)
    try:
        process = subprocess.run(commands, stdout=subprocess.PIPE, env=fdbcli_env, timeout=20)
        return process.stdout.decode('utf-8').strip()
    except subprocess.TimeoutExpired:
        raise Exception('the command is stuck')


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


def get_cluster_connection_str(cluster_file_path):
    with open(cluster_file_path, 'r') as f:
        conn_str = f.readline().strip()
        return conn_str


def metacluster_create(cluster_file, name):
    return run_fdbcli_command(cluster_file, "metacluster create_experimental", name)


def metacluster_register(management_cluster_file, data_cluster_file, name):
    conn_str = get_cluster_connection_str(data_cluster_file)
    return run_fdbcli_command(management_cluster_file, "metacluster register", name, "connection_string={}".format(
        conn_str))


def metacluster_status(cluster_file):
    return run_fdbcli_command(cluster_file, "metacluster status")


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
    N = len(cluster_files)
    assert len(cluster_files) > 1

    fdbcli_bin = args.build_dir + '/bin/fdbcli'

    for cf in cluster_files:
        output = metacluster_status(cf)
        assert output == "This cluster is not part of a metacluster"

    names = ['meta_mgmt']
    names.extend(['data{}'.format(i) for i in range(1, N)])

    metacluster_create(cluster_files[0], names[0])
    for (cf, name) in zip(cluster_files[1:], names[1:]):
        output = metacluster_register(cluster_files[0], cf, name)

    expected = """
number of data clusters: {}
  tenant group capacity: 0
  allocated tenant groups: 0
"""
    expected = expected.format(N - 1).strip()
    output = metacluster_status(cluster_files[0])
    assert expected == output

    for (cf, name) in zip(cluster_files[1:], names[1:]):
        output = metacluster_status(cf)
        expected = "This cluster \"{}\" is a data cluster within the metacluster whose management cluster is \"{" \
                   "}\"".format(name, names[0])
        assert expected == output
