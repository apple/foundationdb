#!/usr/bin/env python3
import argparse
import functools
import logging
import os
import random
import subprocess
from argparse import RawDescriptionHelpFormatter


# TODO: deduplicate with fdbcli_tests.py
def enable_logging(level=logging.DEBUG):
    """Enable logging in the function with the specified logging level

    Args:
        level (logging.<level>, optional): logging level for the decorated function. Defaults to logging.ERROR.
    """

    def func_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # initialize logger
            logger = logging.getLogger(func.__name__)
            logger.setLevel(level)
            # set logging format
            handler = logging.StreamHandler()
            handler_format = logging.Formatter(
                '[%(asctime)s] - %(filename)s:%(lineno)d - %(levelname)s - %(name)s - %(message)s')
            handler.setFormatter(handler_format)
            handler.setLevel(level)
            logger.addHandler(handler)
            # pass the logger to the decorated function
            result = func(logger, *args, **kwargs)
            return result

        return wrapper

    return func_decorator


def run_fdbcli_command(cluster_file, *args):
    """run the fdbcli statement: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Stderr output from fdbcli
    """
    command_template = [fdbcli_bin, '-C', "{}".format(cluster_file), '--exec']
    commands = command_template + ["{}".format(' '.join(args))]
    try:
        process = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env, timeout=20)
        rc = process.returncode
        out = process.stdout.decode('utf-8').strip()
        err = process.stderr.decode('utf-8').strip()
        return rc, out, err
    except subprocess.TimeoutExpired:
        raise Exception('The fdbcli command is stuck, database is unavailable')


def get_cluster_connection_str(cluster_file_path):
    with open(cluster_file_path, 'r') as f:
        conn_str = f.readline().strip()
        return conn_str


@enable_logging()
def metacluster_create(logger, cluster_file, name, tenant_id_prefix):
    rc, out, err = run_fdbcli_command(cluster_file, "metacluster create_experimental", name, str(tenant_id_prefix))
    if rc != 0:
        raise Exception(err)
    logger.debug(out)
    logger.debug('Metacluster {} created'.format(name))


def metacluster_register(management_cluster_file, data_cluster_file, name, max_tenant_groups):
    conn_str = get_cluster_connection_str(data_cluster_file)
    rc, out, err = run_fdbcli_command(management_cluster_file,
                       "metacluster register",
                       name,
                       "connection_string={}".format(conn_str),
                       'max_tenant_groups={}'.format(max_tenant_groups))
    if rc != 0:
        raise Exception(err)


@enable_logging()
def setup_metacluster(logger, management_cluster, data_clusters, max_tenant_groups_per_cluster):
    management_cluster_file = management_cluster[0]
    management_cluster_name = management_cluster[1]
    tenant_id_prefix = random.randint(0, 32767)
    logger.debug('management cluster: {}'.format(management_cluster_name))
    logger.debug('data clusters: {}'.format([name for (cf, name) in data_clusters]))
    metacluster_create(management_cluster_file, management_cluster_name, tenant_id_prefix)
    for (cf, name) in data_clusters:
        metacluster_register(management_cluster_file, cf, name, max_tenant_groups=max_tenant_groups_per_cluster)


def metacluster_status(cluster_file):
    _, out, _ = run_fdbcli_command(cluster_file, "metacluster status")
    return out


def setup_tenants(management_cluster_file, data_cluster_files, tenants):
    for tenant in tenants:
        _, output, _ = run_fdbcli_command(management_cluster_file, 'tenant create', tenant)
        expected_output = 'The tenant `{}\' has been created'.format(tenant)
        assert output == expected_output


def configure_tenant(management_cluster_file, data_cluster_files, tenant, tenant_group=None, assigned_cluster=None):
    command = 'tenant configure {}'.format(tenant)
    if tenant_group:
        command = command + ' tenant_group={}'.format(tenant_group)
    if assigned_cluster:
        command = command + ' assigned_cluster={}'.format(assigned_cluster)

    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def clear_database_and_tenants(management_cluster_file, data_cluster_files):
    subcmd1 = 'writemode on'
    subcmd2 = 'option on SPECIAL_KEY_SPACE_ENABLE_WRITES'
    subcmd3 = 'clearrange ' '"" \\xff'
    subcmd4 = 'clearrange \\xff\\xff/management/tenant/map/ \\xff\\xff/management/tenant/map0'
    run_fdbcli_command(management_cluster_file, subcmd1, subcmd2, subcmd3, subcmd4)


@enable_logging()
def clusters_status_test(logger, cluster_files, max_tenant_groups_per_cluster):
    logger.debug('Start')
    for cf in cluster_files:
        output = metacluster_status(cf)
        assert output == "This cluster is not part of a metacluster"

    num_clusters = len(cluster_files)
    names = ['meta_mgmt']
    names.extend(['data{}'.format(i) for i in range(1, num_clusters)])
    setup_metacluster([cluster_files[0], names[0]], list(zip(cluster_files[1:], names[1:])),
                      max_tenant_groups_per_cluster=max_tenant_groups_per_cluster)

    expected = """
number of data clusters: {}
  tenant group capacity: {}
  allocated tenant groups: 0
"""
    expected = expected.format(num_clusters - 1, (num_clusters - 1) * max_tenant_groups_per_cluster).strip()
    output = metacluster_status(cluster_files[0])
    assert expected == output

    for (cf, name) in zip(cluster_files[1:], names[1:]):
        output = metacluster_status(cf)
        expected = "This cluster \"{}\" is a data cluster within the metacluster named \"{}\"".format(name, names[0])
        assert expected == output


@enable_logging()
def configure_tenants_test_disableClusterAssignment(logger, cluster_files):
    tenants = ['tenant1', 'tenant2']
    logger.debug('Tenants to create: {}'.format(tenants))
    setup_tenants(cluster_files[0], cluster_files[1:], tenants)
    # Once we reach here, the tenants have been created successfully
    logger.debug('Tenants created: {}'.format(tenants))
    for tenant in tenants:
        out, err = configure_tenant(cluster_files[0], cluster_files[1:], tenant, assigned_cluster='cluster')
        assert err == 'ERROR: Tenant configuration is invalid (2140)'
    logger.debug('Tenants configured')
    clear_database_and_tenants(cluster_files[0], cluster_files[1:])
    logger.debug('Tenants cleared')


@enable_logging()
def test_main(logger):
    logger.debug('Tests start')
    # This must be the first test to run, since it sets up the metacluster that
    # will be used throughout the test
    clusters_status_test(cluster_files, max_tenant_groups_per_cluster=5)

    configure_tenants_test_disableClusterAssignment(cluster_files)
    logger.debug('Tests complete')


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
    assert len(cluster_files) > 1

    fdbcli_bin = args.build_dir + '/bin/fdbcli'

    test_main()
