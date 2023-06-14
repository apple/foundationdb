#!/usr/bin/env python3
import argparse
import functools
import logging
import os
import random
import re
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
                "[%(asctime)s] - %(filename)s:%(lineno)d - %(levelname)s - %(name)s - %(message)s"
            )
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
        rc, stdout, stderr from fdbcli
    """
    command_template = [fdbcli_bin, "-C", "{}".format(cluster_file), "--exec"]
    commands = command_template + ["{}".format(" ".join(args))]
    try:
        process = subprocess.run(
            commands,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=fdbcli_env,
            timeout=20,
        )
        rc = process.returncode
        out = process.stdout.decode("utf-8").strip()
        err = process.stderr.decode("utf-8").strip()
        return rc, out, err
    except subprocess.TimeoutExpired:
        raise Exception("The fdbcli command is stuck, database is unavailable")


def get_cluster_connection_str(cluster_file_path):
    with open(cluster_file_path, "r") as f:
        conn_str = f.readline().strip()
        return conn_str


@enable_logging()
def metacluster_create(logger, cluster_file, name, tenant_id_prefix):
    # creating a metacluster with optional tenant mode should fail
    rc, out, err = run_fdbcli_command(
        cluster_file, "configure tenant_mode=optional_experimental"
    )
    if rc != 0:
        raise Exception(err)
    rc, out, err = run_fdbcli_command(
        cluster_file, "metacluster create_experimental", name, str(tenant_id_prefix)
    )
    if "ERROR" not in out:
        raise Exception("Metacluster creation should have failed")
    # set the tenant mode to disabled for the metacluster otherwise creation will fail
    rc, out, err = run_fdbcli_command(cluster_file, "configure tenant_mode=disabled")
    logger.debug("Metacluster tenant mode set to disabled")
    if rc != 0:
        raise Exception(err)
    rc, out, err = run_fdbcli_command(
        cluster_file, "metacluster create_experimental", name, str(tenant_id_prefix)
    )
    if rc != 0:
        raise Exception(err)
    logger.debug(out)
    logger.debug("Metacluster {} created".format(name))


def metacluster_register(
    management_cluster_file,
    data_cluster_file,
    name,
    max_tenant_groups,
    auto_tenant_assignment,
):
    conn_str = get_cluster_connection_str(data_cluster_file)
    rc, out, err = run_fdbcli_command(
        management_cluster_file,
        "metacluster register",
        name,
        "connection_string={}".format(conn_str),
        "max_tenant_groups={}".format(max_tenant_groups),
        "auto_tenant_assignment={}".format(auto_tenant_assignment),
    )
    if rc != 0:
        raise Exception(err)


# Returns the tenant_id_prefix since it is randomly generated
# and we want to validate it in the status test
@enable_logging()
def setup_metacluster(
    logger, management_cluster, data_clusters, max_tenant_groups_per_cluster
):
    management_cluster_file = management_cluster[0]
    management_cluster_name = management_cluster[1]
    tenant_id_prefix = random.randint(0, 32767)
    logger.debug("management cluster: {}".format(management_cluster_name))
    logger.debug("data clusters: {}".format([name for (_, name, _) in data_clusters]))
    metacluster_create(
        management_cluster_file, management_cluster_name, tenant_id_prefix
    )
    cluster_names_to_files[management_cluster_name] = management_cluster_file
    for (cf, name, auto_assignment) in data_clusters:
        metacluster_register(
            management_cluster_file,
            cf,
            name,
            max_tenant_groups=max_tenant_groups_per_cluster,
            auto_tenant_assignment=auto_assignment,
        )
        cluster_names_to_files[name] = cf
    assert len(cluster_names_to_files) == len(data_clusters) + 1
    return tenant_id_prefix


def metacluster_status(cluster_file):
    _, out, _ = run_fdbcli_command(cluster_file, "metacluster status")
    return out


def remove_data_cluster(management_cluster_file, data_cluster_name):
    rc, out, err = run_fdbcli_command(
        management_cluster_file, "metacluster remove", data_cluster_name
    )
    return rc, out, err


def cleanup_after_test(management_cluster_file, data_cluster_names):
    for data_cluster_name in data_cluster_names:
        rc, out, err = remove_data_cluster(management_cluster_file, data_cluster_name)
        assert 0 == rc
    rc, out, err = run_fdbcli_command(
        management_cluster_file, "metacluster decommission"
    )
    assert 0 == rc


def create_tenant(
    management_cluster_file,
    tenant,
    tenant_group=None,
    assigned_cluster=None,
    ignore_capacity_limit=False,
):
    command = "tenant create {}".format(tenant)
    if tenant_group:
        command = command + " tenant_group={}".format(tenant_group)
    if assigned_cluster:
        command = command + " assigned_cluster={}".format(assigned_cluster)
    if ignore_capacity_limit:
        command = command + " ignore_capacity_limit"
    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def setup_tenants(management_cluster_file, tenant_creation_args):
    for tenant_arg in tenant_creation_args:
        assert len(tenant_arg["name"]) > 0
        tenant = tenant_arg["name"]
        tenant_group = tenant_arg.get("tenant_group", None)
        assigned_cluster = tenant_arg.get("assigned_cluster", None)
        output, err = create_tenant(
            management_cluster_file,
            tenant,
            tenant_group,
            assigned_cluster,
        )
        expected_output = "The tenant `{}' has been created".format(tenant)
        assert output == expected_output
        assert len(err) == 0


def configure_tenant(
    management_cluster_file,
    tenant,
    tenant_group=None,
    assigned_cluster=None,
    tenant_state=None,
):
    command = "tenant configure {}".format(tenant)
    if tenant_group:
        command = command + " tenant_group={}".format(tenant_group)
    if assigned_cluster:
        command = command + " assigned_cluster={}".format(assigned_cluster)
    if tenant_state:
        command = command + " tenant_state={}".format(tenant_state)

    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def list_tenants(
    management_cluster_file,
    tenant_name_begin=None,
    tenant_name_end=None,
):
    command = "tenant list"
    if tenant_name_begin:
        command = command + " {}".format(tenant_name_begin)
    if tenant_name_end:
        command = command + " {}".format(tenant_name_end)
    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def get_tenant_names(
    management_cluster_file,
    tenant_name_begin=None,
    tenant_name_end=None,
):
    command = "tenant list"
    if tenant_name_begin:
        command = command + " {}".format(tenant_name_begin)
    if tenant_name_end:
        command = command + " {}".format(tenant_name_end)
    rc, output, err = run_fdbcli_command(management_cluster_file, command)
    if rc != 0:
        return []
    print(output)
    res = []
    lines = output.split("\n")
    for ln in lines:
        m = re.match(r"^\d+\.\s+(?P<name>\w+)$", ln.strip())
        if m:
            res.append(m.group("name"))
    return res


def rename_tenant(management_cluster_file, old_name, new_name):
    command = "tenant rename {old_name} {new_name}".format(
        old_name=old_name, new_name=new_name
    )
    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def get_tenant(management_cluster_file, name):
    command = "tenant get {}".format(name)
    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def delete_tenant(management_cluster_file, name):
    command = "tenant delete {}".format(name)
    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def delete_tenant_by_id(management_cluster_file, id):
    command = "tenant deleteId {}".format(id)
    _, output, err = run_fdbcli_command(management_cluster_file, command)
    return output, err


def clear_all_tenants(management_cluster_file):
    all_tenants = get_tenant_names(management_cluster_file)
    for tenant in all_tenants:
        delete_tenant(management_cluster_file, tenant)


def put_kv_with_tenant(
    management_cluster_file, data_cluster_file, tenant_name, key, value
):
    subcmd1 = "usetenant {};".format(tenant_name)
    subcmd2 = "writemode on;"
    subcmd3 = "set {} {}".format(key, value)
    rc, out, err = run_fdbcli_command(data_cluster_file, subcmd1, subcmd2, subcmd3)
    return rc, out, err


def get_kv_with_tenant(management_cluster_file, data_cluster_file, tenant_name, key):
    subcmd1 = "usetenant {};".format(tenant_name)
    subcmd2 = "get {}".format(key)
    rc, out, err = run_fdbcli_command(data_cluster_file, subcmd1, subcmd2)
    if rc != 0 or len(err) > 0:
        raise Exception("Error executing {} {}".format(subcmd1, subcmd2))
    lines = out.split("\n")
    assert len(lines) > 0
    ln = lines[-1].strip()
    m = re.search(r"is `(?P<value>\S+)\'$", ln)
    if m:
        return m.group("value")
    return None


def clear_kv_with_tenant(management_cluster_file, data_cluster_file, tenant_name, key):
    subcmd1 = "usetenant {};".format(tenant_name)
    subcmd2 = "writemode on;"
    subcmd3 = "clear {}".format(key)
    rc, out, err = run_fdbcli_command(data_cluster_file, subcmd1, subcmd2, subcmd3)
    return rc, out, err


def clear_kv_range_with_tenant(
    management_cluster_file, data_cluster_file, tenant_name, begin_key, end_key
):
    subcmd1 = "usetenant {};".format(tenant_name)
    subcmd2 = "writemode on;"
    subcmd3 = "clearrange {} {}".format(begin_key, end_key)
    rc, out, err = run_fdbcli_command(data_cluster_file, subcmd1, subcmd2, subcmd3)
    return rc, out, err


@enable_logging()
def register_and_configure_data_clusters_test(logger, cluster_files):
    logger.debug("Setting up a metacluster")
    management_cluster_file = cluster_files[0]
    tenant_id_prefix = random.randint(0, 32767)
    logger.debug("management cluster: {}".format(management_cluster_name))
    metacluster_create(
        management_cluster_file, management_cluster_name, tenant_id_prefix
    )
    cluster_names_to_files[management_cluster_name] = management_cluster_file
    conn_str = get_cluster_connection_str(cluster_files[1])

    # Register a data cluster
    rc, _, err = run_fdbcli_command(
        management_cluster_file,
        "metacluster register",
        data_cluster_names[0],
        "connection_string={}".format(conn_str),
        "max_tenant_groups={}".format(5),
        "auto_tenant_assignment=disable",
    )
    assert rc != 0
    assert err == "ERROR: invalid configuration `disable' for `auto_tenant_assignment'."
    # Second attempt
    rc, _, err = run_fdbcli_command(
        management_cluster_file,
        "metacluster register",
        data_cluster_names[0],
        "connection_string={}".format(conn_str),
        "max_tenant_groups={}".format(5),
        "auto_tenant_assignment=enable",
    )
    assert rc != 0
    assert err == "ERROR: invalid configuration `enable' for `auto_tenant_assignment'."
    # Third attempt
    rc, out, err = run_fdbcli_command(
        management_cluster_file,
        "metacluster register",
        data_cluster_names[0],
        "connection_string={}".format(conn_str),
        "max_tenant_groups=5",
        "auto_tenant_assignment=disabled",
    )
    assert 0 == rc
    rc, out, err = run_fdbcli_command(
        management_cluster_file,
        "metacluster get",
        data_cluster_names[0],
    )
    assert rc == 0

    # Try creating a tenant without specifying data cluster
    out, err = create_tenant(management_cluster_file, "tenant1")
    assert (
        err == "ERROR: Metacluster does not have capacity to create new tenants (2166)"
    )
    out, err = create_tenant(
        management_cluster_file, "tenant1", assigned_cluster=data_cluster_names[0]
    )
    assert out == "The tenant `tenant1' has been created"
    assert len(err) == 0

    rc, out, err = run_fdbcli_command(
        management_cluster_file,
        "metacluster configure",
        data_cluster_names[0],
        "auto_tenant_assignment=enabled",
    )
    assert 0 == rc

    out, err = create_tenant(management_cluster_file, "tenant2")
    assert out == "The tenant `tenant2' has been created"
    assert len(err) == 0

    # clean up
    out, err = delete_tenant(management_cluster_file, "tenant1")
    assert len(err) == 0
    out, err = delete_tenant(management_cluster_file, "tenant2")
    assert len(err) == 0
    rc, out, err = remove_data_cluster(management_cluster_file, data_cluster_names[0])
    assert 0 == rc
    rc, out, err = run_fdbcli_command(
        management_cluster_file, "metacluster decommission"
    )
    assert 0 == rc


@enable_logging()
def clusters_status_test(logger, cluster_files, max_tenant_groups_per_cluster):
    logger.debug("Verifying no cluster is part of a metacluster")
    for cf in cluster_files:
        output = metacluster_status(cf)
        assert output == "This cluster is not part of a metacluster"

    logger.debug("Verified")
    num_clusters = len(cluster_files)
    logger.debug("Setting up a metacluster")
    auto_assignment = ["enabled"] * (num_clusters - 1)
    tenant_id_prefix = setup_metacluster(
        [cluster_files[0], management_cluster_name],
        list(zip(cluster_files[1:], data_cluster_names, auto_assignment)),
        max_tenant_groups_per_cluster=max_tenant_groups_per_cluster,
    )

    expected = """
number of data clusters: {}
  tenant group capacity: {}
  allocated tenant groups: 0
  tenant id prefix: {}
"""
    expected = expected.format(
        num_clusters - 1,
        (num_clusters - 1) * max_tenant_groups_per_cluster,
        tenant_id_prefix,
    ).strip()
    output = metacluster_status(cluster_files[0])
    assert expected == output

    logger.debug("Metacluster setup correctly")

    for (cf, name) in zip(cluster_files[1:], data_cluster_names):
        output = metacluster_status(cf)
        expected = 'This cluster "{}" is a data cluster within the metacluster named "{}"'.format(
            name, management_cluster_name
        )
        assert expected == output


@enable_logging()
def create_tenants_test(logger, cluster_files):
    logger.debug("Verifying no cluster is part of a metacluster")
    for cf in cluster_files:
        output = metacluster_status(cf)
        assert output == "This cluster is not part of a metacluster"
    logger.debug("Verified")
    num_clusters = len(cluster_files)
    logger.debug("Setting up a metacluster")
    auto_assignment = ["enabled"] * (num_clusters - 1)
    setup_metacluster(
        [cluster_files[0], management_cluster_name],
        list(zip(cluster_files[1:], data_cluster_names, auto_assignment)),
        max_tenant_groups_per_cluster=1,
    )

    # On data_cluster[0]
    output, err = create_tenant(
        cluster_files[0],
        "tenant1",
        tenant_group=None,
        assigned_cluster=None,
        ignore_capacity_limit=True,
    )
    assert (
        err
        == "ERROR: `ignore_capacity_limit' can only be used if `assigned_cluster' is set."
    )

    output, err = create_tenant(
        cluster_files[0],
        "tenant1",
        tenant_group="group1",
        assigned_cluster=data_cluster_names[0],
    )
    assert len(err) == 0
    output, err = create_tenant(
        cluster_files[0],
        "tenant11",
        tenant_group="group1",
        assigned_cluster=data_cluster_names[0],
    )
    assert len(err) == 0
    output, err = create_tenant(
        cluster_files[0],
        "tenant12",
        tenant_group=None,
        assigned_cluster=data_cluster_names[0],
    )
    assert (
        err
        == "ERROR: Cluster does not have capacity to perform the specified operation (2141)"
    )
    output, err = create_tenant(
        cluster_files[0],
        "tenant12",
        tenant_group=None,
        assigned_cluster=data_cluster_names[0],
        ignore_capacity_limit=True,
    )
    assert len(err) == 0
    output, err = create_tenant(
        cluster_files[0],
        "tenant13",
        tenant_group="group1",
        assigned_cluster=None,
        ignore_capacity_limit=True,
    )
    assert (
        err
        == "ERROR: `ignore_capacity_limit' can only be used if `assigned_cluster' is set."
    )

    all_tenants = get_tenant_names(cluster_files[0])
    assert all_tenants == ["tenant1", "tenant11", "tenant12"]

    clear_all_tenants(cluster_files[0])
    cleanup_after_test(cluster_files[0], data_cluster_names)


@enable_logging()
def list_tenants_test(logger, cluster_files):
    tenants = [
        {"name": "tenant10"},
        {"name": "tenant11"},
        {"name": "tenant2", "assigned_cluster": "data1"},
    ]
    setup_tenants(cluster_files[0], tenants)
    all_tenants = get_tenant_names(cluster_files[0])
    assert all_tenants == [tenant.get("name") for tenant in tenants]
    tenants1 = get_tenant_names(cluster_files[0], "a", "b")
    assert [] == tenants1
    tenants2 = get_tenant_names(cluster_files[0], "a", "tenant10")
    assert [] == tenants2
    tenants3 = get_tenant_names(cluster_files[0], "tenant1", "tenant2")
    assert ["tenant10", "tenant11"] == tenants3
    tenants4 = get_tenant_names(cluster_files[0], "tenant10", "tenant11")
    assert ["tenant10"] == tenants4
    tenants5 = get_tenant_names(cluster_files[0], "tenant10", "tenant10")
    assert [] == tenants5
    clear_all_tenants(cluster_files[0])


@enable_logging()
def delete_tenants_test(logger, cluster_files):
    tenants = [
        {"name": "tenant10", "assigned_cluster": "data1"},
        {"name": "tenant11"},
        {"name": "tenant2", "assigned_cluster": "data2"},
    ]
    setup_tenants(cluster_files[0], tenants)
    all_tenants = get_tenant_names(cluster_files[0])
    assert all_tenants == [tenant.get("name") for tenant in tenants]

    rc, out, _ = put_kv_with_tenant(
        cluster_files[0], cluster_files[1], "tenant10", "foo", "v0"
    )
    assert rc == 0

    value = get_kv_with_tenant(cluster_files[0], cluster_files[1], "tenant10", "foo")
    assert value == "v0"
    value = get_kv_with_tenant(cluster_files[0], cluster_files[1], "tenant10", "fo")
    assert value == None

    # Cannot delete non-empty tenant with data
    out, err = delete_tenant(cluster_files[0], "tenant10")
    assert err == "ERROR: Cannot delete a non-empty tenant (2133)"
    all_tenants = get_tenant_names(cluster_files[0])
    assert all_tenants == [tenant.get("name") for tenant in tenants]

    rc, out, err = clear_kv_range_with_tenant(
        cluster_files[0], cluster_files[1], "tenant10", '""', '"\\xff"'
    )
    assert rc == 0

    delete_tenant(cluster_files[0], "tenant10")
    all_tenants1 = get_tenant_names(cluster_files[0])
    assert all_tenants1 == ["tenant11", "tenant2"]
    delete_tenant(cluster_files[0], "tenant11")
    all_tenants2 = get_tenant_names(cluster_files[0])
    assert all_tenants2 == ["tenant2"]
    delete_tenant(cluster_files[0], "tenant2")
    all_tenants3 = get_tenant_names(cluster_files[0])
    assert all_tenants3 == []
    out, err = delete_tenant(cluster_files[0], "dontcare")
    assert err == "ERROR: Tenant does not exist (2131)"
    all_tenants4 = get_tenant_names(cluster_files[0])
    assert all_tenants4 == []
    clear_all_tenants(cluster_files[0])


@enable_logging()
def configure_tenant_group_test(logger, cluster_files):
    tenants = [
        {"name": "tenant1", "tenant_group": "group0", "assigned_cluster": "data1"},
        {"name": "tenant2"},
        {"name": "tenant3", "assigned_cluster": "data2"},
    ]
    setup_tenants(cluster_files[0], tenants)
    all_tenants = get_tenant_names(cluster_files[0])
    assert all_tenants == [tenant.get("name") for tenant in tenants]

    out, err = configure_tenant(cluster_files[0], "tenant2", tenant_group="group0")
    assert out == "The configuration for tenant `tenant2' has been updated"
    assert len(err) == 0

    # tenant group cannot span multiple data clusters
    out, err = configure_tenant(cluster_files[0], "tenant3", tenant_group="group0")
    assert len(out) == 0
    assert err == "ERROR: Tenant configuration is invalid (2140)"

    out, err = configure_tenant(cluster_files[0], "tenant1", tenant_group="group1")
    assert out == "The configuration for tenant `tenant1' has been updated"
    assert len(err) == 0

    out, err = configure_tenant(cluster_files[0], "tenant100", tenant_group="group0")
    assert len(out) == 0
    assert err == "ERROR: Tenant does not exist (2131)"

    clear_all_tenants(cluster_files[0])


@enable_logging()
def configure_tenants_test_disableClusterAssignment(logger, cluster_files):
    tenants = [{"name": "tenant1"}, {"name": "tenant2"}]
    logger.debug("Tenants to create: {}".format(tenants))
    setup_tenants(cluster_files[0], tenants)
    output, err = list_tenants(cluster_files[0])
    assert "1. tenant1\n  2. tenant2" == output
    names = get_tenant_names(cluster_files[0])
    assert ["tenant1", "tenant2"] == names
    # Once we reach here, the tenants have been created successfully
    logger.debug("Tenants created: {}".format(tenants))
    for tenant in tenants:
        out, err = configure_tenant(
            cluster_files[0],
            tenant["name"],
            assigned_cluster="cluster",
        )
        assert err == "ERROR: Tenant configuration is invalid (2140)"
    logger.debug("Tenants configured")
    clear_all_tenants(cluster_files[0])
    logger.debug("Tenants cleared")


@enable_logging()
def configure_tenants_test_disableConfigureTenantState(logger, cluster_files):
    tenants = [{"name": "tenant1"}, {"name": "tenant2"}]
    logger.debug("Tenants to create: {}".format(tenants))
    setup_tenants(cluster_files[0], tenants)
    output, err = list_tenants(cluster_files[0])
    assert "1. tenant1\n  2. tenant2" == output
    names = get_tenant_names(cluster_files[0])
    assert ["tenant1", "tenant2"] == names
    # Once we reach here, the tenants have been created successfully
    logger.debug("Tenants created: {}".format(tenants))
    disallowed_states = [
        "registering",
        "removing",
        '"updating configuration"',
        "renaming",
        "error",
    ]
    for tenant_state in disallowed_states:
        out, err = configure_tenant(
            cluster_files[0],
            "tenant1",
            tenant_state=tenant_state,
        )
        expected_err_msg = str()
        if tenant_state != '"updating configuration"':
            expected_err_msg = "ERROR: only support setting tenant state back to `ready', but `{}' given.".format(
                tenant_state
            )
        else:
            expected_err_msg = "ERROR: only support setting tenant state back to `ready', but `updating configuration' given."
        assert err == expected_err_msg

    # Cannot configure tenant state together with other configurations
    out, err = configure_tenant(
        cluster_files[0], "tenant2", tenant_state="ready", tenant_group="group1"
    )
    assert err == "ERROR: Tenant configuration is invalid (2140)"
    out, err = configure_tenant(cluster_files[0], "tenant2", tenant_state="ready")
    assert len(err) == 0
    clear_all_tenants(cluster_files[0])
    logger.debug("Tenants cleared")


@enable_logging()
def test_main(logger):
    logger.debug("Tests start")
    register_and_configure_data_clusters_test(cluster_files)

    create_tenants_test(cluster_files)

    clusters_status_test(cluster_files, max_tenant_groups_per_cluster=5)

    configure_tenants_test_disableClusterAssignment(cluster_files)

    configure_tenants_test_disableConfigureTenantState(cluster_files)

    list_tenants_test(cluster_files)

    delete_tenants_test(cluster_files)

    configure_tenant_group_test(cluster_files)

    cleanup_after_test(cluster_files[0], data_cluster_names)
    logger.debug("Tests complete")


if __name__ == "__main__":
    print("metacluster_fdbcli_tests")
    script_desc = """
    This script executes a series of commands on multiple clusters within an FDB metacluster.
    """

    parser = argparse.ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter, description=script_desc
    )

    parser.add_argument(
        "build_dir", metavar="BUILD_DIRECTORY", help="FDB build directory"
    )
    args = parser.parse_args()

    # keep current environment variables
    fdbcli_env = os.environ.copy()
    cluster_files = fdbcli_env.get("FDB_CLUSTERS").split(";")
    assert len(cluster_files) > 2

    fdbcli_bin = args.build_dir + "/bin/fdbcli"

    cluster_names_to_files = {}
    management_cluster_name = "meta_mgmt"
    data_cluster_names = ["data{}".format(i) for i in range(1, len(cluster_files))]

    test_main()
