#!/usr/bin/env python3

import sys
import shutil
import os
import subprocess
import logging
import functools
import json
import tempfile
import time
import random
from argparse import ArgumentParser, RawDescriptionHelpFormatter


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
            # log the start and the end of the function call
            logger.debug("STARTED")
            # pass the logger to the decorated function
            result = func(logger, *args, **kwargs)
            logger.debug("FINISHED")
            return result

        return wrapper

    return func_decorator


def run_fdbcli_command(*args):
    """run the fdbcli statement: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Console output from fdbcli
    """
    commands = command_template + ["{}".format(" ".join(args))]
    process = subprocess.run(commands, stdout=subprocess.PIPE, env=fdbcli_env)
    return process.stdout.decode("utf-8").strip()


def run_fdbcli_command_and_get_error(*args):
    """run the fdbcli statement: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Stderr output from fdbcli
    """
    commands = command_template + ["{}".format(" ".join(args))]
    return (
        subprocess.run(
            commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env
        )
        .stderr.decode("utf-8")
        .strip()
    )


@enable_logging()
def advanceversion(logger):
    # get current read version
    version1 = int(run_fdbcli_command("getversion"))
    logger.debug("Read version: {}".format(version1))
    # advance version to a much larger value compared to the current version
    version2 = version1 * 10000
    logger.debug("Advanced to version: " + str(version2))
    run_fdbcli_command("advanceversion", str(version2))
    # after running the advanceversion command,
    # check the read version is advanced to the specified value
    version3 = int(run_fdbcli_command("getversion"))
    logger.debug("Read version: {}".format(version3))
    assert version3 >= version2
    # advance version to a smaller value compared to the current version
    # this should be a no-op
    run_fdbcli_command("advanceversion", str(version1))
    # get the current version to make sure the version did not decrease
    version4 = int(run_fdbcli_command("getversion"))
    logger.debug("Read version: {}".format(version4))
    assert version4 >= version3


@enable_logging()
def maintenance(logger):
    # expected fdbcli output when running 'maintenance' while there's no ongoing maintenance
    no_maintenance_output = "No ongoing maintenance."
    output1 = run_fdbcli_command("maintenance")
    assert output1 == no_maintenance_output
    # set maintenance on a fake zone id for 10 seconds
    run_fdbcli_command("maintenance", "on", "fake_zone_id", "10")
    # show current maintenance status
    output2 = run_fdbcli_command("maintenance")
    logger.debug("Maintenance status: " + output2)
    items = output2.split(" ")
    # make sure this specific zone id is under maintenance
    assert "fake_zone_id" in items
    logger.debug("Remaining time(seconds): " + items[-2])
    assert 0 < int(items[-2]) < 10
    # turn off maintenance
    run_fdbcli_command("maintenance", "off")
    # check maintenance status
    output3 = run_fdbcli_command("maintenance")
    assert output3 == no_maintenance_output


@enable_logging()
def quota(logger):
    # Should be a noop
    command = "quota clear green"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Successfully cleared quota."

    command = "quota get green total_throughput"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "<empty>"

    # Ignored update
    command = "quota set red total_throughput 49152"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Successfully updated quota."

    command = "quota set green total_throughput 32768"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Successfully updated quota."

    command = "quota set green reserved_throughput 16384"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Successfully updated quota."

    command = "quota set green storage 98765"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Successfully updated quota."

    command = "quota get green total_throughput"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "32768"

    command = "quota get green reserved_throughput"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "16384"

    command = "quota get green storage"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "98765"

    command = "quota clear green"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Successfully cleared quota."

    command = "quota get green total_throughput"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "<empty>"

    command = "quota get green storage"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "<empty>"

    # Too few arguments, should log help message
    command = "quota get green"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)


@enable_logging()
def setclass(logger):
    # get all processes' network addresses
    output1 = run_fdbcli_command("setclass")
    logger.debug(output1)
    # except the first line, each line is one process
    process_types = output1.split("\n")[1:]
    assert len(process_types) == args.process_number
    addresses = []
    for line in process_types:
        assert "127.0.0.1" in line
        # check class type
        assert "unset" in line
        # check class source
        assert "command_line" in line
        # check process' network address
        network_address = ":".join(line.split(":")[:2])
        logger.debug("Network address: {}".format(network_address))
        addresses.append(network_address)
    random_address = random.choice(addresses)
    logger.debug("Randomly selected address: {}".format(random_address))
    # set class to a random valid type
    class_types = [
        "storage",
        "transaction",
        "resolution",
        "commit_proxy",
        "grv_proxy",
        "master",
        "stateless",
        "log",
        "router",
        "cluster_controller",
        "fast_restore",
        "data_distributor",
        "coordinator",
        "ratekeeper",
        "backup",
    ]
    random_class_type = random.choice(class_types)
    logger.debug("Change to type: {}".format(random_class_type))
    run_fdbcli_command("setclass", random_address, random_class_type)
    # check the set successful
    output2 = run_fdbcli_command("setclass")
    logger.debug(output2)
    assert random_address in output2
    process_types = output2.split("\n")[1:]
    # check process' network address
    for line in process_types:
        if random_address in line:
            # check class type changed to the specified value
            assert random_class_type in line
            # check class source
            assert "set_class" in line
    # set back to unset
    run_fdbcli_command("setclass", random_address, "unset")
    # Attempt to set an invalid address and check error message
    output3 = run_fdbcli_command("setclass", "0.0.0.0:4000", "storage")
    logger.debug(output3)
    assert "No matching addresses found" in output3
    # Verify setclass did not execute
    output4 = run_fdbcli_command("setclass")
    logger.debug(output4)
    # except the first line, each line is one process
    process_types = output4.split("\n")[1:]
    assert len(process_types) == args.process_number
    addresses = []
    for line in process_types:
        assert "127.0.0.1" in line
        # check class type
        assert "unset" in line
        # check class source
        assert "command_line" in line or "set_class" in line


@enable_logging()
def lockAndUnlock(logger):
    # lock an unlocked database, should be successful
    output1 = run_fdbcli_command("lock")
    # UID is 32 bytes
    lines = output1.split("\n")
    lock_uid = lines[0][-32:]
    assert lines[1] == "Database locked."
    logger.debug("UID: {}".format(lock_uid))
    assert get_value_from_status_json(True, "cluster", "database_lock_state", "locked")
    # lock a locked database, should get the error code 1038
    output2 = run_fdbcli_command_and_get_error("lock")
    assert output2 == "ERROR: Database is locked (1038)"
    # unlock the database
    process = subprocess.Popen(
        command_template + ["unlock " + lock_uid],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    process.stdout.readline()
    # The randome passphrease we need to confirm to proceed the unlocking
    line2 = process.stdout.readline()
    logger.debug("Random passphrase: {}".format(line2))
    output3, err = process.communicate(input=line2)
    # No error and unlock was successful
    assert err is None
    assert output3.decode("utf-8").strip() == "Database unlocked."
    assert not get_value_from_status_json(
        True, "cluster", "database_lock_state", "locked"
    )


@enable_logging()
def kill(logger):
    output1 = run_fdbcli_command("kill")
    lines = output1.split("\n")
    assert len(lines) == 2
    assert lines[1].startswith("127.0.0.1:")
    address = lines[1]
    logger.debug("Address: {}".format(address))
    old_generation = get_value_from_status_json(False, "cluster", "generation")
    # This is currently an issue with fdbcli,
    # where you need to first run 'kill' to initialize processes' list
    # and then specify the certain process to kill
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    #
    output2, err = process.communicate(
        input="kill; kill {}; sleep 1\n".format(address).encode()
    )
    logger.debug(output2)
    # wait for a second for the cluster recovery
    time.sleep(1)
    new_generation = get_value_from_status_json(True, "cluster", "generation")
    logger.debug("Old: {}, New: {}".format(old_generation, new_generation))
    assert new_generation > old_generation


@enable_logging()
def killall(logger):
    # test is designed to make sure 'kill all' sends all requests simultaneously
    old_generation = get_value_from_status_json(False, "cluster", "generation")
    # This is currently an issue with fdbcli,
    # where you need to first run 'kill' to initialize processes' list
    # and then specify the certain process to kill
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    output, error = process.communicate(input="kill; kill all; sleep 1\n".encode())
    logger.debug(output)
    # wait for a second for the cluster recovery
    time.sleep(1)
    new_generation = get_value_from_status_json(True, "cluster", "generation")
    logger.debug(
        "Old generation: {}, New generation: {}".format(old_generation, new_generation)
    )
    # Make sure the kill is not happening sequentially
    # Pre: each recovery will increase the generated number by 2
    # Relax the condition to allow one additional recovery happening when we fetched the old generation
    assert new_generation <= (old_generation + 4)


@enable_logging()
def suspend(logger):
    if not shutil.which("pidof"):
        logger.debug("Skipping suspend test. Pidof not available")
        return
    output1 = run_fdbcli_command("suspend")
    lines = output1.split("\n")
    assert len(lines) == 2
    assert lines[1].startswith("127.0.0.1:")
    address = lines[1]
    logger.debug("Address: {}".format(address))
    db_available = get_value_from_status_json(
        False, "client", "database_status", "available"
    )
    assert db_available
    # use pgrep to get the pid of the fdb process
    pinfos = (
        subprocess.check_output(["pgrep", "-a", "fdbserver"])
        .decode()
        .strip()
        .split("\n")
    )
    port = address.split(":")[1]
    logger.debug("Port: {}".format(port))
    # use the port number to find the exact fdb process we are connecting to
    # child process like fdbserver -r flowprocess does not provide `datadir` in the command line
    pinfo = list(filter(lambda x: port in x and "datadir" in x, pinfos))
    assert len(pinfo) == 1
    pid = pinfo[0].split(" ")[0]
    logger.debug("Pid: {}".format(pid))
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    # suspend the process for enough long time
    output2, err = process.communicate(
        input="suspend; suspend 3600 {}; sleep 1\n".format(address).encode()
    )
    # the cluster should be unavailable after the only process being suspended
    assert not get_value_from_status_json(
        False, "client", "database_status", "available"
    )
    # check the process pid still exists
    pids = subprocess.check_output(["pidof", "fdbserver"]).decode().strip()
    logger.debug("PIDs: {}".format(pids))
    assert pid in pids
    # kill the process by pid
    kill_output = subprocess.check_output(["kill", pid]).decode().strip()
    logger.debug("Kill result: {}".format(kill_output))
    # The process should come back after a few time
    duration = 0  # seconds we already wait
    while (
        not get_value_from_status_json(False, "client", "database_status", "available")
        and duration < 60
    ):
        logger.debug("Sleep for 1 second to wait cluster recovery")
        time.sleep(1)
        duration += 1
    # at most after 60 seconds, the cluster should be available
    assert get_value_from_status_json(False, "client", "database_status", "available")


@enable_logging()
def versionepoch(logger):
    version1 = run_fdbcli_command("versionepoch")
    assert version1 == "Version epoch is unset"
    version2 = run_fdbcli_command("versionepoch get")
    assert version2 == "Version epoch is unset"
    version3 = run_fdbcli_command("versionepoch commit")
    assert (
        version3
        == "Must set the version epoch before committing it (see `versionepoch enable`)"
    )
    version4 = run_fdbcli_command("versionepoch enable")
    assert (
        version4
        == "Version epoch enabled. Run `versionepoch commit` to irreversibly jump to the target version"
    )
    version5 = run_fdbcli_command("versionepoch get")
    assert version5 == "Current version epoch is 0"
    version6 = run_fdbcli_command("versionepoch set 10")
    assert (
        version6
        == "Version epoch enabled. Run `versionepoch commit` to irreversibly jump to the target version"
    )
    version7 = run_fdbcli_command("versionepoch get")
    assert version7 == "Current version epoch is 10"
    run_fdbcli_command("versionepoch disable")
    version8 = run_fdbcli_command("versionepoch get")
    assert version8 == "Version epoch is unset"
    version9 = run_fdbcli_command("versionepoch enable")
    assert (
        version9
        == "Version epoch enabled. Run `versionepoch commit` to irreversibly jump to the target version"
    )
    version10 = run_fdbcli_command("versionepoch get")
    assert version10 == "Current version epoch is 0"
    version11 = run_fdbcli_command("versionepoch commit")
    assert version11.startswith("Current read version is ")
    # the test can trigger recovery, thus we wait until the recovery is finished to move to the next test
    wait_for_database_available(logger)


def get_value_from_status_json(retry, *args):
    while True:
        result = json.loads(run_fdbcli_command("status", "json"))
        if result["client"]["database_status"]["available"] or not retry:
            break
    for arg in args:
        assert arg in result
        result = result[arg]

    return result


@enable_logging()
def consistencycheck(logger):
    consistency_check_on_output = "ConsistencyCheck is on"
    consistency_check_off_output = "ConsistencyCheck is off"
    output1 = run_fdbcli_command("consistencycheck")
    assert output1 == consistency_check_on_output
    run_fdbcli_command("consistencycheck", "off")
    output2 = run_fdbcli_command("consistencycheck")
    assert output2 == consistency_check_off_output
    run_fdbcli_command("consistencycheck", "on")
    output3 = run_fdbcli_command("consistencycheck")
    assert output3 == consistency_check_on_output


@enable_logging()
def knobmanagement(logger):
    # this test will set knobs and verify that the knobs are properly set
    # must use begin/commit to avoid prompt for description

    # Incorrect arguments
    output = run_fdbcli_command("setknob")
    assert output == "Usage: setknob <KEY> <VALUE> [CONFIG_CLASS]"
    output = run_fdbcli_command("setknob", "min_trace_severity")
    assert output == "Usage: setknob <KEY> <VALUE> [CONFIG_CLASS]"
    output = run_fdbcli_command("getknob")
    assert output == "Usage: getknob <KEY> [CONFIG_CLASS]"
    logger.debug("incorrect args passed")

    # Invalid knob name
    err = run_fdbcli_command_and_get_error(
        'begin; setknob dummy_knob 20; commit "fdbcli change";'
    )
    logger.debug("err is: {}".format(err))
    assert len(err) > 0
    logger.debug("invalid knob name passed")

    # Invalid type for knob
    err = run_fdbcli_command_and_get_error(
        'begin; setknob min_trace_severity dummy-text; commit "fdbcli change";'
    )
    logger.debug("err is: {}".format(err))
    assert len(err) > 0
    logger.debug("invalid knob type passed")

    # Verifying we can't do a normal set, clear, get, getrange, clearrange
    # with a setknob
    err = run_fdbcli_command_and_get_error(
        "writemode on; begin; set foo bar; setknob max_metric_size 1000; commit;"
    )
    logger.debug("err is: {}".format(err))
    assert len(err) > 0

    err = run_fdbcli_command_and_get_error(
        "writemode on; begin; clear foo; setknob max_metric_size 1000; commit"
    )
    logger.debug("err is: {}".format(err))
    assert len(err) > 0

    # Various setknobs and verified by getknob
    output = run_fdbcli_command(
        'begin; setknob min_trace_severity 30; setknob max_metric_size 1000; \
                                setknob tracing_udp_listener_addr 192.168.0.1;                       \
                                setknob tracing_sample_rate 0.3;                                     \
                                commit "This is an fdbcli test for knobs";'
    )
    assert "Committed" in output
    output = run_fdbcli_command("getknob", "min_trace_severity")
    assert r"`min_trace_severity' is `30'" == output
    output = run_fdbcli_command("getknob", "max_metric_size")
    assert r"`max_metric_size' is `1000'" == output
    output = run_fdbcli_command("getknob", "tracing_udp_listener_addr")
    assert r"`tracing_udp_listener_addr' is `'192.168.0.1''" == output
    output = run_fdbcli_command("getknob", "tracing_sample_rate")
    assert r"`tracing_sample_rate' is `0.300000'" == output

    output = run_fdbcli_command(
        'begin; clearknob min_trace_severity; commit "fdbcli clearknob";'
    )
    assert "Committed" in output
    output = run_fdbcli_command("getknob", "min_trace_severity")
    assert r"`min_trace_severity' is not found" == output


@enable_logging()
def datadistribution(logger):
    output1 = run_fdbcli_command("datadistribution", "off")
    assert output1 == "Data distribution is turned off."
    output2 = run_fdbcli_command("datadistribution", "on")
    assert output2 == "Data distribution is turned on."
    output3 = run_fdbcli_command("datadistribution", "disable", "ssfailure")
    assert output3 == "Data distribution is disabled for storage server failures."
    # While we disable ssfailure, maintenance should fail
    error_msg = run_fdbcli_command_and_get_error(
        "maintenance", "on", "fake_zone_id", "1"
    )
    assert (
        error_msg
        == "ERROR: Maintenance mode cannot be used while data distribution is disabled for storage server failures. Use 'datadistribution on' to reenable data distribution."
    )
    output4 = run_fdbcli_command("datadistribution", "enable", "ssfailure")
    assert output4 == "Data distribution is enabled for storage server failures."
    output5 = run_fdbcli_command("datadistribution", "disable", "rebalance")
    assert output5 == "Data distribution is disabled for rebalance."
    output6 = run_fdbcli_command("datadistribution", "enable", "rebalance")
    assert output6 == "Data distribution is enabled for rebalance."
    time.sleep(1)


@enable_logging()
def transaction(logger):
    """This test will cover the transaction related fdbcli commands.
    In particular,
    'begin', 'rollback', 'option'
    'getversion', 'get', 'getrange', 'clear', 'clearrange', 'set', 'commit'
    """
    err1 = run_fdbcli_command_and_get_error("set", "key", "value")
    assert (
        err1 == "ERROR: writemode must be enabled to set or clear keys in the database."
    )
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    transaction_flow = [
        "writemode on",
        "begin",
        "getversion",
        "set key value",
        "get key",
        "commit",
    ]
    output1, _ = process.communicate(input="\n".join(transaction_flow).encode())
    # split the output into lines
    lines = list(filter(len, output1.decode().split("\n")))[-4:]
    assert lines[0] == "Transaction started"
    read_version = int(lines[1])
    logger.debug("Read version {}".format(read_version))
    # line[1] is the printed read version
    assert lines[2] == "`key' is `value'"
    assert lines[3].startswith("Committed (") and lines[3].endswith(")")
    # validate commit version is larger than the read version
    commit_verion = int(lines[3][len("Committed (") : -1])
    logger.debug("Commit version: {}".format(commit_verion))
    assert commit_verion >= read_version
    # check the transaction is committed
    output2 = run_fdbcli_command("get", "key")
    assert output2 == "`key' is `value'"
    # test rollback and read-your-write behavior
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    transaction_flow = [
        "writemode on",
        "begin",
        "getrange a z",
        "clear key",
        "get key",
        # 'option on READ_YOUR_WRITES_DISABLE', 'get key',
        "rollback",
    ]
    output3, _ = process.communicate(input="\n".join(transaction_flow).encode())
    lines = list(filter(len, output3.decode().split("\n")))[-5:]
    # lines[0] == "Transaction started" and lines[1] == 'Range limited to 25 keys'
    assert lines[2] == "`key' is `value'"
    assert lines[3] == "`key': not found"
    assert lines[4] == "Transaction rolled back"
    # make sure the rollback works
    output4 = run_fdbcli_command("get", "key")
    assert output4 == "`key' is `value'"
    # test read_your_write_disable option and clear the inserted key
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    transaction_flow = [
        "writemode on",
        "begin",
        "option on READ_YOUR_WRITES_DISABLE",
        "clear key",
        "get key",
        "commit",
    ]
    output6, _ = process.communicate(input="\n".join(transaction_flow).encode())
    lines = list(filter(len, output6.decode().split("\n")))[-4:]
    assert lines[1] == "Option enabled for current transaction"
    # the get key should still return the value even we clear it in the transaction
    assert lines[2] == "`key' is `value'"
    # Check the transaction is committed
    output7 = run_fdbcli_command("get", "key")
    assert output7 == "`key': not found"


def get_fdb_process_addresses(logger):
    # get all processes' network addresses
    output = run_fdbcli_command("kill")
    logger.debug(output)
    # except the first line, each line is one process
    addresses = output.split("\n")[1:]
    assert len(addresses) == args.process_number
    return addresses


@enable_logging(logging.DEBUG)
def coordinators(logger):
    # we should only have one coordinator for now
    output1 = run_fdbcli_command("coordinators")
    assert len(output1.split("\n")) > 2
    cluster_description = output1.split("\n")[0].split(": ")[-1]
    logger.debug("Cluster description: {}".format(cluster_description))
    coordinators = output1.split("\n")[1].split(": ")[-1]
    # verify the coordinator
    coordinator_list = get_value_from_status_json(
        True, "client", "coordinators", "coordinators"
    )
    assert len(coordinator_list) == 1
    assert coordinator_list[0]["address"] == coordinators
    # verify the cluster description
    assert get_value_from_status_json(True, "cluster", "connection_string").startswith(
        "{}:".format(cluster_description)
    )
    addresses = get_fdb_process_addresses(logger)
    # set all 5 processes as coordinators and update the cluster description
    new_cluster_description = "a_simple_description"
    run_fdbcli_command(
        "coordinators", *addresses, "description={}".format(new_cluster_description)
    )
    # verify now we have 5 coordinators and the description is updated
    output2 = run_fdbcli_command("coordinators")
    assert output2.split("\n")[0].split(": ")[-1] == new_cluster_description
    assert output2.split("\n")[1] == "Cluster coordinators ({}): {}".format(
        args.process_number, ",".join(addresses)
    )
    # auto change should go back to 1 coordinator
    run_fdbcli_command("coordinators", "auto")
    assert (
        len(get_value_from_status_json(True, "client", "coordinators", "coordinators"))
        == 1
    )
    wait_for_database_available(logger)


@enable_logging(logging.DEBUG)
def exclude(logger):
    # get all processes' network addresses
    addresses = get_fdb_process_addresses(logger)
    logger.debug("Cluster processes: {}".format(" ".join(addresses)))
    # There should be no excluded process for now
    no_excluded_process_output = (
        "There are currently no servers or localities excluded from the database."
    )
    output1 = run_fdbcli_command("exclude")
    assert no_excluded_process_output in output1
    # randomly pick one and exclude the process
    excluded_address = random.choice(addresses)
    # If we see "not enough space" error, use FORCE option to proceed
    # this should be a safe operation as we do not need any storage space for the test
    force = False
    # sometimes we need to retry the exclude
    while True:
        logger.debug("Excluding process: {}".format(excluded_address))
        if force:
            error_message = run_fdbcli_command_and_get_error(
                "exclude", "FORCE", excluded_address
            )
        else:
            error_message = run_fdbcli_command_and_get_error(
                "exclude", excluded_address
            )
        if error_message == "WARNING: {} is a coordinator!".format(excluded_address):
            # exclude coordinator will print the warning, verify the randomly selected process is the coordinator
            coordinator_list = get_value_from_status_json(
                True, "client", "coordinators", "coordinators"
            )
            assert len(coordinator_list) == 1
            assert coordinator_list[0]["address"] == excluded_address
            break
        elif (
            "ERROR: This exclude may cause the total available space in the cluster to drop below 10%."
            in error_message
        ):
            # exclude the process may cause the available space not enough
            # use FORCE option to ignore it and proceed
            assert not force
            force = True
            logger.debug("Use FORCE option to exclude the process")
        elif not error_message:
            break
        else:
            logger.debug("Error message: {}\n".format(error_message))
        logger.debug("Retry exclude after 1 second")
        time.sleep(1)
    output2 = run_fdbcli_command("exclude")
    assert (
        "There are currently 1 servers or localities being excluded from the database"
        in output2
    )
    assert excluded_address in output2
    run_fdbcli_command("include", excluded_address)
    # check the include is successful
    output4 = run_fdbcli_command("exclude")
    assert no_excluded_process_output in output4
    wait_for_database_available(logger)


# read the system key 'k', need to enable the option first


def read_system_key(k):
    output = run_fdbcli_command("option", "on", "READ_SYSTEM_KEYS;", "get", k)
    if "is" not in output:
        # key not present
        return None
    _, value = output.split(" is ")
    return value


@enable_logging()
def throttle(logger):
    # no throttled tags at the beginning
    no_throttle_tags_output = "There are no throttled tags"
    output = run_fdbcli_command("throttle", "list")
    logger.debug(output)
    assert output == no_throttle_tags_output
    # test 'throttle enable auto'
    run_fdbcli_command("throttle", "enable", "auto")
    # verify the change is applied by reading the system key
    # not an elegant way, may change later
    enable_flag = read_system_key("\\xff\\x02/throttledTags/autoThrottlingEnabled")
    assert enable_flag == "`1'"
    run_fdbcli_command("throttle", "disable", "auto")
    enable_flag = read_system_key("\\xff\\x02/throttledTags/autoThrottlingEnabled")
    # verify disabled
    assert enable_flag == "`0'"
    # TODO : test manual throttling, not easy to do now


def wait_for_database_available(logger):
    # sometimes the change takes some time to have effect and the database can be unavailable at that time
    # this is to wait until the database is available again
    while not get_value_from_status_json(
        True, "client", "database_status", "available"
    ):
        logger.debug("Database unavailable for now, wait for one second")
        time.sleep(1)


@enable_logging()
def profile(logger):
    # profile list should return the same list as kill
    addresses = get_fdb_process_addresses(logger)
    output1 = run_fdbcli_command("profile", "list")
    assert output1.split("\n") == addresses
    # check default output
    default_profile_client_get_output = (
        "Client profiling rate is set to default and size limit is set to default."
    )
    output2 = run_fdbcli_command("profile", "client", "get")
    assert output2 == default_profile_client_get_output
    # set rate and size limit
    run_fdbcli_command("profile", "client", "set", "0.5", "1GB")
    time.sleep(1)  # global config can take some time to sync
    output3 = run_fdbcli_command("profile", "client", "get")
    logger.debug(output3)
    output3_list = output3.split(" ")
    assert float(output3_list[6]) == 0.5
    # size limit should be 1GB
    assert output3_list[-1] == "1000000000."
    # change back to default value and check
    run_fdbcli_command("profile", "client", "set", "default", "default")
    time.sleep(1)  # global config can take some time to sync
    assert (
        run_fdbcli_command("profile", "client", "get")
        == default_profile_client_get_output
    )


@enable_logging()
def test_available(logger):
    duration = 0  # seconds we already wait
    while (
        not get_value_from_status_json(False, "client", "database_status", "available")
        and duration < 10
    ):
        logger.debug("Sleep for 1 second to wait cluster recovery")
        time.sleep(1)
        duration += 1
    if duration >= 10:
        logger.debug(run_fdbcli_command("status", "json"))
        assert False


@enable_logging()
def triggerddteaminfolog(logger):
    # this command is straightforward and only has one code path
    output = run_fdbcli_command("triggerddteaminfolog")
    assert output == "Triggered team info logging in data distribution."


def setup_tenants(tenants):
    command = "; ".join(["tenant create %s" % t for t in tenants])
    run_fdbcli_command(command)


def clear_database_and_tenants():
    run_fdbcli_command(
        'writemode on; option on SPECIAL_KEY_SPACE_ENABLE_WRITES; clearrange "" \\xff; clearrange \\xff\\xff/management/tenant/map/ \\xff\\xff/management/tenant/map0'
    )


def run_tenant_test(test_func):
    test_func()
    clear_database_and_tenants()


@enable_logging()
def tenant_create(logger):
    output1 = run_fdbcli_command("tenant create tenant")
    assert output1 == "The tenant `tenant' has been created"

    output = run_fdbcli_command("tenant create tenant2 tenant_group=tenant_group2")
    assert output == "The tenant `tenant2' has been created"

    output = run_fdbcli_command_and_get_error("tenant create tenant")
    assert output == "ERROR: A tenant with the given name already exists (2132)"


@enable_logging()
def tenant_delete(logger):
    setup_tenants(["tenant", "tenant2"])
    run_fdbcli_command("writemode on; usetenant tenant2; set tenant_test value")

    # delete a tenant while the fdbcli is using that tenant
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=fdbcli_env,
    )
    cmd_sequence = [
        "writemode on",
        "usetenant tenant",
        "tenant delete tenant",
        "get tenant_test",
        "defaulttenant",
        "usetenant tenant",
    ]
    output, error_output = process.communicate(input="\n".join(cmd_sequence).encode())

    lines = output.decode().strip().split("\n")[-6:]
    error_lines = error_output.decode().strip().split("\n")[-2:]
    assert lines[0] == "Using tenant `tenant'"
    assert lines[1] == "The tenant `tenant' has been deleted"
    assert (
        lines[2]
        == "WARNING: the active tenant was deleted. Use the `usetenant' or `defaulttenant'"
    )
    assert lines[3] == "command to choose a new tenant."
    assert error_lines[0] == "ERROR: Tenant does not exist (2131)"
    assert lines[5] == "Using the default tenant"
    assert error_lines[1] == "ERROR: Tenant `tenant' does not exist"

    # delete a non-empty tenant
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=fdbcli_env,
    )
    cmd_sequence = [
        "writemode on",
        "tenant delete tenant2",
        "usetenant tenant2",
        "clear tenant_test",
        "defaulttenant",
        "tenant delete tenant2",
    ]
    output, error_output = process.communicate(input="\n".join(cmd_sequence).encode())

    lines = output.decode().strip().split("\n")[-4:]
    error_lines = error_output.decode().strip().split("\n")[-1:]
    assert error_lines[0] == "ERROR: Cannot delete a non-empty tenant (2133)"
    assert lines[0] == "Using tenant `tenant2'"
    assert lines[1].startswith("Committed")
    assert lines[2] == "Using the default tenant"
    assert lines[3] == "The tenant `tenant2' has been deleted"

    # delete a non-existing tenant
    output = run_fdbcli_command_and_get_error("tenant delete tenant")
    assert output == "ERROR: Tenant does not exist (2131)"


@enable_logging()
def tenant_list(logger):
    output = run_fdbcli_command("tenant list")
    assert output == "The cluster has no tenants"

    setup_tenants(["tenant", "tenant2"])

    output = run_fdbcli_command("tenant list")
    assert output == "1. tenant\n  2. tenant2"

    output = run_fdbcli_command("tenant list a z limit=1")
    assert output == "1. tenant"

    output = run_fdbcli_command("tenant list a tenant2")
    assert output == "1. tenant"

    output = run_fdbcli_command("tenant list tenant2 z")
    assert output == "1. tenant2"

    output = run_fdbcli_command("tenant list a b")
    assert output == "The cluster has no tenants in the specified range"

    output = run_fdbcli_command_and_get_error("tenant list b a")
    assert output == "ERROR: end must be larger than begin"

    output = run_fdbcli_command_and_get_error("tenant list a b limit=12x")
    assert output == "ERROR: invalid limit `12x'"

    output = run_fdbcli_command_and_get_error("tenant list a b offset=13y")
    assert output == "ERROR: invalid offset `13y'"

    output = run_fdbcli_command_and_get_error("tenant list a b state=14z")
    assert output == "ERROR: unrecognized tenant state(s) `14z'."


@enable_logging()
def tenant_lock(logger):
    logger.debug("Create tenant")
    setup_tenants(["tenant"])

    logger.debug("Write test key")
    run_fdbcli_command("usetenant tenant; writemode on; set foo bar")
    logger.debug("Lock tenant in read-only mode")
    output = run_fdbcli_command("tenant lock tenant w")
    output = output.strip()
    logger.debug("output: {}".format(output))
    start_string = "Locked tenant `tenant' with UID `"
    assert output.startswith(start_string)
    assert output.endswith("'")
    uid_str = output[len(start_string) : -1]
    assert len(uid_str) == 32

    logger.debug("Verify tenant is readable")
    output = run_fdbcli_command("usetenant tenant; get foo").strip()
    logger.debug("output: {}".format(output))
    lines = output.split("\n")
    assert lines[-1] == "`foo' is `bar'"

    logger.debug("Verify tenant is NOT writeable")
    output = run_fdbcli_command_and_get_error(
        "usetenant tenant; writemode on; set foo bar2"
    ).strip()
    logger.debug("output: {}".format(output))
    assert output == "ERROR: Tenant is locked (2144)"

    logger.debug('Unlock tenant with UID "{}"'.format(uid_str))
    output = run_fdbcli_command("tenant unlock tenant {}".format(uid_str))
    logger.debug("output: {}".format(output.strip()))
    assert output.strip() == "Unlocked tenant `tenant'"

    logger.debug("Lock tenant in rw mode")
    output = run_fdbcli_command("tenant lock tenant rw {}".format(uid_str)).strip()
    logger.debug("output: {}".format(output))
    assert output == "Locked tenant `tenant' with UID `{}'".format(uid_str)

    logger.debug("Verify tenant is NOT readable")
    output = run_fdbcli_command_and_get_error("usetenant tenant; get foo").strip()
    logger.debug("output: {}".format(output))
    assert output == "ERROR: Tenant is locked (2144)"

    logger.debug("Verify tenant is NOT writeable")
    output = run_fdbcli_command_and_get_error(
        "usetenant tenant; writemode on; set foo bar2"
    ).strip()
    logger.debug("output: {}".format(output))
    assert output == "ERROR: Tenant is locked (2144)"

    logger.debug("Unlock tenant")
    output = run_fdbcli_command("tenant unlock tenant {}".format(uid_str))
    logger.debug("output: {}".format(output.strip()))
    assert output.strip() == "Unlocked tenant `tenant'"


@enable_logging()
def tenant_get(logger):
    setup_tenants(["tenant", "tenant2 tenant_group=tenant_group2"])

    output = run_fdbcli_command("tenant get tenant")
    lines = output.split("\n")
    assert len(lines) == 4
    assert lines[0].strip().startswith("id: ")
    assert lines[1].strip().startswith("prefix: ")
    assert lines[2].strip().startswith("name: ")
    assert lines[3].strip() == "lock state: unlocked"
    # id = lines[0].strip().removeprefix("id: ")
    # Workaround until Python 3.9+ for removeprefix
    id = lines[0].strip()[len("id: ") :]

    id_output = run_fdbcli_command("tenant getId {}".format(id))
    assert id_output == output

    output = run_fdbcli_command("tenant get tenant JSON")
    json_output = json.loads(output, strict=False)
    assert len(json_output) == 2
    assert "tenant" in json_output
    assert json_output["type"] == "success"
    assert len(json_output["tenant"]) == 4
    assert "id" in json_output["tenant"]
    assert "name" in json_output["tenant"]
    assert len(json_output["tenant"]["name"]) == 2
    assert "base64" in json_output["tenant"]["name"]
    assert "printable" in json_output["tenant"]["name"]
    assert "prefix" in json_output["tenant"]
    assert len(json_output["tenant"]["prefix"]) == 2
    assert "base64" in json_output["tenant"]["prefix"]
    assert "printable" in json_output["tenant"]["prefix"]
    assert "lock_state" in json_output["tenant"]
    assert json_output["tenant"]["lock_state"] == "unlocked"

    id_output = run_fdbcli_command("tenant getId {} JSON".format(id))
    assert id_output == output

    output = run_fdbcli_command("tenant get tenant2")
    lines = output.split("\n")
    assert len(lines) == 5
    assert lines[0].strip().startswith("id: ")
    assert lines[1].strip().startswith("prefix: ")
    assert lines[2].strip().startswith("name: ")
    assert lines[3].strip() == "lock state: unlocked"
    assert lines[4].strip() == "tenant group: tenant_group2"
    # id2 = lines[0].strip().removeprefix("id: ")
    # Workaround until Python 3.9+ for removeprefix
    id2 = lines[0].strip()[len("id: ") :]

    id_output = run_fdbcli_command("tenant getId {}".format(id2))
    assert id_output == output

    output = run_fdbcli_command("tenant get tenant2 JSON")
    json_output = json.loads(output, strict=False)
    assert len(json_output) == 2
    assert "tenant" in json_output
    assert json_output["type"] == "success"
    assert len(json_output["tenant"]) == 5
    assert "id" in json_output["tenant"]
    assert "name" in json_output["tenant"]
    assert len(json_output["tenant"]["name"]) == 2
    assert "base64" in json_output["tenant"]["name"]
    assert "printable" in json_output["tenant"]["name"]
    assert "prefix" in json_output["tenant"]
    assert "lock_state" in json_output["tenant"]
    assert json_output["tenant"]["lock_state"] == "unlocked"
    assert "tenant_group" in json_output["tenant"]
    assert len(json_output["tenant"]["tenant_group"]) == 2
    assert "base64" in json_output["tenant"]["tenant_group"]
    assert json_output["tenant"]["tenant_group"]["printable"] == "tenant_group2"

    id_output = run_fdbcli_command("tenant getId {} JSON".format(id2))
    assert id_output == output


@enable_logging()
def tenant_configure(logger):
    setup_tenants(["tenant"])

    output = run_fdbcli_command("tenant configure tenant tenant_group=tenant_group1")
    assert output == "The configuration for tenant `tenant' has been updated"

    output = run_fdbcli_command(
        "tenant configure tenant tenant_group=tenant_group1 ignore_capacity_limit"
    )
    assert output == "The configuration for tenant `tenant' has been updated"

    output = run_fdbcli_command("tenant get tenant")
    lines = output.split("\n")
    assert len(lines) == 5
    assert lines[4].strip() == "tenant group: tenant_group1"

    output = run_fdbcli_command("tenant configure tenant unset tenant_group")
    assert output == "The configuration for tenant `tenant' has been updated"

    output = run_fdbcli_command(
        "tenant configure tenant unset tenant_group ignore_capacity_limit"
    )
    assert output == "The configuration for tenant `tenant' has been updated"

    output = run_fdbcli_command("tenant get tenant")
    lines = output.split("\n")
    assert len(lines) == 4

    output = run_fdbcli_command_and_get_error(
        "tenant configure tenant tenant_group=tenant_group1 tenant_group=tenant_group2"
    )
    assert (
        output
        == "ERROR: configuration parameter `tenant_group' specified more than once."
    )

    output = run_fdbcli_command_and_get_error("tenant configure tenant unset")
    assert output == "ERROR: `unset' specified without a configuration parameter."

    output = run_fdbcli_command_and_get_error(
        "tenant configure tenant unset tenant_group=tenant_group1"
    )
    assert (
        output
        == "ERROR: unrecognized configuration parameter `tenant_group=tenant_group1'."
    )

    output = run_fdbcli_command_and_get_error("tenant configure tenant tenant_group")
    assert (
        output
        == "ERROR: invalid configuration string `tenant_group'. String must specify a value using `='."
    )

    output = run_fdbcli_command_and_get_error(
        "tenant configure tenant tenant_group=tenant1 unknown_token"
    )
    assert (
        output
        == "ERROR: invalid configuration string `unknown_token'. String must specify a value using `='."
    )

    output = run_fdbcli_command_and_get_error(
        "tenant configure tenant3 tenant_group=tenant_group1"
    )
    assert output == "ERROR: Tenant does not exist (2131)"

    expected_output = """
ERROR: assigned_cluster is only valid in metacluster configuration.
ERROR: Tenant configuration is invalid (2140)
    """.strip()
    output = run_fdbcli_command_and_get_error(
        "tenant configure tenant assigned_cluster=nonexist"
    )
    assert output == expected_output


@enable_logging()
def tenant_rename(logger):
    setup_tenants(["tenant", "tenant2"])

    output = run_fdbcli_command("tenant rename tenant tenant3")
    assert output == "The tenant `tenant' has been renamed to `tenant3'"

    output = run_fdbcli_command_and_get_error("tenant rename tenant tenant4")
    assert output == "ERROR: Tenant does not exist (2131)"

    output = run_fdbcli_command_and_get_error("tenant rename tenant2 tenant3")
    assert output == "ERROR: A tenant with the given name already exists (2132)"


@enable_logging()
def tenant_usetenant(logger):
    setup_tenants(["tenant", "tenant2"])

    output = run_fdbcli_command("usetenant")
    assert output == "Using the default tenant"

    output = run_fdbcli_command_and_get_error("usetenant tenant3")
    assert output == "ERROR: Tenant `tenant3' does not exist"

    # Test writing keys to different tenants and make sure they all work correctly
    run_fdbcli_command("writemode on; set tenant_test default_tenant")
    output = run_fdbcli_command("get tenant_test")
    assert output == "`tenant_test' is `default_tenant'"

    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    cmd_sequence = [
        "writemode on",
        "usetenant tenant",
        "get tenant_test",
        "set tenant_test tenant",
    ]
    output, _ = process.communicate(input="\n".join(cmd_sequence).encode())

    lines = output.decode().strip().split("\n")[-3:]
    assert lines[0] == "Using tenant `tenant'"
    assert lines[1] == "`tenant_test': not found"
    assert lines[2].startswith("Committed")

    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    cmd_sequence = [
        "writemode on",
        "usetenant tenant2",
        "get tenant_test",
        "set tenant_test tenant2",
        "get tenant_test",
    ]
    output, _ = process.communicate(input="\n".join(cmd_sequence).encode())

    lines = output.decode().strip().split("\n")[-4:]
    assert lines[0] == "Using tenant `tenant2'"
    assert lines[1] == "`tenant_test': not found"
    assert lines[2].startswith("Committed")
    assert lines[3] == "`tenant_test' is `tenant2'"

    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=fdbcli_env,
    )
    cmd_sequence = [
        "usetenant tenant",
        "get tenant_test",
        "usetenant tenant2",
        "get tenant_test",
        "defaulttenant",
        "get tenant_test",
    ]
    output, _ = process.communicate(input="\n".join(cmd_sequence).encode())

    lines = output.decode().strip().split("\n")[-6:]
    assert lines[0] == "Using tenant `tenant'"
    assert lines[1] == "`tenant_test' is `tenant'"
    assert lines[2] == "Using tenant `tenant2'"
    assert lines[3] == "`tenant_test' is `tenant2'"
    assert lines[4] == "Using the default tenant"
    assert lines[5] == "`tenant_test' is `default_tenant'"


@enable_logging()
def tenant_old_commands(logger):
    create_output = run_fdbcli_command("tenant create tenant")
    list_output = run_fdbcli_command("tenant list")
    get_output = run_fdbcli_command("tenant get tenant")
    # Run the gettenant command here because the ID will be different in the second block
    get_output_old = run_fdbcli_command("gettenant tenant")
    configure_output = run_fdbcli_command(
        "tenant configure tenant tenant_group=tenant_group1"
    )
    rename_output = run_fdbcli_command("tenant rename tenant tenant2")
    delete_output = run_fdbcli_command("tenant delete tenant2")

    create_output_old = run_fdbcli_command("createtenant tenant")
    list_output_old = run_fdbcli_command("listtenants")
    configure_output_old = run_fdbcli_command(
        "configuretenant tenant tenant_group=tenant_group1"
    )
    rename_output_old = run_fdbcli_command("renametenant tenant tenant2")
    delete_output_old = run_fdbcli_command("deletetenant tenant2")

    assert create_output == create_output_old
    assert list_output == list_output_old
    assert get_output == get_output_old
    assert configure_output == configure_output_old
    assert rename_output == rename_output_old
    assert delete_output == delete_output_old


@enable_logging()
def tenant_group_list(logger):
    output = run_fdbcli_command("tenantgroup list")
    assert output == "The cluster has no tenant groups"

    setup_tenants(
        [
            "tenant",
            "tenant2 tenant_group=tenant_group2",
            "tenant3 tenant_group=tenant_group3",
        ]
    )

    output = run_fdbcli_command("tenantgroup list")
    assert output == "1. tenant_group2\n  2. tenant_group3"

    output = run_fdbcli_command("tenantgroup list a z 1")
    assert output == "1. tenant_group2"

    output = run_fdbcli_command("tenantgroup list a tenant_group3")
    assert output == "1. tenant_group2"

    output = run_fdbcli_command("tenantgroup list tenant_group3 z")
    assert output == "1. tenant_group3"

    output = run_fdbcli_command("tenantgroup list a b")
    assert output == "The cluster has no tenant groups in the specified range"

    output = run_fdbcli_command_and_get_error("tenantgroup list b a")
    assert output == "ERROR: end must be larger than begin"

    output = run_fdbcli_command_and_get_error("tenantgroup list a b 12x")
    assert output == "ERROR: invalid limit `12x'"


@enable_logging()
def tenant_group_get(logger):
    setup_tenants(["tenant tenant_group=tenant_group"])

    output = run_fdbcli_command("tenantgroup get tenant_group")
    assert output == "The tenant group is present in the cluster"

    output = run_fdbcli_command("tenantgroup get tenant_group JSON")
    json_output = json.loads(output, strict=False)
    assert len(json_output) == 2
    assert "tenant_group" in json_output
    assert json_output["type"] == "success"
    assert len(json_output["tenant_group"]) == 0

    output = run_fdbcli_command_and_get_error("tenantgroup get tenant_group2")
    assert output == "ERROR: tenant group not found"

    output = run_fdbcli_command("tenantgroup get tenant_group2 JSON")
    json_output = json.loads(output, strict=False)
    assert len(json_output) == 2
    assert json_output["type"] == "error"
    assert json_output["error"] == "tenant group not found"


def tenants():
    run_tenant_test(tenant_create)
    run_tenant_test(tenant_delete)
    run_tenant_test(tenant_list)
    run_tenant_test(tenant_get)
    run_tenant_test(tenant_configure)
    run_tenant_test(tenant_rename)
    run_tenant_test(tenant_usetenant)
    run_tenant_test(tenant_old_commands)
    run_tenant_test(tenant_group_list)
    run_tenant_test(tenant_group_get)
    run_tenant_test(tenant_lock)


@enable_logging()
def idempotency_ids(logger):
    command = "idempotencyids status"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    json.loads(output)

    command = "idempotencyids clear 5"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Successfully cleared idempotency IDs.", output

    # Incorrect number of tokens
    command = "idempotencyids clear"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Usage: idempotencyids [status | clear <min_age_seconds>]", output

    # Incorrect number of tokens
    command = "idempotencyids"
    output = run_fdbcli_command(command)
    logger.debug(command + " : " + output)
    assert output == "Usage: idempotencyids [status | clear <min_age_seconds>]", output


def integer_options():
    process = subprocess.Popen(
        command_template[:-1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=fdbcli_env,
    )
    cmd_sequence = ["option on TIMEOUT 1000", "writemode on", "clear foo"]
    output, error_output = process.communicate(input="\n".join(cmd_sequence).encode())

    lines = output.decode().strip().split("\n")[-2:]
    assert lines[0] == "Option enabled for all transactions"
    assert lines[1].startswith("Committed")
    assert error_output == b""


def tls_address_suffix():
    # fdbcli shall prevent a non-TLS fdbcli run from connecting to an all-TLS cluster
    preamble = "eNW1yf1M:eNW1yf1M@"
    num_server_addrs = [1, 2, 5]
    err_output_server_tls = "ERROR: fdbcli is not configured with TLS, but all of the coordinators have TLS addresses."

    with tempfile.TemporaryDirectory() as tmpdir:
        cluster_fn = tmpdir + "/fdb.cluster"
        for num_server_addr in num_server_addrs:
            with open(cluster_fn, "w") as fp:
                fp.write(
                    preamble
                    + ",".join(
                        [
                            "127.0.0.1:{}:tls".format(4000 + addr_idx)
                            for addr_idx in range(num_server_addr)
                        ]
                    )
                )
                fp.close()
                fdbcli_process = subprocess.run(
                    command_template[:2] + [cluster_fn], capture_output=True
                )
                assert fdbcli_process.returncode != 0
                err_out = fdbcli_process.stderr.decode("utf8").strip()
                assert err_out == err_output_server_tls, f"unexpected output: {err_out}"


if __name__ == "__main__":
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="""
    The test calls fdbcli commands through fdbcli --exec "<command>" interactively using subprocess.
    The outputs from fdbcli are returned and compared to predefined results.
    Consequently, changing fdbcli outputs or breaking any commands will cause the test to fail.
    Commands that are easy to test will run against a single process cluster.
    For complex commands like exclude, they will run against a cluster with multiple(current set to 5) processes.
    If external_client_library is given, we will disable the local client and use the external client to run fdbcli.
    """,
    )
    parser.add_argument(
        "build_dir", metavar="BUILD_DIRECTORY", help="FDB build directory"
    )
    parser.add_argument("cluster_file", metavar="CLUSTER_FILE", help="FDB cluster file")
    parser.add_argument(
        "process_number",
        nargs="?",
        metavar="PROCESS_NUMBER",
        help="Number of fdb processes",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--external-client-library",
        "-e",
        metavar="EXTERNAL_CLIENT_LIBRARY_PATH",
        help="External client library path",
    )
    args = parser.parse_args()

    # keep current environment variables
    fdbcli_env = os.environ.copy()
    # set external client library if provided
    if args.external_client_library:
        # disable local client and use the external client library
        fdbcli_env["FDB_NETWORK_OPTION_DISABLE_LOCAL_CLIENT"] = ""
        fdbcli_env[
            "FDB_NETWORK_OPTION_EXTERNAL_CLIENT_LIBRARY"
        ] = args.external_client_library

    # shell command template
    command_template = [
        args.build_dir + "/bin/fdbcli",
        "-C",
        args.cluster_file,
        "--exec",
    ]
    # tests for fdbcli commands
    # assertions will fail if fdbcli does not work as expected
    test_available()
    if args.process_number == 1:
        # TODO: disable for now, the change can cause the database unavailable
        # advanceversion()
        consistencycheck()
        datadistribution()
        kill()
        lockAndUnlock()
        maintenance()
        profile()
        # TODO: reenable it until it's stable
        # suspend()
        transaction()
        # this is replaced by the "quota" command
        # throttle()
        triggerddteaminfolog()
        tenants()
        versionepoch()
        integer_options()
        tls_address_suffix()
        knobmanagement()
        # TODO: fix the issue when running through the external client
        # quota()
        idempotency_ids()
    else:
        assert args.process_number > 1, "Process number should be positive"
        coordinators()
        exclude()
        killall()
        # TODO: fix the failure where one process is not available after setclass call
        # setclass()
