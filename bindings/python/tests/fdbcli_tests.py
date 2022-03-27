#!/usr/bin/env python3

import sys
import shutil
import os
import subprocess
import logging
import functools
import json
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
            handler_format = logging.Formatter('[%(asctime)s] - %(filename)s:%(lineno)d - %(levelname)s - %(name)s - %(message)s')
            handler.setFormatter(handler_format)
            handler.setLevel(level)
            logger.addHandler(handler)
            # pass the logger to the decorated function
            result = func(logger, *args, **kwargs)
            return result
        return wrapper
    return func_decorator


def run_fdbcli_command(*args):
    """run the fdbcli statement: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Console output from fdbcli
    """
    commands = command_template + ["{}".format(' '.join(args))]
    return subprocess.run(commands, stdout=subprocess.PIPE, env=fdbcli_env).stdout.decode('utf-8').strip()


def run_fdbcli_command_and_get_error(*args):
    """run the fdbcli statement: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Stderr output from fdbcli
    """
    commands = command_template + ["{}".format(' '.join(args))]
    return subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env).stderr.decode('utf-8').strip()


@enable_logging()
def advanceversion(logger):
    # get current read version
    version1 = int(run_fdbcli_command('getversion'))
    logger.debug("Read version: {}".format(version1))
    # advance version to a much larger value compared to the current version
    version2 = version1 * 10000
    logger.debug("Advanced to version: " + str(version2))
    run_fdbcli_command('advanceversion', str(version2))
    # after running the advanceversion command,
    # check the read version is advanced to the specified value
    version3 = int(run_fdbcli_command('getversion'))
    logger.debug("Read version: {}".format(version3))
    assert version3 >= version2
    # advance version to a smaller value compared to the current version
    # this should be a no-op
    run_fdbcli_command('advanceversion', str(version1))
    # get the current version to make sure the version did not decrease
    version4 = int(run_fdbcli_command('getversion'))
    logger.debug("Read version: {}".format(version4))
    assert version4 >= version3


@enable_logging()
def maintenance(logger):
    # expected fdbcli output when running 'maintenance' while there's no ongoing maintenance
    no_maintenance_output = 'No ongoing maintenance.'
    output1 = run_fdbcli_command('maintenance')
    assert output1 == no_maintenance_output
    # set maintenance on a fake zone id for 10 seconds
    run_fdbcli_command('maintenance', 'on', 'fake_zone_id', '10')
    # show current maintenance status
    output2 = run_fdbcli_command('maintenance')
    logger.debug("Maintenance status: " + output2)
    items = output2.split(' ')
    # make sure this specific zone id is under maintenance
    assert 'fake_zone_id' in items
    logger.debug("Remaining time(seconds): " + items[-2])
    assert 0 < int(items[-2]) < 10
    # turn off maintenance
    run_fdbcli_command('maintenance', 'off')
    # check maintenance status
    output3 = run_fdbcli_command('maintenance')
    assert output3 == no_maintenance_output


@enable_logging()
def setclass(logger):
    # get all processes' network addresses
    output1 = run_fdbcli_command('setclass')
    logger.debug(output1)
    # except the first line, each line is one process
    process_types = output1.split('\n')[1:]
    assert len(process_types) == args.process_number
    addresses = []
    for line in process_types:
        assert '127.0.0.1' in line
        # check class type
        assert 'unset' in line
        # check class source
        assert 'command_line' in line
        # check process' network address
        network_address = ':'.join(line.split(':')[:2])
        logger.debug("Network address: {}".format(network_address))
        addresses.append(network_address)
    random_address = random.choice(addresses)
    logger.debug("Randomly selected address: {}".format(random_address))
    # set class to a random valid type
    class_types = ['storage', 'transaction', 'resolution',
                   'commit_proxy', 'grv_proxy', 'master', 'stateless', 'log',
                   'router', 'cluster_controller', 'fast_restore', 'data_distributor',
                   'coordinator', 'ratekeeper', 'storage_cache', 'backup'
                   ]
    random_class_type = random.choice(class_types)
    logger.debug("Change to type: {}".format(random_class_type))
    run_fdbcli_command('setclass', random_address, random_class_type)
    # check the set successful
    output2 = run_fdbcli_command('setclass')
    logger.debug(output2)
    assert random_address in output2
    process_types = output2.split('\n')[1:]
    # check process' network address
    for line in process_types:
        if random_address in line:
            # check class type changed to the specified value
            assert random_class_type in line
            # check class source
            assert 'set_class' in line
    # set back to unset
    run_fdbcli_command('setclass', random_address, 'unset')
    # Attempt to set an invalid address and check error message
    output3 = run_fdbcli_command('setclass', '0.0.0.0:4000', 'storage')
    logger.debug(output3)
    assert 'No matching addresses found' in output3
    # Verify setclass did not execute
    output4 = run_fdbcli_command('setclass')
    logger.debug(output4)
    # except the first line, each line is one process
    process_types = output4.split('\n')[1:]
    assert len(process_types) == args.process_number
    addresses = []
    for line in process_types:
        assert '127.0.0.1' in line
        # check class type
        assert 'unset' in line
        # check class source
        assert 'command_line' in line or 'set_class' in line


@enable_logging()
def lockAndUnlock(logger):
    # lock an unlocked database, should be successful
    output1 = run_fdbcli_command('lock')
    # UID is 32 bytes
    lines = output1.split('\n')
    lock_uid = lines[0][-32:]
    assert lines[1] == 'Database locked.'
    logger.debug("UID: {}".format(lock_uid))
    assert get_value_from_status_json(True, 'cluster', 'database_lock_state', 'locked')
    # lock a locked database, should get the error code 1038
    output2 = run_fdbcli_command_and_get_error("lock")
    assert output2 == 'ERROR: Database is locked (1038)'
    # unlock the database
    process = subprocess.Popen(command_template + ['unlock ' + lock_uid], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    line1 = process.stdout.readline()
    # The randome passphrease we need to confirm to proceed the unlocking
    line2 = process.stdout.readline()
    logger.debug("Random passphrase: {}".format(line2))
    output3, err = process.communicate(input=line2)
    # No error and unlock was successful
    assert err is None
    assert output3.decode('utf-8').strip() == 'Database unlocked.'
    assert not get_value_from_status_json(True, 'cluster', 'database_lock_state', 'locked')


@enable_logging()
def kill(logger):
    output1 = run_fdbcli_command('kill')
    lines = output1.split('\n')
    assert len(lines) == 2
    assert lines[1].startswith('127.0.0.1:')
    address = lines[1]
    logger.debug("Address: {}".format(address))
    old_generation = get_value_from_status_json(False, 'cluster', 'generation')
    # This is currently an issue with fdbcli,
    # where you need to first run 'kill' to initialize processes' list
    # and then specify the certain process to kill
    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    #
    output2, err = process.communicate(input='kill; kill {}; sleep 1\n'.format(address).encode())
    logger.debug(output2)
    # wait for a second for the cluster recovery
    time.sleep(1)
    new_generation = get_value_from_status_json(True, 'cluster', 'generation')
    logger.debug("Old: {}, New: {}".format(old_generation, new_generation))
    assert new_generation > old_generation


@enable_logging()
def suspend(logger):
    if not shutil.which("pidof"):
        logger.debug("Skipping suspend test. Pidof not available")
        return
    output1 = run_fdbcli_command('suspend')
    lines = output1.split('\n')
    assert len(lines) == 2
    assert lines[1].startswith('127.0.0.1:')
    address = lines[1]
    logger.debug("Address: {}".format(address))
    db_available = get_value_from_status_json(False, 'client', 'database_status', 'available')
    assert db_available
    # use pgrep to get the pid of the fdb process
    pinfos = subprocess.check_output(['pgrep', '-a', 'fdbserver']).decode().strip().split('\n')
    port = address.split(':')[1]
    logger.debug("Port: {}".format(port))
    # use the port number to find the exact fdb process we are connecting to
    pinfo = list(filter(lambda x: port in x, pinfos))
    assert len(pinfo) == 1
    pid = pinfo[0].split(' ')[0]
    logger.debug("Pid: {}".format(pid))
    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    # suspend the process for enough long time
    output2, err = process.communicate(input='suspend; suspend 3600 {}; sleep 1\n'.format(address).encode())
    # the cluster should be unavailable after the only process being suspended
    assert not get_value_from_status_json(False, 'client', 'database_status', 'available')
    # check the process pid still exists
    pids = subprocess.check_output(['pidof', 'fdbserver']).decode().strip()
    logger.debug("PIDs: {}".format(pids))
    assert pid in pids
    # kill the process by pid
    kill_output = subprocess.check_output(['kill', pid]).decode().strip()
    logger.debug("Kill result: {}".format(kill_output))
    # The process should come back after a few time
    duration = 0  # seconds we already wait
    while not get_value_from_status_json(False, 'client', 'database_status', 'available') and duration < 60:
        logger.debug("Sleep for 1 second to wait cluster recovery")
        time.sleep(1)
        duration += 1
    # at most after 60 seconds, the cluster should be available
    assert get_value_from_status_json(False, 'client', 'database_status', 'available')


def get_value_from_status_json(retry, *args):
    while True:
        result = json.loads(run_fdbcli_command('status', 'json'))
        if result['client']['database_status']['available'] or not retry:
            break
    for arg in args:
        assert arg in result
        result = result[arg]

    return result


@enable_logging()
def consistencycheck(logger):
    consistency_check_on_output = 'ConsistencyCheck is on'
    consistency_check_off_output = 'ConsistencyCheck is off'
    output1 = run_fdbcli_command('consistencycheck')
    assert output1 == consistency_check_on_output
    run_fdbcli_command('consistencycheck', 'off')
    output2 = run_fdbcli_command('consistencycheck')
    assert output2 == consistency_check_off_output
    run_fdbcli_command('consistencycheck', 'on')
    output3 = run_fdbcli_command('consistencycheck')
    assert output3 == consistency_check_on_output


@enable_logging()
def cache_range(logger):
    # this command is currently experimental
    # just test we can set and clear the cached range
    run_fdbcli_command('cache_range', 'set', 'a', 'b')
    run_fdbcli_command('cache_range', 'clear', 'a', 'b')


@enable_logging()
def datadistribution(logger):
    output1 = run_fdbcli_command('datadistribution', 'off')
    assert output1 == 'Data distribution is turned off.'
    output2 = run_fdbcli_command('datadistribution', 'on')
    assert output2 == 'Data distribution is turned on.'
    output3 = run_fdbcli_command('datadistribution', 'disable', 'ssfailure')
    assert output3 == 'Data distribution is disabled for storage server failures.'
    # While we disable ssfailure, maintenance should fail
    error_msg = run_fdbcli_command_and_get_error('maintenance', 'on', 'fake_zone_id', '1')
    assert error_msg == "ERROR: Maintenance mode cannot be used while data distribution is disabled for storage server failures. Use 'datadistribution on' to reenable data distribution."
    output4 = run_fdbcli_command('datadistribution', 'enable', 'ssfailure')
    assert output4 == 'Data distribution is enabled for storage server failures.'
    output5 = run_fdbcli_command('datadistribution', 'disable', 'rebalance')
    assert output5 == 'Data distribution is disabled for rebalance.'
    output6 = run_fdbcli_command('datadistribution', 'enable', 'rebalance')
    assert output6 == 'Data distribution is enabled for rebalance.'
    time.sleep(1)


@enable_logging()
def transaction(logger):
    """This test will cover the transaction related fdbcli commands.
        In particular, 
        'begin', 'rollback', 'option'
        'getversion', 'get', 'getrange', 'clear', 'clearrange', 'set', 'commit'
    """
    err1 = run_fdbcli_command_and_get_error('set', 'key', 'value')
    assert err1 == 'ERROR: writemode must be enabled to set or clear keys in the database.'
    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    transaction_flow = ['writemode on', 'begin', 'getversion', 'set key value', 'get key', 'commit']
    output1, _ = process.communicate(input='\n'.join(transaction_flow).encode())
    # split the output into lines
    lines = list(filter(len, output1.decode().split('\n')))[-4:]
    assert lines[0] == 'Transaction started'
    read_version = int(lines[1])
    logger.debug("Read version {}".format(read_version))
    # line[1] is the printed read version
    assert lines[2] == "`key' is `value'"
    assert lines[3].startswith('Committed (') and lines[3].endswith(')')
    # validate commit version is larger than the read version
    commit_verion = int(lines[3][len('Committed ('):-1])
    logger.debug("Commit version: {}".format(commit_verion))
    assert commit_verion >= read_version
    # check the transaction is committed
    output2 = run_fdbcli_command('get', 'key')
    assert output2 == "`key' is `value'"
    # test rollback and read-your-write behavior
    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    transaction_flow = [
        'writemode on', 'begin', 'getrange a z',
        'clear key', 'get key',
        # 'option on READ_YOUR_WRITES_DISABLE', 'get key',
        'rollback'
    ]
    output3, _ = process.communicate(input='\n'.join(transaction_flow).encode())
    lines = list(filter(len, output3.decode().split('\n')))[-5:]
    # lines[0] == "Transaction started" and lines[1] == 'Range limited to 25 keys'
    assert lines[2] == "`key' is `value'"
    assert lines[3] == "`key': not found"
    assert lines[4] == "Transaction rolled back"
    # make sure the rollback works
    output4 = run_fdbcli_command('get', 'key')
    assert output4 == "`key' is `value'"
    # test read_your_write_disable option and clear the inserted key
    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    transaction_flow = [
        'writemode on', 'begin',
        'option on READ_YOUR_WRITES_DISABLE',
        'clear key', 'get key',
        'commit'
    ]
    output6, _ = process.communicate(input='\n'.join(transaction_flow).encode())
    lines = list(filter(len, output6.decode().split('\n')))[-4:]
    assert lines[1] == 'Option enabled for current transaction'
    # the get key should still return the value even we clear it in the transaction
    assert lines[2] == "`key' is `value'"
    # Check the transaction is committed
    output7 = run_fdbcli_command('get', 'key')
    assert output7 == "`key': not found"


def get_fdb_process_addresses(logger):
    # get all processes' network addresses
    output = run_fdbcli_command('kill')
    logger.debug(output)
    # except the first line, each line is one process
    addresses = output.split('\n')[1:]
    assert len(addresses) == args.process_number
    return addresses


@enable_logging(logging.DEBUG)
def coordinators(logger):
    # we should only have one coordinator for now
    output1 = run_fdbcli_command('coordinators')
    assert len(output1.split('\n')) > 2
    cluster_description = output1.split('\n')[0].split(': ')[-1]
    logger.debug("Cluster description: {}".format(cluster_description))
    coordinators = output1.split('\n')[1].split(': ')[-1]
    # verify the coordinator
    coordinator_list = get_value_from_status_json(True, 'client', 'coordinators', 'coordinators')
    assert len(coordinator_list) == 1
    assert coordinator_list[0]['address'] == coordinators
    # verify the cluster description
    assert get_value_from_status_json(True, 'cluster', 'connection_string').startswith('{}:'.format(cluster_description))
    addresses = get_fdb_process_addresses(logger)
    # set all 5 processes as coordinators and update the cluster description
    new_cluster_description = 'a_simple_description'
    run_fdbcli_command('coordinators', *addresses, 'description={}'.format(new_cluster_description))
    # verify now we have 5 coordinators and the description is updated
    output2 = run_fdbcli_command('coordinators')
    assert output2.split('\n')[0].split(': ')[-1] == new_cluster_description
    assert output2.split('\n')[1] == 'Cluster coordinators ({}): {}'.format(args.process_number, ','.join(addresses))
    # auto change should go back to 1 coordinator
    run_fdbcli_command('coordinators', 'auto')
    assert len(get_value_from_status_json(True, 'client', 'coordinators', 'coordinators')) == 1
    wait_for_database_available(logger)


@enable_logging(logging.DEBUG)
def exclude(logger):
    # get all processes' network addresses
    addresses = get_fdb_process_addresses(logger)
    logger.debug("Cluster processes: {}".format(' '.join(addresses)))
    # There should be no excluded process for now
    no_excluded_process_output = 'There are currently no servers or localities excluded from the database.'
    output1 = run_fdbcli_command('exclude')
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
            error_message = run_fdbcli_command_and_get_error('exclude', 'FORCE', excluded_address)
        else:
            error_message = run_fdbcli_command_and_get_error('exclude', excluded_address)
        if error_message == 'WARNING: {} is a coordinator!'.format(excluded_address):
            # exclude coordinator will print the warning, verify the randomly selected process is the coordinator
            coordinator_list = get_value_from_status_json(True, 'client', 'coordinators', 'coordinators')
            assert len(coordinator_list) == 1
            assert coordinator_list[0]['address'] == excluded_address
            break
        elif 'ERROR: This exclude may cause the total free space in the cluster to drop below 10%.' in error_message:
            # exclude the process may cause the free space not enough
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
    output2 = run_fdbcli_command('exclude')
    assert 'There are currently 1 servers or localities being excluded from the database' in output2
    assert excluded_address in output2
    run_fdbcli_command('include', excluded_address)
    # check the include is successful
    output4 = run_fdbcli_command('exclude')
    assert no_excluded_process_output in output4
    wait_for_database_available(logger)

# read the system key 'k', need to enable the option first


def read_system_key(k):
    output = run_fdbcli_command('option', 'on', 'READ_SYSTEM_KEYS;', 'get', k)
    if 'is' not in output:
        # key not present
        return None
    _, value = output.split(' is ')
    return value


@enable_logging()
def throttle(logger):
    # no throttled tags at the beginning
    no_throttle_tags_output = 'There are no throttled tags'
    output = run_fdbcli_command('throttle', 'list')
    logger.debug(output)
    assert output == no_throttle_tags_output
    # test 'throttle enable auto'
    run_fdbcli_command('throttle', 'enable', 'auto')
    # verify the change is applied by reading the system key
    # not an elegant way, may change later
    enable_flag = read_system_key('\\xff\\x02/throttledTags/autoThrottlingEnabled')
    assert enable_flag == "`1'"
    run_fdbcli_command('throttle', 'disable', 'auto')
    enable_flag = read_system_key('\\xff\\x02/throttledTags/autoThrottlingEnabled')
    # verify disabled
    assert enable_flag == "`0'"
    # TODO : test manual throttling, not easy to do now


def wait_for_database_available(logger):
    # sometimes the change takes some time to have effect and the database can be unavailable at that time
    # this is to wait until the database is available again
    while not get_value_from_status_json(True, 'client', 'database_status', 'available'):
        logger.debug("Database unavailable for now, wait for one second")
        time.sleep(1)


@enable_logging()
def profile(logger):
    # profile list should return the same list as kill
    addresses = get_fdb_process_addresses(logger)
    output1 = run_fdbcli_command('profile', 'list')
    assert output1.split('\n') == addresses
    # check default output
    default_profile_client_get_output = 'Client profiling rate is set to default and size limit is set to default.'
    output2 = run_fdbcli_command('profile', 'client', 'get')
    assert output2 == default_profile_client_get_output
    # set rate and size limit
    run_fdbcli_command('profile', 'client', 'set', '0.5', '1GB')
    output3 = run_fdbcli_command('profile', 'client', 'get')
    logger.debug(output3)
    output3_list = output3.split(' ')
    assert float(output3_list[6]) == 0.5
    # size limit should be 1GB
    assert output3_list[-1] == '1000000000.'
    # change back to default value and check
    run_fdbcli_command('profile', 'client', 'set', 'default', 'default')
    assert run_fdbcli_command('profile', 'client', 'get') == default_profile_client_get_output


@enable_logging()
def test_available(logger):
    duration = 0  # seconds we already wait
    while not get_value_from_status_json(False, 'client', 'database_status', 'available') and duration < 10:
        logger.debug("Sleep for 1 second to wait cluster recovery")
        time.sleep(1)
        duration += 1
    if duration >= 10:
        logger.debug(run_fdbcli_command('status', 'json'))
        assert False


@enable_logging()
def triggerddteaminfolog(logger):
    # this command is straightforward and only has one code path
    output = run_fdbcli_command('triggerddteaminfolog')
    assert output == 'Triggered team info logging in data distribution.'

@enable_logging()
def tenants(logger):
    output = run_fdbcli_command('listtenants')
    assert output == 'The cluster has no tenants'

    output = run_fdbcli_command('createtenant tenant')
    assert output == 'The tenant `tenant\' has been created'

    output = run_fdbcli_command('createtenant tenant2')
    assert output == 'The tenant `tenant2\' has been created'

    output = run_fdbcli_command('listtenants')
    assert output == '1. tenant\n  2. tenant2'

    output = run_fdbcli_command('listtenants a z 1')
    assert output == '1. tenant'

    output = run_fdbcli_command('listtenants a tenant2')
    assert output == '1. tenant'

    output = run_fdbcli_command('listtenants tenant2 z')
    assert output == '1. tenant2'

    output = run_fdbcli_command('gettenant tenant')
    lines = output.split('\n')
    assert len(lines) == 2
    assert lines[0].strip().startswith('id: ')
    assert lines[1].strip().startswith('prefix: ')
    
    output = run_fdbcli_command('usetenant')
    assert output == 'Using the default tenant'

    output = run_fdbcli_command_and_get_error('usetenant tenant3')
    assert output == 'ERROR: Tenant `tenant3\' does not exist'

    # Test writing keys to different tenants and make sure they all work correctly
    run_fdbcli_command('writemode on; set tenant_test default_tenant')
    output = run_fdbcli_command('get tenant_test')
    assert output == '`tenant_test\' is `default_tenant\''

    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    cmd_sequence = ['writemode on', 'usetenant tenant', 'get tenant_test', 'set tenant_test tenant']
    output, _ = process.communicate(input='\n'.join(cmd_sequence).encode())

    lines = output.decode().strip().split('\n')[-3:]
    assert lines[0] == 'Using tenant `tenant\''
    assert lines[1] == '`tenant_test\': not found'
    assert lines[2].startswith('Committed')

    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    cmd_sequence = ['writemode on', 'usetenant tenant2', 'get tenant_test', 'set tenant_test tenant2', 'get tenant_test']
    output, _ = process.communicate(input='\n'.join(cmd_sequence).encode())

    lines = output.decode().strip().split('\n')[-4:]
    assert lines[0] == 'Using tenant `tenant2\''
    assert lines[1] == '`tenant_test\': not found'
    assert lines[2].startswith('Committed')
    assert lines[3] == '`tenant_test\' is `tenant2\''

    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=fdbcli_env)
    cmd_sequence = ['usetenant tenant', 'get tenant_test', 'defaulttenant', 'get tenant_test']
    output, _ = process.communicate(input='\n'.join(cmd_sequence).encode())

    lines = output.decode().strip().split('\n')[-4:]
    assert lines[0] == 'Using tenant `tenant\''
    assert lines[1] == '`tenant_test\' is `tenant\''
    assert lines[2] == 'Using the default tenant'
    assert lines[3] == '`tenant_test\' is `default_tenant\''

    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env)
    cmd_sequence = ['writemode on', 'usetenant tenant', 'clear tenant_test', 'deletetenant tenant', 'get tenant_test', 'defaulttenant', 'usetenant tenant']
    output, error_output = process.communicate(input='\n'.join(cmd_sequence).encode())

    lines = output.decode().strip().split('\n')[-7:]
    error_lines = error_output.decode().strip().split('\n')[-2:]
    assert lines[0] == 'Using tenant `tenant\''
    assert lines[1].startswith('Committed')
    assert lines[2] == 'The tenant `tenant\' has been deleted'
    assert lines[3] == 'WARNING: the active tenant was deleted. Use the `usetenant\' or `defaulttenant\''
    assert lines[4] == 'command to choose a new tenant.'
    assert error_lines[0] == 'ERROR: Tenant does not exist (2131)'
    assert lines[6] == 'Using the default tenant'
    assert error_lines[1] == 'ERROR: Tenant `tenant\' does not exist'

    process = subprocess.Popen(command_template[:-1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env)
    cmd_sequence = ['writemode on', 'deletetenant tenant2', 'usetenant tenant2', 'clear tenant_test', 'defaulttenant', 'deletetenant tenant2']
    output, error_output = process.communicate(input='\n'.join(cmd_sequence).encode())

    lines = output.decode().strip().split('\n')[-4:]
    error_lines = error_output.decode().strip().split('\n')[-1:]
    assert error_lines[0] == 'ERROR: Cannot delete a non-empty tenant (2133)'
    assert lines[0] == 'Using tenant `tenant2\''
    assert lines[1].startswith('Committed')
    assert lines[2] == 'Using the default tenant'
    assert lines[3] == 'The tenant `tenant2\' has been deleted'

    run_fdbcli_command('writemode on; clear tenant_test')

if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
                            description="""
    The test calls fdbcli commands through fdbcli --exec "<command>" interactively using subprocess.
    The outputs from fdbcli are returned and compared to predefined results.
    Consequently, changing fdbcli outputs or breaking any commands will casue the test to fail.
    Commands that are easy to test will run against a single process cluster.
    For complex commands like exclude, they will run against a cluster with multiple(current set to 5) processes.
    If external_client_library is given, we will disable the local client and use the external client to run fdbcli.
    """)
    parser.add_argument('build_dir', metavar='BUILD_DIRECTORY', help='FDB build directory')
    parser.add_argument('cluster_file', metavar='CLUSTER_FILE', help='FDB cluster file')
    parser.add_argument('process_number', nargs='?', metavar='PROCESS_NUMBER', help="Number of fdb processes", type=int, default=1)
    parser.add_argument('--external-client-library', '-e', metavar='EXTERNAL_CLIENT_LIBRARY_PATH', help="External client library path")
    args = parser.parse_args()

    # keep current environment variables
    fdbcli_env = os.environ.copy()
    # set external client library if provided
    if args.external_client_library:
        # disable local client and use the external client library
        fdbcli_env['FDB_NETWORK_OPTION_DISABLE_LOCAL_CLIENT'] = ''
        fdbcli_env['FDB_NETWORK_OPTION_EXTERNAL_CLIENT_LIBRARY'] = args.external_client_library

    # shell command template
    command_template = [args.build_dir + '/bin/fdbcli', '-C', args.cluster_file, '--exec']
    # tests for fdbcli commands
    # assertions will fail if fdbcli does not work as expected
    test_available()
    if args.process_number == 1:
        # TODO: disable for now, the change can cause the database unavailable
        # advanceversion()
        cache_range()
        consistencycheck()
        datadistribution()
        kill()
        lockAndUnlock()
        maintenance()
        profile()
        suspend()
        transaction()
        throttle()
        triggerddteaminfolog()
        tenants()
    else:
        assert args.process_number > 1, "Process number should be positive"
        coordinators()
        exclude()
        # TODO: fix the failure where one process is not available after setclass call
        #setclass()
