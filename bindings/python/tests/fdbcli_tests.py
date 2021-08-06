#!/usr/bin/env python3

import sys
import subprocess
import logging
import functools
import json
import time
import random

def enable_logging(level=logging.ERROR):
    """Enable logging in the function with the specified logging level

    Args:
        level (logging.<level>, optional): logging level for the decorated function. Defaults to logging.ERROR.
    """
    def func_decorator(func):
        @functools.wraps(func)
        def wrapper(*args,**kwargs):
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
    return subprocess.run(commands, stdout=subprocess.PIPE).stdout.decode('utf-8').strip()

def run_fdbcli_command_and_get_error(*args):
    """run the fdbcli statement: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Stderr output from fdbcli
    """
    commands = command_template + ["{}".format(' '.join(args))]
    return subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stderr.decode('utf-8').strip()

@enable_logging()
def advanceversion(logger):
    # get current read version
    version1 = int(run_fdbcli_command('getversion'))
    logger.debug("Read version: {}".format(version1))
    # advance version to a much larger value compared to the current version
    version2 = version1 * 100
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
    # wait for the recovery to finish
    # if the database is unavailable, the test will timeout and fail
    while not get_value_from_status_json(False, 'client', 'database_status', 'available'):
        logger.debug("Database unavailable after advanceversion, wait for 1 second")
        time.sleep(1)

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
    output1 = run_fdbcli_command('setclass')
    class_type_line_1 = output1.split('\n')[-1]
    logger.debug(class_type_line_1)
    # check process' network address
    assert '127.0.0.1' in class_type_line_1
    network_address = ':'.join(class_type_line_1.split(':')[:2])
    logger.debug("Network address: {}".format(network_address))
    # check class type
    assert 'unset' in class_type_line_1
    # check class source
    assert 'command_line' in class_type_line_1
    # set class to a random valid type
    class_types = ['storage', 'storage', 'transaction', 'resolution', 
                    'commit_proxy', 'grv_proxy', 'master', 'stateless', 'log', 
                    'router', 'cluster_controller', 'fast_restore', 'data_distributor',
	                'coordinator', 'ratekeeper', 'storage_cache', 'backup'
                ]
    random_class_type = random.choice(class_types)
    logger.debug("Change to type: {}".format(random_class_type))
    run_fdbcli_command('setclass', network_address, random_class_type)
    # check the set successful
    output2 = run_fdbcli_command('setclass')
    class_type_line_2 = output2.split('\n')[-1]
    logger.debug(class_type_line_2)
    # check process' network address
    assert network_address in class_type_line_2
    # check class type changed to the specified value
    assert random_class_type in class_type_line_2
    # check class source
    assert 'set_class' in class_type_line_2
    # set back to default
    run_fdbcli_command('setclass', network_address, 'default')
    # everything should be back to the same as before
    output3 = run_fdbcli_command('setclass')
    class_type_line_3 = output3.split('\n')[-1]
    logger.debug(class_type_line_3)
    assert class_type_line_3 == class_type_line_1

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
    process = subprocess.Popen(command_template + ['unlock ' + lock_uid], stdin = subprocess.PIPE, stdout = subprocess.PIPE)
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
    process = subprocess.Popen(command_template[:-1], stdin = subprocess.PIPE, stdout = subprocess.PIPE)
    # 
    output2, err = process.communicate(input='kill; kill {}\n'.format(address).encode())
    logger.debug(output2)
    # wait for a second for the cluster recovery
    time.sleep(1)
    new_generation = get_value_from_status_json(True, 'cluster', 'generation')
    logger.debug("Old: {}, New: {}".format(old_generation, new_generation))
    assert new_generation > old_generation

@enable_logging()
def suspend(logger):
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
    process = subprocess.Popen(command_template[:-1], stdin = subprocess.PIPE, stdout = subprocess.PIPE)
    # suspend the process for enough long time
    output2, err = process.communicate(input='suspend; suspend 3600 {}\n'.format(address).encode())
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
    duration = 0 # seconds we already wait
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
    process = subprocess.Popen(command_template[:-1], stdin = subprocess.PIPE, stdout = subprocess.PIPE)
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
    process = subprocess.Popen(command_template[:-1], stdin = subprocess.PIPE, stdout = subprocess.PIPE)
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
    process = subprocess.Popen(command_template[:-1], stdin = subprocess.PIPE, stdout = subprocess.PIPE)
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

def get_fdb_process_addresses():
    # get all processes' network addresses
    output = run_fdbcli_command('kill')
    # except the first line, each line is one process
    addresses = output.split('\n')[1:]
    assert len(addresses) == process_number
    return addresses

@enable_logging()
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
    addresses = get_fdb_process_addresses()
    # set all 5 processes as coordinators and update the cluster description
    new_cluster_description = 'a_simple_description'
    run_fdbcli_command('coordinators', *addresses, 'description={}'.format(new_cluster_description))
    # verify now we have 5 coordinators and the description is updated
    output2 = run_fdbcli_command('coordinators')
    assert output2.split('\n')[0].split(': ')[-1] == new_cluster_description
    assert output2.split('\n')[1] == 'Cluster coordinators ({}): {}'.format(5, ','.join(addresses))
    # auto change should go back to 1 coordinator
    run_fdbcli_command('coordinators', 'auto')
    assert len(get_value_from_status_json(True, 'client', 'coordinators', 'coordinators')) == 1

@enable_logging()
def exclude(logger):
    # get all processes' network addresses
    addresses = get_fdb_process_addresses()
    logger.debug("Cluster processes: {}".format(' '.join(addresses)))
    # There should be no excluded process for now
    no_excluded_process_output = 'There are currently no servers or localities excluded from the database.'
    output1 = run_fdbcli_command('exclude')
    assert no_excluded_process_output in output1
    # randomly pick one and exclude the process
    excluded_address = random.choice(addresses)
    # sometimes we need to retry the exclude
    while True:
        logger.debug("Excluding process: {}".format(excluded_address))
        error_message = run_fdbcli_command_and_get_error('exclude', excluded_address)
        if error_message == 'WARNING: {} is a coordinator!'.format(excluded_address):
            # exclude coordinator will print the warning, verify the randomly selected process is the coordinator
            coordinator_list = get_value_from_status_json(True, 'client', 'coordinators', 'coordinators')
            assert len(coordinator_list) == 1
            assert coordinator_list[0]['address'] == excluded_address
            break
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
    assert run_fdbcli_command('throttle', 'list') == no_throttle_tags_output
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

if __name__ == '__main__':
    # fdbcli_tests.py <path_to_fdbcli_binary> <path_to_fdb_cluster_file> <process_number>
    assert len(sys.argv) == 4, "Please pass arguments: <path_to_fdbcli_binary> <path_to_fdb_cluster_file> <process_number>"
    # shell command template
    command_template = [sys.argv[1], '-C', sys.argv[2], '--exec']
    # tests for fdbcli commands
    # assertions will fail if fdbcli does not work as expected
    process_number = int(sys.argv[3])
    if process_number == 1:
        advanceversion()
        cache_range()
        consistencycheck()
        datadistribution()
        kill()
        lockAndUnlock()
        maintenance()
        setclass()
        suspend()
        transaction()
        throttle()
    else:
        assert process_number > 1, "Process number should be positive"
        # the kill command which used to list processes seems to not work as expected sometime
        # which makes the test flaky.
        # We need to figure out the reason and then re-enable these tests
        #coordinators()
        #exclude()

    
