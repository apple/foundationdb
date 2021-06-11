#!/usr/bin/env python3

import sys
import subprocess
import logging
import functools

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
            result = func(logger, *args,**kwargs)
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

if __name__ == '__main__':
    # fdbcli_tests.py <path_to_fdbcli_binary> <path_to_fdb_cluster_file>
    assert len(sys.argv) == 3, "Please pass arguments: <path_to_fdbcli_binary> <path_to_fdb_cluster_file>"
    # shell command template
    command_template = [sys.argv[1], '-C', sys.argv[2], '--exec']
    # tests for fdbcli commands
    # assertions will fail if fdbcli does not work as expected
    advanceversion()
    maintenance()
