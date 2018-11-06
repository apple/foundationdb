#!/usr/bin/env python3

from argparse import ArgumentParser
from TestDirectory import TestDirectory

import logging
import os
import sys
import subprocess
import json
import functools
import multiprocessing
import re


address_re = re.compile(r'(0x[0-9a-f]+\s+)+')

def sanitize_backtrace(obj):
    if sys.platform != "linux" and sys.platform != "linux2":
        return None
    raw_backtrace = obj.get('Backtrace', None)
    if raw_backtrace is None:
        return None
    match = address_re.search(raw_backtrace)
    if not match:
        return None
    return match.group(0)

def apply_addr2line(basedir, obj):
    addresses = sanitize_backtrace(obj)
    assert addresses is not None
    fdbbin = os.path.join(basedir, 'bin', 'fdbserver')
    try:
        resolved = subprocess.check_output(
                ('addr2line -e %s -C -f -i' % fdbbin).split() + addresses.split()).splitlines()
        tmp = dict(**obj)
        for i, line in enumerate(resolved):
            tmp['line%04d' % i] = line.decode('utf-8')
    except (subprocess.CalledProcessError, UnicodeDecodeError):
        obj['FailedAddr2LineResolution'] = 'true'
        return obj
    return tmp

logger = None

def init_logging(loglevel, logdir):
    global _logger
    _logger = logging.getLogger('TestRunner')
    _logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(message)s')
    try:
        os.makedirs(logdir)
    except:
        pass
    fh = logging.FileHandler(os.path.join(logdir, 'run_test.log'))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setLevel(loglevel)
    sh.setFormatter(formatter)
    _logger.addHandler(fh)
    _logger.addHandler(sh)


def log_trace_parse_error(line, e, infile, eof):
    obj = {}
    _logger.error("Process trace line file {} Failed".format(infile))
    _logger.error("Exception {} args: {}".format(type(e), e.args))
    _logger.error("Line: '{}'".format(line))
    obj['Severity'] = "warning"
    obj['Type'] = "TestInfastructureLogLineGarbled"
    obj['isLastLine'] = "TestFailure"
    obj['TraceLine'] = line
    obj['File'] = infile
    return obj


def process_trace(basedir, name, infile, out, backtraces):
    """
    backtraces is an out param
    """
    res = True
    with open(infile) as f:
        try:
            line = f.readline()
            while line != '':
                obj = {}
                nextLine = f.readline()
                eof = nextLine == ''
                try:
                    obj = json.loads(line)
                    line = nextLine
                except Exception as e:
                    obj = log_trace_parse_error(line, e, infile, eof)
                    line = nextLine
                if 'Type' not in obj:
                    continue
                if obj['Type'] == 'TestFailure':
                    res = False
                if obj['Severity'] == '40':
                    res = False
                if name is not None:
                    obj['testname'] = name
                # For now only addr2line one backtrace.
                if sanitize_backtrace(obj) is not None and len(backtraces) == 0:
                    backtraces.append(obj)
                else:
                    print(json.dumps(obj), file=out)
        except BaseException as e:
            _logger.error("Error: {}".format(e))
    return res


def get_traces(d):
    p = re.compile('^trace\\..*\\.json$')
    traces = list(map(
        functools.partial(os.path.join, d),
        filter(
            lambda f: p.match(f) is not None,
            os.listdir(d))))
    if os.path.isdir(os.path.join(d, 'testrunner')):
        traces += list(map(
            functools.partial(os.path.join, d, 'testrunner'),
            filter(
                lambda f: p.match(f) is not None,
                os.listdir(os.path.join(d, 'testrunner')))))
    return traces


def process_traces(basedir, testname, path, out):
    res = True
    backtraces = []
    for trace in get_traces(path):
        res = process_trace(
                basedir, testname, trace, out, backtraces) and res
    with multiprocessing.Pool(max(multiprocessing.cpu_count() - 1, 1)) as pool:
        for o in pool.map(
                functools.partial(apply_addr2line, basedir), backtraces):
            print(json.dumps(o), file=out)
    return res

def process_return_codes(return_codes, name, out):
    res = True
    for (command, return_code) in return_codes.items():
        return_code_trace = {'Command': command, 'ReturnCode': return_code, 'testname': name}
        if return_code != 0:
            return_code_trace['Type'] = 'TestFailure'
            res = False
        print(json.dumps(return_code_trace), file=out)
    return res
    
def run_simulation_test(basedir,
                        testtype,
                        testname,
                        testfiles,
                        restart=False,
                        buggify=False,
                        seed=None):
    pargs = [os.path.join(basedir, 'bin', 'fdbserver'),
             '-r', testtype]
    if testtype == 'test':
        pargs.append('-C')
        pargs.append(os.path.join(args.builddir, 'fdb.cluster'))
    td = TestDirectory(basedir)
    if restart:
        pargs.append('-R')
    if buggify:
        pargs.append('-b')
        pargs.append('on')
    pargs.append('--knob_trace_format=json')
    test_dir = td.get_current_test_dir()
    if seed is not None:
        pargs.append('-s')
        pargs.append(str(args.seed))
    wd = os.path.join(test_dir,
                      'test_{}'.format(testname.replace('/', '_')))
    os.mkdir(wd)
    return_codes = {} # {command: return_code}
    first = True
    for testfile in testfiles:
        tmp = list(pargs)
        if not first:
            tmp.append('-R')
        first = False
        tmp.append('-f')
        tmp.append(testfile)
        command = ' '.join(tmp)
        _logger.info("COMMAND: {}".format(command))
        proc = subprocess.Popen(tmp,
                                stdout=sys.stdout,
                                stderr=sys.stderr,
                                cwd=wd)
        proc.wait()
        return_codes[command] = proc.returncode
        if proc.returncode != 0:
            break
    jsonfile = os.path.join(basedir, 'traces.json')
    res = True
    with open(jsonfile, 'a') as f:
        os.lockf(f.fileno(), os.F_LOCK, 0)
        pos = f.tell()
        res = process_traces(basedir, testname,
                             os.path.join(os.getcwd(), wd), f)
        res = process_return_codes(return_codes, testname, f) and res
        f.seek(pos)
        os.lockf(f.fileno(), os.F_ULOCK, 0)
    return res


if __name__ == '__main__':
    testtypes = ['simulation', 'test']
    parser = ArgumentParser(description='Run a test preprocess trace')
    parser.add_argument('-b', '--builddir', help='Path to build directory')
    parser.add_argument('-s', '--sourcedir', help='Path to source directory')
    parser.add_argument('-n', '--name', help='Name of the test')
    parser.add_argument('-t', '--testtype', choices=testtypes,
                        default='simulation',
                        help='The type of test to run, choices are [{}]'.format(
                            ', '.join(testtypes))),
    parser.add_argument('-R', '--restart', action='store_true',
                        help='Mark as restart test')
    parser.add_argument('-B', '--buggify', action='store_true',
                        help='Enable buggify')
    parser.add_argument('--logdir', default='logs',
                        help='Directory for logs')
    parser.add_argument('-l', '--loglevel',
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO',
                                 'DEBUG'],
                        default='INFO')
    parser.add_argument('-x', '--seed', required=False, default=None,
                        help='The seed to use for this test')
    parser.add_argument('testfile', nargs="+", help='The tests to run')
    args = parser.parse_args()
    init_logging(args.loglevel, args.logdir)
    basedir = os.getcwd()
    if args.builddir is not None:
        basedir = args.builddir
    res = run_simulation_test(basedir, args.testtype, args.name,
                              args.testfile, args.restart, args.buggify,
                              args.seed)
    sys.exit(0 if res else 1)
