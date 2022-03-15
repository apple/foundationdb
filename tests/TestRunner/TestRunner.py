#!/usr/bin/env python

from argparse import ArgumentParser
from TestDirectory import TestDirectory

import logging
import os
import sys
import subprocess
import json
import xml.sax
import xml.sax.handler
import functools
import multiprocessing
import re
import shutil
import io
import random

_logger = None

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


class LogParser:
    def __init__(self, basedir, name, infile, out, aggregationPolicy, symbolicateBacktraces):
        self.basedir = basedir
        self.name = name
        self.infile = infile
        self.backtraces = []
        self.result = True
        self.address_re = re.compile(r'(0x[0-9a-f]+\s+)+')
        self.aggregationPolicy = aggregationPolicy
        self.symbolicateBacktraces = symbolicateBacktraces
        self.outStream = None
        if self.aggregationPolicy == 'NONE':
            self.out = None
        elif self.aggregationPolicy != 'ALL':
            self.out = io.StringIO()
            self.outStream = out
        else:
            self.out = out

    def write(self, txt):
        if self.aggregationPolicy == 'NONE':
            pass
        elif not self.result or self.aggregationPolicy == 'ALL':
            self.out.write(txt)
        else:
            self.outStream.wite(txt)

    def fail(self):
        self.result = False
        if self.aggregationPolicy == 'FAILED':
            self.out.write(self.outStream.getvalue())
            self.outStream = None

    def writeHeader(self):
        pass

    def writeFooter(self):
        pass

    def applyAddr2line(self, obj):
        addresses = self.sanitizeBacktrace(obj)
        assert addresses is not None
        config = ''
        binaryExt = ''
        if sys.platform == 'win32':
            #config = options.config
            binaryExt = '.exe'
        fdbbin = os.path.realpath(os.path.join(basedir, 'bin', 'Release', 'fdbserver' + binaryExt))
        try:
            resolved = subprocess.check_output(
                    ('addr2line -e %s -C -f -i' % fdbbin).split() + addresses.split()).splitlines()
            tmp = dict(**obj)
            for i, line in enumerate(resolved):
                tmp['line%04d' % i] = line.decode('utf-8')
            return tmp
        except (subprocess.CalledProcessError, UnicodeDecodeError):
            obj['FailedAddr2LineResolution'] = 'true'
            return obj


    def sanitizeBacktrace(self, obj):
        if sys.platform != "linux" and sys.platform != "linux2":
            return None
        raw_backtrace = obj.get('Backtrace', None)
        if raw_backtrace is None:
            return None
        match = self.address_re.search(raw_backtrace)
        if not match:
            return None
        return match.group(0)

    def processTraces(self):
        linenr = 0
        with open(self.infile) as f:
            line = f.readline()
            while line != '':
                obj = self.processLine(line, linenr)
                line = f.readline()
                linenr += 1
                if obj is None:
                    continue
                if 'Type' not in obj:
                    continue
                if obj['Severity'] == '40' and obj.get('ErrorIsInjectedFault', None) != '1':
                    self.fail()
                if self.name is not None:
                    obj['testname'] = self.name
                if self.symbolicateBacktraces and self.sanitizeBacktrace(obj) is not None:
                    obj = self.applyAddr2line(obj)
                self.writeObject(obj)

    def log_trace_parse_error(self, linenr, e):
        obj = {}
        _logger.error("Process trace line file {} Failed".format(self.infile))
        _logger.error("Exception {} args: {}".format(type(e), e.args))
        _logger.error("Line: '{}'".format(linenr))
        obj['Severity'] = "warning"
        obj['Type'] = "TestInfastructureLogLineGarbled"
        obj['isLastLine'] = "TestFailure"
        obj['TraceLine'] = linenr
        obj['File'] = self.infile
        return obj

    def processReturnCodes(self, return_codes):
        for (command, return_code) in return_codes.items():
            return_code_trace = {}
            if return_code != 0:
                return_code_trace['Severity'] = '40'
                return_code_trace['Type'] = 'TestFailure'
                self.fail()
            else:
                return_code_trace['Severity'] = '10'
                return_code_trace['Type'] = 'ReturnCode'
            return_code_trace['Command'] = command
            return_code_trace['ReturnCode'] = return_code
            return_code_trace['testname'] = self.name
            self.writeObject(return_code_trace)



class JSONParser(LogParser):
    def __init__(self, basedir, name, infile, out, aggregationPolicy, symbolicateBacktraces):
        super().__init__(basedir, name, infile, out, aggregationPolicy, symbolicateBacktraces)

    def processLine(self, line, linenr):
        try:
            return json.loads(line)
        except Exception as e:
            self.log_trace_parse_error(linenr, e)

    def writeObject(self, obj):
        self.write(json.dumps(obj))
        self.write('\n')


class XMLParser(LogParser):

    class XMLHandler(xml.sax.handler.ContentHandler):
        def __init__(self):
            self.result = {}

        def startElement(self, name, attrs):
            if name != 'Event':
                return
            for (key, value) in attrs.items():
                self.result[key] = value

    class XMLErrorHandler(xml.sax.handler.ErrorHandler):
        def __init__(self):
            self.errors = []
            self.fatalErrors = []
            self.warnings = []

        def error(self, exception):
            self.errors.append(exception)

        def fatalError(self, exception):
            self.fatalError.append(exception)

        def warning(self, exception):
            self.warnings.append(exception)

    def __init__(self, basedir, name, infile, out, aggregationPolicy, symbolicateBacktraces):
        super().__init__(basedir, name, infile, out, aggregationPolicy, symbolicateBacktraces)

    def writeHeader(self):
        self.write('<?xml version="1.0"?>\n<Trace>\n')

    def writeFooter(self):
        self.write("</Trace>")

    def writeObject(self, obj):
        self.write('<Event')
        for (key, value) in obj.items():
            self.write(' {}="{}"'.format(key, value))
        self.write('/>\n')

    def processLine(self, line, linenr):
        if linenr < 3:
            # the first two lines don't need to be parsed
            return None
        if line.startswith('</'):
            # don't parse the closing element
            return None
        handler = XMLParser.XMLHandler()
        errorHandler = XMLParser.XMLErrorHandler()
        xml.sax.parseString(line.encode('utf-8'), handler, errorHandler=errorHandler)
        if len(errorHandler.fatalErrors) > 0:
            return self.log_trace_parse_error(linenr, errorHandler.fatalErrors[0])
        return handler.result


def get_traces(d, log_format):
    p = re.compile('^trace\\..*\\.{}$'.format(log_format))
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


def process_traces(basedir, testname, path, out, aggregationPolicy, symbolicateBacktraces, log_format, return_codes, cmake_seed):
    res = True
    backtraces = []
    parser = None
    if log_format == 'json':
        parser = JSONParser(basedir, testname, None, out, aggregationPolicy, symbolicateBacktraces)
    else:
        parser = XMLParser(basedir, testname, None, out, aggregationPolicy, symbolicateBacktraces)
    parser.processReturnCodes(return_codes)
    res = parser.result
    for trace in get_traces(path, log_format):
        if log_format == 'json':
            parser = JSONParser(basedir, testname, trace, out, aggregationPolicy, symbolicateBacktraces)
        else:
            parser = XMLParser(basedir, testname, trace, out, aggregationPolicy, symbolicateBacktraces)
        if not res:
            parser.fail()
        parser.processTraces()
        res = res and parser.result
    parser.writeObject({'CMakeSEED': str(cmake_seed)})
    return res

class RestartTestPolicy:
    def __init__(self, name, old_binary, new_binary):
        # Default is to use the same binary for the restart test, unless constraints are satisfied.
        self._first_binary = new_binary
        self._second_binary = new_binary
        if old_binary is None:
            _logger.info("No old binary provided")
        old_binary_version_raw = subprocess.check_output([old_binary, '--version']).decode('utf-8')
        match = re.match('FoundationDB.*\(v([0-9]+\.[0-9]+\.[0-9]+)\)', old_binary_version_raw)
        assert match, old_binary_version_raw
        old_binary_version = tuple(map(int, match.group(1).split('.')))
        match = re.match('.*/restarting/from_([0-9]+\.[0-9]+\.[0-9]+)/', name)
        if match: # upgrading _from_
            lower_bound = tuple(map(int, match.group(1).split('.')))
            if old_binary_version >= lower_bound:
                self._first_binary = old_binary
                _logger.info("Using old binary as first binary: {} >= {}".format(old_binary_version, lower_bound))
            else:
                _logger.info("Using new binary as first binary: {} < {}".format(old_binary_version, lower_bound))
        match = re.match('.*/restarting/to_([0-9]+\.[0-9]+\.[0-9]+)/', name)
        if match: # downgrading _to_
            lower_bound = tuple(map(int, match.group(1).split('.')))
            if old_binary_version >= lower_bound:
                self._second_binary = old_binary
                _logger.info("Using old binary as second binary: {} >= {}".format(old_binary_version, lower_bound))
            else:
                _logger.info("Using new binary as second binary: {} < {}".format(old_binary_version, lower_bound))

    def first_binary(self):
        return self._first_binary

    def second_binary(self):
        return self._second_binary

def run_simulation_test(basedir, options):
    config = ''
    binaryExt = ''
    if sys.platform == 'win32':
        config = options.config
        binaryExt = '.exe'
    fdbserver = os.path.realpath(os.path.join(basedir, 'bin', config, 'fdbserver' + binaryExt))
    pargs = [fdbserver,
             '-r', options.testtype]
    seed = 0
    if options.seed is not None:
        pargs.append('-s')
        seed = int(options.seed, 0)
        if options.test_number:
            idx = int(options.test_number)
            seed = ((seed + idx) % (2**32-2)) + 1
        pargs.append("{}".format(seed))
    if options.testtype == 'test':
        pargs.append('-C')
        pargs.append(os.path.join(args.builddir, 'fdb.cluster'))
    td = TestDirectory(basedir)
    if options.buggify:
        pargs.append('-b')
        pargs.append('on')
    if options.crash:
        pargs.append('--crash')

    # Use old style argument with underscores because old binaries don't support hyphens
    pargs.append('--trace_format')
    pargs.append(options.log_format)
    test_dir = td.get_current_test_dir()
    if options.seed is not None:
        seed = int(options.seed, 0)
        if options.test_number:
            idx = int(options.test_number)
            seed = ((seed + idx) % (2**32-2)) + 1
    wd = os.path.join(test_dir,
                      'test_{}'.format(options.name.replace('/', '_')))
    os.mkdir(wd)
    return_codes = {} # {command: return_code}
    first = True
    restart_test_policy = None
    if len(options.testfile) > 1:
        restart_test_policy = RestartTestPolicy(options.testfile[0], options.old_binary, fdbserver)
    for testfile in options.testfile:
        tmp = list(pargs)
        valgrind_args = []
        if restart_test_policy is not None:
            if first:
                tmp[0] = restart_test_policy.first_binary()
            else:
                tmp[0] = restart_test_policy.second_binary()
        # old_binary is not under test, so don't run under valgrind
        if options.use_valgrind and tmp[0] == fdbserver:
            valgrind_args = ['valgrind', '--error-exitcode=99', '--']
        if not first:
            tmp.append('-R')
            if seed is not None:
                seed = ((seed + 1) % (2**32-2))
        first = False
        if seed is not None:
            tmp.append('-s')
            tmp.append("{}".format(seed))
        tmp.append('-f')
        tmp.append(testfile)
        tmp = valgrind_args + tmp
        command = ' '.join(tmp)
        _logger.info("COMMAND: {}".format(command))
        proc = subprocess.Popen(tmp,
                                stdout=sys.stdout,
                                stderr=sys.stderr,
                                cwd=wd)
        proc.wait()
        return_codes[command] = proc.returncode
        outfile = os.path.join(test_dir, 'traces.{}'.format(options.log_format))
        res = True
        if options.aggregate_traces == 'NONE':
            res = process_traces(basedir, options.name,
                                 wd, None, 'NONE', options.symbolicate,
                                 options.log_format, return_codes, options.seed)

        else:
            with open(outfile, 'a') as f:
                os.lockf(f.fileno(), os.F_LOCK, 0)
                pos = f.tell()
                res = process_traces(basedir, options.name,
                                     wd, f, options.aggregate_traces, options.symbolicate,
                                     options.log_format, return_codes, options.seed)
                f.seek(pos)
                os.lockf(f.fileno(), os.F_ULOCK, 0)
        if proc.returncode != 0 or res == False:
            break
    if options.keep_logs == 'NONE' or options.keep_logs == 'FAILED' and res:
        print("Deleting old logs in {}".format(wd))
        traces = get_traces(wd, options.log_format)
        for trace in traces:
            os.remove(trace)
    if options.keep_simdirs == 'NONE' or options.keep_simdirs == 'FAILED' and res:
        print("Delete {}".format(os.path.join(wd, 'simfdb')))
        # Don't fail if the directory doesn't exist.
        try:
            shutil.rmtree(os.path.join(wd, 'simfdb'))
        except FileNotFoundError:
            pass
    if len(os.listdir(wd)) == 0:
        print("Delete {} - empty".format(wd))
        os.rmdir(wd)
    return res and proc.returncode == 0


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
    parser.add_argument('-N', '--test-number', required=False, default=None,
                        help='A unique number for this test (for seed generation)')
    parser.add_argument('-F', '--log-format', required=False, default='xml',
                        choices=['xml', 'json'], help='Log format (json or xml)')
    parser.add_argument('-O', '--old-binary', required=False, default=None,
                        help='Path to the old binary to use for upgrade tests')
    parser.add_argument('-S', '--symbolicate', action='store_true', default=False,
                        help='Symbolicate backtraces in trace events')
    parser.add_argument('--config', default=None,
                        help='Configuration type to test')
    parser.add_argument('--crash', action='store_true', default=False,
                        help='Test ASSERT failures should crash the test')
    parser.add_argument('--aggregate-traces', default='NONE',
                        choices=['NONE', 'FAILED', 'ALL'])
    parser.add_argument('--keep-logs', default='FAILED',
                        choices=['NONE', 'FAILED', 'ALL'])
    parser.add_argument('--keep-simdirs', default='NONE',
                        choices=['NONE', 'FAILED', 'ALL'])
    parser.add_argument('testfile', nargs="+", help='The tests to run')
    parser.add_argument('--use-valgrind', action='store_true', default=False,
                        help='Run under valgrind')
    args = parser.parse_args()
    init_logging(args.loglevel, args.logdir)
    basedir = os.getcwd()
    if args.builddir is not None:
        basedir = args.builddir
    res = run_simulation_test(basedir, args)
    sys.exit(0 if res else 1)
