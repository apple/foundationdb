#!/usr/bin/env python3

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
    def __init__(self, basedir, name, infile, out):
        self.basedir = basedir
        self.name = name
        self.infile = infile
        self.out = out
        self.backtraces = []
        self.result = True
        self.address_re = re.compile(r'(0x[0-9a-f]+\s+)+')

    def writeHeader(self):
        pass

    def writeFooter(self):
        pass

    def applyAddr2line(self, obj):
        addresses = self.sanitizeBacktrace(obj)
        assert addresses is not None
        fdbbin = os.path.join(basedir, 'bin', 'fdbserver')
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
        backtraces = 0
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
                # FIXME: I don't know if this is actually a failure or not...
                #if obj['Type'] == 'TestFailure':
                #    self.result = False
                if obj['Severity'] == '40':
                    self.result = False
                if self.name is not None:
                    obj['testname'] = self.name
                if self.sanitizeBacktrace(obj) is not None and backtraces == 0:
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
                self.result = False
            else:
                return_code_trace['Severity'] = '10'
                return_code_trace['Type'] = 'ReturnCode'
            return_code_trace['Command'] = command
            return_code_trace['ReturnCode'] = return_code
            return_code_trace['testname'] = self.name
            self.writeObject(return_code_trace)



class JSONParser(LogParser):
    def __init__(self, basedir, name, infile, out):
        super().__init__(basedir, name, infile, out)

    def processLine(self, line, linenr):
        try:
            return json.loads(line)
        except Exception as e:
            self.log_trace_parse_error(linenr, e)

    def writeObject(self, obj):
        json.dump(obj, self.out)
        self.out.write('\n')



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

    def __init__(self, basedir, name, infile, out):
        super().__init__(basedir, name, infile, out)

    def writeHeader(self):
        self.out.write('<?xml version="1.0"?>\n<Trace>\n')

    def writeFooter(self):
        self.out.write("</Trace>")

    def writeObject(self, obj):
        self.out.write('<Event')
        for (key, value) in obj.items():
            self.out.write(' {}="{}"'.format(key, value))
        self.out.write('/>\n')

    def processLine(self, line, linenr):
        if linenr < 3:
            # the first two lines don't need to be parsed
            return None
        if line.startswith('</'):
            # don't parse the closing element
            return None
        handler = XMLParser.XMLHandler()
        errorHandler = XMLParser.XMLErrorHandler()
        xml.sax.parseString(line, handler, errorHandler=errorHandler)
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


def process_traces(basedir, testname, path, out, log_format, return_codes):
    res = True
    backtraces = []
    parser = None
    for trace in get_traces(path, log_format):
        if log_format == 'json':
            parser = JSONParser(basedir, testname, trace, out)
        else:
            parser = XMLParser(basedir, testname, trace, out)
    parser.processTraces()
    res = res and parser.result
    if log_format == 'json':
        parser = JSONParser(basedir, testname, trace, out)
    else:
        parser = XMLParser(basedir, testname, trace, out)
    parser.processReturnCodes(return_codes)
    return res and parser.result

def run_simulation_test(basedir,
                        testtype,
                        testname,
                        testfiles,
                        log_format,
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
    # FIXME: include these lines as soon as json support is added
    #pargs.append('--trace_format')
    #pargs.append(log_format)
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
    outfile = os.path.join(test_dir, 'traces.{}'.format(log_format))
    res = True
    with open(outfile, 'a') as f:
        os.lockf(f.fileno(), os.F_LOCK, 0)
        pos = f.tell()
        res = process_traces(basedir, testname,
                             os.path.join(os.getcwd(), wd), f, log_format,
                             return_codes)
        f.seek(pos)
        os.lockf(f.fileno(), os.F_ULOCK, 0)
    if res:
        shutil.rmtree(wd)
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
    parser.add_argument('-F', '--log-format', required=False, default='xml',
                        choices=['xml', 'json'], help='Log format (json or xml)')
    parser.add_argument('testfile', nargs="+", help='The tests to run')
    args = parser.parse_args()
    init_logging(args.loglevel, args.logdir)
    basedir = os.getcwd()
    if args.builddir is not None:
        basedir = args.builddir
    res = run_simulation_test(basedir, args.testtype, args.name,
                              args.testfile, args.log_format, args.restart, args.buggify,
                              args.seed)
    sys.exit(0 if res else 1)
