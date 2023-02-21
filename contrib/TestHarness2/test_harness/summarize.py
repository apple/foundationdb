from __future__ import annotations

import collections
import inspect
import json
import os
import re
import sys
import traceback
import uuid
import xml.sax
import xml.sax.handler
import xml.sax.saxutils

from pathlib import Path
from typing import List, Dict, TextIO, Callable, Optional, OrderedDict, Any, Tuple, Iterator, Iterable

from test_harness.config import config
from test_harness.valgrind import parse_valgrind_output


class SummaryTree:
    def __init__(self, name: str):
        self.name = name
        self.children: List[SummaryTree] = []
        self.attributes: Dict[str, str] = {}

    def append(self, element: SummaryTree):
        self.children.append(element)

    def to_dict(self, add_name: bool = True) -> Dict[str, Any] | List[Any]:
        if len(self.children) > 0 and len(self.attributes) == 0:
            children = []
            for child in self.children:
                children.append(child.to_dict())
            if add_name:
                return {self.name: children}
            else:
                return children
        res: Dict[str, Any] = {}
        if add_name:
            res['Type'] = self.name
        for k, v in self.attributes.items():
            res[k] = v
        children = []
        child_keys: Dict[str, int] = {}
        for child in self.children:
            if child.name in child_keys:
                child_keys[child.name] += 1
            else:
                child_keys[child.name] = 1
        for child in self.children:
            if child_keys[child.name] == 1 and child.name not in self.attributes:
                res[child.name] = child.to_dict(add_name=False)
            else:
                children.append(child.to_dict())
        if len(children) > 0:
            res['children'] = children
        return res

    def to_json(self, out: TextIO, prefix: str = ''):
        res = json.dumps(self.to_dict(), indent=('  ' if config.pretty_print else None))
        for line in res.splitlines(False):
            out.write('{}{}\n'.format(prefix, line))

    def to_xml(self, out: TextIO, prefix: str = ''):
        # minidom doesn't support omitting the xml declaration which is a problem for joshua
        # However, our xml is very simple and therefore serializing manually is easy enough
        attrs = []
        print_width = 120
        try:
            print_width, _ = os.get_terminal_size()
        except OSError:
            pass
        for k, v in self.attributes.items():
            attrs.append('{}={}'.format(k, xml.sax.saxutils.quoteattr(v)))
        elem = '{}<{}{}'.format(prefix, self.name, ('' if len(attrs) == 0 else ' '))
        out.write(elem)
        if config.pretty_print:
            curr_line_len = len(elem)
            for i in range(len(attrs)):
                attr_len = len(attrs[i])
                if i == 0 or attr_len + curr_line_len + 1 <= print_width:
                    if i != 0:
                        out.write(' ')
                    out.write(attrs[i])
                    curr_line_len += attr_len
                else:
                    out.write('\n')
                    out.write(' ' * len(elem))
                    out.write(attrs[i])
                    curr_line_len = len(elem) + attr_len
        else:
            out.write(' '.join(attrs))
        if len(self.children) == 0:
            out.write('/>')
        else:
            out.write('>')
        for child in self.children:
            if config.pretty_print:
                out.write('\n')
            child.to_xml(out, prefix=('  {}'.format(prefix) if config.pretty_print else prefix))
        if len(self.children) > 0:
            out.write('{}{}</{}>'.format(('\n' if config.pretty_print else ''), prefix, self.name))

    def dump(self, out: TextIO, prefix: str = '', new_line: bool = True):
        if config.output_format == 'json':
            self.to_json(out, prefix=prefix)
        else:
            self.to_xml(out, prefix=prefix)
        if new_line:
            out.write('\n')


ParserCallback = Callable[[Dict[str, str]], Optional[str]]


class ParseHandler:
    def __init__(self, out: SummaryTree):
        self.out = out
        self.events: OrderedDict[Optional[Tuple[str, Optional[str]]], List[ParserCallback]] = collections.OrderedDict()

    def add_handler(self, attr: Tuple[str, Optional[str]], callback: ParserCallback) -> None:
        self.events.setdefault(attr, []).append(callback)

    def _call(self, callback: ParserCallback, attrs: Dict[str, str]) -> str | None:
        try:
            return callback(attrs)
        except Exception as e:
            _, _, exc_traceback = sys.exc_info()
            child = SummaryTree('NonFatalParseError')
            child.attributes['Severity'] = '30'
            child.attributes['ErrorMessage'] = str(e)
            child.attributes['Trace'] = repr(traceback.format_tb(exc_traceback))
            self.out.append(child)
            return None

    def handle(self, attrs: Dict[str, str]):
        if None in self.events:
            for callback in self.events[None]:
                self._call(callback, attrs)
        for k, v in attrs.items():
            if (k, None) in self.events:
                for callback in self.events[(k, None)]:
                    remap = self._call(callback, attrs)
                    if remap is not None:
                        v = remap
                        attrs[k] = v
            if (k, v) in self.events:
                for callback in self.events[(k, v)]:
                    remap = self._call(callback, attrs)
                    if remap is not None:
                        v = remap
                        attrs[k] = v


class Parser:
    def parse(self, file: TextIO, handler: ParseHandler) -> None:
        pass


class XmlParser(Parser, xml.sax.handler.ContentHandler, xml.sax.handler.ErrorHandler):
    def __init__(self):
        super().__init__()
        self.handler: ParseHandler | None = None

    def parse(self, file: TextIO, handler: ParseHandler) -> None:
        self.handler = handler
        xml.sax.parse(file, self, errorHandler=self)

    def error(self, exception):
        pass

    def fatalError(self, exception):
        pass

    def startElement(self, name, attrs) -> None:
        attributes: Dict[str, str] = {}
        for name in attrs.getNames():
            attributes[name] = attrs.getValue(name)
        assert self.handler is not None
        self.handler.handle(attributes)


class JsonParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, file: TextIO, handler: ParseHandler):
        for line in file:
            obj = json.loads(line)
            handler.handle(obj)


class Coverage:
    def __init__(self, file: str, line: str | int, comment: str | None = None, rare: bool = False):
        self.file = file
        self.line = int(line)
        self.comment = comment
        self.rare = rare

    def to_tuple(self) -> Tuple[str, int, str | None]:
        return self.file, self.line, self.comment, self.rare

    def __eq__(self, other) -> bool:
        if isinstance(other, tuple) and len(other) == 4:
            return self.to_tuple() == other
        elif isinstance(other, Coverage):
            return self.to_tuple() == other.to_tuple()
        else:
            return False

    def __lt__(self, other) -> bool:
        if isinstance(other, tuple) and len(other) == 4:
            return self.to_tuple() < other
        elif isinstance(other, Coverage):
            return self.to_tuple() < other.to_tuple()
        else:
            return False

    def __le__(self, other) -> bool:
        if isinstance(other, tuple) and len(other) == 4:
            return self.to_tuple() <= other
        elif isinstance(other, Coverage):
            return self.to_tuple() <= other.to_tuple()
        else:
            return False

    def __gt__(self, other: Coverage) -> bool:
        if isinstance(other, tuple) and len(other) == 4:
            return self.to_tuple() > other
        elif isinstance(other, Coverage):
            return self.to_tuple() > other.to_tuple()
        else:
            return False

    def __ge__(self, other):
        if isinstance(other, tuple) and len(other) == 4:
            return self.to_tuple() >= other
        elif isinstance(other, Coverage):
            return self.to_tuple() >= other.to_tuple()
        else:
            return False

    def __hash__(self):
        return hash((self.file, self.line, self.comment, self.rare))


class TraceFiles:
    def __init__(self, path: Path):
        self.path: Path = path
        self.timestamps: List[int] = []
        self.runs: OrderedDict[int, List[Path]] = collections.OrderedDict()
        trace_expr = re.compile(r'trace.*\.(json|xml)')
        for file in self.path.iterdir():
            if file.is_file() and trace_expr.match(file.name) is not None:
                ts = int(file.name.split('.')[6])
                if ts in self.runs:
                    self.runs[ts].append(file)
                else:
                    self.timestamps.append(ts)
                    self.runs[ts] = [file]
        self.timestamps.sort(reverse=True)

    def __getitem__(self, idx: int) -> List[Path]:
        res = self.runs[self.timestamps[idx]]
        res.sort()
        return res

    def __len__(self) -> int:
        return len(self.runs)

    def items(self) -> Iterator[List[Path]]:
        class TraceFilesIterator(Iterable[List[Path]]):
            def __init__(self, trace_files: TraceFiles):
                self.current = 0
                self.trace_files: TraceFiles = trace_files

            def __iter__(self):
                return self

            def __next__(self) -> List[Path]:
                if len(self.trace_files) <= self.current:
                    raise StopIteration
                self.current += 1
                return self.trace_files[self.current - 1]

        return TraceFilesIterator(self)


class Summary:
    def __init__(self, binary: Path, runtime: float = 0, max_rss: int | None = None,
                 was_killed: bool = False, uid: uuid.UUID | None = None, expected_unseed: int | None = None,
                 exit_code: int = 0, valgrind_out_file: Path | None = None, stats: str | None = None,
                 error_out: str = None, will_restart: bool = False, long_running: bool = False):
        self.binary = binary
        self.runtime: float = runtime
        self.max_rss: int | None = max_rss
        self.was_killed: bool = was_killed
        self.long_running = long_running
        self.expected_unseed: int | None = expected_unseed
        self.exit_code: int = exit_code
        self.out: SummaryTree = SummaryTree('Test')
        self.test_begin_found: bool = False
        self.test_end_found: bool = False
        self.unseed: int | None = None
        self.valgrind_out_file: Path | None = valgrind_out_file
        self.severity_map: OrderedDict[tuple[str, int], int] = collections.OrderedDict()
        self.error: bool = False
        self.errors: int = 0
        self.warnings: int = 0
        self.coverage: OrderedDict[Coverage, bool] = collections.OrderedDict()
        self.test_count: int = 0
        self.tests_passed: int = 0
        self.error_out = error_out
        self.stderr_severity: str = '40'
        self.will_restart: bool = will_restart
        self.test_dir: Path | None = None

        if uid is not None:
            self.out.attributes['TestUID'] = str(uid)
        if stats is not None:
            self.out.attributes['Statistics'] = stats
        self.out.attributes['JoshuaSeed'] = str(config.joshua_seed)
        self.out.attributes['WillRestart'] = '1' if self.will_restart else '0'

        self.handler = ParseHandler(self.out)
        self.register_handlers()

    def summarize_files(self, trace_files: List[Path]):
        assert len(trace_files) > 0
        for f in trace_files:
            self.parse_file(f)
        self.done()

    def summarize(self, trace_dir: Path, command: str):
        self.test_dir = trace_dir
        trace_files = TraceFiles(trace_dir)
        if len(trace_files) == 0:
            self.error = True
            child = SummaryTree('NoTracesFound')
            child.attributes['Severity'] = '40'
            child.attributes['Path'] = str(trace_dir.absolute())
            child.attributes['Command'] = command
            self.out.append(child)
            return
        self.summarize_files(trace_files[0])
        if config.joshua_dir is not None:
            import test_harness.fdb
            test_harness.fdb.write_coverage(config.cluster_file,
                                            test_harness.fdb.str_to_tuple(config.joshua_dir) + ('coverage',),
                                            test_harness.fdb.str_to_tuple(config.joshua_dir) + ('coverage-metadata',),
                                            self.coverage)

    def list_simfdb(self) -> SummaryTree:
        res = SummaryTree('SimFDB')
        res.attributes['TestDir'] = str(self.test_dir)
        if self.test_dir is None:
            return res
        simfdb = self.test_dir / Path('simfdb')
        if not simfdb.exists():
            res.attributes['NoSimDir'] = "simfdb doesn't exist"
            return res
        elif not simfdb.is_dir():
            res.attributes['NoSimDir'] = 'simfdb is not a directory'
            return res
        for file in simfdb.iterdir():
            child = SummaryTree('Directory' if file.is_dir() else 'File')
            child.attributes['Name'] = file.name
            res.append(child)
        return res

    def ok(self):
        return not self.error

    def done(self):
        if config.print_coverage:
            for k, v in self.coverage.items():
                child = SummaryTree('CodeCoverage')
                child.attributes['File'] = k.file
                child.attributes['Line'] = str(k.line)
                child.attributes['Rare'] = k.rare
                if not v:
                    child.attributes['Covered'] = '0'
                if k.comment is not None and len(k.comment):
                    child.attributes['Comment'] = k.comment
                self.out.append(child)
        if self.warnings > config.max_warnings:
            child = SummaryTree('WarningLimitExceeded')
            child.attributes['Severity'] = '30'
            child.attributes['WarningCount'] = str(self.warnings)
            self.out.append(child)
        if self.errors > config.max_errors:
            child = SummaryTree('ErrorLimitExceeded')
            child.attributes['Severity'] = '40'
            child.attributes['ErrorCount'] = str(self.errors)
            self.out.append(child)
            self.error = True
        if self.was_killed:
            child = SummaryTree('ExternalTimeout')
            child.attributes['Severity'] = '40'
            if self.long_running:
                # debugging info for long-running tests
                child.attributes['LongRunning'] = '1'
                child.attributes['Runtime'] = str(self.runtime)
            self.out.append(child)
            self.error = True
        if self.max_rss is not None:
            self.out.attributes['PeakMemory'] = str(self.max_rss)
        if self.valgrind_out_file is not None:
            try:
                valgrind_errors = parse_valgrind_output(self.valgrind_out_file)
                for valgrind_error in valgrind_errors:
                    if valgrind_error.kind.startswith('Leak'):
                        continue
                    self.error = True
                    child = SummaryTree('ValgrindError')
                    child.attributes['Severity'] = '40'
                    child.attributes['What'] = valgrind_error.what.what
                    child.attributes['Backtrace'] = valgrind_error.what.backtrace
                    aux_count = 0
                    for aux in valgrind_error.aux:
                        child.attributes['WhatAux{}'.format(aux_count)] = aux.what
                        child.attributes['BacktraceAux{}'.format(aux_count)] = aux.backtrace
                        aux_count += 1
                    self.out.append(child)
            except Exception as e:
                self.error = True
                child = SummaryTree('ValgrindParseError')
                child.attributes['Severity'] = '40'
                child.attributes['ErrorMessage'] = str(e)
                _, _, exc_traceback = sys.exc_info()
                child.attributes['Trace'] = repr(traceback.format_tb(exc_traceback))
                self.out.append(child)
        if not self.test_end_found:
            child = SummaryTree('TestUnexpectedlyNotFinished')
            child.attributes['Severity'] = '40'
            self.out.append(child)
            self.error = True
        if self.error_out is not None and len(self.error_out) > 0:
            lines = self.error_out.splitlines()
            stderr_bytes = 0
            for line in lines:
                if line.endswith(
                        "WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!"):
                    # When running ASAN we expect to see this message. Boost coroutine should be using the correct asan annotations so that it shouldn't produce any false positives.
                    continue
                if line.endswith("Warning: unimplemented fcntl command: 1036"):
                    # Valgrind produces this warning when F_SET_RW_HINT is used
                    continue
                if self.stderr_severity == '40':
                    self.error = True
                remaining_bytes = config.max_stderr_bytes - stderr_bytes
                if remaining_bytes > 0:
                    out_err = line[0:remaining_bytes] + ('...' if len(line) > remaining_bytes else '')
                    child = SummaryTree('StdErrOutput')
                    child.attributes['Severity'] = self.stderr_severity
                    child.attributes['Output'] = out_err
                    self.out.append(child)
                stderr_bytes += len(line)
            if stderr_bytes > config.max_stderr_bytes:
                child = SummaryTree('StdErrOutputTruncated')
                child.attributes['Severity'] = self.stderr_severity
                child.attributes['BytesRemaining'] = str(stderr_bytes - config.max_stderr_bytes)
                self.out.append(child)

        self.out.attributes['Ok'] = '1' if self.ok() else '0'
        if not self.ok():
            reason = 'Unknown'
            if self.error:
                reason = 'ProducedErrors'
            elif not self.test_end_found:
                reason = 'TestDidNotFinish'
            elif self.tests_passed == 0:
                reason = 'NoTestsPassed'
            elif self.test_count != self.tests_passed:
                reason = 'Expected {} tests to pass, but only {} did'.format(self.test_count, self.tests_passed)
            self.out.attributes['FailReason'] = reason

    def parse_file(self, file: Path):
        parser: Parser
        if file.suffix == '.json':
            parser = JsonParser()
        elif file.suffix == '.xml':
            parser = XmlParser()
        else:
            child = SummaryTree('TestHarnessBug')
            child.attributes['File'] = __file__
            frame = inspect.currentframe()
            if frame is not None:
                child.attributes['Line'] = str(inspect.getframeinfo(frame).lineno)
            child.attributes['Details'] = 'Unexpected suffix {} for file {}'.format(file.suffix, file.name)
            self.error = True
            self.out.append(child)
            return
        with file.open('r') as f:
            try:
                parser.parse(f, self.handler)
            except Exception as e:
                child = SummaryTree('SummarizationError')
                child.attributes['Severity'] = '40'
                child.attributes['ErrorMessage'] = str(e)
                self.out.append(child)

    def register_handlers(self):
        def remap_event_severity(attrs):
            if 'Type' not in attrs or 'Severity' not in attrs:
                return None
            k = (attrs['Type'], int(attrs['Severity']))
            if k in self.severity_map:
                return str(self.severity_map[k])

        self.handler.add_handler(('Severity', None), remap_event_severity)

        def program_start(attrs: Dict[str, str]):
            if self.test_begin_found:
                return
            self.test_begin_found = True
            self.out.attributes['RandomSeed'] = attrs['RandomSeed']
            self.out.attributes['SourceVersion'] = attrs['SourceVersion']
            self.out.attributes['Time'] = attrs['ActualTime']
            self.out.attributes['BuggifyEnabled'] = attrs['BuggifyEnabled']
            self.out.attributes['DeterminismCheck'] = '0' if self.expected_unseed is None else '1'
            if self.binary.name != 'fdbserver':
                self.out.attributes['OldBinary'] = self.binary.name
            if 'FaultInjectionEnabled' in attrs:
                self.out.attributes['FaultInjectionEnabled'] = attrs['FaultInjectionEnabled']

        self.handler.add_handler(('Type', 'ProgramStart'), program_start)

        def config_string(attrs: Dict[str, str]):
            self.out.attributes['ConfigString'] = attrs['ConfigString']

        self.handler.add_handler(('Type', 'SimulatorConfig'), config_string)

        def set_test_file(attrs: Dict[str, str]):
            test_file = Path(attrs['TestFile'])
            cwd = Path('.').absolute()
            try:
                test_file = test_file.relative_to(cwd)
            except ValueError:
                pass
            self.out.attributes['TestFile'] = str(test_file)

        self.handler.add_handler(('Type', 'Simulation'), set_test_file)
        self.handler.add_handler(('Type', 'NonSimulationTest'), set_test_file)

        def set_elapsed_time(attrs: Dict[str, str]):
            if self.test_end_found:
                return
            self.test_end_found = True
            self.unseed = int(attrs['RandomUnseed'])
            if self.expected_unseed is not None and self.unseed != self.expected_unseed:
                severity = 40 if ('UnseedMismatch', 40) not in self.severity_map \
                    else self.severity_map[('UnseedMismatch', 40)]
                if severity >= 30:
                    child = SummaryTree('UnseedMismatch')
                    child.attributes['Unseed'] = str(self.unseed)
                    child.attributes['ExpectedUnseed'] = str(self.expected_unseed)
                    child.attributes['Severity'] = str(severity)
                    if severity >= 40:
                        self.error = True
                    self.out.append(child)
            self.out.attributes['SimElapsedTime'] = attrs['SimTime']
            self.out.attributes['RealElapsedTime'] = attrs['RealTime']
            if self.unseed is not None:
                self.out.attributes['RandomUnseed'] = str(self.unseed)

        self.handler.add_handler(('Type', 'ElapsedTime'), set_elapsed_time)

        def parse_warning(attrs: Dict[str, str]):
            self.warnings += 1
            if self.warnings > config.max_warnings:
                return
            child = SummaryTree(attrs['Type'])
            for k, v in attrs.items():
                if k != 'Type':
                    child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(('Severity', '30'), parse_warning)

        def parse_error(attrs: Dict[str, str]):
            if 'ErrorIsInjectedFault' in attrs and attrs['ErrorIsInjectedFault'].lower() in ['1', 'true']:
                # ignore injected errors. In newer fdb versions these will have a lower severity
                return
            self.errors += 1
            self.error = True
            if self.errors > config.max_errors:
                return
            child = SummaryTree(attrs['Type'])
            for k, v in attrs.items():
                child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(('Severity', '40'), parse_error)

        def coverage(attrs: Dict[str, str]):
            covered = True
            if 'Covered' in attrs:
                covered = int(attrs['Covered']) != 0
            comment = ''
            if 'Comment' in attrs:
                comment = attrs['Comment']
            rare = False
            if 'Rare' in attrs:
                rare = bool(int(attrs['Rare']))
            c = Coverage(attrs['File'], attrs['Line'], comment, rare)
            if covered or c not in self.coverage:
                self.coverage[c] = covered

        self.handler.add_handler(('Type', 'CodeCoverage'), coverage)

        def expected_test_pass(attrs: Dict[str, str]):
            self.test_count = int(attrs['Count'])

        self.handler.add_handler(('Type', 'TestsExpectedToPass'), expected_test_pass)

        def test_passed(attrs: Dict[str, str]):
            if attrs['Passed'] == '1':
                self.tests_passed += 1

        self.handler.add_handler(('Type', 'TestResults'), test_passed)

        def remap_event_severity(attrs: Dict[str, str]):
            self.severity_map[(attrs['TargetEvent'], int(attrs['OriginalSeverity']))] = int(attrs['NewSeverity'])

        self.handler.add_handler(('Type', 'RemapEventSeverity'), remap_event_severity)

        def buggify_section(attrs: Dict[str, str]):
            if attrs['Type'] == 'FaultInjected' or attrs.get('Activated', '0') == '1':
                child = SummaryTree(attrs['Type'])
                child.attributes['File'] = attrs['File']
                child.attributes['Line'] = attrs['Line']
                self.out.append(child)

        self.handler.add_handler(('Type', 'BuggifySection'), buggify_section)
        self.handler.add_handler(('Type', 'FaultInjected'), buggify_section)

        def running_unit_test(attrs: Dict[str, str]):
            child = SummaryTree('RunningUnitTest')
            child.attributes['Name'] = attrs['Name']
            child.attributes['File'] = attrs['File']
            child.attributes['Line'] = attrs['Line']

        self.handler.add_handler(('Type', 'RunningUnitTest'), running_unit_test)

        def stderr_severity(attrs: Dict[str, str]):
            if 'NewSeverity' in attrs:
                self.stderr_severity = attrs['NewSeverity']

        self.handler.add_handler(('Type', 'StderrSeverity'), stderr_severity)
