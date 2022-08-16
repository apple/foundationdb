from __future__ import annotations

import collections
import inspect
import json
import re
import sys
import traceback
import uuid
import xml.sax
import xml.sax.handler
import xml.sax.saxutils

from pathlib import Path
from typing import List, Dict, TextIO, Callable, Optional, OrderedDict, Any, Tuple
from xml.dom import minidom

from test_harness.config import config


class SummaryTree:
    def __init__(self, name: str):
        self.name = name
        self.children: List[SummaryTree] = []
        self.attributes: Dict[str, str] = {}

    def append(self, element: SummaryTree):
        self.children.append(element)

    def to_dict(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {'Type': self.name}
        for k, v in self.attributes.items():
            res[k] = v
        children = []
        for child in self.children:
            children.append(child.to_dict())
        if len(children) > 0:
            res['children'] = children
        return res

    def to_json(self, out: TextIO):
        json.dump(self.to_dict(), out, indent=('  ' if config.pretty_print else None))

    def to_xml(self, out: TextIO, prefix: str = ''):
        # minidom doesn't support omitting the xml declaration which is a problem for joshua
        # However, our xml is very simple and therefore serializing manually is easy enough
        attrs = []
        for k, v in self.attributes.items():
            attrs.append('{}="{}"'.format(k, xml.sax.saxutils.escape(v)))
        elem = '{}<{}{}'.format(prefix, self.name, ('' if len(attrs) == 0 else ' '))
        out.write(elem)
        if config.pretty_print:
            curr_line_len = len(elem)
            for i in range(len(attrs)):
                attr_len = len(attrs[i])
                if i == 0 or attr_len + curr_line_len + 1 <= 120:
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
            out.write('{}</{}>'.format(('\n' if config.pretty_print else ''), self.name))

    def dump(self, out: TextIO):
        if config.output_format == 'json':
            self.to_json(out)
        else:
            self.to_xml(out)
        out.write('\n')


ParserCallback = Callable[[Dict[str, str]], Optional[str]]


class ParseHandler:
    def __init__(self, out: SummaryTree):
        self.out = out
        self.events: OrderedDict[Optional[Tuple[str, Optional[str]]], List[ParserCallback]] = collections.OrderedDict()

    def add_handler(self, attr: Tuple[str, str], callback: ParserCallback) -> None:
        if attr in self.events:
            self.events[attr].append(callback)
        else:
            self.events[attr] = [callback]

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


class XmlParser(Parser, xml.sax.handler.ContentHandler):
    def __init__(self):
        super().__init__()
        self.handler: ParseHandler | None = None

    def parse(self, file: TextIO, handler: ParseHandler) -> None:
        xml.sax.parse(file, self)

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


def format_test_error(attrs: Dict[str, str], include_details: bool) -> str:
    trace_type = attrs['Type']
    res = trace_type
    if trace_type == 'InternalError':
        res = '{} {} {}'.format(trace_type, attrs['File'], attrs['Line'])
    elif trace_type == 'TestFailure':
        res = '{} {}'.format(trace_type, attrs['Reason'])
    elif trace_type == 'ValgrindError':
        res = '{} {}'.format(trace_type, attrs['What'])
    elif trace_type == 'ExitCode':
        res = '{0} 0x{1:x}'.format(trace_type, int(attrs['Code']))
    elif trace_type == 'StdErrOutput':
        res = '{}: {}'.format(trace_type, attrs['Output'])
    elif trace_type == 'BTreeIntegrityCheck':
        res = '{}: {}'.format(trace_type, attrs['ErrorDetail'])
    for k in ['Error', 'WinErrorCode', 'LinuxErrorCode']:
        if k in attrs:
            res += ' {}'.format(attrs[k])
    if 'Status' in attrs:
        res += ' Status={}'.format(attrs['Status'])
    if 'In' in attrs:
        res += ' In {}'.format(attrs['In'])
    if 'SQLiteError' in attrs:
        res += ' SQLiteError={0}({1})'.format(attrs['SQLiteError'], attrs['SQLiteErrorCode'])
    if 'Details' in attrs and include_details:
        res += ': {}'.format(attrs['Details'])
    return res


class ValgrindError:
    def __init__(self, what: str = '', kind: str = ''):
        self.what: str = what
        self.kind: str = kind

    def __str__(self):
        return 'ValgrindError(what="{}", kind="{}")'.format(self.what, self.kind)


class ValgrindHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
        super().__init__()
        self.stack: List[ValgrindError] = []
        self.result: List[ValgrindError] = []
        self.in_kind = False
        self.in_what = False

    @staticmethod
    def from_content(content):
        if isinstance(content, bytes):
            return content.decode()
        assert isinstance(content, str)
        return content

    def characters(self, content):
        if len(self.stack) == 0:
            return
        elif self.in_kind:
            self.stack[-1].kind += self.from_content(content)
        elif self.in_what:
            self.stack[-1].what += self.from_content(content)

    def startElement(self, name, attrs):
        if name == 'error':
            self.stack.append(ValgrindError())
        if len(self.stack) == 0:
            return
        if name == 'kind':
            self.in_kind = True
        elif name == 'what':
            self.in_what = True

    def endElement(self, name):
        if name == 'error':
            self.result.append(self.stack.pop())
        elif name == 'kind':
            self.in_kind = False
        elif name == 'what':
            self.in_what = False


def parse_valgrind_output(valgrind_out_file: Path) -> List[str]:
    res: List[str] = []
    handler = ValgrindHandler()
    with valgrind_out_file.open('r') as f:
        xml.sax.parse(f, handler)
    for err in handler.result:
        if err.kind.startswith('Leak'):
            continue
        res.append(err.kind)
    return res


class Coverage:
    def __init__(self, file: str, line: str | int, comment: str | None = None):
        self.file = file
        self.line = int(line)
        self.comment = comment

    def to_tuple(self) -> Tuple[str, int, str | None]:
        return self.file, self.line, self.comment

    def __eq__(self, other) -> bool:
        if isinstance(other, tuple) and len(other) == 3:
            return self.to_tuple() == other
        elif isinstance(other, Coverage):
            return self.to_tuple() == other.to_tuple()
        else:
            return False

    def __lt__(self, other) -> bool:
        if isinstance(other, tuple) and len(other) == 3:
            return self.to_tuple() < other
        elif isinstance(other, Coverage):
            return self.to_tuple() < other.to_tuple()
        else:
            return False

    def __le__(self, other) -> bool:
        if isinstance(other, tuple) and len(other) == 3:
            return self.to_tuple() <= other
        elif isinstance(other, Coverage):
            return self.to_tuple() <= other.to_tuple()
        else:
            return False

    def __gt__(self, other: Coverage) -> bool:
        if isinstance(other, tuple) and len(other) == 3:
            return self.to_tuple() > other
        elif isinstance(other, Coverage):
            return self.to_tuple() > other.to_tuple()
        else:
            return False

    def __ge__(self, other):
        if isinstance(other, tuple) and len(other) == 3:
            return self.to_tuple() >= other
        elif isinstance(other, Coverage):
            return self.to_tuple() >= other.to_tuple()
        else:
            return False

    def __hash__(self):
        return hash((self.file, self.line, self.comment))


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
        self.timestamps.sort()

    def __getitem__(self, idx: int) -> List[Path]:
        res = self.runs[self.timestamps[idx]]
        res.sort()
        return res

    def __len__(self) -> int:
        return len(self.runs)


class Summary:
    def __init__(self, binary: Path, runtime: float = 0, max_rss: int | None = None,
                 was_killed: bool = False, uid: uuid.UUID | None = None, expected_unseed: int | None = None,
                 exit_code: int = 0, valgrind_out_file: Path | None = None, stats: str | None = None):
        self.binary = binary
        self.runtime: float = runtime
        self.max_rss: int | None = max_rss
        self.was_killed: bool = was_killed
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
        self.error_list: List[str] = []
        self.warnings: int = 0
        self.coverage: OrderedDict[Coverage, bool] = collections.OrderedDict()
        self.test_count: int = 0
        self.tests_passed: int = 0

        if uid is not None:
            self.out.attributes['TestUID'] = str(uid)
        if stats is not None:
            self.out.attributes['Statistics'] = stats
        self.out.attributes['JoshuaSeed'] = str(config.joshua_seed)

        self.handler = ParseHandler(self.out)
        self.register_handlers()

    def summarize(self, trace_dir: Path):
        trace_files = TraceFiles(trace_dir)
        if len(trace_files) == 0:
            self.error = True
            self.error_list.append('No traces produced')
            child = SummaryTree('NoTracesFound')
            child.attributes['Severity'] = '40'
            child.attributes['Path'] = str(trace_dir.absolute())
            self.out.append(child)
        for f in trace_files[0]:
            self.parse_file(f)
        self.done()

    def ok(self):
        return not self.error and self.tests_passed == self.test_count and self.tests_passed >= 0\
               and self.test_end_found

    def done(self):
        if config.print_coverage:
            for k, v in self.coverage.items():
                child = SummaryTree('CodeCoverage')
                child.attributes['File'] = k.file
                child.attributes['Line'] = str(k.line)
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
            child.attributes['ErrorCount'] = str(self.warnings)
            self.out.append(child)
            self.error_list.append('ErrorLimitExceeded')
        if self.was_killed:
            child = SummaryTree('ExternalTimeout')
            child.attributes['Severity'] = '40'
            self.out.append(child)
            self.error = True
        if self.max_rss is not None:
            self.out.attributes['PeakMemory'] = str(self.max_rss)
        if self.valgrind_out_file is not None:
            try:
                whats = parse_valgrind_output(self.valgrind_out_file)
                for what in whats:
                    self.error = True
                    child = SummaryTree('ValgrindError')
                    child.attributes['Severity'] = '40'
                    child.attributes['What'] = what
                    self.out.append(child)
            except Exception as e:
                self.error = True
                child = SummaryTree('ValgrindParseError')
                child.attributes['Severity'] = '40'
                child.attributes['ErrorMessage'] = str(e)
                self.out.append(child)
                self.error_list.append('Failed to parse valgrind output: {}'.format(str(e)))
        if not self.test_end_found:
            child = SummaryTree('TestUnexpectedlyNotFinished')
            child.attributes['Severity'] = '40'
            self.out.append(child)
        self.out.attributes['Ok'] = '1' if self.ok() else '0'

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
                self.error_list.append('SummarizationError {}'.format(str(e)))

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
                        self.error_list.append('UnseedMismatch')
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
            self.errors += 1
            self.error = True
            if self.errors > config.max_errors:
                return
            child = SummaryTree(attrs['Type'])
            for k, v in attrs.items():
                child.attributes[k] = v
            self.out.append(child)
            self.error_list.append(format_test_error(attrs, True))

        self.handler.add_handler(('Severity', '40'), parse_error)

        def coverage(attrs: Dict[str, str]):
            covered = True
            if 'Covered' in attrs:
                covered = int(attrs['Covered']) != 0
            comment = ''
            if 'Comment' in attrs:
                comment = attrs['Comment']
            c = Coverage(attrs['File'], attrs['Line'], comment)
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
