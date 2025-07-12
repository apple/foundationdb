from __future__ import annotations

import base64
import collections
import json
import math
import os
import re
import sys
import time
import traceback
import uuid
import xml.sax
import xml.sax.handler
import xml.sax.saxutils
import tarfile
import logging
import xml.etree.ElementTree as ET
import copy
import gzip
from typing import (
    List,
    Dict,
    Any,
    Optional,
    Callable,
    OrderedDict,
    TextIO,
    Tuple,
    Iterator,
    Iterable,
    Union,
    Set,
)

from pathlib import Path
from test_harness.config import config
from test_harness.valgrind import parse_valgrind_output

logger = logging.getLogger(__name__)

CURRENT_VERSION = "0.1"

# Define tags to strip from V1 stdout output
TAGS_TO_STRIP_FROM_TEST_ELEMENT_FOR_STDOUT: Set[str] = {
    "CodeCoverage",
    "ValgrindError",
    "StdErrOutput",
    "StdErrOutputTruncated",
    "JoshuaMessage",
}

# Essential child tags to keep in V1 output
ESSENTIAL_V1_CHILD_TAGS_TO_KEEP = {
    "SimFDB",
    "JoshuaLogTool",
    "DisableConnectionFailures_Tester",
    "EnableConnectionFailures_Tester",
    "ScheduleDisableConnectionFailures_Tester",
    "DisableConnectionFailures_BulkDumping",
    "DisableConnectionFailures_BulkLoading",
    "DisableConnectionFailures_ConsistencyCheck",
    "CommitProxyTerminated",
    "QuietDatabaseConsistencyCheckStartFail",
    "UnseedMismatch",
    "WarningLimitExceeded",
    "ErrorLimitExceeded",
}

# Tags to explicitly strip from V1 stdout
STDOUT_EXPLICITLY_STRIPPED_TAGS = {
    "CodeCoverage",
    "ValgrindError",
    "StdErrOutput",
    "StdErrOutputTruncated",
    "Knobs",
    "Metrics",
    "JoshuaMessage",
}

class ArchivalConfig:
    """Configuration for log archival functionality"""
    def __init__(self):
        self.enabled: bool = False
        self.paired_fdb_logs: List[Path] = []
        self.paired_harness_files: List[Path] = []
        self.current_fdb_logs: List[Path] = []
        self.joshua_output_dir: Optional[Path] = None
        self.run_temp_dir: Optional[Path] = None
        self.stdout_path: Optional[Path] = None
        self.stderr_path: Optional[Path] = None
        self.command_path: Optional[Path] = None

class SummaryTree:
    def __init__(self, name: str, is_root_document: bool = False):
        self.name = name
        self.children: List[SummaryTree] = []
        self.attributes: Dict[str, str] = {}
        self.root: Optional[ET.Element] = None
        if is_root_document:
            self.root = ET.Element(self.name)

    def append(self, element: SummaryTree):
        self.children.append(element)
        if self.root is not None:
            self.root.append(element.to_et_element())

    def to_et_element(self) -> ET.Element:
        element = ET.Element(self.name)
        for k, v_raw in self.attributes.items():
            element.set(str(k), xml.sax.saxutils.escape(str(v_raw)))
        
        for child_summary_tree in self.children:
            element.append(child_summary_tree.to_et_element())
        
        if self.root is None:
            self.root = element
        
        return element

    def to_dict(self, add_name: bool = True) -> Dict[str, Any] | List[Any]:
        if len(self.children) > 0 and len(self.attributes) == 0:
            res: List[Any] = []
            for child in self.children:
                res.append(child.to_dict())
            return res
        res: Dict[str, Any] = {}
        if add_name:
            res["tag"] = self.name
        for k, v in self.attributes.items():
            res[k] = v
        if len(self.children) > 0:
            res["children"] = []
            for child in self.children:
                res["children"].append(child.to_dict())
        return res

    def dump(self, out: TextIO, new_line: bool = True, prefix: str = ""):
        out.write(prefix)
        out.write("<")
        out.write(self.name)
        for k, v in self.attributes.items():
            out.write(' {}="{}"'.format(k, xml.sax.saxutils.escape(str(v))))
        if len(self.children) == 0:
            out.write("/>")
        else:
            out.write(">")
            if len(self.children) == 1 and len(self.children[0].children) == 0:
                self.children[0].dump(out, False, "")
            else:
                for child in self.children:
                    out.write("\n")
                    child.dump(out, False, prefix + "\t")
                out.write("\n")
                out.write(prefix)
            out.write("</")
            out.write(self.name)
            out.write(">")
        if new_line:
            out.write("\n")

    def to_string_document(self) -> str:
        """Serializes the entire XML tree to a string, including XML declaration."""
        if self.root is None:
            self.to_et_element()
        
        if self.root is None:
            return "<Error>Cannot serialize XML document: root element is missing</Error>"

        try:
            xml_str = ET.tostring(self.root, encoding='unicode', short_empty_elements=True)
            return f'<?xml version="1.0"?>\n{xml_str}'
        except Exception as e:
            logger.error(f"Error during XML serialization: {e}")
            return f'<?xml version="1.0"?>\n<Error>SerializationFailed: {xml.sax.saxutils.escape(str(e))}</Error>'

ParserCallback = Callable[[Dict[str, str]], Optional[str]]

class ParseHandler:
    def __init__(self, out: SummaryTree):
        self.out = out
        self.events: Dict[
            tuple[str, str] | tuple[str, None] | None, List[ParserCallback]
        ] = {}

    def add_handler(
        self, event: tuple[str, str] | tuple[str, None] | None, callback: ParserCallback
    ):
        if event not in self.events:
            self.events[event] = []
        self.events[event].append(callback)

    def _call(self, callback: ParserCallback, attrs: Dict[str, str]) -> Optional[str]:
        try:
            return callback(attrs)
        except Exception as e:
            logger.error(f"Error in parse handler: {e}")
            return None

    def handle(self, attrs: Dict[str, str]):
        # Call specific handlers first
        for k, v_attr in attrs.items():
            if (k, v_attr) in self.events:
                for callback in self.events[(k, v_attr)]:
                    self._call(callback, attrs)
            
            if (k, None) in self.events:
                for callback in self.events[(k, None)]:
                    remap = self._call(callback, attrs)
                    if remap is not None:
                        attrs[k] = remap

        # Call generic handlers
        if None in self.events:
            for callback in self.events[None]:
                self._call(callback, attrs)

class Parser:
    def parse(self, file: TextIO, handler: ParseHandler) -> None:
        raise NotImplementedError

class XmlParser(Parser, xml.sax.handler.ContentHandler, xml.sax.handler.ErrorHandler):
    def __init__(self):
        super().__init__()
        self.handler: ParseHandler | None = None

    def parse(self, file: TextIO, handler: ParseHandler):
        self.handler = handler
        parser = xml.sax.make_parser()
        parser.setContentHandler(self)
        parser.setErrorHandler(self)
        parser.parse(file)

    def startElement(self, name, attrs):
        if self.handler is not None:
            self.handler.handle(dict(attrs))

    def error(self, exception):
        pass

    def fatalError(self, exception):
        raise exception

    def warning(self, exception):
        pass

class JsonParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, file: TextIO, handler: ParseHandler):
        for line in file.readlines():
            handler.handle(json.loads(line))

class Coverage:
    def __init__(self, file: str, line: int, comment: str | None, rare: bool):
        self.file = file
        self.line = line
        self.comment = comment
        self.rare = rare

    def to_tuple(self) -> Tuple[str, int, str | None, bool]:
        return self.file, self.line, self.comment, self.rare

    def __eq__(self, other) -> bool:
        if not isinstance(other, Coverage):
            return NotImplemented
        return self.to_tuple() == other.to_tuple()

    def __lt__(self, other) -> bool:
        if not isinstance(other, Coverage):
            return NotImplemented
        return self.to_tuple() < other.to_tuple()

    def __le__(self, other) -> bool:
        if not isinstance(other, Coverage):
            return NotImplemented
        return self.to_tuple() <= other.to_tuple()

    def __gt__(self, other: Coverage) -> bool:
        if not isinstance(other, Coverage):
            return NotImplemented
        return self.to_tuple() > other.to_tuple()

    def __ge__(self, other):
        if not isinstance(other, Coverage):
            return NotImplemented
        return self.to_tuple() >= other.to_tuple()

    def __hash__(self):
        return hash(self.to_tuple())

class TraceFiles:
    def __init__(self, path: Path):
        self.traces: OrderedDict[int, List[Path]] = collections.OrderedDict()
        for file in path.glob("*.xml*"):
            match = re.search(r"trace\.(?:\d{10}\.\d{6})\.(\d+)\.xml", file.name)
            if match is None:
                continue
            ts = int(match.group(1))
            if ts not in self.traces:
                self.traces[ts] = []
            self.traces[ts].append(file)

    def __getitem__(self, idx: int) -> List[Path]:
        return list(self.traces.values())[idx]

    def __len__(self) -> int:
        return len(self.traces)

    def items(self) -> Iterator[List[Path]]:
        class TraceFilesIterator(Iterable[List[Path]]):
            def __init__(self, trace_files: TraceFiles):
                self.trace_files = trace_files
                self.iter = iter(self.trace_files.traces.values())

            def __iter__(self):
                return self.iter

            def __next__(self) -> List[Path]:
                return next(self.iter)

        return TraceFilesIterator(self)

class Summary:
    def __init__(
        self,
        binary: Path,
        runtime: float = 0.0,
        max_rss: int | None = None,
        was_killed: bool = False,
        uid: uuid.UUID | None = None,
        current_part_uid: int | str | None = None,
        expected_unseed: int | None = None,
        exit_code: int = 0,
        valgrind_out_file: Path | None = None,
        is_negative_test: bool = False,
        error_out: str = None,
        will_restart: bool = False,
        long_running: bool = False,
        archival_config: Optional[ArchivalConfig] = None,
        stats_attribute_for_v1: Optional[str] = None,
    ):
        self.binary = binary
        self.runtime: float = runtime
        self.max_rss: int | None = max_rss
        self.was_killed: bool = was_killed
        self.long_running = long_running
        self.uid: uuid.UUID | None = uid
        self.current_part_uid: int | str | None = current_part_uid
        self.expected_unseed: int | None = expected_unseed
        self.exit_code: int = exit_code
        self.why: str | None = None
        self.test_file: Path | None = None
        self.seed: int | None = None
        self.test_name: str | None = None
        self.out = SummaryTree("Test", is_root_document=False)
        self.test_begin_found: bool = False
        self.test_end_found: bool = False
        self.unseed: int | None = None
        self.valgrind_out_file: Path | None = valgrind_out_file
        self.archival_config = archival_config or ArchivalConfig()
        self.archival_references_added = False
        self.severity_map: OrderedDict[tuple[str, int], int] = collections.OrderedDict()
        self.error: bool = False
        self.errors: int = 0
        self.warnings: int = 0
        self.coverage: Dict[Coverage, bool] = {}
        self.is_negative_test: bool = is_negative_test
        self.stderr_severity: str = "30"
        self.test_count: int = 0
        self.tests_passed: int = 0
        self.negative_test_success = False
        self.max_trace_time = -1
        self.max_trace_time_type = "None"
        self.error_out = error_out
        
        # File paths for output
        self.run_times_file_path = None
        self.stats_file_path = None
        if config.joshua_output_dir is not None:
            joshua_output_path = Path(config.joshua_output_dir)
            self.joshua_xml_file_path = joshua_output_path / "joshua.xml"
            self.run_times_file_path = joshua_output_path / "run_times.json"
            self.stats_file_path = joshua_output_path / "stats.json"

        if uid is not None:
            self.out.attributes["TestUID"] = str(uid)
        if current_part_uid is not None:
            self.out.attributes["PartUID"] = str(current_part_uid)
        if expected_unseed is not None:
            self.out.attributes["ExpectedUnseed"] = str(expected_unseed)
        self.out.attributes["NegativeTest"] = "1" if self.is_negative_test else "0"

        self.handler = ParseHandler(self.out)
        self.test_begin_time: Optional[float] = None
        self._already_done = False
        self.stats_attribute_for_v1 = stats_attribute_for_v1
        self.no_verbose_on_failure = config.no_verbose_on_failure
        self.register_handlers()

    # Events handled by specific logic - don't duplicate as generic children
    INTERNALLY_HANDLED_EVENT_TYPES = {
        "ProgramStart", "ElapsedTime", "SimulatorConfig", "Simulation", "NonSimulationTest",
        "NegativeTestSuccess", "TestsExpectedToPass", "TestResults", "RemapEventSeverity",
        "BuggifySection", "FaultInjected", "RunningUnitTest", "StderrSeverity", "CodeCoverage",
        "UnseedMismatch", "FailureLogArchive",
    }

    def _sanitize_event_type_for_xml_tag(self, type_str: str) -> str:
        """Sanitize event type for use as XML tag name"""
        if not type_str:
            return "GenericEvent"
        sanitized = re.sub(r'[^a-zA-Z0-9_.-]', '_', type_str)
        if not re.match(r'^[a-zA-Z_]', sanitized):
            sanitized = '_' + sanitized
        return sanitized

    def parse_generic_event_as_child(self, attrs: Dict[str, str]):
        """Convert generic trace events to XML children (simplified version)"""
        event_type = attrs.get("Type")
        if not event_type or event_type in self.INTERNALLY_HANDLED_EVENT_TYPES:
            return

        # Simple check for duplicate severity-based children
        severity = attrs.get("Severity")
        if severity in ["30", "40"]:
            for child in self.out.children:
                if child.name == event_type and child.attributes.get("Severity") == severity:
                    return
        
        tag_name = self._sanitize_event_type_for_xml_tag(event_type)
        child = SummaryTree(tag_name)
        for k, v_raw in attrs.items():
            child.attributes[str(k)] = str(v_raw)
        self.out.append(child)

    def summarize_files(self, trace_files: List[Path]):
        assert len(trace_files) > 0
        for f in trace_files:
            self.parse_file(f)
        self.done()

    def summarize(self, temp_dir: Path | None, command: str):
        if config.joshua_output_dir is not None:
            joshua_output_path = Path(config.joshua_output_dir)
            if not joshua_output_path.exists():
                try:
                    joshua_output_path.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    logger.error(f"Could not create joshua output directory: {e}")

        if temp_dir is not None:
            self.temp_dir = temp_dir
            self.command_file_path = temp_dir / "command.txt"
            self.stderr_file_path = temp_dir / "stderr.txt"
            self.stdout_file_path = temp_dir / "stdout.txt"

        self.command_line = command
        if hasattr(self, 'command_file_path'):
            with open(self.command_file_path, "w") as f:
                f.write(command)
        
        if hasattr(self, 'stderr_file_path'):
            self.stderr = self._try_read_file(self.stderr_file_path, config.max_stderr_bytes)
        if hasattr(self, 'stdout_file_path'):
            self.stdout = self._try_read_file(self.stdout_file_path)

        if config.write_run_times and self.run_times_file_path:
            self._write_run_times()

        if config.read_stats and self.stats_file_path:
            self._read_stats()

        if not self._already_done:
            self.done()

        self._generate_xml_summary()

    def _generate_xml_summary(self):
        """Generate final XML summary with all attributes"""
        self.out.attributes["TestUID"] = str(self.uid) if self.uid else "UNKNOWN_UID"
        # Only set PartUID if it's not already set (don't override constructor)
        if "PartUID" not in self.out.attributes:
            self.out.attributes["PartUID"] = str(self.current_part_uid) if self.current_part_uid is not None else "0"
        self.out.attributes["Version"] = CURRENT_VERSION

        if self.test_file:
            self.out.attributes["TestFile"] = xml.sax.saxutils.escape(str(self.test_file))
        
        if self.test_end_found and self.unseed is not None:
             self.out.attributes["RandomUnseed"] = str(self.unseed)
        
        if self.expected_unseed is not None:
            self.out.attributes["ExpectedUnseed"] = str(self.expected_unseed)

        if config.joshua_output_dir:
            self.out.attributes["JoshuaOutputDir"] = str(config.joshua_output_dir)
        if config.run_temp_dir:
            self.out.attributes["RunTempDir"] = str(config.run_temp_dir)

        # Add child elements for compatibility
        if self.seed is not None and "RandomSeed" not in self.out.attributes:
            seed_element = SummaryTree("RandomSeed")
            seed_element.attributes["Value"] = str(self.seed)
            self.out.append(seed_element)

        if self.test_name and "TestName" not in self.out.attributes:
            test_name_element = SummaryTree("TestName")
            test_name_element.attributes["Value"] = xml.sax.saxutils.escape(self.test_name)
            self.out.append(test_name_element)

    def _try_read_file(self, path: Path | None, max_len: int = -1) -> str:
        """Safely read file content"""
        if path is None or not path.exists():
            return ""
        try:
            with open(path, "r") as f:
                return f.read(max_len)
        except Exception:
            return ""

    def _write_run_times(self):
        """Write runtime statistics"""
        pass

    def _read_stats(self):
        """Read test statistics"""
        pass

    def parse_file(self, file: Path):
        parser: Parser
        if file.suffix == ".xml":
            parser = XmlParser()
        elif file.suffix == ".json":
            parser = JsonParser()
        else:
            return
        
        try:
            with file.open("r", encoding="utf-8", errors="replace") as f:
                parser.parse(f, self.handler)
        except Exception as e:
            logger.error(f"Error parsing {file}: {e}")
            self.error = True
            child = SummaryTree("SummarizationError")
            child.attributes["Severity"] = "40"
            child.attributes["ErrorMessage"] = str(e)
            self.out.append(child)

    def register_handlers(self):
        """Register event handlers for trace parsing"""
        
        def remap_event_severity(attrs: Dict[str, str]):
            try:
                k = (attrs["Type"], int(attrs["Severity"]))
                if k in self.severity_map:
                    return str(self.severity_map[k])
                return None
            except (KeyError, ValueError) as e:
                logger.warning(f"Invalid severity in event: {attrs}. Error: {e}")
                return None

        self.handler.add_handler(("Severity", None), remap_event_severity)

        def get_max_trace_time(attrs: Dict[str, str]):
            if "Time" not in attrs:
                return
            try:
                trace_time = float(attrs["Time"])
                if trace_time > self.max_trace_time:
                    self.max_trace_time = trace_time
                    self.max_trace_time_type = attrs.get("Type", "Unknown")
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid Time value: {attrs}. Error: {e}")

        self.handler.add_handler(("Time", None), get_max_trace_time)

        def program_start(attrs: Dict[str, str]):
            if self.test_begin_found:
                return
            self.test_begin_found = True
            self.out.attributes["RandomSeed"] = attrs["RandomSeed"]
            self.out.attributes["SourceVersion"] = attrs["SourceVersion"]
            self.out.attributes["Time"] = attrs["ActualTime"]
            self.out.attributes["BuggifyEnabled"] = attrs["BuggifyEnabled"]
            if "FaultInjectionEnabled" in attrs:
                self.out.attributes["FaultInjectionEnabled"] = attrs["FaultInjectionEnabled"]

        self.handler.add_handler(("Type", "ProgramStart"), program_start)

        def simulator_config(attrs: Dict[str, str]):
            self.out.attributes["ConfigString"] = attrs["ConfigString"]

        self.handler.add_handler(("Type", "SimulatorConfig"), simulator_config)

        def negative_test_success(attrs: Dict[str, str]):
            self.negative_test_success = True
            child = SummaryTree(attrs["Type"])
            for k, v in attrs.items():
                if k != "Type":
                    child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(("Type", "NegativeTestSuccess"), negative_test_success)

        def set_test_count(attrs: Dict[str, str]):
            try:
                self.test_count = int(attrs["Count"])
            except (KeyError, ValueError) as e:
                logger.warning(f"Invalid Count in TestsExpectedToPass: {attrs}. Error: {e}")
                self.test_count = 0

        self.handler.add_handler(("Type", "TestsExpectedToPass"), set_test_count)

        def set_tests_passed(attrs: Dict[str, str]):
            try:
                # Try 'Count' first, fall back to 'Passed' if not found
                if "Count" in attrs:
                    self.tests_passed = int(attrs["Count"])
                elif "Passed" in attrs:
                    self.tests_passed = int(attrs["Passed"])
                else:
                    raise KeyError("Neither 'Count' nor 'Passed' found in TestResults")
            except (KeyError, ValueError) as e:
                logger.warning(f"Invalid Count/Passed in TestResults: {attrs}. Error: {e}")
                self.tests_passed = 0

        self.handler.add_handler(("Type", "TestResults"), set_tests_passed)

        def remap_severity(attrs: Dict[str, str]):
            try:
                k = (attrs["OriginalType"], int(attrs["OriginalSeverity"]))
                self.severity_map[k] = int(attrs["NewSeverity"])
            except (KeyError, ValueError) as e:
                logger.warning(f"Invalid severity remapping: {attrs}. Error: {e}")

        self.handler.add_handler(("Type", "RemapEventSeverity"), remap_severity)

        def set_test_file(attrs: Dict[str, str]):
            self.test_file = Path(attrs["TestFile"])
            self.out.attributes["TestFile"] = attrs["TestFile"]

        self.handler.add_handler(("Type", "Simulation"), set_test_file)
        self.handler.add_handler(("Type", "NonSimulationTest"), set_test_file)

        def set_elapsed_time(attrs: Dict[str, str]):
            if self.test_end_found:
                return
            self.test_end_found = True
            try:
                self.unseed = int(attrs["RandomUnseed"])
            except (KeyError, ValueError):
                logger.error(f"Invalid RandomUnseed in ElapsedTime: {attrs}")
                self.unseed = -1

            # Check for unseed mismatch
            if (self.expected_unseed is not None and 
                self.unseed != self.expected_unseed and 
                self.unseed != -1):
                
                severity = 40 if ("UnseedMismatch", 40) not in self.severity_map else self.severity_map[("UnseedMismatch", 40)]
                
                if severity >= 30:
                    child = SummaryTree("UnseedMismatch")
                    child.attributes["Expected"] = str(self.expected_unseed)
                    child.attributes["Actual"] = str(self.unseed)
                    child.attributes["Severity"] = str(severity)
                    if severity >= 40:
                        self.error = True
                        self.why = "UnseedMismatch"
                    self.out.append(child)

            self.out.attributes["SimElapsedTime"] = attrs["SimTime"]
            self.out.attributes["RealElapsedTime"] = attrs["RealTime"]
            if self.unseed is not None:
                self.out.attributes["RandomUnseed"] = str(self.unseed)

        self.handler.add_handler(("Type", "ElapsedTime"), set_elapsed_time)

        def parse_unseed_mismatch(attrs: Dict[str, str]):
            self.error = True
            if "FailReason" not in self.out.attributes:
                self.out.attributes["FailReason"] = "UnseedMismatch"
            child = SummaryTree("UnseedMismatch")
            for k, v in attrs.items():
                if k != "Type":
                    child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(("Type", "UnseedMismatch"), parse_unseed_mismatch)

        def parse_warning(attrs: Dict[str, str]):
            self.warnings += 1
            if self.warnings > config.max_warnings:
                return
            child = SummaryTree(attrs["Type"])
            for k, v in attrs.items():
                if k != "Type":
                    child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(("Severity", "30"), parse_warning)

        def parse_error(attrs: Dict[str, str]):
            self.error = True
            self.errors += 1
            if self.errors > config.max_errors:
                return
            child = SummaryTree(attrs["Type"])
            for k, v in attrs.items():
                if k != "Type":
                    child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(("Severity", "40"), parse_error)

        def buggify_section(attrs: Dict[str, str]):
            child = SummaryTree("BuggifySection")
            for k, v in attrs.items():
                if k != "Type":
                    child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(("Type", "BuggifySection"), buggify_section)

        def fault_injected(attrs: Dict[str, str]):
            child = SummaryTree("FaultInjected")
            for k, v in attrs.items():
                if k != "Type":
                    child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(("Type", "FaultInjected"), fault_injected)

        def coverage_file(attrs: Dict[str, str]):
            try:
                file = attrs["File"]
                line = int(attrs["Line"])
                comment = attrs.get("Comment")
                rare = attrs.get("Rare", "0") != "0"
                covered = attrs.get("Covered", "1") != "0"
                self.coverage[Coverage(file, line, comment, rare)] = covered
            except (KeyError, ValueError) as e:
                logger.warning(f"Invalid coverage data: {attrs}. Error: {e}")

        self.handler.add_handler(("Type", "CodeCoverage"), coverage_file)

        def running_unit_test(attrs: Dict[str, str]):
            child = SummaryTree("RunningUnitTest")
            child.attributes["Name"] = attrs["Name"]
            child.attributes["File"] = attrs["File"]
            child.attributes["Line"] = attrs["Line"]
            self.out.append(child)

        self.handler.add_handler(("Type", "RunningUnitTest"), running_unit_test)

        def stderr_severity(attrs: Dict[str, str]):
            self.stderr_severity = attrs["Severity"]

        self.handler.add_handler(("Type", "StderrSeverity"), stderr_severity)

        # Generic event handler for unhandled events
        if not self.no_verbose_on_failure:
            self.handler.add_handler(None, self.parse_generic_event_as_child)

    def done(self):
        """Finalize summary processing"""
        if self._already_done:
            return
        self._already_done = True

        self._detect_errors()
        self._add_archival_info()
        self._finalize_attributes()

    def _detect_errors(self):
        """Detect and handle various error conditions"""
        if self.test_begin_found and not self.test_end_found and not self.was_killed:
            self.error = True
            if "FailReason" not in self.out.attributes:
                 self.out.attributes["FailReason"] = "TestDidNotFinish"

        if self.exit_code != 0 and not self.error and not self.was_killed and not self.is_negative_test:
            self.error = True
            if "FailReason" not in self.out.attributes:
                self.out.attributes["FailReason"] = "NonZeroExitCodeNoError"

        if self.was_killed:
            self.error = True
            self.out.attributes["Killed"] = "1"
            if "FailReason" not in self.out.attributes:
                self.out.attributes["FailReason"] = "TestKilled"

        # Check stderr output for positive tests
        if (not self.error and not self.is_negative_test and self.exit_code == 0 and 
            self.error_out and len(self.error_out.strip()) > 0):
            self.error = True
            if "FailReason" not in self.out.attributes:
                self.out.attributes["FailReason"] = "StdErrOutputWithZeroExit"

        # Handle valgrind errors
        if self.valgrind_out_file is not None and self.valgrind_out_file.exists():
            try:
                errors = parse_valgrind_output(self.valgrind_out_file)
                if len(errors) > 0:
                    self.error = True
                    if "FailReason" not in self.out.attributes:
                        self.out.attributes["FailReason"] = "ValgrindErrors"
                    for error in errors:
                        child = SummaryTree("ValgrindError")
                        child.attributes["What"] = str(error)
                        self.out.append(child)
            except Exception as e:
                logger.error(f"Error parsing valgrind output: {e}")

        # Handle stderr output
        if self.error_out and len(self.error_out) > 0 and self.stderr_severity != "0":
            if len(self.error_out) > config.max_stderr_bytes:
                trunc_tree = SummaryTree("StdErrOutputTruncated")
                trunc_tree.attributes["Bytes"] = str(config.max_stderr_bytes)
                self.out.append(trunc_tree)
                content_tree = SummaryTree("StdErrOutput")
                content_tree.attributes["Content"] = self.error_out[:config.max_stderr_bytes]
                self.out.append(content_tree)
            else:
                error_out_tree = SummaryTree("StdErrOutput")
                error_out_tree.attributes["Content"] = self.error_out
                self.out.append(error_out_tree)

        # Handle coverage
        if config.print_coverage:
            for k, v in self.coverage.items():
                child = SummaryTree("CodeCoverage")
                child.attributes["File"] = k.file
                child.attributes["Line"] = str(k.line)
                child.attributes["Rare"] = str(k.rare)
                if not v:
                    child.attributes["Covered"] = "0"
                if k.comment:
                    child.attributes["Comment"] = k.comment
                self.out.append(child)

        # Handle warning/error limits
        if self.warnings > config.max_warnings:
            child = SummaryTree("WarningLimitExceeded")
            child.attributes["Severity"] = "30"
            child.attributes["WarningCount"] = str(self.warnings)
            self.out.append(child)

        if self.errors > config.max_errors:
            child = SummaryTree("ErrorLimitExceeded")
            child.attributes["Severity"] = "40"
            child.attributes["ErrorCount"] = str(self.errors)
            self.out.append(child)
            self.error = True

    def _add_archival_info(self):
        """Add archival information if test failed"""
        if self.archival_config.enabled and not self.ok():
            self.archival_references_added = True

            if self.archival_config.run_temp_dir and self.archival_config.run_temp_dir.exists():
                log_dir_elem = SummaryTree("FDBClusterLogDir")
                log_dir_elem.attributes["path"] = str(self.archival_config.run_temp_dir.resolve())
                self.out.append(log_dir_elem)
            
            if self.archival_config.stdout_path and self.archival_config.stdout_path.exists():
                stdout_elem = SummaryTree("HarnessLogFile")
                stdout_elem.attributes["type"] = "stdout"
                stdout_elem.attributes["path"] = str(self.archival_config.stdout_path.resolve())
                self.out.append(stdout_elem)

            if self.archival_config.stderr_path and self.archival_config.stderr_path.exists():
                stderr_elem = SummaryTree("HarnessLogFile")
                stderr_elem.attributes["type"] = "stderr"
                stderr_elem.attributes["path"] = str(self.archival_config.stderr_path.resolve())
                self.out.append(stderr_elem)
            
            if self.archival_config.command_path and self.archival_config.command_path.exists():
                cmd_elem = SummaryTree("HarnessLogFile")
                cmd_elem.attributes["type"] = "command"
                cmd_elem.attributes["path"] = str(self.archival_config.command_path.resolve())
                self.out.append(cmd_elem)

            for log_file_path in self.archival_config.current_fdb_logs:
                if log_file_path.exists():
                    fdb_file_elem = SummaryTree("FDBLogFile")
                    fdb_file_elem.attributes["path"] = str(log_file_path.resolve())
                    self.out.append(fdb_file_elem)
            
            if self.archival_config.joshua_output_dir and self.archival_config.joshua_output_dir.exists():
                jod_elem = SummaryTree("JoshuaOutputDirRef")
                jod_elem.attributes["path"] = str(self.archival_config.joshua_output_dir.resolve())
                self.out.append(jod_elem)

    def _finalize_attributes(self):
        """Finalize top-level attributes"""
        # Determine final test result
        if self.error:
            if self.is_negative_test:
                self.negative_test_success = True
                self.out.attributes["NegativeTestSuccess"] = "1"
            else:
                self.out.attributes["Ok"] = "0"
        else:
            if self.is_negative_test:
                self.negative_test_success = False
                self.out.attributes["Ok"] = "0"
                if "FailReason" not in self.out.attributes:
                    self.out.attributes["FailReason"] = "NegativeTestDidNotFail"
            else:
                self.out.attributes["Ok"] = "1"
                self.tests_passed += 1
        
        self.test_count += 1

        # Set memory and runtime attributes
        if self.max_rss is not None:
            self.out.attributes["PeakMemory"] = str(self.max_rss)

        if self.runtime is not None:
            self.out.attributes["Runtime"] = str(self.runtime)

        # Final OK status and fail reason
        current_ok_status = self.ok()
        self.out.attributes["Ok"] = "1" if current_ok_status else "0"
        
        if not current_ok_status:
            final_reason = self.why or "Unknown"
            if not final_reason:
                if self.error:
                    final_reason = "ProducedErrors"
                elif not self.test_end_found:
                    final_reason = "TestDidNotFinish"
                elif self.tests_passed == 0 and self.test_count > 0:
                    final_reason = "NoTestsPassed"
                elif self.test_count != self.tests_passed:
                    final_reason = f"Expected {self.test_count} tests to pass, but only {self.tests_passed} did"
            self.out.attributes["FailReason"] = final_reason
        elif "FailReason" in self.out.attributes:
            del self.out.attributes["FailReason"]

    def ok(self):
        """Check if test passed (logical xor for negative tests)"""
        return (not self.error) != self.is_negative_test

    def get_v1_stdout_line(self) -> Optional[str]:
        """Generate V1-compatible stdout line"""
        if not self._already_done:
            self.done()

        if not hasattr(self, 'out') or not self.out or self.out.name != "Test":
            return '<Test Ok="0" Error="V1SummaryGenFailed_InvalidState" PartUID="0"/>'

        try:
            v1_test_summary_tree = copy.deepcopy(self.out)
            v1_test_element = v1_test_summary_tree.to_et_element()
        except Exception as e:
            logger.error(f"Failed to create V1 summary: {e}")
            return f'<Test Ok="0" Error="V1SummaryGenFailed_CopyError" PartUID="{self.current_part_uid or "0"}"/>'
        
        # Set V1-specific attributes
        v1_test_element.set("TestRunCount", "1")

        # Set TotalTestTime from RealElapsedTime
        real_et_str = self.out.attributes.get("RealElapsedTime")
        if real_et_str:
            try:
                total_test_time_val = str(int(float(real_et_str)))
                v1_test_element.set("TotalTestTime", total_test_time_val)
            except ValueError:
                v1_test_element.set("TotalTestTime", "0")
        else:
            v1_test_element.set("TotalTestTime", "0")

        # Add Statistics attribute
        if self.stats_attribute_for_v1:
            v1_test_element.set("Statistics", self.stats_attribute_for_v1)
        
        # Add essential child elements
        for child_summary_tree in self.out.children:
            if (child_summary_tree.name in ESSENTIAL_V1_CHILD_TAGS_TO_KEEP and 
                child_summary_tree.name not in STDOUT_EXPLICITLY_STRIPPED_TAGS):
                try:
                    child_et_element = child_summary_tree.to_et_element()
                    v1_test_element.append(child_et_element)
                except Exception as e:
                    logger.error(f"Error adding child '{child_summary_tree.name}' to V1 output: {e}")

        try:
            v1_xml_string = ET.tostring(v1_test_element, encoding='unicode', method='xml', short_empty_elements=True)
            return v1_xml_string.strip()
        except Exception as e:
            logger.error(f"Failed to serialize V1 XML: {e}")
            return f'<Test Ok="0" Error="V1SummaryGenFailed_SerializationFailed" PartUID="{self.current_part_uid or "0"}"/>'