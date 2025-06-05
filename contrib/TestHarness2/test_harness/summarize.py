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
import tarfile
import logging
import xml.etree.ElementTree as ET
import copy
import gzip
from typing import (
    List,
    Dict,
    TextIO,
    Callable,
    Optional,
    OrderedDict,
    Any,
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

# Define TAGS_TO_STRIP_FROM_TEST_ELEMENT_FOR_STDOUT at module level
TAGS_TO_STRIP_FROM_TEST_ELEMENT_FOR_STDOUT: Set[str] = {
    "CodeCoverage", 
    "ValgrindError", 
    "StdErrOutput", 
    "StdErrOutputTruncated", 
    "JoshuaMessage",    # V1 successful stdout didn't always have this. Its presence in the failing example is noted.
    # "SimFDB",           # REMOVE THIS LINE to stop stripping SimFDB
    # "FailureLogArchive",# New tag specific to TestHarnessV2, NOT STRIPPED FROM STDOUT so j tail -s can see it.
    # "JoshuaLogTool"     # REMOVE THIS LINE to stop stripping JoshuaLogTool
    # Children from trace events (e.g., <DisableConnectionFailures_Tester>) ARE KEPT.
}

# This set remains, as these are elements we *don't* want in the detailed joshua.xml if they are too verbose
# but it's not directly used by get_v1_stdout_line for *inclusion*.
TAGS_TO_STRIP_FROM_JOSHUA_XML_IF_EMPTY_OR_DEFAULT = {"Knobs", "Metrics", "BuggifySection"}

# Define the set of child tags essential for the V1 stdout line.
# These will be included if present in the main summary.
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
    "UnseedMismatch",             # Add for V1 error case compatibility
    "WarningLimitExceeded",       # Add for V1 error case compatibility
    "ErrorLimitExceeded",          # Add for V1 error case compatibility
    # Note: ArchiveFile is handled separately and explicitly added if present.
}

# Define tags that should be explicitly STRIPPED from the V1 stdout line
# because they are too verbose, not V1-like, or handled differently.
STDOUT_EXPLICITLY_STRIPPED_TAGS = {
    "CodeCoverage",
    "ValgrindError", 
    "StdErrOutput",             # Not in V1 success example, can be verbose
    "StdErrOutputTruncated",    # Not in V1 success example, can be verbose
    "Knobs",                    # Usually very verbose
    "Metrics",                  # Usually very verbose
    "JoshuaMessage",            # Not in V1 success example (though can appear in errors/warnings)
    # SimFDB is not in the V1 example; if it appears and is verbose, it could be added here.
    # JoshuaLogTool is in the V1 example, so it's NOT stripped.
    # Event-derived tags like *_Tester are NOT stripped by default.
}

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
        if self.root is not None and element.root is not None:
            self.root.append(element.to_et_element())
        elif self.root is not None:
            self.root.append(element.to_et_element())

    def to_et_element(self) -> ET.Element:
        element = ET.Element(self.name)
        for k, v_raw in self.attributes.items():
            element.set(str(k), xml.sax.saxutils.escape(str(v_raw)))
        
        for child_summary_tree in self.children:
            element.append(child_summary_tree.to_et_element())
        
        if self.root is None:
            self.root = element
        elif self.root is not element:
            pass
        
        return element

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
            res["Type"] = self.name
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
            res["children"] = children
        return res

    def to_json(self, out: TextIO, prefix: str = ""):
        res = json.dumps(self.to_dict(), indent=("  " if config.pretty_print else None))
        for line in res.splitlines(False):
            out.write("{}{}\n".format(prefix, line))

    def to_xml(self, out: TextIO, prefix: str = ""):
        # minidom doesn't support omitting the xml declaration which is a problem for joshua
        # However, our xml is very simple and therefore serializing manually is easy enough
        attrs = []
        print_width = 120
        try:
            print_width, _ = os.get_terminal_size()
        except OSError:
            pass
        for k, v in self.attributes.items():
            attrs.append("{}={}".format(k, xml.sax.saxutils.quoteattr(v)))
        elem = "{}<{}{}".format(prefix, self.name, ("" if len(attrs) == 0 else " "))
        out.write(elem)
        if config.pretty_print:
            curr_line_len = len(elem)
            for i in range(len(attrs)):
                attr_len = len(attrs[i])
                if i == 0 or attr_len + curr_line_len + 1 <= print_width:
                    if i != 0:
                        out.write(" ")
                    out.write(attrs[i])
                    curr_line_len += attr_len
                else:
                    out.write("\n")
                    out.write(" " * len(elem))
                    out.write(attrs[i])
                    curr_line_len = len(elem) + attr_len
        else:
            out.write(" ".join(attrs))
        if len(self.children) == 0:
            out.write("/>")
        else:
            out.write(">")
        for child in self.children:
            if config.pretty_print:
                out.write("\n")
            child.to_xml(
                out, prefix=("  {}".format(prefix) if config.pretty_print else prefix)
            )
        if len(self.children) > 0:
            out.write(
                "{}{}</{}>".format(
                    ("\n" if config.pretty_print else ""), prefix, self.name
                )
            )

    def dump(self, out: TextIO, prefix: str = "", new_line: bool = True):
        if config.output_format == "json":
            self.to_json(out, prefix=prefix)
        else:
            self.to_xml(out, prefix=prefix)
        if new_line:
            out.write("\n")

    def to_string_document(self) -> str:
        """Serializes the entire XML tree to a string, including XML declaration."""
        if self.root is None:
            # Ensure the root element is built if it hasn't been already
            # This might happen if to_et_element was never called on this specific SummaryTree instance
            # but it's expected to be the root of a document.
            # However, the root is typically set during __init__ if is_root_document=True
            # or when to_et_element is first called.
            # For safety, we can try to build it, but this indicates a potential logic issue
            # if self.root is None when we expect a full document.
            logger.warning("to_string_document called on a SummaryTree with no self.root. Attempting to build from self.")
            self.to_et_element() # This will build and assign self.root if it's None
        
        if self.root is None:
            logger.error("Cannot serialize SummaryTree to string document: self.root is still None after attempted build.")
            return "<Error>Cannot serialize XML document: root element is missing</Error>"

        try:
            xml_str = ET.tostring(self.root, encoding='unicode', short_empty_elements=True)
            return f'<?xml version="1.0"?>\n{xml_str}'
        except Exception as e:
            logger.error(f"Error during SummaryTree.to_string_document serialization: {e}", exc_info=True)
            return f'<?xml version="1.0"?>\n<Error>SerializationFailed: {xml.sax.saxutils.escape(str(e))}</Error>'


ParserCallback = Callable[[Dict[str, str]], Optional[str]]


class ParseHandler:
    def __init__(self, out: SummaryTree):
        self.out = out
        self.events: OrderedDict[
            Optional[Tuple[str, Optional[str]]], List[ParserCallback]
        ] = collections.OrderedDict()

    def add_handler(
        self, attr: Tuple[str, Optional[str]], callback: ParserCallback
    ) -> None:
        self.events.setdefault(attr, []).append(callback)

    def _call(self, callback: ParserCallback, attrs: Dict[str, str]) -> str | None:
        try:
            return callback(attrs)
        except Exception as e:
            _, _, exc_traceback = sys.exc_info()
            child = SummaryTree("NonFatalParseError")
            child.attributes["Severity"] = "30"
            child.attributes["ErrorMessage"] = str(e)
            child.attributes["Trace"] = repr(traceback.format_tb(exc_traceback))
            self.out.append(child)
            return None

    def handle(self, attrs: Dict[str, str]):
        # Call specific handlers first
        # This allows them to add children to self.out before the generic handler runs.
        # The generic handler (parse_generic_event_as_child) has de-duplication logic
        # that relies on seeing what specific handlers have already done.
        for k, v_attr in attrs.items():
            # Check for (key, specific_value) handlers, e.g., ("Severity", "30")
            if (k, v_attr) in self.events:
                for callback in self.events[(k, v_attr)]:
                    # We don't need to update attrs here based on remap for this specific issue,
                    # as parse_warning/parse_error don't rely on remapped values from other handlers.
                    self._call(callback, attrs)
            
            # Check for (key, None) handlers (match key, any value), e.g., ("Time", None)
            # These are typically for attribute remapping or broad side effects.
            if (k, None) in self.events:
                for callback in self.events[(k, None)]:
                    remap = self._call(callback, attrs)
                    if remap is not None:
                        # If a remapping occurred, update the attribute value for subsequent handlers
                        # (though for this specific pass, it mainly affects other (key,None) or the generic handler).
                        attrs[k] = remap

        # Now call generic (None) handlers, like parse_generic_event_as_child
        if None in self.events:
            for callback in self.events[None]:
                self._call(callback, attrs)


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
    def __init__(
        self, file: str, line: str | int, comment: str | None = None, rare: bool = False
    ):
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
        trace_expr = re.compile(r"trace.*\.(json|xml)")
        for file in self.path.iterdir():
            if file.is_file() and trace_expr.match(file.name) is not None:
                ts = int(file.name.split(".")[6])
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
    def __init__(
        self,
        binary: Path,
        runtime: float = 0,
        max_rss: int | None = None,
        was_killed: bool = False,
        uid: uuid.UUID | None = None,
        current_part_uid: uuid.UUID | None = None,
        expected_unseed: int | None = None,
        exit_code: int = 0,
        valgrind_out_file: Path | None = None,
        stats: str | None = None,
        error_out: str = None,
        will_restart: bool = False,
        long_running: bool = False,
        paired_run_fdb_logs_for_archival: Optional[List[Path]] = None,
        paired_run_harness_files_for_archival: Optional[List[Path]] = None,
        archive_logs_on_failure: bool = False,
        joshua_output_dir: Optional[Path] = None,
        run_temp_dir: Optional[Path] = None,
        stats_attribute_for_v1: Optional[str] = None,
        # Adding paths for current run's harness outputs for archival
        current_run_stdout_path: Optional[Path] = None,
        current_run_stderr_path: Optional[Path] = None,
        current_run_command_file_path: Optional[Path] = None,
        fdb_log_files_for_archival: Optional[List[Path]] = None # Already existed, just ensuring it's here
    ):
        self.binary = binary
        self.runtime: float = runtime
        self.max_rss: int | None = max_rss
        self.was_killed: bool = was_killed
        self.long_running = long_running
        self.uid: uuid.UUID | None = uid
        self.current_part_uid: uuid.UUID | None = current_part_uid
        self.expected_unseed: int | None = expected_unseed
        self.exit_code: int = exit_code
        self.why: str | None = None
        self.test_file: Path | None = None
        self.seed: int | None = None
        self.test_name: str | None = None
        self.out: SummaryTree = SummaryTree("Test", is_root_document=False)
        self.test_begin_found: bool = False
        self.test_end_found: bool = False
        self.unseed: int | None = None
        self.valgrind_out_file: Path | None = valgrind_out_file
        self.fdb_log_files_for_archival: Optional[List[Path]] = []
        if fdb_log_files_for_archival is not None: # Ensure it's initialized if passed
            self.fdb_log_files_for_archival = fdb_log_files_for_archival
        self.paired_run_fdb_logs_for_archival = paired_run_fdb_logs_for_archival
        self.paired_run_harness_files_for_archival = paired_run_harness_files_for_archival
        self.archive_logs_on_failure = archive_logs_on_failure
        self.archival_references_added = False # Flag to indicate if archival tags were added
        self._jod_for_archive = joshua_output_dir
        self._rtd_for_archive = run_temp_dir
        # Store current run's harness output paths
        self.current_run_stdout_path = current_run_stdout_path
        self.current_run_stderr_path = current_run_stderr_path
        self.current_run_command_file_path = current_run_command_file_path
        self.severity_map: OrderedDict[tuple[str, int], int] = collections.OrderedDict()
        self.error: bool = False
        self.errors: int = 0
        self.warnings: int = 0
        self.coverage: OrderedDict[Coverage, bool] = collections.OrderedDict()
        self.test_count: int = 0
        self.tests_passed: int = 0
        self.error_out = error_out
        self.stderr_severity: str = "40"
        self.will_restart: bool = will_restart
        self.test_dir: Path | None = None
        self.is_negative_test = False
        self.negative_test_success = False
        self.max_trace_time = -1
        self.max_trace_time_type = "None"
        self.run_times_file_path = None
        self.stats_file_path = None
        if config.joshua_output_dir is not None:
            joshua_output_path = Path(config.joshua_output_dir)
            self.joshua_xml_file_path = joshua_output_path / "joshua.xml"
            self.run_times_file_path = joshua_output_path / "run_times.json"
            self.stats_file_path = joshua_output_path / "stats.json"

        if uid is not None:
            self.out.attributes["TestUID"] = str(uid)
        if stats is not None:
            self.out.attributes["Statistics"] = stats
        self.out.attributes["JoshuaSeed"] = str(config.joshua_seed)
        self.out.attributes["WillRestart"] = "1" if self.will_restart else "0"
        self.out.attributes["NegativeTest"] = "1" if self.is_negative_test else "0"

        self.handler = ParseHandler(self.out)
        self.register_handlers()
        self._already_done = False
        self.stats_attribute_for_v1 = stats_attribute_for_v1

    # Event types that are handled by specific logic (e.g., setting top-level attributes, errors)
    # and should generally not be duplicated as generic child elements by parse_generic_event_as_child.
    # This list might need refinement.
    INTERNALLY_HANDLED_EVENT_TYPES = {
        "ProgramStart",             # Data merged into top-level Test attributes
        "ElapsedTime",              # Data merged into top-level Test attributes, sets test_end_found
        "SimulatorConfig",          # Sets ConfigString attribute
        "Simulation",               # Sets TestFile attribute
        "NonSimulationTest",        # Sets TestFile attribute
        "NegativeTestSuccess",      # Sets self.negative_test_success, adds specific child
        "TestsExpectedToPass",      # Sets self.test_count
        "TestResults",              # Sets self.tests_passed
        "RemapEventSeverity",       # Modifies severity_map
        "BuggifySection",           # Adds specific child
        "FaultInjected",            # Adds specific child (often via BuggifySection handler)
        "RunningUnitTest",          # Adds specific child
        "StderrSeverity",           # Sets self.stderr_severity
        "CodeCoverage",             # Processed by coverage logic in done()
        "UnseedMismatch",           # Handled by set_elapsed_time, adds specific child
        "FailureLogArchive",        # Add this to prevent generic parsing if it ever appears as an event
        # Generic "Error" and "Warning" are usually caught by Severity 40/30 handlers
        # However, if an event has Type "FooError" and Severity 40, it will be caught by
        # the Severity 40 handler AND potentially by the generic handler.
        # The specific Severity 40 handler already creates a child with the event's original Type.
        # So, we might not need to list explicit Error/Warning types here if their specific
        # handlers (parse_error, parse_warning) correctly use the event's Type for the child tag.
    }

    # For events that become children, ensure their 'Type' attribute is sanitized if used as a tag.
    # This function is also used by the stderr parsing in done()
    def _sanitize_event_type_for_xml_tag(self, type_str: str) -> str:
        if not type_str:
            return "GenericEvent"
        # Replace non-alphanumeric (plus _, ., -) with underscore
        sanitized = re.sub(r'[^a-zA-Z0-9_.-]', '_', type_str)
        # Ensure it starts with a letter or underscore
        if not re.match(r'^[a-zA-Z_]', sanitized):
            sanitized = '_' + sanitized
        return sanitized

    def parse_generic_event_as_child(self, attrs: Dict[str, str]):
        event_type = attrs.get("Type")
        if not event_type or event_type in self.INTERNALLY_HANDLED_EVENT_TYPES:
            return

        # Check if a more specific handler (Severity 30/40) already created a similar child.
        # This is a bit heuristic: if an error/warning child with this exact type already exists, skip.
        # This aims to prevent duplicate children if, e.g., an event "MyCustomError" with Severity 40
        # was already added by the parse_error handler.
        severity = attrs.get("Severity")
        if severity in ["30", "40"]:
            for child in self.out.children:
                if child.name == event_type and child.attributes.get("Severity") == severity:
                    # Likely already handled by parse_warning or parse_error
                    # which use the original event Type as the tag name.
                    return
        
        tag_name = self._sanitize_event_type_for_xml_tag(event_type)
        child = SummaryTree(tag_name)
        for k, v_raw in attrs.items():
            # Add all attributes from the event.
            # Convert value to string and escape for XML. Escape happens in SummaryTree.to_et_element()
            v = str(v_raw)
            child.attributes[str(k)] = v 
        self.out.append(child)


    def summarize_files(self, trace_files: List[Path]):
        assert len(trace_files) > 0
        for f in trace_files:
            self.parse_file(f)
        self.done()

    def summarize(self, temp_dir: Path | None, command: str):
        logger.info(f"Summarize.summarize called for temp_dir: {temp_dir}, command: {command}")
        if config.joshua_output_dir is not None:
            joshua_output_path = Path(config.joshua_output_dir)
            if not joshua_output_path.exists():
                try:
                    joshua_output_path.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    logger.error(f"Could not create joshua output directory {joshua_output_path}: {e}")
                    # Not raising here, as we might still be able to produce some output

        if temp_dir is not None: # Update paths if they weren't set in init (e.g. fatal error before TestRun)
            self.temp_dir = temp_dir # type: ignore
            self.command_file_path = temp_dir / "command.txt"
            self.stderr_file_path = temp_dir / "stderr.txt"
            self.stdout_file_path = temp_dir / "stdout.txt"
            # run_times and stats are tied to joshua_output_dir, not temp_dir for the run part
            # self.run_times_file_path = temp_dir / "run_times.json" # This was incorrect
            # self.stats_file_path = temp_dir / "stats.json" # This was incorrect

        self.command_line = command
        if self.command_file_path:
            with open(self.command_file_path, "w") as f:
                f.write(command)
        self.stderr = self._try_read_file(self.stderr_file_path, config.max_stderr_bytes)
        self.stdout = self._try_read_file(self.stdout_file_path)

        if config.write_run_times and self.run_times_file_path:
            self._write_run_times()

        if config.read_stats and self.stats_file_path:
            self._read_stats()

        # Ensure done() is called to finalize error states, in case summarize_files wasn't called (no trace files)
        if not self._already_done:
            logger.info("Calling self.done() from summarize() as it wasn't called via summarize_files().")
            self.done()

        # The actual XML generation happens here, using the state finalized by done()
        self._generate_xml_summary()

    def _generate_xml_summary(self):
        # This method should now primarily use self attributes that have been fully populated
        # by event parsing and the self.done() method.

        self.out.attributes["TestUID"] = str(self.uid) if self.uid else "UNKNOWN_UID"
        self.out.attributes["PartUID"] = str(self.current_part_uid) if self.current_part_uid else "UNKNOWN_PART_UID"
        # Ok, Why, FailReason are set by self.done()
        # Version is static
        self.out.attributes["Version"] = CURRENT_VERSION

        if self.test_file: # Set by specific event handlers
            self.out.attributes["TestFile"] = xml.sax.saxutils.escape(str(self.test_file))
        
        # Attributes from ProgramStart event (already set on self.out.attributes by handler)
        # self.out.attributes["RandomSeed"] -> already set if ProgramStart event occurred
        # self.out.attributes["SourceVersion"] -> already set if ProgramStart
        # self.out.attributes["Time"] -> (ActualTime from ProgramStart) already set
        # self.out.attributes["BuggifyEnabled"] -> already set if ProgramStart
        # self.out.attributes["FaultInjectionEnabled"] -> already set if ProgramStart
        
        # Attributes from SimulatorConfig event
        # self.out.attributes["ConfigString"] -> already set if SimulatorConfig event occurred

        # Attributes from ElapsedTime event (already set on self.out.attributes by handler)
        # self.out.attributes["SimElapsedTime"] -> already set if ElapsedTime event occurred
        # self.out.attributes["RealElapsedTime"] -> already set if ElapsedTime event occurred
        if self.test_end_found and self.unseed is not None: # unseed is set by ElapsedTime handler
             self.out.attributes["RandomUnseed"] = str(self.unseed)

        if self.expected_unseed: # Passed in constructor
            self.out.attributes["ExpectedUnseed"] = str(self.expected_unseed)
        
        # PeakMemory is set in done() and assigned to self.out.attributes there
        # Runtime is set by TestRun, passed to Summary, then assigned in done()
        # self.out.attributes["PeakMemory"] -> set in done()
        # self.out.attributes["Runtime"] -> set in done()

        # Ok and FailReason are definitively set in done()
        # self.out.attributes["Ok"] = "1" if self.ok() else "0"
        # if not self.ok() and self.why:
        #     self.out.attributes["FailReason"] = self.why
        # elif not self.ok():
        #     self.out.attributes["FailReason"] = "Unknown"

        # Add information from the config that might be relevant (already present)
        if config.joshua_output_dir:
            self.out.attributes["JoshuaOutputDir"] = str(config.joshua_output_dir)
        if config.run_temp_dir:
            self.out.attributes["RunTempDir"] = str(config.run_temp_dir)

        # These are added as child elements if not already part of top-level attributes by specific handlers
        # Ensure RandomSeed and TestName are added if not populated by ProgramStart/Simulation events
        # However, ProgramStart handler sets RandomSeed. TestFile handler sets TestFile.
        # TestName is set from test_file.stem in TestRun and passed to Summary.
        # For V1 compatibility, these seem to be both attributes and children sometimes, or just attributes.
        # The current logic:
        # - Top-level <Test RandomSeed="..."> (from ProgramStart)
        # - Top-level <Test TestFile="..."> (from Simulation/NonSimulationTest)
        # - Child <RandomSeed Value="...">
        # - Child <TestName Value="...">

        # Let's ensure the child elements are present, as per prior logic
        # but avoid adding them if the information is already a primary attribute
        # from a specific event (like ProgramStart's RandomSeed).

        # The `program_start` handler sets self.out.attributes["RandomSeed"]
        # The `set_test_file` handler sets self.out.attributes["TestFile"]
        # `self.seed` and `self.test_name` are set by `TestRun` and passed to `Summary.__init__`
        # `_generate_xml_summary` (original version) added child elements for these.

        # Let's stick to adding them as children if they aren't already top-level attributes,
        # or if V1 style dictates they are *also* children.
        # V1 example shows <Test RandomSeed="X"> and <RandomSeed Value="X"/> - this is duplication.
        # Let's prioritize top-level attributes from events, and add children only if necessary
        # or if data isn't on top-level.

        # If self.seed (from TestRun) is available and not already set by ProgramStart on self.out
        if self.seed is not None and "RandomSeed" not in self.out.attributes:
             # This case should be rare if ProgramStart event exists
            seed_element = SummaryTree("RandomSeed")
            seed_element.attributes["Value"] = str(self.seed)
            self.out.append(seed_element)
        elif self.seed is not None and "RandomSeed" in self.out.attributes and str(self.out.attributes["RandomSeed"]) != str(self.seed):
            # If ProgramStart RandomSeed differs from the initial seed, log/note it
            logger.warning(f"Initial seed {self.seed} differs from ProgramStart event RandomSeed {self.out.attributes['RandomSeed']}")
            # Potentially add the initial seed as a different named child if needed
            # For now, ProgramStart event's seed takes precedence for the RandomSeed attribute.

        if self.test_name and "TestName" not in self.out.attributes: # TestName is not typically a direct event attribute
            test_name_element = SummaryTree("TestName")
            test_name_element.attributes["Value"] = xml.sax.saxutils.escape(self.test_name)
            self.out.append(test_name_element)
        
        # The rest of self.out.children (generic events, errors, warnings) are added by their handlers.
        # The overall <Test> attributes (Ok, FailReason, etc.) are finalized in done().
        # This method now just ensures the basic shell of self.out.attributes is there.
        # The critical part is that `self.out.children` is populated by `parse_generic_event_as_child`
        # and specific handlers, and `self.out.attributes` by specific handlers and `done()`.

    def _try_read_file(self, path: Path | None, max_len: int = -1) -> str:
        if path is None or not path.exists():
            return ""
        with open(path, "r") as f:
            return f.read(max_len)

    def _write_run_times(self):
        # Implementation of _write_run_times method
        pass

    def _read_stats(self):
        # Implementation of _read_stats method
        pass

    def parse_file(self, file: Path):
        parser: Parser
        if file.suffix == ".json":
            parser = JsonParser()
        elif file.suffix == ".xml":
            parser = XmlParser()
        else:
            child = SummaryTree("TestHarnessBug")
            child.attributes["File"] = __file__
            frame = inspect.currentframe()
            if frame is not None:
                child.attributes["Line"] = str(inspect.getframeinfo(frame).lineno)
            child.attributes["Details"] = "Unexpected suffix {} for file {}".format(
                file.suffix, file.name
            )
            self.error = True
            self.out.append(child)
            return
        with file.open("r") as f:
            try:
                parser.parse(f, self.handler)
            except Exception as e:
                child = SummaryTree("SummarizationError")
                child.attributes["Severity"] = "40"
                child.attributes["ErrorMessage"] = str(e)
                _, _, exc_traceback = sys.exc_info()
                child.attributes["Trace"] = repr(traceback.format_tb(exc_traceback))
                self.out.append(child)

    def register_handlers(self):
        def remap_event_severity(attrs):
            if "Type" not in attrs or "Severity" not in attrs:
                return None
            k = (attrs["Type"], int(attrs["Severity"]))
            if k in self.severity_map:
                return str(self.severity_map[k])

        self.handler.add_handler(("Severity", None), remap_event_severity)

        def get_max_trace_time(attrs):
            if "Type" not in attrs:
                return None
            time = float(attrs["Time"])
            if time >= self.max_trace_time:
                self.max_trace_time = time
                self.max_trace_time_type = attrs["Type"]
            return None

        self.handler.add_handler(("Time", None), get_max_trace_time)

        def program_start(attrs: Dict[str, str]):
            if self.test_begin_found:
                return
            self.test_begin_found = True
            self.out.attributes["RandomSeed"] = attrs["RandomSeed"]
            self.out.attributes["SourceVersion"] = attrs["SourceVersion"]
            self.out.attributes["Time"] = attrs["ActualTime"]
            self.out.attributes["BuggifyEnabled"] = attrs["BuggifyEnabled"]
            self.out.attributes["DeterminismCheck"] = (
                "0" if self.expected_unseed is None else "1"
            )
            if self.binary.name != "fdbserver":
                self.out.attributes["OldBinary"] = self.binary.name
            if "FaultInjectionEnabled" in attrs:
                self.out.attributes["FaultInjectionEnabled"] = attrs[
                    "FaultInjectionEnabled"
                ]

        self.handler.add_handler(("Type", "ProgramStart"), program_start)

        def negative_test_success(attrs: Dict[str, str]):
            self.negative_test_success = True
            child = SummaryTree(attrs["Type"])
            for k, v in attrs:
                if k != "Type":
                    child.attributes[k] = v
            self.out.append(child)
            pass

        self.handler.add_handler(("Type", "NegativeTestSuccess"), negative_test_success)

        def config_string(attrs: Dict[str, str]):
            self.out.attributes["ConfigString"] = attrs["ConfigString"]

        self.handler.add_handler(("Type", "SimulatorConfig"), config_string)

        def set_test_file(attrs: Dict[str, str]):
            test_file = Path(attrs["TestFile"])
            cwd = Path(".").absolute()
            try:
                test_file = test_file.relative_to(cwd)
            except ValueError:
                pass
            self.out.attributes["TestFile"] = str(test_file)

        self.handler.add_handler(("Type", "Simulation"), set_test_file)
        self.handler.add_handler(("Type", "NonSimulationTest"), set_test_file)

        def set_elapsed_time(attrs: Dict[str, str]):
            logger.debug(f"set_elapsed_time called with attrs: {attrs}") # Log all attributes
            if self.test_end_found:
                logger.debug("set_elapsed_time: test_end_found was already True, returning.")
                return
            self.test_end_found = True
            try:
                self.unseed = int(attrs["RandomUnseed"])
                logger.debug(f"set_elapsed_time: self.unseed set to {self.unseed} from attrs['RandomUnseed']")
            except KeyError:
                logger.error(f"set_elapsed_time: 'RandomUnseed' key not found in ElapsedTime event attrs: {attrs}")
                self.unseed = -1 # Or some other indicator of invalidity
            except ValueError:
                random_unseed_val = attrs.get("RandomUnseed")
                logger.error(f"set_elapsed_time: Could not convert RandomUnseed '{random_unseed_val}' to int. Attrs: {attrs}")
                self.unseed = -1 # Or some other indicator of invalidity

            logger.debug(f"set_elapsed_time: Checking for unseed mismatch. self.expected_unseed: {self.expected_unseed}, current self.unseed: {self.unseed}")
            if (
                self.expected_unseed is not None
                and self.unseed != self.expected_unseed
                and self.unseed != -1
            ):
                logger.info(f"UnseedMismatch DETECTED. Expected: {self.expected_unseed}, Got: {self.unseed}")
                severity = (
                    40
                    if ("UnseedMismatch", 40) not in self.severity_map
                    else self.severity_map[("UnseedMismatch", 40)]
                )
                if severity >= 30:
                    child = SummaryTree("UnseedMismatch")
                    child.attributes["Unseed"] = str(self.unseed)
                    child.attributes["ExpectedUnseed"] = str(self.expected_unseed)
                    child.attributes["Severity"] = str(severity)
                    if severity >= 40:
                        self.error = True
                        self.why = "UnseedMismatch"
                        logger.info(f"UnseedMismatch (Severity {severity}) causing self.error = True and self.why = \"UnseedMismatch\"")
                    else:
                        logger.info(f"UnseedMismatch (Severity {severity}) NOT causing self.error = True (severity < 40)")
                    self.out.append(child)
                else:
                    logger.info(f"UnseedMismatch severity {severity} is < 30, not adding XML child or setting error.")
            elif self.expected_unseed is not None:
                logger.debug(f"set_elapsed_time: No UnseedMismatch. Conditions: expected_unseed_is_None={self.expected_unseed is None}, unseed_equals_expected={self.unseed == self.expected_unseed}, unseed_is_-1={self.unseed == -1}")

            self.out.attributes["SimElapsedTime"] = attrs["SimTime"]
            self.out.attributes["RealElapsedTime"] = attrs["RealTime"]
            if self.unseed is not None:
                self.out.attributes["RandomUnseed"] = str(self.unseed)

        self.handler.add_handler(("Type", "ElapsedTime"), set_elapsed_time)

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
            if "ErrorIsInjectedFault" in attrs and attrs[
                "ErrorIsInjectedFault"
            ].lower() in ["1", "true"]:
                # ignore injected errors. In newer fdb versions these will have a lower severity
                return
            self.errors += 1
            self.error = True
            if self.errors > config.max_errors:
                return
            child = SummaryTree(attrs["Type"])
            for k, v in attrs.items():
                child.attributes[k] = v
            self.out.append(child)

        self.handler.add_handler(("Severity", "40"), parse_error)

        def coverage(attrs: Dict[str, str]):
            covered = True
            if "Covered" in attrs:
                covered = int(attrs["Covered"]) != 0
            comment = ""
            if "Comment" in attrs:
                comment = attrs["Comment"]
            rare = False
            if "Rare" in attrs:
                rare = bool(int(attrs["Rare"]))
            c = Coverage(attrs["File"], attrs["Line"], comment, rare)
            if covered or c not in self.coverage:
                self.coverage[c] = covered

        self.handler.add_handler(("Type", "CodeCoverage"), coverage)

        def expected_test_pass(attrs: Dict[str, str]):
            self.test_count = int(attrs["Count"])

        self.handler.add_handler(("Type", "TestsExpectedToPass"), expected_test_pass)

        def test_passed(attrs: Dict[str, str]):
            if attrs["Passed"] == "1":
                self.tests_passed += 1

        self.handler.add_handler(("Type", "TestResults"), test_passed)

        def remap_event_severity(attrs: Dict[str, str]):
            self.severity_map[
                (attrs["TargetEvent"], int(attrs["OriginalSeverity"]))
            ] = int(attrs["NewSeverity"])

        self.handler.add_handler(("Type", "RemapEventSeverity"), remap_event_severity)

        def buggify_section(attrs: Dict[str, str]):
            if attrs["Type"] == "FaultInjected" or attrs.get("Activated", "0") == "1":
                child = SummaryTree(attrs["Type"])
                child.attributes["File"] = attrs["File"]
                child.attributes["Line"] = attrs["Line"]
                self.out.append(child)

        self.handler.add_handler(("Type", "BuggifySection"), buggify_section)
        self.handler.add_handler(("Type", "FaultInjected"), buggify_section)

        def running_unit_test(attrs: Dict[str, str]):
            child = SummaryTree("RunningUnitTest")
            child.attributes["Name"] = attrs["Name"]
            child.attributes["File"] = attrs["File"]
            child.attributes["Line"] = attrs["Line"]

        self.handler.add_handler(("Type", "RunningUnitTest"), running_unit_test)

        def stderr_severity(attrs: Dict[str, str]):
            if "NewSeverity" in attrs:
                self.stderr_severity = attrs["NewSeverity"]

        self.handler.add_handler(("Type", "StderrSeverity"), stderr_severity)

        # Add the generic event handler - should be processed for events not caught by specific handlers above.
        # The ParseHandler.handle logic might need adjustment if order of add_handler matters greatly
        # for preventing double processing. For now, parse_generic_event_as_child has internal skip logic.
        self.handler.add_handler(None, self.parse_generic_event_as_child)

    def done(self):
        if self._already_done:
            logger.debug("Summary.done() called again, skipping.")
            return
        self._already_done = True

        if self.test_begin_found and not self.test_end_found and not self.was_killed:
            logger.warning(
                f"Test {self.test_name} (UID: {self.uid}, PartUID: {self.current_part_uid}) started but did not finish. Exit code: {self.exit_code}. Marking as error."
            )
            self.error = True
            if "FailReason" not in self.out.attributes:
                 self.out.attributes["FailReason"] = "TestDidNotFinish"

        if self.exit_code != 0 and not self.error and not self.was_killed and not self.is_negative_test:
            logger.warning(
                f"Test {self.test_name} (UID: {self.uid}, PartUID: {self.current_part_uid}) had non-zero exit code {self.exit_code} but no error reported. Marking as error."
            )
            self.error = True
            if "FailReason" not in self.out.attributes:
                self.out.attributes["FailReason"] = "NonZeroExitCodeNoError"

        if self.was_killed:
            self.error = True
            self.out.attributes["Killed"] = "1"
            if "FailReason" not in self.out.attributes:
                self.out.attributes["FailReason"] = "TestKilled"

        # New: Check for stderr output if no other error reported yet and exit code was 0 for a positive test
        if not self.error and not self.is_negative_test and self.exit_code == 0 and \
           self.error_out and len(self.error_out.strip()) > 0:
            logger.warning(
                f"Test {self.test_name} (UID: {self.uid}, PartUID: {self.current_part_uid}) "
                f"had exit code 0 but produced stderr output. Marking as error. Stderr: {self.error_out[:200]}"
            )
            self.error = True
            if "FailReason" not in self.out.attributes:
                self.out.attributes["FailReason"] = "StdErrOutputWithZeroExit"
            # Also ensure 'Ok' is set to '0' for the attribute logic that follows
            self.out.attributes["Ok"] = "0"

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

        if self.valgrind_out_file is not None and self.valgrind_out_file.exists():
            errors = parse_valgrind_output(self.valgrind_out_file)
            if len(errors) > 0:
                self.error = True
                self.out.attributes["Ok"] = "0"
                if "FailReason" not in self.out.attributes:
                    self.out.attributes["FailReason"] = "ValgrindErrors"
                valgrind_summary = SummaryTree("ValgrindError")
                for error in errors:
                    valgrind_summary.children.append(SummaryTree(error))
                self.out.append(valgrind_summary)

        if self.error_out is not None and len(self.error_out) > 0 and self.stderr_severity != "0":
            if len(self.error_out) > config.max_stderr_bytes:
                self.out.append(
                    SummaryTree(
                        "StdErrOutputTruncated",
                        attributes={"Bytes": str(config.max_stderr_bytes)},
                    )
                )
                self.out.append(
                    SummaryTree(
                        "StdErrOutput",
                        attributes={"Content": self.error_out[0 : config.max_stderr_bytes]},
                    )
                )
            else:
                self.out.append(
                    SummaryTree("StdErrOutput", attributes={"Content": self.error_out})
                )

        if config.write_run_times and self.test_name is not None:
            # ... existing code ...
            pass

        if config.print_coverage:
            for k, v in self.coverage.items():
                child = SummaryTree("CodeCoverage")
                child.attributes["File"] = k.file
                child.attributes["Line"] = str(k.line)
                child.attributes["Rare"] = k.rare
                if not v:
                    child.attributes["Covered"] = "0"
                if k.comment is not None and len(k.comment):
                    child.attributes["Comment"] = k.comment
                self.out.append(child)
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
        if self.max_rss is not None:
            self.out.attributes["PeakMemory"] = str(self.max_rss)

        # Add Runtime attribute, formatting to match V1 example if possible
        if self.runtime is not None:
            # V1 example shows many decimal places, e.g., "16.95333433151245"
            # Using a general float to string conversion, can be refined if specific precision is critical.
            self.out.attributes["Runtime"] = str(self.runtime) # Format as float string

        # Final determination of Ok and FailReason for the top-level <Test> attributes
        # This was previously in _generate_xml_summary, but makes more sense here after all error checks
        current_ok_status = self.ok() # Call ok() once after all self.error updates
        self.out.attributes["Ok"] = "1" if current_ok_status else "0"
        if not current_ok_status:
            final_reason = self.why # Use reason determined by the error logic
            if not final_reason: # Fallbacks, though self.why should ideally be set
                if self.error:
                    final_reason = "ProducedErrors"
                elif not self.test_end_found and (self.exit_code is None or self.exit_code == 0):
                    final_reason = "TestDidNotFinish"
                elif self.tests_passed == 0 and self.test_count > 0:
                    final_reason = "NoTestsPassed"
                elif self.test_count != self.tests_passed:
                    final_reason = "Expected {} tests to pass, but only {} did".format(
                        self.test_count, self.tests_passed
                    )
                else:
                    final_reason = "Unknown"
            self.out.attributes["FailReason"] = final_reason
        elif "FailReason" in self.out.attributes: # Clean up FailReason if test is Ok
            del self.out.attributes["FailReason"]
        
        if self.archive_logs_on_failure and not self.ok(): # self.ok() reflects the final status
            logger.info(f"Test part {self.current_part_uid} (TestUID: {self.uid}, Name: {self.test_name}) failed and archive_logs_on_failure is True. Adding log location tags to summary.")
            self.archival_references_added = True # Set flag

            if self._rtd_for_archive and self._rtd_for_archive.exists():
                log_dir_elem = SummaryTree("FDBClusterLogDir")
                log_dir_elem.attributes["path"] = str(self._rtd_for_archive.resolve())
                self.out.append(log_dir_elem)
            
            if self.current_run_stdout_path and self.current_run_stdout_path.exists():
                stdout_elem = SummaryTree("HarnessLogFile")
                stdout_elem.attributes["type"] = "stdout"
                stdout_elem.attributes["path"] = str(self.current_run_stdout_path.resolve())
                self.out.append(stdout_elem)

            if self.current_run_stderr_path and self.current_run_stderr_path.exists():
                stderr_elem = SummaryTree("HarnessLogFile")
                stderr_elem.attributes["type"] = "stderr"
                stderr_elem.attributes["path"] = str(self.current_run_stderr_path.resolve())
                self.out.append(stderr_elem)
            
            if self.current_run_command_file_path and self.current_run_command_file_path.exists():
                cmd_elem = SummaryTree("HarnessLogFile")
                cmd_elem.attributes["type"] = "command"
                cmd_elem.attributes["path"] = str(self.current_run_command_file_path.resolve())
                self.out.append(cmd_elem)

            if self.fdb_log_files_for_archival:
                for log_file_path in self.fdb_log_files_for_archival:
                    if log_file_path.exists():
                        fdb_file_elem = SummaryTree("FDBLogFile")
                        fdb_file_elem.attributes["path"] = str(log_file_path.resolve())
                        self.out.append(fdb_file_elem)
            
            if self._jod_for_archive and self._jod_for_archive.exists():
                jod_elem = SummaryTree("JoshuaOutputDirRef")
                jod_elem.attributes["path"] = str(self._jod_for_archive.resolve())
                self.out.append(jod_elem)
        
        self._already_done = True # Mark as done

    def ok(self):
        # logical xor -- a test is successful if there was either no error or we expected errors (negative test)
        return (not self.error) != self.is_negative_test

    def get_v1_stdout_line(self) -> Optional[str]:
        if not self._already_done: # Ensure done() has run to determine ok() status and archival_references_added
            logger.warning("get_v1_stdout_line called before self.done(). Finalizing summary state.")
            self.done()

        if not hasattr(self, 'out') or not self.out or self.out.name != "Test":
            logger.error("get_v1_stdout_line: self.out is not a valid 'Test' SummaryTree.")
            return '<Test Ok="0" Error="V1SummaryGenFailed_InvalidState" PartUID="UNKNOWN_PART_UID"/>'

        logger.debug(f"Generating V1 stdout line for TestUID: {self.uid}, PartUID: {self.current_part_uid} - Attempting to include essential V1 child elements and specific attributes.")

        if not self._already_done:
            logger.warning("get_v1_stdout_line called before self.done(). Finalizing summary state.")
            self.done() # self.done() populates self.out.attributes, including PeakMemory and Runtime from self.max_rss and self.runtime
        
        v1_test_element = ET.Element("Test")

        # Copy all attributes from the main summary's <Test> element
        for k, v in self.out.attributes.items():
            v1_test_element.set(k, v)

        # Explicitly set/override specific attributes for V1 stdout compatibility
        # Runtime: Use the value from self.runtime (populated by TestRun from process stats)
        if self.runtime is not None:
            v1_test_element.set("Runtime", str(self.runtime))
        elif "Runtime" not in v1_test_element.attrib: # Fallback if self.runtime was None but we need the attr
            v1_test_element.set("Runtime", "0")

        # PeakMemory: Use the value from self.max_rss (populated by TestRun from process stats)
        if self.max_rss is not None:
            v1_test_element.set("PeakMemory", str(self.max_rss))
        elif "PeakMemory" not in v1_test_element.attrib:
            v1_test_element.set("PeakMemory", "0")

        # TestRunCount: Set to "1" for a single test stdout line
        v1_test_element.set("TestRunCount", "1")

        # TotalTestTime: Use RealElapsedTime (if available) converted to int, as an approximation of V1's TotalTestTime
        real_et_str = self.out.attributes.get("RealElapsedTime")
        if real_et_str is not None:
            try:
                total_test_time_val = str(int(float(real_et_str)))
                v1_test_element.set("TotalTestTime", total_test_time_val)
            except ValueError:
                logger.warning(f"Could not convert RealElapsedTime ('{real_et_str}') to int for TotalTestTime for V1 stdout.")
        elif "TotalTestTime" not in v1_test_element.attrib: # Fallback if RealElapsedTime missing
             v1_test_element.set("TotalTestTime", "0")

        # Statistics attribute (special handling as before)
        if self.stats_attribute_for_v1:
            v1_test_element.set("Statistics", self.stats_attribute_for_v1)
        
        # Add essential child elements (logic from previous step)
        for child_summary_tree in self.out.children:
            if child_summary_tree.name in ESSENTIAL_V1_CHILD_TAGS_TO_KEEP and \
               child_summary_tree.name not in STDOUT_EXPLICITLY_STRIPPED_TAGS:
                try:
                    child_et_element = child_summary_tree.to_et_element()
                    v1_test_element.append(child_et_element)
                    logger.debug(f"Included child element '{child_summary_tree.name}' in V1 stdout.")
                except Exception as e_child_conversion:
                    logger.error(f"Error converting or appending child '{child_summary_tree.name}' for V1 stdout: {e_child_conversion}", exc_info=True)

        try:
            v1_xml_string = ET.tostring(v1_test_element, encoding='unicode', method='xml', short_empty_elements=True)
            final_xml_output = v1_xml_string.strip()
            logger.debug(f"Generated V1 stdout XML (with children and explicit V1 attrs): {final_xml_output}")
            return final_xml_output
        except Exception as e_tostring:
            logger.error(f"get_v1_stdout_line: Failed to serialize test element (with children and V1 attrs): {e_tostring}", exc_info=True)
            return '<Test Ok="0" Error="V1SummaryGenFailed_SerializationFailed_V1Attrs" PartUID="{}"/>'.format(self.current_part_uid if self.current_part_uid else "UNKNOWN_PART_UID")
