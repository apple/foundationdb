import argparse
import logging
import pathlib
import random
import sys
import time
import traceback
import os
import xml.etree.ElementTree as ET
from xml.sax import saxutils # For escaping
import io # For StreamTee fileno exception
import copy # For deepcopy in stripping function

# Initialize logger at module level with a NullHandler to prevent "No handlers found" warnings
# if the script fails before logging is fully configured in main().
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

APP_PY_DEBUG_LOG_FILE_PATH = None # Global to store the path of the app.py specific debug log
original_stdout = sys.stdout # Capture at module load time
original_stderr = sys.stderr # Capture at module load time

# Import from summarize for stripping logic
from test_harness.summarize import TAGS_TO_STRIP_FROM_TEST_ELEMENT_FOR_STDOUT

class StreamTee:
    """
    A file-like object that "tees" writes to two streams: an original target
    stream and a log stream.

    It can be configured to write to both, or only to the log stream, effectively
    suppressing output to the original target while still capturing it.

    This class is intended to replace streams like `sys.stdout` or `sys.stderr`
    to allow simultaneous capture of output to a log file and optional pass-through
    to the console or original destination.

    Attributes:
        original_stream_target: The primary stream (e.g., `sys.stdout`, `sys.stderr`)
                                that output may be passed to.
        log_stream: The secondary stream (e.g., an open file object) where all
                    output is unconditionally written.
        pass_to_original_target (bool): If True, writes are passed to
                                        `original_stream_target`. If False, writes
                                        only go to `log_stream`.
    """
    def __init__(self, original_stream_target, log_stream, pass_to_original_target: bool):
        self.original_stream_target = original_stream_target # e.g. original_stdout
        self.log_stream = log_stream                 # e.g. log_file_stream_ref
        self.pass_to_original_target = pass_to_original_target

    def write(self, data):
        # Always write to log_stream (if available)
        if self.log_stream and not self.log_stream.closed:
            try:
                self.log_stream.write(data)
            except Exception as e_log:
                # Fallback: print to original stderr if logging to file fails
                print(f"StreamTee: Error writing to log_stream: {e_log}", file=original_stderr)

        # Conditionally write to original_stream_target
        if self.pass_to_original_target:
            if self.original_stream_target:
                try:
                    self.original_stream_target.write(data)
                except Exception as e_orig:
                    print(f"StreamTee: Error writing to original_stream_target: {e_orig}", file=original_stderr)

        self.flush() # Flush both/either based on their state/flags after every write

    def flush(self):
        # Always flush log_stream (if available and flushing is enabled for it)
        if self.log_stream and not self.log_stream.closed and hasattr(self.log_stream, 'flush'):
            try:
                self.log_stream.flush()
            except Exception as e_log_flush:
                print(f"StreamTee: Error flushing log_stream: {e_log_flush}", file=original_stderr)

        # Conditionally flush original_stream_target
        if self.pass_to_original_target:
            if self.original_stream_target and hasattr(self.original_stream_target, 'flush'):
                try:
                    self.original_stream_target.flush()
                except Exception as e_orig_flush:
                    print(f"StreamTee: Error flushing original_stream_target: {e_orig_flush}", file=original_stderr)

    def fileno(self):
        # fileno should primarily refer to the stream that replaces the original sys.std*, if any pass-through is happening.
        # If not passing to original, or if original doesn't support fileno, this becomes tricky.
        # For many use cases (like subprocess), if sys.stdout is a pipe, fileno is needed.
        # If pass_to_original_target is True and original_stream_target has fileno, use it.
        if self.pass_to_original_target and hasattr(self.original_stream_target, 'fileno'):
            return self.original_stream_target.fileno()
        # If only logging or original target doesn't have fileno, but log_stream does (e.g. if it were a pipe, unlikely for file):
        # This might be problematic. For now, prefer original_stream_target if available and passing through.
        # If not passing to original, what should fileno be? Some libraries check this.
        # Defaulting to log_stream's fileno if original isn't used/doesn't have one can be an option,
        # or raising an error. Raising error is safer if the behavior is undefined for the use case.
        if hasattr(self.log_stream, 'fileno'): # Check log_stream if not using original_stream_target for fileno
             # This case might be for when original_stream_target is not active but something still needs a fileno.
             # However, usually fileno() is called on what sys.stdout IS.
             # If not passing to original, then sys.stdout *is* this Tee object which primarily writes to log.
             pass

        # Safest: if original_stream_target (conditionally active) doesn't have fileno, then this Tee object doesn't robustly provide one.
        # The previous code used self.stream1.fileno() which was original_stdout/stderr
        if hasattr(self.original_stream_target, 'fileno'):
             return self.original_stream_target.fileno()
        raise io.UnsupportedOperation("StreamTee: fileno not available on the configured original_stream_target or not passing through.")

from test_harness.config import config
from test_harness.run import TestRunner
from test_harness.summarize import SummaryTree

def strip_elements_for_v1_stdout(xml_string: str) -> str:
    """
    Parses an XML string, removes specified child elements from the root,
    and returns the modified XML string.
    Designed for fatal error XMLs in app.py that need V1 stdout formatting.
    """
    global logger
    try:
        source_element = ET.fromstring(xml_string)
        logger.debug(f"strip_elements_for_v1_stdout: Original XML for stripping: {xml_string}")

        children_tags_before_filtering = [child.tag for child in list(source_element)]
        logger.debug(f"strip_elements_for_v1_stdout: Children tags BEFORE filtering: {children_tags_before_filtering}")

        # Create a new root element (e.g., <Test>) and copy attributes
        filtered_element = ET.Element(source_element.tag)
        for k, v in source_element.attrib.items():
            filtered_element.set(k, v)

        kept_children_tags = []
        for child in list(source_element):
            if child.tag not in TAGS_TO_STRIP_FROM_TEST_ELEMENT_FOR_STDOUT:
                # Important: We need to append a copy of the child, not the child itself if it's from a tree
                # that might be used elsewhere, though for ET.fromstring result, it's a new tree.
                # For safety and consistency with summarize.py's deepcopy behavior before filtering:
                filtered_element.append(copy.deepcopy(child))
                kept_children_tags.append(child.tag)
            else:
                logger.debug(f"strip_elements_for_v1_stdout: Identified for stripping - child.tag: '{child.tag}'")

        logger.debug(f"strip_elements_for_v1_stdout: Children tags AFTER filtering: {kept_children_tags}")

        # Serialize the new filtered element
        # short_empty_elements=True is generally preferred for V1.
        # create_fatal_error_xml uses short_empty_elements=False, but if JoshuaMessage is the only child and stripped,
        # short_empty_elements=True makes <Test ... /> which is fine.
        modified_xml_string = ET.tostring(filtered_element, encoding='unicode', short_empty_elements=True).strip()
        logger.debug(f"strip_elements_for_v1_stdout: XML after stripping: {modified_xml_string}")
        return modified_xml_string

    except Exception as e:
        logger.error(f"strip_elements_for_v1_stdout: Failed to parse or strip XML string '{xml_string[:100]}...': {e}", exc_info=True)
        # Fallback: return the original string, or a generic error XML if parsing failed badly
        # For safety, return original string as stripping is best-effort for these fatal paths.
        return xml_string

def create_fatal_error_xml(message: str, error_type: str = "FatalError", test_uid: str = "UNKNOWN_UID", joshua_seed: str = "UNKNOWN_SEED") -> str:
    global logger
    try:
        # config.joshua_seed might not be initialized if this is called very early.
        # Access it carefully.
        joshua_seed_val = "UNKNOWN_SEED_IN_FATAL_XML"
        if hasattr(config, 'joshua_seed') and config.joshua_seed is not None:
            # In config.py, joshua_seed is an int after extract_args.
            # If extract_args hasn't run, it might be a ConfigValue object or the default int.
            # For robustness, handle if it's a ConfigValue object, though ideally extract_args runs first.
            if hasattr(config.joshua_seed, 'value') and config.joshua_seed.value is not None:
                 joshua_seed_val = str(config.joshua_seed.value)
            elif isinstance(config.joshua_seed, int):
                 joshua_seed_val = str(config.joshua_seed)
            # else, it remains UNKNOWN_SEED_IN_FATAL_XML or the passed joshua_seed param
        elif joshua_seed != "UNKNOWN_SEED": # Use parameter if config is not available
            joshua_seed_val = str(joshua_seed)

        test_element = ET.Element("Test")
        test_element.set("TestUID", test_uid)
        test_element.set("JoshuaSeed", joshua_seed_val)
        test_element.set("Ok", "0")
        test_element.set("Error", saxutils.escape(str(error_type)))

        jm_element = ET.SubElement(test_element, "JoshuaMessage")
        jm_element.set("Severity", "40") # Error severity
        jm_element.set("Message", saxutils.escape(str(message)))

        # short_empty_elements=False ensures <JoshuaMessage ...></JoshuaMessage> if no other children
        return ET.tostring(test_element, encoding='unicode', short_empty_elements=False).strip()
    except Exception as e:
        logger.error(f"CRITICAL_ERROR_IN_CREATE_FATAL_ERROR_XML: {e}", exc_info=True)
        # Fallback string if XML creation itself fails.
        return f'<Test TestUID="{saxutils.escape(test_uid)}" JoshuaSeed="{saxutils.escape(joshua_seed)}" Ok="0" Error="EmergencyXmlCreationFail"><JoshuaMessage Severity="40" Message="XML creation itself failed: {saxutils.escape(str(e))}" /></Test>'

def main():
    pid = os.getpid()
    timestamp = int(time.time())
    final_exit_code = 1 # Default to error
    final_stdout_xml = '<Test Ok="0" Error="AppPy_UnhandledError_BeforeTry" />' # Fallback stdout
    full_summary_tree_result = None
    log_file_stream = None
    # raw_fatal_xml_for_file is used to signal if a specific fatal error XML was already generated and should take precedence for joshua.xml
    raw_fatal_xml_for_file_main_scope = None

    try:
        log_file_stream = setup_logging(pid, timestamp)

        # Initial config parsing and validation (may exit)
        # This function would internally handle its specific errors, XML generation, and sys.exit()
        # or raise a specific exception to be caught here.
        # For simplicity in this sketch, let's assume it raises on error.
        try:
            parse_and_validate_config(pid, timestamp) # This would use the global config object
        except ConfigError as ce: # Custom exception from parse_and_validate_config
            raw_fatal_xml_for_file_main_scope = ce.xml_content # Assume exception carries this
            final_stdout_xml = ce.stdout_xml
            final_exit_code = 1
            # original_stdout.write(final_stdout_xml + '\\n') is done in the handler or here
            # writing to joshua.xml for this specific error is also handled in the handler or here
            raise # Re-raise to go to the main finally block for cleanup

        setup_stream_redirection(log_file_stream) # Uses global config

        # Early config checks after stream redirection (may exit or raise)
        try:
            perform_early_config_checks_and_exit_on_error(pid, timestamp) # Uses global config
        except EarlyExitError as eee: # Custom exception
            raw_fatal_xml_for_file_main_scope = eee.xml_content
            final_stdout_xml = eee.stdout_xml
            final_exit_code = 1
            raise

        # Main test execution
        full_summary_tree_result, final_exit_code = run_tests_and_get_summary(config)
        # At this point, if successful, individual test summaries were written to config._v1_summary_output_stream by TestRunner/Summary.get_v1_stdout_line()
        # We don't write a single stdout XML line here for the *overall* run in the success path.
        # Joshua expects per-test XML lines if multiple tests/parts are run.
        # If only one "test" (the whole app invocation) is considered, TestRunner would need to make one XML for stdout.
        # Let's assume TestRunner/Summary handle the V1 stdout for actual tests.
        # The `stdout_xml_final_str` is mainly for app-level fatal errors.

    except ConfigError: # Or other specific custom exceptions from setup
        # Already handled in terms of preparing XML, just ensure it flows to finally
        pass # final_exit_code and raw_fatal_xml_for_file_main_scope are set
    except EarlyExitError:
        pass # final_exit_code and raw_fatal_xml_for_file_main_scope are set
    except Exception as e_global:
        logger.error(f"Unhandled global exception in main: {e_global}", exc_info=True)
        is_v1_stdout = config._v1_summary_output_stream == original_stdout if hasattr(config, '_v1_summary_output_stream') else True
        raw_fatal_xml_for_file_main_scope, final_stdout_xml, final_exit_code = handle_global_exception_outputs(
            e_global, pid, timestamp, is_v1_stdout
        )
        if original_stdout and not original_stdout.closed:
            original_stdout.write(final_stdout_xml + '\\n')
            original_stdout.flush()
    finally:
        logger.info(f"--- TestHarness2 app.py execution finishing. Exit code: {final_exit_code} ---")

        if raw_fatal_xml_for_file_main_scope:
            # A specific fatal error XML was generated (config, early exit, or global unhandled)
            # This should be written to joshua.xml if joshua_output_dir is known
            if hasattr(config, 'joshua_output_dir') and config.joshua_output_dir:
                try:
                    joshua_xml_path = config.joshua_output_dir / "joshua.xml"
                    config.joshua_output_dir.mkdir(parents=True, exist_ok=True)
                    with open(joshua_xml_path, "w") as f:
                        f.write(raw_fatal_xml_for_file_main_scope)
                    logger.info(f"Fatal error XML saved to {joshua_xml_path}")
                except Exception as e_write_fatal_final:
                    logger.error(f"Could not write fatal error XML to {joshua_xml_path} in finally: {e_write_fatal_final}", exc_info=True)
            else:
                logger.error("joshua_output_dir not configured, cannot save fatal error joshua.xml in finally.")

        elif full_summary_tree_result:
            # Normal path: tests ran, write their summary
            write_summary_to_joshua_xml(full_summary_tree_result, config)
        else:
            # No summary tree and no specific fatal error XML recorded for joshua.xml
            logger.warning("No test summary tree available and no specific fatal error XML generated for joshua.xml in finally.")
            # Optionally, write a very basic failure XML to joshua.xml here if desired
            if hasattr(config, 'joshua_output_dir') and config.joshua_output_dir and final_exit_code != 0:
                try:
                    fallback_xml = create_fatal_error_xml(f"App.py finished with exit code {final_exit_code} but no specific summary/error XML was designated for joshua.xml.", "AppPyGenericFinalError")
                    joshua_xml_path = config.joshua_output_dir / "joshua.xml"
                    config.joshua_output_dir.mkdir(parents=True, exist_ok=True)
                    with open(joshua_xml_path, "w") as f: f.write(fallback_xml)
                    logger.info(f"Generic fallback error XML written to {joshua_xml_path}")
                except Exception as e_write_fallback:
                     logger.error(f"Could not write generic fallback error XML to joshua.xml in finally: {e_write_fallback}", exc_info=True)


        if log_file_stream and not log_file_stream.closed:
            log_file_stream.close()

        logger.info(f"Exiting with code {final_exit_code}.")
        sys.exit(final_exit_code)

# Define helper:
def handle_global_exception_outputs(exc, pid, timestamp, is_v1_stdout):
    joshua_seed_str = str(config.joshua_seed) if hasattr(config, 'joshua_seed') and config.joshua_seed is not None else "UNKNOWN_SEED_IN_EXCEPTION"
    raw_xml = create_fatal_error_xml(
        message=f"Unhandled exception: {traceback.format_exc()}",
        error_type=type(exc).__name__,
        joshua_seed=joshua_seed_str
    )
    stdout_xml = raw_xml.replace('\\n', ' ').strip()
    if is_v1_stdout:
        stdout_xml = strip_elements_for_v1_stdout(raw_xml).replace('\\n', ' ').strip()
    return raw_xml, stdout_xml, 1

# Need to define ConfigError and EarlyExitError custom exceptions
class AppPySetupError(Exception):
    """Base class for errors during app.py setup that should produce specific XML output."""
    def __init__(self, message, stdout_xml, xml_content_for_file, *args):
        super().__init__(message, *args)
        self.stdout_xml = stdout_xml
        self.xml_content = xml_content_for_file

class ConfigError(AppPySetupError): pass
class EarlyExitError(AppPySetupError): pass

if __name__ == "__main__":
    main()
