from __future__ import annotations

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
from pathlib import Path
from typing import Optional, List, Tuple
import signal
from datetime import datetime

from test_harness.config import Config

# Initialize logger at module level with a NullHandler to prevent "No handlers found" warnings
# if the script fails before logging is fully configured in main().
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

APP_PY_DEBUG_LOG_FILE_PATH = None # Global to store the path of the app.py specific debug log
original_stdout = sys.stdout # Capture at module load time
original_stderr = sys.stderr # Capture at module load time

# Import from summarize for stripping logic
from test_harness.summarize import TAGS_TO_STRIP_FROM_TEST_ELEMENT_FOR_STDOUT

from test_harness.config import config # Ensure config is imported for setup_logging
from test_harness.run import TestRunner # Import TestRunner
from test_harness.summarize import SummaryTree # Import SummaryTree

# Define setup_logging function here
def setup_logging(config, process_id: int, timestamp: int, existing_logger: Optional[logging.Logger] = None) -> logging.Logger:
    """
    Configures the global logger for the application.
    If an existing_logger is passed, it reconfigures it instead of creating a new one.
    """
    logger = existing_logger or logging.getLogger("__main__")
    
    # Explicitly close and remove old handlers to release file locks
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    log_file_path: Optional[Path] = None

    # Preferred path: <joshua_output_dir>/app_log.txt
    if hasattr(config, 'joshua_output_dir') and config.joshua_output_dir:
        try:
            # The shell script should have already created this directory
            jod = Path(config.joshua_output_dir)
            jod.mkdir(parents=True, exist_ok=True)
            log_file_path = jod / 'app_log.txt'
        except (OSError, PermissionError) as e:
            original_stderr.write(f"app.py setup_logging: Could not create or access joshua_output_dir '{config.joshua_output_dir}'. Error: {e}. Falling back to emergency log.\n")
            log_file_path = None

    # Fallback path if the preferred path is not usable
    if log_file_path is None:
        emergency_dir = Path('/tmp')
        emergency_dir.mkdir(exist_ok=True)
        log_file_path = emergency_dir / f"th_emergency_app_log.{process_id}.{timestamp}.log"
        if existing_logger is None: # Only print this on the very first setup
            original_stderr.write(f"app.py setup_logging: Using emergency log file: {log_file_path}\n")

    # Basic logging config
    log_level = logging.INFO
    if hasattr(config, 'log_level') and isinstance(config.log_level, str):
        level_from_config = getattr(logging, config.log_level.upper(), logging.INFO)
        if isinstance(level_from_config, int):
            log_level = level_from_config

    formatter = logging.Formatter("%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s")
    
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    
    logger.info(f"Logger configured by setup_logging. Log file: {log_file_path}, Level: {logging.getLevelName(log_level)}")
    return logger

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
    """Main entry point of the TestHarnessV2 application."""
    # =========================================================================
    # Phase 1: Pre-Setup and Initial Checks (No File Logging)
    # =========================================================================
    # Until config is parsed, we cannot safely log to a file.
    # All critical early messages will go to the original stderr.
    pid = os.getpid()
    timestamp = int(datetime.now().timestamp())
    logger = logging.getLogger("__main__")
    logger.addHandler(logging.NullHandler()) # Prevent "no handlers" warnings
    config: Optional[Config] = None
    summary_tree_for_joshua_xml: Optional[SummaryTree] = None
    final_exit_code = 0

    try:
        # =====================================================================
        # Phase 2: Configuration Parsing
        # =====================================================================
        # This is the first major step. If this fails, we exit.
        try:
            config, args = parse_and_validate_config(sys.argv)
        except ConfigError as e:
            original_stderr.write(f"FATAL: Configuration error: {e}\\n")
            sys.exit(1)
        except Exception as e:
            original_stderr.write(f"FATAL: Unexpected error during config parsing: {e}\\n")
            original_stderr.write(traceback.format_exc() + "\\n")
            sys.exit(1)

        # =====================================================================
        # Phase 3: Setup Logging and Stream Redirection (NOW with config)
        # =====================================================================
        # Now that we have a valid config, we can set up file logging once.
        logger = setup_logging(config, pid, timestamp)
        
        logger.info("Configuration parsed and logger initialized successfully.")
        
        # Now, set up stream redirection to capture stdout/stderr for archival
        stdout_tee, stderr_tee = setup_stream_redirection(config, logger)


        # =====================================================================
        # Phase 4: Main Test Execution Logic
        # =====================================================================
        perform_early_config_checks_and_exit_on_error(config)
        
        summary_tree_for_joshua_xml, structural_exit_code = run_tests_and_get_summary(config, args)
        
        if structural_exit_code != 0:
            final_exit_code = structural_exit_code
            logger.warning(f"Test runner returned a non-zero structural exit code: {structural_exit_code}")

    except Exception as e_global:
        final_exit_code = 1
        logger.error(f"Unhandled global exception in main: {e_global}", exc_info=True)
        # Create a minimal failure summary if one doesn't exist
        if summary_tree_for_joshua_xml is None:
            summary_tree_for_joshua_xml = SummaryTree("TestHarnessRun")
            summary_tree_for_joshua_xml.attributes["Ok"] = "0"
            summary_tree_for_joshua_xml.attributes["FailReason"] = "UnhandledException"
            summary_tree_for_joshua_xml.attributes["Exception"] = str(e_global)
    
    finally:
        # =====================================================================
        # Phase 5: Finalization and Cleanup
        # =====================================================================
        
        # Determine final exit code based on test results
        if summary_tree_for_joshua_xml is not None and final_exit_code == 0:
            if summary_tree_for_joshua_xml.attributes.get("BatchSuccess") == "0":
                logger.info("Root summary tree BatchSuccess='0', setting final_exit_code to 1.")
                final_exit_code = 1
        
        # Write the final summary to joshua.xml
        if config is not None:
            write_summary_to_joshua_xml(summary_tree_for_joshua_xml, config)
        else:
            # This can happen if config parsing itself failed.
            original_stderr.write("Could not write final joshua.xml because config was not available.\\n")

        # Restore original stdout/stderr
        if 'stdout_tee' in locals() and sys.stdout == stdout_tee:
            sys.stdout = original_stdout
        if 'stderr_tee' in locals() and sys.stderr == stderr_tee:
            sys.stderr = original_stderr
            
        logger.info(f"--- TestHarness2 app.py execution finishing. Exit code: {final_exit_code} ---")
        logging.shutdown()
        sys.exit(final_exit_code)

def parse_and_validate_config(argv: List[str]) -> Tuple[Config, argparse.Namespace]:
    """
    Parses command line arguments and environment variables, validates them,
    and returns a populated Config object. Exits on error.
    """
    # Custom ArgumentParser to prevent sys.exit on error and allow XML generation.
    # --help and --version will still exit cleanly.
    class NonExitingArgumentParser(argparse.ArgumentParser):
        def error(self, message: str):
            # message already contains "prog_name: error: ..."
            # Create fatal XMLs for ConfigError.
            # config.joshua_seed might be default int at this stage, before extract_args.
            seed_str = str(config.joshua_seed) if hasattr(config, 'joshua_seed') and isinstance(config.joshua_seed, int) else "CONFIG_PARSE_SEED_UNAVAILABLE"
            
            err_type = "ArgParseError"
            xml_message = f"Argument parsing error: {message}"
            
            raw_xml = create_fatal_error_xml(message=xml_message, error_type=err_type, joshua_seed=seed_str)
            # Assume V1 stripping for stdout as this is before stream redirection decision.
            stdout_xml = strip_elements_for_v1_stdout(raw_xml).replace('\\n', ' ').strip()

            # Raise our specific ConfigError, which main() expects to have these attributes.
            raise ConfigError(
                message=xml_message, # Console message
                stdout_xml=stdout_xml, # What goes to V1 stdout
                xml_content_for_file=raw_xml # What goes to joshua.xml
            )
        
        def exit(self, status=0, message=None):
            # Handle --help, --version which also call exit.
            if status == 0: # Typically --help output, or other clean exits.
                 # argparse already printed the help/version message to stdout/stderr.
                 # We should let the process exit cleanly as intended by argparse.
                 sys.exit(status) 
            
            # For other non-zero status exits, treat as an error.
            error_message = message if message else f"ArgumentParser called exit with status {status}"
            seed_str = str(config.joshua_seed) if hasattr(config, 'joshua_seed') and isinstance(config.joshua_seed, int) else "CONFIG_EXIT_SEED_UNAVAILABLE"
            err_type = f"ArgParseExitErrorStatus{status}"
            xml_message = f"ArgumentParser exit: {error_message}"

            raw_xml = create_fatal_error_xml(message=xml_message, error_type=err_type, joshua_seed=seed_str)
            stdout_xml = strip_elements_for_v1_stdout(raw_xml).replace('\\n', ' ').strip()
            
            raise ConfigError(
                message=xml_message,
                stdout_xml=stdout_xml,
                xml_content_for_file=raw_xml
            )

    try:
        parser = NonExitingArgumentParser(
            prog="TestHarnessV2 app.py",
            description="Test Harness V2 Main Application",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter, # Good for help messages
            add_help=True # Ensure --help is handled by our exit method.
        )
        config.build_arguments(parser)
        
        # parse_args() will use sys.argv[1:] by default.
        # If NonExitingArgumentParser.error or .exit (with non-zero status) is called, it raises ConfigError.
        parsed_args = parser.parse_args(argv[1:])

        config.extract_args(parsed_args) # This populates the global 'config' object

        # Log successful parsing and key config values.
        logger.info("Configuration parsed and extracted successfully.")
        if hasattr(config, 'joshua_output_dir') and config.joshua_output_dir is not None:
            logger.info(f"Effective joshua_output_dir from config: {config.joshua_output_dir}")
        else:
            logger.warning("joshua_output_dir is not set after config parsing.")

        if hasattr(config, 'log_level') and config.log_level is not None:
            logger.info(f"Effective log_level from config: {config.log_level}")
        else:
            logger.warning("log_level is not set after config parsing.")

        # Placeholder for any additional explicit validation checks on 'config' attributes.
        # Example:
        # if not config.some_critical_value:
        #    msg = f"Critical configuration 'some_critical_value' is missing or invalid."
        #    raw_xml = create_fatal_error_xml(message=msg, error_type="ConfigValidation", joshua_seed=str(config.joshua_seed))
        #    stdout_xml = strip_elements_for_v1_stdout(raw_xml)
        #    raise ConfigError(msg, stdout_xml, raw_xml)

    except ConfigError: # Re-raise ConfigErrors from NonExitingArgumentParser or explicit checks.
        # logger.debug("parse_and_validate_config is re-raising a ConfigError.")
        raise
    except Exception as e_config_setup: # Catch any other unexpected errors.
        logger.error(f"Unexpected error during config parsing/validation: {type(e_config_setup).__name__} - {e_config_setup}", exc_info=True)
        
        joshua_seed_val = "UNKNOWN_EXC_SEED"
        if hasattr(config, 'joshua_seed'): # Should be an int after __init__
            joshua_seed_val = str(config.joshua_seed)
        
        error_message_for_xml = f"Config setup failed: {type(e_config_setup).__name__} - {str(e_config_setup)}. Traceback: {traceback.format_exc()}"
        
        raw_xml = create_fatal_error_xml(
            message=error_message_for_xml,
            error_type=f"ConfigSetupUnexpectedError_{type(e_config_setup).__name__}",
            joshua_seed=joshua_seed_val
        )
        stdout_xml = strip_elements_for_v1_stdout(raw_xml).replace('\\n', ' ').strip()
        
        raise ConfigError( # Wrap in ConfigError for consistent handling in main()
            message=f"Unexpected config setup error: {e_config_setup}",
            stdout_xml=stdout_xml,
            xml_content_for_file=raw_xml
        )

    return config, parsed_args

def setup_stream_redirection(config, logger):
    # The 'config' object is now passed in as a parameter.
    # The 'global config' declaration is no longer needed and causes a SyntaxError.

    # Default to passing stdout through if config is not fully formed yet
    pass_stdout_to_original = True

    try:
        log_file_path_from_handler = None
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                log_file_path_from_handler = handler.baseFilename
                break
        
        if log_file_path_from_handler is None:
            raise RuntimeError("Logger is not configured with a FileHandler; cannot set up stream redirection.")

        # The logger has already created the file, so we open it in append mode.
        log_file_stream_for_tee = open(log_file_path_from_handler, 'a')

        # This logic determines if the V1-style stdout lines should be printed
        # or if all stdout should be suppressed (when archival is on).
        if hasattr(config, '_v1_summary_output_stream') and config._v1_summary_output_stream != original_stdout:
            # This indicates V1 compatibility mode is NOT active for stdout.
            # We check if archival is on to decide whether to suppress stdout entirely.
            if hasattr(config, 'archive_logs_on_failure') and config.archive_logs_on_failure:
                pass_stdout_to_original = False

        logger.info(f"Stream redirection: config._v1_summary_output_stream IS original_stdout. General app stdout will pass through.")
        
        if hasattr(config, 'archive_logs_on_failure') and config.archive_logs_on_failure:
             logger.info(f"Stream redirection: archive_logs_on_failure is TRUE. General app stdout will be suppressed from original stdout.")
             pass_stdout_to_original = False


        # Tee stdout to the log file. Pass-through to the original stdout is conditional.
        sys.stdout = StreamTee(original_stdout, log_file_stream_for_tee, pass_to_original_target=pass_stdout_to_original)
        logger.info(f"sys.stdout redirected. Pass to original: {pass_stdout_to_original}")

        # Tee stderr to the log file. Always pass-through to original stderr.
        sys.stderr = StreamTee(original_stderr, log_file_stream_for_tee, pass_to_original_target=True)
        logger.info("sys.stderr redirected. Pass to original: True")

        logger.info("Stream redirection configured.")

    except Exception as e:
        logger.error(f"CRITICAL: Failed to setup stream redirection: {e}", exc_info=True)
        # We write directly to original_stderr because the logger's stream might be the problem.
        if original_stderr and not original_stderr.closed:
            original_stderr.write(f"app.py: CRITICAL FAILURE during stream redirection setup: {type(e).__name__}: {e}\\n{traceback.format_exc()}\\n\\n")
        raise # Re-raise to be caught by main's exception handler.

    return sys.stdout, sys.stderr

def perform_early_config_checks_and_exit_on_error(config):
    global logger
    # The 'config' object is now passed in as a parameter.
    # The 'global config' declaration is no longer needed and causes a SyntaxError.
    
    logger.info("Performing early configuration checks...")

    try:
        # Example check (can be expanded):
        # if not config.joshua_output_dir or not os.access(config.joshua_output_dir, os.W_OK):
        #     msg = f"joshua_output_dir '{config.joshua_output_dir}' is not set or not writable."
        #     logger.error(msg)
        #     raw_xml = create_fatal_error_xml(message=msg, error_type="EarlyConfigCheckFail_OutputDir", joshua_seed=str(config.joshua_seed))
        #     stdout_xml = strip_elements_for_v1_stdout(raw_xml).replace('\\n', ' ').strip()
        #     raise EarlyExitError(msg, stdout_xml, raw_xml)
        
        # Add other critical checks here as needed.

        logger.info("Early configuration checks passed (or no checks implemented yet).")

    except EarlyExitError: # Allow EarlyExitErrors to propagate directly.
        raise
    except Exception as e_check:
        error_message = f"Unexpected error during early config checks: {type(e_check).__name__} - {e_check}"
        logger.error(error_message, exc_info=True)
        
        joshua_seed_val = str(config.joshua_seed) if hasattr(config, 'joshua_seed') else "EARLY_CHECK_EXC_SEED"
        xml_err_msg = f"{error_message}. Traceback: {traceback.format_exc()}"

        raw_xml = create_fatal_error_xml(
            message=xml_err_msg,
            error_type=f"EarlyConfigCheckUnexpectedError_{type(e_check).__name__}",
            joshua_seed=joshua_seed_val
        )
        stdout_xml = strip_elements_for_v1_stdout(raw_xml).replace('\\n', ' ').strip()
        
        # Wrap in EarlyExitError for consistent handling in main()
        raise EarlyExitError(
            message=f"Unexpected early config check error: {e_check}",
            stdout_xml=stdout_xml,
            xml_content_for_file=raw_xml
        )

def run_tests_and_get_summary(config, args):
    global logger

    logger.info("Initializing TestRunner and starting test execution...")
    
    final_summary_tree = None
    overall_exit_code = 1 # Default to error

    try:
        runner = TestRunner(config) # Pass the global config object
        
        # Call run_all_tests() which returns the main SummaryTree.
        summary_tree_result = runner.run_all_tests() # Returns a SummaryTree
        
        # If run_all_tests completes without an exception, we consider this stage successful.
        # The actual pass/fail status of tests within the tree is handled by main's finally block.
        exit_code_from_runner = 0 

        if not isinstance(summary_tree_result, SummaryTree):
            logger.error(f"TestRunner.run_all_tests() returned an invalid type for summary tree: {type(summary_tree_result)}. Expected SummaryTree.")
            final_summary_tree = SummaryTree(config) # Initialize an empty one
            overall_exit_code = 1 # This variable is local to the function
        else:
            final_summary_tree = summary_tree_result
            logger.info("TestRunner.run_all_tests() completed. Summary tree received.")
            overall_exit_code = exit_code_from_runner # Use the 0 from above if no type error

        # This check for exit_code_from_runner type is less critical now as we set it to 0 by default
        # but keeping it for robustness in case of future changes.
        if not isinstance(exit_code_from_runner, int):
            logger.error(f"run_tests_and_get_summary determined an invalid type for exit code: {type(exit_code_from_runner)}. Expected int.")
            overall_exit_code = 1 # Ensure error exit code if something went wrong with our logic
        # else overall_exit_code already reflects exit_code_from_runner

    except Exception as e_runner:
        logger.error(f"CRITICAL: Unhandled exception from TestRunner instantiation or run_all_tests(): {type(e_runner).__name__} - {e_runner}", exc_info=True)
        overall_exit_code = 1 
        # Let main's global handler create the fatal XML. final_summary_tree will be None.
        raise

    logger.info(f"run_tests_and_get_summary finished. Returning summary tree and determined structural exit code {overall_exit_code}.")
    return final_summary_tree, overall_exit_code


def write_summary_to_joshua_xml(summary_tree: Optional[SummaryTree], current_config: 'Config'):
    """Serializes the final summary tree to joshua.xml."""
    # This function uses `current_config` as its parameter, so no `global` statement is needed.
    if summary_tree is None:
        logger.error("write_summary_to_joshua_xml called with None summary_tree. Cannot write file.")
        return

    if not (hasattr(current_config, 'joshua_output_dir') and current_config.joshua_output_dir):
        logger.error("write_summary_to_joshua_xml: joshua_output_dir not configured.")
        return

    try:
        joshua_xml_path = current_config.joshua_output_dir / "joshua.xml"
        current_config.joshua_output_dir.mkdir(parents=True, exist_ok=True)
        
        xml_content_for_file = ""
        # Prioritize to_et_element()
        if hasattr(summary_tree, 'to_et_element') and callable(summary_tree.to_et_element):
            xml_element = summary_tree.to_et_element()
            if xml_element is not None:
                xml_content_for_file = ET.tostring(xml_element, encoding='unicode', short_empty_elements=False).strip()
            else:
                logger.error("SummaryTree.to_et_element() returned None.")
                xml_content_for_file = create_fatal_error_xml("SummaryTree.to_et_element() was None", "SummaryTreeXmlError", str(current_config.joshua_seed))
        # Fallback: Check for a specific to_xml_string method
        elif hasattr(summary_tree, 'to_xml_string') and callable(summary_tree.to_xml_string):
            xml_content_for_file = summary_tree.to_xml_string()
        # Generic fallback to str(), with a warning
        else: 
            logger.warning("SummaryTree has no .to_xml_string(), using str(). This might not be correct XML.")
            xml_content_for_file = str(summary_tree)

        if not xml_content_for_file:
             logger.error("XML content from SummaryTree was empty. Writing a fallback error to joshua.xml.")
             xml_content_for_file = create_fatal_error_xml("SummaryTree produced empty XML", "EmptySummaryTreeXml", str(current_config.joshua_seed))

        with open(joshua_xml_path, "w", encoding='utf-8') as f:
            f.write(xml_content_for_file)
        logger.info(f"Final summary XML (joshua.xml) saved to {joshua_xml_path}")

    except Exception as e_write_summary:
        logger.error(f"Could not write final summary joshua.xml: {type(e_write_summary).__name__} - {e_write_summary}", exc_info=True)


if __name__ == "__main__":
    main()
