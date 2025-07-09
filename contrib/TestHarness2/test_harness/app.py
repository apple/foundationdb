from __future__ import annotations

import argparse
import logging
import sys
import traceback
import os
import xml.etree.ElementTree as ET
from xml.sax import saxutils
from pathlib import Path
from typing import Optional
from datetime import datetime

from test_harness.config import config, Config, ConfigError, EarlyExitError
from test_harness.run import TestRunner
from test_harness.summarize import SummaryTree

def setup_logging(config_obj: Config) -> logging.Logger:
    """Setup logging with file and optional console output."""
    # Determine log file location
    if config_obj.joshua_output_dir:
        log_file = config_obj.joshua_output_dir / "app_log.txt"
        config_obj.joshua_output_dir.mkdir(parents=True, exist_ok=True)
    else:
        log_file = Path("/tmp") / f"th_app_{os.getpid()}_{int(datetime.now().timestamp())}.log"
    
    # Setup logging level
    log_level = getattr(logging, config_obj.log_level.upper(), logging.INFO)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            # Only log to console if not in archival mode
            logging.StreamHandler(sys.stderr) if not config_obj.archive_logs_on_failure else logging.NullHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured. File: {log_file}, Level: {logging.getLevelName(log_level)}")
    return logger

def create_error_xml(message: str, error_type: str = "FatalError", joshua_seed: str = "UNKNOWN") -> str:
    """Create a simple error XML for Joshua."""
    try:
        test_element = ET.Element("Test")
        test_element.set("TestUID", "UNKNOWN_UID")
        test_element.set("JoshuaSeed", str(joshua_seed))
        test_element.set("Ok", "0")
        test_element.set("Error", saxutils.escape(error_type))
        
        jm_element = ET.SubElement(test_element, "JoshuaMessage")
        jm_element.set("Severity", "40")
        jm_element.set("Message", saxutils.escape(message))
        
        return ET.tostring(test_element, encoding='unicode').strip()
    except Exception:
        # Fallback if XML creation fails
        return f'<Test TestUID="UNKNOWN_UID" JoshuaSeed="{saxutils.escape(str(joshua_seed))}" Ok="0" Error="XmlCreationFailed"><JoshuaMessage Severity="40" Message="{saxutils.escape(message)}" /></Test>'

def main():
    """Main entry point of TestHarness2."""
    config_obj: Optional[Config] = None
    summary_tree: Optional[SummaryTree] = None
    logger: Optional[logging.Logger] = None
    
    try:
        # Parse configuration
        parser = argparse.ArgumentParser(
            "TestHarness2",
            description="FoundationDB TestHarness2 Application",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        config.build_arguments(parser)
        args = parser.parse_args()
        config.extract_args(args)
        config_obj = config
        
        # Setup logging
        logger = setup_logging(config_obj)
        logger.info("TestHarness2 starting execution")
        logger.info(f"Joshua seed: {config_obj.joshua_seed}")
        logger.info(f"Output directory: {config_obj.joshua_output_dir}")
        logger.info(f"Archive logs on failure: {config_obj.archive_logs_on_failure}")
        
        # Run tests
        logger.info("Starting test execution")
        test_runner = TestRunner(config_obj)
        summary_tree = test_runner.run_all_tests()
        
        if summary_tree is None:
            logger.error("TestRunner returned None summary")
            summary_tree = SummaryTree("TestHarnessRun")
            summary_tree.attributes["Ok"] = "0"
            summary_tree.attributes["FailReason"] = "TestRunnerReturnedNone"
        
        logger.info("Test execution completed")
        
    except (ConfigError, EarlyExitError) as e:
        # These exceptions have pre-formatted XML for Joshua
        if hasattr(e, 'stdout_xml'):
            print(e.stdout_xml)
        else:
            error_xml = create_error_xml(str(e), "ConfigError", 
                                       str(config_obj.joshua_seed) if config_obj else "UNKNOWN")
            print(error_xml)
        
        # Create summary tree for file output
        if hasattr(e, 'xml_content_for_file'):
            try:
                summary_tree = SummaryTree.from_xml(e.xml_content_for_file)
            except Exception:
                summary_tree = SummaryTree("TestHarnessRun")
                summary_tree.attributes["Ok"] = "0"
                summary_tree.attributes["FailReason"] = "ConfigError"
        
        return 1
        
    except Exception as e:
        # Unexpected error
        error_msg = f"Unhandled exception: {e}"
        joshua_seed = str(config_obj.joshua_seed) if config_obj else "UNKNOWN"
        
        if logger:
            logger.error(error_msg, exc_info=True)
        else:
            print(f"FATAL: {error_msg}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        
        # Output error XML to stdout for Joshua
        error_xml = create_error_xml(error_msg, "UnhandledException", joshua_seed)
        print(error_xml)
        
        # Create minimal summary tree
        summary_tree = SummaryTree("TestHarnessRun")
        summary_tree.attributes["Ok"] = "0"
        summary_tree.attributes["FailReason"] = "UnhandledException"
        summary_tree.attributes["Exception"] = str(e)
        
        return 1
    
    finally:
        # Write final summary to joshua.xml
        if config_obj and summary_tree and config_obj.joshua_output_dir:
            try:
                joshua_xml_path = config_obj.joshua_output_dir / "joshua.xml"
                config_obj.joshua_output_dir.mkdir(parents=True, exist_ok=True)
                
                # Get XML content from summary tree
                if hasattr(summary_tree, 'to_et_element') and callable(summary_tree.to_et_element):
                    xml_element = summary_tree.to_et_element()
                    xml_content = ET.tostring(xml_element, encoding='unicode').strip()
                elif hasattr(summary_tree, 'to_xml_string') and callable(summary_tree.to_xml_string):
                    xml_content = summary_tree.to_xml_string()
                else:
                    xml_content = str(summary_tree)
                
                with open(joshua_xml_path, "w", encoding='utf-8') as f:
                    f.write(xml_content)
                
                if logger:
                    logger.info(f"Summary written to {joshua_xml_path}")
                    
            except Exception as e:
                if logger:
                    logger.error(f"Failed to write joshua.xml: {e}")
                else:
                    print(f"ERROR: Failed to write joshua.xml: {e}", file=sys.stderr)
        
        # Determine exit code
        exit_code = 0
        if summary_tree:
            if summary_tree.attributes.get("BatchSuccess") == "0":
                exit_code = 1
            elif summary_tree.attributes.get("Ok") == "0":
                exit_code = 1
        
        if logger:
            logger.info(f"TestHarness2 execution finished. Exit code: {exit_code}")
            logging.shutdown()
        
        sys.exit(exit_code)

if __name__ == "__main__":
    main()
