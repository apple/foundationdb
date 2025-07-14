import argparse
import logging
import os
import sys
import traceback
from pathlib import Path
from datetime import datetime

from test_harness.config import config
from test_harness.run import TestRunner
from test_harness.summarize import SummaryTree

def setup_logging():
    """Setup logging with file and optional console output."""
    # Determine log file location - put in main test run directory
    if config.run_temp_dir:
        # Put app_log.txt directly in run_temp_dir (which is now the main test run directory)
        log_file = config.run_temp_dir / "app_log.txt"
        config.run_temp_dir.mkdir(parents=True, exist_ok=True)
    else:
        log_file = Path("/tmp") / f"th_app_{os.getpid()}_{int(datetime.now().timestamp())}.log"
    
    # Setup logging level
    log_level = logging.INFO
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            # Only log to console if stderr is not being redirected (i.e., if it's a TTY)
            logging.StreamHandler(sys.stderr) if sys.stderr.isatty() else logging.NullHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured. File: {log_file}, Level: {logging.getLevelName(log_level)}")
    return logger

def create_error_xml(message: str, error_type: str = "FatalError", joshua_seed: str = "UNKNOWN") -> str:
    """Create a simple error XML for Joshua."""
    error = SummaryTree("Test")
    error.attributes["TestUID"] = "UNKNOWN_UID"
    error.attributes["JoshuaSeed"] = str(joshua_seed)
    error.attributes["Ok"] = "0"
    error.attributes["Error"] = error_type
    
    jm_element = SummaryTree("JoshuaMessage")
    jm_element.attributes["Severity"] = "40"
    jm_element.attributes["Message"] = message
    error.append(jm_element)
    
    return error

if __name__ == "__main__":
    logger = None
    try:
        parser = argparse.ArgumentParser(
            "TestHarness", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        config.build_arguments(parser)
        args = parser.parse_args()
        config.extract_args(args)
        
        # Setup logging
        logger = setup_logging()
        logger.info("TestHarness2 starting")
        logger.info(f"Joshua seed: {config.joshua_seed}")
        logger.info(f"Run temp dir: {config.run_temp_dir}")
        
        test_runner = TestRunner()
        success = test_runner.run()
        
        logger.info(f"TestHarness2 completed. Success: {success}")
        
        if not success:
            exit(1)
            
    except Exception as e:
        if logger:
            logger.exception("TestHarness2 failed with exception")
        
        _, _, exc_traceback = sys.exc_info()
        error = create_error_xml(str(e), "FatalError", str(getattr(config, 'joshua_seed', 'UNKNOWN')))
        error.dump(sys.stdout)
        exit(1)
