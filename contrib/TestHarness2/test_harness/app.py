import argparse
import sys
import traceback

from test_harness.config import config
from test_harness.run import TestRunner
from test_harness.summarize import SummaryTree

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            "TestHarness", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        config.build_arguments(parser)
        args = parser.parse_args()
        config.extract_args(args)
        test_runner = TestRunner()
        if not test_runner.run():
            exit(1)
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        error = SummaryTree("TestHarnessError")
        error.attributes["Severity"] = "40"
        error.attributes["ErrorMessage"] = str(e)
        error.attributes["Trace"] = repr(traceback.format_tb(exc_traceback))
        error.dump(sys.stdout)
        exit(1)
