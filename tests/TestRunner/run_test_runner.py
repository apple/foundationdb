#!/usr/bin/env python3
"""
Wrapper script to run TestRunner as a direct script (not as a module).
This avoids the relative import issue when Python runs TestRunner.py directly.
"""
from fdb_test_runner.TestRunner import main

main()