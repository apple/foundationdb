#
# util.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import signal
import os
import glob

import fdb


def initialize_logger_level(logging_level):
    logger = get_logger()

    assert logging_level in ["DEBUG", "INFO", "WARNING", "ERROR"]

    if logging_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif logging_level == "INFO":
        logger.setLevel(logging.INFO)
    elif logging_level == "WARNING":
        logger.setLevel(logging.WARNING)
    elif logging_level == "ERROR":
        logger.setLevel(logging.ERROR)


def get_logger():
    return logging.getLogger("foundationdb.bindingtester")


# Attempts to get the name associated with a process termination signal
def signal_number_to_name(signal_num):
    name = []
    for key in signal.__dict__.keys():
        if key.startswith("SIG") and getattr(signal, key) == signal_num:
            name.append(key)
    if len(name) == 1:
        return name[0]
    else:
        return str(signal_num)


def import_subclasses(filename, module_path):
    for f in glob.glob(os.path.join(os.path.dirname(filename), "*.py")):
        fn = os.path.basename(f)
        if fn == "__init__.py":
            continue
        __import__("%s.%s" % (module_path, os.path.splitext(fn)[0]))


# Attempts to unpack a subspace
# This throws an exception if the subspace cannot be unpacked as a tuple
# As a result, the binding tester cannot use subspaces that have non-tuple raw prefixes
def subspace_to_tuple(subspace):
    try:
        return fdb.tuple.unpack(subspace.key())
    except Exception as e:
        get_logger().debug(e)
        raise Exception(
            "The binding tester does not support subspaces with non-tuple raw prefixes"
        )
