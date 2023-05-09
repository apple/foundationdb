#!/usr/bin/env python3
#
# fdb_c_version.py
#
#  This source file is part of the FoundationDB open source project
#
#  Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import argparse
import ctypes
import sys
import platform
import os


def error(message):
    print(message)
    sys.exit(1)


def get_version_string(library_path):
    try:
        lib = ctypes.cdll.LoadLibrary(library_path)
    except Exception as e:
        error("Could not load library %r: %s" % (library_path, str(e)))

    lib.fdb_get_error.restype = ctypes.c_char_p

    try:
        r = lib.fdb_select_api_version_impl(410, 410)
        if r != 0:
            error("Error setting API version: %s (%d)" % (lib.fdb_get_error(r), r))
    except Exception as e:
        error("Error calling fdb_select_api_version_impl: %s" % str(e))

    try:
        lib.fdb_get_client_version.restype = ctypes.c_char_p
        version_str = lib.fdb_get_client_version().decode("utf-8")
    except Exception as e:
        error("Error getting version information from client library: %s" % str(e))

    version_components = version_str.split(",")
    package_version = ".".join(version_components[0].split(".")[0:2])

    version_str = "FoundationDB Client %s (v%s)\n" % (
        package_version,
        version_components[0],
    )
    version_str += "source version %s\n" % version_components[1]
    version_str += "protocol %s" % version_components[2]

    return version_str


if __name__ == "__main__":
    if platform.system() == "Linux":
        default_lib = "libfdb_c.so"
        platform_name = "Linux"
        dlopen = "dlopen"
    elif platform.system() == "Windows":
        default_lib = "fdb_c.dll"
        platform_name = "Windows"
        dlopen = "LoadLibrary"
    elif platform.system() == "Darwin":
        default_lib = "libfdb_c.dylib"
        platform_name = "macOS"
        dlopen = "dlopen"
    else:
        error("Unsupported platform: %s" % platform.system())

    parser = argparse.ArgumentParser(
        description="Prints version information for an FDB client library (e.g. %s). Must be run on a library built for the current platform (%s)."
        % (default_lib, platform_name)
    )
    parser.add_argument(
        "library_path",
        type=str,
        help="Path to the client library. If not specified, the library will be searched for according to the procedures for %s on the current platform (%s)."
        % (dlopen, platform_name),
        default=None,
        nargs="?",
    )

    args = parser.parse_args()

    if args.library_path is None:
        args.library_path = default_lib
    elif not os.path.isfile(args.library_path):
        error("Library does not exist: %r" % args.library_path)

    print(get_version_string(args.library_path))
