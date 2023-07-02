#
# __init__.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

import os
import json
import random
import traceback
import argparse


class Result(object):
    def __init__(self):
        self.id = random.randint(0, 2**63)
        self.kpis = {}
        self.errors = []

    def add_kpi(self, name, value, units):
        self.kpis[name] = {"value": value, "units": units}

    def add_error(self, error):
        self.errors.append(error)

    def save(self, dir):
        file = "pyresult-%d.json" % self.id
        if dir:
            file = os.path.join(dir, file)

        with open(file, "w") as f:
            json.dump({"kpis": self.kpis, "errors": self.errors}, f)


class PythonTest(object):
    def __init__(self):
        self.result = Result()
        self.args = None

    def run_test(self):
        pass

    def multi_version_description(self):
        if self.args.disable_multiversion_api:
            return "multi-version API disabled"
        elif self.args.use_external_client:
            if self.args.enable_callbacks_on_external_threads:
                return "external client on external thread"
            else:
                return "external client on main thread"
        else:
            return "local client"

    def run(self, parser=None):
        import fdb

        # API version should already be set by the caller

        if parser is None:
            parser = argparse.ArgumentParser()

        parser.add_argument(
            "--output-directory",
            default="",
            type=str,
            help="The directory to store the output JSON in. If not set, the current directory is used",
        )
        parser.add_argument(
            "--disable-multiversion-api",
            action="store_true",
            help="Disables the multi-version client API",
        )
        parser.add_argument(
            "--enable-callbacks-on-external-threads",
            action="store_true",
            help="Allows callbacks to be called on threads created by the client library",
        )
        parser.add_argument(
            "--use-external-client",
            action="store_true",
            help="Connect to the server using an external client",
        )

        self.args = parser.parse_args()

        if self.args.disable_multiversion_api:
            if (
                self.args.enable_callbacks_on_external_threads
                or self.args.use_external_client
            ):
                raise Exception("Invalid multi-version API argument combination")
            fdb.options.set_disable_multi_version_client_api()
        if self.args.enable_callbacks_on_external_threads:
            if not self.args.use_external_client:
                raise Exception(
                    "Cannot enable callbacks on external threads without using external clients"
                )
            fdb.options.set_callbacks_on_external_threads()
        if self.args.use_external_client:
            fdb.options.set_disable_local_client()
            fdb.options.set_external_client_directory(
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "external_libraries"
                )
            )

        try:
            self.run_test()
        except Exception:
            self.result.add_error(traceback.format_exc())

        self.result.save(self.args.output_directory)
