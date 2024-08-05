# server.py
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

import os
from flask import Flask
import fdb

app = Flask(__name__)

fdb.api_version(int(os.getenv("FDB_API_VERSION")))
db = fdb.open()

COUNTER_KEY = fdb.tuple.pack(("counter",))


def _increment_counter(tr):
    counter_value = tr[COUNTER_KEY]
    if counter_value == None:
        counter = 1
    else:
        counter = fdb.tuple.unpack(counter_value)[0] + 1
    tr[COUNTER_KEY] = fdb.tuple.pack((counter,))
    return counter


@app.route("/counter", methods=["GET"])
def get_counter():
    counter_value = db[COUNTER_KEY]
    if counter_value == None:
        return "0"

    return str(fdb.tuple.unpack(counter_value)[0])


@app.route("/counter/increment", methods=["POST"])
def increment_counter():
    return str(_increment_counter(db))
