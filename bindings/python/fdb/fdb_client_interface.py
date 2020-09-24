#
# fdb_client_interface.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

# FoundationDB Python Client Interface

class FdbClientInterface:
    def select_api_version(self, version, header_version):
        raise NotImplementedError()

    def get_max_api_version(self):
        raise NotImplementedError()

    def get_error(self, error_code):
        raise NotImplementedError()

    def error_predicate(self, predicate, error_code):
        raise NotImplementedError()

    def setup_network(self):
        raise NotImplementedError()

    def network_set_option(self, option, param, length):
        raise NotImplementedError()

    def run_network(self):
        raise NotImplementedError()

    def stop_network(self):
        raise NotImplementedError()
        
    def create_database(self, connection_string):
        raise NotImplementedError()

class FutureInterface:
    def destroy(self):
        raise NotImplementedError()

    def release_memory(self):
        raise NotImplementedError()

    def cancel(self):
        raise NotImplementedError()

    def block_until_ready(self):
        raise NotImplementedError()

    def is_ready(self):
        raise NotImplementedError()

    def set_callback(self, callback):
        raise NotImplementedError()

    def get_error(self):
        raise NotImplementedError()

    def get_int64(self):
        raise NotImplementedError()

    def get_key(self):
        raise NotImplementedError()

    def get_value(self):
        raise NotImplementedError()

    def get_keyvalue_array(self):
        raise NotImplementedError()

    def get_string_array(self):
        raise NotImplementedError()

class DatabaseInterface:
    def destroy(self):
        raise NotImplementedError()

    def create_transaction(self):
        raise NotImplementedError()

    def set_option(self, option, param, length):
        raise NotImplementedError()

class TransactionInterface:
    def destroy(self):
        raise NotImplementedError()

    def cancel(self):
        raise NotImplementedError()

    def set_read_version(self, version):
        raise NotImplementedError()

    def get_read_version(self):
        raise NotImplementedError()

    def get(self, key, snapshot):
        raise NotImplementedError()

    def get_key(self, key_selector, snapshot):
        raise NotImplementedError()

    def get_range(self, begin, end, limit, streaming_mode, iteration, snapshot, reverse):
        raise NotImplementedError()

    def get_estimated_range_size_bytes(self, begin, end):
        raise NotImplementedError()

    def add_conflict_range(self, begin, end, type):
        raise NotImplementedError()

    def get_addresses_for_key(self, key):
        raise NotImplementedError()

    def set_option(self, option, param, length):
        raise NotImplementedError()

    def atomic_op(self, key, param, opcode):
        raise NotImplementedError()

    def set(self, key, value):
        raise NotImplementedError()

    def clear(self, key):
        raise NotImplementedError()

    def clear_range(self, begin, end):
        raise NotImplementedError()

    def watch(self, key):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def get_approximate_size(self):
        raise NotImplementedError()

    def get_versionstamp(self):
        raise NotImplementedError()

    def on_error(self, error_code):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()
