#!/usr/bin/python
#
# python_correctness.py
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
import sys
import time
import random
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from python_tests import PythonTest

import fdb
import fdb.tuple

fdb.api_version(400)


# A class that mimics some of the operations of the FoundationDB key-value store
class KeyValueStore:

    # Uses a simple dictionary to store key-value pairs
    # Any operations that depend on the order of keys first sort the data
    store = dict()

    def get(self, key):
        if key is fdb.KeySelector:
            key = self.get_key(key)

        if key in self.store:
            return self.store[key]
        else:
            return None

    def get_key(self, key_selector):
        sorted_keys = list(sorted(self.store.keys()))
        for index, key in enumerate(sorted_keys):
            if key >= key_selector.key:
                index += key_selector.offset
                if (
                    key == key_selector.key and not key_selector.or_equal
                ) or key != key_selector.key:
                    index -= 1

                if index < 0 or index >= len(sorted_keys):
                    return ""
                else:
                    return sorted_keys[index]

        index = len(sorted_keys) + key_selector.offset - 1
        if index < 0 or index >= len(sorted_keys):
            return ""
        else:
            return sorted_keys[index]

    def get_range(self, begin, end, limit=None):
        values = []
        count = 0

        if begin is fdb.KeySelector:
            begin = self.get_key(begin)
        if end is fdb.KeySelector:
            end = self.get_key(end)

        for key, value in sorted(self.store.items()):
            if limit is not None and count >= limit:
                break

            if key >= end:
                break

            if key >= begin:
                values.append([key, value])
                count += 1

        return values

    def get_range_startswith(self, prefix, limit=None):
        values = []
        count = 0
        for key, value in sorted(self.store.items()):
            if limit is not None and count >= limit:
                break

            if key > prefix and not key.startswith(prefix):
                break

            if key.startswith(prefix):
                values.append([key, value])
                count += 1

        return values

    def set(self, key, value):
        self.store[key] = value

    def clear(self, key):
        if key is fdb.KeySelector:
            key = self.get_key(key)

        if key in self.store:
            del self.store[key]

    def clear_range(self, begin, end):
        for key, value in self.get_range(begin, end):
            del self.store[key]

    def clear_range_startswith(self, prefix):
        for key, value in self.get_range_startswith(prefix):
            del self.store[key]


class PythonCorrectness(PythonTest):
    callback = False
    callbackError = ""

    # Python correctness tests (checks if functions run and yield correct results)
    def run_test(self):
        try:
            db = fdb.open(None, "DB")
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("fdb.open failed"))
            return

        try:
            print("Testing functions...")
            self.test_functions(db)

            print("Testing correctness...")
            del db[:]
            self.test_correctness(db)
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Failed to complete all tests"))

    # Generates a random set of keys and values
    def generate_data(
        self,
        num_keys,
        min_key_length,
        max_key_length,
        min_value_length,
        max_value_length,
        prefix="",
        allow_duplicates=True,
    ):
        data = list()
        keys = set()
        while len(data) < num_keys:
            # key = prefix + ''.join(random.choice(string.ascii_lowercase)
            #                        for i in range(0, random.randint(minKeyLength - len(prefix), maxKeyLength - len(prefix))))
            key = prefix + "".join(
                chr(random.randint(0, 254))
                for _ in range(
                    0,
                    random.randint(
                        min_key_length - len(prefix), max_key_length - len(prefix)
                    ),
                )
            )
            if not allow_duplicates:
                if key in keys:
                    continue
                else:
                    keys.add(key)

            value = "".join(
                "x" for _ in range(0, random.randint(max_key_length, max_value_length))
            )
            data.append([key, value])

        return data

    # Function to test the callback feature of Future objects
    def test_callback(self, future):
        try:
            future.wait()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.callbackError = self.get_error("Callback future get failed")

        self.callback = True

    # Tests that all of the functions in the python API can be called without failing
    def test_functions(self, db):
        self.callback = False
        self.callbackError = ""

        try:
            tr = db.create_transaction()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("db.create_transaction failed"))
            return

        try:
            tr["testkey"] = "testvalue"
            value = tr["testkey"]
            value.is_ready()
            value.block_until_ready()
            value.wait()

        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(
                self.get_error("Set/Get value failed (block until ready)")
            )

        try:
            value = tr["testkey"]
            value.wait()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Get value failed"))

        try:
            tr["testkey"] = "newtestvalue"
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Replace value failed"))

        try:
            value = tr["fakekey"]
            # The following line would generate a segfault
            # value.capi.fdb_future_block_until_ready(0)
            value.wait()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Get non-existent key failed"))

        try:
            tr.commit().wait()
            tr.get_committed_version()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Commit failed"))

        try:
            tr.reset()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Reset failed"))

        try:
            version = tr.get_read_version()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("tr.get_read_version failed"))

        try:
            value = tr["testkey"]
            value.wait()
            tr.reset()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Get and reset failed"))

        try:
            tr.set_read_version(version.wait())
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Set read version failed"))

        try:
            value = tr["testkey"]
            callback_time = time.time()
            value.on_ready(self.test_callback)
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Get future and set callback failed"))

        try:
            del tr["testkey"]
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Delete key failed"))

        try:
            del tr["fakekey"]
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Delete non-existent key failed"))

        try:
            tr.set("testkey", "testvalue")
            value = tr.get("testkey")
            value.wait()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Future.get failed"))

        try:
            tr.clear("testkey")
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Clear key failed"))

        try:
            tr["testkey1"] = "testvalue1"
            tr["testkey2"] = "testvalue2"
            tr["testkey3"] = "testvalue3"

            for k, v in tr.get_range("testkey1", "testkey3"):
                v += ""

            for k, v in tr.get_range("testkey1", "testkey2", 2):
                v += ""

            for k, v in tr["testkey1":"testkey3"]:
                v += ""
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Get range failed"))

        try:
            tr["otherkey1"] = "othervalue1"
            tr["otherkey2"] = "othervalue2"

            for k, v in tr.get_range_startswith("testkey"):
                v += ""
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Get range starts with failed"))

        try:
            tr.clear_range_startswith("otherkey")
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Clear range starts with failed"))

        try:
            tr.clear_range("testkey1", "testkey3")
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Clear range failed"))

        try:
            tr["testkey1"] = "testvalue1"
            tr["testkey2"] = "testvalue2"
            tr["testkey3"] = "testvalue3"

            begin = fdb.KeySelector("testkey2", 0, 0)
            end = fdb.KeySelector("testkey2", 0, 1)
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Create key selector failed"))

        try:
            for k, v in tr.get_range(begin, end):
                v += ""

            for k, v in tr.get_range(begin, end, 2):
                v += ""

            for k, v in tr[begin:end]:
                v += ""
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Get range (key selectors) failed"))

        try:
            tr.clear_range(begin, end)

            tr["testkey1"] = "testvalue1"
            tr["testkey2"] = "testvalue2"
            tr["testkey3"] = "testvalue3"

            del tr[begin:end]

            tr["testkey1"] = "testvalue1"
            tr["testkey2"] = "testvalue2"
            tr["testkey3"] = "testvalue3"
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Clear range (key selectors) failed"))

        try:
            begin = fdb.KeySelector.last_less_than("testkey2")
            end = fdb.KeySelector.first_greater_or_equal("testkey2")

            for k, v in tr.get_range(begin, end):
                v += ""

            begin = fdb.KeySelector.last_less_or_equal("testkey2")
            end = fdb.KeySelector.first_greater_than("testkey2")

            for k, v in tr.get_range(begin, end):
                v += ""
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Builtin key selectors failed"))

        try:
            del tr["testkey1":"testkey3"]
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Delete key range failed"))

        try:
            tr.commit().wait()
            tr.get_committed_version()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Commit failed"))

        try:
            key = fdb.tuple.pack(("k1", "k2", "k3"))
            k_tuple = fdb.tuple.unpack(key)
            if k_tuple[0] != "k1" and k_tuple[1] != "k2" and k_tuple[2] != "k3":
                self.result.add_error(
                    "Tuple <-> key conversion yielded incorrect results"
                )
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Tuple <-> key conversion failed"))

        try:
            tr[fdb.tuple.pack(("k1", "k2"))] = "v"
            tr[fdb.tuple.pack(("k1", "k2", "k3"))] = "v1"
            tr[fdb.tuple.pack(("k1", "k2", "k3", "k4"))] = "v2"

            for k, v in tr[fdb.tuple.range(("k1", "k2"))]:
                v += ""

        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Tuple get range failed"))

        try:
            tr["testint"] = "10"
            y = int(tr["testint"]) + 1
            if y != 11:
                self.result.add_error("Value retrieval yielded incorrect results")
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Future value retrieval failed"))

        if not self.callback:
            time.sleep(5)
            if not self.callback:
                self.result.add_error(
                    "Warning: Future callback not called after %f seconds"
                    % (time.time() - callback_time)
                )
        if len(self.callbackError) > 0:
            self.result.add_error(self.callbackError)

    # Compares a FoundationDB database with an in-memory key-value store
    def compare_database_to_memory(self, db, store):
        db_result = self.correctness_get_range_transactional(db, "\x00", "\xff")
        store_result = store.get_range("\x00", "\xff")

        return self.compare_results(db_result, store_result)

    # Compares result sets coming from a FoundationDB database and an in-memory key-value store
    def compare_results(self, db_results, store_results):
        if len(db_results) != len(store_results):
            # print 'mismatched lengths: ' + str(len(dbResults)) + ' - ' + str(len(storeResults))
            return False

        for i in range(0, len(db_results)):
            # if i >= len(storeResults):
            #    print 'mismatched key: ' + dbResults[i].key
            #    return False
            if (
                db_results[i].key != store_results[i][0]
                or db_results[i].value != store_results[i][1]
            ):
                # print 'mismatched key: ' + dbResults[i].key + ' - ' + storeResults[i][0]
                return False

        return True

    # Performs the same operations on a FoundationDB database and an in-memory key-value store and compares the results
    def test_correctness(self, db):
        num_keys = 5000
        ratio_short_keys = 0.5
        min_short_key_length = 1
        max_short_key_length = 3
        min_long_key_length = 1
        max_long_key_length = 128
        min_value_length = 1
        max_value_length = 10000
        max_transaction_bytes = 5000000

        num_reads = 100
        num_range_reads = 100
        num_prefix_reads = 100
        num_get_keys = 100

        # num_clears = 100
        num_range_clears = 10
        num_prefix_clears = 10

        max_keys_per_transaction = max(
            1, int(max_transaction_bytes / (max_value_length + max_long_key_length))
        )

        try:
            store = KeyValueStore()

            # Generate some random data
            data = self.generate_data(
                num_keys * ratio_short_keys,
                min_short_key_length,
                max_short_key_length,
                min_value_length,
                max_value_length,
            )
            data.extend(
                self.generate_data(
                    num_keys * (1 - ratio_short_keys),
                    min_long_key_length,
                    max_long_key_length,
                    min_value_length,
                    max_value_length,
                )
            )

            # Insert the data
            self.correctness_set(db, store, data, max_keys_per_transaction)
            if not self.compare_database_to_memory(db, store):
                self.result.add_error("transaction.set resulted in incorrect database")

            # Compare the results of single key reads
            if not self.correctness_get(
                db, store, data, num_reads, max_keys_per_transaction
            ):
                self.result.add_error("transaction.get returned incorrect result")

            # Compare the results of range reads
            for i in range(0, num_range_reads):
                if not self.correctness_get_range(db, store, data):
                    self.result.add_error(
                        "transaction.get_range returned incorrect results"
                    )
                    break

            # Compare the results of prefix reads
            for i in range(0, num_prefix_reads):
                if not self.correctness_get_prefix(db, store, data):
                    self.result.add_error(
                        "transaction.get_range_startswith returned incorrect results"
                    )
                    break

            # Compare the results of get key
            if not self.correctness_get_key(
                db, store, data, num_get_keys, max_keys_per_transaction
            ):
                self.result.add_error("transaction.get_key returned incorrect results")

            # Compare the results of clear
            # clearedKeys = self.correctnessClear(db, store, data, num_clears, max_keys_per_transaction)
            if not self.compare_database_to_memory(db, store):
                self.result.add_error(
                    "transaction.clear resulted in incorrect database"
                )

            #    for key in clearedKeys:
            #         print 'clearing key ' + key
            # else:
            #     print 'successful compare'

            # Fill the database back up with data
            self.correctness_set(db, store, data, max_keys_per_transaction)
            if not self.compare_database_to_memory(db, store):
                self.result.add_error("transaction.set resulted in incorrect database")

            # Compare the results of clear_range
            for i in range(0, num_range_clears):
                self.correctness_clear_range(db, store, data)

                success = self.compare_database_to_memory(db, store)
                if not success:
                    self.result.add_error(
                        "transaction.clear_range resulted in incorrect database"
                    )

                # Fill the database back up with data
                self.correctness_set(db, store, data, max_keys_per_transaction)
                if not self.compare_database_to_memory(db, store):
                    self.result.add_error(
                        "transaction.set resulted in incorrect database"
                    )
                    break

                if not success:
                    break

            # Compare the results of clear_range_startswith
            self.correctness_clear_prefix(db, store, data, num_prefix_clears)
            if not self.compare_database_to_memory(db, store):
                self.result.add_error(
                    "transaction.clear_range_startswith resulted in incorrect database"
                )

        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Database error in correctness test"))

    # Stores data in the database and a memory key-value store
    def correctness_set(self, db, store, data, max_keys_per_transaction):
        for [key, value] in data:
            store.set(key, value)

        keys_committed = 0
        while keys_committed < len(data):
            self.correctness_set_transactional(
                db, data[keys_committed : keys_committed + max_keys_per_transaction]
            )
            keys_committed += max_keys_per_transaction

    # Stores data in the database
    @fdb.transactional
    def correctness_set_transactional(self, tr, data):
        for [key, value] in data:
            tr.set(key, value)

    # Compares the results of the get operation from the database and a memory key-value store
    def correctness_get(self, db, store, data, num_reads, max_keys_per_transaction):
        keys = []
        for i in range(0, num_reads):
            index = random.randint(0, len(data) - 1)
            keys.append(data[index][0])

        keys_retrieved = 0
        while keys_retrieved < len(keys):
            sub_keys = keys[keys_retrieved : keys_retrieved + max_keys_per_transaction]

            values = self.correctness_get_transactional(db, sub_keys)
            for i in range(0, num_reads):
                if values[i] != store.get(sub_keys[i]):
                    print("mismatch: %s", sub_keys[i])
                    return False
            keys_retrieved += max_keys_per_transaction

        return True

    # Gets the values for the specified list of keys from the database
    @fdb.transactional
    def correctness_get_transactional(self, tr, keys):
        futures = []
        for key in keys:
            futures.append(tr.get(key))

        values = []
        for future in futures:
            values.append(future.wait())

        return values

    # Compares the results of the get_range operation from the database and a memory key-value store
    def correctness_get_range(self, db, store, data):
        index = random.randint(0, len(data) - 1)
        index2 = random.randint(0, len(data) - 1)

        key1 = min(data[index][0], data[index2][0])
        key2 = max(data[index][0], data[index2][0])

        db_results = self.correctness_get_range_transactional(db, key1, key2, data)
        store_results = store.get_range(key1, key2)

        return self.compare_results(db_results, store_results)

    # Gets the entries in the range [key1,key2) from the database
    @fdb.transactional
    def correctness_get_range_transactional(self, tr, key1, key2, data=None):
        if data is not None:
            return list(tr.get_range(key1, key2, len(data)))
        else:
            return list(tr.get_range(key1, key2))

    # Compares the results of the get_range_startswith operation from the database and a memory key-value store
    def correctness_get_prefix(self, db, store, data):
        prefix = "".join(
            chr(random.randint(0, 254)) for _ in range(0, random.randint(1, 3))
        )
        db_results = self.correctness_get_prefix_transactional(db, prefix)
        store_results = store.get_range_startswith(prefix)

        return self.compare_results(db_results, store_results)

    # Gets the entries with a given prefix from the database
    @fdb.transactional
    def correctness_get_prefix_transactional(self, tr, prefix):
        return list(tr.get_range_startswith(prefix))

    # Compares the results of the get_key operation from the database and a memory key-value store
    def correctness_get_key(
        self, db, store, data, num_get_keys, max_keys_per_transaction
    ):
        selectors = []
        for i in range(0, num_get_keys):
            index = random.randint(0, len(data) - 1)
            or_equal = random.randint(0, 1)
            offset = random.randint(
                max(-index + (1 - or_equal), -10),
                min(len(data) - index - 1 + or_equal, 10),
            )

            key = sorted(data)[index][0]
            selector = fdb.KeySelector(key, or_equal, offset)
            selectors.append(selector)

        keys_retrieved = 0
        while keys_retrieved < len(selectors):
            sub_selectors = selectors[
                keys_retrieved : keys_retrieved + max_keys_per_transaction
            ]
            db_keys = self.correctness_get_key_transactional(db, sub_selectors)
            for i in range(0, num_get_keys):
                if db_keys[i] != store.get_key(sub_selectors[i]):
                    return False
            keys_retrieved += max_keys_per_transaction

        return True

    # Gets the keys specified by the list of key selectors
    @fdb.transactional
    def correctness_get_key_transactional(self, tr, key_selectors):
        futures = []
        count = 0
        for selector in key_selectors:
            futures.append(tr.get_key(selector))
            count += 1

        keys = []
        count = 0
        for future in futures:
            keys.append(future.wait())
            count += 1

        return keys

    # Clears data from a database and a memory key-value store
    def correctness_clear(self, db, store, data, num_clears, max_keys_per_transaction):
        cleared_keys = []
        for i in range(0, num_clears):
            index = random.randint(0, len(data) - 1)
            cleared_keys.append(data[index][0])
            store.clear(data[index][0])

        keys_cleared = 0
        while keys_cleared < len(cleared_keys):
            self.correctness_clear_transactional(
                db, cleared_keys[keys_cleared : keys_cleared + max_keys_per_transaction]
            )
            keys_cleared += max_keys_per_transaction

        return cleared_keys

    # Clears a list of keys from the database
    @fdb.transactional
    def correctness_clear_transactional(self, tr, cleared_keys):
        for key in cleared_keys:
            tr.clear(key)

    # Clears a range of data from a database and a memory key-value store
    def correctness_clear_range(self, db, store, data):
        index = random.randint(0, len(data) - 1)
        index2 = random.randint(0, len(data) - 1)

        key1 = min(data[index][0], data[index2][0])
        key2 = max(data[index][0], data[index2][0])

        self.correctness_clear_range_transactional(db, key1, key2)
        store.clear_range(key1, key2)

    # Clears a range of memory from a database
    @fdb.transactional
    def correctness_clear_range_transactional(self, tr, key1, key2):
        tr.clear_range(key1, key2)

    # Clears data with random prefixes from a database and a memory key-value store
    def correctness_clear_prefix(self, db, store, data, num_prefix_clears):
        prefixes = []
        for i in range(0, num_prefix_clears):
            prefix = "".join(
                chr(random.randint(0, 254)) for _ in range(0, random.randint(1, 3))
            )
            prefixes.append(prefix)
            store.clear_range_startswith(prefix)

        self.correctness_clear_prefix_transactional(db, prefixes)

    # Clears keys from a database that have a prefix in the prefixes list
    @fdb.transactional
    def correctness_clear_prefix_transactional(self, tr, prefixes):
        for prefix in prefixes:
            tr.clear_range_startswith(prefix)

    # Adds the stack trace to an error message
    def get_error(self, message):
        error_message = message + "\n" + traceback.format_exc()
        print("%s", error_message)
        return error_message


if __name__ == "__main__":
    print(
        "Running PythonCorrectness test on Python version %d.%d.%d%s%d"
        % (
            sys.version_info[0],
            sys.version_info[1],
            sys.version_info[2],
            sys.version_info[3][0],
            sys.version_info[4],
        )
    )

    PythonCorrectness().run()
