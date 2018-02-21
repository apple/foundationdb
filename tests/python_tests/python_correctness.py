#!/usr/bin/python
#
# python_correctness.py
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
import sys
import time
import string
import random
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from python_tests import PythonTest

import fdb
import fdb.tuple
fdb.api_version(400)


# A class that mimics some of the operations of the FoundationDB key-value store
class KeyValueStore():

    # Uses a simple dictionary to store key-value pairs
    # Any operations that depend on the order of keys first sort the data
    store = dict()

    def get(self, key):
        if key is fdb.KeySelector:
            key = get_key(key)

        if key in self.store:
            return self.store[key]
        else:
            return None

    def get_key(self, keySelector):
        sortedKeys = list(sorted(self.store.keys()))
        for index, key in enumerate(sortedKeys):
            if key >= keySelector.key:
                index += keySelector.offset
                if (key == keySelector.key and not keySelector.or_equal) or key != keySelector.key:
                    index -= 1

                if index < 0 or index >= len(sortedKeys):
                    return ''
                else:
                    return sortedKeys[index]

        index = len(sortedKeys) + keySelector.offset - 1
        if index < 0 or index >= len(sortedKeys):
            return ''
        else:
            return sortedKeys[index]

    def get_range(self, begin, end, limit=None):
        values = []
        count = 0

        if begin is fdb.KeySelector:
            begin = get_key(begin)
        if end is fdb.KeySelector:
            end = get_key(end)

        for key, value in sorted(self.store.iteritems()):
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
        for key, value in sorted(self.store.iteritems()):
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
            key = get_key(key)

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
    callbackError = ''

    # Python correctness tests (checks if functions run and yield correct results)
    def run_test(self):
        try:
            db = fdb.open(None, 'DB')
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('fdb.open failed'))
            return

        try:
            print('Testing functions...')
            self.testFunctions(db)

            print('Testing correctness...')
            del db[:]
            self.testCorrectness(db)
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Failed to complete all tests'))

    # Generates a random set of keys and values
    def generateData(self, numKeys, minKeyLength, maxKeyLength, minValueLength, maxValueLength, prefix='', allowDuplicates=True):
        data = list()
        keys = set()
        while len(data) < numKeys:
            # key = prefix + ''.join(random.choice(string.ascii_lowercase)
            #                        for i in range(0, random.randint(minKeyLength - len(prefix), maxKeyLength - len(prefix))))
            key = prefix + ''.join(chr(random.randint(0, 254))
                                   for i in range(0, random.randint(minKeyLength - len(prefix), maxKeyLength - len(prefix))))
            if not allowDuplicates:
                if key in keys:
                    continue
                else:
                    keys.add(key)

            value = ''.join('x' for i in range(0, random.randint(maxKeyLength, maxValueLength)))
            data.append([key, value])

        return data

    # Function to test the callback feature of Future objects
    def testCallback(self, future):
        try:
            future.wait()
        except KeyboardInterrupt:
            raise
        except:
            self.callbackError = getError('Callback future get failed')

        self.callback = True

    # Tests that all of the functions in the python API can be called without failing
    def testFunctions(self, db):
        self.callback = False
        self.callbackError = ''

        try:
            tr = db.create_transaction()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('db.create_transaction failed'))
            return

        try:
            tr['testkey'] = 'testvalue'
            value = tr['testkey']
            value.is_ready()
            value.block_until_ready()
            value.wait()

        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Set/Get value failed (block until ready)'))

        try:
            value = tr['testkey']
            value.wait()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Get value failed'))

        try:
            tr['testkey'] = 'newtestvalue'
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Replace value failed'))

        try:
            value = tr['fakekey']
            # The following line would generate a segfault
            # value.capi.fdb_future_block_until_ready(0)
            value.wait()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Get non-existent key failed'))

        try:
            tr.commit().wait()
            tr.get_committed_version()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Commit failed'))

        try:
            tr.reset()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Reset failed'))

        try:
            version = tr.get_read_version()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('tr.get_read_version failed'))

        try:
            value = tr['testkey']
            value.wait()
            tr.reset()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Get and reset failed'))

        try:
            tr.set_read_version(version.wait())
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Set read version failed'))

        try:
            value = tr['testkey']
            callbackTime = time.time()
            value.on_ready(self.testCallback)
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Get future and set callback failed'))

        try:
            del tr['testkey']
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Delete key failed'))

        try:
            del tr['fakekey']
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Delete non-existent key failed'))

        try:
            tr.set('testkey', 'testvalue')
            value = tr.get('testkey')
            value.wait()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Future.get failed'))

        try:
            tr.clear('testkey')
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Clear key failed'))

        try:
            tr['testkey1'] = 'testvalue1'
            tr['testkey2'] = 'testvalue2'
            tr['testkey3'] = 'testvalue3'

            for k, v in tr.get_range('testkey1', 'testkey3'):
                v += ''

            for k, v in tr.get_range('testkey1', 'testkey2', 2):
                v += ''

            for k, v in tr['testkey1':'testkey3']:
                v += ''
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Get range failed'))

        try:
            tr['otherkey1'] = 'othervalue1'
            tr['otherkey2'] = 'othervalue2'

            for k, v in tr.get_range_startswith('testkey'):
                v += ''
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Get range starts with failed'))

        try:
            tr.clear_range_startswith('otherkey')
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Clear range starts with failed'))

        try:
            tr.clear_range('testkey1', 'testkey3')
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Clear range failed'))

        try:
            tr['testkey1'] = 'testvalue1'
            tr['testkey2'] = 'testvalue2'
            tr['testkey3'] = 'testvalue3'

            begin = fdb.KeySelector('testkey2', 0, 0)
            end = fdb.KeySelector('testkey2', 0, 1)
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Create key selector failed'))

        try:
            for k, v in tr.get_range(begin, end):
                v += ''

            for k, v in tr.get_range(begin, end, 2):
                v += ''

            for k, v in tr[begin:end]:
                v += ''
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Get range (key selectors) failed'))

        try:
            tr.clear_range(begin, end)

            tr['testkey1'] = 'testvalue1'
            tr['testkey2'] = 'testvalue2'
            tr['testkey3'] = 'testvalue3'

            del tr[begin:end]

            tr['testkey1'] = 'testvalue1'
            tr['testkey2'] = 'testvalue2'
            tr['testkey3'] = 'testvalue3'
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Clear range (key selectors) failed'))

        try:
            begin = fdb.KeySelector.last_less_than('testkey2')
            end = fdb.KeySelector.first_greater_or_equal('testkey2')

            for k, v in tr.get_range(begin, end):
                v += ''

            begin = fdb.KeySelector.last_less_or_equal('testkey2')
            end = fdb.KeySelector.first_greater_than('testkey2')

            for k, v in tr.get_range(begin, end):
                v += ''
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Builtin key selectors failed'))

        try:
            del tr['testkey1':'testkey3']
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Delete key range failed'))

        try:
            tr.commit().wait()
            tr.get_committed_version()
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Commit failed'))

        try:
            key = fdb.tuple.pack(('k1', 'k2', 'k3'))
            kTuple = fdb.tuple.unpack(key)
            if kTuple[0] != 'k1' and kTuple[1] != 'k2' and kTuple[2] != 'k3':
                self.result.add_error('Tuple <-> key conversion yielded incorrect results')
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Tuple <-> key conversion failed'))

        try:
            tr[fdb.tuple.pack(('k1', 'k2'))] = 'v'
            tr[fdb.tuple.pack(('k1', 'k2', 'k3'))] = 'v1'
            tr[fdb.tuple.pack(('k1', 'k2', 'k3', 'k4'))] = 'v2'

            for k, v in tr[fdb.tuple.range(('k1', 'k2'))]:
                v += ''

        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Tuple get range failed'))

        try:
            tr['testint'] = '10'
            y = int(tr['testint']) + 1
            if y != 11:
                self.result.add_error('Value retrieval yielded incorrect results')
        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(getError('Future value retrieval failed'))

        if not self.callback:
            time.sleep(5)
            if not self.callback:
                self.result.add_error('Warning: Future callback not called after %f seconds' % (time.time() - callbackTime))
        if len(self.callbackError) > 0:
            self.result.add_error(self.callbackError)

    # Compares a FoundationDB database with an in-memory key-value store
    def compareDatabaseToMemory(self, db, store):
        dbResult = self.correctnessGetRangeTransactional(db, '\x00', '\xff')
        storeResult = store.get_range('\x00', '\xff')

        return self.compareResults(dbResult, storeResult)

    # Compares result sets coming from a FoundationDB database and an in-memory key-value store
    def compareResults(self, dbResults, storeResults):
        if len(dbResults) != len(storeResults):
            # print 'mismatched lengths: ' + str(len(dbResults)) + ' - ' + str(len(storeResults))
            return False

        for i in range(0, len(dbResults)):
            # if i >= len(storeResults):
            #    print 'mismatched key: ' + dbResults[i].key
            #    return False
            if dbResults[i].key != storeResults[i][0] or dbResults[i].value != storeResults[i][1]:
                # print 'mismatched key: ' + dbResults[i].key + ' - ' + storeResults[i][0]
                return False

        return True

    # Performs the same operations on a FoundationDB database and an in-memory key-value store and compares the results
    def testCorrectness(self, db):
        numKeys = 5000
        ratioShortKeys = 0.5
        minShortKeyLength = 1
        maxShortKeyLength = 3
        minLongKeyLength = 1
        maxLongKeyLength = 128
        minValueLength = 1
        maxValueLength = 10000
        maxTransactionBytes = 5000000

        numReads = 100
        numRangeReads = 100
        numPrefixReads = 100
        numGetKeys = 100

        numClears = 100
        numRangeClears = 10
        numPrefixClears = 10

        maxKeysPerTransaction = max(1, maxTransactionBytes / (maxValueLength + maxLongKeyLength))

        try:
            store = KeyValueStore()

            # Generate some random data
            data = self.generateData(numKeys * ratioShortKeys, minShortKeyLength, maxShortKeyLength, minValueLength, maxValueLength)
            data.extend(self.generateData(numKeys * (1 - ratioShortKeys), minLongKeyLength, maxLongKeyLength, minValueLength, maxValueLength))

            # Insert the data
            self.correctnessSet(db, store, data, maxKeysPerTransaction)
            if not self.compareDatabaseToMemory(db, store):
                self.result.add_error('transaction.set resulted in incorrect database')

            # Compare the results of single key reads
            if not self.correctnessGet(db, store, data, numReads, maxKeysPerTransaction):
                self.result.add_error('transaction.get returned incorrect result')

            # Compare the results of range reads
            for i in range(0, numRangeReads):
                if not self.correctnessGetRange(db, store, data):
                    self.result.add_error('transaction.get_range returned incorrect results')
                    break

            # Compare the results of prefix reads
            for i in range(0, numPrefixReads):
                if not self.correctnessGetPrefix(db, store, data):
                    self.result.add_error('transaction.get_range_startswith returned incorrect results')
                    break

            # Compare the results of get key
            if not self.correctnessGetKey(db, store, data, numGetKeys, maxKeysPerTransaction):
                self.result.add_error('transaction.get_key returned incorrect results')

            # Compare the results of clear
            clearedKeys = self.correctnessClear(db, store, data, numClears, maxKeysPerTransaction)
            if not self.compareDatabaseToMemory(db, store):
                self.result.add_error('transaction.clear resulted in incorrect database')

            #    for key in clearedKeys:
            #         print 'clearing key ' + key
            # else:
            #     print 'successful compare'

            # Fill the database back up with data
            self.correctnessSet(db, store, data, maxKeysPerTransaction)
            if not self.compareDatabaseToMemory(db, store):
                self.result.add_error('transaction.set resulted in incorrect database')

            # Compare the results of clear_range
            for i in range(0, numRangeClears):
                self.correctnessClearRange(db, store, data)

                success = self.compareDatabaseToMemory(db, store)
                if not success:
                    self.result.add_error('transaction.clear_range resulted in incorrect database')

                # Fill the database back up with data
                self.correctnessSet(db, store, data, maxKeysPerTransaction)
                if not self.compareDatabaseToMemory(db, store):
                    self.result.add_error('transaction.set resulted in incorrect database')
                    break

                if not success:
                    break

            # Compare the results of clear_range_startswith
            self.correctnessClearPrefix(db, store, data, numPrefixClears)
            if not self.compareDatabaseToMemory(db, store):
                self.result.add_error('transaction.clear_range_startswith resulted in incorrect database')

        except KeyboardInterrupt:
            raise
        except:
            self.result.add_error(self.getError('Database error in correctness test'))

    # Stores data in the database and a memory key-value store
    def correctnessSet(self, db, store, data, maxKeysPerTransaction):
        for [key, value] in data:
            store.set(key, value)

        keysCommitted = 0
        while keysCommitted < len(data):
            self.correctnessSetTransactional(db, data[keysCommitted: keysCommitted + maxKeysPerTransaction])
            keysCommitted += maxKeysPerTransaction

    # Stores data in the database
    @fdb.transactional
    def correctnessSetTransactional(self, tr, data):
        for [key, value] in data:
            tr.set(key, value)

    # Compares the results of the get operation from the database and a memory key-value store
    def correctnessGet(self, db, store, data, numReads, maxKeysPerTransaction):
        keys = []
        for i in range(0, numReads):
            index = random.randint(0, len(data) - 1)
            keys.append(data[index][0])

        keysRetrieved = 0
        while keysRetrieved < len(keys):
            subKeys = keys[keysRetrieved: keysRetrieved + maxKeysPerTransaction]

            values = self.correctnessGetTransactional(db, subKeys)
            for i in range(0, numReads):
                if values[i] != store.get(subKeys[i]):
                    print('mismatch: %s', subKeys[i])
                    return False
            keysRetrieved += maxKeysPerTransaction

        return True

    # Gets the values for the specified list of keys from the database
    @fdb.transactional
    def correctnessGetTransactional(self, tr, keys):
        futures = []
        for key in keys:
            futures.append(tr.get(key))

        values = []
        for future in futures:
            values.append(future.wait())

        return values

    # Compares the results of the get_range operation from the database and a memory key-value store
    def correctnessGetRange(self, db, store, data):
        index = random.randint(0, len(data) - 1)
        index2 = random.randint(0, len(data) - 1)

        key1 = min(data[index][0], data[index2][0])
        key2 = max(data[index][0], data[index2][0])

        dbResults = self.correctnessGetRangeTransactional(db, key1, key2, data)
        storeResults = store.get_range(key1, key2)

        return self.compareResults(dbResults, storeResults)

    # Gets the entries in the range [key1,key2) from the database
    @fdb.transactional
    def correctnessGetRangeTransactional(self, tr, key1, key2, data=None):
        if data is not None:
            return list(tr.get_range(key1, key2, len(data)))
        else:
            return list(tr.get_range(key1, key2))

    # Compares the results of the get_range_startswith operation from the database and a memory key-value store
    def correctnessGetPrefix(self, db, store, data):
        prefix = ''.join(chr(random.randint(0, 254)) for i in range(0, random.randint(1, 3)))
        dbResults = self.correctnessGetPrefixTransactional(db, prefix)
        storeResults = store.get_range_startswith(prefix)

        return self.compareResults(dbResults, storeResults)

    # Gets the entries with a given prefix from the database
    @fdb.transactional
    def correctnessGetPrefixTransactional(self, tr, prefix):
        return list(tr.get_range_startswith(prefix))

    # Compares the results of the get_key operation from the database and a memory key-value store
    def correctnessGetKey(self, db, store, data, numGetKeys, maxKeysPerTransaction):
        selectors = []
        for i in range(0, numGetKeys):
            index = random.randint(0, len(data) - 1)
            or_equal = random.randint(0, 1)
            offset = random.randint(max(-index + (1 - or_equal), -10), min(len(data) - index - 1 + or_equal, 10))

            key = sorted(data)[index][0]
            selector = fdb.KeySelector(key, or_equal, offset)
            selectors.append(selector)

        keysRetrieved = 0
        while keysRetrieved < len(selectors):
            subSelectors = selectors[keysRetrieved: keysRetrieved + maxKeysPerTransaction]
            dbKeys = self.correctnessGetKeyTransactional(db, subSelectors)
            for i in range(0, numGetKeys):
                if dbKeys[i] != store.get_key(subSelectors[i]):
                    return False
            keysRetrieved += maxKeysPerTransaction

        return True

    # Gets the keys specified by the list of key selectors
    @fdb.transactional
    def correctnessGetKeyTransactional(self, tr, keySelectors):
        futures = []
        count = 0
        for selector in keySelectors:
            futures.append(tr.get_key(selector))
            count += 1

        keys = []
        count = 0
        for future in futures:
            keys.append(future.wait())
            count += 1

        return keys

    # Clears data from a database and a memory key-value store
    def correctnessClear(self, db, store, data, numClears, maxKeysPerTransaction):
        clearedKeys = []
        for i in range(0, numClears):
            index = random.randint(0, len(data) - 1)
            clearedKeys.append(data[index][0])
            store.clear(data[index][0])

        keysCleared = 0
        while keysCleared < len(clearedKeys):
            self.correctnessClearTransactional(db, clearedKeys[keysCleared: keysCleared + maxKeysPerTransaction])
            keysCleared += maxKeysPerTransaction

        return clearedKeys

    # Clears a list of keys from the database
    @fdb.transactional
    def correctnessClearTransactional(self, tr, clearedKeys):
        for key in clearedKeys:
            tr.clear(key)

    # Clears a range of data from a database and a memory key-value store
    def correctnessClearRange(self, db, store, data):
        index = random.randint(0, len(data) - 1)
        index2 = random.randint(0, len(data) - 1)

        key1 = min(data[index][0], data[index2][0])
        key2 = max(data[index][0], data[index2][0])

        self.correctnessClearRangeTransactional(db, key1, key2)
        store.clear_range(key1, key2)

    # Clears a range of memory from a database
    @fdb.transactional
    def correctnessClearRangeTransactional(self, tr, key1, key2):
        tr.clear_range(key1, key2)

    # Clears data with random prefixes from a database and a memory key-value store
    def correctnessClearPrefix(self, db, store, data, numPrefixClears):
        prefixes = []
        for i in range(0, numPrefixClears):
            prefix = ''.join(chr(random.randint(0, 254)) for i in range(0, random.randint(1, 3)))
            prefixes.append(prefix)
            store.clear_range_startswith(prefix)

        self.correctnessClearPrefixTransactional(db, prefixes)

    # Clears keys from a database that have a prefix in the prefixes list
    @fdb.transactional
    def correctnessClearPrefixTransactional(self, tr, prefixes):
        for prefix in prefixes:
            tr.clear_range_startswith(prefix)

    # Adds the stack trace to an error message
    def getError(self, message):
        errorMessage = message + "\n" + traceback.format_exc()
        print('%s', errorMessage)
        return errorMessage


if __name__ == '__main__':
    print("Running PythonCorrectness test on Python version %d.%d.%d%s%d" %
          (sys.version_info[0], sys.version_info[1], sys.version_info[2], sys.version_info[3][0], sys.version_info[4]))

    PythonCorrectness().run()
