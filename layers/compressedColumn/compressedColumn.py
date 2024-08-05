#
# compressedColumn.py
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

import fdb
import fdb.tuple
import struct

_unpackedPrefix = 'unpacked'
_packedPrefix = 'packed'

fdb.api_version(16)


class _MergedData:
    def __init__(self):
        self.results = []
        self.finishedPack = False
        self.finishedUnpack = False
        self.packedIndex = 0
        pass


class Column:
    def __init__(self, columnName):
        self.columnName = columnName
        self.packFetchCount = 10
        self.targetChunkSize = 5000
        self.maxChunkSize = 10000
        # self.mergeChunkSize = 2500

    def _getSubKeyTuple(self, key):
        return fdb.tuple.unpack(key)[2:]

    def _getSubKey(self, key):
        return fdb.tuple.unpack(key)[2]

    def _isPackedKey(self, key):
        return str(key).startswith(fdb.tuple.pack((self.columnName, _packedPrefix)))

    # This results in slight inefficiencies when the key being searched for comes before the first packed segment with strictRange=False
    def _getPackedData(self, key, packedRange, requireKey=True, strictRange=True):
        found = False
        keyRange = None
        packedKeyRange = None
        packedData = None

        for k, v in packedRange:
            # print 'Searching ' + k + ' for ' + key
            if self._isPackedKey(k):
                if found:
                    endRange = self._getSubKeyTuple(k)
                    keyRange = (keyRange[0], endRange[0])
                    break

                keyRange = self._getSubKeyTuple(k)
                packedKeyRange = keyRange
                # print str(keyRange)
                if (not requireKey or key >= keyRange[0]) and key <= keyRange[1]:
                    if strictRange:
                        packedData = _PackedData(v)
                        break
                    else:
                        found = True
                        keyRange = (keyRange[0], keyRange[1] + chr(0))
                        packedData = _PackedData(v)

            elif found:
                keyRange = (keyRange[0], '\xff')

        if strictRange:
            return [keyRange, packedData]
        else:
            return [packedKeyRange, keyRange, packedData]

    def _getPackedRange(self, tr, key):
        return tr.get_range(fdb.KeySelector.last_less_than(fdb.tuple.pack((self.columnName, _packedPrefix, key + chr(0)))),
                            fdb.tuple.pack((self.columnName, _packedPrefix + chr(0))), 2)

    def _getUnpackedData(self, tr, key):
        return tr[fdb.tuple.pack((self.columnName, _unpackedPrefix, key))]

    def _getUnpackedRange(self, tr, keyBegin, keyEnd, limit):
        return tr.get_range(fdb.tuple.pack((self.columnName, _unpackedPrefix, keyBegin)),
                            fdb.tuple.pack((self.columnName, _unpackedPrefix, keyEnd)), limit)

    def _mergeResults(self, packed, unpacked, totalUnpacked, packedIndex=0, minPackedKey='', maxKey=None):
        data = _MergedData()
        if packed is None:
            # print 'No merge necessary'
            data.finishedUnpack = True
            data.finishedPack = True
            data.packedIndex = 0

            if maxKey is None:
                data.results = [fdb.KeyValue(self._getSubKey(k), v) for k, v in unpacked]
            else:
                for k, v in unpacked:
                    if k < maxKey:
                        data.results.append(fdb.KeyValue(self._getSubKey(k), v))
                    else:
                        data.finishedUnpack = False
                        break
        else:
            # print 'Merging packed'
            unpackedCount = 0
            for k, v in unpacked:
                subKey = self._getSubKey(k)
                # print 'Unpacked: ' + subKey
                if maxKey is not None and subKey >= maxKey:
                    # print 'subKey >= maxKey %s, %s' % (subKey, maxKey)
                    break

                exactMatch = False
                while packedIndex < len(packed.rows) \
                        and packed.rows[packedIndex].key <= subKey \
                        and (maxKey is None or packed.rows[packedIndex].key < maxKey):
                    exactMatch = packed.rows[packedIndex].key == subKey
                    if packed.rows[packedIndex].key < subKey and packed.rows[packedIndex].key >= minPackedKey:
                        data.results.append(packed.rows[packedIndex])

                    packedIndex += 1

                if maxKey is None and packedIndex == len(packed.rows):
                    if exactMatch:
                        data.results.append(fdb.KeyValue(self._getSubKey(k), v))

                    # print 'packedIndex == len(packed.rows)'
                    break

                data.results.append(fdb.KeyValue(self._getSubKey(k), v))
                unpackedCount += 1

            # print "Packed index: %d, Unpacked: %d, total: %d" % (packedIndex, unpackedCount, totalUnpacked)
            if unpackedCount < totalUnpacked:
                while packedIndex < len(packed.rows) and (maxKey is None or packed.rows[packedIndex].key < maxKey):
                    if packed.rows[packedIndex].key >= minPackedKey:
                        data.results.append(packed.rows[packedIndex])

                    packedIndex += 1

            data.finishedPack = packedIndex == len(packed.rows)
            data.finishedUnpack = unpackedCount == totalUnpacked
            data.packedIndex = packedIndex

            # print str(data.results)
            # print 'Num Results: %d' % len(data.results)

        return data

    @fdb.transactional
    def setRow(self, tr, rowName, value):
        tr[fdb.tuple.pack((self.columnName, _unpackedPrefix, rowName))] = value

    @fdb.transactional
    def getRow(self, tr, rowName):
        unpacked = self._getUnpackedData(tr, rowName)
        packedRange = self._getPackedRange(tr, rowName)

        if unpacked.present():
            return unpacked
        else:
            packedData = self._getPackedData(rowName, packedRange)[1]
            if packedData is None:
                return None

            return packedData.getRow(rowName)

        return None

    @fdb.transactional
    def delete(self, tr):
        tr.clear_range_startswith(self.columnName)

    def getColumnStream(self, db, startRow=''):
        return _ColumnStream(db, self, startRow)

    # This function is not fully transactional.  Each compressed block will be created in a transaction
    def pack(self, db, startRow='', endRow='\xff'):
        currentRow = startRow
        numFetched = self.packFetchCount

        while numFetched == self.packFetchCount:
            # print 'outer: \'' + repr(currentRow) + '\''
            try:
                tr = db.create_transaction()
                packedIndex = 0
                packedData = None
                lastRow = currentRow
                newPack = _PackedData()
                oldRows = []

                while True:
                    # print 'inner: \'' + repr(currentRow) + '\''
                    unpacked = list(self._getUnpackedRange(tr, lastRow, endRow, self.packFetchCount))

                    unpackedCount = len(unpacked)
                    if len(unpacked) == 0:
                        break

                    if packedData is None:
                        subKey = self._getSubKey(unpacked[0].key)
                        packedRange = self._getPackedRange(tr, subKey)
                        [packedKeyRange, keyRange, packedData] = self._getPackedData(subKey, packedRange, False, False)
                        if packedKeyRange is not None:
                            # print 'Deleting old rows'
                            oldRows.append(fdb.tuple.pack((self.columnName, _packedPrefix, packedKeyRange[0], packedKeyRange[1])))

                    maxKey = None
                    if keyRange is not None:
                        maxKey = keyRange[1]

                    merged = self._mergeResults(packedData, unpacked, self.packFetchCount, packedIndex, lastRow, maxKey)
                    for row in merged.results:
                        oldRows.append(fdb.tuple.pack((self.columnName, _unpackedPrefix, row.key)))
                        newPack.addRow(row)
                        lastRow = row.key
                        # print 'Set lastRow = \'' + repr(lastRow) + '\''

                    lastRow = lastRow + chr(0)
                    if (maxKey is not None and merged.finishedPack) or (maxKey is None and newPack.bytes > self.targetChunkSize):
                        break

                # print 'Deleting rows'
                for row in oldRows:
                    # print 'Deleting row ' + repr(row)
                    del tr[row]

                for k, v in newPack.getPackedKeyValues(self, self.targetChunkSize, self.maxChunkSize):
                    tr[k] = v

                tr.commit().wait()
                currentRow = lastRow
                numFetched = unpackedCount
            except fdb.FDBError as e:
                if e.code == 1007:  # transaction_too_old
                    pass
                    # FIXME: Unpack the overlapping packed block and try again
                tr.on_error(e.code).wait()


class _ColumnStream:

    def __init__(self, db, column, startKey):
        self.column = column
        self.db = db
        self.currentKey = startKey
        self.results = []
        self.resultsIndex = 0
        self.firstRead = True
        self.fetchCount = 5
        self.packedData = None
        self.packedIndex = 0

    def __iter__(self):
        return self

    def next(self):
        value = self._readNextRow(self.db)
        if value is None:
            raise StopIteration
        else:
            return value

    def _readNextRow(self, db):
        # print 'Reading next row'
        if self.resultsIndex >= len(self.results):
            # print 'Fetching rows'
            self._fetchRows(db)

        if self.resultsIndex >= len(self.results):
            # print 'Finished iterating: (%d/%d)' % (self.resultsIndex, len(self.results))
            return None

        else:
            self.currentKey = self.results[self.resultsIndex].key
            value = self.results[self.resultsIndex].value
            self.resultsIndex += 1
            # print 'Returning value (%s, %s)' % (self.currentKey, value)
            return (self.currentKey, value)

    @fdb.transactional
    def _fetchRows(self, tr):

        if self.firstRead:
            # print 'First fetch'
            startKey = self.currentKey
        else:
            # print 'Subsequent fetch %s' % self.currentKey
            startKey = self.currentKey + chr(0)

        # print 'Using start key %s' % startKey

        # Read next packed and unpacked entries
        # FIXME: Should we read unpacked after getting the result of the packed data?  If we do, then we can more accurately limit the number
        # of results that we get back
        unpacked = self.column._getUnpackedRange(tr, startKey, '\xff', self.fetchCount)

        if self.packedData is None:
            packedRange = self.column._getPackedRange(tr, startKey)
            [keyRange, self.packedData] = self.column._getPackedData(startKey, packedRange, False)

        merged = self.column._mergeResults(self.packedData, unpacked, self.fetchCount, self.packedIndex, startKey)

        if merged.finishedPack:
            # print 'reset packed'
            self.packedData = None
            self.packedIndex = 0
        else:
            # print 'more packed %d' % merged.packedIndex
            self.packedIndex = merged.packedIndex

        self.results = merged.results

        # print 'Getting range %s - %s (%d)' % (startKey, fdb.tuple.pack((self.column.columnName + chr(0))), self.fetchCount)
        self.resultsIndex = 0
        self.firstRead = False


class _PackedData:
    def __init__(self, packedValue=None):
        self.rows = []
        self.bytes = 0

        if packedValue is not None:
            self._unpack(packedValue)

    def addRow(self, row):
        # print 'adding row %s' % row.key
        self.rows.append(row)
        self.bytes += len(row.key) + len(row.value) + 12

    def getRow(self, rowName):
        for row in self.rows:
            if row.key == rowName:
                return row.value

        return None

    def getPackedKeyValues(self, column, targetSize, maxSize):
        rowIndex = 0
        currentByte = 0

        results = []

        while currentByte < self.bytes:
            headerItems = []
            size = targetSize
            if self.bytes - currentByte < maxSize:
                size = maxSize

            startRowIndex = rowIndex
            startKey = None
            endKey = None
            bodyLength = 0
            packBytes = 0
            while rowIndex < len(self.rows) and packBytes < size:
                row = self.rows[rowIndex]
                headerItems.append(struct.pack('iii', len(row.key), len(row.value), bodyLength) + row.key)
                bodyLength += len(row.value)
                packBytes += len(row.key) + len(row.value) + 12
                rowIndex += 1

                if startKey is None:
                    startKey = row.key
                endKey = row.key

            header = ''.join(headerItems)
            body = ''.join(row.value for row in self.rows[startRowIndex:rowIndex])

            results.append(fdb.KeyValue(fdb.tuple.pack((column.columnName, _packedPrefix, startKey, endKey)),
                                        struct.pack('i', len(header)) + header + body))
            currentByte += packBytes

        return results

    def _unpack(self, str):
        self.bytes = len(str)
        headerLength = struct.unpack('i', str[0:4])[0]
        header = str[4:4 + headerLength]
        body = str[4 + headerLength:]

        index = 0
        while index < headerLength:
            # print 'header length: %d, %d' % (len(self.header), index)
            (keyLength, valueLength, valueOffset) = struct.unpack('iii', header[index:index + 12])
            key = header[index + 12:index + 12 + keyLength]
            index = index + 12 + keyLength
            value = body[valueOffset:valueOffset + valueLength]
            self.rows.append(fdb.KeyValue(key, value))
