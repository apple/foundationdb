#
# compressedColumn.py
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

import fdb
import fdb.tuple
import struct

_unpackedPrefix = "unpacked"
_packedPrefix = "packed"

fdb.api_version(16)


class _MergedData:
    def __init__(self):
        self.results = []
        self.finishedPack = False
        self.finishedUnpack = False
        self.packedIndex = 0
        pass


class Column:
    def __init__(self, column_name):
        self.columnName = column_name
        self.packFetchCount = 10
        self.targetChunkSize = 5000
        self.maxChunkSize = 10000
        # self.mergeChunkSize = 2500

    def _get_sub_key_tuple(self, key):
        return fdb.tuple.unpack(key)[2:]

    def _get_sub_key(self, key):
        return fdb.tuple.unpack(key)[2]

    def _is_packed_key(self, key):
        return str(key).startswith(fdb.tuple.pack((self.columnName, _packedPrefix)))

    # This results in slight inefficiencies when the key being searched for comes before the first packed segment with strictRange=False
    def _get_packed_data(self, key, packed_range, require_key=True, strict_range=True):
        found = False
        key_range = None
        packed_key_range = None
        packed_data = None

        for k, v in packed_range:
            # print 'Searching ' + k + ' for ' + key
            if self._is_packed_key(k):
                if found:
                    end_range = self._get_sub_key_tuple(k)
                    key_range = (key_range[0], end_range[0])
                    break

                key_range = self._get_sub_key_tuple(k)
                packed_key_range = key_range
                # print str(key_range)
                if (not require_key or key >= key_range[0]) and key <= key_range[1]:
                    if strict_range:
                        packed_data = _PackedData(v)
                        break
                    else:
                        found = True
                        key_range = (key_range[0], key_range[1] + chr(0))
                        packed_data = _PackedData(v)

            elif found:
                key_range = (key_range[0], "\xff")

        if strict_range:
            return [key_range, packed_data]
        else:
            return [packed_key_range, key_range, packed_data]

    def _get_packed_range(self, tr, key):
        return tr.get_range(
            fdb.KeySelector.last_less_than(
                fdb.tuple.pack((self.columnName, _packedPrefix, key + chr(0)))
            ),
            fdb.tuple.pack((self.columnName, _packedPrefix + chr(0))),
            2,
        )

    def _get_unpacked_data(self, tr, key):
        return tr[fdb.tuple.pack((self.columnName, _unpackedPrefix, key))]

    def _get_unpacked_range(self, tr, key_begin, key_end, limit):
        return tr.get_range(
            fdb.tuple.pack((self.columnName, _unpackedPrefix, key_begin)),
            fdb.tuple.pack((self.columnName, _unpackedPrefix, key_end)),
            limit,
        )

    def _merge_results(
            self,
            packed,
            unpacked,
            total_unpacked,
            packed_index=0,
            min_packed_key="",
            max_key=None,
    ):
        data = _MergedData()
        if packed is None:
            # print 'No merge necessary'
            data.finishedUnpack = True
            data.finishedPack = True
            data.packedIndex = 0

            if max_key is None:
                data.results = [
                    fdb.KeyValue(self._get_sub_key(k), v) for k, v in unpacked
                ]
            else:
                for k, v in unpacked:
                    if k < max_key:
                        data.results.append(fdb.KeyValue(self._get_sub_key(k), v))
                    else:
                        data.finishedUnpack = False
                        break
        else:
            # print 'Merging packed'
            unpacked_count = 0
            for k, v in unpacked:
                sub_key = self._get_sub_key(k)
                # print 'Unpacked: ' + sub_key
                if max_key is not None and sub_key >= max_key:
                    # print 'sub_key >= maxKey %s, %s' % (sub_key, maxKey)
                    break

                exact_match = False
                while (
                        packed_index < len(packed.rows)
                        and packed.rows[packed_index].key <= sub_key
                        and (max_key is None or packed.rows[packed_index].key < max_key)
                ):
                    exact_match = packed.rows[packed_index].key == sub_key
                    if (
                            sub_key > packed.rows[packed_index].key >= min_packed_key
                    ):
                        data.results.append(packed.rows[packed_index])

                    packed_index += 1

                if max_key is None and packed_index == len(packed.rows):
                    if exact_match:
                        data.results.append(fdb.KeyValue(self._get_sub_key(k), v))

                    # print 'packedIndex == len(packed.rows)'
                    break

                data.results.append(fdb.KeyValue(self._get_sub_key(k), v))
                unpacked_count += 1

            # print "Packed index: %d, Unpacked: %d, total: %d" % (packedIndex, unpacked_count, totalUnpacked)
            if unpacked_count < total_unpacked:
                while packed_index < len(packed.rows) and (
                        max_key is None or packed.rows[packed_index].key < max_key
                ):
                    if packed.rows[packed_index].key >= min_packed_key:
                        data.results.append(packed.rows[packed_index])

                    packed_index += 1

            data.finishedPack = packed_index == len(packed.rows)
            data.finishedUnpack = unpacked_count == total_unpacked
            data.packedIndex = packed_index

            # print str(data.results)
            # print 'Num Results: %d' % len(data.results)

        return data

    @fdb.transactional
    def set_row(self, tr, row_name, value):
        tr[fdb.tuple.pack((self.columnName, _unpackedPrefix, row_name))] = value

    @fdb.transactional
    def get_row(self, tr, row_name):
        unpacked = self._get_unpacked_data(tr, row_name)
        packed_range = self._get_packed_range(tr, row_name)

        if unpacked.present():
            return unpacked
        else:
            packed_data = self._get_packed_data(row_name, packed_range)[1]
            if packed_data is None:
                return None

            return packed_data.get_row(row_name)

    @fdb.transactional
    def delete(self, tr):
        tr.clear_range_startswith(self.columnName)

    def get_column_stream(self, db, start_row=""):
        return _ColumnStream(db, self, start_row)

    # This function is not fully transactional.  Each compressed block will be created in a transaction
    def pack(self, db, start_row="", end_row="\xff"):
        current_row = start_row
        num_fetched = self.packFetchCount

        while num_fetched == self.packFetchCount:
            # print 'outer: \'' + repr(current_row) + '\''
            try:
                tr = db.create_transaction()
                packed_index = 0
                packed_data = None
                last_row = current_row
                new_pack = _PackedData()
                old_rows = []

                while True:
                    # print 'inner: \'' + repr(current_row) + '\''
                    unpacked = list(
                        self._get_unpacked_range(tr, last_row, end_row, self.packFetchCount)
                    )

                    unpacked_count = len(unpacked)
                    if len(unpacked) == 0:
                        break

                    if packed_data is None:
                        sub_key = self._get_sub_key(unpacked[0].key)
                        packed_range = self._get_packed_range(tr, sub_key)
                        [packed_key_range, key_range, packed_data] = self._get_packed_data(
                            sub_key, packed_range, False, False
                        )
                        if packed_key_range is not None:
                            # print 'Deleting old rows'
                            old_rows.append(
                                fdb.tuple.pack(
                                    (
                                        self.columnName,
                                        _packedPrefix,
                                        packed_key_range[0],
                                        packed_key_range[1],
                                    )
                                )
                            )

                    max_key = None
                    if key_range is not None:
                        max_key = key_range[1]

                    merged = self._merge_results(
                        packed_data,
                        unpacked,
                        self.packFetchCount,
                        packed_index,
                        last_row,
                        max_key,
                    )
                    for row in merged.results:
                        old_rows.append(
                            fdb.tuple.pack((self.columnName, _unpackedPrefix, row.key))
                        )
                        new_pack.add_row(row)
                        last_row = row.key
                        # print 'Set last_row = \'' + repr(last_row) + '\''

                    last_row = last_row + chr(0)
                    if (max_key is not None and merged.finishedPack) or (
                            max_key is None and new_pack.bytes > self.targetChunkSize
                    ):
                        break

                # print 'Deleting rows'
                for row in old_rows:
                    # print 'Deleting row ' + repr(row)
                    del tr[row]

                for k, v in new_pack.get_packed_key_values(
                        self, self.targetChunkSize, self.maxChunkSize
                ):
                    tr[k] = v

                tr.commit().wait()
                current_row = last_row
                num_fetched = unpacked_count
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
            self._fetch_rows(db)

        if self.resultsIndex >= len(self.results):
            # print 'Finished iterating: (%d/%d)' % (self.resultsIndex, len(self.results))
            return None

        else:
            self.currentKey = self.results[self.resultsIndex].key
            value = self.results[self.resultsIndex].value
            self.resultsIndex += 1
            # print 'Returning value (%s, %s)' % (self.currentKey, value)
            return self.currentKey, value

    @fdb.transactional
    def _fetch_rows(self, tr):

        if self.firstRead:
            # print 'First fetch'
            start_key = self.currentKey
        else:
            # print 'Subsequent fetch %s' % self.currentKey
            start_key = self.currentKey + chr(0)

        # print 'Using start key %s' % start_key

        # Read next packed and unpacked entries
        # FIXME: Should we read unpacked after getting the result of the packed data?  If we do, then we can more accurately limit the number
        # of results that we get back
        unpacked = self.column._get_unpacked_range(tr, start_key, "\xff", self.fetchCount)

        if self.packedData is None:
            packed_range = self.column._get_packed_range(tr, start_key)
            [_, self.packedData] = self.column._get_packed_data(
                start_key, packed_range, False
            )

        merged = self.column._merge_results(
            self.packedData, unpacked, self.fetchCount, self.packedIndex, start_key
        )

        if merged.finishedPack:
            # print 'reset packed'
            self.packedData = None
            self.packedIndex = 0
        else:
            # print 'more packed %d' % merged.packedIndex
            self.packedIndex = merged.packedIndex

        self.results = merged.results

        # print 'Getting range %s - %s (%d)' % (start_key, fdb.tuple.pack((self.column.columnName + chr(0))), self.fetchCount)
        self.resultsIndex = 0
        self.firstRead = False


class _PackedData:
    def __init__(self, packed_value=None):
        self.rows = []
        self.bytes = 0

        if packed_value is not None:
            self._unpack(packed_value)

    def add_row(self, row):
        # print 'adding row %s' % row.key
        self.rows.append(row)
        self.bytes += len(row.key) + len(row.value) + 12

    def get_row(self, row_name):
        for row in self.rows:
            if row.key == row_name:
                return row.value

        return None

    def get_packed_key_values(self, column, target_size, max_size):
        row_index = 0
        current_byte = 0

        results = []

        while current_byte < self.bytes:
            header_items = []
            size = target_size
            if self.bytes - current_byte < max_size:
                size = max_size

            start_row_index = row_index
            start_key = None
            end_key = None
            body_length = 0
            pack_bytes = 0
            while row_index < len(self.rows) and pack_bytes < size:
                row = self.rows[row_index]
                header_items.append(
                    struct.pack("iii", len(row.key), len(row.value), body_length)
                    + row.key
                )
                body_length += len(row.value)
                pack_bytes += len(row.key) + len(row.value) + 12
                row_index += 1

                if start_key is None:
                    start_key = row.key
                end_key = row.key

            header = "".join(header_items)
            body = "".join(row.value for row in self.rows[start_row_index:row_index])

            results.append(
                fdb.KeyValue(
                    fdb.tuple.pack(
                        (column.columnName, _packedPrefix, start_key, end_key)
                    ),
                    struct.pack("i", len(header)) + header + body,
                )
            )
            current_byte += pack_bytes

        return results

    def _unpack(self, str):
        self.bytes = len(str)
        header_length = struct.unpack("i", str[0:4])[0]
        header = str[4: 4 + header_length]
        body = str[4 + header_length:]

        index = 0
        while index < header_length:
            # print 'header length: %d, %d' % (len(self.header), index)
            (keyLength, valueLength, valueOffset) = struct.unpack(
                "iii", header[index: index + 12]
            )
            key = header[index + 12: index + 12 + keyLength]
            index = index + 12 + keyLength
            value = body[valueOffset: valueOffset + valueLength]
            self.rows.append(fdb.KeyValue(key, value))
