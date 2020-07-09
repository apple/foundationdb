#!/usr/bin/env python3
#
# transaction_profiling_analyzer.py
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

"""
Requirements:
python3
fdb python bindings
optional packages:
  dateparser (for human date parsing)
  sortedcontainers (for estimating key range read/write density)
"""


import argparse
from collections import defaultdict
from enum import Enum
import fdb
from fdb.impl import strinc
import json
from json import JSONEncoder
import logging
import struct
from bisect import bisect_left
from bisect import bisect_right
import time
import datetime

PROTOCOL_VERSION_5_2 = 0x0FDB00A552000001
PROTOCOL_VERSION_6_0 = 0x0FDB00A570010001
PROTOCOL_VERSION_6_1 = 0x0FDB00B061060001
PROTOCOL_VERSION_6_2 = 0x0FDB00B062010001
PROTOCOL_VERSION_6_3 = 0x0FDB00B063010001
supported_protocol_versions = frozenset([PROTOCOL_VERSION_5_2, PROTOCOL_VERSION_6_0, PROTOCOL_VERSION_6_1,
                                         PROTOCOL_VERSION_6_2, PROTOCOL_VERSION_6_3])


fdb.api_version(520)

BASIC_FORMAT = "%(asctime)s - %(levelname)-8s %(message)s"
LOG_PATH = "transaction_profiling_analyzer.log"


def setup_logger(name):
    root = logging.getLogger(name)
    root.setLevel(logging.DEBUG)
    root.propagate = False

    file_formatter = logging.Formatter(BASIC_FORMAT)

    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    root.addHandler(file_handler)

    return root


logger = setup_logger(__name__)


class ByteBuffer(object):
    def __init__(self, val):
        self._offset = 0
        self.val = val

    def get_bytes(self, n):
        if self._offset + n > len(self.val):
            raise IndexError("Request to read %d bytes with only %d remaining" % (n, self.get_remaining_bytes()))
        ret = self.val[self._offset:self._offset + n]
        self._offset += n
        return ret

    def get_int(self):
        return struct.unpack("<i", self.get_bytes(4))[0]

    def get_long(self):
        return struct.unpack("<q", self.get_bytes(8))[0]

    def get_double(self):
        return struct.unpack("<d", self.get_bytes(8))[0]

    def get_bytes_with_length(self):
        length = self.get_int()
        return self.get_bytes(length)

    def get_key_range(self):
        return KeyRange(self.get_bytes_with_length(), self.get_bytes_with_length())

    def get_key_range_list(self):
        length = self.get_int()
        return [self.get_key_range() for _ in range(0, length)]

    def get_mutation(self):
        return Mutation(ord(self.get_bytes(1)), self.get_bytes_with_length(), self.get_bytes_with_length())

    def get_mutation_list(self):
        length = self.get_int()
        return [self.get_mutation() for _ in range(0, length)]

    def get_remaining_bytes(self):
        return len(self.val) - self._offset


class ObjJsonEncoder(JSONEncoder):
    def default(self, o):
        try:
            super().default(o)
        except TypeError:
            if isinstance(o, Enum):
                return str(o)
            if hasattr(o, "__dict__"):
                return o.__dict__
            return str(o)


class TrInfoChunk(object):
    def __init__(self, num_chunks, chunk_num, key, value):
        self.num_chunks = num_chunks
        self.chunk_num = chunk_num
        self.key = key
        self.value = value


class KeyRange(object):
    def __init__(self, start_key, end_key):
        self.start_key = start_key
        self.end_key = end_key


class MutationType(Enum):
    SET_VALUE = 0
    CLEAR_RANGE = 1
    ADD_VALUE = 2
    DEBUG_KEY_RANGE = 3
    DEBUG_KEY = 4
    NO_OP = 5
    AND = 6
    OR = 7
    XOR = 8
    APPEND_IF_FITS = 9
    AVAILABLE_FOR_REUSE = 10
    RESERVED_FOR_LOG_PROTOCOL_MESSAGE = 11
    MAX = 12
    MIN = 13
    SET_VERSION_STAMPED_KEY = 14
    SET_VERSION_STAMPED_VALUE = 15


class Mutation(object):
    def __init__(self, code, param_one, param_two):
        self.code = MutationType(code)
        self.param_one = param_one
        self.param_two = param_two


class BaseInfo(object):
    def __init__(self, bb, protocol_version):
        self.start_timestamp = bb.get_double()
        if protocol_version >= PROTOCOL_VERSION_6_3:
            self.dc_id = bb.get_bytes_with_length()

class GetVersionInfo(BaseInfo):
    def __init__(self, bb, protocol_version):
        super().__init__(bb, protocol_version)
        self.latency = bb.get_double()
        if protocol_version >= PROTOCOL_VERSION_6_2:
            self.transaction_priority_type = bb.get_int()
        if protocol_version >= PROTOCOL_VERSION_6_3:
            self.read_version = bb.get_long()

class GetInfo(BaseInfo):
    def __init__(self, bb, protocol_version):
        super().__init__(bb, protocol_version)
        self.latency = bb.get_double()
        self.value_size = bb.get_int()
        self.key = bb.get_bytes_with_length()


class GetRangeInfo(BaseInfo):
    def __init__(self, bb, protocol_version):
        super().__init__(bb, protocol_version)
        self.latency = bb.get_double()
        self.range_size = bb.get_int()
        self.key_range = bb.get_key_range()


class CommitInfo(BaseInfo):
    def __init__(self, bb, protocol_version, full_output=True):
        super().__init__(bb, protocol_version)
        self.latency = bb.get_double()
        self.num_mutations = bb.get_int()
        self.commit_bytes = bb.get_int()

        if protocol_version >= PROTOCOL_VERSION_6_3:
            self.commit_version = bb.get_long()
        read_conflict_range = bb.get_key_range_list()
        if full_output:
            self.read_conflict_range = read_conflict_range
        write_conflict_range = bb.get_key_range_list()
        if full_output:
            self.write_conflict_range = write_conflict_range
        mutations = bb.get_mutation_list()
        if full_output:
            self.mutations = mutations

        self.read_snapshot_version = bb.get_long()


class ErrorGetInfo(BaseInfo):
    def __init__(self, bb, protocol_version):
        super().__init__(bb, protocol_version)
        self.error_code = bb.get_int()
        self.key = bb.get_bytes_with_length()


class ErrorGetRangeInfo(BaseInfo):
    def __init__(self, bb, protocol_version):
        super().__init__(bb, protocol_version)
        self.error_code = bb.get_int()
        self.key_range = bb.get_key_range()


class ErrorCommitInfo(BaseInfo):
    def __init__(self, bb, protocol_version, full_output=True):
        super().__init__(bb, protocol_version)
        self.error_code = bb.get_int()

        read_conflict_range = bb.get_key_range_list()
        if full_output:
            self.read_conflict_range = read_conflict_range
        write_conflict_range = bb.get_key_range_list()
        if full_output:
            self.write_conflict_range = write_conflict_range
        mutations = bb.get_mutation_list()
        if full_output:
            self.mutations = mutations

        self.read_snapshot_version = bb.get_long()


class UnsupportedProtocolVersionError(Exception):
    def __init__(self, protocol_version):
        super().__init__("Unsupported protocol version 0x%0.2X" % protocol_version)


class ClientTransactionInfo:
    def __init__(self, bb, full_output=True, type_filter=None):
        self.get_version = None
        self.gets = []
        self.get_ranges = []
        self.commit = None
        self.error_gets = []
        self.error_get_ranges = []
        self.error_commits = []

        protocol_version = bb.get_long()
        if protocol_version not in supported_protocol_versions:
            raise UnsupportedProtocolVersionError(protocol_version)
        while bb.get_remaining_bytes():
            event = bb.get_int()
            if event == 0:
                # we need to read it to consume the buffer even if we don't want to store it
                get_version = GetVersionInfo(bb, protocol_version)
                if (not type_filter or "get_version" in type_filter):
                    self.get_version = get_version
            elif event == 1:
                get = GetInfo(bb, protocol_version)
                if (not type_filter or "get" in type_filter):
                    # because of the crappy json serializtion using __dict__ we have to set the list here otherwise
                    # it doesn't print
                    if not self.gets: self.gets = []
                    self.gets.append(get)
            elif event == 2:
                get_range = GetRangeInfo(bb, protocol_version)
                if (not type_filter or "get_range" in type_filter):
                    if not self.get_ranges: self.get_ranges = []
                    self.get_ranges.append(get_range)
            elif event == 3:
                commit = CommitInfo(bb, protocol_version, full_output=full_output)
                if (not type_filter or "commit" in type_filter):
                    self.commit = commit
            elif event == 4:
                error_get = ErrorGetInfo(bb, protocol_version)
                if (not type_filter or "error_gets" in type_filter):
                    if not self.error_gets: self.error_gets = []
                    self.error_gets.append(error_get)
            elif event == 5:
                error_get_range = ErrorGetRangeInfo(bb, protocol_version)
                if (not type_filter or "error_get_range" in type_filter):
                    if not self.error_get_ranges: self.error_get_ranges = []
                    self.error_get_ranges.append(error_get_range)
            elif event == 6:
                error_commit = ErrorCommitInfo(bb, protocol_version, full_output=full_output)
                if (not type_filter or "error_commit" in type_filter):
                    if not self.error_commits: self.error_commits = []
                    self.error_commits.append(error_commit)
            else:
                raise Exception("Unknown event type %d" % event)

    def has_types(self):
        return self.get_version or self.gets or self.get_ranges or self.commit or self.error_gets \
            or self.error_get_ranges or self.error_commits

    def to_json(self):
        return json.dumps(self, cls=ObjJsonEncoder, sort_keys=True)


class TransactionInfoLoader(object):
    max_num_chunks_to_store = 1000 # Each chunk would be 100 KB in size

    def __init__(self, db, full_output=True, type_filter=None, min_timestamp=None, max_timestamp=None):
        self.db = db
        self.full_output = full_output
        self.type_filter = type_filter
        self.min_timestamp = min_timestamp
        self.max_timestamp = max_timestamp
        '''
        Keys look like this
            FF               - 2 bytes \xff\x02
            SSSSSSSSSS       - 10 bytes Version Stamp
            RRRRRRRRRRRRRRRR - 16 bytes Transaction id
            NNNN             - 4 Bytes Chunk number
            TTTT             - 4 Bytes Total number of chunks
        '''
        sample_key = "FF/fdbClientInfo/client_latency/SSSSSSSSSS/RRRRRRRRRRRRRRRR/NNNNTTTT/"

        self.client_latency_start = b'\xff\x02/fdbClientInfo/client_latency/'
        self.client_latency_start_key_selector = fdb.KeySelector.first_greater_than(self.client_latency_start)
        self.client_latency_end_key_selector = fdb.KeySelector.first_greater_or_equal(strinc(self.client_latency_start))
        self.version_stamp_start_idx = sample_key.index('S')
        self.version_stamp_end_idx = sample_key.rindex('S')
        self.tr_id_start_idx = sample_key.index('R')
        self.tr_id_end_idx = sample_key.rindex('R')
        self.chunk_num_start_idx = sample_key.index('N')
        self.num_chunks_start_idx = sample_key.index('T')

        self.tr_info_map = {}
        self.num_chunks_stored = 0
        self.num_transactions_discarded = 0

    def _check_and_adjust_chunk_cache_size(self):
        if self.num_chunks_stored > self.max_num_chunks_to_store:
            c_list = self.tr_info_map.pop(next(iter(self.tr_info_map)))
            self.num_chunks_stored -= len(c_list)
            self.num_transactions_discarded += 1

    def parse_key(self, k):
        version_stamp_bytes = k[self.version_stamp_start_idx:self.version_stamp_end_idx + 1]
        tr_id = k[self.tr_id_start_idx:self.tr_id_end_idx + 1]
        num_chunks = struct.unpack(">i", k[self.num_chunks_start_idx:self.num_chunks_start_idx + 4])[0]
        chunk_num = struct.unpack(">i", k[self.chunk_num_start_idx:self.chunk_num_start_idx + 4])[0]
        return version_stamp_bytes, tr_id, num_chunks, chunk_num

    def get_key_prefix_for_version_stamp(self, version_stamp):
        return self.client_latency_start + struct.pack(">Q", version_stamp) + b'\x00\x00'

    @fdb.transactional
    def find_version_for_timestamp(self, tr, timestamp, start):
        """
        Uses Timekeeper to find the closest version to a timestamp.
        If start is True, will find the greatest version at or before timestamp.
        If start is False, will find the smallest version at or after the timestamp.

        :param tr:
        :param timestamp:
        :param start:
        :return:
        """
        tr.options.set_read_system_keys()
        tr.options.set_read_lock_aware()
        timekeeper_prefix = b'\xff\x02/timeKeeper/map/'
        timestamp_packed = fdb.tuple.pack((timestamp,))
        if start:
            start_key = timekeeper_prefix
            end_key = fdb.KeySelector.first_greater_than(timekeeper_prefix + timestamp_packed)
            reverse = True
        else:
            start_key = fdb.KeySelector.first_greater_or_equal(timekeeper_prefix + timestamp_packed)
            end_key = fdb.KeySelector.first_greater_or_equal(strinc(timekeeper_prefix))
            reverse = False
        for k, v in tr.snapshot.get_range(start_key, end_key, limit=1, reverse=reverse):
            return fdb.tuple.unpack(v)[0]
        return 0 if start else 0x8000000000000000 # we didn't find any timekeeper data so find the max range

    def fetch_transaction_info(self):
        if self.min_timestamp:
            start_version = self.find_version_for_timestamp(self.db, self.min_timestamp, True)
            logger.debug("Using start version %s" % start_version)
            start_key = self.get_key_prefix_for_version_stamp(start_version)
        else:
            start_key = self.client_latency_start_key_selector

        if self.max_timestamp:
            end_version = self.find_version_for_timestamp(self.db, self.max_timestamp, False)
            logger.debug("Using end version %s" % end_version)
            end_key = self.get_key_prefix_for_version_stamp(end_version)
        else:
            end_key = self.client_latency_end_key_selector

        transaction_infos = 0
        invalid_transaction_infos = 0

        def build_client_transaction_info(v):
            return ClientTransactionInfo(ByteBuffer(v), full_output=self.full_output, type_filter=self.type_filter)

        more = True
        tr = self.db.create_transaction()
        while more:
            tr.options.set_read_system_keys()
            tr.options.set_read_lock_aware()
            found = 0
            buffer = []
            try:
                logger.debug("Querying [%s:%s]" % (start_key, end_key))
                transaction_info_range = tr.snapshot.get_range(start_key, end_key,
                                                               streaming_mode=fdb.impl.StreamingMode.want_all)
                for k, v in transaction_info_range:
                    found += 1
                    #logger.debug(k)
                    start_key = fdb.KeySelector.first_greater_than(k)

                    _, tr_id, num_chunks, chunk_num = self.parse_key(k)

                    #logger.debug("num_chunks=%d, chunk_num=%d" % (num_chunks,chunk_num))

                    if num_chunks == 1:
                        assert chunk_num == 1
                        try:
                            info = build_client_transaction_info(v)
                            if info.has_types():
                                buffer.append(info)
                        except UnsupportedProtocolVersionError as e:
                            invalid_transaction_infos += 1
                        except ValueError:
                            invalid_transaction_infos += 1

                        transaction_infos += 1
                    else:
                        if chunk_num == 1:
                            # first chunk
                            assert tr_id not in self.tr_info_map
                            self.tr_info_map[tr_id] = [TrInfoChunk(num_chunks, chunk_num, k, v)]
                            self.num_chunks_stored += 1
                            self._check_and_adjust_chunk_cache_size()
                        else:
                            if tr_id not in self.tr_info_map:
                                logger.error("Got a middle chunk without getting beginning part. Discarding transaction id: %s\n" % tr_id)
                                continue
                            c_list = self.tr_info_map[tr_id]
                            if c_list[-1].num_chunks != num_chunks or c_list[-1].chunk_num != chunk_num - 1:
                                self.tr_info_map.pop(tr_id)
                                self.num_chunks_stored -= len(c_list)
                                raise Exception("Chunk numbers do not match for Transaction id: %s" % tr_id)
                            c_list.append(TrInfoChunk(num_chunks, chunk_num, k, v))
                            self.num_chunks_stored += 1
                            if num_chunks == chunk_num:
                                self.tr_info_map.pop(tr_id)
                                self.num_chunks_stored -= len(c_list)
                                try:
                                    info = build_client_transaction_info(b''.join([chunk.value for chunk in c_list]))
                                    if info.has_types():
                                        buffer.append(info)
                                except UnsupportedProtocolVersionError as e:
                                    invalid_transaction_infos += 1
                                except ValueError:
                                    invalid_transaction_infos += 1

                                transaction_infos += 1
                            self._check_and_adjust_chunk_cache_size()
                    if transaction_infos % 1000 == 0:
                        print("Processed %d transactions, %d invalid" % (transaction_infos, invalid_transaction_infos))
                if found == 0:
                    more = False
            except fdb.FDBError as e:
                # if too old then reset and don't wait
                if e.code == 1007:
                    tr.reset()
                else:
                    tr.on_error(e).wait()
            for item in buffer:
                yield item

        print("Processed %d transactions, %d invalid\n" % (transaction_infos, invalid_transaction_infos))


def has_sortedcontainers():
    try:
        import sortedcontainers
        return True
    except ImportError:
        logger.warn("Can't find sortedcontainers so disabling ReadCounter")
        return False


def has_dateparser():
    try:
        import dateparser
        return True
    except ImportError:
        logger.warn("Can't find dateparser so disabling human date parsing")
        return False

class ReadCounter(object):
    def __init__(self):
        from sortedcontainers import SortedDict
        self.reads = SortedDict()
        self.reads[b''] = [0, 0]

        self.read_counts = {}
        self.hit_count=0

    def process(self, transaction_info):
        for get in transaction_info.gets:
            self._insert_read(get.key, None)
        for get_range in transaction_info.get_ranges:
            self._insert_read(get_range.key_range.start_key, get_range.key_range.end_key)

    def _insert_read(self, start_key, end_key):
        self.read_counts.setdefault((start_key, end_key), 0)
        self.read_counts[(start_key, end_key)] += 1

        self.reads.setdefault(start_key, [0, 0])[0] += 1
        if end_key is not None:
            self.reads.setdefault(end_key, [0, 0])[1] += 1
        else:
            self.reads.setdefault(start_key+b'\x00', [0, 0])[1] += 1

    def get_total_reads(self):
        return sum([v for v in self.read_counts.values()])
    
    def matches_filter(addresses, required_addresses):
        for addr in required_addresses:
            if addr not in addresses:
                return False
        return True

    def get_top_k_reads(self, num, filter_addresses, shard_finder=None):
        count_pairs = sorted([(v, k) for (k, v) in self.read_counts.items()], reverse=True, key=lambda item: item[0])
        if not filter_addresses:
            count_pairs = count_pairs[0:num]

        if shard_finder:
            results = []
            for (count, (start, end)) in count_pairs:
                results.append((start, end, count, shard_finder.get_addresses_for_key(start)))

            shard_finder.wait_for_shard_addresses(results, 0, 3)

            if filter_addresses:
                filter_addresses = set(filter_addresses)
                results = [r for r in results if filter_addresses.issubset(set(r[3]))][0:num]
        else:
            results = [(start, end, count) for (count, (start, end)) in count_pairs[0:num]]

        return results

    def get_range_boundaries(self, num_buckets, shard_finder=None):
        total = sum([start_count for (start_count, end_count) in self.reads.values()])
        range_size = total // num_buckets
        output_range_counts = []

        if total == 0:
            return output_range_counts

        def add_boundary(start, end, started_count, total_count):
            if shard_finder:
                shard_count = shard_finder.get_shard_count(start, end)
                if shard_count == 1:
                    addresses = shard_finder.get_addresses_for_key(start)
                else:
                    addresses = None
                output_range_counts.append((start, end, started_count, total_count, shard_count, addresses))
            else:
                output_range_counts.append((start, end, started_count, total_count, None, None))

        this_range_start_key = None
        last_end = None
        open_count = 0
        opened_this_range = 0
        count_this_range = 0

        for (start_key, (start_count, end_count)) in self.reads.items():
            open_count -= end_count

            if opened_this_range >= range_size:
                add_boundary(this_range_start_key, start_key, opened_this_range, count_this_range)
                count_this_range = open_count
                opened_this_range = 0
                this_range_start_key = None

            count_this_range += start_count
            opened_this_range += start_count
            open_count += start_count

            if count_this_range > 0 and this_range_start_key is None:
                this_range_start_key = start_key

            if end_count > 0:
                last_end = start_key

        if last_end is None:
            last_end = b'\xff'
        if count_this_range > 0:
            add_boundary(this_range_start_key, last_end, opened_this_range, count_this_range)

        shard_finder.wait_for_shard_addresses(output_range_counts, 0, 5)
        return output_range_counts


class ShardFinder(object):
    def __init__(self, db, exclude_ports):
        self.db = db
        self.exclude_ports = exclude_ports

        self.tr = db.create_transaction()
        self.refresh_tr()

        self.outstanding = []
        self.boundary_keys = list(fdb.locality.get_boundary_keys(db, b'', b'\xff\xff'))
        self.shard_cache = {}

    def _get_boundary_keys(self, begin, end):
        start_pos = max(0, bisect_right(self.boundary_keys, begin)-1)
        end_pos = max(0, bisect_right(self.boundary_keys, end)-1)

        return self.boundary_keys[start_pos:end_pos]

    def refresh_tr(self):
        self.tr.options.set_read_lock_aware()
        if not self.exclude_ports:
            self.tr.options.set_include_port_in_address()

    @staticmethod
    def _get_addresses_for_key(tr, key):
        return fdb.locality.get_addresses_for_key(tr, key)

    def get_shard_count(self, start_key, end_key):
        return len(self._get_boundary_keys(start_key, end_key)) + 1

    def get_addresses_for_key(self, key):
        shard = self.boundary_keys[max(0, bisect_right(self.boundary_keys, key)-1)]
        do_load = False
        if not shard in self.shard_cache:
            do_load = True
        elif self.shard_cache[shard].is_ready():
            try:
                self.shard_cache[shard].wait()
            except fdb.FDBError as e:
                self.tr.on_error(e).wait()
                self.refresh_tr()
                do_load = True

        if do_load:
            if len(self.outstanding) > 1000:
                for f in self.outstanding:
                    try:
                        f.wait()
                    except fdb.FDBError as e:
                        pass

                self.outstanding = []
                self.tr.reset()
                self.refresh_tr()

            self.outstanding.append(self._get_addresses_for_key(self.tr, shard))
            self.shard_cache[shard] = self.outstanding[-1]

        return self.shard_cache[shard]

    def wait_for_shard_addresses(self, ranges, key_idx, addr_idx):
        for index in range(len(ranges)):
            item = ranges[index]
            if item[addr_idx] is not None:
                while True:
                    try:
                        ranges[index] = item[0:addr_idx] + ([a.decode('ascii') for a in item[addr_idx].wait()],) + item[addr_idx+1:]
                        break
                    except fdb.FDBError as e:
                        ranges[index] = item[0:addr_idx] + (self.get_addresses_for_key(item[key_idx]),) + item[addr_idx+1:]

class WriteCounter(object):
    mutation_types_to_consider = frozenset([MutationType.SET_VALUE, MutationType.ADD_VALUE])

    def __init__(self):
        self.writes = defaultdict(lambda: 0)

    def process(self, transaction_info):
        if transaction_info.commit:
            for mutation in transaction_info.commit.mutations:
                if mutation.code in self.mutation_types_to_consider:
                    self.writes[mutation.param_one] += 1

    def get_range_boundaries(self, num_buckets, shard_finder=None):
        total = sum([v for (k, v) in self.writes.items()])
        range_size = total // num_buckets
        key_counts_sorted = sorted(self.writes.items())
        output_range_counts = []

        def add_boundary(start, end, count):
            if shard_finder:
                shard_count = shard_finder.get_shard_count(start, end)
                if shard_count == 1:
                    addresses = shard_finder.get_addresses_for_key(start)
                else:
                    addresses = None
                output_range_counts.append((start, end, count, None, shard_count, addresses))
            else:
                output_range_counts.append((start, end, count, None, None, None))

        start_key = None
        count_this_range = 0
        for (k, v) in key_counts_sorted:
            if not start_key:
                start_key = k
            count_this_range += v
            if count_this_range >= range_size:
                add_boundary(start_key, k, count_this_range)
                count_this_range = 0
                start_key = None
        if count_this_range > 0:
            add_boundary(start_key, k, count_this_range)

        shard_finder.wait_for_shard_addresses(output_range_counts, 0, 5)
        return output_range_counts

    def get_total_writes(self):
        return sum([v for v in self.writes.values()])

    def get_top_k_writes(self, num, filter_addresses, shard_finder=None):
        count_pairs = sorted([(v, k) for (k, v) in self.writes.items()], reverse=True)
        if not filter_addresses:
            count_pairs = count_pairs[0:num]

        if shard_finder:
            results = []
            for (count, key) in count_pairs:
                results.append((key, None, count, shard_finder.get_addresses_for_key(key)))

            shard_finder.wait_for_shard_addresses(results, 0, 3)

            if filter_addresses:
                filter_addresses = set(filter_addresses)
                results = [r for r in results if filter_addresses.issubset(set(r[3]))][0:num]
        else:
            results = [(key, end, count) for (count, key) in count_pairs[0:num]]

        return results

def connect(cluster_file=None):
    db = fdb.open(cluster_file=cluster_file)
    return db


def main():
    parser = argparse.ArgumentParser(description="TransactionProfilingAnalyzer")
    parser.add_argument("-C", "--cluster-file", type=str, help="Cluster file")
    parser.add_argument("--full-output", action="store_true", help="Print full output from mutations")
    parser.add_argument("--filter-get-version", action="store_true",
                        help="Include get_version type. If no filter args are given all will be returned.")
    parser.add_argument("--filter-get", action="store_true",
                        help="Include get type. If no filter args are given all will be returned.")
    parser.add_argument("--filter-get-range", action="store_true",
                        help="Include get_range type. If no filter args are given all will be returned.")
    parser.add_argument("--filter-reads", action="store_true",
                        help="Include get and get_range type. If no filter args are given all will be returned.")
    parser.add_argument("--filter-commit", action="store_true",
                        help="Include commit type. If no filter args are given all will be returned.")
    parser.add_argument("--filter-error-get", action="store_true",
                        help="Include error_get type. If no filter args are given all will be returned.")
    parser.add_argument("--filter-error-get-range", action="store_true",
                        help="Include error_get_range type. If no filter args are given all will be returned.")
    parser.add_argument("--filter-error-commit", action="store_true",
                        help="Include error_commit type. If no filter args are given all will be returned.")
    start_time_group = parser.add_mutually_exclusive_group()
    start_time_group.add_argument("--min-timestamp", type=int, help="Don't return events older than this epoch time")
    start_time_group.add_argument("-s", "--start-time", type=str,
                                  help="Don't return events older than this parsed time")
    end_time_group = parser.add_mutually_exclusive_group()
    end_time_group.add_argument("--max-timestamp", type=int, help="Don't return events newer than this epoch time")
    end_time_group.add_argument("-e", "--end-time", type=str, help="Don't return events older than this parsed time")
    parser.add_argument("--num-buckets", type=int, help="The number of buckets to partition the key-space into for operation counts", default=100)
    parser.add_argument("--top-requests", type=int, help="If specified will output this many top keys for reads or writes", default=0)
    parser.add_argument("--exclude-ports", action="store_true", help="Print addresses without the port number. Only works in versions older than 6.3, and is required in versions older than 6.2.")
    parser.add_argument("--single-shard-ranges-only", action="store_true", help="Only print range boundaries that exist in a single shard")
    parser.add_argument("-a", "--filter-address", action="append", help="Only print range boundaries that include the given address. This option can used multiple times to include more than one address in the filter, in which case all addresses must match.")

    args = parser.parse_args()

    type_filter = set()
    if args.filter_get_version: type_filter.add("get_version")
    if args.filter_get or args.filter_reads: type_filter.add("get")
    if args.filter_get_range or args.filter_reads: type_filter.add("get_range")
    if args.filter_commit: type_filter.add("commit")
    if args.filter_error_get: type_filter.add("error_get")
    if args.filter_error_get_range: type_filter.add("error_get_range")
    if args.filter_error_commit: type_filter.add("error_commit")

    if (not type_filter or "commit" in type_filter):
        write_counter = WriteCounter() if args.num_buckets else None
    else:
        write_counter = None

    if (not type_filter or "get" in type_filter or "get_range" in type_filter):
        read_counter = ReadCounter() if (has_sortedcontainers() and args.num_buckets) else None
    else:
        read_counter = None

    full_output = args.full_output or (args.num_buckets is not None)

    if args.min_timestamp:
        min_timestamp = args.min_timestamp
    elif args.start_time:
        if not has_dateparser():
            raise Exception("Can't find dateparser needed to parse human dates")
        import dateparser
        min_timestamp = int(dateparser.parse(args.start_time).timestamp())
    else:
        raise Exception("Must specify start time")

    if args.max_timestamp:
        max_timestamp = args.max_timestamp
    elif args.end_time:
        if not has_dateparser():
            raise Exception("Can't find dateparser needed to parse human dates")
        import dateparser
        max_timestamp = int(dateparser.parse(args.end_time).timestamp())
    else:
        raise Exception("Must specify end time")

    now = time.time()
    if max_timestamp > now:
        raise Exception("max_timestamp is %d seconds in the future" % (max_timestamp - now))
    if min_timestamp > now:
        raise Exception("min_timestamp is %d seconds in the future" % (min_timestamp - now))

    logger.info("Loading transactions from %d to %d" % (min_timestamp, max_timestamp))

    db = connect(cluster_file=args.cluster_file)
    loader = TransactionInfoLoader(db, full_output=full_output, type_filter=type_filter,
                                   min_timestamp=min_timestamp, max_timestamp=max_timestamp)

    for info in loader.fetch_transaction_info():
        if info.has_types():
            if not write_counter and not read_counter:
                print(info.to_json())
            else:
                if write_counter:
                    write_counter.process(info)
                if read_counter:
                    read_counter.process(info)

    def print_top(top, total, context):
        if top:
            running_count = 0
            for (idx, (start, end, count, addresses)) in enumerate(top):
                running_count += count
                if end is not None:
                    op_str = 'Range %r - %r' % (start, end)
                else:
                    op_str = 'Key %r' % start

                print(" %d. %s\n    %d sampled %s (%.2f%%, %.2f%% cumulative)" % (idx+1, op_str, count, context, 100*count/total, 100*running_count/total))
                print("    shard addresses: %s\n" % ", ".join(addresses))

        else:
            print(" No %s found" % context)

    def print_range_boundaries(range_boundaries, context):
        omit_start = None
        for (idx, (start, end, start_count, total_count, shard_count, addresses)) in enumerate(range_boundaries):
            omit = args.single_shard_ranges_only and shard_count is not None and shard_count > 1
            if args.filter_address:
                if not addresses:
                    omit = True
                else:
                    for addr in args.filter_address:
                        if addr not in addresses:
                            omit = True
                            break

            if not omit:
                if omit_start is not None:
                    if omit_start == idx-1:
                        print(" %d. Omitted\n" % (idx))
                    else:
                        print(" %d - %d. Omitted\n" % (omit_start+1, idx))
                    omit_start = None

                if total_count is None:
                    count_str = '%d sampled %s' % (start_count, context)
                else:
                    count_str = '%d sampled %s (%d intersecting)' % (start_count, context, total_count)
                if not shard_count:
                    print(" %d. [%s, %s]\n     %d sampled %s\n" % (idx+1, start, end, count, context))
                else:
                    addresses_string = "; addresses=%s" % ', '.join(addresses) if addresses else ''
                    print(" %d. [%s, %s]\n     %s spanning %d shard(s)%s\n" % (idx+1, start, end, count_str, shard_count, addresses_string))
            elif omit_start is None:
                omit_start = idx

        if omit_start is not None:
            if omit_start == len(range_boundaries)-1:
                print(" %d. Omitted\n" % len(range_boundaries))
            else:
                print(" %d - %d. Omitted\n" % (omit_start+1, len(range_boundaries)))

    shard_finder = ShardFinder(db, args.exclude_ports)

    print("NOTE: shard locations are current and may not reflect where an operation was performed in the past\n")

    if write_counter:
        if args.top_requests:
            top_writes = write_counter.get_top_k_writes(args.top_requests, args.filter_address, shard_finder=shard_finder)

        range_boundaries = write_counter.get_range_boundaries(args.num_buckets, shard_finder=shard_finder)
        num_writes = write_counter.get_total_writes()

        if args.top_requests or range_boundaries:
            print("WRITES")
            print("------\n")
            print("Processed %d total writes\n" % num_writes)

        if args.top_requests:
            suffix = ""
            if args.filter_address:
                suffix = " (%s)" % ", ".join(args.filter_address)
            print("Top %d writes%s:\n" % (args.top_requests, suffix))

            print_top(top_writes, write_counter.get_total_writes(), "writes")
            print("")

        if range_boundaries:
            print("Key-space boundaries with approximately equal mutation counts:\n")
            print_range_boundaries(range_boundaries, "writes")

        if args.top_requests or range_boundaries:
            print("")

    if read_counter:
        if args.top_requests:
            top_reads = read_counter.get_top_k_reads(args.top_requests, args.filter_address, shard_finder=shard_finder)

        range_boundaries = read_counter.get_range_boundaries(args.num_buckets, shard_finder=shard_finder)
        num_reads = read_counter.get_total_reads()

        if args.top_requests or range_boundaries:
            print("READS")
            print("-----\n")
            print("Processed %d total reads\n" % num_reads)

        if args.top_requests:
            suffix = ""
            if args.filter_address:
                suffix = " (%s)" % ", ".join(args.filter_address)
            print("Top %d reads%s:\n" % (args.top_requests, suffix))

            print_top(top_reads, num_reads, "reads")
            print("")

        if range_boundaries:
            print("Key-space boundaries with approximately equal read counts:\n")
            print_range_boundaries(range_boundaries, "reads")

if __name__ == "__main__":
    main()
