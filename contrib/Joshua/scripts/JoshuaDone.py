#!/usr/bin/env python3

import argparse
import datetime
from typing import List

import fdb
import fdb.tuple
import xml.sax
import sys
import json
import hashlib
import time
import uuid
import struct
import os
import shutil
import random
from enum import Enum
from itertools import chain

fdb.api_version(630)


class CodeProbe:
    def __init__(self, filename, line, comment):
        self.filename = filename
        self.line = line
        self.comment = comment
        self.hit_count = 0

    def debug(self, offset=''):
        print("{}({}:{}): {}".format(offset, self.filename, self.line, self.comment))

    def key(self):
        m = hashlib.sha256()
        m.update("foo".encode())
        m.update(self.line.encode())
        m.update(self.comment.encode())
        return m.digest()

    def value(self):
        res = {'File': self.filename, 'Line': self.line, 'Comment': self.comment}
        return json.dumps(res).encode()

    @staticmethod
    def from_json(j):
        obj = json.loads(j.decode())
        return CodeProbe(obj['File'], obj['Line'], obj['Comment'])


class LockState(Enum):
    RETRY = 1
    TRY_LOCK = 2
    MINE = 3
    DONE = 4


class Lease:
    def __init__(self):
        self.done = False
        self.owner = None
        self.lease_time = None
        pass

    @staticmethod
    def from_owner(owner):
        self = Lease()
        self.done = False
        self.owner = owner
        self.lease_time = datetime.datetime.now().timestamp()
        return self

    def to_tuple(self):
        assert self.done or (self.owner is not None and self.lease_time is not None)
        return self.done, self.owner, self.lease_time

    @staticmethod
    def from_tuple(t):
        self = Lease()
        self.done = t[0]
        self.owner = t[1]
        self.lease_time = t[2]
        return self


class CodeProbeWriter(xml.sax.ContentHandler):
    def __init__(self, cluster_file, ensemble_id : str):
        self.db = fdb.open(cluster_file)
        self.ensemble_id = ensemble_id
        self.root_dir = fdb.directory.create_or_open(self.db, 'code-probes')
        self.ensemble_dir = self.root_dir.create_or_open(self.db, ensemble_id)
        self.counter_dir = self.ensemble_dir.create_or_open(self.db, 'counters')
        self.data_dir = self.ensemble_dir.create_or_open(self.db, 'data')
        self.missedProbes: List[CodeProbe] = []
        self.hitProbes: List[CodeProbe] = []
        self.initializing = False
        self.myID = uuid.uuid4()

    def read_missing_from_db(self):
        probes = self.read_all(self.db)
        for probe in probes:
            if probe.hit_count > 0:
                continue
            print("No hits for File: {}, Line: {}, Comment: \"{}\"".format(probe.filename,
                                                                           probe.line,
                                                                           probe.comment))

    def list(self):
        probes = self.read_all(self.db)
        probes.sort(key=(lambda x: x.hit_count), reverse=True)
        for probe in probes:
            print('{} hits for File: {}, Line: {}, Comment: \"{}\"'.format(probe.hit_count,
                                                                           probe.filename,
                                                                           probe.line,
                                                                           probe.comment))

    @fdb.transactional
    def read_all(self, tr) -> List[CodeProbe]:
        res = []
        pairs = {}
        for key, value in tr[self.data_dir.range()]:
            k = self.data_dir.unpack(key)
            pairs[k] = CodeProbe.from_json(value)
        for key, value in tr[self.counter_dir.range()]:
            k = self.counter_dir.unpack(key)
            probe = pairs[k]
            probe.hit_count = struct.unpack('<I', value)[0]
            res.append(probe)
        return res

    def parse_summary(self, summary):
        path = os.path.abspath(summary)
        xml.sax.parse(path, self)

    def startElement(self, name, attrs):
        if name == "CodeCoverage":
            comment = ''
            if 'Comment' in attrs:
                comment = attrs.getValue('Comment')
            probe = CodeProbe(attrs.getValue("File"), attrs.getValue("Line"), comment)
            covered = True
            if "Covered" in attrs:
                covered = attrs.getValue("Covered") != "0"
            if covered:
                self.hitProbes.append(probe)
            else:
                self.missedProbes.append(probe)

    def print_probes(self):
        print("hit probes:")
        print("===========")
        for probe in self.hitProbes:
            probe.debug('\t')
        print("missed probes:")
        print("==============")
        for probe in self.missedProbes:
            probe.debug('\t')

    def report(self, ensemble_id):
        ensemble_dir = self.root_dir.create_or_open(self.db, ensemble_id)
        self.init_state()
        self.increment_counters()

    def hit_generator(self):
        for probe in self.hitProbes:
            yield probe.key()

    @fdb.transactional
    def increment_counters_of_frame(self, tr, frame):
        for key in frame:
            tr.add(self.counter_dir[key], struct.pack('<I', 1))

    def increment_counters(self):
        current_frame = []
        for probe in self.hit_generator():
            if len(current_frame) >= 100:
                self.increment_counters_of_frame(self.db, current_frame)
                current_frame = []
            current_frame.append(probe)
        if len(current_frame) > 0:
            self.increment_counters_of_frame(self.db, current_frame)

    def refresh_lock(self, tr, dir):
        val = tr[dir["init"]]
        if val.present():
            lease = Lease.from_tuple(fdb.tuple.unpack(val))
            now = datetime.datetime.now().now().timestamp()
            if lease.done:
                return LockState.DONE
            elif lease.owner == self.myID:
                lease.lease_time = now
                tr[dir["init"]] = fdb.tuple.pack(lease.to_tuple())
                return LockState.MINE
            elif lease.lease_time + 5.0 < now:
                lease.owner = self.myID
                lease.lease_time = now
                tr[dir["init"]] = fdb.tuple.pack(lease.to_tuple())
                return LockState.TRY_LOCK
            else:
                time.sleep(1.0)
                return LockState.RETRY
        else:
            tr[dir["init"]] = fdb.tuple.pack(Lease.from_owner(self.myID).to_tuple())
            return LockState.TRY_LOCK

    def list_generator(self):
        for probe in chain(self.missedProbes, self.hitProbes):
            yield probe

    def init_state(self):
        tr = self.db.create_transaction()
        generator = self.list_generator()
        current_frame = []
        while True:
            try:
                lock_state = self.refresh_lock(tr, self.ensemble_dir)
                if lock_state == LockState.DONE:
                    return
                elif lock_state == LockState.TRY_LOCK:
                    tr.commit().wait()
                    tr = self.db.create_transaction()
                    continue
                elif lock_state == LockState.RETRY:
                    continue
                assert lock_state == LockState.MINE
                done = True
                count = 0
                if len(current_frame) == 0:
                    for p in generator:
                        if count > 100:
                            done = False
                        count += 1
                        current_frame.append(p)
                for p in current_frame:
                    tr[self.data_dir[p.key()]] = p.value()
                    tr[self.counter_dir[p.key()]] = struct.pack('<I', 0)
                if done:
                    lease = Lease()
                    lease.done = True
                    tr[self.ensemble_dir["init"]] = fdb.tuple.pack(lease.to_tuple())
                tr.commit().wait()
                current_frame = []
                if done:
                    return
                else:
                    tr = self.db.create_transaction()
            except fdb.FDBError as e:
                tr.on_error(e).wait()


if __name__ == "__main__":
    print("JoshuaDone: {}".format(' '.join(sys.argv)))
    parser = argparse.ArgumentParser(description='Code Probe Accumulation')
    parser.add_argument('-C', '--cluster-file', type=str, help='Path to the cluster file')
    parser.add_argument('-f', '--summary-file', type=str, help='Path to summary file')
    parser.add_argument('-r', '--return-code', type=int, help='The return code of the test to be processed')
    parser.add_argument('command', choices=['accumulate', 'summarize', 'list'])
    parser.add_argument('ensemble_id')
    args = parser.parse_args()

    writer = CodeProbeWriter(args.cluster_file, args.ensemble_id)
    if args.command == 'summarize':
        writer.read_missing_from_db()
    elif args.command == 'list':
        writer.list()
    elif args.command == 'accumulate':
        if args.return_code is not None and args.return_code != 0:
            sys.exit(0)
        if args.summary_file is None:
            print("Error: no summary file")
            sys.exit(1)
        writer.parse_summary(args.summary_file)
        writer.report(args.ensemble_id)
    else:
        print("Unknown command: {}".format(args.command))
        assert False

    # if len(sys.argv) < 5 or len(sys.argv) > 6:
    #     print("Wrong number of arguments -- got {}".format(len(sys.argv)))
    # else:
    #     ensemble = sys.argv[1]
    #     # we currently don't use the retcode and the seed
    #     # seed = sys.argv[2]
    #     # retcode = sys.argv[3]
    #     summary = sys.argv[4]
    #     clusterFile = None
    #     if len(sys.argv) > 5:
    #         clusterFile = sys.argv[5]
    #
    #     writer = CodeProbeWriter(clusterFile)
    #     writer.parse_summary(summary)
