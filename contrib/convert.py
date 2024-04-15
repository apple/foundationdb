#!/usr/bin/env python

import argparse
import fdb
import time
from datetime import datetime

fdb.api_version(730)

db = fdb.open(cluster_file="/data/v8/fdb/playstation_carnivalfdbserver/000000265/st.cluster")

@fdb.transactional
def find_version_for_timestamp(tr, timestamp, start):
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

@fdb.transactional
def find_timestamp_for_version(tr, version):
    """
    Uses Timekeeper to find the closest timestamp for a version via binary search
    :param tr:
    :param timestamp:
    :return:
    """
    tr.options.set_read_system_keys()
    tr.options.set_read_lock_aware()
    timekeeper_prefix = b'\xff\x02/timeKeeper/map/'
    timekeeper_prefix_end = b'\xff\x02/timeKeeper/map0'
    prefix_len = len(timekeeper_prefix)
    ts_max = 0
    ts_min = 0
    version_per_sec = 1e6
    exact_match = False
    ts_final = 0
    version_final = 0
    start_key = fdb.KeySelector.first_greater_than(timekeeper_prefix)
    end_key = fdb.KeySelector.last_less_than(timekeeper_prefix_end) # getRange is inclusive
    # check if too small
    for k, v in tr.snapshot.get_range(start_key, end_key, limit=1, reverse=False):
        k = k[prefix_len:] # exclude the prefix
        ts_cur = int(fdb.tuple.unpack(k)[0])
        version_cur = int(fdb.tuple.unpack(v)[0])
        if version_cur > version:
            print ("Version is smaller than anything recorded, estimates might not be accurate. ")
            return ts_cur + (version - version_cur) / version_per_sec
        ts_max = ts_cur

    # check if too large
    for k, v in tr.snapshot.get_range(start_key, end_key, limit=1, reverse=True):
        k = k[prefix_len:] # exclude the prefix
        ts_cur = int(fdb.tuple.unpack(k)[0])
        version_cur = int(fdb.tuple.unpack(v)[0])
        if version_cur < version:
            print ("Version is bigger than anything recorded, estimates might not be accurate. ")
            return ts_cur + (version - version_cur) / version_per_sec
        ts_min = ts_cur

    while ts_min < ts_max:
        ts_mid = int((ts_min + ts_max + 1) / 2)
        min_packed = fdb.tuple.pack((ts_min,))
        mid_packed = fdb.tuple.pack((ts_mid,))
        # print ("min {} mid {} max {}".format(ts_min, ts_mid, ts_max))
        start_key = fdb.KeySelector.first_greater_or_equal(timekeeper_prefix + min_packed)
        end_key = fdb.KeySelector.last_less_or_equal(timekeeper_prefix + mid_packed)

        for k, v in tr.snapshot.get_range(start_key, end_key, limit=1, reverse=True):
            k = k[prefix_len:] # exclude the prefix
            ts_cur = int(fdb.tuple.unpack(k)[0])
            version_cur = int(fdb.tuple.unpack(v)[0])
            if version < version_cur:
                ts_max = ts_cur - 1
            elif version > version_cur:
                ts_min = ts_cur + 1
            else:
                exact_match = True
        if exact_match:
            print ("Saw the exact timestamp")
            break

    # try to give best estimates despite the corresponding time is out of range
    return ts_cur + (version - version_cur) / version_per_sec

def getVersion(args):
    print("Executing getVersion with startTime: {}, endTime: {}, mutationTime: {}".format(args.startTime, args.endTime, args.mutationTime))

    startTime = int(args.startTime)
    endTime = int(args.endTime)
    mutationTime = int(args.mutationTime)
    start_version = find_version_for_timestamp(db, startTime, True)
    end_version = find_version_for_timestamp(db, endTime , True)
    m_version = find_version_for_timestamp(db, mutationTime , True)

    print("Start Version: " + str(start_version))
    print("End Version: " + str(end_version))
    print("Mutation Version: " + str(m_version))

def getTime(args):
    # binary search
    print("Executing getTime with version {}".format(args.version))

    v = int(args.version)
    ts = int(find_timestamp_for_version(db, v))

    print("timestamp: " + str(ts))
    print("local time:" + datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

def main():
    parser = argparse.ArgumentParser(description="My Command Line Tool")
    subparsers = parser.add_subparsers(title="commands", dest="command")

    parser_cmd1 = subparsers.add_parser("getVersion", help="given 3 timestamps, get versions for them")
    parser_cmd1.add_argument("startTime", help="start version")
    parser_cmd1.add_argument("endTime", help="end version")
    parser_cmd1.add_argument("mutationTime", help="mutation version")

    parser_cmd2 = subparsers.add_parser("getTime", help="given a version, get unix and human readable time for it")
    parser_cmd2.add_argument("version", help="version to check")

    args = parser.parse_args()

    if args.command == "getVersion":
        getVersion(args)
    elif args.command == "getTime":
        getTime(args)

if __name__ == "__main__":
    main()