#!/usr/bin/env python

# this script can convert version to an estimated timestamp for a cluster,
# or convert a timestamp to an estimated version.

import argparse
import fdb
import time
from datetime import datetime

fdb.api_version(630)

db = fdb.open(cluster_file="/var/dynamic-conf/fdb.cluster")

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
    prefix_len = len(timekeeper_prefix)
    max_ts = int(time.time()) # current time
    min_ts = int(max_ts - 5e7) # retrive up to 578 days
    version_per_sec = 1e6
    get_range_has_result = False
    exact_match = False
    while min_ts < max_ts:
        mid_ts = int((min_ts + max_ts + 1) / 2)
        min_packed = fdb.tuple.pack((min_ts,))
        mid_packed = fdb.tuple.pack((mid_ts,))
        # print ("min {} mid {} max {}".format(min_ts, mid_ts, max_ts))
        start_key = fdb.KeySelector.first_greater_or_equal(timekeeper_prefix + min_packed)
        end_key = fdb.KeySelector.first_greater_than(timekeeper_prefix + mid_packed)

        for k, v in tr.snapshot.get_range(start_key, end_key, limit=1, reverse=True):
            get_range_has_result = True
            k = k[prefix_len:] # exclude the prefix
            k_unpacked = int(fdb.tuple.unpack(k)[0])
            v_unpacked = int(fdb.tuple.unpack(v)[0])
            if version < v_unpacked:
                max_ts = k_unpacked - 1
            elif version > v_unpacked:
                min_ts = k_unpacked + 1
            else:
                exact_match = True
        if not get_range_has_result:
            print ("Cannot find the exact timestamp, will give best estimates")
            break
        if exact_match:
            print ("Saw the exact timestamp")
            break
        get_range_has_result = False

    # try to give best estimates despite the corresponding time is out of range
    return k_unpacked + (version - v_unpacked) / version_per_sec

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
    print("GMT time:" + datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

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
