#!/usr/bin/env python3
import base64
import fdb
import sys
import argparse
from fdb.tuple import pack, unpack
from fdb.impl import StreamingMode
import os
import json
import subprocess
import time
import uuid
from collections import defaultdict
import multiprocessing
from datetime import datetime
import ast

fdb.api_version(710300)

# Reserved keyrange on management cluster to store info for data movement
moveDataPrefix = b"emergency_movement"

rangeLimit = 100000
timeoutLen = 60000  # in milliseconds
tmpSrcClusterFileName = "tmp_src_fdb.cluster"
tmpDstClusterFileName = "tmp_dst_fdb.cluster"


def readSrcRange(
    tenant: fdb.Tenant, begin: bytes, end: bytes, throttlingTag: bytes
) -> list:
    tr = tenant.create_transaction()
    result = []
    beginKey = begin
    while True:
        try:
            tr.options.set_lock_aware()
            tr.options.set_auto_throttle_tag(throttlingTag)
            readRange = tr.get_range(beginKey, end, 0, False, StreamingMode.want_all)
            for kv in readRange:
                lastKey = kv.key
                result.append(kv)
            break
        except fdb.FDBError as e:
            beginKey = lastKey + b"\x00"
            # transaction_too_old can just be retried
            if e.code != 1007:
                tr.reset()
            else:
                tr.on_error(e).wait()
    return result


# Returns the index of the last kv pair NOT set in this transaction
@fdb.transactional
def writeDstRange(tr: fdb.Transaction, keyRange: list, throttlingTag: bytes) -> int:
    tr.options.set_lock_aware()
    tr.options.set_auto_throttle_tag(throttlingTag)
    sizeLimit = pow(10, 5)
    totalSize = 0
    index = 0
    for k, v in keyRange:
        if totalSize >= sizeLimit:
            break
        totalSize += len(k) + len(v)
        tr.set(k, v)
        index += 1
    return index


def copyData(
    srcClusterFile: str,
    destClusterFile: str,
    srcTenantName: bytes,
    dstTenantName: bytes,
    begin: bytes,
    end: bytes,
    throttlingTag: bytes,
) -> None:
    srcDb = fdb.open(srcClusterFile)
    srcDb.options.set_transaction_timeout(timeoutLen)

    dstDb = fdb.open(destClusterFile)
    dstDb.options.set_transaction_timeout(timeoutLen)

    srcTenant = srcDb.open_tenant(srcTenantName)
    dstTenant = dstDb.open_tenant(dstTenantName)

    while True:
        try:
            startTime = datetime.now()
            readRange = readSrcRange(srcTenant, begin, end, throttlingTag)
            print(
                "Read full source range: {} - {} for tenant {}. Total Keys: {}. Time Elapsed: {}".format(
                    begin,
                    end,
                    srcTenantName,
                    len(readRange),
                    (datetime.now() - startTime).total_seconds(),
                )
            )
            break
        except fdb.FDBError as e:
            print(
                "Exception while reading from {} for range {} - {}. Error: {}".format(
                    srcTenantName, begin, end, str(e)
                )
            )
            raise

    totalKeys = len(readRange)

    startTime2 = datetime.now()
    while len(readRange):
        while True:
            try:
                cutoffIndex = writeDstRange(dstTenant, readRange, throttlingTag)
                # print("Wrote destination range: {} - {}".format(readRange[0], readRange[cutoffIndex - 1]))
                readRange = readRange[cutoffIndex:]
                break
            except fdb.FDBError as e:
                print(
                    "Exception while writing to {} for range {} - {}. Error: {}".format(
                        dstTenantName, begin, end, str(e)
                    )
                )
                raise
    print(
        "Wrote full destination range: {} - {} for tenant {}. Total Keys: {}. Time Elapsed: {}".format(
            begin,
            end,
            dstTenantName,
            totalKeys,
            (datetime.now() - startTime2).total_seconds(),
        )
    )


def run_fdbcli_command_mgmt(*args):
    """run the fdbcli statement on the management cluster: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Console output from fdbcli
    """
    commands = command_template_mgmt + ["{}".format(" ".join(args))]
    print("Management Cluster FDBCLI Commands: {}".format(commands))
    process = subprocess.run(
        commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env
    )
    output = process.stdout.decode("utf-8").strip()
    error = process.stderr.decode("utf-8").strip()
    if error:
        print("Command Failed: {}".format(error))
        exit(0)
    print("Management Cluster FDBCLI Output: {}".format(output))
    return output


@fdb.transactional
def get_move_record(tr: fdb.Transaction, tenantGroup: bytes) -> tuple:
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((tenantGroup,), (moveDataPrefix + b"/move/"))
    result = tr.get(packKey)
    if result == None:
        return None
    moveTuple = fdb.tuple.unpack(result)
    return moveTuple


@fdb.transactional
def get_next_queue_head(
    tr: fdb.Transaction,
    tenantGroup: bytes,
    runId: bytes,
    currentTenant: bytes,
    key: bytes,
) -> tuple:
    tr.options.set_raw_access()
    packKeyBegin = fdb.tuple.pack(
        (tenantGroup, runId, currentTenant, key),
        (moveDataPrefix + b"/split_points/"),
    )
    packKeyEnd = fdb.tuple.pack(
        (tenantGroup, runId, b"\xff"),
        (moveDataPrefix + b"/split_points/"),
    )
    readRange = tr.get_range(packKeyBegin, packKeyEnd, 2).to_list()
    if len(readRange) == 2:
        nextSplitPoint = readRange[1].key
        # By convention, tenant name is 3rd element in the tuple
        # and the key is the 4th element
        nextTuple = fdb.tuple.unpack(
            nextSplitPoint, len(moveDataPrefix + b"/split_points/")
        )
        nextTenant = nextTuple[2]
        key = nextTuple[3]
        return nextTenant, key
    return None, None


@fdb.transactional
def get_queue_head(tr: fdb.Transaction, tenantGroup: bytes, runId: bytes) -> tuple:
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((tenantGroup, runId), (moveDataPrefix + b"/queue/"))
    result = tr.get(packKey)
    if result == None:
        return None
    return fdb.tuple.unpack(result)


@fdb.transactional
def set_queue_head(
    tr: fdb.Transaction, tenantGroup: bytes, runId: bytes, tenantName: bytes, key: bytes
) -> None:
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((tenantGroup, runId), (moveDataPrefix + b"/queue/"))
    packValue = fdb.tuple.pack((tenantName, key))
    tr.set(packKey, packValue)


@fdb.transactional
def clear_queue_head(tr, tenantGroup: bytes, runId: bytes) -> None:
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((tenantGroup, runId), (moveDataPrefix + b"/queue/"))
    tr.clear(packKey)


@fdb.transactional
def get_stored_split_point(
    tr: fdb.Transaction, tenantGroup: bytes, runId: bytes, tenantName: bytes, key: bytes
) -> bytes:
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup, runId, tenantName, key),
        (moveDataPrefix + b"/split_points/"),
    )
    result = tr.get(packKey)
    if result == None:
        return None
    return fdb.tuple.unpack(result)[0]


@fdb.transactional
def get_queue_head_from_split_points(
    tr: fdb.Transaction, tenantGroup: bytes, runId: bytes
) -> tuple:
    tr.options.set_raw_access()
    packBegin = fdb.tuple.pack(
        (tenantGroup, runId), (moveDataPrefix + b"/split_points/")
    )
    packEnd = fdb.tuple.pack(
        (tenantGroup, runId, b"\xff"),
        (moveDataPrefix + b"/split_points/"),
    )
    readRange = tr.get_range(packBegin, packEnd, 1).to_list()
    if len(readRange) > 0:
        tupleStr = readRange[0].key
        result = fdb.tuple.unpack(tupleStr, len(moveDataPrefix + b"/split_points/"))
        tenantName = result[2]
        key = result[3]
        return tenantName, key
    return None, None


# return currentTenant, begin, end: 3 arguments passed to copyData
@fdb.transactional
def get_and_update_queue_head(
    tr: fdb.Transaction, tenantGroup: bytes, runId: bytes
) -> tuple:
    queue_tuple = get_queue_head(tr, tenantGroup, runId)
    if queue_tuple == None:
        (
            headTenant,
            headKey,
        ) = get_queue_head_from_split_points(tr, tenantGroup, runId)
        if headTenant is None:
            return None, None, None
    else:
        headTenant = queue_tuple[0]
        headKey = queue_tuple[1]
    nextTenant, nextKey = get_next_queue_head(
        tr, tenantGroup, runId, headTenant, headKey
    )
    headEnd = get_stored_split_point(tr, tenantGroup, runId, headTenant, headKey)
    if headEnd is None:
        clear_queue_head(tr, tenantGroup, runId)
        return None, None, None
    if nextTenant is None:
        clear_queue_head(tr, tenantGroup, runId)
    else:
        set_queue_head(tr, tenantGroup, runId, nextTenant, nextKey)
    return headTenant, headKey, headEnd


@fdb.transactional
def clear_stored_split_point(
    tr: fdb.Transaction, tenantGroup: bytes, runId: bytes, tenantName: bytes, key: bytes
) -> None:
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup, runId, tenantName, key),
        (moveDataPrefix + b"/split_points/"),
    )
    tr.clear(packKey)


def process_queue(
    mgmtDbName: str, tenantGroup: bytes, srcClusterName: bytes, dstClusterName: bytes
) -> int:
    print("Process Queue Started")
    mgmtDb = fdb.open(mgmtDbName)
    mgmtDb.options.set_transaction_timeout(timeoutLen)
    moveRecord = get_move_record(mgmtDb, tenantGroup)
    if moveRecord is None:
        print("No move record found for the tenant group {}".format(tenantGroup))
        return -1

    # Validate move record with parameters
    runId = moveRecord[0]
    mrSrc = moveRecord[1]
    if mrSrc != srcClusterName:
        print("Mismatch between move record and provided source cluster name:")
        print("  Provided: {}".format(srcClusterName))
        print("  Move Record: {}".format(mrSrc))
        return -1
    mrDst = moveRecord[2]
    if mrDst != dstClusterName:
        print("Mismatch between move record and provided destination cluster name:")
        print("  Provided: {}".format(dstClusterName))
        print("  Move Record: {}".format(mrDst))
        return -1

    while True:
        currentTenant, begin, end = get_and_update_queue_head(
            mgmtDb, tenantGroup, runId
        )
        if begin is None and end is None:
            break
        copyData(
            tmpSrcClusterFileName,
            tmpDstClusterFileName,
            currentTenant,
            currentTenant,  # dst and src have the same name
            begin,
            end,
            tenantGroup,
        )
        clear_stored_split_point(mgmtDb, tenantGroup, runId, currentTenant, begin)
    print("Process Queue Finished")
    return 0


if __name__ == "__main__":
    # Arguments should be in double quotes to preserve escape characters for tenant group and src/dst name
    parser = argparse.ArgumentParser(description="Orchestrates tenant movement")
    parser.add_argument(
        "--cluster",
        help="Location of the cluster file for the management cluster",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--tenant-group",
        help="Name of the tenant group being moved",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--src-name",
        help="Name of the cluster the data is being moved away from",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--dst-name",
        help="Name of the cluster the data is being moved to",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--num-procs",
        help="Number of processes desired to run concurrently for faster data movement",
        required=True,
        type=int,
    )

    args = parser.parse_args()

    # validate cluster file exists
    if not os.path.exists(args.cluster):
        print("The management cluster file does not exist or is inaccessible.")
        exit(0)
    # keep current environment variables
    fdbcli_env = os.environ.copy()

    # TODO: change this to where the fdbcli binary is expected to be
    #   Also add cert files and other args needed for AuthZ
    # mgmt fdbcli command template
    command_template_mgmt = [
        "/usr/bin/fdbcli",
        "-C",
        args.cluster,
        "--tls_certificate_file",
        "/etc/foundationdb/fdb.pem",
        "--tls_ca_file",
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        "--tls_key_file",
        "/etc/foundationdb/fdb.pem",
        "--exec",
    ]

    # Check fdbcli functions as expected
    try:
        srcJson = json.loads(
            run_fdbcli_command_mgmt("metacluster get {} JSON".format(args.src_name))
        )
        dstJson = json.loads(
            run_fdbcli_command_mgmt("metacluster get {} JSON".format(args.dst_name))
        )
    except Exception as e:
        print(
            "Error invoking fdbcli on management cluster. Double check that the cluster file is correctly formatted."
        )
        print(e)
        exit(0)

    if "cluster" not in srcJson or "connection_string" not in srcJson["cluster"]:
        print("Error getting info for data cluster {}. Output:".format(args.src_name))
        print(srcJson)
        exit(0)

    if "cluster" not in dstJson or "connection_string" not in dstJson["cluster"]:
        print("Error getting info for data cluster {}. Output:".format(args.dst_name))
        print(dstJson)
        exit(0)

    # Create temporary cluster files to connect to src/dst clusters
    srcConnString = srcJson["cluster"]["connection_string"]
    dstConnString = dstJson["cluster"]["connection_string"]
    try:
        tmpSrcClusterFile = open(tmpSrcClusterFileName, "w")
        tmpDstClusterFile = open(tmpDstClusterFileName, "w")

        tmpSrcClusterFile.write(srcConnString)
        tmpDstClusterFile.write(dstConnString)

        tmpSrcClusterFile.close()
        tmpDstClusterFile.close()
    except Exception as e:
        print("Error creating temporary cluster files.")
        print(e)
        exit(0)

    # Arguments to the queue
    cluster = args.cluster
    tenantGroup = ast.literal_eval("b'%s'" % args.tenant_group)
    srcName = ast.literal_eval("b'%s'" % args.src_name)
    dstName = ast.literal_eval("b'%s'" % args.dst_name)

    start = datetime.now()
    poolArgs = (cluster, tenantGroup, srcName, dstName)
    multiArgs = [poolArgs for i in range(args.num_procs)]
    with multiprocessing.Pool(processes=args.num_procs) as pool:
        pool.starmap(process_queue, multiArgs)
    print("Time Elapsed for Copy: ", (datetime.now() - start).total_seconds())
    # delete tmp cluster files
    os.remove(tmpSrcClusterFileName)
    os.remove(tmpDstClusterFileName)
