#!/opt/rh/rh-python36/root/bin/python3
import fdb
import sys
import argparse

rangeLimit = 100000
timeoutLen = 60000  # in milliseconds

fdb.api_version(710300)


@fdb.transactional
def readSrcRange(tr, begin, end, throttlingTag):
    tr.options.set_lock_aware()
    tr.options.set_auto_throttle_tag(throttlingTag)
    return tr.get_range(begin, end)


@fdb.transactional
def writeDstRange(tr, keyRange, throttlingTag):
    tr.options.set_lock_aware()
    tr.options.set_auto_throttle_tag(throttlingTag)
    for k, v in keyRange:
        tr.set(k, v)


# Returns bool, where the bool indicates whether the data is consistent
def verifyData(srcTenant, dstTenant, begin, end, throttlingTag):
    print(
        "Verifying integrity of data on destination cluster for the range {} - {}".format(
            begin, end
        )
    )
    while True:
        try:
            srcTr = srcTenant.create_transaction()
            dstTr = dstTenant.create_transaction()
            srcTr.options.set_lock_aware()
            dstTr.options.set_lock_aware()
            srcTr.options.set_auto_throttle_tag(throttlingTag)
            dstTr.options.set_auto_throttle_tag(throttlingTag)
            oldRange = srcTr.get_range(begin, end)
            newRange = dstTr.get_range(begin, end)
            ranges = zip(oldRange, newRange)
            for oldKv, newKv in ranges:
                if oldKv.key != newKv.key or oldKv.value != newKv.value:
                    return False
                begin = oldKv.key
            break
        except fdb.FDBError as e:
            print(str(e))
            continue

    return True


def copyData(
    srcClusterFile,
    destClusterFile,
    srcTenantName,
    dstTenantName,
    begin,
    end,
    throttlingTag,
):
    print("Source Cluster File: {}".format(srcClusterFile))
    print("Destination Cluster File: {}".format(destClusterFile))

    if not isinstance(begin, bytes):
        begin = begin.encode("utf-8")
    if not isinstance(end, bytes):
        end = end.encode("utf-8")

    if not isinstance(srcTenantName, bytes):
        srcTenantName = srcTenantName.encode("utf-8")
    if not isinstance(dstTenantName, bytes):
        dstTenantName = dstTenantName.encode("utf-8")
    print(
        "Copying data from `{}' to `{}' for range `{}' to `{}'".format(
            srcTenantName, dstTenantName, begin, end
        )
    )

    srcDb = fdb.open(srcClusterFile)
    srcDb.options.set_transaction_timeout(timeoutLen)

    dstDb = fdb.open(destClusterFile)
    dstDb.options.set_transaction_timeout(timeoutLen)

    srcTenant = srcDb.open_tenant(srcTenantName)
    dstTenant = dstDb.open_tenant(dstTenantName)

    readRange = readSrcRange(srcTenant, begin, end, throttlingTag)
    writeDstRange(dstTenant, readRange, throttlingTag)
    print("Wrote destination range: {} - {}".format(begin, end))

    result = verifyData(srcTenant, dstTenant, begin, end, throttlingTag)
    if result:
        print("************* SUCCESS *************")
        print("Data copy complete!")
    else:
        print("************* FAILURE *************")
        print(
            "Data copy verification did not match between the source and destination cluster."
        )

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Moves tenant from one data cluster to another"
    )
    parser.add_argument(
        "--src-cluster",
        help="Cluster file path for the source cluster",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--dst-cluster",
        help="Cluster file path for the destination cluster",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--src-tenant", help="Name of source tenant", required=True, type=str
    )
    parser.add_argument(
        "--dst-tenant", help="Name of destination tenant", required=True, type=str
    )
    parser.add_argument(
        "--throttling-tag", help="Name of throttling tag", required=True, type=str
    )
    args = parser.parse_args()
    copyData(
        args.src_cluster,
        args.dst_cluster,
        args.src_tenant,
        args.dst_tenant,
        args.throttling_tag,
    )
