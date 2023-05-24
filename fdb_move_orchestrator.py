#!/opt/rh/rh-python36/root/bin/python3
import base64
import fdb
import sys
import argparse
from fdb.tuple import pack, unpack, range
import os
import json
import subprocess
import time
import uuid
from fdb_copy_tool import verifyData, copyData
from collections import defaultdict

fdb.api_version(710300)

# Reserved keyrange on management cluster to store info for data movement
moveDataPrefix = "emergency_movement"
# emergency_movement/move/<tenant_group_name> = <run_id>
# emergency_movement/<tenant_group_name>/<run_id> = <version>

# dangerous operations:
#   delete, unlock, "finishing" the movement (asserting as much as possible is correct before we release it to the wild)
#   at the very end, we have a simple validation step that we always run
#   check all properties that everything we've updated has the right properties:
#       tenants are there, groups are set up right, renames have not gone haywire and created a mess, etc.
#       make sure other matching tenant is already unlocked before deletion

# Next begin range that has NOT started moving yet read and write the next value immediately before performing work
# emergency_movement/<tenant_group_name>/<run_id>/split_points = (tenant_name, key) head of queue
# emergency_movement/<tenant_group_name>/<run_id>/split_points/<tenant_name>/ = a
# emergency_movement/<tenant_group_name>/<run_id>/split_points/<tenant_name>/a = e
# emergency_movement/<tenant_group_name>/<run_id>/split_points/<tenant_name>/e = j
# emergency_movement/<tenant_group_name>/<run_id>/split_points/<tenant_name>/j = v
# emergency_movement/<tenant_group_name>/<run_id>/split_points/<tenant_name>/v = \xff


rangeLimit = 100000
timeoutLen = 60000  # in milliseconds
tmpSrcClusterFileName = "tmp_src_fdb.cluster"
tmpDstClusterFileName = "tmp_dst_fdb.cluster"


def get_tmp_name(name):
    tmp_name = fdb.tuple.pack((name, "temporary_tuple"))
    return tmp_name


def get_hex_str(name):
    return "".join(["\\x%02x" % c for c in name])


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


def run_fdbcli_command_src(*args):
    """run the fdbcli statement on the source cluster: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Console output from fdbcli
    """
    commands = command_template_src + ["{}".format(" ".join(args))]
    print("Source Cluster FDBCLI Commands: {}".format(commands))
    process = subprocess.run(
        commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env
    )
    output = process.stdout.decode("utf-8").strip()
    error = process.stderr.decode("utf-8").strip()
    if error:
        print("Command Failed: {}".format(error))
        exit(0)
    print("Source Cluster FDBCLI Output: {}".format(output))
    return output


def run_fdbcli_command_dst(*args):
    """run the fdbcli statement on the destination cluster: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Console output from fdbcli
    """
    commands = command_template_dst + ["{}".format(" ".join(args))]
    print("Destination Cluster FDBCLI Commands: {}".format(commands))
    process = subprocess.run(
        commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env
    )
    output = process.stdout.decode("utf-8").strip()
    error = process.stderr.decode("utf-8").strip()
    if error:
        print("Command Failed: {}".format(error))
        exit(0)
    print("Destination Cluster FDBCLI Output: {}".format(output))
    return output


def check_valid_abort_unlock(unlockClusterName, tenantName, tenantGroup):
    unlockTenantName = tenantName
    unlockTenantGroup = get_tmp_name(tenantGroup)
    tmpTenant = get_tmp_name(tenantName)
    if not isinstance(unlockClusterName, bytes):
        unlockClusterName = unlockClusterName.encode("utf-8")
    try:
        # Assert the tmp tenant does NOT exist (the one we're unlocking should have been renamed tmp -> original)
        hexTenant = get_hex_str(tmpTenant)
        tenantJsonOther = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        if tenantJsonOther["type"] != "error":
            print(
                "The tenant {} should not exist in the metacluster.".format(tmpTenant)
            )
            return False

        # Assert that the current tenant's group is the matching tmp name, ready to be renamed back
        hexTenant = get_hex_str(unlockTenantName)
        tenantJson = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        foundGroup = base64.b64decode(tenantJson["tenant"]["tenant_group"]["base64"])
        if foundGroup != unlockTenantGroup:
            print(
                "The tenant group for tenant {} does not match.".format(
                    unlockTenantName
                )
            )
            print("Expected: {}".format(unlockTenantGroup))
            print("Actual: {}".format(foundGroup))
            return False

        # Assert that the tenant's assigned cluster is correct
        foundCluster = base64.b64decode(tenantJson["tenant"]["tenant_group"]["base64"])
        if foundCluster != unlockClusterName:
            print(
                "The assigned cluster for tenant {} does not match.".format(
                    unlockTenantName
                )
            )
            print("Expected: {}".format(unlockClusterName))
            print("Actual: {}".format(foundCluster))
            return False

        return True
    except Exception as e:
        print(e)
        print(
            "Failure while attempting to check for valid unlock while aborting data movement."
        )
        return False


def check_valid_unlock(lockClusterName, unlockClusterName, tenantName, tenantGroup):
    unlockTenantName = tenantName
    lockTenantName = get_tmp_name(tenantName)
    if not isinstance(lockClusterName, bytes):
        lockClusterName = lockClusterName.encode("utf-8")
    if not isinstance(unlockClusterName, bytes):
        unlockClusterName = unlockClusterName.encode("utf-8")
    try:
        # Assert the tenant we are unlocking is on the right cluster
        hexTenant = get_hex_str(unlockTenantName)
        tenantJsonUnlock = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        if (
            base64.b64decode(tenantJsonUnlock["tenant"]["assigned_cluster"]["base64"])
            != unlockClusterName
        ):
            print(
                "The tenant {} is not on the unlock cluster {}.".format(
                    unlockTenantName, unlockClusterName
                )
            )
            return False

        # Assert matching tenant exists on other cluster
        hexTenant = get_hex_str(lockTenantName)
        tenantJsonLock = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        if (
            base64.b64decode(tenantJsonLock["tenant"]["assigned_cluster"]["base64"])
            != lockClusterName
        ):
            print(
                "The matching tenant {} is not on the lock cluster {}.".format(
                    lockTenantName, lockClusterName
                )
            )
            return False

        # Assert other tenant is locked
        if tenantJsonLock["tenant"]["lock_state"] != "locked":
            print("The matching tenant {} is not locked.".format(lockTenantName))
            return False

        # Assert other tenant has the correct tenant group
        unlockGroup = base64.b64decode(
            tenantJsonUnlock["tenant"]["tenant_group"]["base64"]
        )
        lockGroup = base64.b64decode(tenantJsonLock["tenant"]["tenant_group"]["base64"])
        if get_tmp_name(unlockGroup) != lockGroup:
            print(
                "The tenants {} and {} do not have matching original and temporary tenant groups: {} <-> {}".format(
                    unlockTenantName, lockTenantName, unlockGroup, lockGroup
                )
            )
            return False

        srcDb = fdb.open(tmpSrcClusterFileName)
        srcDb.options.set_transaction_timeout(timeoutLen)

        dstDb = fdb.open(tmpDstClusterFileName)
        dstDb.options.set_transaction_timeout(timeoutLen)

        srcTenant = srcDb.open_tenant(lockTenantName)
        dstTenant = dstDb.open_tenant(unlockTenantName)
        # Assert tenant data matches
        return verifyData(srcTenant, dstTenant, b"", b"\xff", tenantGroup)
    except Exception as e:
        print(e)
        print(
            "Failure while attempting to check for valid unlock while finishing data movement."
        )
        return False


def unlock_tenant(tenantName, runId):
    hexTenant = get_hex_str(tenantName)
    return run_fdbcli_command_mgmt("tenant unlock {} {}".format(hexTenant, runId))


@fdb.transactional
def move_cleanup(tr):
    tr.options.set_raw_access()
    # Clear info stored on management cluster
    tupleRange = fdb.tuple.range((moveDataPrefix,))
    tr.clear_range(tupleRange.start, tupleRange.stop)
    # Delete temporary cluster files
    os.remove(tmpSrcClusterFileName)
    os.remove(tmpDstClusterFileName)


def tenants_in_group_src(tenantGroup):
    if not isinstance(tenantGroup, str):
        tenantGroup = tenantGroup.decode("utf-8")
    tenantNames = []
    tenantMetadata = run_fdbcli_command_src(
        'tenant list "" \\xff tenant_group="{}" JSON'.format(tenantGroup)
    )
    jsonLoaded = json.loads(tenantMetadata)
    tenantInfo = jsonLoaded["tenants"]
    for tenant in tenantInfo:
        tenantName = base64.b64decode(tenant["name"]["base64"])
        tenantNames.append(tenantName)
    return tenantNames


def tenants_in_group_dst(tenantGroup):
    if not isinstance(tenantGroup, str):
        tenantGroup = tenantGroup.decode("utf-8")
    tenantNames = []
    tenantMetadata = run_fdbcli_command_dst(
        'tenant list "" \\xff tenant_group="{}" JSON'.format(tenantGroup)
    )
    jsonLoaded = json.loads(tenantMetadata)
    tenantInfo = jsonLoaded["tenants"]
    for tenant in tenantInfo:
        tenantName = base64.b64decode(tenant["name"]["base64"])
        tenantNames.append(tenantName)
    return tenantNames


def list_blobbified_ranges(clusterFile, tenant, begin, end, limit):
    db = fdb.open(clusterFile)
    db.options.set_transaction_timeout(timeoutLen)

    srcTenant = db.open_tenant(tenant)
    result = srcTenant.list_blobbified_ranges(begin, end, limit).wait()
    return result[0]


# Getting tenant names by group is only reliable on the first call to a data movement/
# Entering upon a retry or abort is possible to have half-temp, half-regular names
# on the source cluster. If we always write the split points before issuing renames,
# we can use them as a source of truth. There is also no issue if split points have started
# clearing, because that means the queue has started processing and we no longer need the
# list of tenant names.
@fdb.transactional
def get_names_from_split_points(tr, tenantGroup, runId):
    tr.options.set_raw_access()
    tupleRange = fdb.tuple.range((moveDataPrefix, tenantGroup, runId, "split_points"))
    result = tr.get_range(tupleRange.start, tupleRange.stop)
    names = []
    lastName = None
    for k, _ in result:
        keyTuple = fdb.tuple.unpack(k)
        # Ignore queue head
        if len(keyTuple) != 6:
            continue
        # By convention, tenant name is 2nd from the end in the tuple
        tenantName = keyTuple[-2]
        if tenantName == lastName:
            continue
        lastName = tenantName
        names.append(tenantName)
    return names


@fdb.transactional
def split_points_remaining(tr, tenantGroup, runId):
    tr.options.set_raw_access()
    tupleRange = fdb.tuple.range((moveDataPrefix, tenantGroup, runId, "split_points"))
    result = tr.get_range(tupleRange.start, tupleRange.stop)
    return result == None


# Read the split points from the source cluster
@fdb.transactional
def getRangeSplitPoints(tr):
    tr.options.set_lock_aware()
    splitPoints = tr.get_range_split_points(b"", b"\xff", rangeLimit).wait()
    return splitPoints


@fdb.transactional
def set_stored_run_id(tr, tenantGroup, runId):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((moveDataPrefix, "move", tenantGroup))
    packValue = fdb.tuple.pack((runId,))
    tr.set(packKey, packValue)


@fdb.transactional
def clear_stored_run_id(tr, tenantGroup):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((moveDataPrefix, "move", tenantGroup))
    tr.clear(packKey)


@fdb.transactional
def get_stored_run_id(tr, tenantGroup):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((moveDataPrefix, "move", tenantGroup))
    result = tr.get(packKey)
    if result == None:
        return None
    return fdb.tuple.unpack(result)[0]


@fdb.transactional
def set_stored_version(tr, tenantGroup, runId, version):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((moveDataPrefix, tenantGroup, runId))
    packValue = fdb.tuple.pack((version,))
    tr.set(packKey, packValue)


@fdb.transactional
def get_stored_version(tr, tenantGroup, runId):
    tr.options.set_raw_access()
    keyTuple = fdb.tuple.pack((moveDataPrefix, tenantGroup, runId))
    result = tr.get(keyTuple)
    if result == None:
        return None
    return fdb.tuple.unpack(result)[0]


@fdb.transactional
def init_stored_version(tr, tenantGroup, runId):
    if not get_stored_version(tr, tenantGroup, runId):
        v = float(run_fdbcli_command_src("getversion"))
        set_stored_version(tr, tenantGroup, runId, v)


@fdb.transactional
def get_next_tenant(tr, tenantGroup, runId, currentTenant):
    tr.options.set_raw_access()
    tupleRange = fdb.tuple.range(
        (moveDataPrefix, tenantGroup, runId, "split_points", currentTenant)
    )
    readRange = tr.get_range(tupleRange.start, tupleRange.stop)
    for k, _ in readRange:
        # By convention, tenant name is 2nd from the end in the tuple
        keyTuple = fdb.tuple.unpack(k)
        tenantName = keyTuple[-2]
        if tenantName != currentTenant:
            return tenantName
    return None


@fdb.transactional
def get_queue_head(tr, tenantGroup, runId):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((moveDataPrefix, tenantGroup, runId, "split_points"))
    result = tr.get(packKey)
    if result == None:
        return None
    return fdb.tuple.unpack(result)


@fdb.transactional
def set_queue_head(tr, tenantGroup, runId, tenantName, key):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((moveDataPrefix, tenantGroup, runId, "split_points"))
    packValue = fdb.tuple.pack((tenantName, key))
    tr.set(packKey, packValue)


@fdb.transactional
def clear_queue_head(tr, tenantGroup, runId):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack((moveDataPrefix, tenantGroup, runId, "split_points"))
    tr.clear(packKey)


# return nextTenant, currentTenant, begin, end
@fdb.transactional
def get_and_update_queue_head(tr, tenantGroup, runId):
    queue_tuple = get_queue_head(tr, tenantGroup, runId)
    tenantName = queue_tuple[0]
    begin = queue_tuple[1]
    end = get_stored_split_point(tr, tenantGroup, runId, tenantName, begin)

    # Reached last split point for current tenant. Update the queue with the next tenant
    if end == b"\xff":
        nextTenant = get_next_tenant(tr, tenantGroup, runId, tenantName)
        if nextTenant == None:
            return None, tenantName, begin, end
        set_queue_head(tr, tenantGroup, runId, nextTenant, "")
        return nextTenant, tenantName, begin, end
    set_queue_head(tr, tenantGroup, runId, tenantName, end)
    return tenantName, tenantName, begin, end


@fdb.transactional
def set_stored_split_points(tr, tenantGroup, runId, tenantName, splitPoints):
    tr.options.set_raw_access()
    first = True
    startKey = b""
    endKey = b""
    for k in splitPoints:
        if first:
            startKey = k
            first = False
            continue
        endKey = k
        packKey = fdb.tuple.pack(
            (moveDataPrefix, tenantGroup, runId, "split_points", tenantName, startKey)
        )
        packValue = fdb.tuple.pack((endKey,))
        tr.set(packKey, packValue)
        startKey = k


@fdb.transactional
def get_stored_split_point(tr, tenantGroup, runId, tenantName, key):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (moveDataPrefix, tenantGroup, runId, "split_points", tenantName, key)
    )
    result = tr.get(packKey)
    if result == None:
        return b"\xff"
    return fdb.tuple.unpack(result)[0]


@fdb.transactional
def clear_stored_split_point(tr, tenantGroup, runId, tenantName, key):
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (moveDataPrefix, tenantGroup, runId, "split_points", tenantName, key)
    )
    tr.clear(packKey)


@fdb.transactional
def clear_all_tenant_split_points(tr, tenantGroup, runId):
    tr.options.set_raw_access()
    tupleRange = fdb.tuple.range((moveDataPrefix, tenantGroup, runId, "split_points"))
    tr.clear_range(tupleRange.start, tupleRange.stop)


@fdb.transactional
def clear_data(tr, tenantGroup):
    tr.options.set_lock_aware()
    tr.options.set_auto_throttle_tag(tenantGroup)
    tr.clear_range(b"", b"\xff")


def clear_tenant_data(clusterFile, tenantName, tenantGroup):
    db = fdb.open(clusterFile)
    db.options.set_transaction_timeout(timeoutLen)

    tenant = db.open_tenant(tenantName)
    clear_data(tenant, tenantGroup)


def check_valid_abort_delete(delClusterName, keepClusterName, tenantName):
    keepTenantName = get_tmp_name(tenantName)
    deleteTenantName = tenantName
    if not isinstance(delClusterName, bytes):
        delClusterName = delClusterName.encode("utf-8")
    if not isinstance(keepClusterName, bytes):
        keepClusterName = keepClusterName.encode("utf-8")
    try:
        # Check that tenant exists, is locked before deletion, and is assigned to del cluster
        hexTenant = get_hex_str(deleteTenantName)
        tenantJsonDelete = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        if (
            base64.b64decode(tenantJsonDelete["tenant"]["assigned_cluster"]["base64"])
            != delClusterName
        ):
            print(
                "The tenant {} being deleted is not on the specified cluster {}.".format(
                    deleteTenantName, delClusterName
                )
            )
            print(
                base64.b64decode(
                    tenantJsonDelete["tenant"]["assigned_cluster"]["base64"]
                )
            )
            print(delClusterName)
            return False

        if tenantJsonDelete["tenant"]["lock_state"] != "locked":
            print(
                "The tenant {} being deleted is not locked. Only locked tenants can be deleted.".format(
                    deleteTenantName
                )
            )
            return False

        # Check that the matching tenant is on the other cluster and is locked
        hexTenant = get_hex_str(keepTenantName)
        tenantJsonKeep = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        if (
            base64.b64decode(tenantJsonKeep["tenant"]["assigned_cluster"]["base64"])
            != keepClusterName
        ):
            print(
                "The tenant {} that we want to keep is not on the other cluster {}.".format(
                    keepTenantName, keepClusterName
                )
            )
            return False

        if tenantJsonKeep["tenant"]["lock_state"] != "locked":
            print(
                "The tenant {} is not locked. Ensure the matching tenant's data is locked before aborting the run.".format(
                    keepTenantName
                )
            )
            return False

        # Check tenantGroup and its temporary counterpart match correctly
        deleteGroup = base64.b64decode(
            tenantJsonDelete["tenant"]["tenant_group"]["base64"]
        )
        keepGroup = base64.b64decode(tenantJsonKeep["tenant"]["tenant_group"]["base64"])
        if get_tmp_name(deleteGroup) != keepGroup:
            print(
                "The tenants {} and {} do not have matching original and temporary tenant groups: {} <-> {}".format(
                    deleteTenantName,
                    keepTenantName,
                    get_tmp_name(deleteGroup),
                    keepGroup,
                )
            )
            return False

    except Exception as e:
        print(e)
        print(
            "Failure while attempting to check for valid deletion while aborting data movement."
        )
        return False


# Run to check valid delete at the end of data movement
# Expect to see tmp names on source cluster and unlocked tenants on destination cluster
def check_valid_delete(delClusterName, keepClusterName, tenantName):
    keepTenantName = tenantName
    deleteTenantName = get_tmp_name(tenantName)
    if not isinstance(delClusterName, bytes):
        delClusterName = delClusterName.encode("utf-8")
    if not isinstance(keepClusterName, bytes):
        keepClusterName = keepClusterName.encode("utf-8")
    try:
        # Check that the tenant exists, is locked before deletion, and is assigned to the del cluster
        hexTenant = get_hex_str(deleteTenantName)
        tenantJsonDelete = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        if (
            base64.b64decode(tenantJsonDelete["tenant"]["assigned_cluster"]["base64"])
            != delClusterName
        ):
            print(
                "The tenant {} being deleted is not on the specified cluster {}.".format(
                    deleteTenantName, delClusterName
                )
            )
            return False

        if tenantJsonDelete["tenant"]["lock_state"] != "locked":
            print(
                "The tenant {} being deleted is not locked. Only locked tenants can be deleted.".format(
                    deleteTenantName
                )
            )
            return False

        # Check that the matching tenant is on the other cluster, and is unlocked
        hexTenant = get_hex_str(keepTenantName)
        tenantJsonKeep = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        if (
            base64.b64decode(tenantJsonKeep["tenant"]["assigned_cluster"]["base64"])
            != keepClusterName
        ):
            print(
                "The tenant {} that we want to keep is not on the other cluster {}.".format(
                    keepTenantName, keepClusterName
                )
            )
            return False

        if tenantJsonKeep["tenant"]["lock_state"] == "locked":
            print(
                "The tenant {} is not unlocked. Ensure the matching tenant's data is consistent and available before deleting its counterpart.".format(
                    keepTenantName
                )
            )
            return False

        # Check tenantGroup and its temporary counterpart match correctly
        deleteGroup = base64.b64decode(
            tenantJsonDelete["tenant"]["tenant_group"]["base64"]
        )
        keepGroup = base64.b64decode(tenantJsonKeep["tenant"]["tenant_group"]["base64"])
        if get_tmp_name(keepGroup) != deleteGroup:
            print(
                "The tenants {} and {} do not have matching original and temporary tenant groups: {} <-> {}".format(
                    keepTenantName, deleteTenantName, keepGroup, deleteGroup
                )
            )
            return False
    except Exception as e:
        print(e)
        print(
            "Failure while attempting to check for valid deletion while finishing data movement."
        )
        return False

    return True


def get_and_store_split_points(
    mgmtDb, sourceClusterFile, tenantGroup, tenantName, runId
):
    # Read from source cluster
    srcDb = fdb.open(sourceClusterFile)
    srcDb.options.set_transaction_timeout(timeoutLen)

    srcTenant = srcDb.open_tenant(tenantName)
    splitPoints = getRangeSplitPoints(srcTenant)

    # Write stored data in management cluster
    set_stored_split_points(mgmtDb, tenantGroup, runId, tenantName, splitPoints)


def abort_run(mgmtDb, srcClusterName, dstClusterName, runId, tenantGroup):
    queue_head = get_queue_head(mgmtDb, tenantGroup, runId)
    if queue_head:
        dstTenants = tenants_in_group_dst(tenantGroup)
        for tenantName in dstTenants:
            if check_valid_abort_delete(dstClusterName, srcClusterName, tenantName):
                tmpTenantName = get_tmp_name(tenantName)
                tmpHexTenant = get_hex_str(tmpTenantName)
                hexTenant = get_hex_str(tenantName)
                # Clear all keys, then delete the tenants
                clear_tenant_data(tmpDstClusterFileName, tenantName, tenantGroup)
                run_fdbcli_command_mgmt(
                    "tenant delete {}; tenant rename {} {}".format(
                        hexTenant, tmpHexTenant, hexTenant
                    )
                )
            else:
                print("Deletion data check failed for tenant {}.".format(tenantName))
                exit(0)
    tmpTenantGroup = get_tmp_name(tenantGroup)
    srcTenants = tenants_in_group_src(tmpTenantGroup)

    for tenantName in srcTenants:
        hexTenant = get_hex_str(tenantName)
        if check_valid_abort_unlock(srcClusterName, tenantName, args.tenant_group):
            run_fdbcli_command_mgmt(
                'tenant configure {} "{}"; tenant unlock {} {}'.format(
                    hexTenant, tenantGroup, hexTenant, runId
                )
            )
        else:
            print("Unlock check failed for tenant {}.".format(tenantName))
            exit(0)


def configure_tenant_groups(tenantNames, tenantGroup, runId):
    # Quota units are bytes/second
    quota = run_fdbcli_command_src("quota get {} total_throughput".format(tenantGroup))
    run_fdbcli_command_dst(
        "quota set {} total_throughput {}".format(tenantGroup, quota)
    )

    tmpGroupName = get_tmp_name(tenantGroup).decode("utf-8")
    for tenantName in tenantNames:
        hexTenant = get_hex_str(tenantName)
        tenantJson = json.loads(
            run_fdbcli_command_mgmt("tenant get {} JSON".format(hexTenant))
        )
        # Locked tenants have already been configured
        if tenantJson["tenant"]["lock_state"] == "locked":
            continue
        run_fdbcli_command_mgmt(
            'tenant configure {} tenant_group="{}" ignore_capacity_limit; tenant lock {} rw {}'.format(
                hexTenant, tmpGroupName, hexTenant, runId
            )
        )


def write_all_tenant_split_points(mgmtDb, tenantNames, tenantGroup, runId):
    clear_all_tenant_split_points(mgmtDb, tenantGroup, runId)
    for tenantName in tenantNames:
        get_and_store_split_points(
            mgmtDb, tmpSrcClusterFileName, tenantGroup, tenantName, runId
        )
    # Set queue head after all split points are set
    # This acts as a signal that the split points for all tenants have finished being written to
    set_queue_head(mgmtDb, tenantGroup, runId, tenantNames[0], "")


def create_and_blobbify_tenants(tenantNames, tenantGroup, dstName, runId):
    if not isinstance(tenantGroup, str):
        tenantGroup = tenantGroup.decode("utf-8")
    if not isinstance(dstName, str):
        dstName = dstName.decode("utf-8")
    for tenantName in tenantNames:
        tmpTenantName = get_tmp_name(tenantName)

        hexTenant = get_hex_str(tenantName)
        tmpHexTenant = get_hex_str(tmpTenantName)

        # # Clear blob on destination first?
        # run_fdbcli_command_dst("usetenant {}; blobrange purge \"\" \\xff".format(tmpHexStr))
        # run_fdbcli_command_mgmt("tenant rename {} {}; tenant create {} tenant_group=\"{}\" assigned_cluster=\"{}\"; tenant lock {} rw {}".format(hexStr, tmpHexStr, hexStr, tenantGroup, dstName, hexStr, runId))
        run_fdbcli_command_mgmt("tenant rename {} {}".format(hexTenant, tmpHexTenant))
        run_fdbcli_command_mgmt(
            'tenant create {} tenant_group="{}" assigned_cluster="{}"'.format(
                hexTenant, tenantGroup, dstName
            )
        )
        run_fdbcli_command_mgmt("tenant lock {} rw {}".format(hexTenant, runId))
        # Read from source
        # Pre-blobbify before copying data
        blob_ranges = list_blobbified_ranges(
            tmpSrcClusterFileName, tmpTenantName, b"", b"\xff", rangeLimit
        )
        for k, v in blob_ranges:
            run_fdbcli_command_dst(
                "usetenant {}; blobrange start {} {}".format(hexTenant, k, v)
            )


def process_queue(mgmtDb, tenantGroup, runId):
    ###################################################################
    # This block has potential to be multiprocessed for concurrent work
    while True:
        nextTenant, currentTenant, begin, end = get_and_update_queue_head(
            mgmtDb, tenantGroup, runId
        )
        if nextTenant == None:
            clear_stored_split_point(mgmtDb, tenantGroup, runId, currentTenant, begin)
            clear_queue_head(mgmtDb, tenantGroup, runId)
            break
        tmpTenantName = get_tmp_name(currentTenant)
        result = copyData(
            tmpSrcClusterFileName,
            tmpDstClusterFileName,
            tmpTenantName,
            currentTenant,
            begin,
            end,
            tenantGroup,
        )
        if not result:
            print(
                "Encountered an issue while copying data for tenant {}.".format(
                    currentTenant
                )
            )
            exit(0)
        clear_stored_split_point(mgmtDb, tenantGroup, runId, nextTenant, begin)
    ###################################################################


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
        "--aborting",
        help="Signals to the move orchestrator to abort the data movement",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--retrying",
        help="Signals to the move orchestrator to retry the data movement",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--deleting",
        help="Signals to the move orchestrator to delete all the source tenants after data movement has completed",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--run-id",
        help="ID required to unlock all tenants after a movement is finished.",
        type=str,
    )

    args = parser.parse_args()

    ###########################################
    # Parse Args and set up cluster files
    ###########################################

    # validate cluster file exists
    if not os.path.exists(args.cluster):
        print("The management cluster file does not exist or is inaccessible.")
        exit(0)

    # cannot be both aborting and retrying
    if args.aborting and args.retrying:
        print("A data movement cannot be both aborting and retrying. Please try again.")
        exit(0)

    # keep current environment variables
    fdbcli_env = os.environ.copy()

    # mgmt fdbcli command template
    command_template_mgmt = [
        "/bin/fdbcli",
        "-C",
        args.cluster,
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

    srcConnString = srcJson["cluster"]["connection_string"]
    dstConnString = dstJson["cluster"]["connection_string"]

    # Create temporary cluster files to connect to src/dst clusters
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

    # src fdbcli command template
    command_template_src = [
        "/bin/fdbcli",
        "-C",
        tmpSrcClusterFileName,
        "--exec",
    ]

    # dst fdbcli command template
    command_template_dst = [
        "/bin/fdbcli",
        "-C",
        tmpDstClusterFileName,
        "--exec",
    ]

    ###########################################
    # Begin Tenant Movement
    # Branching for abort/start/continue
    ###########################################

    mgmtDb = fdb.open(args.cluster)
    mgmtDb.options.set_transaction_timeout(timeoutLen)

    # can branch into different helpers
    runId = None
    if args.deleting:
        tenantNames = tenants_in_group_dst(args.tenant_group)
        for tenantName in tenantNames:
            tmpName = get_tmp_name(tenantName)
            if check_valid_delete(args.src_name, args.dst_name, tenantName):
                tmpHexStr = get_hex_str(tmpName)
                run_fdbcli_command_src(
                    'usetenant {}; blobrange purge "" \\xff'.format(tmpHexStr)
                )
                clear_tenant_data(tmpSrcClusterFileName, tenantName, args.tenant_group)
                run_fdbcli_command_mgmt("tenant delete {}".format(tmpHexStr))
            else:
                print(
                    "Deletion data check failed for tenant {}. Double check that this deletion is safe and valid.".format(
                        tmpName
                    )
                )
                exit(0)
        move_cleanup(mgmtDb)
        exit(0)
    elif args.run_id:
        # Check validity of unlock
        # then do all unlocks
        runId = get_stored_run_id(mgmtDb, args.tenant_group)
        if args.run_id != runId:
            print(
                "The provided run ID {} does not match the stored run ID {}.".format(
                    args.run_id, runId
                )
            )
        tenantNames = tenants_in_group_dst(args.tenant_group)
        for tenantName in tenantNames:
            if check_valid_unlock(
                args.src_name, args.dst_name, tenantName, args.tenant_group
            ):
                unlock_tenant(mgmtDb, args.tenant_group, runId)
            else:
                print("{} is not safe to unlock.".format(tenantName))
                exit(0)
        print("Succesfully unlocked all tenants.")
        exit(0)
    elif args.aborting:
        # Verify move in progress
        runId = get_stored_run_id(mgmtDb, args.tenant_group)
        if runId == None:
            print(
                "There is no data movement currently in progress for tenant group {}".format(
                    args.tenant_group
                )
            )
            exit(0)
        abort_run(mgmtDb, args.src_name, args.dst_name, runId, args.tenant_group)
        move_cleanup(mgmtDb)
        exit(0)
    elif args.retrying:
        # Verify move in progress
        runId = get_stored_run_id(mgmtDb, args.tenant_group)
        if runId == None:
            print(
                "There is no data movement currently in progress for tenant group {}".format(
                    args.tenant_group
                )
            )
            exit(0)
        init_stored_version(mgmtDb, args.tenant_group, runId)
        # Branch:
        # 0) Steps before this will not have had runId set
        # 1) configure source tenant group and locking source tenants
        # 2) Writing all tenant split points + setting queue head
        # 3) Renaming tenants on source + creating and blobbifying tenants on dest
        # 3) Processing queue
        queue_head = get_queue_head(mgmtDb, args.tenant_group, runId)
        tenantNames = None
        if queue_head:
            tenantNames = get_names_from_split_points(mgmtDb, args.tenant_group, runId)
        else:
            tenantNames = tenants_in_group_src(
                get_tmp_name(args.tenant_group)
            ) + tenants_in_group_src(args.tenant_group)
            configure_tenant_groups(tenantNames, args.tenant_group, runId)
            write_all_tenant_split_points(tenantNames, args.tenant_group, runId)
        create_and_blobbify_tenants(
            tenantNames, args.tenant_group, args.dst_name, runId
        )
        process_queue()
    else:
        runId = get_stored_run_id(mgmtDb, args.tenant_group)
        # Verify we do NOT have a move in progress
        if runId != None:
            print(
                "There is already data movement in progress for the tenant group {} with ID {}".format(
                    args.tenant_group, runId
                )
            )
            exit(0)

    if runId == None:
        runId = uuid.uuid4().hex

    # Validate tenant group is in the matching cluster
    groupInfo = json.loads(
        run_fdbcli_command_src("tenantgroup", "get", args.tenant_group, "JSON")
    )
    if groupInfo["type"] == "error":
        print(groupInfo)
        print(
            "Tenant Group {} was not found in source cluster {}".format(
                args.tenant_group, args.src_name
            )
        )
        exit(0)

    set_stored_run_id(mgmtDb, args.tenant_group, runId)
    print("Run UID: {}".format(runId))

    init_stored_version(mgmtDb, args.tenant_group, runId)

    tenantNames = tenants_in_group_src(args.tenant_group)
    configure_tenant_groups(tenantNames, args.tenant_group, runId)
    write_all_tenant_split_points(mgmtDb, tenantNames, args.tenant_group, runId)
    create_and_blobbify_tenants(tenantNames, args.tenant_group, args.dst_name, runId)
    process_queue(mgmtDb, args.tenant_group, runId)

    # range read split points for each tenant to verify they have all been deleted, indicating all data moved
    if split_points_remaining(mgmtDb, args.tenant_group, runId):
        print(
            "Some split points still remaining... Issue another movement with the `--retrying` flag"
        )
        exit(0)

    startVersion = get_stored_version(mgmtDb, args.tenant_group, runId)
    checkVersion = float(run_fdbcli_command_dst("getversion"))
    print("Checking Version. \nSource: {}\nDest: {}".format(startVersion, checkVersion))
    while checkVersion < startVersion:
        print(
            "Checking Version. \nSource: {}\nDest: {}".format(
                startVersion, checkVersion
            )
        )
        checkVersion = float(run_fdbcli_command_dst("getversion"))
        time.sleep(5)

    srcDb = fdb.open(tmpSrcClusterFileName)
    srcDb.options.set_transaction_timeout(timeoutLen)

    dstDb = fdb.open(tmpDstClusterFileName)
    dstDb.options.set_transaction_timeout(timeoutLen)

    for tenantName in tenantNames:
        tmpName = get_tmp_name(tenantName)
        srcTenant = srcDb.open_tenant(tmpName)
        dstTenant = dstDb.open_tenant(tenantName)
        result = verifyData(srcTenant, dstTenant, b"", b"\xff", args.tenant_group)
        if result:
            print(
                "Data verification succeeded for tenants {} and {}.".format(
                    tenantName, tmpName
                )
            )
        else:
            print(
                "Data verification failed for tenants {} and {}.".format(
                    tenantName, tmpName
                )
            )
