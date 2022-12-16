#
# tenant_management.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

# FoundationDB Python API

"""Documentation for this API can be found at
https://apple.github.io/foundationdb/api-python.html"""

from fdb import impl as _impl

_tenant_map_prefix = b"\xff\xff/management/tenant/map/"


# If the existence_check_marker is an empty list, then check whether the tenant exists.
# After the check, append an item to the existence_check_marker list so that subsequent
# calls to this function will not perform the existence check.
#
# If the existence_check_marker is a non-empty list, return None.
def _check_tenant_existence(tr, key, existence_check_marker, force_maybe_commited):
    if not existence_check_marker:
        existing_tenant = tr[key].wait()
        existence_check_marker.append(None)
        if force_maybe_commited:
            raise _impl.FDBError(1021)  # maybe_committed
        return existing_tenant != None

    return None


# Attempt to create a tenant in the cluster. If existence_check_marker is an empty
# list, then this function will check if the tenant already exists and fail if it does.
# Once the existence check is completed, it will not be done again if this function
# retries. As a result, this function may return successfully if the tenant is created
# by someone else concurrently. This behavior allows the operation to be idempotent with
# respect to retries.
#
# If the existence_check_marker is a non-empty list, then the existence check is skipped.
@_impl.transactional
def _create_tenant_impl(
    tr, tenant_name, existence_check_marker, force_existence_check_maybe_committed=False
):
    tr.options.set_special_key_space_enable_writes()
    key = b"%s%s" % (_tenant_map_prefix, tenant_name)

    if (
        _check_tenant_existence(
            tr, key, existence_check_marker, force_existence_check_maybe_committed
        )
        is True
    ):
        raise _impl.FDBError(2132)  # tenant_already_exists

    tr[key] = b""


# Attempt to delete a tenant from the cluster. If existence_check_marker is an empty
# list, then this function will check if the tenant already exists and fail if it does
# not. Once the existence check is completed, it will not be done again if this function
# retries. As a result, this function may return successfully if the tenant is deleted
# by someone else concurrently. This behavior allows the operation to be idempotent with
# respect to retries.
#
# If the existence_check_marker is a non-empty list, then the existence check is skipped.
@_impl.transactional
def _delete_tenant_impl(
    tr, tenant_name, existence_check_marker, force_existence_check_maybe_committed=False
):
    tr.options.set_special_key_space_enable_writes()
    key = b"%s%s" % (_tenant_map_prefix, tenant_name)

    if (
        _check_tenant_existence(
            tr, key, existence_check_marker, force_existence_check_maybe_committed
        )
        is False
    ):
        raise _impl.FDBError(2131)  # tenant_not_found

    del tr[key]


class FDBTenantList(object):
    """Iterates over the results of list_tenants query. Returns
    KeyValue objects.

    """

    def __init__(self, rangeresult):
        self._range = rangeresult
        self._iter = iter(self._range)

    def to_list(self):
        return list(self.__iter__())

    def __iter__(self):
        for next_item in self._iter:
            tenant_name = _impl.remove_prefix(next_item.key, _tenant_map_prefix)
            yield _impl.KeyValue(tenant_name, next_item.value)


# Lists the tenants created in the cluster, specified by the begin and end range.
# Also limited in number of results by the limit parameter.
# Returns an iterable object that yields KeyValue objects
# where the keys are the tenant names and the values are the unprocessed
# JSON strings of the tenant metadata
@_impl.transactional
def _list_tenants_impl(tr, begin, end, limit):
    tr.options.set_raw_access()
    begin_key = b"%s%s" % (_tenant_map_prefix, begin)
    end_key = b"%s%s" % (_tenant_map_prefix, end)

    rangeresult = tr.get_range(begin_key, end_key, limit)

    return FDBTenantList(rangeresult)


def create_tenant(db_or_tr, tenant_name):
    tenant_name = _impl.process_tenant_name(tenant_name)

    # Only perform the existence check when run using a database
    # Callers using a transaction are expected to check existence themselves if required
    existence_check_marker = (
        [] if not isinstance(db_or_tr, _impl.TransactionRead) else [None]
    )
    _create_tenant_impl(db_or_tr, tenant_name, existence_check_marker)


def delete_tenant(db_or_tr, tenant_name):
    tenant_name = _impl.process_tenant_name(tenant_name)

    # Only perform the existence check when run using a database
    # Callers using a transaction are expected to check existence themselves if required
    existence_check_marker = (
        [] if not isinstance(db_or_tr, _impl.TransactionRead) else [None]
    )
    _delete_tenant_impl(db_or_tr, tenant_name, existence_check_marker)


def list_tenants(db_or_tr, begin, end, limit):
    begin = _impl.process_tenant_name(begin)
    end = _impl.process_tenant_name(end)

    return _list_tenants_impl(db_or_tr, begin, end, limit)
