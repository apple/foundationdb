#
# fdb_grpc_interface.py
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

# FoundationDB Python GRPC client interface

import grpc

from fdb.grpc import kv_service_pb2
from fdb.grpc import kv_service_pb2_grpc
from fdb.impl import FDBError

from fdb.fdb_client_interface import FdbClientInterface, FutureInterface, DatabaseInterface, TransactionInterface

def get_fdb_error(e):
    # TODO: when using Any type, check kv_service_pb2.FdbError.DESCRIPTOR
    for k,v in e.trailing_metadata():
        if k == 'grpc-status-details-bin':
            rpc_error = kv_service_pb2.FdbError.FromString(v)
            return FDBError(rpc_error.code)

    return None

_grpc_channel = None
_grpc_service = None

class FdbGrpcClient(FdbClientInterface):
    def __init__(self, connection_string):
        global _grpc_channel
        global _grpc_service
        _grpc_channel = grpc.insecure_channel(connection_string)
        _grpc_service = kv_service_pb2_grpc.FdbKvClientStub(_grpc_channel)   

    def select_api_version(self, version, header_version):
        return 0

    def get_max_api_version(self):
        raise NotImplementedError()

    def get_error(self, error_code):
        pass

    def error_predicate(self, predicate, error_code):
        raise NotImplementedError()

    def setup_network(self):
        pass

    def network_set_option(self, option, param, length):
        raise NotImplementedError()

    def run_network(self):
        pass

    def stop_network(self):
        _channel = None
        _grpc_service = None
        
    def create_database(self, connection_string):
        return GrpcDatabase()

class GrpcFuture(FutureInterface):
    def __init__(self, future, extract):
        self._future = future
        self._extract = extract

    def destroy(self):
        pass

    def release_memory(self):
        pass

    def cancel(self):
        raise NotImplementedError()

    def is_ready(self):
        return self._future.done()

    def set_callback(self, callback):
        self._future.add_done_callback(callback)

    def get_error(self):
        try:
            self._future.result()
        except grpc.RpcError as e:
            fdb_error = get_fdb_error(e)
            if fdb_error is not None:
                raise fdb_error
            else:
                raise e

    def get_int64(self):
        return self._extract(self._future.result())

    def get_key(self):
        return self._extract(self._future.result())

    def get_value(self):
        (present, value) = self._extract(self._future.result())
        if present:
            return value
        else:
            return None

    def get_keyvalue_array(self):
        raise NotImplementedError()

    def get_string_array(self):
        raise NotImplementedError()

class GrpcDatabase(DatabaseInterface):
    def destroy(self):
        pass

    def create_transaction(self):
        response_future = _grpc_service.CreateTransaction.future(kv_service_pb2.CreateTransactionRequest())
        return GrpcTransaction(response_future)

    def set_option(self, option, param, length):
        raise NotImplementedError()

class GrpcTransaction(TransactionInterface):
    def __init__(self, tr_future):
        self._committed_version = None
        self._versionstamp = None
        self._tr_future = tr_future
        self._pending_mutations = []

    def _tr(self):
        return self._tr_future.result().transactionId

    def _pending_mutations_set(self):
        mutations = kv_service_pb2.OrderedMutationSet(mutations=self._pending_mutations)
        self._pending_mutations = []
        return mutations

    def destroy(self):
        pass

    def cancel(self):
        raise NotImplementedError()

    def set_read_version(self, version):
        raise NotImplementedError()

    def get_read_version(self):
        raise NotImplementedError()

    def get(self, key, snapshot):
        response_future = _grpc_service.GetValue.future(
                            kv_service_pb2.GetValueRequest(
                                transactionId=self._tr(), 
                                key=key, 
                                snapshot=snapshot,
                                mutations=self._pending_mutations_set()))

        return GrpcFuture(response_future, lambda v: (v.present, v.value))

    def get_key(self, key_selector, snapshot):
        raise NotImplementedError()

    def get_range(self, begin, end, limit, streaming_mode, iteration, snapshot, reverse):
        raise NotImplementedError()

    def get_estimated_range_size_bytes(self, begin, end):
        raise NotImplementedError()

    def add_conflict_range(self, begin, end, type):
        raise NotImplementedError()

    def get_addresses_for_key(self, key):
        raise NotImplementedError()

    def set_option(self, option, param, length):
        raise NotImplementedError()

    def atomic_op(self, key, param, opcode):
        self._pending_mutations.append(
            kv_service_pb2.Mutation(mutationType=opcode, param1=key, param2=param))

    def set(self, key, value):
        self._pending_mutations.append(
            kv_service_pb2.Mutation(mutationType=kv_service_pb2.Mutation.MutationType.SET, param1=key, param2=value))

    def clear(self, key):
        self.clear_range(key, key+'\x00')

    def clear_range(self, begin, end):
        self._pending_mutations.append(
            kv_service_pb2.Mutation(mutationType=kv_service_pb2.Mutation.MutationType.CLEAR, param1=begin, param2=end))

    def watch(self, key):
        raise NotImplementedError()

    def _post_commit(self, f):
        self._committed_version = f.result().committedVersion
        self._versionstamp = f.result().versionstamp

    def commit(self):
        response_future = _grpc_service.Commit.future(
                            kv_service_pb2.CommitRequest(
                                transactionId=self._tr(),
                                mutations=self._pending_mutations_set()))

        response_future.add_done_callback(self._post_commit)
        return GrpcFuture(response_future, None)

    def get_committed_version(self):
        return self._committed_version

    def get_approximate_size(self):
        raise NotImplementedError()

    def get_versionstamp(self):
        raise NotImplementedError()

    def on_error(self, error_code):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()
