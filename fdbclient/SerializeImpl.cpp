/*
 * clientworkerinterface.h
 *
 * this source file is part of the foundationdb open source project
 *
 * copyright 2013-2018 apple inc. and the foundationdb project authors
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 *     http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 */

#include "fdbclient/ClientWorkerInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "flow/SerializeImpl.h"

MAKE_SERIALIZABLE(ClientWorkerInterface);
MAKE_SERIALIZABLE(RebootRequest);
MAKE_SERIALIZABLE(ProfilerRequest);

MAKE_SERIALIZABLE(ClusterInterface);
MAKE_SERIALIZABLE(OpenDatabaseRequest);
MAKE_SERIALIZABLE(FailureMonitoringRequest);
MAKE_SERIALIZABLE(StatusRequest);
MAKE_SERIALIZABLE(GetClientWorkersRequest);
MAKE_SERIALIZABLE(ForceRecoveryRequest);

MAKE_SERIALIZABLE(GetLeaderRequest);
MAKE_SERIALIZABLE(LeaderInfo);

MAKE_SERIALIZABLE(MasterProxyInterface);
MAKE_SERIALIZABLE(CommitTransactionRequest);
MAKE_SERIALIZABLE(GetReadVersionRequest);
MAKE_SERIALIZABLE(GetKeyServerLocationsRequest);
MAKE_SERIALIZABLE(GetStorageServerRejoinInfoRequest);
MAKE_SERIALIZABLE(GetRawCommittedVersionRequest);
MAKE_SERIALIZABLE(TxnStateRequest);
MAKE_SERIALIZABLE(GetHealthMetricsRequest);
MAKE_SERIALIZABLE(ExecRequest);
MAKE_SERIALIZABLE(ProxySnapRequest);
MAKE_SERIALIZABLE(CommitID);

MAKE_SERIALIZABLE(StorageServerInterface);
MAKE_SERIALIZABLE(GetValueRequest);
MAKE_SERIALIZABLE(GetKeyRequest);
MAKE_SERIALIZABLE(GetKeyValuesRequest);
MAKE_SERIALIZABLE(GetShardStateRequest);
MAKE_SERIALIZABLE(WaitMetricsRequest);
MAKE_SERIALIZABLE(SplitMetricsRequest);
MAKE_SERIALIZABLE(GetPhysicalMetricsRequest);
MAKE_SERIALIZABLE(StorageQueuingMetricsRequest);
MAKE_SERIALIZABLE(WatchValueRequest);
MAKE_SERIALIZABLE(GetValueReply);
MAKE_SERIALIZABLE(GetKeyValuesReply);
MAKE_SERIALIZABLE(GetKeyReply);
MAKE_SERIALIZABLE(SplitMetricsReply);
MAKE_SERIALIZABLE(GetPhysicalMetricsReply);
MAKE_SERIALIZABLE(StorageQueuingMetricsReply);

MAKE_SERIALIZABLE(KeyValueStoreType);

template struct SerializedMsg<BinaryWriter, ErrorOr<EnsureTable<Void>>>;
template struct SerializedMsg<PacketWriter, ErrorOr<EnsureTable<Void>>>;
template struct SerializedMsg<ArenaReader, ReplyPromise<KeyValueStoreType>>;
template struct ObjectSerializedMsg<ReplyPromise<KeyValueStoreType>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<StatusReply>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<ClientDBInfo>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<StorageMetrics>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<GetReadVersionReply>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<GetHealthMetricsReply>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<FailureMonitoringReply>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<GetKeyServerLocationsReply>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<Optional<LeaderInfo>>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<std::pair<long long, long long>>>>;
template struct ObjectSerializedMsg<ErrorOr<EnsureTable<std::vector<ClientWorkerInterface>>>>;
