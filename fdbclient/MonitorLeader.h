/*
 * MonitorLeader.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FDBCLIENT_MONITORLEADER_H
#define FDBCLIENT_MONITORLEADER_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/CommitProxyInterface.h"

#define CLUSTER_FILE_ENV_VAR_NAME "FDB_CLUSTER_FILE"

class ClientCoordinators;

struct ClientStatusInfo {
	Key traceLogGroup;
	Standalone<VectorRef<ClientVersionRef>> versions;
	Standalone<VectorRef<StringRef>> issues;

	ClientStatusInfo() {}
	ClientStatusInfo(Key const& traceLogGroup,
	                 Standalone<VectorRef<ClientVersionRef>> versions,
	                 Standalone<VectorRef<StringRef>> issues)
	  : traceLogGroup(traceLogGroup), versions(versions), issues(issues) {}
};

struct ClientData {
	std::map<NetworkAddress, ClientStatusInfo> clientStatusInfoMap;
	Reference<AsyncVar<CachedSerialization<ClientDBInfo>>> clientInfo;

	OpenDatabaseRequest getRequest();

	ClientData() : clientInfo(makeReference<AsyncVar<CachedSerialization<ClientDBInfo>>>()) {}
};

struct MonitorLeaderInfo {
	bool hasConnected;
	Reference<IClusterConnectionRecord> intermediateConnRecord;

	MonitorLeaderInfo() : hasConnected(false) {}
	explicit MonitorLeaderInfo(Reference<IClusterConnectionRecord> intermediateConnRecord)
	  : hasConnected(false), intermediateConnRecord(intermediateConnRecord) {}
};

Optional<std::pair<LeaderInfo, bool>> getLeader(const std::vector<Optional<LeaderInfo>>& nominees);

// This is one place where the leader election algorithm is run. The coodinator contacts all coodinators to collect
// nominees, the nominee with the most nomination is the leader. This function also monitors the change of the leader.
// If a leader is elected for long enough and communication with a quorum of coordinators is possible, eventually
// outKnownLeader will be that leader's interface.
template <class LeaderInterface>
Future<Void> monitorLeader(Reference<IClusterConnectionRecord> const& connFile,
                           Reference<AsyncVar<Optional<LeaderInterface>>> const& outKnownLeader);

// This is one place where the leader election algorithm is run. The coodinator contacts all coodinators to collect
// nominees, the nominee with the most nomination is the leader, and collects client data from the leader. This function
// also monitors the change of the leader.
Future<Void> monitorLeaderAndGetClientInfo(Key const& clusterKey,
                                           std::vector<NetworkAddress> const& coordinators,
                                           ClientData* const& clientData,
                                           Reference<AsyncVar<Optional<LeaderInfo>>> const& leaderInfo,
                                           Reference<AsyncVar<Void>> const& coordinatorsChanged);

Future<Void> monitorProxies(
    Reference<AsyncVar<Reference<IClusterConnectionRecord>>> const& connRecord,
    Reference<AsyncVar<ClientDBInfo>> const& clientInfo,
    Reference<AsyncVar<Optional<ClientLeaderRegInterface>>> const& coordinator,
    Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> const& supportedVersions,
    Key const& traceLogGroup);

void shrinkProxyList(ClientDBInfo& ni,
                     std::vector<UID>& lastCommitProxyUIDs,
                     std::vector<CommitProxyInterface>& lastCommitProxies,
                     std::vector<UID>& lastGrvProxyUIDs,
                     std::vector<GrvProxyInterface>& lastGrvProxies);

#ifndef __INTEL_COMPILER
#pragma region Implementation
#endif

Future<Void> monitorLeaderInternal(Reference<IClusterConnectionRecord> const& connRecord,
                                   Reference<AsyncVar<Value>> const& outSerializedLeaderInfo);

template <class LeaderInterface>
struct LeaderDeserializer {
	Future<Void> operator()(const Reference<AsyncVar<Value>>& serializedInfo,
	                        const Reference<AsyncVar<Optional<LeaderInterface>>>& outKnownLeader) {
		return asyncDeserialize(serializedInfo, outKnownLeader);
	}
};

Future<Void> asyncDeserializeClusterInterface(const Reference<AsyncVar<Value>>& serializedInfo,
                                              const Reference<AsyncVar<Optional<ClusterInterface>>>& outKnownLeader);

template <>
struct LeaderDeserializer<ClusterInterface> {
	Future<Void> operator()(const Reference<AsyncVar<Value>>& serializedInfo,
	                        const Reference<AsyncVar<Optional<ClusterInterface>>>& outKnownLeader) {
		return asyncDeserializeClusterInterface(serializedInfo, outKnownLeader);
	}
};

template <class LeaderInterface>
Future<Void> monitorLeader(Reference<IClusterConnectionRecord> const& connRecord,
                           Reference<AsyncVar<Optional<LeaderInterface>>> const& outKnownLeader) {
	LeaderDeserializer<LeaderInterface> deserializer;
	auto serializedInfo = makeReference<AsyncVar<Value>>();
	Future<Void> m = monitorLeaderInternal(connRecord, serializedInfo);
	return m || deserializer(serializedInfo, outKnownLeader);
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

#endif
