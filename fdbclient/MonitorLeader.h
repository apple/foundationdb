/*
 * MonitorLeader.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "FDBTypes.h"
#include "CoordinationInterface.h"

#define CLUSTER_FILE_ENV_VAR_NAME "FDB_CLUSTER_FILE"

class ClientCoordinators;

template <class LeaderInterface>
Future<Void> monitorLeader( Reference<ClusterConnectionFile> const& connFile, Reference<AsyncVar<Optional<LeaderInterface>>> const& outKnownLeader );
// Monitors the given coordination group's leader election process and provides a best current guess
// of the current leader.  If a leader is elected for long enough and communication with a quorum of
// coordinators is possible, eventually outKnownLeader will be that leader's interface.

#pragma region Implementation

Future<Void> monitorLeaderInternal( Reference<ClusterConnectionFile> const& connFile, Reference<AsyncVar<Value>> const& outSerializedLeaderInfo );

template <class LeaderInterface>
Future<Void> monitorLeader( Reference<ClusterConnectionFile> const& connFile, Reference<AsyncVar<Optional<LeaderInterface>>> const& outKnownLeader ) {
	Reference<AsyncVar<Value>> serializedInfo( new AsyncVar<Value> );
	Future<Void> m = monitorLeaderInternal( connFile, serializedInfo );
	return m || asyncDeserialize( serializedInfo, outKnownLeader );
}

#pragma endregion

#endif