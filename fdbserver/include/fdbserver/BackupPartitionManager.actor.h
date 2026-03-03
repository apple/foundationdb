/*
 * BackupPartitionManager.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BACKUPPARTITIONMANAGER_ACTOR_G_H)
#define FDBSERVER_BACKUPPARTITIONMANAGER_ACTOR_G_H
#include "fdbserver/BackupPartitionManager.actor.g.h"
#elif !defined(FDBSERVER_BACKUPPARTITIONMANAGER_ACTOR_H)
#define FDBSERVER_BACKUPPARTITIONMANAGER_ACTOR_H

#include "fdbserver/BackupPartitionMap.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbclient/CommitProxyInterface.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Request to update backup partition map on CommitProxy
struct BackupPartitionMapRequest {
	constexpr static FileIdentifier file_identifier = 2847362;
	BackupPartitionMap partitionMap;
	Version version;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, partitionMap, version, reply);
	}
};

// Forward declaration
struct DataDistributor;

// Main backup partition manager actor
ACTOR Future<Void> backupPartitionManager(Reference<DataDistributor> self);

// Actor that broadcasts partition map to CommitProxies
ACTOR Future<Void> broadcastPartitionMapToProxies(Reference<DataDistributor> self, BackupPartitionMap partitionMap);

// Actor that cleans up old partition maps
ACTOR Future<Void> cleanupOldPartitionMaps(Reference<DataDistributor> self);

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_BACKUPPARTITIONMANAGER_ACTOR_H