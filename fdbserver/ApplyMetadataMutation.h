/*
 * ApplyMetadataMutation.h
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

#ifndef FDBSERVER_APPLYMETADATAMUTATION_H
#define FDBSERVER_APPLYMETADATAMUTATION_H
#pragma once

#include <cstddef>

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/Notified.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ProxyCommitData.actor.h"
#include "flow/FastRef.h"

// Resolver's data for applyMetadataMutations() calls.
struct ResolverData {
	const UID dbgid;
	IKeyValueStore* txnStateStore = nullptr;
	KeyRangeMap<ServerCacheInfo>* keyInfo = nullptr;
	Arena arena;
	// Whether configuration changes. If so, a recovery is forced.
	bool& confChanges;
	bool initialCommit = false;
	Reference<ILogSystem> logSystem = Reference<ILogSystem>();
	LogPushData* toCommit = nullptr;
	Version popVersion = 0; // exclusive, usually set to commitVersion + 1
	std::map<UID, Reference<StorageInfo>>* storageCache = nullptr;
	std::unordered_map<UID, StorageServerInterface>* tssMapping = nullptr;

	// For initial broadcast
	ResolverData(UID debugId, IKeyValueStore* store, KeyRangeMap<ServerCacheInfo>* info, bool& forceRecovery)
	  : dbgid(debugId), txnStateStore(store), keyInfo(info), confChanges(forceRecovery), initialCommit(true) {}

	// For transaction batches that contain metadata mutations
	ResolverData(UID debugId,
	             Reference<ILogSystem> logSystem,
	             IKeyValueStore* store,
	             KeyRangeMap<ServerCacheInfo>* info,
	             LogPushData* toCommit,
	             bool& forceRecovery,
	             Version popVersion,
	             std::map<UID, Reference<StorageInfo>>* storageCache,
	             std::unordered_map<UID, StorageServerInterface>* tssMapping)
	  : dbgid(debugId), txnStateStore(store), keyInfo(info), confChanges(forceRecovery), logSystem(logSystem),
	    toCommit(toCommit), popVersion(popVersion), storageCache(storageCache), tssMapping(tssMapping) {}
};

inline bool isMetadataMutation(MutationRef const& m) {
	// FIXME: This is conservative - not everything in system keyspace is necessarily processed by
	// applyMetadataMutations
	if (m.type == MutationRef::SetValue) {
		return m.param1.size() && m.param1[0] == systemKeys.begin[0] &&
		       !m.param1.startsWith(nonMetadataSystemKeys.begin);
	} else if (m.type == MutationRef::ClearRange) {
		return m.param2.size() > 1 && m.param2[0] == systemKeys.begin[0] &&
		       !nonMetadataSystemKeys.contains(KeyRangeRef(m.param1, m.param2));
	} else {
		return false;
	}
}

Reference<StorageInfo> getStorageInfo(UID id,
                                      std::map<UID, Reference<StorageInfo>>* storageCache,
                                      IKeyValueStore* txnStateStore);

void applyMetadataMutations(SpanID const& spanContext,
                            ProxyCommitData& proxyCommitData,
                            Arena& arena,
                            Reference<ILogSystem> logSystem,
                            const VectorRef<MutationRef>& mutations,
                            LogPushData* pToCommit,
                            bool& confChange,
                            Version version,
                            Version popVersion,
                            bool initialCommit);
void applyMetadataMutations(SpanID const& spanContext,
                            const UID& dbgid,
                            Arena& arena,
                            const VectorRef<MutationRef>& mutations,
                            IKeyValueStore* txnStateStore);

inline bool isSystemKey(KeyRef key) {
	return key.size() && key[0] == systemKeys.begin[0];
}

inline bool containsMetadataMutation(const VectorRef<MutationRef>& mutations) {
	for (auto const& m : mutations) {

		if (m.type == MutationRef::SetValue && isSystemKey(m.param1)) {
			if (m.param1.startsWith(globalKeysPrefix) || (m.param1.startsWith(cacheKeysPrefix)) ||
			    (m.param1.startsWith(configKeysPrefix)) || (m.param1.startsWith(serverListPrefix)) ||
			    (m.param1.startsWith(storageCachePrefix)) || (m.param1.startsWith(serverTagPrefix)) ||
			    (m.param1.startsWith(tssMappingKeys.begin)) || (m.param1.startsWith(tssQuarantineKeys.begin)) ||
			    (m.param1.startsWith(applyMutationsEndRange.begin)) ||
			    (m.param1.startsWith(applyMutationsKeyVersionMapRange.begin)) ||
			    (m.param1.startsWith(logRangesRange.begin)) || (m.param1.startsWith(serverKeysPrefix)) ||
			    (m.param1.startsWith(keyServersPrefix)) || (m.param1.startsWith(cacheKeysPrefix))) {
				return true;
			}
		} else if (m.type == MutationRef::ClearRange && isSystemKey(m.param2)) {
			KeyRangeRef range(m.param1, m.param2);
			if ((keyServersKeys.intersects(range)) || (configKeys.intersects(range)) ||
			    (serverListKeys.intersects(range)) || (tagLocalityListKeys.intersects(range)) ||
			    (serverTagKeys.intersects(range)) || (serverTagHistoryKeys.intersects(range)) ||
			    (range.intersects(applyMutationsEndRange)) || (range.intersects(applyMutationsKeyVersionMapRange)) ||
			    (range.intersects(logRangesRange)) || (tssMappingKeys.intersects(range)) ||
			    (tssQuarantineKeys.intersects(range)) || (range.contains(coordinatorsKey)) ||
			    (range.contains(databaseLockedKey)) || (range.contains(metadataVersionKey)) ||
			    (range.contains(mustContainSystemMutationsKey)) || (range.contains(writeRecoveryKey)) ||
			    (range.intersects(testOnlyTxnStateStorePrefixRange))) {
				return true;
			}
		}
	}
	return false;
}

// Resolver's version
void applyMetadataMutations(SpanID const& spanContext,
                            ResolverData& resolverData,
                            const VectorRef<MutationRef>& mutations);

#endif
