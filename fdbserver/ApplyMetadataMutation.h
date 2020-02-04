/*
 * ApplyMetadataMutation.h
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

#ifndef FDBSERVER_APPLYMETADATAMUTATION_H
#define FDBSERVER_APPLYMETADATAMUTATION_H
#pragma once

#include "fdbclient/MutationList.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/Notified.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogProtocolMessage.h"

inline bool isMetadataMutation(MutationRef const& m) {
	// FIXME: This is conservative - not everything in system keyspace is necessarily processed by applyMetadataMutations
	return (m.type == MutationRef::SetValue && m.param1.size() && m.param1[0] == systemKeys.begin[0] && !m.param1.startsWith(nonMetadataSystemKeys.begin)) ||
		(m.type == MutationRef::ClearRange && m.param2.size() && m.param2[0] == systemKeys.begin[0] && !nonMetadataSystemKeys.contains(KeyRangeRef(m.param1, m.param2)) );
}

struct applyMutationsData {
	Future<Void> worker;
	Version endVersion;
	Reference<KeyRangeMap<Version>> keyVersion;
};

Reference<StorageInfo> getStorageInfo(UID id, std::map<UID, Reference<StorageInfo>>* storageCache, IKeyValueStore* txnStateStore);

void applyMetadataMutations(UID const& dbgid, Arena &arena, VectorRef<MutationRef> const& mutations, IKeyValueStore* txnStateStore, LogPushData* toCommit, bool *confChange, Reference<ILogSystem> logSystem = Reference<ILogSystem>(), Version popVersion = 0,
	KeyRangeMap<std::set<Key> >* vecBackupKeys = nullptr, KeyRangeMap<ServerCacheInfo>* keyInfo = nullptr, KeyRangeMap<bool>* cacheInfo = nullptr, std::map<Key, applyMutationsData>* uid_applyMutationsData = nullptr, RequestStream<CommitTransactionRequest> commit = RequestStream<CommitTransactionRequest>(),
	Database cx = Database(), NotifiedVersion* commitVersion = nullptr, std::map<UID, Reference<StorageInfo>>* storageCache = nullptr, std::map<Tag, Version>* tag_popped = nullptr, bool initialCommit = false );

#endif
