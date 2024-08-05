/*
 * TSSMappingUtil.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbserver/TSSMappingUtil.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> readTSSMappingRYW(Reference<ReadYourWritesTransaction> tr,
                                     std::map<UID, StorageServerInterface>* tssMapping) {
	KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);
	state KeyBackedMap<UID, UID>::RangeResultType uidMapping =
	    wait(tssMapDB.getRange(tr, UID(), Optional<UID>(), CLIENT_KNOBS->TOO_MANY));
	ASSERT(uidMapping.results.size() < CLIENT_KNOBS->TOO_MANY);

	state std::map<UID, StorageServerInterface> mapping;
	state std::vector<std::pair<UID, UID>>::iterator mapItr;
	for (mapItr = uidMapping.results.begin(); mapItr != uidMapping.results.end(); ++mapItr) {
		state UID ssId = mapItr->first;
		Optional<Value> v = wait(tr->get(serverListKeyFor(mapItr->second)));
		(*tssMapping)[ssId] = decodeServerListValue(v.get());
	}
	return Void();
}

ACTOR Future<Void> readTSSMapping(Transaction* tr, std::map<UID, StorageServerInterface>* tssMapping) {
	state RangeResult mappingList = wait(tr->getRange(tssMappingKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!mappingList.more && mappingList.size() < CLIENT_KNOBS->TOO_MANY);

	for (auto& it : mappingList) {
		state UID ssId = TupleCodec<UID>::unpack(it.key.removePrefix(tssMappingKeys.begin));
		UID tssId = TupleCodec<UID>::unpack(it.value);
		Optional<Value> v = wait(tr->get(serverListKeyFor(tssId)));
		(*tssMapping)[ssId] = decodeServerListValue(v.get());
	}
	return Void();
}

ACTOR Future<Void> removeTSSPairsFromCluster(Database cx, std::vector<std::pair<UID, UID>> pairsToRemove) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			for (auto& tssPair : pairsToRemove) {
				// DO NOT remove server list key - that'll break a bunch of stuff. DD will eventually call
				// removeStorageServer
				tr->clear(serverTagKeyFor(tssPair.second));
				tssMapDB.erase(tr, tssPair.first);
			}
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	return Void();
}
