/*
 * TSSMappingUtil.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbserver/core/TSSMappingUtil.h"
Future<Void> readTSSMappingRYW(Reference<ReadYourWritesTransaction> tr,
                               std::map<UID, StorageServerInterface>* tssMapping) {
	KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);
	KeyBackedMap<UID, UID>::RangeResultType uidMapping =
	    co_await tssMapDB.getRange(tr, UID(), Optional<UID>(), CLIENT_KNOBS->TOO_MANY);
	ASSERT(uidMapping.results.size() < CLIENT_KNOBS->TOO_MANY);

	for (auto mapItr = uidMapping.results.begin(); mapItr != uidMapping.results.end(); ++mapItr) {
		UID ssId = mapItr->first;
		Optional<Value> v = co_await tr->get(serverListKeyFor(mapItr->second));
		(*tssMapping)[ssId] = decodeServerListValue(v.get());
	}
}

Future<Void> readTSSMapping(Transaction* tr, std::map<UID, StorageServerInterface>* tssMapping) {
	RangeResult mappingList = co_await tr->getRange(tssMappingKeys, CLIENT_KNOBS->TOO_MANY);
	ASSERT(!mappingList.more && mappingList.size() < CLIENT_KNOBS->TOO_MANY);

	for (auto& it : mappingList) {
		UID ssId = TupleCodec<UID>::unpack(it.key.removePrefix(tssMappingKeys.begin));
		UID tssId = TupleCodec<UID>::unpack(it.value);
		Optional<Value> v = co_await tr->get(serverListKeyFor(tssId));
		(*tssMapping)[ssId] = decodeServerListValue(v.get());
	}
}
