/*
 * SeedShardServers.cpp
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

#include <algorithm>
#include <map>
#include <vector>

#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/SeedShardServers.h"
#include "flow/Trace.h"

void seedShardServers(Arena& arena, CommitTransactionRef& tr, std::vector<StorageServerInterface> servers) {
	std::map<Optional<Value>, Tag> dcIdLocality;
	std::map<UID, Tag> serverTag;
	int8_t nextLocality = 0;
	for (auto& server : servers) {
		if (!dcIdLocality.contains(server.locality.dcId())) {
			tr.set(arena, tagLocalityListKeyFor(server.locality.dcId()), tagLocalityListValue(nextLocality));
			dcIdLocality[server.locality.dcId()] = Tag(nextLocality, 0);
			nextLocality++;
		}
		Tag& tag = dcIdLocality[server.locality.dcId()];
		serverTag[server.id()] = Tag(tag.locality, tag.id);
		tag.id++;
	}
	std::sort(servers.begin(), servers.end());

	// This isn't strictly necessary, but make sure this is the first transaction.
	tr.read_snapshot = 0;
	tr.read_conflict_ranges.push_back_deep(arena, allKeys);
	KeyBackedObjectMap<UID, StorageMetadataType, decltype(IncludeVersion())> metadataMap(serverMetadataKeys.begin,
	                                                                                     IncludeVersion());
	StorageMetadataType metadata(StorageMetadataType::currentTime());

	for (auto& server : servers) {
		tr.set(arena, serverTagKeyFor(server.id()), serverTagValue(serverTag[server.id()]));
		tr.set(arena, serverListKeyFor(server.id()), serverListValue(server));
		tr.set(arena, metadataMap.packKey(server.id()), metadataMap.packValue(metadata));

		if (SERVER_KNOBS->TSS_HACK_IDENTITY_MAPPING) {
			TraceEvent(SevError, "TSSIdentityMappingEnabled").log();
			Key uidRef = TupleCodec<UID>::pack(server.id());
			tr.set(arena, uidRef.withPrefix(tssMappingKeys.begin), uidRef);
		}
	}
	tr.set(arena, serverMetadataChangeKey, deterministicRandom()->randomUniqueID().toString());

	std::vector<Tag> serverTags;
	std::vector<UID> serverSrcIds;
	serverTags.reserve(servers.size());
	for (auto& server : servers) {
		serverTags.push_back(serverTag[server.id()]);
		serverSrcIds.push_back(server.id());
	}

	auto keyServers = CLIENT_KNOBS->TAG_ENCODE_KEY_SERVERS ? keyServersValue(serverTags)
	                                                       : keyServersValue(RangeResult(), serverSrcIds);
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
		const UID shardId = newDataMoveId(deterministicRandom()->randomUInt64(),
		                                  AssignEmptyRange(false),
		                                  DataMoveType::LOGICAL,
		                                  DataMovementReason::SEED_SHARD_SERVER,
		                                  UnassignShard(false));
		keyServers = keyServersValue(serverSrcIds, /*dest=*/std::vector<UID>(), shardId, UID());
		krmSetPreviouslyEmptyRange(
		    tr, arena, keyServersPrefix, KeyRangeRef(KeyRef(), allKeys.end), keyServers, Value());

		for (auto& server : servers) {
			krmSetPreviouslyEmptyRange(
			    tr, arena, serverKeysPrefixFor(server.id()), allKeys, serverKeysValue(shardId), serverKeysFalse);
		}
	} else {
		krmSetPreviouslyEmptyRange(
		    tr, arena, keyServersPrefix, KeyRangeRef(KeyRef(), allKeys.end), keyServers, Value());

		for (auto& server : servers) {
			krmSetPreviouslyEmptyRange(
			    tr, arena, serverKeysPrefixFor(server.id()), allKeys, serverKeysTrue, serverKeysFalse);
		}
	}
}
