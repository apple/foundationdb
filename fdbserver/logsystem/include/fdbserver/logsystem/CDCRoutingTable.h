/*
 * CDCRoutingTable.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"

class IKeyValueStore;

// Active CDC write routing reconstructed from durable stream and tag-history metadata.
class CDCRoutingTable : NonCopyable {
	struct StreamState {
		std::vector<KeyRange> ranges;
		Optional<std::pair<Version, Tag>> tag;
	};

	std::unordered_map<CDCStreamId, StreamState> streams;
	KeyRangeMap<std::set<Tag>> tagsByRange;

	void updateRanges(CDCStreamId streamId, std::vector<KeyRange> const& ranges);
	bool updateTag(CDCStreamId streamId, Version version, Tag tag);
	void rebuildRanges();

public:
	CDCRoutingTable();
	void setRanges(CDCStreamId streamId, std::vector<KeyRange> const& ranges);
	void setTag(CDCStreamId streamId, Version version, Tag tag);
	void reload(IKeyValueStore* txnStateStore);
	bool empty() const { return streams.empty(); }

	const std::set<Tag>& tagsForKey(KeyRef const& key) const;
	std::set<Tag> tagsForRange(KeyRangeRef const& keys) const;
};
