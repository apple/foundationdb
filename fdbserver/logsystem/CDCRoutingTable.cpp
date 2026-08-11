/*
 * CDCRoutingTable.cpp
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

#include "fdbserver/logsystem/CDCRoutingTable.h"

#include "fdbclient/SystemData.h"
#include "fdbserver/kvstore/IKeyValueStore.h"
#include "flow/UnitTest.h"

CDCRoutingTable::CDCRoutingTable() {
	tagsByRange.insert(allKeys, std::set<Tag>());
}

void CDCRoutingTable::updateRange(CDCStreamId streamId, KeyRangeRef const& keys) {
	streams[streamId].keys = KeyRange(keys);
}

bool CDCRoutingTable::updateTag(CDCStreamId streamId, Version version, Tag tag) {
	ASSERT_EQ(tag.locality, tagLocalityCDC);
	auto& existing = streams[streamId].tag;
	if (!existing.present() || version >= existing.get().first) {
		existing = std::make_pair(version, tag);
		return true;
	}
	return false;
}

void CDCRoutingTable::rebuildRanges() {
	tagsByRange.insert(allKeys, std::set<Tag>());
	for (const auto& [streamId, state] : streams) {
		if (!state.keys.present() || !state.tag.present()) {
			continue;
		}
		for (auto range : tagsByRange.modify(state.keys.get())) {
			range->value().insert(state.tag.get().second);
		}
	}
	tagsByRange.coalesce(allKeys);
}

void CDCRoutingTable::setRange(CDCStreamId streamId, KeyRangeRef const& keys) {
	updateRange(streamId, keys);
	rebuildRanges();
}

void CDCRoutingTable::setTag(CDCStreamId streamId, Version version, Tag tag) {
	if (updateTag(streamId, version, tag)) {
		rebuildRanges();
	}
}

void CDCRoutingTable::reload(IKeyValueStore* txnStateStore) {
	streams.clear();
	const RangeResult streamRows = txnStateStore->readRange(cdcStreamKeys).get();
	for (const auto& kv : streamRows) {
		updateRange(decodeCDCStreamKey(kv.key), decodeCDCStreamKeysValue(kv.value));
	}
	const RangeResult tagHistoryRows = txnStateStore->readRange(cdcTagHistoryKeys).get();
	for (const auto& kv : tagHistoryRows) {
		const CDCTagHistoryEntry history = decodeCDCTagHistoryKey(kv.key);
		updateTag(history.streamId, history.version, history.tag);
	}
	rebuildRanges();
}

const std::set<Tag>& CDCRoutingTable::tagsForKey(KeyRef const& key) const {
	return tagsByRange.rangeContaining(key).value();
}

std::set<Tag> CDCRoutingTable::tagsForRange(KeyRangeRef const& keys) const {
	std::set<Tag> tags;
	for (auto range : tagsByRange.intersectingRanges(keys)) {
		tags.insert(range.value().begin(), range.value().end());
	}
	return tags;
}

TEST_CASE("/NativeCDC/RoutingTable") {
	CDCRoutingTable table;
	const Tag ordersTag(tagLocalityCDC, 1);
	const Tag overlappingTag(tagLocalityCDC, 2);
	const Tag rotatedOrdersTag(tagLocalityCDC, 3);

	ASSERT(table.tagsForKey("b"_sr).empty());
	ASSERT(table.tagsForRange(KeyRangeRef("b"_sr, "x"_sr)).empty());

	table.setRange(1, KeyRangeRef("a"_sr, "m"_sr));
	table.setTag(1, 100, ordersTag);
	table.setRange(2, KeyRangeRef("g"_sr, "z"_sr));
	table.setTag(2, 100, overlappingTag);

	ASSERT_EQ(table.tagsForKey("b"_sr), std::set<Tag>{ ordersTag });
	ASSERT_EQ(table.tagsForKey("h"_sr), (std::set<Tag>{ ordersTag, overlappingTag }));
	ASSERT_EQ(table.tagsForKey("x"_sr), std::set<Tag>{ overlappingTag });
	ASSERT_EQ(table.tagsForRange(KeyRangeRef("b"_sr, "x"_sr)), (std::set<Tag>{ ordersTag, overlappingTag }));

	table.setTag(1, 200, rotatedOrdersTag);
	ASSERT_EQ(table.tagsForKey("b"_sr), std::set<Tag>{ rotatedOrdersTag });
	ASSERT_EQ(table.tagsForKey("h"_sr), (std::set<Tag>{ rotatedOrdersTag, overlappingTag }));

	return Void();
}

TEST_CASE("/NativeCDC/RoutingTable/MetadataOrdering") {
	CDCRoutingTable table;
	const Tag rangeFirstTag(tagLocalityCDC, 1);
	const Tag tagFirstTag(tagLocalityCDC, 2);
	const Tag staleTag(tagLocalityCDC, 3);
	const Tag replacementTag(tagLocalityCDC, 4);

	table.setRange(1, KeyRangeRef("a"_sr, "m"_sr));
	ASSERT(table.tagsForKey("b"_sr).empty());

	table.setTag(2, 100, tagFirstTag);
	ASSERT(table.tagsForKey("n"_sr).empty());

	table.setTag(1, 100, rangeFirstTag);
	table.setRange(2, KeyRangeRef("m"_sr, "z"_sr));
	ASSERT_EQ(table.tagsForKey("b"_sr), std::set<Tag>{ rangeFirstTag });
	ASSERT_EQ(table.tagsForKey("n"_sr), std::set<Tag>{ tagFirstTag });

	table.setTag(1, 99, staleTag);
	ASSERT_EQ(table.tagsForKey("b"_sr), std::set<Tag>{ rangeFirstTag });

	table.setTag(1, 100, replacementTag);
	ASSERT_EQ(table.tagsForKey("b"_sr), std::set<Tag>{ replacementTag });

	return Void();
}

TEST_CASE("/NativeCDC/RoutingTable/SharedTagRangeReplacement") {
	CDCRoutingTable table;
	const Tag sharedTag(tagLocalityCDC, 1);

	table.setRange(1, KeyRangeRef("a"_sr, "m"_sr));
	table.setTag(1, 100, sharedTag);
	table.setRange(2, KeyRangeRef("g"_sr, "z"_sr));
	table.setTag(2, 100, sharedTag);

	ASSERT_EQ(table.tagsForKey("h"_sr), std::set<Tag>{ sharedTag });
	ASSERT_EQ(table.tagsForRange(KeyRangeRef("a"_sr, "z"_sr)), std::set<Tag>{ sharedTag });

	table.setRange(1, KeyRangeRef("n"_sr, "t"_sr));
	ASSERT(table.tagsForKey("b"_sr).empty());
	ASSERT_EQ(table.tagsForKey("h"_sr), std::set<Tag>{ sharedTag });
	ASSERT_EQ(table.tagsForKey("p"_sr), std::set<Tag>{ sharedTag });

	return Void();
}
