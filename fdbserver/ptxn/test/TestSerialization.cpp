/*
 * TestSerialization.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include <cstdint>
#include <vector>

#include "fdbserver/ptxn/Serializer.h"
#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"

struct TestSerializerHeader {
	int item1;
	uint64_t item2;
	uint8_t item3;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, item1, item2, item3);
	}
};

struct TestSerializerItem {
	int item1;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, item1);
	}
};

using TestSerializer = HeaderedItemsSerializerBase<TestSerializerHeader, TestSerializerItem>;

TEST_CASE("/fdbserver/ptxn/test/serialization") {
	TestSerializer serializer;

	// Before writing any properties, the serializer should put a the header placeholder.
	ASSERT_EQ(serializer.getHeaderBytes(), sizeof(int) + sizeof(uint64_t) + sizeof(uint8_t));
	ASSERT_EQ(serializer.getItemsBytes(), 0);
	ASSERT_EQ(serializer.getNumItems(), 0);

	// Write first item
	serializer.writeItem(TestSerializerItem{ 9 });
	ASSERT_EQ(serializer.getNumItems(), 1);
	ASSERT_EQ(serializer.getItemsBytes(), sizeof(int));

	// Write second item
	serializer.writeItem(TestSerializerItem{ 8 });
	ASSERT_EQ(serializer.getNumItems(), 2);
	ASSERT_EQ(serializer.getItemsBytes(), sizeof(int) * 2);
	// Write header
	serializer.completeItemWriting();
	ASSERT(serializer.isWritingCompleted());
	serializer.writeHeader(TestSerializerHeader{ 3, 5, 7 });

	// Deserializer back
	Standalone<StringRef> serialized = serializer.getSerialized();
	TestSerializerHeader header;
	std::vector<TestSerializerItem> items;
	headeredItemDeserializerBase(serialized.arena(), serialized, header, items);
	ASSERT_EQ(header.item1, 3);
	ASSERT_EQ(header.item2, 5);
	ASSERT_EQ(header.item3, 7);
	ASSERT_EQ(items.size(), 2);
	ASSERT_EQ(items[0].item1, 9);
	ASSERT_EQ(items[1].item1, 8);

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/ProxyTLogPushMessageSerializer") {
	using namespace ptxn;

	ProxyTLogPushMessageSerializer serializer;

	const StorageTeamID teamID1{ deterministicRandom()->randomUniqueID() };
	const StorageTeamID teamID2{ deterministicRandom()->randomUniqueID() };

	MutationRef mutation1(MutationRef::SetValue, LiteralStringRef("Key1"), LiteralStringRef("Value1"));
	serializer.writeMessage(mutation1, teamID1);
	MutationRef mutation2(MutationRef::ClearRange, LiteralStringRef("Begin"), LiteralStringRef("End"));
	serializer.writeMessage(mutation2, teamID1);
	MutationRef mutation3(MutationRef::SetValue, LiteralStringRef("Key2"), LiteralStringRef("Value2"));
	serializer.writeMessage(mutation3, teamID2);

	serializer.completeMessageWriting(teamID1);
	serializer.completeMessageWriting(teamID2);

	auto serializedTeam1 = serializer.getSerialized(teamID1);
	auto serializedTeam2 = serializer.getSerialized(teamID2);

	{
		ProxyTLogMessageHeader header;
		std::vector<SubsequenceMutationItem> items;
		proxyTLogPushMessageDeserializer(serializedTeam1.arena(), serializedTeam1, header, items);

		ASSERT(header.protocolVersion == ProxyTLogMessageProtocolVersion);
		ASSERT_EQ(header.numItems, 2);
		ASSERT_EQ(header.length, serializedTeam1.size() - getSerializedBytes<ProxyTLogMessageHeader>());

		ASSERT_EQ(items[0].mutation.type, MutationRef::SetValue);
		ASSERT(items[0].mutation.param1 == LiteralStringRef("Key1"));
		ASSERT(items[0].mutation.param2 == LiteralStringRef("Value1"));

		ASSERT_EQ(items[1].mutation.type, MutationRef::ClearRange);
		ASSERT(items[1].mutation.param1 == LiteralStringRef("Begin"));
		ASSERT(items[1].mutation.param2 == LiteralStringRef("End"));
	}

	{
		ProxyTLogMessageHeader header;
		std::vector<SubsequenceMutationItem> items;
		proxyTLogPushMessageDeserializer(serializedTeam2.arena(), serializedTeam2, header, items);

		ASSERT(header.protocolVersion == ProxyTLogMessageProtocolVersion);
		ASSERT_EQ(header.numItems, 1);

		ASSERT_EQ(items[0].mutation.type, MutationRef::SetValue);
		ASSERT(items[0].mutation.param1 == LiteralStringRef("Key2"));
		ASSERT(items[0].mutation.param2 == LiteralStringRef("Value2"));
	}

	return Void();
}
