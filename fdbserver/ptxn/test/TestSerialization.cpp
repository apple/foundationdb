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

#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/Serializer.h"
#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"
#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"
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

struct TestSerializerSectionHeader {
	int item1;
	uint64_t item2;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, item1, item2);
	}
};

struct TestSerializerItem {
	int item1;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, item1);
	}
};

TEST_CASE("/fdbserver/ptxn/test/headeredSerializer") {
	using TestSerializer = ptxn::HeaderedItemsSerializer<TestSerializerHeader, TestSerializerItem>;
	TestSerializer serializer;

	// Before writing any items, the serializer should put a header placeholder.
	ASSERT_EQ(serializer.getHeaderBytes(), ptxn::getSerializedBytes<TestSerializerHeader>());
	ASSERT_EQ(serializer.getTotalBytes(), serializer.getHeaderBytes());
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
	ptxn::headeredItemDeserializerBase(serialized.arena(), serialized, header, items);
	ASSERT_EQ(header.item1, 3);
	ASSERT_EQ(header.item2, 5);
	ASSERT_EQ(header.item3, 7);
	ASSERT_EQ(items.size(), 2);
	ASSERT_EQ(items[0].item1, 9);
	ASSERT_EQ(items[1].item1, 8);

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/twoLevelHeaderedSerializer") {
	using TestSerializer =
	    ptxn::TwoLevelHeaderedItemsSerializer<TestSerializerHeader, TestSerializerSectionHeader, TestSerializerItem>;
	using TestDeserializer =
	    ptxn::TwoLevelHeaderedItemsDeserializer<TestSerializerHeader, TestSerializerSectionHeader, TestSerializerItem>;

	TestSerializer serializer;

	// Test the sanity of initial state
	ASSERT_EQ(serializer.getTotalBytes(), ptxn::getSerializedBytes<TestSerializerHeader>());
	ASSERT(serializer.isSectionCompleted());
	ASSERT(!serializer.isAllItemsCompleted());
	ASSERT_EQ(serializer.getNumItems(), 0);
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 0);
	ASSERT_EQ(serializer.getNumSections(), 0);

	// Open a new section
	serializer.startNewSection();
	ASSERT(!serializer.isSectionCompleted());
	ASSERT_EQ(serializer.getNumSections(), 1);
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 0);
	ASSERT_EQ(serializer.getTotalBytes(),
	          ptxn::getSerializedBytes<TestSerializerHeader>() +
	              ptxn::getSerializedBytes<TestSerializerSectionHeader>());

	// Add new items in the current section
	serializer.writeItem(TestSerializerItem{ 1 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 1);
	ASSERT_EQ(serializer.getNumItems(), 1);

	serializer.writeItem(TestSerializerItem{ 2 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 2);
	ASSERT_EQ(serializer.getNumItems(), 2);

	// Close the section
	serializer.completeSectionWriting();
	ASSERT(serializer.isSectionCompleted());
	serializer.writeSectionHeader(TestSerializerSectionHeader{ 3, 4 });

	// Another new section
	serializer.startNewSection();
	ASSERT(!serializer.isSectionCompleted());
	ASSERT_EQ(serializer.getNumSections(), 2);
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 0);

	// Add new items in the current section
	serializer.writeItem(TestSerializerItem{ 4 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 1);
	ASSERT_EQ(serializer.getNumItems(), 3);

	serializer.writeItem(TestSerializerItem{ 5 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 2);
	ASSERT_EQ(serializer.getNumItems(), 4);

	// Close the section
	serializer.completeSectionWriting();
	ASSERT(serializer.isSectionCompleted());
	serializer.writeSectionHeader(TestSerializerSectionHeader{ 7, 8 });

	// Finalize
	serializer.completeAllItemsWriting();
	ASSERT(serializer.isSectionCompleted());
	ASSERT(serializer.isAllItemsCompleted());
	serializer.writeHeader(TestSerializerHeader{ 10, 11, 12 });

	Standalone<StringRef> serialized = serializer.getSerialized();
	TestDeserializer deserializer(serialized.arena(), serialized);

	TestSerializerHeader mainHeader = deserializer.deserializeAsMainHeader();
	ASSERT_EQ(mainHeader.item1, 10);
	ASSERT_EQ(mainHeader.item2, 11);
	ASSERT_EQ(mainHeader.item3, 12);

	TestSerializerSectionHeader section1Header = deserializer.deserializeAsSectionHeader();
	ASSERT_EQ(section1Header.item1, 3);
	ASSERT_EQ(section1Header.item2, 4);

	TestSerializerItem item;
	item = deserializer.deserializeItem();
	ASSERT_EQ(item.item1, 1);
	item = deserializer.deserializeItem();
	ASSERT_EQ(item.item1, 2);

	TestSerializerSectionHeader section2Header = deserializer.deserializeAsSectionHeader();
	ASSERT_EQ(section2Header.item1, 7);
	ASSERT_EQ(section2Header.item2, 8);

	item = deserializer.deserializeItem();
	ASSERT_EQ(item.item1, 4);
	item = deserializer.deserializeItem();
	ASSERT_EQ(item.item1, 5);

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/ProxyTLogPushMessageSerializer") {
	using namespace ptxn;

	ProxyTLogPushMessageSerializer serializer;

	const TeamID teamID1{ deterministicRandom()->randomUniqueID() };
	const TeamID teamID2{ deterministicRandom()->randomUniqueID() };

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

namespace {

// Serialize a bunch of {version, subsequence, mutation}
void serializeVersionedSubsequencedMutations(
    ptxn::TLogStorageServerMessageSerializer& serializer,
    const std::vector<ptxn::VersionSubsequenceMutation> versionSubsequenceMutations) {
	Version currentVersion = invalidVersion;
	for (const auto& item : versionSubsequenceMutations) {
		if (currentVersion != item.version) {
			if (currentVersion != invalidVersion) {
				serializer.completeVersionWriting();
			}
			currentVersion = item.version;
			serializer.startVersionWriting(currentVersion);
		}
		serializer.writeSubsequenceMutationRef(item.subsequence, item.mutation);
	}

	if (currentVersion != invalidVersion) {
		serializer.completeVersionWriting();
	}

	// Intended to leave the serializer open to new versions
}

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/TLogStorageServerMessageSerializer/basic") {
	using namespace ptxn;

	const std::vector<VersionSubsequenceMutation> VERSIONED_SUBSEQUENCED_MUTATIONS{
		{ 1, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key1"), LiteralStringRef("Value1")) },
		{ 2, 1, MutationRef(MutationRef::SetValue, LiteralStringRef("Key2"), LiteralStringRef("Value2")) },
		{ 2, 2, MutationRef(MutationRef::SetValue, LiteralStringRef("Key3"), LiteralStringRef("Value3")) },
		{ 3, 1, MutationRef(MutationRef::SetValue, LiteralStringRef("Key4"), LiteralStringRef("Value4")) },
		{ 3, 5, MutationRef(MutationRef::SetValue, LiteralStringRef("Key5"), LiteralStringRef("Value5")) },
		{ 3, 7, MutationRef(MutationRef::SetValue, LiteralStringRef("Key6"), LiteralStringRef("Value6")) },
	};

	TeamID teamID = deterministicRandom()->randomUniqueID();
	TLogStorageServerMessageSerializer serializer(teamID);
	Arena arena;

	serializeVersionedSubsequencedMutations(serializer, VERSIONED_SUBSEQUENCED_MUTATIONS);
	serializer.completeMessageWriting();

	auto serialized = serializer.getSerialized();

	TLogStorageServerMessageDeserializer deserializer(serialized.arena(), serialized);

	ASSERT(deserializer.getTeamID() == teamID);
	ASSERT_EQ(deserializer.getNumVersions(), 3);
	ASSERT_EQ(deserializer.getFirstVersion(), 1);
	ASSERT_EQ(deserializer.getLastVersion(), 3);

	// Test the ranged-for loop
	std::cout << std::endl << "Test ranged for" << std::endl;
	{
		int index = 0;
		for (auto item : deserializer) {
			const auto& expected = VERSIONED_SUBSEQUENCED_MUTATIONS[index++];

			std::cout << std::endl << "Expected: " << expected << std::endl;
			std::cout << "Actual:   " << item << std::endl;
			ASSERT(item == expected);
		}

		ASSERT_EQ(index, static_cast<int>(VERSIONED_SUBSEQUENCED_MUTATIONS.size()));
	}

	// Test the iterator for overloaded operators: de-reference/arrow and prefix operator++
	// Also test the re-usability of the iterator, the deserializer should be re-iteratable
	{
		int index = 0;
		for (auto iter = deserializer.begin(); iter != deserializer.end(); ++iter, ++index) {
			const auto& expected = VERSIONED_SUBSEQUENCED_MUTATIONS[index];

			ASSERT(*iter == expected);

			ASSERT_EQ(iter->version, expected.version);
			ASSERT_EQ(iter->subsequence, expected.subsequence);
			// Intended skipping the MutationRef check, as it is done in the ASSERT(*iter == expected) part
		}

		ASSERT_EQ(index, static_cast<int>(VERSIONED_SUBSEQUENCED_MUTATIONS.size()));
	}

	return Void();
}

// Test for version that contains no mutations, also tests postfix operator++
TEST_CASE("/fdbserver/ptxn/test/TLogStorageServerMessageSerializer/emptyVersion") {
	using namespace ptxn;

	const std::vector<VersionSubsequenceMutation> PART1{
		{ 1, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key1"), LiteralStringRef("Value1")) },
		{ 3, 1, MutationRef(MutationRef::SetValue, LiteralStringRef("Key2"), LiteralStringRef("Value2")) },
	};
	const std::vector<VersionSubsequenceMutation> PART2{
		{ 6, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key3"), LiteralStringRef("Value3")) },
	};

	TLogStorageServerMessageSerializer serializer(deterministicRandom()->randomUniqueID());

	serializeVersionedSubsequencedMutations(serializer, PART1);
	serializer.startVersionWriting(4);
	serializer.completeVersionWriting();
	serializer.startVersionWriting(5);
	serializer.completeVersionWriting();
	serializeVersionedSubsequencedMutations(serializer, PART2);
	serializer.startVersionWriting(7);
	serializer.completeVersionWriting();
	serializer.startVersionWriting(8);
	serializer.completeVersionWriting();

	serializer.completeMessageWriting();

	Standalone<StringRef> serialized = serializer.getSerialized();
	TLogStorageServerMessageDeserializer deserializer(serialized);

	auto iter = deserializer.begin();
	ASSERT(*iter == PART1[0]);
	ASSERT(*iter++ == PART1[0]);
	ASSERT(*iter == PART1[1]);
	ASSERT(*iter++ == PART1[1]);
	ASSERT(*iter == PART2[0]);
	ASSERT(*iter++ == PART2[0]);
	ASSERT(iter == deserializer.end());

	return Void();
}

// Test deserialize empty input
TEST_CASE("/fdbserver/ptxn/test/TLogStorageServerMessageSerializer/empty") {
	using namespace ptxn;

	TLogStorageServerMessageSerializer serializer(deterministicRandom()->randomUniqueID());
	serializer.completeMessageWriting();

	auto serialized = serializer.getSerialized();
	TLogStorageServerMessageDeserializer deserializer(serialized.arena(), serialized);

	ASSERT_EQ(deserializer.getNumVersions(), 0);
	ASSERT(deserializer.begin() == deserializer.end());

	return Void();
}

// Test reset the TLogStorageServerMessageSerializer
TEST_CASE("/fdbserver/ptxn/test/TLogStorageServerMessageSerializer/reset") {
	using namespace ptxn;

	TLogStorageServerMessageSerializer serializer1(deterministicRandom()->randomUniqueID());
	const std::vector<VersionSubsequenceMutation> DATA1{
		{ 1, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key1"), LiteralStringRef("Value1")) },
		{ 3, 1, MutationRef(MutationRef::SetValue, LiteralStringRef("Key2"), LiteralStringRef("Value2")) },
	};
	serializeVersionedSubsequencedMutations(serializer1, DATA1);
	serializer1.completeMessageWriting();
	Standalone<StringRef> serialized1 = serializer1.getSerialized();

	TLogStorageServerMessageSerializer serializer2(deterministicRandom()->randomUniqueID());
	const std::vector<VersionSubsequenceMutation> DATA2{
		{ 6, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key3"), LiteralStringRef("Value3")) },
	};
	serializeVersionedSubsequencedMutations(serializer2, DATA2);
	serializer2.completeMessageWriting();
	Standalone<StringRef> serialized2 = serializer2.getSerialized();

	TLogStorageServerMessageDeserializer deserializer(serialized1.arena(), serialized1);
	auto iter1 = deserializer.begin();
	ASSERT(*iter1++ == DATA1[0]);
	ASSERT(*iter1++ == DATA1[1]);
	ASSERT(iter1 == deserializer.end());

	deserializer.reset(serialized2.arena(), serialized2);
	auto iter2 = deserializer.begin();
	ASSERT(*iter2++ == DATA2[0]);
	ASSERT(iter2 == deserializer.end());

	return Void();
}