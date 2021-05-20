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
#include <sys/time.h>
#include <vector>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/SpanContextMessage.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/Serializer.h"
#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"
#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/serialize.h"

namespace {

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

bool testSubsequenceMutationItem() {
	ptxn::ProxyTLogPushMessageSerializer serializer;

	const ptxn::StorageTeamID team1{ ptxn::test::getNewStorageTeamID() };
	const ptxn::StorageTeamID team2{ ptxn::test::getNewStorageTeamID() };

	MutationRef mutation(MutationRef::SetValue, "KeyXX"_sr, "ValueYY"_sr);
	serializer.writeMessage(mutation, team1);
	serializer.completeMessageWriting(team1);
	auto serializedTeam1 = serializer.getSerialized(team1);

	BinaryWriter wr(IncludeVersion(ProtocolVersion::withPartitionTransaction()));
	wr << mutation;
	Standalone<StringRef> value = wr.toValue();
	StringRef m = value.substr(sizeof(uint64_t)); // skip protocol version

	// Have to use a different serializer to make sure subsequence is the same.
	ptxn::ProxyTLogPushMessageSerializer serializer2;
	serializer2.writeMessage(m, team2);
	serializer2.completeMessageWriting(team2);
	auto serializedTeam2 = serializer2.getSerialized(team2);

	ASSERT(serializedTeam1 == serializedTeam2);

	std::cout << __FUNCTION__ << " passed.\n";
	return true;
}

// Verify transaction info (span IDs) are properly added to serialized data and
// later can be deserialized.
bool testTransactionInfo() {
	ptxn::ProxyTLogPushMessageSerializer serializer;

	const ptxn::StorageTeamID team1{ ptxn::test::getNewStorageTeamID() };
	const ptxn::StorageTeamID team2{ ptxn::test::getNewStorageTeamID() };
	const ptxn::StorageTeamID team3{ ptxn::test::getNewStorageTeamID() };
	std::vector<ptxn::StorageTeamID> teams = { team1, team2, team3 };

	// Mutations for each team before serialization, i.e., ground truth
	Arena arena;
	std::vector<VectorRef<MutationRef>> mutations(3);

	SpanID spanId;
	std::vector<SpanID> written(3, spanId); // last written SpanID
	MutationRef transactionInfo;
	transactionInfo.type = MutationRef::Reserved_For_SpanContextMessage;

	const int totalMutations = 1000;
	for (int i = 0; i < totalMutations; i++) {
		if (deterministicRandom()->random01() < 0.1) {
			// Add transaction info
			spanId = deterministicRandom()->randomUniqueID();
			serializer.addTransactionInfo(spanId);
			transactionInfo.param1 = StringRef(arena, BinaryWriter::toValue(spanId, Unversioned()));
		} else {
			// Add a real mutation
			StringRef key = StringRef(arena, deterministicRandom()->randomAlphaNumeric(10));
			StringRef value = StringRef(arena, deterministicRandom()->randomAlphaNumeric(32));
			MutationRef m(MutationRef::SetValue, key, value);

			// Randomly add this mutation to teams
			for (int team = 0; team < 3; team++) {
				if (deterministicRandom()->random01() < 0.5) {
					if (written[team] != spanId && FLOW_KNOBS->WRITE_TRACING_ENABLED) {
						// Add Transaction Info for this team in ground truth
						mutations[team].push_back(arena, transactionInfo);
						written[team] = spanId;
					}
					serializer.writeMessage(m, teams[team]);
					mutations[team].push_back(arena, m);
				}
			}
		}
	}

	auto results = serializer.getAllSerialized();
	int spanCount = 0;
	for (const auto& [team, messages] : results) {
		VectorRef<MutationRef> teamMutations;
		if (team == team1) {
			teamMutations = mutations[0];
		} else if (team == team2) {
			teamMutations = mutations[1];
		} else if (team == team3) {
			teamMutations = mutations[2];
		} else {
			ASSERT(false);
		}

		// deserialize message
		ptxn::ProxyTLogMessageHeader header;
		std::vector<ptxn::SubsequenceMutationItem> seqMutations;
		proxyTLogPushMessageDeserializer(arena, messages, header, seqMutations);
		ASSERT_EQ(teamMutations.size(), seqMutations.size());
		for (int i = 0; i < teamMutations.size(); i++) {
			const MutationRef& a = teamMutations[i];
			if (seqMutations[i].isMutation()) {
				const MutationRef& b = seqMutations[i].mutation();
				ASSERT(a == b);
			} else {
				ASSERT(a.type == MutationRef::Reserved_For_SpanContextMessage);
				const SpanContextMessage& span = seqMutations[i].span();
				StringRef str(arena, BinaryWriter::toValue(span.spanContext, Unversioned()));
				ASSERT(a.param1 == str);
				spanCount++;
			}
		}
	}
	ASSERT(spanCount > 0 || !FLOW_KNOBS->WRITE_TRACING_ENABLED);

	std::cout << __FUNCTION__ << " passed (" << spanCount << " SpanIDs).\n";
	return true;
}

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/headeredSerializer") {
	using TestSerializer = ptxn::HeaderedItemsSerializer<TestSerializerHeader, TestSerializerItem>;
	TestSerializer serializer;

	// Before writing any items, the serializer should put a header placeholder.
	ASSERT_EQ(serializer.getHeaderBytes(), ptxn::getSerializedBytes<TestSerializerHeader>());
	ASSERT_EQ(serializer.getTotalBytes(), ptxn::SerializerVersionOptionBytes + serializer.getHeaderBytes());
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
	ASSERT_EQ(serializer.getTotalBytes(),
	          ptxn::SerializerVersionOptionBytes + ptxn::getSerializedBytes<TestSerializerHeader>());
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
	          ptxn::SerializerVersionOptionBytes + ptxn::getSerializedBytes<TestSerializerHeader>() +
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

	const StorageTeamID storageTeamID1{ deterministicRandom()->randomUniqueID() };
	const StorageTeamID storageTeamID2{ deterministicRandom()->randomUniqueID() };

	MutationRef mutation1(MutationRef::SetValue, LiteralStringRef("Key1"), LiteralStringRef("Value1"));
	serializer.writeMessage(mutation1, storageTeamID1);
	MutationRef mutation2(MutationRef::ClearRange, LiteralStringRef("Begin"), LiteralStringRef("End"));
	serializer.writeMessage(mutation2, storageTeamID1);
	MutationRef mutation3(MutationRef::SetValue, LiteralStringRef("Key2"), LiteralStringRef("Value2"));
	serializer.writeMessage(mutation3, storageTeamID2);

	serializer.completeMessageWriting(storageTeamID1);
	serializer.completeMessageWriting(storageTeamID2);

	auto serializedTeam1 = serializer.getSerialized(storageTeamID1);
	auto serializedTeam2 = serializer.getSerialized(storageTeamID2);

	{
		ProxyTLogMessageHeader header;
		std::vector<SubsequenceMutationItem> items;
		proxyTLogPushMessageDeserializer(serializedTeam1.arena(), serializedTeam1, header, items);

		ASSERT(header.protocolVersion == ProxyTLogMessageProtocolVersion);
		ASSERT_EQ(header.numItems, 2);
		ASSERT_EQ(header.length,
		          serializedTeam1.size() - getSerializedBytes<ProxyTLogMessageHeader>() - SerializerVersionOptionBytes);

		MutationRef m = items[0].mutation();
		ASSERT_EQ(m.type, MutationRef::SetValue);
		ASSERT(m.param1 == "Key1"_sr);
		ASSERT(m.param2 == "Value1"_sr);

		m = items[1].mutation();
		ASSERT_EQ(m.type, MutationRef::ClearRange);
		ASSERT(m.param1 == "Begin"_sr);
		ASSERT(m.param2 == "End"_sr);
	}

	{
		ProxyTLogMessageHeader header;
		std::vector<SubsequenceMutationItem> items;
		proxyTLogPushMessageDeserializer(serializedTeam2.arena(), serializedTeam2, header, items);

		ASSERT(header.protocolVersion == ProxyTLogMessageProtocolVersion);
		ASSERT_EQ(header.numItems, 1);

		MutationRef m = items[0].mutation();
		ASSERT_EQ(m.type, MutationRef::SetValue);
		ASSERT(m.param1 == "Key2"_sr);
		ASSERT(m.param2 == "Value2"_sr);
	}

	ASSERT(testSubsequenceMutationItem());
	ASSERT(testTransactionInfo());

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

// Basic test for the TLogStorageServerMessageSerializer
bool testTLogStorageServerMessageSerializer() {
	std::cout << __FUNCTION__ << ">> Test started" << std::endl;

	using namespace ptxn;

	const std::vector<VersionSubsequenceMutation> VERSIONED_SUBSEQUENCED_MUTATIONS{
		{ 1, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key1"), LiteralStringRef("Value1")) },
		{ 2, 1, MutationRef(MutationRef::SetValue, LiteralStringRef("Key2"), LiteralStringRef("Value2")) },
		{ 2, 2, MutationRef(MutationRef::SetValue, LiteralStringRef("Key3"), LiteralStringRef("Value3")) },
		{ 3, 1, MutationRef(MutationRef::SetValue, LiteralStringRef("Key4"), LiteralStringRef("Value4")) },
		{ 3, 5, MutationRef(MutationRef::SetValue, LiteralStringRef("Key5"), LiteralStringRef("Value5")) },
		{ 3, 7, MutationRef(MutationRef::SetValue, LiteralStringRef("Key6"), LiteralStringRef("Value6")) },
	};

	StorageTeamID storageTeamID = deterministicRandom()->randomUniqueID();
	TLogStorageServerMessageSerializer serializer(storageTeamID);
	Arena arena;

	serializeVersionedSubsequencedMutations(serializer, VERSIONED_SUBSEQUENCED_MUTATIONS);
	serializer.completeMessageWriting();

	auto serialized = serializer.getSerialized();

	TLogStorageServerMessageDeserializer deserializer(serialized.arena(), serialized);

	ASSERT(deserializer.getStorageTeamID() == storageTeamID);
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

	return true;
}

// Test for version that contains no mutations, also tests postfix operator++
bool testTLogStorageServerMessageSerializer_VersionWithNoMutation() {
	std::cout << __FUNCTION__ << ">> Test started" << std::endl;

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

	return true;
}

// Test deserialize empty input
bool testTLogStorageServerMessageSerializer_EmptyInput() {
	std::cout << __FUNCTION__ << ">> Test started" << std::endl;

	using namespace ptxn;

	TLogStorageServerMessageSerializer serializer(deterministicRandom()->randomUniqueID());
	serializer.completeMessageWriting();

	auto serialized = serializer.getSerialized();
	TLogStorageServerMessageDeserializer deserializer(serialized.arena(), serialized);

	ASSERT_EQ(deserializer.getNumVersions(), 0);
	ASSERT(deserializer.begin() == deserializer.end());

	return true;
}

// Test reset the TLogStorageServerMessageSerializer
bool testTLogStorageServerMessageSerializer_Reset() {
	std::cout << __FUNCTION__ << ">> Test started" << std::endl;

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

	return true;
}

double getTime() {
	static struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_usec / 1000000.0 + tv.tv_sec;
}

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/TLogStorageServerMessageSerializer/serialization") {
	ASSERT(testTLogStorageServerMessageSerializer());
	ASSERT(testTLogStorageServerMessageSerializer_VersionWithNoMutation());
	ASSERT(testTLogStorageServerMessageSerializer_EmptyInput());
	ASSERT(testTLogStorageServerMessageSerializer_Reset());

	return Void();
}

// Test serialize huge amount of data
// This test is inspired by that the fact that the internal Arena in the serializer might reallocate, and could make
// overwriting serialized data leading to corrupted results.
TEST_CASE("/fdbserver/ptxn/test/TLogStorageServerMessageSerializer/hugeData") {
	using namespace ptxn;

	const int numMutations = params.getInt("numMutations").orDefault(32 * 1024);
	TLogStorageServerMessageSerializer serializer(deterministicRandom()->randomUniqueID());
	Arena mutationArena;
	Version version = 1;
	Subsequence subsequence = 1;
	std::vector<VersionSubsequenceMutation> data;
	data.reserve(numMutations);
	double startTime = getTime();
	std::cout << " Generating " << numMutations << " mutations" << std::endl;
	for (auto _ = 0; _ < numMutations; ++_) {
		if (deterministicRandom()->randomInt(0, 20) == 0) {
			version += deterministicRandom()->randomInt(1, 5);
			subsequence = 1;
		}
		data.emplace_back(version,
		                  subsequence++,
		                  MutationRef(mutationArena,
		                              MutationRef::SetValue,
		                              deterministicRandom()->randomAlphaNumeric(10),
		                              deterministicRandom()->randomAlphaNumeric(100)));
	}
	double generatingTime = getTime();
	serializeVersionedSubsequencedMutations(serializer, data);
	serializer.completeMessageWriting();
	double serializerTime = getTime();

	std::cout << " Serialized " << numMutations << " mutations, the serialized data used " << serializer.getTotalBytes()
	          << " bytes." << std::endl;

	Standalone<StringRef> serialized = serializer.getSerialized();
	TLogStorageServerMessageDeserializer deserializer(serialized);

	double deserializeTime = getTime();
	std::cout << "Generating time: " << generatingTime - startTime
	          << ", Serialization time: " << serializerTime - generatingTime
	          << ", Deserialization time: " << deserializeTime - serializerTime << "\n";

	return Void();
}
