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

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/SpanContextMessage.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/Serializer.h"
#include "fdbserver/ptxn/MessageSerializer.h"
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

struct TestSerializerItemA {
	int item1;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, item1);
	}
};

struct TestSerializerItemB {
	long item2;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, item2);
	}
};

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/headeredSerializer") {
	using TestSerializer = ptxn::HeaderedItemsSerializer<TestSerializerHeader, TestSerializerItemA>;

	ptxn::test::print::PrintTiming printTiming("test/headeredSerializer");

	TestSerializer serializer;

	// Before writing any items, the serializer should put a header placeholder.
	ASSERT_EQ(serializer.getHeaderBytes(), ptxn::getSerializedBytes<TestSerializerHeader>());
	ASSERT_EQ(serializer.getTotalBytes(), ptxn::SerializerVersionOptionBytes + serializer.getHeaderBytes());
	ASSERT_EQ(serializer.getItemsBytes(), 0);
	ASSERT_EQ(serializer.getNumItems(), 0);

	// Write first item
	serializer.writeItem(TestSerializerItemA{ 9 });
	ASSERT_EQ(serializer.getNumItems(), 1);
	ASSERT_EQ(serializer.getItemsBytes(), sizeof(int));

	// Write second item
	serializer.writeItem(TestSerializerItemA{ 8 });
	ASSERT_EQ(serializer.getNumItems(), 2);
	ASSERT_EQ(serializer.getItemsBytes(), sizeof(int) * 2);
	// Write header
	serializer.completeItemWriting();
	ASSERT(serializer.isWritingCompleted());
	serializer.writeHeader(TestSerializerHeader{ 3, 5, 7 });

	// Deserializer back
	Standalone<StringRef> serialized = serializer.getSerialized();
	TestSerializerHeader header;
	std::vector<TestSerializerItemA> items;
	ptxn::headeredItemDeserializerBase(serialized.arena(), serialized, header, items);
	ASSERT_EQ(header.item1, 3);
	ASSERT_EQ(header.item2, 5);
	ASSERT_EQ(header.item3, 7);
	ASSERT_EQ(items.size(), 2);
	ASSERT_EQ(items[0].item1, 9);
	ASSERT_EQ(items[1].item1, 8);

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/twoLevelHeaderedSerializer/base") {
	using TestSerializer = ptxn::TwoLevelHeaderedItemsSerializer<TestSerializerHeader, TestSerializerSectionHeader>;
	using TestDeserializer = ptxn::TwoLevelHeaderedItemsDeserializer<TestSerializerHeader, TestSerializerSectionHeader>;

	ptxn::test::print::PrintTiming printTiming("test/twoLevelHeaderedSerializer");

	TestSerializer serializer;

	// Test the sanity of initial state
	ASSERT_EQ(serializer.getTotalBytes(),
	          ptxn::SerializerVersionOptionBytes + ptxn::getSerializedBytes<TestSerializerHeader>());
	ASSERT(serializer.isSectionCompleted());
	ASSERT(!serializer.isAllItemsCompleted());
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
	serializer.writeItem(TestSerializerItemA{ 1 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 1);

	serializer.writeItem(TestSerializerItemB{ 2 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 2);

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
	serializer.writeItem(TestSerializerItemA{ 4 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 1);

	serializer.writeItem(TestSerializerItemB{ 5 });
	ASSERT_EQ(serializer.getNumItemsCurrentSection(), 2);

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
	TestDeserializer deserializer(serialized);

	TestSerializerHeader mainHeader = deserializer.deserializeAsMainHeader();
	ASSERT_EQ(mainHeader.item1, 10);
	ASSERT_EQ(mainHeader.item2, 11);
	ASSERT_EQ(mainHeader.item3, 12);

	TestSerializerSectionHeader section1Header = deserializer.deserializeAsSectionHeader();
	ASSERT_EQ(section1Header.item1, 3);
	ASSERT_EQ(section1Header.item2, 4);

	TestSerializerItemA itemA;
	itemA = deserializer.deserializeItem<TestSerializerItemA>();
	ASSERT_EQ(itemA.item1, 1);
	TestSerializerItemB itemB = deserializer.deserializeItem<TestSerializerItemB>();
	ASSERT_EQ(itemB.item2, 2);

	TestSerializerSectionHeader section2Header = deserializer.deserializeAsSectionHeader();
	ASSERT_EQ(section2Header.item1, 7);
	ASSERT_EQ(section2Header.item2, 8);

	deserializer.deserializeItem(itemA);
	ASSERT_EQ(itemA.item1, 4);
	deserializer.deserializeItem(itemB);
	ASSERT_EQ(itemB.item2, 5);

	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/twoLevelHeaderedSerializer/preserialized") {
	using TestSerializer = ptxn::TwoLevelHeaderedItemsSerializer<TestSerializerHeader, TestSerializerSectionHeader>;
	using TestDeserializer = ptxn::TwoLevelHeaderedItemsDeserializer<TestSerializerHeader, TestSerializerSectionHeader>;

	ptxn::test::print::PrintTiming printTiming("test/twoLevelHeaderedSerializer/preserialized");

	const size_t HEADER_LENGTH = ptxn::getSerializedBytes<TestSerializerHeader>();

	// Pre-serialize some data
	TestSerializer preserializer;
	preserializer.startNewSection();
	preserializer.writeItem(TestSerializerItemA{ 0xff });
	preserializer.writeItem(TestSerializerItemB{ 0xee });
	preserializer.completeSectionWriting();
	preserializer.writeSectionHeader(TestSerializerSectionHeader{ 0xdd, 0xcc });
	preserializer.completeAllItemsWriting();

	Standalone<StringRef> preserialized = preserializer.getSerialized();

	// Inject data into serializer
	TestSerializer serializer;

	serializer.startNewSection();
	serializer.writeItem(TestSerializerItemA{ 1 });
	serializer.completeSectionWriting();
	serializer.writeSectionHeader(TestSerializerSectionHeader{ 10, 20 });

	serializer.writeSerializedSection(
	    StringRef(preserialized.begin() + ptxn::SerializerVersionOptionBytes + HEADER_LENGTH,
	              preserialized.size() - ptxn::SerializerVersionOptionBytes - HEADER_LENGTH));

	serializer.startNewSection();
	serializer.writeItem(TestSerializerItemB{ 2 });
	serializer.completeSectionWriting();
	serializer.writeSectionHeader(TestSerializerSectionHeader{ 30, 40 });

	serializer.completeAllItemsWriting();

	serializer.writeHeader(TestSerializerHeader{ 1, 2, 3 });

	Standalone<StringRef> serialized = serializer.getSerialized();

	// Deserialize and verify
	TestDeserializer deserializer(serialized);

	auto mainHeader = deserializer.deserializeAsMainHeader();
	ASSERT_EQ(mainHeader.item1, 1);
	ASSERT_EQ(mainHeader.item2, 2);
	ASSERT_EQ(mainHeader.item3, 3);

	auto section1Header = deserializer.deserializeAsSectionHeader();
	ASSERT_EQ(section1Header.item1, 10);
	ASSERT_EQ(section1Header.item2, 20);

	auto section1Item1 = deserializer.deserializeItem<TestSerializerItemA>();
	ASSERT_EQ(section1Item1.item1, 1);

	auto section2Header = deserializer.deserializeAsSectionHeader();
	ASSERT_EQ(section2Header.item1, 0xdd);
	ASSERT_EQ(section2Header.item2, 0xcc);

	auto section2Item1 = deserializer.deserializeItem<TestSerializerItemA>();
	ASSERT_EQ(section2Item1.item1, 0xff);

	auto section2Item2 = deserializer.deserializeItem<TestSerializerItemB>();
	ASSERT_EQ(section2Item2.item2, 0xee);

	auto section3Header = deserializer.deserializeAsSectionHeader();
	ASSERT_EQ(section3Header.item1, 30);
	ASSERT_EQ(section3Header.item2, 40);

	auto section3Item1 = deserializer.deserializeItem<TestSerializerItemB>();
	ASSERT_EQ(section3Item1.item2, 2);

	ASSERT(deserializer.allConsumed());

	return Void();
}

namespace {

// The test is done by serializing data for multiple versions, in each version there are differenty types of objects.
// Different section should have different versions in order to avoid confusion.
namespace testSubsequencedMessageSerializer {

// Section 1: Serialize multiple MutationRefs
namespace S1 {

const Version version = 1;
// The tag _x stands for the subsequence, similarly hereinafter
const MutationRef mutation_1 = MutationRef(MutationRef::SetValue, "Key1"_sr, "Value1"_sr);
const MutationRef mutation_3 = MutationRef(MutationRef::ClearRange, "Key999"_sr, "Key2000"_sr);

void write(ptxn::SubsequencedMessageSerializer& serializer) {
	serializer.startVersionWriting(version);
	ASSERT_EQ(serializer.getCurrentVersion(), version);
	ASSERT_EQ(serializer.getCurrentSubsequence(), invalidSubsequence);

	serializer.write(1, mutation_1);
	ASSERT_EQ(serializer.getCurrentVersion(), version);
	ASSERT_EQ(serializer.getCurrentSubsequence(), 1);

	serializer.write({ 3, mutation_3 });
	ASSERT_EQ(serializer.getCurrentVersion(), version);
	ASSERT_EQ(serializer.getCurrentSubsequence(), 3);

	serializer.completeVersionWriting();
}

void verify(ptxn::SubsequencedMessageDeserializer::iterator& iterator) {
	ASSERT_EQ(iterator->version, version);
	ASSERT_EQ(iterator->subsequence, 1);
	ASSERT_EQ(iterator->message.getType(), ptxn::Message::Type::MUTATION_REF);
	ASSERT(std::get<MutationRef>(iterator->message) == mutation_1);
	++iterator;

	ASSERT_EQ(iterator->version, version);
	ASSERT_EQ(iterator->subsequence, 3);
	ASSERT_EQ(iterator->message.getType(), ptxn::Message::Type::MUTATION_REF);
	ASSERT(std::get<MutationRef>(iterator->message) == mutation_3);
	++iterator;
}

} // namespace S1

// Section 2: serializing SpanContext/MutationRefs
namespace S2 {

const Version version = 3;
const SpanContextMessage spanContext_5 = SpanContextMessage(ptxn::test::randomUID());
const SpanContextMessage spanContext_6 = SpanContextMessage(ptxn::test::randomUID());
const MutationRef mutation_7 = MutationRef(MutationRef::SetValue, "Key9"_sr, "Value12"_sr);

void write(ptxn::SubsequencedMessageSerializer& serializer) {
	serializer.startVersionWriting(version);

	serializer.write(5, spanContext_5);
	serializer.write({ 6, spanContext_6 });
	serializer.write(7, mutation_7);

	serializer.completeVersionWriting();
}

void verify(ptxn::SubsequencedMessageDeserializer::iterator& iterator) {
	ASSERT_EQ(iterator->version, version);
	ASSERT_EQ(iterator->subsequence, 5);
	ASSERT_EQ(iterator->message.getType(), ptxn::Message::Type::SPAN_CONTEXT_MESSAGE);
	ASSERT(std::get<SpanContextMessage>(iterator->message) == spanContext_5);
	++iterator;

	ASSERT(std::get<SpanContextMessage>(iterator->message) == spanContext_6);
	++iterator;

	ASSERT(std::get<MutationRef>(iterator->message) == mutation_7);
	++iterator;
}

} // namespace S2

// Section 3: serializing LogProtocolMessage/MutationRefs
namespace S3 {

const Version version = 7;
const LogProtocolMessage protocol_1 = LogProtocolMessage();
const LogProtocolMessage protocol_2 = LogProtocolMessage();
const MutationRef mutation_3 = MutationRef(MutationRef::SetValue, "Key10"_sr, "Value99"_sr);

void write(ptxn::SubsequencedMessageSerializer& serializer) {
	serializer.startVersionWriting(version);

	serializer.write(1, protocol_1);
	serializer.write({ 2, protocol_2 });
	serializer.write(3, mutation_3);

	serializer.completeVersionWriting();
}

void verify(ptxn::SubsequencedMessageDeserializer::iterator& iterator) {
	ASSERT(std::get<LogProtocolMessage>(iterator->message) == protocol_1);
	++iterator;

	ASSERT(std::get<LogProtocolMessage>(iterator->message) == protocol_2);
	++iterator;

	ASSERT(std::get<MutationRef>(iterator->message) == mutation_3);
	++iterator;
}

} // namespace S3

// Section 4: Serializing an empty version
namespace S4 {

const Version version = 10;

void write(ptxn::SubsequencedMessageSerializer& serializer) {
	serializer.startVersionWriting(version);
	serializer.completeVersionWriting();
}

void verify(ptxn::SubsequencedMessageDeserializer::iterator& iterator) {
	// The empty version section should not be touched by iterator
}

} // namespace S4

// Section 5: Test write with std::variant type variable
namespace S5 {

const Version version = 20;

void write(ptxn::SubsequencedMessageSerializer& serializer) {
	serializer.startVersionWriting(version);
	serializer.write(1, MutationRef(MutationRef::SetValue, "key"_sr, "value"_sr));
	serializer.write(2, SpanContextMessage(SpanID(1, 1)));
	serializer.write(3, LogProtocolMessage());
	serializer.completeVersionWriting();
}

void verify(ptxn::SubsequencedMessageDeserializer::iterator& iterator) {
	ASSERT_EQ(iterator->message.getType(), ptxn::Message::Type::MUTATION_REF);
	auto mutation = std::get<MutationRef>(iterator->message);
	ASSERT_EQ(mutation.type, MutationRef::SetValue);
	ASSERT(mutation.param1 == "key"_sr);
	ASSERT(mutation.param2 == "value"_sr);
	++iterator;

	ASSERT_EQ(iterator->message.getType(), ptxn::Message::Type::SPAN_CONTEXT_MESSAGE);
	auto span = std::get<SpanContextMessage>(iterator->message);
	ASSERT(span == SpanID(1, 1));
	++iterator;

	ASSERT_EQ(iterator->message.getType(), ptxn::Message::Type::LOG_PROTOCOL_MESSAGE);
	auto logProtocol = std::get<LogProtocolMessage>(iterator->message);
	ASSERT(logProtocol == LogProtocolMessage());
	++iterator;
}

} // namespace S5

// Section 6: Test writing pre-serialized data
namespace S6 {

const Version version = 30;
const MutationRef mutationRef = MutationRef(MutationRef::SetValue, "keyS6"_sr, "valueS6"_sr);

void write(ptxn::SubsequencedMessageSerializer& serializer) {
	serializer.startVersionWriting(version);

	// Serializing a mutationRef requires the writer include a serialization protocol version.
	Standalone<StringRef> serialized = BinaryWriter::toValue(mutationRef, IncludeVersion());
	StringRef serializedWithoutVersionOptions(serialized.begin() + ptxn::SerializerVersionOptionBytes,
	                                          serialized.size() - ptxn::SerializerVersionOptionBytes);

	serializer.write(1, serializedWithoutVersionOptions);

	serializer.completeVersionWriting();
}

void verify(ptxn::SubsequencedMessageDeserializer::iterator& iterator) {
	ASSERT_EQ(iterator->message.getType(), ptxn::Message::Type::MUTATION_REF);
	auto mutation = std::get<MutationRef>(iterator->message);
	ASSERT_EQ(mutation.type, MutationRef::SetValue);
	ASSERT(mutation.param1 == "keyS6"_sr);
	ASSERT(mutation.param2 == "valueS6"_sr);
	++iterator;
}

} // namespace S6

bool testSerialization() {
	using namespace ptxn;

	ptxn::test::print::PrintTiming printTiming(__FUNCTION__);

	const StorageTeamID storageTeamID = ptxn::test::getNewStorageTeamID();
	SubsequencedMessageSerializer serializer(storageTeamID);

	// At the beginning the serializer should have invalid version and subsequence, as no data written
	ASSERT_EQ(serializer.getCurrentVersion(), invalidVersion);
	ASSERT_EQ(serializer.getCurrentSubsequence(), invalidSubsequence);

	S1::write(serializer);
	S2::write(serializer);
	S3::write(serializer);
	S4::write(serializer);
	S5::write(serializer);
	S6::write(serializer);

	serializer.completeMessageWriting();

	printTiming << "Serialization, storageTeamID = " << storageTeamID.toString() << std::endl;

	Standalone<StringRef> serialized = serializer.getSerialized();
	SubsequencedMessageDeserializer deserializer(serialized);
	SubsequencedMessageDeserializer::iterator iterator = deserializer.cbegin();
	printTiming << deserializer.getStorageTeamID() << std::endl;

	ASSERT(deserializer.getStorageTeamID() == storageTeamID);
	ASSERT_EQ(deserializer.getNumVersions(), 6);
	ASSERT_EQ(deserializer.getFirstVersion(), S1::version);
	ASSERT_EQ(deserializer.getLastVersion(), S6::version);

	S1::verify(iterator);
	S2::verify(iterator);
	S3::verify(iterator);
	S4::verify(iterator);
	S5::verify(iterator);
	S6::verify(iterator);

	ASSERT(iterator == deserializer.cend());

	printTiming << "Deserialization" << std::endl;

	return true;
}

bool testSerializationEmpty() {
	using namespace ptxn;

	ptxn::test::print::PrintTiming printTiming(__FUNCTION__);

	const StorageTeamID storageTeamID = ptxn::test::getNewStorageTeamID();
	SubsequencedMessageSerializer serializer(storageTeamID);
	serializer.completeMessageWriting();

	Standalone<StringRef> serialized = serializer.getSerialized();
	SubsequencedMessageDeserializer deserializer(serialized);
	SubsequencedMessageDeserializer::iterator iterator = deserializer.cbegin();

	ASSERT(iterator == deserializer.cend());

	return true;
}

// Serialize a bunch of {version, subsequence, mutation}
void serializeVersionedSubsequencedMutations(
    ptxn::SubsequencedMessageSerializer& serializer,
    const std::vector<ptxn::VersionSubsequenceMessage> versionSubsequenceMutations) {

	Version currentVersion = invalidVersion;
	for (const auto& item : versionSubsequenceMutations) {
		if (currentVersion != item.version) {
			if (currentVersion != invalidVersion) {
				serializer.completeVersionWriting();
			}
			currentVersion = item.version;
			serializer.startVersionWriting(currentVersion);
		}
		serializer.write(item.subsequence, item.message);
	}

	if (currentVersion != invalidVersion) {
		serializer.completeVersionWriting();
	}

	// Intended to leave the serializer open to new versions
}

bool testDeserializerIterators() {
	using namespace ptxn;

	ptxn::test::print::PrintTiming printTiming(__FUNCTION__);

	const std::vector<VersionSubsequenceMessage> VERSIONED_SUBSEQUENCED_MUTATIONS{
		{ 2, 1, MutationRef(MutationRef::SetValue, "Key2"_sr, "Value2"_sr) },
		{ 2, 2, MutationRef(MutationRef::SetValue, "Key3"_sr, "Value3"_sr) },
		{ 3, 1, MutationRef(MutationRef::SetValue, "Key4"_sr, "Value4"_sr) },
		{ 3, 5, MutationRef(MutationRef::SetValue, "Key5"_sr, "Value5"_sr) },
		{ 3, 7, MutationRef(MutationRef::SetValue, "Key6"_sr, "Value6"_sr) },
	};

	StorageTeamID storageTeamID = deterministicRandom()->randomUniqueID();
	SubsequencedMessageSerializer serializer(storageTeamID);
	serializeVersionedSubsequencedMutations(serializer, VERSIONED_SUBSEQUENCED_MUTATIONS);
	serializer.completeMessageWriting();

	auto serialized = serializer.getSerialized();

	SubsequencedMessageDeserializer deserializer(serialized);

	ASSERT(deserializer.getStorageTeamID() == storageTeamID);
	ASSERT_EQ(deserializer.getNumVersions(), 2);
	ASSERT_EQ(deserializer.getFirstVersion(), 2);
	ASSERT_EQ(deserializer.getLastVersion(), 3);

	// Test the ranged-for loop
	printTiming << "Test ranged-for" << std::endl;
	{
		int index = 0;
		for (auto item : deserializer) {
			const auto& expected = VERSIONED_SUBSEQUENCED_MUTATIONS[index++];
			ASSERT(item == expected);
		}

		ASSERT_EQ(index, static_cast<int>(VERSIONED_SUBSEQUENCED_MUTATIONS.size()));
	}

	// Test the iterator for overloaded operators: de-reference/arrow and prefix operator++
	// Also test the re-usability of the iterator, the deserializer should be re-iteratable
	printTiming << "Test regular iterator operations" << std::endl;
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

// Test postfix operator++
bool testDeserializerIteratorPostfixOperator() {
	using namespace ptxn;

	ptxn::test::print::PrintTiming printTiming(__FUNCTION__);

	const std::vector<VersionSubsequenceMessage> PART1{
		{ 1, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key1"), LiteralStringRef("Value1")) },
		{ 3, 1, MutationRef(MutationRef::SetValue, LiteralStringRef("Key2"), LiteralStringRef("Value2")) },
	};
	const std::vector<VersionSubsequenceMessage> PART2{
		{ 6, 3, MutationRef(MutationRef::SetValue, LiteralStringRef("Key3"), LiteralStringRef("Value3")) },
	};

	SubsequencedMessageSerializer serializer(deterministicRandom()->randomUniqueID());

	serializeVersionedSubsequencedMutations(serializer, PART1);
	serializeVersionedSubsequencedMutations(serializer, PART2);

	serializer.completeMessageWriting();

	Standalone<StringRef> serialized = serializer.getSerialized();
	SubsequencedMessageDeserializer deserializer(serialized);

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

// Test reset the SubsequencedMessageSerializer
bool testDeserializerReset() {
	using namespace ptxn;

	ptxn::test::print::PrintTiming printTiming(__FUNCTION__);

	SubsequencedMessageSerializer serializer1(deterministicRandom()->randomUniqueID());
	const std::vector<VersionSubsequenceMessage> DATA1{
		{ 1, 3, MutationRef(MutationRef::SetValue, "Key1"_sr, "Value1"_sr) },
		{ 3, 1, MutationRef(MutationRef::SetValue, "Key2"_sr, "Value2"_sr) },
	};
	serializeVersionedSubsequencedMutations(serializer1, DATA1);
	serializer1.completeMessageWriting();
	Standalone<StringRef> serialized1 = serializer1.getSerialized();

	SubsequencedMessageSerializer serializer2(deterministicRandom()->randomUniqueID());
	const std::vector<VersionSubsequenceMessage> DATA2{
		{ 6, 3, MutationRef(MutationRef::SetValue, "Key3"_sr, "Value3"_sr) },
	};
	serializeVersionedSubsequencedMutations(serializer2, DATA2);
	serializer2.completeMessageWriting();
	Standalone<StringRef> serialized2 = serializer2.getSerialized();

	SubsequencedMessageDeserializer deserializer(serialized1);
	auto iter1 = deserializer.begin();
	ASSERT(*iter1++ == DATA1[0]);
	ASSERT(*iter1++ == DATA1[1]);
	ASSERT(iter1 == deserializer.end());

	deserializer.reset(serialized2);
	auto iter2 = deserializer.begin();
	ASSERT(*iter2++ == DATA2[0]);
	ASSERT(iter2 == deserializer.end());

	return true;
}
} // namespace testSubsequencedMessageSerializer

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/SubsequencedMessageSerializer/normal") {
	using namespace testSubsequencedMessageSerializer;

	ptxn::test::print::PrintTiming printTiming("test/SubsequencedMessageSerializer");

	ASSERT(testSerialization());
	ASSERT(testSerializationEmpty());
	ASSERT(testDeserializerIterators());
	ASSERT(testDeserializerIteratorPostfixOperator());
	ASSERT(testDeserializerReset());

	return Void();
}
namespace {

namespace testSubsequencedMessageSerializer {

void generateMutations(std::vector<ptxn::VersionSubsequenceMessage>& mutations, Arena& arena, const int numMutations) {
	Version version = 1;
	Subsequence subsequence = 0;

	for (int _ = 0; _ < numMutations; ++_) {
		if (deterministicRandom()->randomInt(0, 20) == 0) {
			version += deterministicRandom()->randomInt(1, 5);
			subsequence = 0;
		}

		mutations.emplace_back(version,
		                       ++subsequence,
		                       MutationRef(arena,
		                                   MutationRef::SetValue,
		                                   deterministicRandom()->randomAlphaNumeric(10),
		                                   deterministicRandom()->randomAlphaNumeric(100)));
	}
}

} // namespace testSubsequencedMessageSerializer

} // anonymous namespace

// Test serialize huge amount of data
// This test is inspired by that the fact that the internal Arena in the serializer might reallocate, and could make
// overwriting serialized data leading to corrupted results.
TEST_CASE("/fdbserver/ptxn/test/SubsequencedMessageSerializer/timing") {
	using namespace ptxn;
	using namespace testSubsequencedMessageSerializer;

	ptxn::test::print::PrintTiming printTiming("test/SubsequencedMessageSerializer/timing");

	const int numMutations = params.getInt("numMutations").orDefault(10); // 32 * 1024);

	Arena mutationArena;
	std::vector<VersionSubsequenceMessage> data;
	data.reserve(numMutations);

	printTiming << "Generating " << numMutations << " mutations" << std::endl;
	generateMutations(data, mutationArena, numMutations);
	printTiming << numMutations << " mutations generated" << std::endl;

	SubsequencedMessageSerializer serializer(deterministicRandom()->randomUniqueID());
	serializeVersionedSubsequencedMutations(serializer, data);
	serializer.completeMessageWriting();
	printTiming << "Serialized " << numMutations << " mutations, the serialized data used "
	            << serializer.getTotalBytes() << " bytes." << std::endl;

	Standalone<StringRef> serialized = serializer.getSerialized();
	SubsequencedMessageDeserializer deserializer(serialized);
	for (SubsequencedMessageDeserializer::iterator iter = deserializer.begin(); iter != deserializer.end(); ++iter)
		;
	printTiming << "Deserialized " << numMutations << " mutations" << std::endl;

	return Void();
}

namespace {

namespace testTLogSubsequencedMessageSerializer {

bool testWriteSerialized() {
	using namespace ptxn;

	ptxn::details::SubsequencedItemsHeader header;
	header.version = 0xab; 
	header.lastSubsequence = 1;
	header.numItems = 1;
	header.length = 0xff; // TODO even in this test the length is not being used, set the proper value
	ptxn::SubsequenceSpanContextItem item;
	item.subsequence = 1;
	item.spanContext = SpanContextMessage(SpanID(1, 2));

	BinaryWriter preserializer(IncludeVersion());
	preserializer << header << item;
	Standalone<StringRef> preserialized = preserializer.toValue();
	const size_t PREFIX_LENGTH = ptxn::SerializerVersionOptionBytes;
	StringRef serializedWithoutVersionOptions(preserialized.begin() + PREFIX_LENGTH,
	                                          preserialized.size() - PREFIX_LENGTH);

	TLogSubsequencedMessageSerializer serializer(ptxn::test::randomUID());
	serializer.writeSerializedVersionSection(serializedWithoutVersionOptions);
	Standalone<StringRef> serialized = serializer.getSerialized();
	SubsequencedMessageDeserializer deserializer(serialized);

	auto iter = deserializer.begin();
	ASSERT_EQ(iter->version, 0xab);
	ASSERT_EQ(iter->subsequence, 1);
	ASSERT(std::get<SpanContextMessage>(iter->message).spanContext == SpanID(1, 2));

	++iter;
	ASSERT(iter == deserializer.end());

	return true;
}

} // namespace testTLogSubsequencedMessageSerializer

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/TLogSubsequencedMessageSerializer/normal") {
	using namespace testTLogSubsequencedMessageSerializer;

	ptxn::test::print::PrintTiming printTiming("test/TLogSubsequencedMessageSerializer");

	ASSERT(testWriteSerialized());

	return Void();
}

namespace {

namespace testProxySubsequenceMessageSerializer {

void checkSubequenceIncremental(const ptxn::SubsequencedMessageDeserializer& deserializer) {
	Subsequence lastSubsequence = 0;
	for (const auto& item : deserializer) {
		ASSERT(item.subsequence > lastSubsequence);
	}
}

bool testWriteMessage(const UnitTestParameters& params) {
	using namespace ptxn;

	ptxn::test::print::PrintTiming printTiming(__FUNCTION__);

	const int numMutations = params.getInt("numMutations").orDefault(32 * 1024);
	const int numStorageTeamIDs = params.getInt("numStorageTeamIDs").orDefault(16);
	const Version version = 1;

	std::vector<StorageTeamID> storageTeamIDs(numStorageTeamIDs);
	std::generate(std::begin(storageTeamIDs), std::end(storageTeamIDs), test::getNewStorageTeamID);
	Arena mutationRefArena;
	std::vector<MutationRef> mutationRefs(numMutations);
	std::generate(std::begin(mutationRefs), std::end(mutationRefs), [&mutationRefArena]() {
		return MutationRef(mutationRefArena,
		                   MutationRef::SetValue,
		                   StringRef(mutationRefArena, deterministicRandom()->randomAlphaNumeric(20)),
		                   StringRef(mutationRefArena, deterministicRandom()->randomAlphaNumeric(100)));
	});

	ProxySubsequencedMessageSerializer serializer(version);

	ASSERT_EQ(serializer.getVersion(), version);

	const SpanID spanID = test::randomUID();
	serializer.broadcastSpanContext(spanID);

	for (const auto& mutation : mutationRefs) {
		const auto& storageTeamID = storageTeamIDs[deterministicRandom()->randomInt(0, numStorageTeamIDs)];

		serializer.write(mutation, storageTeamID);
	}

	auto serialized = serializer.getAllSerialized();

	std::map<Subsequence, VersionSubsequenceMessage> allMessages;
	for (const auto& [_, serializedData] : serialized.second) {
		SubsequencedMessageDeserializer deserializer(serializedData);
		checkSubequenceIncremental(deserializer);

		for (const auto& item : deserializer) {
			// Assure subsequence are not re-used
			ASSERT(allMessages.find(item.subsequence) == allMessages.end());

			if (item.message.getType() == Message::Type::SPAN_CONTEXT_MESSAGE) {
				allMessages[item.subsequence] =
				    VersionSubsequenceMessage(version, item.subsequence, std::get<SpanContextMessage>(item.message));
			} else if (item.message.getType() == Message::Type::MUTATION_REF) {
				allMessages[item.subsequence] = VersionSubsequenceMessage(
				    version, item.subsequence, MutationRef(mutationRefArena, std::get<MutationRef>(item.message)));
			} else {
				UNREACHABLE();
			}
		}
	}

	ASSERT_EQ(mutationRefs.size() + numStorageTeamIDs /* for broadcasted spanID */, allMessages.size());

	auto mutationRefIter = mutationRefs.cbegin();
	auto allMessagesIter = allMessages.cbegin();
	int numSpanContextMessages = 0;

	while (allMessagesIter != allMessages.cend()) {
		const auto& m = allMessagesIter->second.message;
		if (m.getType() == Message::Type::SPAN_CONTEXT_MESSAGE) {
			++numSpanContextMessages;
			ASSERT(std::get<SpanContextMessage>(m) == spanID);
		} else {
			auto deserializedMutation = std::get<MutationRef>(allMessagesIter->second.message);
			ASSERT(std::get<MutationRef>(m) == deserializedMutation);

			++mutationRefIter;
		}
		++allMessagesIter;
	}

	return true;
}

} // namespace testProxySubsequenceMessageSerializer

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/ProxySubsequenceMessageSerializer/normal") {
	using namespace testProxySubsequenceMessageSerializer;

	ptxn::test::print::PrintTiming printTiming("test/ProxySubsequenceMessageSerializer");

	ASSERT(testWriteMessage(params));

	return Void();
}