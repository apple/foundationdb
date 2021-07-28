/*
 * MessageSerializer.cpp
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

#include "fdbserver/ptxn/MessageSerializer.h"

#include <string>

#include "fdbclient/FDBTypes.h"
#include "flow/Error.h"
#include "flow/String.h"

namespace ptxn {

SubsequencedMessageSerializer::SubsequencedMessageSerializer(const StorageTeamID& storageTeamID) {
	header.storageTeamID = storageTeamID;
	header.lastVersion = invalidVersion;
}

void SubsequencedMessageSerializer::startVersionWriting(const Version& version) {
	// Ensure the ordering of version.
	ASSERT(version > header.lastVersion);

	if (header.firstVersion == invalidVersion) {
		header.firstVersion = version;
	}
	header.lastVersion = version;

	sectionHeader.version = version;

	serializer.startNewSection();
}

void SubsequencedMessageSerializer::write(const SubsequenceMutationItem& subsequenceMutationItem) {
	writeImpl(subsequenceMutationItem);
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence, const MutationRef& mutation) {
	write(SubsequenceMutationItem{ subsequence, mutation });
}

void SubsequencedMessageSerializer::write(const SubsequenceSpanContextItem& subsequenceSpanContextItem) {
	writeImpl(subsequenceSpanContextItem);
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence, const SpanContextMessage& spanContext) {
	write(SubsequenceSpanContextItem{ subsequence, spanContext });
}

void SubsequencedMessageSerializer::write(const SubsequenceLogProtocolMessageItem& subsequenceLogProtocolMessageItem) {
	writeImpl(subsequenceLogProtocolMessageItem);
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence,
                                          const LogProtocolMessage& logProtocolMessage) {
	write(SubsequenceLogProtocolMessageItem{ subsequence, logProtocolMessage });
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence, const Message& message) {
	switch (message.getType()) {
	case Message::Type::SPAN_CONTEXT_MESSAGE:
		write(subsequence, std::get<SpanContextMessage>(message));
		break;
	case Message::Type::LOG_PROTOCOL_MESSAGE:
		write(subsequence, std::get<LogProtocolMessage>(message));
		break;
	case Message::Type::MUTATION_REF:
		write(subsequence, std::get<MutationRef>(message));
		break;
	default:
		throw internal_error_msg("message to be serialized is valueless, or having undefined type");
	}
}

void SubsequencedMessageSerializer::write(const SubsequenceSerializedMessageItem& subsequenceSerializedMessageItem) {
	writeImpl(subsequenceSerializedMessageItem);
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence, StringRef serializedMessage) {
	write(SubsequenceSerializedMessageItem{ subsequence, serializedMessage });
}

const Version& SubsequencedMessageSerializer::getCurrentVersion() const {
	return header.lastVersion;
}

const Subsequence& SubsequencedMessageSerializer::getCurrentSubsequence() const {
	return sectionHeader.lastSubsequence;
}

void SubsequencedMessageSerializer::writeSection(StringRef serialized) {
	ASSERT(serializer.isSectionCompleted());

	const auto incomingSectionHeader = serializer.writeSerializedSection(serialized);
	if (header.firstVersion == invalidVersion) {
		header.firstVersion = incomingSectionHeader.version;
	} else {
		ASSERT(header.lastVersion < incomingSectionHeader.version);
	}
	header.lastVersion = incomingSectionHeader.version;

	serializer.completeSectionWriting();

	header.length = serializer.getTotalBytes() - serializer.getMainHeaderBytes();
}

void SubsequencedMessageSerializer::completeVersionWriting() {
	serializer.completeSectionWriting();

	sectionHeader.numItems = serializer.getNumItemsCurrentSection();
	// | Main Header | Section Header | Data ... | Section Header | Data ... |
	// ^---------serializer.getTotalBytes()----------------------------------^
	//               ^-----------header.length---^
	// ^-------------^ getMainHeaderBytes()      ^----------------^ getSectionHeaderBytes()
	// NOTE: At this stage header.length does not include the current section.
	sectionHeader.length = serializer.getTotalBytes() - header.length - serializer.getMainHeaderBytes() -
	                       serializer.getSectionHeaderBytes();
	serializer.writeSectionHeader(sectionHeader);

	header.length = serializer.getTotalBytes() - serializer.getMainHeaderBytes();

	sectionHeader = details::SubsequencedItemsHeader();
}

void SubsequencedMessageSerializer::completeMessageWriting() {
	serializer.completeAllItemsWriting();

	header.numItems = serializer.getNumSections();
	header.length = serializer.getTotalBytes() - serializer.getMainHeaderBytes();

	serializer.writeHeader(header);
}

Standalone<StringRef> SubsequencedMessageSerializer::getSerialized() {
	ASSERT(serializer.isAllItemsCompleted());

	return serializer.getSerialized();
}

size_t SubsequencedMessageSerializer::getTotalBytes() const {
	return serializer.getTotalBytes();
}

TLogSubsequencedMessageSerializer::TLogSubsequencedMessageSerializer(const StorageTeamID& storageTeamID_)
  : serializer(storageTeamID_) {}

void TLogSubsequencedMessageSerializer::writeSerializedVersionSection(StringRef serialized) {
	serializer.writeSection(serialized);
}

Standalone<StringRef> TLogSubsequencedMessageSerializer::getSerialized() {
	serializer.completeMessageWriting();

	return serializer.getSerialized();
}

size_t TLogSubsequencedMessageSerializer::getTotalBytes() const {
	return serializer.getTotalBytes();
}

ProxySubsequencedMessageSerializer::ProxySubsequencedMessageSerializer(const Version& version_) : version(version_) {}

const Version& ProxySubsequencedMessageSerializer::getVersion() const {
	return version;
}

void ProxySubsequencedMessageSerializer::prepareWriteMessage(const StorageTeamID& storageTeamID) {
	// If the storage team ID is unseen, create a serializer for it.
	if (serializers.find(storageTeamID) == serializers.end()) {
		serializers.emplace(storageTeamID, storageTeamID);
		serializers.at(storageTeamID).startVersionWriting(version);
	}

	// If span context message exists, and not being written to the serializer, then serialize it first.
	if (spanContextMessage.present() &&
	    storageTeamInjectedSpanContext.find(storageTeamID) == storageTeamInjectedSpanContext.end()) {

		const SpanContextMessage& message = spanContextMessage.get();

		storageTeamInjectedSpanContext.insert(storageTeamID);
		serializers.at(storageTeamID).write(SubsequenceSpanContextItem{ subsequence++, message });
	}
}

void ProxySubsequencedMessageSerializer::broadcastSpanContext(const SpanContextMessage& spanContext) {
	spanContextMessage = spanContext;
	storageTeamInjectedSpanContext.clear();
}

void ProxySubsequencedMessageSerializer::write(const MutationRef& mutation, const StorageTeamID& storageTeamID) {
	prepareWriteMessage(storageTeamID);
	serializers.at(storageTeamID).write(subsequence++, mutation);
}

void ProxySubsequencedMessageSerializer::write(const StringRef& serialized, const StorageTeamID& storageTeamID) {
	prepareWriteMessage(storageTeamID);
	serializers.at(storageTeamID).write(subsequence++, serialized);
}

Standalone<StringRef> ProxySubsequencedMessageSerializer::getSerialized(const StorageTeamID& storageTeamID) {
	auto& serializer = serializers.at(storageTeamID);

	serializer.completeVersionWriting();
	serializer.completeMessageWriting();

	return serializer.getSerialized();
}

std::pair<Arena, std::unordered_map<StorageTeamID, StringRef>> ProxySubsequencedMessageSerializer::getAllSerialized() {
	std::unordered_map<StorageTeamID, StringRef> result;
	Arena sharedArena;
	for (auto& [storageTeamID, serializer] : serializers) {
		result[storageTeamID] = StringRef(sharedArena, getSerialized(storageTeamID));
	}
	return { sharedArena, result };
}

namespace details {

void SubsequencedMessageDeserializerBase::resetImpl(const StringRef serialized_) {
	ASSERT(serialized_.size() > 0);

	serialized = serialized_;
	header = ptxn::details::readSerializedHeader<MessageHeader>(serialized);
}

const StorageTeamID& SubsequencedMessageDeserializerBase::getStorageTeamID() const {
	return header.storageTeamID;
}

size_t SubsequencedMessageDeserializerBase::getNumVersions() const {
	return header.numItems;
}

const Version& SubsequencedMessageDeserializerBase::getFirstVersion() const {
	return header.firstVersion;
}

const Version& SubsequencedMessageDeserializerBase::getLastVersion() const {
	return header.lastVersion;
}

} // namespace details

SubsequencedMessageDeserializer::iterator::iterator(const StringRef serialized_, bool isEndIterator)
  : deserializer(serialized_), rawSerializedData(serialized_) {

	header = deserializer.deserializeAsMainHeader();

	// Set the iterator to a state that has consumed all items in the current version section
	// See TLogStorageServerMessageTLogStorageServerMessageDeserializer::begin
	sectionIndex = -1;
	versionHeader.numItems = 0;
	itemIndex = 0;

	// If it is marked as end, place the iterator to the end
	if (isEndIterator) {
		sectionIndex = header.numItems;
	}
}

bool SubsequencedMessageDeserializer::iterator::operator==(const iterator& another) const {
	return rawSerializedData.begin() == another.rawSerializedData.begin() &&
	       rawSerializedData.size() == another.rawSerializedData.size() && sectionIndex == another.sectionIndex &&
	       itemIndex == another.itemIndex;
}

bool SubsequencedMessageDeserializer::iterator::operator!=(const iterator& another) const {
	return !(*this == another);
}

SubsequencedMessageDeserializer::iterator::reference SubsequencedMessageDeserializer::iterator::operator*() const {
	return currentItem;
}

SubsequencedMessageDeserializer::iterator::pointer SubsequencedMessageDeserializer::iterator::operator->() const {
	return &currentItem;
}

SubsequencedMessageDeserializer::iterator& SubsequencedMessageDeserializer::iterator::operator++() {
	// Move to next section if the current section is consumed
	if (itemIndex == versionHeader.numItems) {
		++sectionIndex;
		itemIndex = 0;
		while (sectionIndex != header.numItems) {
			versionHeader = deserializer.deserializeAsSectionHeader();
			currentItem.version = versionHeader.version;
			if (versionHeader.numItems != 0) {
				break;
			}
			++sectionIndex;
		}

		if (sectionIndex == header.numItems) {
			// Reached the end, do not try to deserialize the data
			ASSERT(deserializer.allConsumed());
			return *this;
		}
	}

	// Consume this section
	++itemIndex;
	// NOTE This peek assumes the serializer never pad between Subsequence and the following message.
	const MutationRef::Type type = static_cast<MutationRef::Type>(
	    *(deserializer.peekBytes(sizeof(Subsequence) + sizeof(MutationRef::Type)) + sizeof(Subsequence)));

	if (type == MutationRef::Reserved_For_LogProtocolMessage) {
		const auto message = deserializer.deserializeItem<SubsequenceLogProtocolMessageItem>();
		currentItem.subsequence = message.subsequence;
		currentItem.message = message.logProtocolMessage;
	} else if (type == MutationRef::Reserved_For_SpanContextMessage) {
		const auto message = deserializer.deserializeItem<SubsequenceSpanContextItem>();
		currentItem.subsequence = message.subsequence;
		currentItem.message = message.spanContext;
	} else {
		// A normal MutationRef
		const auto message = deserializer.deserializeItem<SubsequenceMutationItem>();
		currentItem.subsequence = message.subsequence;
		currentItem.message = message.mutation;
	}

	return *this;
}

SubsequencedMessageDeserializer::iterator SubsequencedMessageDeserializer::iterator::operator++(int) {
	iterator prev(*this);
	this->operator++();
	return prev;
}

Arena& SubsequencedMessageDeserializer::iterator::arena() {
	return deserializer.arena();
}

SubsequencedMessageDeserializer::SubsequencedMessageDeserializer(const StringRef serialized_)
  : endIterator(serialized_, true) {

	reset(serialized_);
}

void SubsequencedMessageDeserializer::reset(const StringRef serialized_) {
	details::SubsequencedMessageDeserializerBase::resetImpl(serialized_);

	endIterator = iterator(serialized, true);
}

SubsequencedMessageDeserializer::iterator SubsequencedMessageDeserializer::begin() const {
	// Since the iterator is setting to a state that it is located at the end of a version section,
	// doing a prefix ++ will trigger it read a new version section, and place itself to the beginning
	// of the items in the section.
	return ++iterator(serialized, false);
}

const SubsequencedMessageDeserializer::iterator& SubsequencedMessageDeserializer::end() const {
	return endIterator;
}

SubsequencedMessageDeserializer::const_iterator SubsequencedMessageDeserializer::cbegin() const {
	return begin();
}

const SubsequencedMessageDeserializer::const_iterator& SubsequencedMessageDeserializer::cend() const {
	return end();
}

} // namespace ptxn
