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
	header.lastSubsequence = invalidSubsequence;
}

void SubsequencedMessageSerializer::startVersionWriting(const Version& version) {
	// Ensure the ordering of version.
	ASSERT(version > header.lastVersion);

	if (header.firstVersion == invalidVersion) {
		header.firstVersion = version;
	}
	header.lastVersion = version;
	header.lastSubsequence = invalidSubsequence;

	serializer.startNewSection();
}

void SubsequencedMessageSerializer::write(const SubsequenceMutationItem& subsequenceMutationItem) {
	ASSERT(subsequenceMutationItem.subsequence > header.lastSubsequence);

	header.lastSubsequence = subsequenceMutationItem.subsequence;
	serializer.writeItem(subsequenceMutationItem);
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence, const MutationRef& mutation) {
	write(SubsequenceMutationItem{ subsequence, mutation });
}

void SubsequencedMessageSerializer::write(const SubsequenceSpanContextItem& subsequenceSpanContextItem) {
	ASSERT(subsequenceSpanContextItem.subsequence > header.lastSubsequence);

	header.lastSubsequence = subsequenceSpanContextItem.subsequence;
	serializer.writeItem(subsequenceSpanContextItem);
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence, const SpanContextMessage& spanContext) {
	write(SubsequenceSpanContextItem{ subsequence, spanContext });
}

void SubsequencedMessageSerializer::write(const SubsequenceLogProtocolMessageItem& subsequenceLogProtocolMessageItem) {
	ASSERT(subsequenceLogProtocolMessageItem.subsequence > header.lastSubsequence);

	header.lastSubsequence = subsequenceLogProtocolMessageItem.subsequence;
	serializer.writeItem(subsequenceLogProtocolMessageItem);
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
	ASSERT(subsequenceSerializedMessageItem.subsequence > header.lastSubsequence);

	header.lastSubsequence = subsequenceSerializedMessageItem.subsequence;
	serializer.writeItem(subsequenceSerializedMessageItem);
}

void SubsequencedMessageSerializer::write(const Subsequence& subsequence, StringRef serializedMessage) {
	write(SubsequenceSerializedMessageItem{ subsequence, serializedMessage });
}

const Version& SubsequencedMessageSerializer::getCurrentVersion() const {
	return header.lastVersion;
}

const Subsequence& SubsequencedMessageSerializer::getCurrentSubsequence() const {
	return header.lastSubsequence;
}

void SubsequencedMessageSerializer::completeVersionWriting() {
	// If a version exists, yet has 0 items (an empty commit), a NOOP is injected to avoid version skipping. Since the
	// NOOP basically does nothing, it has the maximum subsequence number, which *hopefully* will never be reached.
	if (serializer.getNumItemsCurrentSection() == 0) {
		write(MAX_SUBSEQUENCE, MutationRef{ MutationRef::NoOp, ""_sr, ""_sr });
	}

	serializer.completeSectionWriting();

	details::SubsequencedItemsHeader sectionHeader;

	sectionHeader.version = header.lastVersion;
	sectionHeader.numItems = serializer.getNumItemsCurrentSection();
	sectionHeader.length = serializer.getTotalBytes() - header.length;

	serializer.writeSectionHeader(sectionHeader);
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

ProxySubsequencedMessageSerializer::ProxySubsequencedMessageSerializer(const Version& version_) : version(version_) {}

const Version& ProxySubsequencedMessageSerializer::getVersion() const {
	return version;
}

void ProxySubsequencedMessageSerializer::broadcastSpanContext(const SpanContextMessage& spanContext) {
	spanContextMessage = spanContext;
	storageTeamInjectedSpanContext.clear();
}

void ProxySubsequencedMessageSerializer::write(const MutationRef& mutation, const StorageTeamID& storageTeamID) {
	if (serializers.find(storageTeamID) == serializers.end()) {
		serializers.emplace(storageTeamID, storageTeamID);
		serializers.at(storageTeamID).startVersionWriting(version);
	}

	if (spanContextMessage.present()) {
		if (storageTeamInjectedSpanContext.find(storageTeamID) == storageTeamInjectedSpanContext.end()) {
			const SpanContextMessage& message = spanContextMessage.get();

			storageTeamInjectedSpanContext.insert(storageTeamID);
			serializers.at(storageTeamID).write(SubsequenceSpanContextItem{ subsequence++, message });
		}
	}

	serializers.at(storageTeamID).write(SubsequenceMutationItem{ subsequence++, mutation });
}

StringRef ProxySubsequencedMessageSerializer::getSerialized(const StorageTeamID& storageTeamID) {
	auto& serializer = serializers.at(storageTeamID);

	serializer.completeVersionWriting();
	serializer.completeMessageWriting();

	return serializer.getSerialized();
}

std::unordered_map<StorageTeamID, Standalone<StringRef>> ProxySubsequencedMessageSerializer::getAllSerialized() {
	std::unordered_map<StorageTeamID, Standalone<StringRef>> result;
	for (auto& [storageTeamID, serializer] : serializers) {

		result[storageTeamID] = getSerialized(storageTeamID);
	}
	return result;
}

SubsequencedMessageDeserializer::SubsequencedMessageDeserializer(const Standalone<StringRef>& serialized_)
  : endIterator(serialized_.arena(), serialized_, true) {
	reset(serialized_.arena(), serialized_);
}

SubsequencedMessageDeserializer::SubsequencedMessageDeserializer(const Arena& serializedArena_,
                                                                 const StringRef serialized_)
  : endIterator(serializedArena_, serialized_, true) {
	reset(serializedArena_, serialized_);
}

void SubsequencedMessageDeserializer::reset(const Arena& serializedArena_, const StringRef serialized_) {
	ASSERT(serialized_.size() > 0);

	serializedArena = serializedArena_;
	serialized = serialized_;

	DeserializerImpl deserializer(serializedArena, serialized);
	header = deserializer.deserializeAsMainHeader();

	endIterator = iterator(serializedArena, serialized, true);
}

const StorageTeamID& SubsequencedMessageDeserializer::getStorageTeamID() const {
	return header.storageTeamID;
}

size_t SubsequencedMessageDeserializer::getNumVersions() const {
	return header.numItems;
}

const Version& SubsequencedMessageDeserializer::getFirstVersion() const {
	return header.firstVersion;
}

const Version& SubsequencedMessageDeserializer::getLastVersion() const {
	return header.lastVersion;
}
SubsequencedMessageDeserializer::iterator::iterator(const Arena& serializedArena_,
                                                    StringRef serialized_,
                                                    bool isEndIterator)
  : deserializer(serializedArena_, serialized_), rawSerializedData(serialized_) {

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

	// Current version section is not completely consumed
	++itemIndex;

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

SubsequencedMessageDeserializer::iterator SubsequencedMessageDeserializer::begin() const {
	// Since the iterator is setting to a state that it is located at the end of a version section,
	// doing a prefix ++ will trigger it read a new version section, and place itself to the beginning
	// of the items in the section.
	return ++iterator(serializedArena, serialized, false);
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
