/*
 * TLogStorageServerPeekMessageSerializer.cpp
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

#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"

#include <string>

#include "fdbclient/FDBTypes.h"
#include "flow/Error.h"
#include "flow/String.h"

namespace ptxn {

TLogStorageServerMessageSerializer::TLogStorageServerMessageSerializer(const TeamID& teamID) {
	header.teamID = teamID;
}

void TLogStorageServerMessageSerializer::startVersionWriting(const Version& version) {
	// Ensure the ordering of version.
	ASSERT(version > header.lastVersion);

	if (header.firstVersion == invalidVersion) {
		header.firstVersion = version;
	}
	header.lastVersion = version;
	header.lastSubsequence = 0;

	serializer.startNewSection();
}

void TLogStorageServerMessageSerializer::writeSubsequenceMutationRef(const SubsequenceMutationItem& item) {
	ASSERT(header.lastVersion != invalidVersion && header.lastSubsequence < item.subsequence);

	serializer.writeItem(item);

	header.lastSubsequence = item.subsequence;
}

void TLogStorageServerMessageSerializer::writeSubsequenceMutationRef(const Subsequence& subsequence,
                                                                     const MutationRef& mutationRef) {
	writeSubsequenceMutationRef({ subsequence, mutationRef });
}

const Version& TLogStorageServerMessageSerializer::getCurrentVersion() const {
	return header.lastVersion;
}

const Subsequence& TLogStorageServerMessageSerializer::getCurrentSubsequence() const {
	return header.lastSubsequence;
}

void TLogStorageServerMessageSerializer::completeVersionWriting() {
	serializer.completeSectionWriting();

	SubsequenceMutationItemsHeader sectionHeader;

	sectionHeader.version = header.lastVersion;
	sectionHeader.numItems = serializer.getNumItemsCurrentSection();
	sectionHeader.length = serializer.getTotalBytes() - header.length;

	serializer.writeSectionHeader(sectionHeader);
}

void TLogStorageServerMessageSerializer::completeMessageWriting() {
	serializer.completeAllItemsWriting();

	header.numItems = serializer.getNumSections();
	header.length = serializer.getTotalBytes() - serializer.getMainHeaderBytes();

	serializer.writeHeader(header);
}

Standalone<StringRef> TLogStorageServerMessageSerializer::getSerialized() {
	ASSERT(serializer.isAllItemsCompleted());

	return serializer.getSerialized();
}

size_t TLogStorageServerMessageSerializer::getTotalBytes() const {
	return serializer.getTotalBytes();
}

TLogStorageServerMessageDeserializer::TLogStorageServerMessageDeserializer(const Standalone<StringRef>& serialized_)
  : endIterator(serialized_.arena(), serialized_, true) {
	reset(serialized_.arena(), serialized_);
}

TLogStorageServerMessageDeserializer::TLogStorageServerMessageDeserializer(const Arena& serializedArena_,
                                                                           const StringRef serialized_)
  : endIterator(serializedArena_, serialized_, true) {
	reset(serializedArena_, serialized_);
}

void TLogStorageServerMessageDeserializer::reset(const Arena& serializedArena_, const StringRef serialized_) {
	ASSERT(serialized_.size() > 0);

	serializedArena = serializedArena_;
	serialized = serialized_;

	DeserializerImpl deserializer(serializedArena, serialized);
	header = deserializer.deserializeAsMainHeader();

	endIterator = iterator(serializedArena, serialized, true);
}

const TeamID& TLogStorageServerMessageDeserializer::getTeamID() const {
	return header.teamID;
}

size_t TLogStorageServerMessageDeserializer::getNumVersions() const {
	return header.numItems;
}

const Version& TLogStorageServerMessageDeserializer::getFirstVersion() const {
	return header.firstVersion;
}

const Version& TLogStorageServerMessageDeserializer::getLastVersion() const {
	return header.lastVersion;
}

TLogStorageServerMessageDeserializer::iterator::iterator(const Arena& serializedArena_,
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

bool TLogStorageServerMessageDeserializer::iterator::operator==(const iterator& another) const {
	return (rawSerializedData == another.rawSerializedData && sectionIndex == another.sectionIndex &&
	        itemIndex == another.itemIndex);
}

bool TLogStorageServerMessageDeserializer::iterator::operator!=(const iterator& another) const {
	return !(*this == another);
}

TLogStorageServerMessageDeserializer::iterator::reference TLogStorageServerMessageDeserializer::iterator::operator*()
    const {
	return currentItem;
}

TLogStorageServerMessageDeserializer::iterator::pointer TLogStorageServerMessageDeserializer::iterator::operator->()
    const {
	return &currentItem;
}

TLogStorageServerMessageDeserializer::iterator& TLogStorageServerMessageDeserializer::iterator::operator++() {
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

	SubsequenceMutationItem item = deserializer.deserializeItem();
	currentItem.subsequence = item.subsequence;
	currentItem.mutation = item.mutation;

	return *this;
}

TLogStorageServerMessageDeserializer::iterator TLogStorageServerMessageDeserializer::iterator::operator++(int) {
	iterator prev(*this);
	this->operator++();
	return prev;
}

TLogStorageServerMessageDeserializer::iterator TLogStorageServerMessageDeserializer::begin() const {
	// Since the iterator is setting to a state that it is located at the end of a version section,
	// doing a prefix ++ will trigger it read a new version section, and place itself to the beginning
	// of the items in the section.
	return ++iterator(serializedArena, serialized, false);
}

const TLogStorageServerMessageDeserializer::iterator& TLogStorageServerMessageDeserializer::end() const {
	return endIterator;
}

TLogStorageServerMessageDeserializer::const_iterator TLogStorageServerMessageDeserializer::cbegin() const {
	return begin();
}

const TLogStorageServerMessageDeserializer::const_iterator& TLogStorageServerMessageDeserializer::cend() const {
	return end();
}

} // namespace ptxn