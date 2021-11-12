/*
 * Serializer.h
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

#ifndef FDBSERVER_PTXN_SERIALIZER_H
#define FDBSERVER_PTXN_SERIALIZER_H

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "flow/crc32c.h"
#include "flow/Error.h"
#include "flow/serialize.h"

namespace ptxn {

// Number of bytes of VersionOption, which is serialized when constructing the BinaryWriter
const size_t SerializerVersionOptionBytes =
    BinaryWriter(IncludeVersion(ProtocolVersion::withPartitionTransaction())).getLength();

// Get the number of bytes of a serialized object. Does not work with any objects that has flexible size.
template <typename T>
size_t getSerializedBytes() {
	// Due to padding, the serialized object may take less space, have to figure out by doing real serialization.
	static size_t value =
	    BinaryWriter::toValue(T(), IncludeVersion(ProtocolVersion::withPartitionTransaction())).size() -
	    SerializerVersionOptionBytes;
	return value;
}

using SerializationProtocolVersion = uint8_t;

// Base class for headers with multiple items following
struct MultipleItemHeaderBase {
	// The version of the protocol
	SerializationProtocolVersion protocolVersion;

	// Number of items
	int32_t numItems = 0;

	// The raw length, i.e. the number of bytes, in this message, excluding the header
	int32_t length = 0;

	explicit MultipleItemHeaderBase(SerializationProtocolVersion protocolVersion_)
	  : protocolVersion(protocolVersion_) {}

	std::string toString() const {
		return concatToString("Protocol=", static_cast<int>(protocolVersion), " Items=", numItems, " Length=", length);
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, protocolVersion, numItems, length);
	}
};

// Encode objects in format
//
//  | Header | Item | Item | ... |
//
// The Header MUST have a fixed size when serialized.
template <typename Header, typename Item>
class HeaderedItemsSerializer {
	BinaryWriter writer;

	// If true, no more items should be written to the writer
	bool itemComplete = false;

	// Number of items
	size_t numItems = 0;

public:
	using header_t = Header;
	using item_t = Item;

	HeaderedItemsSerializer() : writer(IncludeVersion(ProtocolVersion::withPartitionTransaction())) {
		writer << header_t();
	}

	// Write a new item into the serializer
	void writeItem(const item_t& item) {
		ASSERT(!isWritingCompleted());
		writer << item;
		++numItems;
	}

	// Write the header of the serialized data, use only after calling completeItemWriting
	void writeHeader(const header_t& header) {
		ASSERT(isWritingCompleted());

		// Note: Due to padding, we cannot directly copy the header to writer.getData()
		// Need to serialize it first
		Standalone<StringRef> serialized =
		    BinaryWriter::toValue(header, IncludeVersion(ProtocolVersion::withPartitionTransaction()));
		std::memcpy(reinterpret_cast<uint8_t*>(writer.getData()) + SerializerVersionOptionBytes,
		            serialized.begin() + SerializerVersionOptionBytes,
		            getHeaderBytes());
	}

	// Returns the bytes used to serialize the header
	size_t getHeaderBytes() const { return getSerializedBytes<header_t>(); }

	// Returns the bytes used to serialize the items
	size_t getItemsBytes() const { return writer.getLength() - SerializerVersionOptionBytes - getHeaderBytes(); }

	// Returns the total bytes the serializer used
	size_t getTotalBytes() const { return writer.getLength(); }

	// Returns the number of items
	size_t getNumItems() const { return numItems; }

	uint32_t getItemsCRC32() const {
		return crc32c_append(
		    0, reinterpret_cast<uint8_t*>(const_cast<void*>(writer.getData())) + getHeaderBytes(), getItemsBytes());
	}

	// Marks the serializer not accepting more items, and the header is ready for writing
	void completeItemWriting() { itemComplete = true; }

	// Returns true if the serializer is not accepting more items
	bool isWritingCompleted() const { return itemComplete; }

	// Returns the serialized data
	Standalone<StringRef> getSerialized() const {
		ASSERT(itemComplete);

		return writer.toValue();
	}
};

namespace details {
template <typename Header>
Header readSerializedHeader(StringRef serializedData) {
	BinaryReader reader(serializedData, IncludeVersion(ProtocolVersion::withPartitionTransaction()));

	Header header;
	reader >> header;

	return header;
}

} // namespace details

// Encode objects in format
//
//   | Main Header | Section Header | Item | Item | ... | Section Header | Item | ... |
//
// Main Header and Section Header *must* have a fixed size. Each Item is an object that being serialized into a
// StringRef, and the user must provide informations (e.g. object type, length, etc.) to deserialize them. Main Header
// and Section Header *should* inherit from MultipleItemHeaderBase It is obvious that TwoLevelHeaderedItemSerializer can
// be implemented using HeaderedItemsSerializer. The reason not use this strategy is that one additional memory copy
// will be included when a new section is opened.
template <typename MainHeader, typename SectionHeader>
class TwoLevelHeaderedItemsSerializer {
	BinaryWriter writer;

	// If true, no more items should be written to the writer
	bool allComplete = false;

	// If true, no more items should be written to the current section
	bool sectionComplete = true;

	// Number of sections
	size_t numSections = 0;

	// Number of items in the current section
	size_t numItemsCurrentSection = 0;

	// Number of items (in total)
	size_t numItems = 0;

	// Number of bytes before the new section starts
	// NOTE: We *CANNOT* use a pointer to mark the end of last section, as Arena *may* or *may not* reallocate
	size_t bytesBeforeCurrentSection = 0;

public:
	using main_header_t = MainHeader;
	using section_header_t = SectionHeader;

	TwoLevelHeaderedItemsSerializer() : writer(IncludeVersion(ProtocolVersion::withPartitionTransaction())) {
		writer << main_header_t();
	}

	// Writes the header of the serialized data, call only after completeAllItemsWriting()
	void writeHeader(const main_header_t& header) {
		ASSERT(isAllItemsCompleted() && isSectionCompleted());

		Standalone<StringRef> serialized =
		    BinaryWriter::toValue(header, IncludeVersion(ProtocolVersion::withPartitionTransaction()));
		std::memcpy(reinterpret_cast<uint8_t*>(writer.getData()) + SerializerVersionOptionBytes,
		            serialized.begin() + SerializerVersionOptionBytes,
		            getMainHeaderBytes());
	}

	// Writes the section header, call only after completeSectionWriting()
	void writeSectionHeader(const section_header_t& header) {
		ASSERT(!isAllItemsCompleted() && isSectionCompleted());

		Standalone<StringRef> serialized =
		    BinaryWriter::toValue(header, IncludeVersion(ProtocolVersion::withPartitionTransaction()));
		std::memcpy(reinterpret_cast<uint8_t*>(writer.getData()) + bytesBeforeCurrentSection,
		            serialized.begin() + SerializerVersionOptionBytes,
		            getSectionHeaderBytes());
	}

	// Starts a new section, must be called after the previous section is completed and the section header is written.
	void startNewSection() {
		ASSERT(!isAllItemsCompleted() && isSectionCompleted());

		sectionComplete = false;
		bytesBeforeCurrentSection = writer.getLength();
		writer << section_header_t();
		++numSections;
		numItemsCurrentSection = 0;
	}

	// Writes an item to the current section, the current section must be open
	template <typename Item_t>
	void writeItem(const Item_t& item) {
		ASSERT(!isAllItemsCompleted() && !isSectionCompleted());

		writer << item;

		++numItemsCurrentSection;
		++numItems;
	}

	// Writes an serialized section. The section will be closed from further appending.
	// The serialized data should *NOT* have the protocol version information prefixed.
	// Returns the header of the section
	section_header_t writeSerializedSection(StringRef serialized) {
		ASSERT(!isAllItemsCompleted() && isSectionCompleted());

		// We assume a protocol version, the assumed version is *NEVER* used.
		section_header_t sectionHeader =
		    BinaryReader::fromStringRef<section_header_t>(serialized, AssumeVersion(currentProtocolVersion));

		if constexpr (std::is_base_of<MultipleItemHeaderBase, section_header_t>::value) {
			numItemsCurrentSection = sectionHeader.numItems;
			numItems += sectionHeader.numItems;
		}

		++numSections;

		writer.serializeBytes(serialized);

		return sectionHeader;
	}

	// Returns the size of the main header, in bytes
	size_t getMainHeaderBytes() const { return getSerializedBytes<main_header_t>(); }

	// Returns the size of a single section header, in bytes
	size_t getSectionHeaderBytes() const { return getSerializedBytes<section_header_t>(); }

	// Returns the bytes of the serialized data
	size_t getTotalBytes() const { return writer.getLength(); }

	// Returns the number of sections
	size_t getNumSections() const { return numSections; }

	// Returns the number of items in the current section
	size_t getNumItemsCurrentSection() const { return numItemsCurrentSection; }

	// Marks the current section not accepting new items, and the section header is ready to be written.
	void completeSectionWriting() { sectionComplete = true; }

	// True if the current section is closed.
	bool isSectionCompleted() const { return sectionComplete; }

	// Marks the current serializer is not accepting new items/sections, and the header is ready to be written.
	void completeAllItemsWriting() {
		ASSERT(isSectionCompleted());

		allComplete = true;
	}

	// True if the serializer is closed.
	bool isAllItemsCompleted() const { return allComplete; }

	// Gets the final serialized data
	Standalone<StringRef> getSerialized() const {
		ASSERT(isAllItemsCompleted());

		return writer.toValue();
	}
};

// Loads the serialized data into header and items
template <typename Header, typename Item>
bool headeredItemDeserializerBase(const Arena& arena, StringRef serialized, Header& header, std::vector<Item>& items) {
	ArenaReader reader(arena, serialized, IncludeVersion(ProtocolVersion::withPartitionTransaction()));

	if (reader.empty()) {
		return false;
	};

	reader >> header;
	while (!reader.empty()) {
		Item item;
		reader >> item;
		items.emplace_back(std::move(item));
	}
	return true;
}

// Deserializes the TwoLevelHeaderedItemsSerializer
template <typename MainHeader, typename SectionHeader>
class TwoLevelHeaderedItemsDeserializer {
	BinaryReader reader;

public:
	using main_header_t = MainHeader;
	using section_header_t = SectionHeader;

	// serialized is the StringRef points to the serialied data
	TwoLevelHeaderedItemsDeserializer(StringRef serialized)
	  : reader(serialized, IncludeVersion(ProtocolVersion::withPartitionTransaction())) {
		// The serialized data *MUST* have a header
		ASSERT(!allConsumed());
	}

	// Extracts an element from serialized data in Main Header format
	// NOTE It is the caller's obligation to ensure the element is in main header format
	main_header_t deserializeAsMainHeader() {
		main_header_t mainHeader;
		deserializeItem(mainHeader);
		return mainHeader;
	}

	// Extracts an element from serialized data in Section Header format
	// NOTE It is the caller's obligation to ensure the element is in section header format
	section_header_t deserializeAsSectionHeader() {
		section_header_t sectionHeader;
		deserializeItem(sectionHeader);
		return sectionHeader;
	}

	// Extracts an element from serialized data in item_t format and returns the element
	// NOTE It is the caller's obligation to ensure the element is in item format
	template <typename Item_t>
	Item_t deserializeItem() {
		Item_t item;
		deserializeItem(item);
		return item;
	}

	// Extracts an element from serialized data in item_t format, and stores it in the incoming parameter.
	template <typename Item_t>
	void deserializeItem(Item_t& item) {
		ASSERT(!allConsumed());
		reader >> item;
	}

	// Returns true if reached the end of serialized data
	bool allConsumed() const { return reader.empty(); }

	// Peeks raw bytes, but doesn't consume the bytes
	const uint8_t* peekBytes(int nBytes) const { return reinterpret_cast<const uint8_t*>(reader.peekBytes(nBytes)); }

	// Returns the arena the deserialized data uses.
	Arena& arena() { return reader.arena(); }
};

} // namespace ptxn

#endif // FDBSERVER_PTXN_SERIALIZER_H
