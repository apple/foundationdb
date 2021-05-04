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

// Get the number of bytes of a serialized object. Does not work with any objects that has flexible size.
template <typename T>
size_t getSerializedBytes() {
	// Due to padding, the serialized object may take less space, have to figure
	// out by evaluating.
	static size_t value = BinaryWriter::toValue(T(), AssumeVersion(g_network->protocolVersion())).size();
	return value;
}

// Encode objects in the format
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

	HeaderedItemsSerializer() : writer(AssumeVersion(g_network->protocolVersion())) { writer << header_t(); }

	// Write a new item into the serializer
	void writeItem(const item_t& item) {
		ASSERT(!isWritingCompleted());
		writer << item;
		++numItems;
	}

	// Write the header of the serialized data, use only after calling completeItemWriting
	void writeHeader(const header_t& header) {
		ASSERT(isWritingCompleted());

		// Note: Due to padding, we can NOT directly copy the header to writer.getData()
		Standalone<StringRef> serialized = BinaryWriter::toValue(header, AssumeVersion(g_network->protocolVersion()));
		std::memcpy(reinterpret_cast<uint8_t*>(writer.getData()), serialized.begin(), getHeaderBytes());
	}

	size_t getHeaderBytes() const { return getSerializedBytes<header_t>(); }

	size_t getItemsBytes() const { return writer.getLength() - getHeaderBytes(); }

	size_t getTotalBytes() const { return writer.getLength(); }

	size_t getNumItems() const { return numItems; }

	uint32_t getItemsCRC32() const {
		return crc32c_append(
		    0, reinterpret_cast<uint8_t*>(const_cast<void*>(writer.getData())) + getHeaderBytes(), getItemsBytes());
	}

	// Marks the serializer not accepting more items, and the header is ready for writing
	void completeItemWriting() { itemComplete = true; }

	// Return true if the serializer is not accepting more items
	bool isWritingCompleted() const { return itemComplete; }

	// Get the serialized data
	Standalone<StringRef> getSerialized() const {
		ASSERT(itemComplete);

		return writer.toValue();
	}
};

template <typename MainHeader, typename SectionHeader, typename Item>
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
	using item_t = Item;

	TwoLevelHeaderedItemsSerializer() : writer(AssumeVersion(g_network->protocolVersion())) {
		writer << main_header_t();
	}

	// Write the header of the serialized data, call only after completeAllItemsWriting()
	void writeHeader(const main_header_t& header) {
		ASSERT(isAllItemsCompleted() && isSectionCompleted());

		Standalone<StringRef> serialized = BinaryWriter::toValue(header, AssumeVersion(g_network->protocolVersion()));
		std::memcpy(reinterpret_cast<uint8_t*>(writer.getData()), serialized.begin(), getMainHeaderBytes());
	}

	// Write the section header, call only after completeSectionWriting()
	void writeSectionHeader(const section_header_t& header) {
		ASSERT(!isAllItemsCompleted() && isSectionCompleted());

		Standalone<StringRef> serialized = BinaryWriter::toValue(header, AssumeVersion(g_network->protocolVersion()));
		std::memcpy(reinterpret_cast<uint8_t*>(writer.getData()) + bytesBeforeCurrentSection, serialized.begin(), getSectionHeaderBytes());
	}

	// Start a new section, must be called after the previous section is completed and the section header is written.
	void startNewSection() {
		ASSERT(!isAllItemsCompleted() && isSectionCompleted());

		sectionComplete = false;
		bytesBeforeCurrentSection = writer.getLength();
		writer << section_header_t();
		++numSections;
		numItemsCurrentSection = 0;
	}

	// Write an item to the current section, the current section must be open
	void writeItem(const Item& item) {
		ASSERT(!isAllItemsCompleted() && !isSectionCompleted());

		writer << item;

		++numItemsCurrentSection;
		++numItems;
	}

	size_t getMainHeaderBytes() const { return getSerializedBytes<main_header_t>(); }

	size_t getSectionHeaderBytes() const { return getSerializedBytes<section_header_t>(); }

	size_t getTotalBytes() const { return writer.getLength(); }

	size_t getNumSections() const { return numSections; }

	size_t getNumItemsCurrentSection() const { return numItemsCurrentSection; }

	// Get the number of items in *ALL* sections
	size_t getNumItems() const { return numItems; }

	// Mark the current section not accepting new items, and the section header is ready to be written.
	void completeSectionWriting() { sectionComplete = true; }

	// True if the current section is closed.
	bool isSectionCompleted() const { return sectionComplete; }

	// Mark the current serializer is not accepting new items/sections, and the header is ready to be written.
	void completeAllItemsWriting() {
		ASSERT(isSectionCompleted());

		allComplete = true;
	}

	// True if the serializer is closed.
	bool isAllItemsCompleted() const { return allComplete; }

	// Get the final serialized data
	Standalone<StringRef> getSerialized() const {
		ASSERT(isAllItemsCompleted());

		return writer.toValue();
	}
};

// Load the serialized data into header and items
template <typename Header, typename Item>
bool headeredItemDeserializerBase(const Arena& arena, StringRef serialized, Header& header, std::vector<Item>& items) {
	ArenaReader reader(arena, serialized, AssumeVersion(g_network->protocolVersion()));

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

// Deserialize the TwoLevelHeaderedItemsSerializer
template <typename MainHeader, typename SectionHeader, typename Item>
class TwoLevelHeaderedItemsDeserializer {
	ArenaReader reader;

public:
	using main_header_t = MainHeader;
	using section_header_t = SectionHeader;
	using item_t = Item;

	// arena is the arena that contains the serialized data
	// serialized is the StringRef points to the serialied data
	TwoLevelHeaderedItemsDeserializer(const Arena& arena, StringRef serialized)
	  : reader(arena, serialized, AssumeVersion(g_network->protocolVersion())) {
		// The serialized data *MUST* have a header
		ASSERT(!allConsumed());
	}

	// Extract an element from serialized data in Main Header format
	// NOTE It is the caller's obligation to ensure the element is in main header format
	main_header_t deserializeAsMainHeader() {
		ASSERT(!allConsumed());

		main_header_t mainHeader;
		reader >> mainHeader;
		return mainHeader;
	}

	// Extract an element from serialized data in Section Header format
	// NOTE It is the caller's obligation to ensure the element is in section header format
	section_header_t deserializeAsSectionHeader() {
		ASSERT(!allConsumed());

		section_header_t sectionHeader;
		reader >> sectionHeader;
		return sectionHeader;
	}

	// Extract an element from serialized data in item format
	// NOTE It is the caller's obligation to ensure the element is in item format
	item_t deserializeItem() {
		ASSERT(!allConsumed());

		item_t item;
		reader >> item;
		return item;
	}

	// Return true if reached the end of serialized data
	bool allConsumed() const { return reader.empty(); }
};

using SerializationProtocolVersion = uint8_t;

// Base class for headers with multiple items following
struct MultipleItemHeaderBase {
	// The version of the protocol
	SerializationProtocolVersion protocolVersion;

	// Number of items
	size_t numItems;

	// The raw length, i.e. the number of bytes, in this message, excluding the header
	size_t length;

	explicit MultipleItemHeaderBase(SerializationProtocolVersion protocolVersion_)
	  : protocolVersion(protocolVersion_) {}

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		reader >> protocolVersion >> numItems >> length;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, protocolVersion, numItems, length);
	}
};

} // namespace ptxn

#endif // FDBSERVER_PTXN_SERIALIZER_H
