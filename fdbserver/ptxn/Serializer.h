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

#ifndef FDBSERVER_PTXN_ENCODER_H
#define FDBSERVER_PTXN_ENCODER_H

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "flow/crc32c.h"
#include "flow/Error.h"
#include "flow/error_definitions.h"
#include "flow/serialize.h"

/**
 * Get the number of bytes of a serialized object. Does not work with any objects that has flexible size.
 */
template <typename T>
size_t getSerializedBytes() {
	// Due to padding, the serialized object may take less space, have to figure
	// out by evaluating.
	static size_t value = BinaryWriter::toValue(T(), AssumeVersion(g_network->protocolVersion())).size();
	return value;
}

/**
 * @class HeaderedItemsSerializerBase
 *
 * Encode objects in the format
 *
 *  | Header | Item | Item | ... |
 *
 * The Header MUST have a fixed size when serialized.
 */
template <typename Header, typename Item>
class HeaderedItemsSerializerBase {
	BinaryWriter writer;

	/// If true, no more items should be written to the writer
	bool itemComplete = false;

	/// Number of items
	size_t numItems = 0;

public:
	using header_t = Header;
	using item_t = Item;

	HeaderedItemsSerializerBase() : writer(BinaryWriter(AssumeVersion(g_network->protocolVersion()))) {
		writer << header_t();
	}

	void writeItem(const Item& item) {
		ASSERT(!itemComplete);
		writer << item;
		++numItems;
	}

	void writeHeader(const Header& header) {
		ASSERT(itemComplete);

		// Note: Due to padding, we can NOT directly copy the header to writer.getData()
		Standalone<StringRef> serializedHeader =
		    BinaryWriter::toValue(header, AssumeVersion(g_network->protocolVersion()));
		std::memcpy(reinterpret_cast<uint8_t*>(writer.getData()), serializedHeader.begin(), getHeaderBytes());
	}

	size_t getHeaderBytes() const { return getSerializedBytes<header_t>(); }

	size_t getItemsBytes() const { return writer.getLength() - getHeaderBytes(); }

	size_t getTotalBytes() const { return writer.getLength(); }

	size_t getNumItems() const { return numItems; }

	uint32_t getItemsCRC32() const {
		return crc32c_append(
		    0, reinterpret_cast<uint8_t*>(const_cast<void*>(writer.getData())) + getHeaderBytes(), getItemsBytes());
	}

	void completeItemWriting() { itemComplete = true; }

	bool isWritingCompleted() const { return itemComplete; }

	Standalone<StringRef> getSerialized() const {
		ASSERT(itemComplete);

		return writer.toValue();
	}
};

/**
 * @brief Load the serialized data into header and items
 */
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

#endif // FDBSERVER_PTXN_ENCODER_H
