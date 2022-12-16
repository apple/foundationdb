/*
 * Msgpack.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#ifndef FDBRPC_MSGPACK_H
#define FDBRPC_MSGPACK_H
#include <limits>
#pragma once

#include <memory>
#include <algorithm>
#include "flow/Trace.h"
#include "flow/Error.h"
#include "flow/network.h"

struct MsgpackBuffer {
	std::unique_ptr<uint8_t[]> buffer;
	// Amount of data in buffer (bytes).
	std::size_t data_size;
	// Size of buffer (bytes).
	std::size_t buffer_size;

	void write_byte(uint8_t byte) { write_bytes(&byte, 1); }

	// This assumes that pos <= data_size
	void edit_byte(uint8_t byte, size_t pos) { buffer[pos] = byte; }

	void write_bytes(const uint8_t* buf, std::size_t n) {
		resize(n);
		std::copy(buf, buf + n, buffer.get() + data_size);
		data_size += n;
	}

	void resize(std::size_t n) {
		if (data_size + n <= buffer_size) {
			return;
		}

		std::size_t size = buffer_size;
		while (size < data_size + n) {
			size *= 2;
		}

		TraceEvent(SevInfo, "MsgpackResizedBuffer").detail("OldSize", buffer_size).detail("NewSize", size);
		auto new_buffer = std::make_unique<uint8_t[]>(size);
		std::copy(buffer.get(), buffer.get() + data_size, new_buffer.get());
		buffer = std::move(new_buffer);
		buffer_size = size;
	}

	void reset() { data_size = 0; }
};

inline void serialize_bool(bool val, MsgpackBuffer& buf) {
	if (val) {
		buf.write_byte(0xc3);
	} else {
		buf.write_byte(0xc2);
	}
}

// Writes the given value in big-endian format to the request. Sets the
// first byte to msgpack_type.
template <typename T>
inline void serialize_value(const T& val, MsgpackBuffer& buf, uint8_t msgpack_type) {
	buf.write_byte(msgpack_type);

	const uint8_t* p = reinterpret_cast<const uint8_t*>(std::addressof(val));
	for (size_t i = 0; i < sizeof(T); ++i) {
		buf.write_byte(p[sizeof(T) - i - 1]);
	}
}

// Writes the given string to the request as a sequence of bytes. Inserts a
// format byte at the beginning of the string according to the its length,
// as specified by the msgpack specification.
inline void serialize_string(const uint8_t* c, int length, MsgpackBuffer& buf) {
	if (length <= 31) {
		// A size 0 string is ok. We still need to write a byte
		// identifiying the item as a string, but can set the size to 0.
		buf.write_byte(static_cast<uint8_t>(length) | 0b10100000);
	} else if (length <= 255) {
		buf.write_byte(0xd9);
		buf.write_byte(static_cast<uint8_t>(length));
	} else if (length <= 65535) {
		buf.write_byte(0xda);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&length)[1]);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&length)[0]);
	} else {
		TraceEvent(SevWarn, "MsgpackSerializeString").detail("Failed to MessagePack encode very large string", length);
		ASSERT_WE_THINK(false);
	}

	buf.write_bytes(c, length);
}

inline void serialize_string(const std::string& str, MsgpackBuffer& buf) {
	serialize_string(reinterpret_cast<const uint8_t*>(str.data()), str.size(), buf);
}

template <typename T, typename F>
inline void serialize_vector(const std::vector<T>& vec, MsgpackBuffer& buf, F f) {
	size_t size = vec.size();
	if (size <= 15) {
		buf.write_byte(static_cast<uint8_t>(size) | 0b10010000);
	} else if (size <= 65535) {
		buf.write_byte(0xdc);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[1]);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[0]);
	} else if (size <= std::numeric_limits<uint32_t>::max()) {
		buf.write_byte(0xdd);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[3]);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[2]);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[1]);
		buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[0]);
	} else {
		TraceEvent(SevWarn, "MsgPackSerializeVector").detail("Failed to MessagePack encode large vector", size);
		ASSERT_WE_THINK(false);
	}
	// Use the provided serializer function to serialize the individual types of the vector
	for (const auto& val : vec) {
		f(val, buf);
	}
}

template <class Map>
inline void serialize_map(const Map& map, MsgpackBuffer& buf) {
	int size = map.size();

	if (size <= 15) {
		buf.write_byte(static_cast<uint8_t>(size) | 0b10000000);
	} else {
		TraceEvent(SevWarn, "MsgPackSerializeMap").detail("Failed to MessagePack encode large map", size);
		ASSERT_WE_THINK(false);
	}

	for (const auto& [key, value] : map) {
		serialize_string(key.begin(), key.size(), buf);
		serialize_string(value.begin(), value.size(), buf);
	}
}

// Serializes object T according to ext msgpack specification
template <typename T, typename F>
inline void serialize_ext(const T& t, MsgpackBuffer& buf, uint8_t type, F f) {
	buf.write_byte(0xc9);
	// We don't know for sure the amount of bytes we'll be writing.
	// So for now we set the payload size as zero and then we take the difference in data size
	// from now and after we invoke f to determine how many bytes were written
	size_t byte_idx = buf.data_size;
	for (int i = 0; i < 4; i++) {
		buf.write_byte(0);
	}
	buf.write_byte(type);
	size_t prev_size = buf.data_size;
	f(t, buf);
	size_t updated_size = static_cast<size_t>(buf.data_size - prev_size);
	ASSERT_WE_THINK(updated_size <= std::numeric_limits<uint32_t>::max());
	buf.edit_byte(reinterpret_cast<const uint8_t*>(&updated_size)[3], byte_idx);
	buf.edit_byte(reinterpret_cast<const uint8_t*>(&updated_size)[2], byte_idx + 1);
	buf.edit_byte(reinterpret_cast<const uint8_t*>(&updated_size)[1], byte_idx + 2);
	buf.edit_byte(reinterpret_cast<const uint8_t*>(&updated_size)[0], byte_idx + 3);
}
#endif
