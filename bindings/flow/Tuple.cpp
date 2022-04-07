/*
 * Tuple.cpp
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

#include "Tuple.h"

namespace FDB {
// The floating point operations depend on this using the IEEE 754 standard.
static_assert(std::numeric_limits<float>::is_iec559);
static_assert(std::numeric_limits<double>::is_iec559);

const size_t Uuid::SIZE = 16;

const uint8_t Tuple::NULL_CODE = 0x00;
const uint8_t Tuple::BYTES_CODE = 0x01;
const uint8_t Tuple::STRING_CODE = 0x02;
const uint8_t Tuple::NESTED_CODE = 0x05;
const uint8_t Tuple::INT_ZERO_CODE = 0x14;
const uint8_t Tuple::POS_INT_END = 0x1c;
const uint8_t Tuple::NEG_INT_START = 0x0c;
const uint8_t Tuple::FLOAT_CODE = 0x20;
const uint8_t Tuple::DOUBLE_CODE = 0x21;
const uint8_t Tuple::FALSE_CODE = 0x26;
const uint8_t Tuple::TRUE_CODE = 0x27;
const uint8_t Tuple::UUID_CODE = 0x30;

static float bigEndianFloat(float orig) {
	int32_t big = *(int32_t*)&orig;
	big = bigEndian32(big);
	return *(float*)&big;
}

static double bigEndianDouble(double orig) {
	int64_t big = *(int64_t*)&orig;
	big = bigEndian64(big);
	return *(double*)&big;
}

static size_t find_string_terminator(const StringRef data, size_t offset) {
	size_t i = offset;
	while (i < data.size() - 1 && !(data[i] == (uint8_t)'\x00' && data[i + 1] != (uint8_t)'\xff')) {
		i += (data[i] == '\x00' ? 2 : 1);
	}

	return i;
}

static size_t find_string_terminator(const Standalone<VectorRef<unsigned char>> data, size_t offset) {
	size_t i = offset;
	while (i < data.size() - 1 && !(data[i] == '\x00' && data[i + 1] != (uint8_t)'\xff')) {
		i += (data[i] == '\x00' ? 2 : 1);
	}

	return i;
}

// If encoding and the sign bit is 1 (the number is negative), flip all the bits.
// If decoding and the sign bit is 0 (the number is negative), flip all the bits.
// Otherwise, the number is positive, so flip the sign bit.
static void adjust_floating_point(uint8_t* bytes, size_t size, bool encode) {
	if ((encode && ((uint8_t)(bytes[0] & 0x80) != (uint8_t)0x00)) ||
	    (!encode && ((uint8_t)(bytes[0] & 0x80) != (uint8_t)0x80))) {
		for (size_t i = 0; i < size; i++) {
			bytes[i] ^= (uint8_t)0xff;
		}
	} else {
		bytes[0] ^= (uint8_t)0x80;
	}
}

Tuple::Tuple(StringRef const& str) {
	data.append(data.arena(), str.begin(), str.size());

	size_t i = 0;
	int depth = 0;
	while (i < data.size()) {
		if (depth == 0)
			offsets.push_back(i);

		if (depth > 0 && data[i] == NULL_CODE) {
			if (i + 1 < data.size() && data[i + 1] == 0xff) {
				// NULL value.
				i += 2;
			} else {
				// Nested terminator.
				i += 1;
				depth -= 1;
			}
		} else if (data[i] == BYTES_CODE || data[i] == STRING_CODE) {
			i = find_string_terminator(str, i + 1) + 1;
		} else if (data[i] >= NEG_INT_START && data[i] <= POS_INT_END) {
			i += abs(data[i] - INT_ZERO_CODE) + 1;
		} else if (data[i] == NULL_CODE || data[i] == TRUE_CODE || data[i] == FALSE_CODE) {
			i += 1;
		} else if (data[i] == UUID_CODE) {
			i += Uuid::SIZE + 1;
		} else if (data[i] == FLOAT_CODE) {
			i += sizeof(float) + 1;
		} else if (data[i] == DOUBLE_CODE) {
			i += sizeof(double) + 1;
		} else if (data[i] == NESTED_CODE) {
			i += 1;
			depth += 1;
		} else {
			throw invalid_tuple_data_type();
		}
	}

	if (depth != 0) {
		throw invalid_tuple_data_type();
	}
}

// Note: this is destructive of the original offsets, so should only
// be used once we are done.
Tuple::Tuple(Standalone<VectorRef<uint8_t>> data, std::vector<size_t> offsets) {
	this->data = data;
	this->offsets = std::move(offsets);
}

Tuple Tuple::unpack(StringRef const& str) {
	return Tuple(str);
}

Tuple& Tuple::append(Tuple const& tuple) {
	for (size_t offset : tuple.offsets) {
		offsets.push_back(offset + data.size());
	}

	data.append(data.arena(), tuple.data.begin(), tuple.data.size());

	return *this;
}

Tuple& Tuple::append(StringRef const& str, bool utf8) {
	offsets.push_back(data.size());

	const uint8_t utfChar = utf8 ? STRING_CODE : BYTES_CODE;
	data.append(data.arena(), &utfChar, 1);

	size_t lastPos = 0;
	for (size_t pos = 0; pos < str.size(); ++pos) {
		if (str[pos] == '\x00') {
			data.append(data.arena(), str.begin() + lastPos, pos - lastPos);
			data.push_back(data.arena(), (uint8_t)'\x00');
			data.push_back(data.arena(), (uint8_t)'\xff');
			lastPos = pos + 1;
		}
	}

	data.append(data.arena(), str.begin() + lastPos, str.size() - lastPos);
	data.push_back(data.arena(), (uint8_t)'\x00');

	return *this;
}

Tuple& Tuple::append(int32_t value) {
	return append((int64_t)value);
}

Tuple& Tuple::append(int64_t value) {
	uint64_t swap = value;
	bool neg = false;

	offsets.push_back(data.size());

	if (value < 0) {
		value = ~(-value);
		neg = true;
	}

	swap = bigEndian64(value);

	for (int i = 0; i < 8; i++) {
		if (((uint8_t*)&swap)[i] != (neg ? 255 : 0)) {
			data.push_back(data.arena(), (uint8_t)(INT_ZERO_CODE + (8 - i) * (neg ? -1 : 1)));
			data.append(data.arena(), ((const uint8_t*)&swap) + i, 8 - i);
			return *this;
		}
	}

	data.push_back(data.arena(), INT_ZERO_CODE);
	return *this;
}

Tuple& Tuple::append(bool value) {
	offsets.push_back(data.size());
	if (value) {
		data.push_back(data.arena(), TRUE_CODE);
	} else {
		data.push_back(data.arena(), FALSE_CODE);
	}
	return *this;
}

Tuple& Tuple::append(float value) {
	offsets.push_back(data.size());
	float swap = bigEndianFloat(value);
	uint8_t* bytes = (uint8_t*)&swap;
	adjust_floating_point(bytes, sizeof(float), true);

	data.push_back(data.arena(), FLOAT_CODE);
	data.append(data.arena(), bytes, sizeof(float));
	return *this;
}

Tuple& Tuple::append(double value) {
	offsets.push_back(data.size());
	double swap = value;
	swap = bigEndianDouble(swap);
	uint8_t* bytes = (uint8_t*)&swap;
	adjust_floating_point(bytes, sizeof(double), true);

	data.push_back(data.arena(), DOUBLE_CODE);
	data.append(data.arena(), bytes, sizeof(double));
	return *this;
}

Tuple& Tuple::append(Uuid value) {
	offsets.push_back(data.size());
	data.push_back(data.arena(), UUID_CODE);
	data.append(data.arena(), value.getData().begin(), Uuid::SIZE);
	return *this;
}

Tuple& Tuple::appendNested(Tuple const& value) {
	offsets.push_back(data.size());
	data.push_back(data.arena(), NESTED_CODE);

	for (size_t i = 0; i < value.size(); i++) {
		size_t offset = value.offsets[i];
		size_t next_offset = (i + 1 < value.offsets.size() ? value.offsets[i + 1] : value.data.size());
		ASSERT_LT(offset, value.data.size());
		ASSERT_LE(next_offset, value.data.size());
		uint8_t code = value.data[offset];
		if (code == NULL_CODE) {
			data.push_back(data.arena(), NULL_CODE);
			data.push_back(data.arena(), 0xff);
		} else {
			data.append(data.arena(), value.data.begin() + offset, next_offset - offset);
		}
	}

	data.push_back(data.arena(), (uint8_t)'\x00');

	return *this;
}

Tuple& Tuple::appendNull() {
	offsets.push_back(data.size());
	data.push_back(data.arena(), NULL_CODE);
	return *this;
}

Tuple::ElementType Tuple::getType(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	uint8_t code = data[offsets[index]];

	if (code == NULL_CODE) {
		return ElementType::NULL_TYPE;
	} else if (code == BYTES_CODE) {
		return ElementType::BYTES;
	} else if (code == STRING_CODE) {
		return ElementType::UTF8;
	} else if (code == NESTED_CODE) {
		return ElementType::NESTED;
	} else if (code >= NEG_INT_START && code <= POS_INT_END) {
		return ElementType::INT;
	} else if (code == FLOAT_CODE) {
		return ElementType::FLOAT;
	} else if (code == DOUBLE_CODE) {
		return ElementType::DOUBLE;
	} else if (code == FALSE_CODE || code == TRUE_CODE) {
		return ElementType::BOOL;
	} else if (code == UUID_CODE) {
		return ElementType::UUID;
	} else {
		throw invalid_tuple_data_type();
	}
}

Standalone<StringRef> Tuple::getString(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	uint8_t code = data[offsets[index]];
	if (code != BYTES_CODE && code != STRING_CODE) {
		throw invalid_tuple_data_type();
	}

	size_t b = offsets[index] + 1;
	size_t e;
	if (offsets.size() > index + 1) {
		e = offsets[index + 1];
	} else {
		e = data.size();
	}

	Standalone<StringRef> result;
	VectorRef<uint8_t> staging;

	for (size_t i = b; i < e; ++i) {
		if (data[i] == '\x00') {
			staging.append(result.arena(), data.begin() + b, i - b);
			++i;
			b = i + 1;

			if (i < e) {
				staging.push_back(result.arena(), '\x00');
			}
		}
	}

	if (b < e) {
		staging.append(result.arena(), data.begin() + b, e - b);
	}

	result.StringRef::operator=(StringRef(staging.begin(), staging.size()));
	return result;
}

int64_t Tuple::getInt(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	int64_t swap;
	bool neg = false;

	ASSERT_LT(offsets[index], data.size());
	uint8_t code = data[offsets[index]];
	if (code < NEG_INT_START || code > POS_INT_END) {
		throw invalid_tuple_data_type();
	}

	int8_t len = code - INT_ZERO_CODE;

	if (len < 0) {
		len = -len;
		neg = true;
	}

	memset(&swap, neg ? '\xff' : 0, 8 - len);
	memcpy(((uint8_t*)&swap) + 8 - len, data.begin() + offsets[index] + 1, len);

	swap = bigEndian64(swap);

	if (neg) {
		swap = -(~swap);
	}

	return swap;
}

bool Tuple::getBool(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}
	ASSERT_LT(offsets[index], data.size());
	uint8_t code = data[offsets[index]];
	if (code == FALSE_CODE) {
		return false;
	} else if (code == TRUE_CODE) {
		return true;
	} else {
		throw invalid_tuple_data_type();
	}
}

float Tuple::getFloat(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}
	ASSERT_LT(offsets[index], data.size());
	uint8_t code = data[offsets[index]];
	if (code != FLOAT_CODE) {
		throw invalid_tuple_data_type();
	}

	float swap;
	uint8_t* bytes = (uint8_t*)&swap;
	ASSERT_LE(offsets[index] + 1 + sizeof(float), data.size());
	swap = *(float*)(data.begin() + offsets[index] + 1);
	adjust_floating_point(bytes, sizeof(float), false);

	return bigEndianFloat(swap);
}

double Tuple::getDouble(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}
	ASSERT_LT(offsets[index], data.size());
	uint8_t code = data[offsets[index]];
	if (code != DOUBLE_CODE) {
		throw invalid_tuple_data_type();
	}

	double swap;
	uint8_t* bytes = (uint8_t*)&swap;
	ASSERT_LE(offsets[index] + 1 + sizeof(double), data.size());
	swap = *(double*)(data.begin() + offsets[index] + 1);
	adjust_floating_point(bytes, sizeof(double), false);

	return bigEndianDouble(swap);
}

Uuid Tuple::getUuid(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}
	size_t offset = offsets[index];
	ASSERT_LT(offset, data.size());
	uint8_t code = data[offset];
	if (code != UUID_CODE) {
		throw invalid_tuple_data_type();
	}
	ASSERT_LE(offset + Uuid::SIZE + 1, data.size());
	StringRef uuidData(data.begin() + offset + 1, Uuid::SIZE);
	return Uuid(uuidData);
}

Tuple Tuple::getNested(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}
	size_t offset = offsets[index];
	ASSERT_LT(offset, data.size());
	uint8_t code = data[offset];
	if (code != NESTED_CODE) {
		throw invalid_tuple_data_type();
	}

	size_t next_offset = (index + 1 < offsets.size() ? offsets[index + 1] : data.size());
	ASSERT_LE(next_offset, data.size());
	ASSERT_EQ(data[next_offset - 1], (uint8_t)0x00);
	Standalone<VectorRef<uint8_t>> dest;
	dest.reserve(dest.arena(), next_offset - offset);
	std::vector<size_t> dest_offsets;

	size_t i = offset + 1;
	int depth = 0;
	while (i < next_offset - 1) {
		if (depth == 0)
			dest_offsets.push_back(dest.size());
		uint8_t code = data[i];
		dest.push_back(dest.arena(), code); // Copy over the type code.
		if (code == NULL_CODE) {
			if (depth > 0) {
				if (i + 1 < next_offset - 1 && data[i + 1] == 0xff) {
					// Null with a tuple nested in the nested tuple.
					dest.push_back(dest.arena(), 0xff);
					i += 2;
				} else {
					// Nested terminator.
					depth -= 1;
					i += 1;
				}
			} else {
				// A null object within the nested tuple.
				ASSERT_LT(i + 1, next_offset - 1);
				ASSERT_EQ(data[i + 1], 0xff);
				i += 2;
			}
		} else if (code == BYTES_CODE || code == STRING_CODE) {
			size_t next_i = find_string_terminator(data, i + 1) + 1;
			ASSERT_LE(next_i, next_offset - 1);
			size_t length = next_i - i - 1;
			dest.append(dest.arena(), data.begin() + i + 1, length);
			i = next_i;
		} else if (code >= NEG_INT_START && code <= POS_INT_END) {
			size_t int_size = abs(code - INT_ZERO_CODE);
			ASSERT_LE(i + int_size, next_offset - 1);
			dest.append(dest.arena(), data.begin() + i + 1, int_size);
			i += int_size + 1;
		} else if (code == TRUE_CODE || code == FALSE_CODE) {
			i += 1;
		} else if (code == UUID_CODE) {
			ASSERT_LE(i + 1 + Uuid::SIZE, next_offset - 1);
			dest.append(dest.arena(), data.begin() + i + 1, Uuid::SIZE);
			i += Uuid::SIZE + 1;
		} else if (code == FLOAT_CODE) {
			ASSERT_LE(i + 1 + sizeof(float), next_offset - 1);
			dest.append(dest.arena(), data.begin() + i + 1, sizeof(float));
			i += sizeof(float) + 1;
		} else if (code == DOUBLE_CODE) {
			ASSERT_LE(i + 1 + sizeof(double), next_offset - 1);
			dest.append(dest.arena(), data.begin() + i + 1, sizeof(double));
			i += sizeof(double) + 1;
		} else if (code == NESTED_CODE) {
			i += 1;
			depth += 1;
		} else {
			throw invalid_tuple_data_type();
		}
	}

	// The item may shrink because of escaped nulls that are unespaced.
	return Tuple(dest, dest_offsets);
}

KeyRange Tuple::range(Tuple const& tuple) const {
	VectorRef<uint8_t> begin;
	VectorRef<uint8_t> end;

	KeyRange keyRange;

	begin.reserve(keyRange.arena(), data.size() + tuple.pack().size() + 1);
	begin.append(keyRange.arena(), data.begin(), data.size());
	begin.append(keyRange.arena(), tuple.pack().begin(), tuple.pack().size());
	begin.push_back(keyRange.arena(), uint8_t('\x00'));

	end.reserve(keyRange.arena(), data.size() + tuple.pack().size() + 1);
	end.append(keyRange.arena(), data.begin(), data.size());
	end.append(keyRange.arena(), tuple.pack().begin(), tuple.pack().size());
	end.push_back(keyRange.arena(), uint8_t('\xff'));

	keyRange.KeyRangeRef::operator=(
	    KeyRangeRef(StringRef(begin.begin(), begin.size()), StringRef(end.begin(), end.size())));
	return keyRange;
}

Tuple Tuple::subTuple(size_t start, size_t end) const {
	if (start >= offsets.size() || end <= start) {
		return Tuple();
	}

	size_t endPos = end < offsets.size() ? offsets[end] : data.size();
	return Tuple(StringRef(data.begin() + offsets[start], endPos - offsets[start]));
}

// Comparisons
int compare(Standalone<VectorRef<unsigned char>> const& v1, Standalone<VectorRef<unsigned char>> const& v2) {
	size_t i = 0;
	while (i < v1.size() && i < v2.size()) {
		if (v1[i] < v2[i]) {
			return -1;
		} else if (v1[i] > v2[i]) {
			return 1;
		}
		i += 1;
	}

	if (i < v1.size()) {
		return 1;
	}
	if (i < v2.size()) {
		return -1;
	}
	return 0;
}

bool Tuple::operator==(Tuple const& other) const {
	return compare(data, other.data) == 0;
}
bool Tuple::operator!=(Tuple const& other) const {
	return compare(data, other.data) != 0;
}
bool Tuple::operator<(Tuple const& other) const {
	return compare(data, other.data) < 0;
}
bool Tuple::operator<=(Tuple const& other) const {
	return compare(data, other.data) <= 0;
}
bool Tuple::operator>(Tuple const& other) const {
	return compare(data, other.data) > 0;
}
bool Tuple::operator>=(Tuple const& other) const {
	return compare(data, other.data) >= 0;
}

// UUID implementation
Uuid::Uuid(const StringRef& data) {
	if (data.size() != Uuid::SIZE) {
		throw invalid_uuid_size();
	}
	this->data = data;
}

StringRef Uuid::getData() const {
	return data;
}

bool Uuid::operator==(Uuid const& other) const {
	return data == other.data;
}
bool Uuid::operator!=(Uuid const& other) const {
	return data != other.data;
}
bool Uuid::operator<(Uuid const& other) const {
	return data < other.data;
}
bool Uuid::operator<=(Uuid const& other) const {
	return data <= other.data;
}
bool Uuid::operator>(Uuid const& other) const {
	return data > other.data;
}
bool Uuid::operator>=(Uuid const& other) const {
	return data >= other.data;
}
} // namespace FDB
