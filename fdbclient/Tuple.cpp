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

#include "fdbclient/Tuple.h"

// TODO: Many functions copied from bindings/flow/Tuple.cpp. Merge at some point.
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

static size_t findStringTerminator(const StringRef data, size_t offset) {
	size_t i = offset;
	while (i < data.size() - 1 && !(data[i] == '\x00' && data[i + 1] != (uint8_t)'\xff')) {
		i += (data[i] == '\x00' ? 2 : 1);
	}

	return i;
}

// If encoding and the sign bit is 1 (the number is negative), flip all the bits.
// If decoding and the sign bit is 0 (the number is negative), flip all the bits.
// Otherwise, the number is positive, so flip the sign bit.
static void adjustFloatingPoint(uint8_t* bytes, size_t size, bool encode) {
	if ((encode && ((uint8_t)(bytes[0] & 0x80) != (uint8_t)0x00)) ||
	    (!encode && ((uint8_t)(bytes[0] & 0x80) != (uint8_t)0x80))) {
		for (size_t i = 0; i < size; i++) {
			bytes[i] ^= (uint8_t)0xff;
		}
	} else {
		bytes[0] ^= (uint8_t)0x80;
	}
}

Tuple::Tuple(StringRef const& str, bool exclude_incomplete) {
	data.append(data.arena(), str.begin(), str.size());

	size_t i = 0;
	while (i < data.size()) {
		offsets.push_back(i);

		if (data[i] == '\x01' || data[i] == '\x02') {
			i = findStringTerminator(str, i + 1) + 1;
		} else if (data[i] >= '\x0c' && data[i] <= '\x1c') {
			i += abs(data[i] - '\x14') + 1;
		} else if (data[i] == 0x20) {
			i += sizeof(float) + 1;
		} else if (data[i] == 0x21) {
			i += sizeof(double) + 1;
		} else if (data[i] == 0x26 || data[i] == 0x27) {
			i += 1;
		} else if (data[i] == '\x00') {
			i += 1;
		} else {
			throw invalid_tuple_data_type();
		}
	}
	// If incomplete tuples are allowed, remove the last offset if i is now beyond size()
	// Strings will never be considered incomplete due to the way the string end is found.
	if (exclude_incomplete && i > data.size())
		offsets.pop_back();
}

Tuple Tuple::unpack(StringRef const& str, bool exclude_incomplete) {
	return Tuple(str, exclude_incomplete);
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

	const uint8_t utfChar = uint8_t(utf8 ? '\x02' : '\x01');
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
			data.push_back(data.arena(), (uint8_t)(20 + (8 - i) * (neg ? -1 : 1)));
			data.append(data.arena(), ((const uint8_t*)&swap) + i, 8 - i);
			return *this;
		}
	}

	data.push_back(data.arena(), (uint8_t)'\x14');
	return *this;
}

Tuple& Tuple::appendBool(bool value) {
	offsets.push_back(data.size());
	if (value) {
		data.push_back(data.arena(), 0x27);
	} else {
		data.push_back(data.arena(), 0x26);
	}
	return *this;
}

Tuple& Tuple::appendFloat(float value) {
	offsets.push_back(data.size());
	float swap = bigEndianFloat(value);
	uint8_t* bytes = (uint8_t*)&swap;
	adjustFloatingPoint(bytes, sizeof(float), true);

	data.push_back(data.arena(), 0x20);
	data.append(data.arena(), bytes, sizeof(float));
	return *this;
}

Tuple& Tuple::appendDouble(double value) {
	offsets.push_back(data.size());
	double swap = value;
	swap = bigEndianDouble(swap);
	uint8_t* bytes = (uint8_t*)&swap;
	adjustFloatingPoint(bytes, sizeof(double), true);

	data.push_back(data.arena(), 0x21);
	data.append(data.arena(), bytes, sizeof(double));
	return *this;
}

Tuple& Tuple::appendNull() {
	offsets.push_back(data.size());
	data.push_back(data.arena(), (uint8_t)'\x00');
	return *this;
}

Tuple::ElementType Tuple::getType(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	uint8_t code = data[offsets[index]];

	if (code == '\x00') {
		return ElementType::NULL_TYPE;
	} else if (code == '\x01') {
		return ElementType::BYTES;
	} else if (code == '\x02') {
		return ElementType::UTF8;
	} else if (code >= '\x0c' && code <= '\x1c') {
		return ElementType::INT;
	} else if (code == 0x20) {
		return ElementType::FLOAT;
	} else if (code == 0x21) {
		return ElementType::DOUBLE;
	} else if (code == 0x26 || code == 0x27) {
		return ElementType::BOOL;
	} else {
		throw invalid_tuple_data_type();
	}
}

Standalone<StringRef> Tuple::getString(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	uint8_t code = data[offsets[index]];
	if (code != '\x01' && code != '\x02') {
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

int64_t Tuple::getInt(size_t index, bool allow_incomplete) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	int64_t swap;
	bool neg = false;

	ASSERT(offsets[index] < data.size());
	uint8_t code = data[offsets[index]];
	if (code < '\x0c' || code > '\x1c') {
		throw invalid_tuple_data_type();
	}

	int8_t len = code - '\x14';

	if (len < 0) {
		len = -len;
		neg = true;
	}

	memset(&swap, neg ? '\xff' : 0, 8 - len);
	// presentLen is how many of len bytes are actually present, it will be < len if the encoded tuple was truncated
	int presentLen = std::min<int8_t>(len, data.size() - offsets[index] - 1);
	ASSERT(len == presentLen || allow_incomplete);
	memcpy(((uint8_t*)&swap) + 8 - len, data.begin() + offsets[index] + 1, presentLen);
	if (presentLen < len) {
		int suffix = len - presentLen;
		if (presentLen == 0) {
			// The first byte in an int would always be at least 1, because if was 0 then a shorter int type would have
			// been used. So if we don't have the first (most significant) byte in the encoded string, use 1 so that the
			// decoded result maintains the encoded form's sort order with an encoded value of a shorter and same-signed
			// type.
			*(((uint8_t*)&swap) + 8 - len) = 1;
			--suffix; // The suffix to clear below is now 1 byte shorter.
		}
		memset(((uint8_t*)&swap) + 8 - suffix, 0, suffix);
	}

	swap = bigEndian64(swap);

	if (neg) {
		swap = -(~swap);
	}

	return swap;
}

// TODO: Combine with bindings/flow/Tuple.*. This code is copied from there.
bool Tuple::getBool(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}
	ASSERT_LT(offsets[index], data.size());
	uint8_t code = data[offsets[index]];
	if (code == 0x26) {
		return false;
	} else if (code == 0x27) {
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
	if (code != 0x20) {
		throw invalid_tuple_data_type();
	}

	float swap;
	uint8_t* bytes = (uint8_t*)&swap;
	ASSERT_LE(offsets[index] + 1 + sizeof(float), data.size());
	swap = *(float*)(data.begin() + offsets[index] + 1);
	adjustFloatingPoint(bytes, sizeof(float), false);

	return bigEndianFloat(swap);
}

double Tuple::getDouble(size_t index) const {
	if (index >= offsets.size()) {
		throw invalid_tuple_index();
	}
	ASSERT_LT(offsets[index], data.size());
	uint8_t code = data[offsets[index]];
	if (code != 0x21) {
		throw invalid_tuple_data_type();
	}

	double swap;
	uint8_t* bytes = (uint8_t*)&swap;
	ASSERT_LE(offsets[index] + 1 + sizeof(double), data.size());
	swap = *(double*)(data.begin() + offsets[index] + 1);
	adjustFloatingPoint(bytes, sizeof(double), false);

	return bigEndianDouble(swap);
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
