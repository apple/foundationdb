/*
 * Tuple.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

static size_t find_string_terminator(const StringRef data, size_t offset) {
	size_t i = offset;
	while (i < data.size() - 1 && !(data[i] == '\x00' && data[i+1] != (uint8_t)'\xff')) {
		i += (data[i] == '\x00' ? 2 : 1);
	}

	return i;
}

Tuple::Tuple(StringRef const& str) {
	data.append(data.arena(), str.begin(), str.size());

	size_t i = 0;
	while(i < data.size()) {
		offsets.push_back(i);

		if(data[i] == '\x01' || data[i] == '\x02') {
			i = find_string_terminator(str, i+1) + 1;
		}
		else if(data[i] >= '\x0c' && data[i] <= '\x1c') {
			i += abs(data[i] - '\x14') + 1;
		}
		else if(data[i] == '\x00') {
			i += 1;
		}
		else {
			throw invalid_tuple_data_type();
		}
	}
}

Tuple Tuple::unpack(StringRef const& str) {
	return Tuple(str);
}

Tuple& Tuple::append(Tuple const& tuple) {
	for(size_t offset : tuple.offsets) {
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
	for(size_t pos = 0; pos < str.size(); ++pos) {
		if(str[pos] == '\x00') {
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

Tuple& Tuple::append( int64_t value ) {
	uint64_t swap = value;
	bool neg = false;

	offsets.push_back( data.size() );

	if ( value < 0 ) {
		value = ~(-value);
		neg = true;
	}

	swap = bigEndian64(value);

	for ( int i = 0; i < 8; i++ ) {
		if ( ((uint8_t*)&swap)[i] != (neg ? 255 : 0) ) {
			data.push_back( data.arena(), (uint8_t)(20 + (8-i) * (neg ? -1 : 1)) );
			data.append( data.arena(), ((const uint8_t *)&swap) + i, 8 - i );
			return *this;
		}
	}

	data.push_back( data.arena(), (uint8_t)'\x14' );
	return *this;
}

Tuple& Tuple::appendNull() {
	offsets.push_back(data.size());
	data.push_back(data.arena(), (uint8_t)'\x00');
	return *this;
}

Tuple::ElementType Tuple::getType(size_t index) const {
	if(index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	uint8_t code = data[offsets[index]];

	if(code == '\x00') {
		return ElementType::NULL_TYPE;
	}
	else if(code == '\x01') {
		return ElementType::BYTES;
	}
	else if(code == '\x02') {
		return ElementType::UTF8;
	}
	else if(code >= '\x0c' && code <= '\x1c') {
		return ElementType::INT;
	}
	else {
		throw invalid_tuple_data_type();
	}
}

Standalone<StringRef> Tuple::getString(size_t index) const {
	if(index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	uint8_t code = data[offsets[index]];
	if(code != '\x01' && code != '\x02') {
		throw invalid_tuple_data_type();
	}

	size_t b = offsets[index] + 1;
	size_t e;
	if (offsets.size() > index + 1) {
		e = offsets[index+1];
	} else {
		e = data.size();
	}

	Standalone<StringRef> result;
	VectorRef<uint8_t> staging;

	for (size_t i = b; i < e; ++i) {
		if(data[i] == '\x00') {
			staging.append(result.arena(), data.begin() + b, i - b);
			++i;
			b = i + 1;

			if(i < e) {
				staging.push_back(result.arena(), '\x00');
			}
		}
	}

	if(b < e) {
		staging.append(result.arena(), data.begin() + b, e - b);
	}

	result.StringRef::operator=(StringRef(staging.begin(), staging.size()));
	return result;
}

int64_t Tuple::getInt(size_t index) const {
	if(index >= offsets.size()) {
		throw invalid_tuple_index();
	}

	int64_t swap;
	bool neg = false;

	ASSERT(offsets[index] < data.size());
	uint8_t code = data[offsets[index]];
	if(code < '\x0c' || code > '\x1c') {
		throw invalid_tuple_data_type();
	}

	int8_t len = code - '\x14';

	if ( len < 0 ) {
		len = -len;
		neg = true;
	}

	memset( &swap, neg ? '\xff' : 0, 8 - len );
	memcpy( ((uint8_t*)&swap) + 8 - len, data.begin() + offsets[index] + 1, len );

	swap = bigEndian64( swap );

	if ( neg ) {
		swap = -(~swap);
	}

	return swap;
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

	keyRange.KeyRangeRef::operator=(KeyRangeRef(StringRef(begin.begin(), begin.size()), StringRef(end.begin(), end.size())));
	return keyRange;
}

Tuple Tuple::subTuple(size_t start, size_t end) const {
	if(start >= offsets.size() || end <= start) {
		return Tuple();
	}

	size_t endPos = end < offsets.size() ? offsets[end] : data.size();
	return Tuple(StringRef(data.begin() + offsets[start], endPos - offsets[start]));
}
