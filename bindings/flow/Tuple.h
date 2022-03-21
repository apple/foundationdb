/*
 * Tuple.h
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

#ifndef FDB_FLOW_TUPLE_H
#define FDB_FLOW_TUPLE_H

#pragma once

#include "bindings/flow/fdb_flow.h"

namespace FDB {
struct Uuid {
	const static size_t SIZE;

	Uuid(StringRef const& data);

	StringRef getData() const;

	// Comparisons
	bool operator==(Uuid const& other) const;
	bool operator!=(Uuid const& other) const;
	bool operator<(Uuid const& other) const;
	bool operator<=(Uuid const& other) const;
	bool operator>(Uuid const& other) const;
	bool operator>=(Uuid const& other) const;

private:
	Standalone<StringRef> data;
};

struct Tuple {
	Tuple() {}

	static Tuple unpack(StringRef const& str);

	Tuple& append(Tuple const& tuple);
	Tuple& append(StringRef const& str, bool utf8 = false);
	Tuple& append(int32_t);
	Tuple& append(int64_t);
	Tuple& append(bool);
	Tuple& append(float);
	Tuple& append(double);
	Tuple& append(Uuid);
	Tuple& appendNested(Tuple const&);
	Tuple& appendNull();

	StringRef pack() const { return StringRef(data.begin(), data.size()); }

	template <typename T>
	Tuple& operator<<(T const& t) {
		return append(t);
	}

	enum ElementType { NULL_TYPE, INT, BYTES, UTF8, BOOL, FLOAT, DOUBLE, UUID, NESTED };

	// this is number of elements, not length of data
	size_t size() const { return offsets.size(); }

	ElementType getType(size_t index) const;
	Standalone<StringRef> getString(size_t index) const;
	int64_t getInt(size_t index) const;
	bool getBool(size_t index) const;
	float getFloat(size_t index) const;
	double getDouble(size_t index) const;
	Uuid getUuid(size_t index) const;
	Tuple getNested(size_t index) const;

	KeyRange range(Tuple const& tuple = Tuple()) const;

	Tuple subTuple(size_t beginIndex, size_t endIndex = std::numeric_limits<size_t>::max()) const;

	// Comparisons
	bool operator==(Tuple const& other) const;
	bool operator!=(Tuple const& other) const;
	bool operator<(Tuple const& other) const;
	bool operator<=(Tuple const& other) const;
	bool operator>(Tuple const& other) const;
	bool operator>=(Tuple const& other) const;

private:
	static const uint8_t NULL_CODE;
	static const uint8_t BYTES_CODE;
	static const uint8_t STRING_CODE;
	static const uint8_t NESTED_CODE;
	static const uint8_t INT_ZERO_CODE;
	static const uint8_t POS_INT_END;
	static const uint8_t NEG_INT_START;
	static const uint8_t FLOAT_CODE;
	static const uint8_t DOUBLE_CODE;
	static const uint8_t FALSE_CODE;
	static const uint8_t TRUE_CODE;
	static const uint8_t UUID_CODE;

	Tuple(const StringRef& data);
	Tuple(Standalone<VectorRef<uint8_t>> data, std::vector<size_t> offsets);
	Standalone<VectorRef<uint8_t>> data;
	std::vector<size_t> offsets;
};
} // namespace FDB

#endif /* _FDB_TUPLE_H_ */
