/*
 * Tuple.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/TupleVersionstamp.h"

typedef TupleVersionstamp Versionstamp;

namespace FDB {
struct Uuid {
	static size_t const SIZE;

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
	Tuple& appendVersionstamp(Versionstamp const&);

	Standalone<StringRef> pack() const {
		return Standalone<StringRef>(StringRef(data.begin(), data.size()), data.arena());
	}

	template <typename T>
	Tuple& operator<<(T const& t) {
		return append(t);
	}

	enum ElementType { NULL_TYPE, INT, BYTES, UTF8, BOOL, FLOAT, DOUBLE, UUID, NESTED, VERSIONSTAMP };

	// this is number of elements, not length of data
	size_t size() const { return offsets.size(); }

	ElementType getType(size_t index) const;
	Standalone<StringRef> getString(size_t index) const;
	Versionstamp getVersionstamp(size_t index) const;
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
	static uint8_t const NULL_CODE;
	static uint8_t const BYTES_CODE;
	static uint8_t const STRING_CODE;
	static uint8_t const NESTED_CODE;
	static uint8_t const INT_ZERO_CODE;
	static uint8_t const POS_INT_END;
	static uint8_t const NEG_INT_START;
	static uint8_t const FLOAT_CODE;
	static uint8_t const DOUBLE_CODE;
	static uint8_t const FALSE_CODE;
	static uint8_t const TRUE_CODE;
	static uint8_t const UUID_CODE;
	// Java Tuple layer VERSIONSTAMP has 96 bits(12 bytes).
	// It has additional 2 bytes user code than the internal VERSIONTAMP of size 10 bytes
	static uint8_t const VERSIONSTAMP_96_CODE;

	Tuple(StringRef const& data);
	Tuple(Standalone<VectorRef<uint8_t>> data, std::vector<size_t> offsets);
	Standalone<VectorRef<uint8_t>> data;
	std::vector<size_t> offsets;
};
} // namespace FDB

#endif /* _FDB_TUPLE_H_ */
