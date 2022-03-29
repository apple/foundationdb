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

#ifndef FDBCLIENT_TUPLE_H
#define FDBCLIENT_TUPLE_H

#pragma once

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"

struct Tuple {
	Tuple() {}

	// Tuple parsing normally does not care of the final value is a numeric type and is incomplete.
	// The exclude_incomplete will exclude such incomplete final numeric tuples from the result.
	// Note that strings can't be incomplete because they are parsed such that the end of the packed
	// byte string is considered the end of the string in lieu of a specific end.
	static Tuple unpack(StringRef const& str, bool exclude_incomplete = false);

	Tuple& append(Tuple const& tuple);
	Tuple& append(StringRef const& str, bool utf8 = false);
	Tuple& append(int64_t);
	// There are some ambiguous append calls in fdbclient, so to make it easier
	// to add append for floats and doubles, name them differently for now.
	Tuple& appendBool(bool);
	Tuple& appendFloat(float);
	Tuple& appendDouble(double);
	Tuple& appendNull();

	StringRef pack() const { return StringRef(data.begin(), data.size()); }

	template <typename T>
	Tuple& operator<<(T const& t) {
		return append(t);
	}

	enum ElementType { NULL_TYPE, INT, BYTES, UTF8, BOOL, FLOAT, DOUBLE };

	// this is number of elements, not length of data
	size_t size() const { return offsets.size(); }

	ElementType getType(size_t index) const;
	Standalone<StringRef> getString(size_t index) const;
	int64_t getInt(size_t index, bool allow_incomplete = false) const;
	bool getBool(size_t index) const;
	float getFloat(size_t index) const;
	double getDouble(size_t index) const;

	KeyRange range(Tuple const& tuple = Tuple()) const;

	Tuple subTuple(size_t beginIndex, size_t endIndex = std::numeric_limits<size_t>::max()) const;

	// Return packed data with the arena it resides in
	Standalone<VectorRef<uint8_t>> getData() { return data; }
	Standalone<StringRef> getDataAsStandalone() { return Standalone<StringRef>(pack(), data.arena()); }

private:
	Tuple(const StringRef& data, bool exclude_incomplete = false);
	Standalone<VectorRef<uint8_t>> data;
	std::vector<size_t> offsets;
};

#endif /* FDBCLIENT_TUPLE_H */
