/*
 * Tuple.h
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

#ifndef FDBCLIENT_TUPLE_H
#define FDBCLIENT_TUPLE_H

#pragma once

#include "flow/flow.h"
#include "FDBTypes.h"

struct Tuple {
	Tuple() {}

	static Tuple unpack(StringRef const& str);

	Tuple& append(Tuple const& tuple);
	Tuple& append(StringRef const& str, bool utf8=false);
	Tuple& append(int64_t);
	Tuple& appendNull();

	StringRef pack() const { return StringRef(data.begin(), data.size()); }

	template <typename T>
	Tuple& operator<<(T const& t) {
		return append(t);
	}

	enum ElementType { NULL_TYPE, INT, BYTES, UTF8 };

	// this is number of elements, not length of data
	size_t size() const { return offsets.size(); }

	ElementType getType(size_t index) const;
	Standalone<StringRef> getString(size_t index) const;
	int64_t getInt(size_t index) const;

	KeyRange range(Tuple const& tuple = Tuple()) const;

	Tuple subTuple(size_t beginIndex, size_t endIndex = std::numeric_limits<size_t>::max()) const;

private:
	Tuple(const StringRef& data);
	Standalone<VectorRef<uint8_t>> data;
	std::vector<size_t> offsets;
};

#endif /* FDBCLIENT_TUPLE_H */
