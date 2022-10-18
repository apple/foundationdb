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
#include "fdbclient/TupleVersionstamp.h"

struct Tuple {
	struct UnicodeStr {
		StringRef str;
		explicit UnicodeStr(StringRef str) : str(str) {}
	};

	struct UserTypeStr {
		uint8_t code;
		Standalone<StringRef> str;
		UserTypeStr(uint8_t code, StringRef str) : code(code), str(str) {}

		bool operator==(const UserTypeStr& other) const { return (code == other.code && str == other.str); }
	};

	Tuple() {}

	// Tuple parsing normally does not care of the final value is a numeric type and is incomplete.
	// The exclude_incomplete will exclude such incomplete final numeric tuples from the result.
	// Note that strings can't be incomplete because they are parsed such that the end of the packed
	// byte string is considered the end of the string in lieu of a specific end.
	static Tuple unpack(StringRef const& str, bool exclude_incomplete = false);
	static std::string tupleToString(Tuple const& tuple);
	static Tuple unpackUserType(StringRef const& str, bool exclude_incomplete = false);

	Tuple& append(Tuple const& tuple);

	// the str needs to be a Tuple encoded string.
	Tuple& appendRaw(StringRef const& str);
	Tuple& append(StringRef const& str, bool utf8 = false);
	Tuple& append(UnicodeStr const& str);
	Tuple& append(int32_t);
	Tuple& append(int64_t);
	Tuple& append(bool);
	Tuple& append(float);
	Tuple& append(double);
	Tuple& append(std::nullptr_t);
	Tuple& appendNull();
	Tuple& append(TupleVersionstamp const&);
	Tuple& append(UserTypeStr const&);

	Standalone<StringRef> pack() const {
		return Standalone<StringRef>(StringRef(data.begin(), data.size()), data.arena());
	}

	template <typename T>
	Tuple& operator<<(T const& t) {
		return append(t);
	}

	enum ElementType { NULL_TYPE, INT, BYTES, UTF8, BOOL, FLOAT, DOUBLE, VERSIONSTAMP, USER_TYPE };

	bool isUserType(uint8_t code) const;

	// this is number of elements, not length of data
	size_t size() const { return offsets.size(); }
	void reserve(size_t cap) { offsets.reserve(cap); }
	void clear() {
		// Make a new Standalone to use different memory so that
		// previously returned objects from pack() are valid.
		data = Standalone<VectorRef<uint8_t>>();
		offsets.clear();
	}
	// Return a Tuple encoded raw string.
	StringRef subTupleRawString(size_t index) const;
	ElementType getType(size_t index) const;
	Standalone<StringRef> getString(size_t index) const;
	TupleVersionstamp getVersionstamp(size_t index) const;
	int64_t getInt(size_t index, bool allow_incomplete = false) const;
	bool getBool(size_t index) const;
	float getFloat(size_t index) const;
	double getDouble(size_t index) const;
	Tuple::UserTypeStr getUserType(size_t index) const;

	KeyRange range(Tuple const& tuple = Tuple()) const;

	Tuple subTuple(size_t beginIndex, size_t endIndex = std::numeric_limits<size_t>::max()) const;

	// Return packed data with the arena it resides in
	Standalone<VectorRef<uint8_t>> getData() { return data; }
	Standalone<StringRef> getDataAsStandalone() { return Standalone<StringRef>(pack(), data.arena()); }

	// Create a tuple from a parameter pack
	template <class... Types>
	static Tuple makeTuple(Types&&... args) {
		Tuple t;

		// Use a fold expression to append each argument using the << operator.
		// https://en.cppreference.com/w/cpp/language/fold
		(t << ... << std::forward<Types>(args));

		return t;
	}

private:
	Tuple(const StringRef& data, bool exclude_incomplete = false, bool exclude_user_type = false);
	Standalone<VectorRef<uint8_t>> data;
	std::vector<size_t> offsets;
};

#endif /* FDBCLIENT_TUPLE_H */
