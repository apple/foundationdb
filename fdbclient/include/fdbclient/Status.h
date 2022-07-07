/*
 * Status.h
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

#ifndef FDBCLIENT_STATUS_H
#define FDBCLIENT_STATUS_H

#include "fdbclient/JSONDoc.h"

// Reads the entire string s as a JSON value
// Throws if no value can be parsed or if s contains data after the first JSON value
// Trailing whitespace in s is allowed
json_spirit::mValue readJSONStrictly(const std::string& s);

struct StatusObject : json_spirit::mObject {
	typedef json_spirit::mObject Map;
	typedef json_spirit::mArray Array;

	StatusObject() {}
	StatusObject(json_spirit::mObject const& o) : json_spirit::mObject(o) {}
};

template <class Ar>
void load(Ar& ar, StatusObject& statusObj) {
	std::string value;
	int32_t length;
	ar >> length;
	value.resize(length);
	ar.serializeBytes(&value[0], (int)value.length());
	json_spirit::mValue mv;
	json_spirit::read_string(value, mv);
	statusObj = StatusObject(mv.get_obj());

	ASSERT(ar.protocolVersion().isValid());
}

template <class Ar>
void save(Ar& ar, StatusObject const& statusObj) {
	std::string value = json_spirit::write_string(json_spirit::mValue(statusObj));
	ar << (int32_t)value.length();
	ar.serializeBytes((void*)&value[0], (int)value.length());
	ASSERT(ar.protocolVersion().isValid());
}

struct StatusArray : json_spirit::mArray {
	StatusArray() {}
	StatusArray(json_spirit::mArray const& o) : json_spirit::mArray(o.begin(), o.end()) {}
};

struct StatusValue : json_spirit::mValue {
	StatusValue() {}
	StatusValue(json_spirit::mValue const& o) : json_spirit::mValue(o) {}
};

inline StatusObject makeMessage(const char* name, const char* description) {
	StatusObject out;
	out["name"] = name;
	out["description"] = description;
	return out;
}

// Typedef to cover older code that was written when this class was only a reader and called StatusObjectReader
typedef JSONDoc StatusObjectReader;

// Template specialization for get<JSONDoc> because is convenient to get() an
// element from an object directly into a JSONDoc to have a handle to that sub-doc.
template <>
inline bool JSONDoc::get<JSONDoc>(const std::string path, StatusObjectReader& out, bool split) {
	bool r = has(path, split);
	if (r)
		out = pLast->get_obj();
	return r;
}

// Takes an object by reference so make usage look clean and avoid the client doing object["messages"] which will create
// the key.
inline bool findMessagesByName(StatusObjectReader object, std::set<std::string> to_find) {

	if (!object.has("messages") || object.last().type() != json_spirit::array_type)
		return false;

	StatusObject::Array const& messages = object.last().get_array();

	// Iterate over message array and look up each name in to_find.
	// Not using StatusObjectReader here because it would throw if an i in messages is NOT an mObject
	for (auto i : messages) {
		// Since we are looking for a positive match, any exceptions thrown when trying to read
		// the object in the messages array will be perceived as not-a-match and therefore ignored.
		try {
			if (to_find.count(i.get_obj().at("name").get_str()))
				return true;
		} catch (std::exception&) {
		}
	}
	return false;
}

#endif
