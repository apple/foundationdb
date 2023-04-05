/*
 * JSONDoc.h
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

#pragma once

#include "fdbclient/json_spirit/json_spirit_writer_template.h"
#include "fdbclient/json_spirit/json_spirit_reader_template.h"
#include "flow/Error.h"

// JSONDoc is a convenient reader/writer class for manipulating JSON documents using "paths".
// Access is done using a "path", which is a string of dot-separated
// substrings representing representing successively deeper keys found in nested
// JSON objects within the top level object
//
// Most methods are read-only with respect to the source JSON object.
// The only modifying methods are create(), put(), subDoc(), and mergeInto()
//
// JSONDoc maintains some state which is the JSON value that was found during the most recent
// *successful* path lookup.
//
// Examples:
//    JSONDoc r(some_obj);
//
//    // See if JSON doc path a.b.c exists
//    bool exists = r.has("a.b.c");
//
//    // See if JSON doc path a.b.c exists, if it does then assign value to x.  Throws if path exists but T is not
//    compatible. T x; bool exists = r.has("a.b.c", x);
//
//    // This way you can chain things like this:
//    bool is_two = r.has("a.b.c", x) && x == 2;
//
//    // Alternatively, you can avoid the temp var by making use of the last() method which returns a reference
//    // to the JSON value at the last successfully found path that has() has seen.
//    bool is_int = r.has("a.b.c") && r.last().type == json_spirit::int_type;
//    bool is_two = r.has("a.b.c") && r.last().get_int() == 2;
//
//    // The familiar at() method also exists but now supports the same path concept.
//    // It will throw in the same circumstances as the original method
//    int x = r.at("a.b.c").get_int();
//
//    // If you wish to access an element with the dot character within its name (e.g., "hostname.example.com"),
//    // you can do so by setting the "split" flag to false in either the "has" or "get" methods. The example
//    // below will look for the key "hostname.example.com" as a subkey of the path "a.b.c" (or, more
//    // precisely, it will look to see if r.has("a").has("b").has("c").has("hostname.example.com", false)).
//    bool exists = r.has("a.b.c").has("hostname.example.com", false);
//
//    // And the familiar operator[] interface exists as well, however only as a synonym for at()
//    // because this class is only for reading.  Using operator [] will not auto-create null things.
//    // The following would throw if a.b.c did not exist, or if it was not an int.
//    int x = r["a.b.c"].get_int();
struct JSONDoc {
	JSONDoc() : pObj(nullptr) {}

	// Construction from const json_spirit::mObject, trivial and will never throw.
	// Resulting JSONDoc will not allow modifications.
	JSONDoc(const json_spirit::mObject& o) : pObj(&o), wpObj(nullptr) {}

	// Construction from json_spirit::mObject.  Allows modifications.
	JSONDoc(json_spirit::mObject& o) : pObj(&o), wpObj(&o) {}

	// Construction from const json_spirit::mValue (which is a Variant type) which will try to
	// convert it to an mObject.  This will throw if that fails, just as it would
	// if the caller called get_obj() itself and used the previous constructor instead.
	JSONDoc(const json_spirit::mValue& v) : pObj(&v.get_obj()), wpObj(nullptr) {}

	// Construction from non-const json_spirit::mValue - will convert the mValue to
	// an object if it isn't already and then attach to it.
	JSONDoc(json_spirit::mValue& v) {
		if (v.type() != json_spirit::obj_type)
			v = json_spirit::mObject();
		wpObj = &v.get_obj();
		pObj = wpObj;
	}

	// Returns whether or not a "path" exists.
	// Returns true if all elements along path exist
	// Returns false if any elements along the path are MISSING
	// Will throw if a non-terminating path element exists BUT is not a JSON Object.
	// If the "split" flag is set to "false", then this skips the splitting of a
	// path into on the "dot" character.
	// When a path is found, pLast is updated.
	bool has(std::string path, bool split = true) {
		if (pObj == nullptr)
			return false;

		if (path.empty())
			return false;
		size_t start = 0;
		const json_spirit::mValue* curVal = nullptr;
		while (start < path.size()) {
			// If a path segment is found then curVal must be an object
			size_t dot;
			if (split) {
				dot = path.find_first_of('.', start);
				if (dot == std::string::npos)
					dot = path.size();
			} else {
				dot = path.size();
			}
			std::string key = path.substr(start, dot - start);

			// Get pointer to the current Object that the key has to be in
			// This will throw if the value is not an Object
			const json_spirit::mObject* curObj = curVal ? &curVal->get_obj() : pObj;

			// Make sure key exists, if not then return false
			if (!curObj->count(key))
				return false;

			// Advance curVal
			curVal = &curObj->at(key);

			// Advance start position in path
			start = dot + 1;
		}

		pLast = curVal;
		return true;
	}

	// Creates the given path (forcing Objects to exist along its depth, replacing whatever else might have been there)
	// and returns a reference to the Value at that location.
	json_spirit::mValue& create(std::string path, bool split = true) {
		if (wpObj == nullptr || path.empty())
			throw std::runtime_error("JSON Object not writable or bad JSON path");

		size_t start = 0;
		json_spirit::mValue* curVal = nullptr;
		while (start < path.size()) {
			// Get next path segment name
			size_t dot;
			if (split) {
				dot = path.find_first_of('.', start);
				if (dot == std::string::npos)
					dot = path.size();
			} else {
				dot = path.size();
			}
			std::string key = path.substr(start, dot - start);
			if (key.empty())
				throw std::runtime_error("invalid JSON path");

			// Get/create pointer to the current Object that the key has to be in
			// If curVal is defined then force it to be an Object
			json_spirit::mObject* curObj;
			if (curVal != nullptr) {
				if (curVal->type() != json_spirit::obj_type)
					*curVal = json_spirit::mObject();
				curObj = &curVal->get_obj();
			} else // Otherwise start with the object *this is writing to
				curObj = wpObj;

			// Make sure key exists, if not then return false
			if (!curObj->count(key))
				(*curObj)[key] = json_spirit::mValue();

			// Advance curVal
			curVal = &((*curObj)[key]);

			// Advance start position in path
			start = dot + 1;
		}

		return *curVal;
	}

	// Creates the path given, puts a value at it, and returns a reference to the value
	template <typename T>
	T& put(std::string path, const T& value, bool split = true) {
		json_spirit::mValue& v = create(path, split);
		v = value;
		return v.get_value<T>();
	}

	// Ensures that an Object exists at path and returns a JSONDoc that writes to it.
	JSONDoc subDoc(std::string path, bool split = true) {
		json_spirit::mValue& v = create(path, split);
		if (v.type() != json_spirit::obj_type)
			v = json_spirit::mObject();
		return JSONDoc(v.get_obj());
	}

	// Apply a merge operation to two values.  Works for int, double, and string
	template <typename T>
	static json_spirit::mObject mergeOperator(const std::string& op,
	                                          const json_spirit::mObject& op_a,
	                                          const json_spirit::mObject& op_b,
	                                          T const& a,
	                                          T const& b) {
		if (op == "$max")
			return { { op, std::max<T>(a, b) } };
		if (op == "$min")
			return { { op, std::min<T>(a, b) } };
		if (op == "$sum")
			return { { op, a + b } };
		throw std::exception();
	}

	// This is just a convenience function to make calling mergeOperator look cleaner
	template <typename T>
	static json_spirit::mObject mergeOperatorWrapper(const std::string& op,
	                                                 const json_spirit::mObject& op_a,
	                                                 const json_spirit::mObject& op_b,
	                                                 const json_spirit::mValue& a,
	                                                 const json_spirit::mValue& b) {
		return mergeOperator<T>(op, op_a, op_b, a.get_value<T>(), b.get_value<T>());
	}

	static inline const std::string& getOperator(const json_spirit::mObject& obj) {
		static const std::string empty;
		for (auto& k : obj)
			if (!k.first.empty() && k.first[0] == '$')
				return k.first;
		return empty;
	}

	// Merge src into dest, applying merge operators
	static void mergeInto(json_spirit::mObject& dst, const json_spirit::mObject& src);
	static void mergeValueInto(json_spirit::mValue& d, const json_spirit::mValue& s);

	// Remove any merge operators that never met any mates.
	static void cleanOps(json_spirit::mObject& obj);
	void cleanOps() {
		if (wpObj == nullptr)
			throw std::runtime_error("JSON Object not writable");

		return cleanOps(*wpObj);
	}

	void absorb(const JSONDoc& doc) {
		if (wpObj == nullptr)
			throw std::runtime_error("JSON Object not writable");

		if (doc.pObj == nullptr)
			throw std::runtime_error("JSON Object not readable");

		mergeInto(*wpObj, *doc.pObj);
	}

	// Returns whether or not a "path" exists.
	// Returns true if all elements along path exist
	// Returns false if any elements along the path are MISSING
	// Sets out to the value of the thing that path refers to
	// Will throw if a non-terminating path element exists BUT is not a JSON Object.
	// Will throw if all elements along path exists but T is an incompatible type
	template <typename T>
	bool get(const std::string path, T& out, bool split = true) {
		bool r = has(path, split);
		if (r)
			out = pLast->get_value<T>();
		return r;
	}

	// For convenience, wraps get() in a try/catch and returns false UNLESS the path existed and was a compatible type.
	template <typename T>
	bool tryGet(const std::string path, T& out, bool split = true) {
		try {
			return get(path, out, split);
		} catch (...) {
		}
		return false;
	}

	const json_spirit::mValue& at(const std::string path, bool split = true) {
		if (has(path, split))
			return last();
		throw std::runtime_error("JSON path doesn't exist");
	}

	const json_spirit::mValue& operator[](const std::string path) { return at(path); }

	const json_spirit::mValue& last() const { return *pLast; }
	bool valid() const { return pObj != nullptr; }

	const json_spirit::mObject& obj() {
		// This dummy object is necessary to make working with obj() easier when this does not currently
		// point to a valid mObject.  valid() can be called to explicitly check for this scenario, but
		// calling obj() at least will not seg fault and instead return a const reference to an empty mObject.
		// This is very useful when iterating using obj() to access the underlying mObject.
		static const json_spirit::mObject dummy;
		return pObj ? *pObj : dummy;
	}

	// Return reference to writeable underlying mObject but only if *this was initialized with a writeable value or
	// object
	json_spirit::mObject& wobj() {
		ASSERT(wpObj != nullptr);
		return *wpObj;
	}

	// This is the version used to represent 'now' for use by the $expires operator.
	// By default, nothing will expire and it is up to the user of JSONDoc to update this value if
	// it is intended to be used.
	// This is slightly hackish but otherwise the JSON merge functions would require a Transaction.
	static uint64_t expires_reference_version;

private:
	const json_spirit::mObject* pObj;
	// Writeable pointer to the same object.  Will be nullptr if initialized from a const object.
	json_spirit::mObject* wpObj;
	const json_spirit::mValue* pLast;
};
