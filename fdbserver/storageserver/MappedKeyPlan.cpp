/*
 * MappedKeyPlan.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

#include "MappedKeyPlan.h"

#include <exception>
#include <string>

#include "flow/Trace.h"

namespace {

const Tuple& unpackKeyTuple(Optional<Tuple>& keyTuple, const KeyValueRef& keyValue) {
	if (!keyTuple.present()) {
		try {
			keyTuple = Tuple::unpack(keyValue.key);
		} catch (Error& e) {
			TraceEvent("KeyNotTuple").error(e).detail("Key", keyValue.key.printable());
			throw key_not_tuple();
		}
	}
	return keyTuple.get();
}

const Tuple& unpackValueTuple(Optional<Tuple>& valueTuple, const KeyValueRef& keyValue) {
	if (!valueTuple.present()) {
		try {
			valueTuple = Tuple::unpack(keyValue.value);
		} catch (Error& e) {
			TraceEvent("ValueNotTuple").error(e).detail("Value", keyValue.value.printable());
			throw value_not_tuple();
		}
	}
	return valueTuple.get();
}

bool unescapeLiterals(std::string& value, const std::string& before, const std::string& after) {
	bool escaped = false;
	size_t position = 0;
	while (true) {
		size_t found = value.find(before, position);
		if (found == std::string::npos) {
			break;
		}
		value.replace(found, before.length(), after);
		position = found + after.length();
		escaped = true;
	}
	return escaped;
}

bool singleKeyOrValue(const std::string& value, size_t size) {
	return size > 5 && value[0] == '{' && (value[1] == 'K' || value[1] == 'V') && value[2] == '[' &&
	       value[size - 2] == ']' && value[size - 1] == '}';
}

} // namespace

MappedKeyPlan::MappedKeyPlan(StringRef mapper) {
	try {
		mappedKeyFormatTuple = Tuple::unpack(mapper);
	} catch (Error& e) {
		TraceEvent("MapperNotTuple").error(e).detail("Mapper", mapper);
		throw mapper_not_tuple();
	}

	mappedKeyElements.reserve(mappedKeyFormatTuple.size());

	for (size_t i = 0; i < mappedKeyFormatTuple.size(); i++) {
		Tuple::ElementType type = mappedKeyFormatTuple.getType(i);
		if (type == Tuple::BYTES || type == Tuple::UTF8) {
			std::string value = mappedKeyFormatTuple.getString(i).toString();
			auto size = value.size();
			bool escaped = unescapeLiterals(value, "{{", "{");
			escaped = unescapeLiterals(value, "}}", "}") || escaped;
			if (escaped) {
				mappedKeyElements.emplace_back(Tuple::makeTuple(value));
			} else if (singleKeyOrValue(value, size)) {
				mappedKeyElements.emplace_back(Tuple());
			} else if (value == "{...}") {
				if (i != mappedKeyFormatTuple.size() - 1) {
					throw mapper_bad_range_decriptor();
				}
				mappedKeyElements.emplace_back(Optional<Tuple>());
				rangeQuery = true;
			} else {
				Tuple element;
				element.appendRaw(mappedKeyFormatTuple.subTupleRawString(i));
				mappedKeyElements.emplace_back(element);
			}
		} else {
			Tuple element;
			element.appendRaw(mappedKeyFormatTuple.subTupleRawString(i));
			mappedKeyElements.emplace_back(element);
		}
	}
}

Key MappedKeyPlan::constructMappedKey(const KeyValueRef& keyValue) const {
	Optional<Tuple> keyTuple;
	Optional<Tuple> valueTuple;
	Tuple mappedKeyTuple;

	mappedKeyTuple.reserve(mappedKeyElements.size());

	for (size_t i = 0; i < mappedKeyElements.size(); i++) {
		if (!mappedKeyElements[i].present()) {
			continue;
		}
		if (mappedKeyElements[i].get().size()) {
			mappedKeyTuple.append(mappedKeyElements[i].get());
		} else {
			std::string value = mappedKeyFormatTuple.getString(i).toString();
			auto size = value.size();
			int index;
			try {
				index = std::stoi(value.substr(3, size - 5));
			} catch (std::exception& e) {
				throw mapper_bad_index();
			}

			const Tuple* referenceTuple;
			if (value[1] == 'K') {
				referenceTuple = &unpackKeyTuple(keyTuple, keyValue);
			} else if (value[1] == 'V') {
				referenceTuple = &unpackValueTuple(valueTuple, keyValue);
			} else {
				ASSERT(false);
				throw internal_error();
			}
			if (index < 0 || index >= referenceTuple->size()) {
				throw mapper_bad_index();
			}
			mappedKeyTuple.appendRaw(referenceTuple->subTupleRawString(index));
		}
	}

	return mappedKeyTuple.pack();
}
