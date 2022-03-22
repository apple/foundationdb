/*
 * FDBTypes.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"

KeyRef keyBetween(const KeyRangeRef& keys) {
	int pos = 0; // will be the position of the first difference between keys.begin and keys.end
	int minSize = std::min(keys.begin.size(), keys.end.size());
	for (; pos < minSize && pos < CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT; pos++) {
		if (keys.begin[pos] != keys.end[pos]) {
			return keys.end.substr(0, pos + 1);
		}
	}

	// If one more character keeps us in the limit, and the latter key is simply
	// longer, then we only need one more byte of the end string.
	if (pos < CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT && keys.begin.size() < keys.end.size()) {
		return keys.end.substr(0, pos + 1);
	}

	return keys.end;
}

void KeySelectorRef::setKey(KeyRef const& key) {
	// There are no keys in the database with size greater than KEY_SIZE_LIMIT, so if this key selector has a key
	// which is large, then we can translate it to an equivalent key selector with a smaller key
	if (key.size() >
	    (key.startsWith(LiteralStringRef("\xff")) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		this->key = key.substr(0,
		                       (key.startsWith(LiteralStringRef("\xff")) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT
		                                                                 : CLIENT_KNOBS->KEY_SIZE_LIMIT) +
		                           1);
	else
		this->key = key;
}

void KeySelectorRef::setKeyUnlimited(KeyRef const& key) {
	this->key = key;
}

std::string KeySelectorRef::toString() const {
	if (offset > 0) {
		if (orEqual)
			return format("%d+firstGreaterThan(%s)", offset - 1, printable(key).c_str());
		else
			return format("%d+firstGreaterOrEqual(%s)", offset - 1, printable(key).c_str());
	} else {
		if (orEqual)
			return format("%d+lastLessOrEqual(%s)", offset, printable(key).c_str());
		else
			return format("%d+lastLessThan(%s)", offset, printable(key).c_str());
	}
}

std::string describe(const std::string& s) {
	return s;
}

std::string describe(UID const& item) {
	return item.shortString();
}
