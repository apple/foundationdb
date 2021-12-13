/*
 * ReadPredicate.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ReadPredicate.h"
#include <string_view>

const KeyRef PRED_FIND_IN_VALUE = "std/findInVal"_sr;

class FindInValuePredicate : public IReadPredicate {
public:
	FindInValuePredicate(StringRef searchStr) : searchStr(searchStr.toStringView()) {}

	virtual Optional<KeyValueRef> apply(Arena& ar, const KeyValueRef& input, bool mustClone) const {
		bool match = (input.value.toStringView().find(searchStr) != std::string_view::npos);
		if (match) {
			if (mustClone) {
				return KeyValueRef(ar, input);
			} else {
				return input;
			}
		} else {
			return Optional<KeyValueRef>();
		}
	}

	const std::string_view searchStr;
};

Reference<IReadPredicate> createReadPredicate(StringRef name, VectorRef<StringRef> predicateArgs) {
	if (name == PRED_FIND_IN_VALUE) {
		if (predicateArgs.size() == 1) {
			return makeReference<FindInValuePredicate>(predicateArgs[0]);
		}
	}
	return Reference<IReadPredicate>();
}
