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

#include "fdbclient/ReadAggregate.h"
#include <string_view>

class CountAggregate: public IReadAggregate {
public:
	CountAggregate() = default;

	virtual int apply(const KeyValueRef& input, int aggr_intermediate) const {
		return ++aggr_intermediate;
	}

};

Reference<IReadAggregate> createReadAggregate(StringRef name, VectorRef<StringRef> predicateArgs) {
	if (name == AGGR_COUNT) {
		return makeReference<CountAggregate>();
	}
	return Reference<IReadAggregate>();
}
