/*
 * TraceValue.cpp
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

#include "flow/Arena.h"
#include "flow/TraceValue.h"

std::string TraceBool::toString() const {
	return format("%d", value);
}

std::string TraceCounter::toString() const {
	return format("%g %g %lld", rate, roughness, value);
}

void TraceVector::truncate(int maxFieldLength) {
	this->maxFieldLength = maxFieldLength;
}

void TraceVector::push_back(TraceValue&& tv) {
	values.push_back(std::move(tv));
}

size_t TraceVector::heapSize() const {
	size_t result = 0;
	for (const auto& v : values) {
		result += v.size();
	}
	return result;
}

std::string TraceVector::toString() const {
	std::string result;
	bool first = true;
	for (const auto& v : values) {
		if (first) {
			first = false;
		} else {
			result.push_back(' ');
		}
		result += v.toString();
		if (maxFieldLength >= 0 && result.size() > maxFieldLength) {
			result = result.substr(0, maxFieldLength) + "...";
			return result;
		}
	}
	return result;
}
