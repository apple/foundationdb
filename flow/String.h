/*
 * String.h
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

#ifndef FLOW_STRING_H
#define FLOW_STRING_H

#include <sstream>
#include <string>

namespace {

template <typename T>
class HasToString {
	using YES = char;
	using NO = char[2];

	template <typename C>
	static YES& tester(decltype(&C::toString));
	template <typename C>
	static NO& tester(...);

public:
	enum { value = sizeof(tester<T>(nullptr)) == sizeof(YES) };
};

template <typename Arg>
std::stringstream& _concatHelper(std::stringstream&& ss, const Arg& arg) {
	if constexpr (HasToString<Arg>::value) {
		ss << arg.toString();
		return ss;
	} else {
		ss << arg;
		return ss;
	}
}

template <typename First, typename... Args>
std::stringstream& _concatHelper(std::stringstream&& ss, const First& first, const Args&... args) {
	if constexpr (HasToString<First>::value) {
		ss << first.toString();
		return _concatHelper(std::move(ss), std::forward<const Args&>(args)...);
	} else {
		ss << first;
		return _concatHelper(std::move(ss), std::forward<const Args&>(args)...);
	}
}

} // anonymous namespace

// Concatencate a list of objects to string. If the object has toString() const, then call the toString(); otherwise
// call operator<< to convert it to a string.
template <typename... Args>
std::string concatToString(const Args&... args) {
	return _concatHelper(std::stringstream(), args...).str();
}

#endif // FLOW_STRING_H