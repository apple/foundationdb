/*
 * Printable.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

std::string printable(const VectorRef<KeyValueRef>& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++)
		s = s + printable(val[i].key) + format(":%d ", val[i].value.size());
	return s;
}

std::string printable(const KeyValueRef& val) {
	return printable(val.key) + format(":%d ", val.value.size());
}

std::string printable(const VectorRef<StringRef>& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++)
		s = s + printable(val[i]) + " ";
	return s;
}

std::string printable(const StringRef& val) {
	return val.printable();
}

std::string printable(const std::string& str) {
	return StringRef(str).printable();
}

std::string printable(const KeyRangeRef& range) {
	return printable(range.begin) + " - " + printable(range.end);
}

std::string printable(const VectorRef<KeyRangeRef>& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++)
		s = s + printable(val[i]) + " ";
	return s;
}

int unhex(char c) {
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	UNREACHABLE();
}

std::string unprintable(std::string const& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++) {
		char c = val[i];
		if (c == '\\') {
			if (++i == val.size())
				ASSERT(false);
			if (val[i] == '\\') {
				s += '\\';
			} else if (val[i] == 'x') {
				if (i + 2 >= val.size())
					ASSERT(false);
				s += char((unhex(val[i + 1]) << 4) + unhex(val[i + 2]));
				i += 2;
			} else
				ASSERT(false);
		} else
			s += c;
	}
	return s;
}
