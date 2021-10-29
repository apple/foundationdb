/*
 * JsonTraceLogFormatter.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/flow.h"
#include "flow/JsonTraceLogFormatter.h"

#include <sstream>

void JsonTraceLogFormatter::addref() {
	ReferenceCounted<JsonTraceLogFormatter>::addref();
}

void JsonTraceLogFormatter::delref() {
	ReferenceCounted<JsonTraceLogFormatter>::delref();
}

const char* JsonTraceLogFormatter::getExtension() const {
	return "json";
}

const char* JsonTraceLogFormatter::getHeader() const {
	return "";
}

const char* JsonTraceLogFormatter::getFooter() const {
	return "";
}

namespace {

void escapeString(std::ostringstream& oss, const std::string& source) {
	for (auto c : source) {
		if (c == '"') {
			oss << "\\\"";
		} else if (c == '\\') {
			oss << "\\\\";
		} else if (c == '\n') {
			oss << "\\n";
		} else if (c == '\r') {
			oss << "\\r";
		} else if (isprint(c)) {
			oss << c;
		} else {
			constexpr char hex[] = "0123456789abcdef";
			int x = int{ static_cast<uint8_t>(c) };
			oss << "\\x" << hex[x / 16] << hex[x % 16];
		}
	}
}

} // namespace

std::string JsonTraceLogFormatter::formatEvent(const TraceEventFields& fields) const {
	std::ostringstream oss;
	oss << "{  ";
	for (auto iter = fields.begin(); iter != fields.end(); ++iter) {
		if (iter != fields.begin()) {
			oss << ", ";
		}
		oss << "\"";
		escapeString(oss, iter->first);
		oss << "\": \"";
		escapeString(oss, iter->second);
		oss << "\"";
	}
	oss << " }\r\n";
	return std::move(oss).str();
}
