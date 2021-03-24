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

namespace {

std::string escapeString(const std::string& source) {
	std::string result;
	for (auto c : source) {
		if (c == '"') {
			result += "\\\"";
		} else if (c == '\\') {
			result += "\\\\";
		} else if (c == '\n') {
			result += "\\n";
		} else if (c == '\r') {
			result += "\\r";
		} else if (isprint(c)) {
			result += c;
		} else {
			constexpr char hex[] = "0123456789abcdef";
			int x = int{ static_cast<uint8_t>(c) };
			result += "\\x" + hex[x / 16] + hex[x % 16];
		}
	}
	return result;
}

class FormatValue {
public:
	std::string operator()(TraceBool const& v) const { return v.value ? "true" : "false"; }
	std::string operator()(TraceString const& v) const { return "\"" + escapeString(v.value) + "\""; }
	std::string operator()(TraceCounter const& v) const { return format("[%g,%g,%lld]", v.rate, v.roughness, v.value); }
	std::string operator()(TraceNumeric const& v) const { return v.value; }
	std::string operator()(TraceVector const& v) const {
		std::string result = "[";
		bool first = true;
		for (const auto& tv : v.values) {
			if (first) {
				first = false;
			} else {
				result.push_back(',');
			}
			result.push_back('\"');
			result += tv;
			result.push_back('\"');
		}
		result.push_back(']');
		return result;
	}
} jsonValueFormatter;

} //namespace

void JsonTraceLogFormatter::addref() {
	ReferenceCounted<JsonTraceLogFormatter>::addref();
}

void JsonTraceLogFormatter::delref() {
	ReferenceCounted<JsonTraceLogFormatter>::delref();
}

const char* JsonTraceLogFormatter::getExtension() {
	return "json";
}

const char* JsonTraceLogFormatter::getHeader() {
	return "";
}

const char* JsonTraceLogFormatter::getFooter() {
	return "";
}

std::string JsonTraceLogFormatter::formatEvent(const TraceEventFields& fields) {
	std::string result = "{  ";
	for (auto iter = fields.begin(); iter != fields.end(); ++iter) {
		if (iter != fields.begin()) {
			result += ", ";
		}
		const auto& [key, value] = *iter;
		result += "\"" + escapeString(key) + "\": ";
		result += value.format(jsonValueFormatter);
	}
	result += " }\r\n";
	return result;
}
