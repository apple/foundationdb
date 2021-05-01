/*
 * XmlTraceLogFormatter.cpp
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
#include "flow/XmlTraceLogFormatter.h"
#include "flow/actorcompiler.h"

static std::string escape(std::string source) {
	std::string result;
	loop {
		int index = source.find_first_of(std::string({ '&', '"', '<', '>', '\r', '\n', '\0' }));
		if (index == source.npos) {
			break;
		}

		result += source.substr(0, index);
		if (source[index] == '&') {
			result += "&amp;";
		} else if (source[index] == '"') {
			result += "&quot;";
		} else if (source[index] == '<') {
			result += "&lt;";
		} else if (source[index] == '>') {
			result += "&gt;";
		} else if (source[index] == '\n' || source[index] == '\r') {
			result += " ";
		} else if (source[index] == '\0') {
			result += " ";
			TraceEvent(SevWarnAlways, "StrippedIllegalCharacterFromTraceEvent")
			    .detail("Source", StringRef(source).printable())
			    .detail("Character", StringRef(source.substr(index, 1)).printable());
		} else {
			ASSERT(false);
		}

		source = source.substr(index + 1);
	}
	result += source;
	return result;
}

void XmlTraceLogFormatter::addref() {
	ReferenceCounted<XmlTraceLogFormatter>::addref();
}

void XmlTraceLogFormatter::delref() {
	ReferenceCounted<XmlTraceLogFormatter>::delref();
}

const char* XmlTraceLogFormatter::getExtension() {
	return "xml";
}

const char* XmlTraceLogFormatter::getHeader() {
	return "<?xml version=\"1.0\"?>\r\n<Trace>\r\n";
}

const char* XmlTraceLogFormatter::getFooter() {
	return "</Trace>\r\n";
}

std::string XmlTraceLogFormatter::formatEvent(const TraceEventFields& fields) {
	std::string result = "<Event ";
	for (const auto& [key, value] : fields) {
		result += escape(key) + "=\"";
		result += value.toString();
		result += "\" ";
	}
	result += "/>\r\n";
	return result;
}
