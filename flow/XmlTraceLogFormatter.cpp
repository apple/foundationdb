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

void XmlTraceLogFormatter::addref() {
	ReferenceCounted<XmlTraceLogFormatter>::addref();
}

void XmlTraceLogFormatter::delref() {
	ReferenceCounted<XmlTraceLogFormatter>::delref();
}

const char* XmlTraceLogFormatter::getExtension() const {
	return "xml";
}

const char* XmlTraceLogFormatter::getHeader() const {
	return "<?xml version=\"1.0\"?>\r\n<Trace>\r\n";
}

const char* XmlTraceLogFormatter::getFooter() const {
	return "</Trace>\r\n";
}

void XmlTraceLogFormatter::escape(std::ostringstream& oss, std::string source) const {
	loop {
		int index = source.find_first_of(std::string({ '&', '"', '<', '>', '\r', '\n', '\0' }));
		if (index == source.npos) {
			break;
		}

		oss << source.substr(0, index);
		if (source[index] == '&') {
			oss << "&amp;";
		} else if (source[index] == '"') {
			oss << "&quot;";
		} else if (source[index] == '<') {
			oss << "&lt;";
		} else if (source[index] == '>') {
			oss << "&gt;";
		} else if (source[index] == '\n' || source[index] == '\r') {
			oss << " ";
		} else if (source[index] == '\0') {
			oss << " ";
			TraceEvent(SevWarnAlways, "StrippedIllegalCharacterFromTraceEvent")
			    .detail("Source", StringRef(source).printable())
			    .detail("Character", StringRef(source.substr(index, 1)).printable());
		} else {
			ASSERT(false);
		}

		source = source.substr(index + 1);
	}

	oss << std::move(source);
}

std::string XmlTraceLogFormatter::formatEvent(const TraceEventFields& fields) const {
	std::ostringstream oss;
	oss << "<Event ";

	for (auto itr : fields) {
		escape(oss, itr.first);
		oss << "=\"";
		escape(oss, itr.second);
		oss << "\" ";
	}

	oss << "/>\r\n";
	return std::move(oss).str();
}
