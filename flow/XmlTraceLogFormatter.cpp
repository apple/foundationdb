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
#include "flow/ScopeExit.h"
#include "flow/UnitTest.h"
#include "XmlTraceLogFormatter.h"

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

static Severity xmlIllegalCharSeverity = SevWarnAlways;

void XmlTraceLogFormatter::escape(std::ostringstream& oss, std::string source) const {
	for (;;) {
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
			TraceEvent(xmlIllegalCharSeverity, "StrippedIllegalCharacterFromTraceEvent")
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

	for (const auto& itr : fields) {
		escape(oss, itr.first);
		oss << "=\"";
		escape(oss, itr.second);
		oss << "\" ";
	}

	oss << "/>\n";
	return std::move(oss).str();
}

// Test case to ensure that TraceEvent details with XML junk get escaped.
//
// This doesn't attempt to ensure that whatever we log is valid XML.
// We might be logging a response from a remote server which may or
// may not be 100% well formed.  In this situation we want to know
// exactly what it is telling us, so we escape meta characters but log
// the string regardless of its parseability.
TEST_CASE("/flow/XmlTraceEscape") {
	std::string garbage_input = "<root attr=\"value&weird\">\n"
	                            "  <child>Some <b>bold</b> & \"quoted\" text</child>\n"
	                            "  <child2><![CDATA[<not>really</not> xml]]></child2>\n"
	                            "  <empty/>\n"
	                            "  <<<mismatched>>>>>>><<\n"
	                            "  <weird attr='><&\"'><inner>><<&&\"\"</inner></weird>\n"
	                            "  <!-- comment with <tags> & stuff -->\n"
	                            "  <deep>\n"
	                            "    <level1>\n"
	                            "      <level2>\n"
	                            "        <level3 attr=\"a&b<c>d\">text & more text</level3>\n"
	                            "      </level2>\n"
	                            "    </level1>\n"
	                            "  </deep>\n"
	                            "</root>"
	                            "more junk\n"
	                            "<<even more>\n";

	auto defaultSeverity = xmlIllegalCharSeverity;
	ScopeExit cleanup = [&]() { xmlIllegalCharSeverity = defaultSeverity; };

	xmlIllegalCharSeverity = SevInfo;

	TraceEvent event("TestEvent");
	event.detail("Garbage", garbage_input);

	XmlTraceLogFormatter formatter;
	std::ostringstream output;
	for (const auto& f : event.getFields()) {
		formatter.escape(output, f.first);
		formatter.escape(output, f.second);
	}

	std::string str = output.str();
	for (const char ch : str) {
		ASSERT(ch != '<' && ch != '>');
	}

	return Void();
}
