/*
 * XmlTraceLogFormatter.h
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

#ifndef FLOW_XML_TRACE_LOG_FORMATTER_H
#define FLOW_XML_TRACE_LOG_FORMATTER_H
#pragma once

#include <sstream>

#include "flow/FastRef.h"
#include "flow/Trace.h"

struct XmlTraceLogFormatter final : public ITraceLogFormatter, ReferenceCounted<XmlTraceLogFormatter> {
	void addref() override;
	void delref() override;

	const char* getExtension() const override;
	const char* getHeader() const override;
	const char* getFooter() const override;

	void escape(std::ostringstream& oss, std::string source) const;
	std::string formatEvent(const TraceEventFields& fields) const override;
};

#endif
