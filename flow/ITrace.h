/*
 * ITrace.h
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

#pragma once

#include <string>
#include <set>

class StringRef;

struct ITraceLogWriter {
	virtual void open() = 0;
	virtual void roll() = 0;
	virtual void close() = 0;
	virtual void write(const std::string&) = 0;
	virtual void write(const StringRef&) = 0;
	virtual void sync() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

class TraceEventFields;

struct ITraceLogFormatter {
	virtual const char* getExtension() const = 0;
	virtual const char* getHeader() const = 0; // Called when starting a new file
	virtual const char* getFooter() const = 0; // Called when ending a file
	virtual std::string formatEvent(const TraceEventFields&) const = 0; // Called for each event

	virtual void addref() = 0;
	virtual void delref() = 0;
};

struct ITraceLogIssuesReporter {
	virtual ~ITraceLogIssuesReporter();
	virtual void addIssue(std::string const& issue) = 0;
	virtual void resolveIssue(std::string const& issue) = 0;

	virtual void retrieveIssues(std::set<std::string>& out) const = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};
