/*
 * FileTraceLogWriter.h
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


#ifndef FLOW_FILE_TRACE_LOG_WRITER_H
#define FLOW_FILE_TRACE_LOG_WRITER_H
#pragma once

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"

#include <functional>

struct IssuesListImpl;
struct IssuesList : ITraceLogIssuesReporter, ThreadSafeReferenceCounted<IssuesList> {
	IssuesList();
	virtual ~IssuesList();
	void addIssue(std::string issue) override;

	void retrieveIssues(std::set<std::string>& out) override;

	void resolveIssue(std::string issue) override;

	void addref() { ThreadSafeReferenceCounted<IssuesList>::addref(); }
	void delref() { ThreadSafeReferenceCounted<IssuesList>::delref(); }

private:
	IssuesListImpl* impl;
};

class FileTraceLogWriter : public ITraceLogWriter, ReferenceCounted<FileTraceLogWriter> {
private:
	std::string directory;
	std::string processName;
	std::string basename;
	std::string extension;

	uint64_t maxLogsSize;
	int traceFileFD;
	uint32_t index;
	Reference<ITraceLogIssuesReporter> issues;

	std::function<void()> onError;

	void write(const char* str, size_t size);

public:
	FileTraceLogWriter(std::string directory, std::string processName, std::string basename, std::string extension,
	                   uint64_t maxLogsSize, std::function<void()> onError, Reference<ITraceLogIssuesReporter> issues);

	void addref();
	void delref();

	void lastError(int err);

	void write(const std::string& str) override;
	void write(StringRef const& str) override;
	void open() override;
	void close() override;
	void roll() override;
	void sync() override;

	void cleanupTraceFiles();
};

#endif
