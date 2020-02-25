/*
 * FileTraceLogWriter.cpp
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


#include "flow/FileTraceLogWriter.h"
#include "flow/flow.h"
#include "flow/ThreadHelper.actor.h"

#if defined(__unixish__)
#define __open ::open
#define __write ::write
#define __close ::close
#define __fsync ::fsync
#define TRACEFILE_FLAGS O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC
#define TRACEFILE_MODE 0664
#elif defined(_WIN32)
#include <windows.h>
#undef max
#undef min
#include <io.h>
#include <stdio.h>
#include <sys/stat.h>
#define __open _open
#define __write _write
#define __close _close
#define __fsync _commit
#define TRACEFILE_FLAGS _O_WRONLY | _O_CREAT | _O_EXCL
#define TRACEFILE_MODE _S_IWRITE
#endif

#include <fcntl.h>
#include <cmath>

FileTraceLogWriter::FileTraceLogWriter(std::string directory, std::string processName, std::string basename,
                                       std::string extension, uint64_t maxLogsSize, std::function<void()> onError,
                                       Reference<ITraceLogIssuesReporter> issues)
  : directory(directory), processName(processName), basename(basename), extension(extension), maxLogsSize(maxLogsSize),
    traceFileFD(-1), index(0), onError(onError), issues(issues) {}

void FileTraceLogWriter::addref() {
	ReferenceCounted<FileTraceLogWriter>::addref();
}

void FileTraceLogWriter::delref() {
	ReferenceCounted<FileTraceLogWriter>::delref();
}

void FileTraceLogWriter::lastError(int err) {
	// Whenever we get a serious error writing a trace log, all flush barriers posted between the operation encountering
	// the error and the occurrence of the error are unblocked, even though we haven't actually succeeded in flushing.
	// Otherwise a permanent write error would make the program block forever.
	if (err != 0 && err != EINTR) {
		onError();
	}
}

void FileTraceLogWriter::write(const std::string& str) {
	auto ptr = str.c_str();
	int remaining = str.size();
	bool needsResolve = false;

	while ( remaining ) {
		int ret = __write( traceFileFD, ptr, remaining );
		if ( ret > 0 ) {
			lastError(0);
			remaining -= ret;
			ptr += ret;
			if (needsResolve) {
				issues->resolveIssue("trace_log_file_write_error");
				needsResolve = false;
			}
		} else {
			issues->addIssue("trace_log_file_write_error");
			needsResolve = true;
			fprintf(stderr, "Unexpected error [%d] when flushing trace log.\n", errno);
			lastError(errno);
			threadSleep(0.1);
		}
	}
}

void FileTraceLogWriter::open() {
	cleanupTraceFiles();
	bool needsResolve = false;

	++index;

	// this allows one process to write 10 billion log files
	// this should be enough - if not we could make the base larger...
	ASSERT(index > 0);
	auto indexWidth = unsigned(::floor(log10f(float(index))));

	// index is at most 2^32 - 1. 2^32 - 1 < 10^10 - therefore
	// log10(index) < 10
	UNSTOPPABLE_ASSERT(indexWidth < 10);

	auto finalname = format("%s.%d.%d.%s", basename.c_str(), indexWidth, index, extension.c_str());
	while ( (traceFileFD = __open( finalname.c_str(), TRACEFILE_FLAGS, TRACEFILE_MODE )) == -1 ) {
		lastError(errno);
		if (errno == EEXIST) {
			++index;
			indexWidth = unsigned(::floor(log10f(float(index))));

			UNSTOPPABLE_ASSERT(indexWidth < 10);
			finalname = format("%s.%d.%d.%s", basename.c_str(), indexWidth, index, extension.c_str());
		}
		else {
			fprintf(stderr, "ERROR: could not create trace log file `%s' (%d: %s)\n", finalname.c_str(), errno, strerror(errno));
			issues->addIssue("trace_log_could_not_create_file");
			needsResolve = true;

			int errorNum = errno;
			onMainThreadVoid([finalname, errorNum]{
				TraceEvent(SevWarnAlways, "TraceFileOpenError")
					.detail("Filename", finalname)
					.detail("ErrorCode", errorNum)
					.detail("Error", strerror(errorNum))
					.trackLatest("TraceFileOpenError"); }, NULL);
			threadSleep(FLOW_KNOBS->TRACE_RETRY_OPEN_INTERVAL);
		}
	}
	onMainThreadVoid([]{ latestEventCache.clear("TraceFileOpenError"); }, NULL);
	if (needsResolve) {
		issues->resolveIssue("trace_log_could_not_create_file");
	}
	lastError(0);
}

void FileTraceLogWriter::close() {
	if (traceFileFD >= 0) {
		while ( __close(traceFileFD) ) threadSleep(0.1);
	}
}

void FileTraceLogWriter::roll() {
	close();
	open();
}

void FileTraceLogWriter::sync() {
	__fsync(traceFileFD);
}

void FileTraceLogWriter::cleanupTraceFiles() {
	// Setting maxLogsSize=0 disables trace file cleanup based on dir size
	if(!g_network->isSimulated() && maxLogsSize > 0) {
		try {
			std::vector<std::string> existingFiles = platform::listFiles(directory, extension);
			std::vector<std::string> existingTraceFiles;

			for(auto f = existingFiles.begin(); f != existingFiles.end(); ++f) {
				if(f->substr(0, processName.length()) == processName) {
					existingTraceFiles.push_back(*f);
				}
			}

			// reverse sort, so we preserve the most recent files and delete the oldest
			std::sort(existingTraceFiles.begin(), existingTraceFiles.end(), std::greater<std::string>());

			uint64_t runningTotal = 0;
			std::vector<std::string>::iterator fileListIterator = existingTraceFiles.begin();

			while(runningTotal < maxLogsSize && fileListIterator != existingTraceFiles.end()) {
				runningTotal += (fileSize(joinPath(directory, *fileListIterator)) + FLOW_KNOBS->ZERO_LENGTH_FILE_PAD);
				++fileListIterator;
			}

			while(fileListIterator != existingTraceFiles.end()) {
				deleteFile(joinPath(directory, *fileListIterator));
				++fileListIterator;
			}
		} catch( Error & ) {}
	}
}
