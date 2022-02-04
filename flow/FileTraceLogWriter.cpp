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
#include "flow/Platform.h"
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
//#include <windows.h>
//#undef max
//#undef min
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

FileTraceLogWriter::FileTraceLogWriter() {}

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
		writerParams.onError();
	}
}

void FileTraceLogWriter::write(const std::string& str) {
	if (!initialized) {
		pendingLogs.push_back(str);
	} else {
		write(str.data(), str.size());
	}
}

void FileTraceLogWriter::write(const StringRef& str) {
	if (!initialized) {
		pendingLogs.push_back(str.toString());
	} else {
		write(reinterpret_cast<const char*>(str.begin()), str.size());
	}
}

void FileTraceLogWriter::write(const char* str, size_t len) {
	if (traceFileFD < 0) {
		return;
	}
	auto ptr = str;
	int remaining = len;
	bool needsResolve = false;

	while (remaining) {
		int ret = __write(traceFileFD, ptr, remaining);
		if (ret > 0) {
			lastError(0);
			remaining -= ret;
			ptr += ret;
			if (needsResolve) {
				writerParams.issues->resolveIssue("trace_log_file_write_error");
				needsResolve = false;
			}
		} else {
			writerParams.issues->addIssue("trace_log_file_write_error");
			needsResolve = true;
			fprintf(stderr, "Unexpected error [%d] when flushing trace log.\n", errno);
			lastError(errno);
			threadSleep(0.1);
		}
	}
}

void FileTraceLogWriter::open(TraceLogWriterParams const& params) {
	this->writerParams = params;
	opened = true;

	if (address.present()) {
		initializeFile();
	}
}

void FileTraceLogWriter::setNetworkAddress(NetworkAddress const& address) {
	this->address = address;
	if (opened) {
		initializeFile();
	}
}

void FileTraceLogWriter::initializeFile() {
	initialized = true;

	bool needsResolve = false;

	++index;

	// this allows one process to write 10 billion log files
	// this should be enough - if not we could make the base larger...
	ASSERT(index > 0);
	auto indexWidth = unsigned(::floor(log10f(float(index))));

	// index is at most 2^32 - 1. 2^32 - 1 < 10^10 - therefore
	// log10(index) < 10
	UNSTOPPABLE_ASSERT(indexWidth < 10);

	ASSERT(address.present());

	std::string ip = address.get().ip.toString();
	std::replace(ip.begin(), ip.end(), ':', '_'); // For IPv6, Windows doesn't accept ':' in filenames.

	NetworkAddress defaultAddress;
	std::string defaultIp = defaultAddress.ip.toString();
	std::replace(defaultIp.begin(), defaultIp.end(), ':', '_');

	if (writerParams.identifier.size() > 0) {
		basenameWithProcess =
		    format("%s.%s.%s", writerParams.basename.c_str(), ip.c_str(), writerParams.identifier.c_str());
		noAddressBasename =
		    format("%s.%s.%s", writerParams.basename.c_str(), defaultIp.c_str(), writerParams.identifier.c_str());
	} else {
		basenameWithProcess = format("%s.%s.%d", writerParams.basename.c_str(), ip.c_str(), address.get().port);
		noAddressBasename = format("%s.%s.%d", writerParams.basename.c_str(), defaultIp.c_str(), defaultAddress.port);
	}

	finalname = format("%s.%d.%d.%s%s",
	                   basenameWithProcess.c_str(),
	                   indexWidth,
	                   index,
	                   writerParams.extension.c_str(),
	                   writerParams.tracePartialFileSuffix.c_str());
	while ((traceFileFD = __open(finalname.c_str(), TRACEFILE_FLAGS, TRACEFILE_MODE)) == -1) {
		lastError(errno);
		if (errno == EEXIST) {
			++index;
			indexWidth = unsigned(::floor(log10f(float(index))));

			UNSTOPPABLE_ASSERT(indexWidth < 10);
			finalname = format("%s.%d.%d.%s%s",
			                   basenameWithProcess.c_str(),
			                   indexWidth,
			                   index,
			                   writerParams.extension.c_str(),
			                   writerParams.tracePartialFileSuffix.c_str());
		} else {
			fprintf(stderr,
			        "ERROR: could not create trace log file `%s' (%d: %s)\n",
			        finalname.c_str(),
			        errno,
			        strerror(errno));
			writerParams.issues->addIssue("trace_log_could_not_create_file");
			needsResolve = true;

			int errorNum = errno;
			onMainThreadVoid(
			    [finalname = finalname, errorNum] {
				    TraceEvent(SevWarnAlways, "TraceFileOpenError")
				        .detail("Filename", finalname)
				        .detail("ErrorCode", errorNum)
				        .detail("Error", strerror(errorNum))
				        .trackLatest("TraceFileOpenError");
			    },
			    nullptr);
			threadSleep(FLOW_KNOBS->TRACE_RETRY_OPEN_INTERVAL);
		}
	}
	onMainThreadVoid([] { latestEventCache.clear("TraceFileOpenError"); }, nullptr);
	if (needsResolve) {
		writerParams.issues->resolveIssue("trace_log_could_not_create_file");
	}
	lastError(0);

	for (auto line : pendingLogs) {
		write(line.data(), line.size());
	}
}

void FileTraceLogWriter::close() {
	if (opened && !initialized) {
		// Dump to a file with a 0.0.0.0 address if we don't have one yet
		setNetworkAddress(NetworkAddress());
		ASSERT(initialized);
	}

	if (traceFileFD >= 0) {
		while (__close(traceFileFD))
			threadSleep(0.1);
	}
	traceFileFD = -1;
	if (!writerParams.tracePartialFileSuffix.empty()) {
		renameFile(finalname, finalname.substr(0, finalname.size() - writerParams.tracePartialFileSuffix.size()));
	}
	finalname = "";
}

void FileTraceLogWriter::roll() {
	// FIXME: save a roll entry in the buffer
	if (initialized) {
		close();
		initializeFile();
	}
}

void FileTraceLogWriter::sync() {
	if (initialized) {
		__fsync(traceFileFD);
	}
}

void FileTraceLogWriter::cleanupTraceFiles() {
	// Setting maxLogsSize=0 disables trace file cleanup based on dir size
	if (!g_network->isSimulated() && writerParams.maxLogsSize > 0) {
		try {
			// Rename/finalize any stray files ending in tracePartialFileSuffix for this process.
			if (!writerParams.tracePartialFileSuffix.empty()) {
				for (const auto& f : platform::listFiles(writerParams.directory, writerParams.tracePartialFileSuffix)) {
					if (f.substr(0, basenameWithProcess.size()) == basenameWithProcess) {
						renameFile(f, f.substr(0, f.size() - writerParams.tracePartialFileSuffix.size()));
					}
				}
			}

			std::vector<std::string> existingFiles =
			    platform::listFiles(writerParams.directory, writerParams.extension);
			std::vector<std::string> existingTraceFiles;

			for (auto f = existingFiles.begin(); f != existingFiles.end(); ++f) {
				if (f->substr(0, basenameWithProcess.size()) == basenameWithProcess ||
				    f->substr(0, noAddressBasename.size()) == noAddressBasename) {
					existingTraceFiles.push_back(*f);
				}
			}

			// reverse sort, so we preserve the most recent files and delete the oldest
			std::sort(existingTraceFiles.begin(), existingTraceFiles.end(), std::greater<std::string>());

			uint64_t runningTotal = 0;
			std::vector<std::string>::iterator fileListIterator = existingTraceFiles.begin();

			while (runningTotal < writerParams.maxLogsSize && fileListIterator != existingTraceFiles.end()) {
				runningTotal +=
				    (fileSize(joinPath(writerParams.directory, *fileListIterator)) + FLOW_KNOBS->ZERO_LENGTH_FILE_PAD);
				++fileListIterator;
			}

			while (fileListIterator != existingTraceFiles.end()) {
				deleteFile(joinPath(writerParams.directory, *fileListIterator));
				++fileListIterator;
			}
		} catch (Error&) {
		}
	}
}
