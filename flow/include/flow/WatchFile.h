/*
 * WatchFile.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include <ctime>
#include <string>
#include "flow/IAsyncFile.h"
#include "flow/genericactors.actor.h"

static Future<Void> watchFileForChanges(std::string filename,
                                        AsyncTrigger* fileChanged,
                                        const int* intervalSeconds,
                                        const char* errorType) {
	if (filename.empty()) {
		co_await Future<Void>(Never());
	}
	bool firstRun = true;
	bool statError = false;
	std::time_t lastModTime = 0;
	while (true) {
		try {
			std::time_t modtime = co_await IAsyncFileSystem::filesystem()->lastWriteTime(filename);
			if (firstRun) {
				lastModTime = modtime;
				firstRun = false;
			}
			if (lastModTime != modtime || statError) {
				lastModTime = modtime;
				statError = false;
				fileChanged->trigger();
			}
		} catch (Error& e) {
			if (e.code() == error_code_io_error) {
				// EACCES, ELOOP, ENOENT all come out as io_error(), but are more of a system
				// configuration issue than an FDB problem.  If we managed to load valid
				// certificates, then there's no point in crashing, but we should complain
				// loudly.  IAsyncFile will log the error, but not necessarily as a warning.
				TraceEvent(SevWarnAlways, errorType).detail("File", filename);
				statError = true;
			} else {
				throw;
			}
		}
		co_await delay(*intervalSeconds);
	}
}
