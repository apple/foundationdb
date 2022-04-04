/*
 * Net2FileSystem.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_NET2FILESYSTEM_H
#define FLOW_NET2FILESYSTEM_H
#include <string>
#pragma once

#include "fdbrpc/IAsyncFile.h"

class Net2FileSystem final : public IAsyncFileSystem {
public:
	// Opens a file for asynchronous I/O
	Future<Reference<class IAsyncFile>> open(const std::string& filename, int64_t flags, int64_t mode) override;

	// Deletes the given file. If mustBeDurable, returns only when the file is guaranteed to be deleted even after a
	// power failure.
	Future<Void> deleteFile(const std::string& filename, bool mustBeDurable) override;

	// Returns the time of the last modification of the file.
	Future<std::time_t> lastWriteTime(const std::string& filename) override;

	Future<Void> renameFile(std::string const& from, std::string const& to) override;

#ifdef ENABLE_SAMPLING
	ActorLineageSet& getActorLineageSet() override;
#endif

	// void init();
	static void stop();

	Net2FileSystem(double ioTimeout = 0.0, const std::string& fileSystemPath = "");

	~Net2FileSystem() override {}

	static void newFileSystem(double ioTimeout = 0.0, const std::string& fileSystemPath = "");

#ifdef __linux__
	dev_t fileSystemDeviceId;
	bool checkFileSystem;
#endif
#ifdef ENABLE_SAMPLING
	ActorLineageSet actorLineageSet;
#endif
};

#endif
