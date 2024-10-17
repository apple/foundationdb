/*
 * Net2FileSystem.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/Net2FileSystem.h"

// Define boost::asio::io_service
#include <algorithm>
#ifndef BOOST_SYSTEM_NO_LIB
#define BOOST_SYSTEM_NO_LIB
#endif
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include <boost/asio.hpp>

#define FILESYSTEM_IMPL 1

#include "fdbrpc/AsyncFileCached.actor.h"
#include "fdbrpc/AsyncFileChaos.h"
#include "fdbrpc/AsyncFileEIO.actor.h"
#include "fdbrpc/AsyncFileEncrypted.h"
#include "fdbrpc/AsyncFileWinASIO.actor.h"
#include "fdbrpc/AsyncFileKAIO.actor.h"
#include "flow/AsioReactor.h"
#include "flow/Platform.h"
#include "fdbrpc/AsyncFileWriteChecker.actor.h"

// Opens a file for asynchronous I/O
Future<Reference<class IAsyncFile>> Net2FileSystem::open(const std::string& filename, int64_t flags, int64_t mode) {
#ifdef __linux__
	if (checkFileSystem) {
		dev_t fileDeviceId = getDeviceId(filename);
		if (fileDeviceId != this->fileSystemDeviceId) {
			TraceEvent(SevError, "DeviceIdMismatched")
			    .detail("FileSystemDeviceId", this->fileSystemDeviceId)
			    .detail("FileDeviceId", fileDeviceId);
			throw io_error();
		}
	}
#endif

	if ((flags & IAsyncFile::OPEN_EXCLUSIVE))
		ASSERT(flags & IAsyncFile::OPEN_CREATE);
	if (!(flags & IAsyncFile::OPEN_UNCACHED))
		return AsyncFileCached::open(filename, flags, mode);

	Future<Reference<IAsyncFile>> f;
#ifdef __linux__
	// In the vast majority of cases, we wish to use Kernel AIO. However, some systems
	// don’t properly support kernel async I/O without O_DIRECT or AIO at all. In such
	// cases, DISABLE_POSIX_KERNEL_AIO knob can be enabled to fallback to EIO instead
	// of Kernel AIO. And EIO_USE_ODIRECT can be used to turn on or off O_DIRECT within
	// EIO.
	if ((flags & IAsyncFile::OPEN_UNBUFFERED) && !(flags & IAsyncFile::OPEN_NO_AIO) &&
	    !FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO)
		f = AsyncFileKAIO::open(filename, flags, mode, nullptr);
	else
#endif
		f = Net2AsyncFile::open(
		    filename,
		    flags,
		    mode,
		    static_cast<boost::asio::io_service*>((void*)g_network->global(INetwork::enASIOService)));
	if (FLOW_KNOBS->PAGE_WRITE_CHECKSUM_HISTORY > 0)
		f = map(f, [=](Reference<IAsyncFile> r) { return Reference<IAsyncFile>(new AsyncFileWriteChecker(r)); });
	if (FLOW_KNOBS->ENABLE_CHAOS_FEATURES)
		f = map(f, [=](Reference<IAsyncFile> r) { return Reference<IAsyncFile>(new AsyncFileChaos(r)); });
	if (flags & IAsyncFile::OPEN_ENCRYPTED)
		f = map(f, [flags](Reference<IAsyncFile> r) {
			auto mode = flags & IAsyncFile::OPEN_READWRITE ? AsyncFileEncrypted::Mode::APPEND_ONLY
			                                               : AsyncFileEncrypted::Mode::READ_ONLY;
			return Reference<IAsyncFile>(new AsyncFileEncrypted(r, mode));
		});
	return f;
}

// Deletes the given file.  If mustBeDurable, returns only when the file is guaranteed to be deleted even after a power
// failure.
Future<Void> Net2FileSystem::deleteFile(const std::filesystem::path& filename, bool mustBeDurable) {
	return Net2AsyncFile::deleteFile(filename, mustBeDurable);
}

Future<std::time_t> Net2FileSystem::lastWriteTime(const std::string& filename) {
	return Net2AsyncFile::lastWriteTime(filename);
}

#ifdef ENABLE_SAMPLING
ActorLineageSet& Net2FileSystem::getActorLineageSet() {
	return actorLineageSet;
}
#endif

void Net2FileSystem::newFileSystem(double ioTimeout, const std::string& fileSystemPath) {
	g_network->setGlobal(INetwork::enFileSystem, (flowGlobalType) new Net2FileSystem(ioTimeout, fileSystemPath));
}

Net2FileSystem::Net2FileSystem(double ioTimeout, const std::string& fileSystemPath) {
	Net2AsyncFile::init();
#ifdef __linux__
	if (!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO)
		AsyncFileKAIO::init(Reference<IEventFD>(N2::ASIOReactor::getEventFD()), ioTimeout);

	if (fileSystemPath.empty()) {
		checkFileSystem = false;
	} else {
		checkFileSystem = true;

		try {
			this->fileSystemDeviceId = getDeviceId(fileSystemPath);
			if (fileSystemPath != "/") {
				dev_t fileSystemParentDeviceId = getDeviceId(parentDirectory(fileSystemPath));
				if (this->fileSystemDeviceId == fileSystemParentDeviceId) {
					criticalError(FDB_EXIT_ERROR,
					              "FileSystemError",
					              format("`%s' is not a mount point", fileSystemPath.c_str()).c_str());
				}
			}
		} catch (Error&) {
			criticalError(FDB_EXIT_ERROR,
			              "FileSystemError",
			              format("Could not get device id from `%s'", fileSystemPath.c_str()).c_str());
		}
	}
#endif
}

Future<Void> Net2FileSystem::renameFile(const std::filesystem::path& from, const std::filesystem::path& to) {
	return Net2AsyncFile::renameFile(from, to);
}

void Net2FileSystem::stop() {
	Net2AsyncFile::stop();
}
