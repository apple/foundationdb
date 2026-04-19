/*
 * Net2FileSystem.cpp
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

#include "fdbrpc/AsyncFileCached.h"
#include "fdbrpc/AsyncFileChaos.h"
#include "fdbrpc/AsyncFileEIO.h"
#include "fdbrpc/AsyncFileEncrypted.h"
#include "fdbrpc/AsyncFileWinASIO.h"
#include "fdbrpc/AsyncFileKAIO.h"
#include "flow/AsioReactor.h"
#include "flow/Platform.h"
#include "fdbrpc/AsyncFileWriteChecker.h"
#include "flow/UnitTest.h"

#ifdef __linux__
namespace {
Future<Void> runAsyncFileKAIOTestOps(Reference<IAsyncFile> f, int numIterations, int fileSize, bool expectedToSucceed) {
	void* buf = FastAllocator<4096>::allocate(); // we leak this if there is an error, but that shouldn't be a big deal

	bool opTimedOut = false;

	for (int iteration = 0; iteration < numIterations; ++iteration) {
		std::vector<Future<Void>> futures;
		for (int numOps = deterministicRandom()->randomInt(1, 20); numOps > 0; --numOps) {
			if (deterministicRandom()->coinflip()) {
				futures.push_back(
				    success(f->read(buf, 4096, deterministicRandom()->randomInt(0, fileSize) / 4096 * 4096)));
			} else {
				futures.push_back(f->write(buf, 4096, deterministicRandom()->randomInt(0, fileSize) / 4096 * 4096));
			}
		}
		for (int fIndex = 0; fIndex < futures.size(); ++fIndex) {
			try {
				co_await futures[fIndex];
			} catch (Error& e) {
				ASSERT(!expectedToSucceed);
				ASSERT(e.code() == error_code_io_timeout);
				opTimedOut = true;
			}
		}

		try {
			co_await (f->sync() && delay(0.1));
			ASSERT(expectedToSucceed);
		} catch (Error& e) {
			ASSERT(!expectedToSucceed && e.code() == error_code_io_timeout);
		}
	}

	FastAllocator<4096>::release(buf);

	ASSERT(expectedToSucceed || opTimedOut);
}
} // namespace

TEST_CASE("/fdbrpc/AsyncFileKAIO/RequestList") {
	// This test does nothing in simulation because simulation doesn't support AsyncFileKAIO
	if (!g_network->isSimulated()) {
		Reference<IAsyncFile> f;
		Optional<Error> err;
		try {
			f = co_await AsyncFileKAIO::open("/tmp/__KAIO_TEST_FILE__",
			                                 IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE |
			                                     IAsyncFile::OPEN_CREATE,
			                                 0666,
			                                 nullptr);
			int fileSize = 2 << 27; // ~100MB
			co_await f->truncate(fileSize);

			// Test that the request list works as intended with default timeout
			AsyncFileKAIO::setTimeout(0.0);
			co_await runAsyncFileKAIOTestOps(f, 100, fileSize, true);
			ASSERT(!((AsyncFileKAIO*)f.getPtr())->failed);

			// Test that the request list works as intended with long timeout
			AsyncFileKAIO::setTimeout(20.0);
			co_await runAsyncFileKAIOTestOps(f, 100, fileSize, true);
			ASSERT(!((AsyncFileKAIO*)f.getPtr())->failed);

			// Test that requests timeout correctly
			AsyncFileKAIO::setTimeout(0.0001);
			co_await runAsyncFileKAIOTestOps(f, 10, fileSize, false);
			ASSERT(((AsyncFileKAIO*)f.getPtr())->failed);
		} catch (Error& e) {
			err = e;
		}
		if (err.present()) {
			if (f) {
				co_await AsyncFileEIO::deleteFile(f->getFilename(), true);
			}
			throw err.get();
		}

		co_await AsyncFileEIO::deleteFile(f->getFilename(), true);
	}
}
#endif // __linux__

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
Future<Void> Net2FileSystem::deleteFile(const std::string& filename, bool mustBeDurable) {
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

Future<Void> Net2FileSystem::renameFile(const std::string& from, const std::string& to) {
	return Net2AsyncFile::renameFile(from, to);
}

void Net2FileSystem::stop() {
	Net2AsyncFile::stop();
}
