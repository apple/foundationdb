/*
 * Net2FileSystem.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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


#include "Net2FileSystem.h"

// Define boost::asio::io_service
#include <algorithm>
#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include <boost/asio.hpp>
#include <boost/bind.hpp>

#define FILESYSTEM_IMPL 1

#include "AsyncFileCached.actor.h"
#include "AsyncFileEIO.actor.h"
#include "AsyncFileWinASIO.actor.h"
#include "AsyncFileKAIO.actor.h"
#include "flow/AsioReactor.h"
#include "flow/Platform.h"
#include "AsyncFileWriteChecker.h"

// Opens a file for asynchronous I/O
Future< Reference<class IAsyncFile> > Net2FileSystem::open( std::string filename, int64_t flags, int64_t mode )
{
#ifdef __linux__
	if (checkFileSystem) {
		dev_t fileDeviceId = getDeviceId(filename);
		if (fileDeviceId != this->fileSystemDeviceId) {
			TraceEvent(SevError, "DeviceIdMismatched").detail("FileSystemDeviceId", this->fileSystemDeviceId).detail("FileDeviceId", fileDeviceId);
			throw io_error();
		}
	}
#endif

	if ( (flags & IAsyncFile::OPEN_EXCLUSIVE) ) ASSERT( flags & IAsyncFile::OPEN_CREATE );
	if (!(flags & IAsyncFile::OPEN_UNCACHED))
		return AsyncFileCached::open(filename, flags, mode);

	Future<Reference<IAsyncFile>> f;
#ifdef __linux__
	if ( (flags & IAsyncFile::OPEN_UNBUFFERED) && !(flags & IAsyncFile::OPEN_NO_AIO) )
		f = AsyncFileKAIO::open(filename, flags, mode, NULL);
	else
#endif
	f = Net2AsyncFile::open(filename, flags, mode, static_cast<boost::asio::io_service*> ((void*) g_network->global(INetwork::enASIOService)));
	if(FLOW_KNOBS->PAGE_WRITE_CHECKSUM_HISTORY > 0)
		f = map(f, [=](Reference<IAsyncFile> r) { return Reference<IAsyncFile>(new AsyncFileWriteChecker(r)); });
	return f;
}

// Deletes the given file.  If mustBeDurable, returns only when the file is guaranteed to be deleted even after a power failure.
Future< Void > Net2FileSystem::deleteFile( std::string filename, bool mustBeDurable )
{
	return Net2AsyncFile::deleteFile(filename, mustBeDurable);
}

void Net2FileSystem::newFileSystem(double ioTimeout, std::string fileSystemPath)
{
	g_network->setGlobal(INetwork::enFileSystem, (flowGlobalType) new Net2FileSystem(ioTimeout, fileSystemPath));
}

Net2FileSystem::Net2FileSystem(double ioTimeout, std::string fileSystemPath)
{
	Net2AsyncFile::init();
#ifdef __linux__
	AsyncFileKAIO::init( Reference<IEventFD>(N2::ASIOReactor::getEventFD()), ioTimeout );

	if (fileSystemPath.empty()) {
		checkFileSystem = false;
	} else {
		checkFileSystem = true;

		try {
			this->fileSystemDeviceId = getDeviceId(fileSystemPath);
			if (fileSystemPath != "/") {
				dev_t fileSystemParentDeviceId = getDeviceId(parentDirectory(fileSystemPath));
				if (this->fileSystemDeviceId == fileSystemParentDeviceId) {
					criticalError(FDB_EXIT_ERROR, "FileSystemError", format("`%s' is not a mount point", fileSystemPath.c_str()).c_str());
				}
			}
		} catch (Error& e) {
			criticalError(FDB_EXIT_ERROR, "FileSystemError", format("Could not get device id from `%s'", fileSystemPath.c_str()).c_str());
		}
	}
#endif
}
