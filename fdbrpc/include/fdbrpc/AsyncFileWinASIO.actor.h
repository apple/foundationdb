/*
 * AsyncFileWinASIO.actor.h
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

#pragma once

#ifdef WIN32

#define Net2AsyncFile AsyncFileWinASIO

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILEWINASIO_ACTOR_G_H)
#define FLOW_ASYNCFILEWINASIO_ACTOR_G_H
#include "fdbrpc/AsyncFileWinASIO.actor.g.h"
#elif !defined(FLOW_ASYNCFILEWINASIO_ACTOR_H)
#define FLOW_ASYNCFILEWINASIO_ACTOR_H

#include <Windows.h>
#include <boost/bind/bind.hpp>
#undef min
#undef max

class AsyncFileWinASIO final : public IAsyncFile, public ReferenceCounted<AsyncFileWinASIO> {
public:
	static void init() {}

	static void stop() {}

	virtual StringRef getClassName() override { return "AsnycFileWinASIO"_sr; }

	static bool should_poll() { return false; }
	// FIXME: This implementation isn't actually asynchronous - it just does operations synchronously!

	static Future<Reference<IAsyncFile>> open(std::string filename, int flags, int mode, boost::asio::io_service* ios) {
		ASSERT(flags & OPEN_UNBUFFERED);

		std::string open_filename = filename;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			ASSERT((flags & OPEN_CREATE) && (flags & OPEN_READWRITE) && !(flags & OPEN_EXCLUSIVE));
			open_filename = filename + ".part";
		}

		HANDLE h = CreateFile(open_filename.c_str(),
		                      GENERIC_READ | ((flags & OPEN_READWRITE) ? GENERIC_WRITE : 0),
		                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		                      nullptr,
		                      (flags & OPEN_EXCLUSIVE) ? CREATE_NEW
		                      : (flags & OPEN_CREATE)  ? OPEN_ALWAYS
		                                               : OPEN_EXISTING,
		                      FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,
		                      nullptr);
		if (h == INVALID_HANDLE_VALUE) {
			bool notFound = GetLastError() == ERROR_FILE_NOT_FOUND;
			Error e = notFound ? file_not_found() : io_error();
			TraceEvent(notFound ? SevWarn : SevWarnAlways, "FileOpenError")
			    .error(e)
			    .GetLastError()
			    .detail("File", filename)
			    .detail("Flags", flags)
			    .detail("Mode", mode);
			return e;
		}
		return Reference<IAsyncFile>(new AsyncFileWinASIO(*ios, h, flags, filename));
	}
	static Future<Void> deleteFile(std::string filename, bool mustBeDurable) {
		::deleteFile(filename);
		// SOMEDAY: What is necessary to implement mustBeDurable on Windows?  Does DeleteFile take care of it?
		// DeleteFileTransacted?
		return Void();
	}
	static Future<std::time_t> lastWriteTime(std::string filename) {
		// TODO(alexmiller): I have no idea about windows
		struct _stat buf;
		if (_stat(filename.c_str(), &buf) != 0) {
			throw io_error();
		}
		return buf.st_mtime;
	}

	void addref() override { ReferenceCounted<AsyncFileWinASIO>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileWinASIO>::delref(); }

	int64_t debugFD() const override { return (int64_t)(const_cast<decltype(file)&>(file).native_handle()); }

	static void onReadReady(Promise<int> onReady, const boost::system::error_code& error, size_t bytesRead) {
		if (error) {
			Error e = io_error();
			TraceEvent("AsyncReadError")
			    .error(e)
			    .GetLastError()
			    .detail("ASIOCode", error.value())
			    .detail("ASIOMessage", error.message());
			onReady.sendError(e);
		} else {
			onReady.send(bytesRead);
		}
	}
	static void onWriteReady(Promise<Void> onReady,
	                         size_t bytesExpected,
	                         const boost::system::error_code& error,
	                         size_t bytesWritten) {
		if (error) {
			Error e = io_error();
			TraceEvent("AsyncWriteError")
			    .error(e)
			    .GetLastError()
			    .detail("ASIOCode", error.value())
			    .detail("ASIOMessage", error.message());
			onReady.sendError(e);
		} else if (bytesWritten != bytesExpected) {
			Error e = io_error();
			TraceEvent("AsyncWriteError").detail("BytesExpected", bytesExpected).detail("BytesWritten", bytesWritten);
			onReady.sendError(io_error());
		} else {
			onReady.send(Void());
		}
	}

	Future<int> read(void* data, int length, int64_t offset) override {
		// the size call is set inline
		auto end = this->size().get();
		//TraceEvent("WinAsyncRead").detail("Offset", offset).detail("Length", length).detail("FileSize", end).detail("FileName", filename);
		if (offset >= end)
			return 0;

		Promise<int> result;
		file.async_read_some_at(
		    offset,
		    boost::asio::mutable_buffers_1(data, length),
		    boost::bind(
		        &onReadReady, result, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));

		return result.getFuture();
	}

	static Future<Void> renameFile(std::filesystem::path const& from, std::filesystem::path const& to) {
		::renameFile(from, to);
		return Void();
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		/*
		FIXME
		if ( length + offset >= fileValidData ) {
		    SetFileValidData( length+offset );
		    fileValidData = length+offset;
		}*/
		Promise<Void> result;
		boost::asio::async_write_at(file,
		                            offset,
		                            boost::asio::const_buffers_1(data, length),
		                            boost::bind(&onWriteReady,
		                                        result,
		                                        length,
		                                        boost::asio::placeholders::error,
		                                        boost::asio::placeholders::bytes_transferred));
		return result.getFuture();
	}
	Future<Void> truncate(int64_t size) override {
		// FIXME: Possibly use SetFileInformationByHandle( file.native_handle(), FileEndOfFileInfo, ... ) instead
		if (!SetFilePointerEx(file.native_handle(), *(LARGE_INTEGER*)&size, nullptr, FILE_BEGIN))
			throw io_error();
		if (!SetEndOfFile(file.native_handle()))
			throw io_error();
		return Void();
	}
	Future<Void> sync() override {
		// FIXME: Do FlushFileBuffers in a worker thread (using g_network->createThreadPool)?
		if (!FlushFileBuffers(file.native_handle()))
			throw io_error();

		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;
			// FIXME: MoveFileEx(..., MOVEFILE_WRITE_THROUGH) in thread?
			MoveFile((filename + ".part").c_str(), filename.c_str());
		}

		return Void();
	}
	Future<int64_t> size() const override {
		LARGE_INTEGER s;
		if (!GetFileSizeEx(const_cast<decltype(file)&>(file).native_handle(), &s))
			throw io_error();
		return *(int64_t*)&s;
	}
	std::string getFilename() const override { return filename; }

	~AsyncFileWinASIO() {}

private:
	boost::asio::windows::random_access_handle file;
	int flags;
	std::string filename;

	AsyncFileWinASIO(boost::asio::io_service& ios, HANDLE h, int flags, std::string filename)
	  : file(ios, h), flags(flags), filename(filename) {}
};

#endif
#endif
