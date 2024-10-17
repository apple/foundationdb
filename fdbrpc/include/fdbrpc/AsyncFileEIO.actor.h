/*
 * AsyncFileEIO.actor.h
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

#if defined(__unixish__)

#define Net2AsyncFile AsyncFileEIO

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILEEIO_ACTOR_G_H)
#define FLOW_ASYNCFILEEIO_ACTOR_G_H
#include "fdbrpc/AsyncFileEIO.actor.g.h"
#elif !defined(FLOW_ASYNCFILEEIO_ACTOR_H)
#define FLOW_ASYNCFILEEIO_ACTOR_H

#include <fcntl.h>
#include <sys/stat.h>

#include "eio.h"
#include "flow/flow.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/IAsyncFile.h"
#include "flow/TDMetric.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class AsyncFileEIO : public IAsyncFile, public ReferenceCounted<AsyncFileEIO> {

public:
	virtual StringRef getClassName() override { return "AsyncFileDetachable"_sr; }

	static void init() {
		eio_set_max_parallel(FLOW_KNOBS->EIO_MAX_PARALLELISM);
		if (eio_init(&eio_want_poll, nullptr)) {
			TraceEvent("EioInitError").detail("ErrorNo", errno);
			throw platform_error();
		}
	}

	static void stop() { eio_set_max_parallel(0); }

	static bool should_poll() { return want_poll; }

	static bool lock_fd(int fd) {
		// Acquire a "write" lock for the entire file
		struct flock lockDesc;
		lockDesc.l_type = F_WRLCK;
		lockDesc.l_whence = SEEK_SET;
		lockDesc.l_start = 0;
		lockDesc.l_len =
		    0; // "Specifying 0 for l_len has the special meaning: lock all bytes starting at the location specified by
		       // l_whence and l_start through to the end of file, no matter how large the file grows."
		lockDesc.l_pid = 0;
		if (fcntl(fd, F_SETLK, &lockDesc) == -1) {
			return false;
		}
		return true;
	}

	ACTOR static Future<Reference<IAsyncFile>> open(std::string filename, int flags, int mode, void* ignore) {
		std::string open_filename = filename;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			ASSERT((flags & OPEN_CREATE) && (flags & OPEN_READWRITE) && !(flags & OPEN_EXCLUSIVE));
			open_filename = filename + ".part";
		}

		state Promise<Void> p;
		state eio_req* r = eio_open(open_filename.c_str(), openFlags(flags), mode, 0, eio_callback, &p);
		try {
			wait(p.getFuture());
		} catch (...) {
			eio_cancel(r);
			throw;
		}
		if (r->result < 0) {
			errno = r->errorno;
			bool notFound = errno == ENOENT;
			Error e = notFound ? file_not_found() : io_error();
			TraceEvent(notFound ? SevWarn : SevWarnAlways, "FileOpenError")
			    .error(e)
			    .GetLastError()
			    .detail("File", filename)
			    .detail("Flags", flags)
			    .detail("Mode", mode);
			throw e;
		}
		TraceEvent("AsyncFileOpened")
		    .suppressFor(1.0)
		    .detail("Filename", filename)
		    .detail("Fd", r->result)
		    .detail("Flags", flags);

		if ((flags & OPEN_LOCK) && !lock_fd(r->result)) {
			TraceEvent(SevWarn, "UnableToLockFile").detail("Filename", filename).GetLastError();
			throw lock_file_failure();
		}

		return Reference<IAsyncFile>(new AsyncFileEIO(r->result, flags, filename));
	}
	static Future<Void> deleteFile(std::filesystem::path const& filename, bool mustBeDurable) {
		::deleteFile(filename);
		if (mustBeDurable) {
			CODE_PROBE(true, "deleteFile and fsync parent dir", probe::decoration::rare);
			return async_fsync_parent(filename);
		} else
			return Void();
	}

	ACTOR static Future<Void> renameFile(std::filesystem::path from, std::filesystem::path to) {
		state TaskPriority taskID = g_network->getCurrentTask();
		state Promise<Void> p;
		state eio_req* r = eio_rename(from.string(), to.string(), 0, eio_callback, &p);
		try {
			wait(p.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}
		try {
			state int result = r->result;
			if (result == -1) {
				TraceEvent(SevError, "FileRenameError").detail("Errno", r->errorno);
				throw internal_error();
			} else {
				wait(delay(0, taskID));
				return Void();
			}
		} catch (Error& e) {
			state Error _e = e;
			wait(delay(0, taskID));
			throw _e;
		}
	}

	ACTOR static Future<std::time_t> lastWriteTime(std::string filename) {
		EIO_STRUCT_STAT statdata = wait(stat_impl(filename));
		return statdata.st_mtime;
	}

	void addref() override { ReferenceCounted<AsyncFileEIO>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileEIO>::delref(); }

	int64_t debugFD() const override { return fd; }

	Future<int> read(void* data, int length, int64_t offset) override {
		++countFileLogicalReads;
		++countLogicalReads;
		return read_impl(fd, data, length, offset);
	}
	Future<Void> write(void const* data, int length, int64_t offset) override // Copies data synchronously
	{
		++countFileLogicalWrites;
		++countLogicalWrites;
		// Standalone<StringRef> copy = StringRef((const uint8_t*)data, length);
		return write_impl(fd, err, StringRef((const uint8_t*)data, length), offset);
	}
	Future<Void> truncate(int64_t size) override {
		++countFileLogicalWrites;
		++countLogicalWrites;
		return truncate_impl(fd, err, size);
	}
	Future<Void> sync() override {
		++countFileLogicalWrites;
		++countLogicalWrites;
		auto fsync = sync_impl(fd, err);

		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;

			return waitAndAtomicRename(fsync, filename + ".part", filename);
		}

		return fsync;
	}
	Future<int64_t> size() const override {
		++countFileLogicalReads;
		++countLogicalReads;
		return size_impl(fd);
	}
	std::string getFilename() const override { return filename; }

	ACTOR static Future<Void> async_fsync_parent(std::filesystem::path const& filename) {
		std::string folder = parentDirectory(filename);
		TraceEvent("FSyncParentDir").detail("Folder", folder).detail("File", filename);
		state int folderFD = ::open(folder.c_str(), O_DIRECTORY | O_CLOEXEC, 0);
		if (folderFD < 0)
			throw io_error();
		try {
			wait(async_fsync(folderFD)); // not sure if fdatasync on the folder has the same effect
		} catch (...) {
			close(folderFD);
			throw;
		}
		close(folderFD);
		return Void();
	}

	static Future<Void> async_fdatasync(int fd) {
		// Used by AsyncFileKAIO, since kernel AIO doesn't really implement fdatasync yet
		return sync_impl(fd, makeReference<ErrorInfo>());
	}
	static Future<Void> async_fsync(int fd) {
		// Used by AsyncFileKAIO, since kernel AIO doesn't really implement fsync yet
		return sync_impl(fd, makeReference<ErrorInfo>(), true);
	}
	ACTOR static Future<Void> waitAndAtomicRename(Future<Void> fsync,
	                                              std::string part_filename,
	                                              std::string final_filename) {
		// First wait for the data in the part file to be durable
		wait(fsync);

		// rename() is atomic
		if (rename(part_filename.c_str(), final_filename.c_str())) {
			TraceEvent("AsyncFileEIORenameError").detail("Filename", final_filename).GetLastError();
			throw io_error();
		}

		// fsync the parent directory to make it durable as well
		wait(async_fsync_parent(final_filename));

		return Void();
	}

	// Run the given function on the EIO thread pool and return its result
	template <class R>
	static Future<R> dispatch(std::function<R()> const& func) {
		return dispatch_impl(func);
	}

	~AsyncFileEIO() override { close_impl(fd); }

private:
	struct ErrorInfo : ReferenceCounted<ErrorInfo>, FastAllocated<ErrorInfo> {
		Error err;
		void set(const Error& e) {
			if (err.code() == invalid_error_code)
				err = e;
		}
		void report() {
			if (err.code() != invalid_error_code)
				throw err;
		}
	};

	template <class R>
	struct Dispatch {
		std::function<R()> func;
		ErrorOr<R> result;
		Promise<Void> done;
		explicit Dispatch(std::function<R()> const& func) : func(func) {}
	};

	int fd, flags;
	Reference<ErrorInfo> err;
	std::string filename;
	mutable Int64MetricHandle countFileLogicalWrites;
	mutable Int64MetricHandle countFileLogicalReads;

	mutable Int64MetricHandle countLogicalWrites;
	mutable Int64MetricHandle countLogicalReads;

	AsyncFileEIO(int fd, int flags, std::string const& filename)
	  : fd(fd), flags(flags), err(new ErrorInfo), filename(filename) {
		if (!g_network->isSimulated()) {
			countFileLogicalWrites.init("AsyncFile.CountFileLogicalWrites"_sr, filename);
			countFileLogicalReads.init("AsyncFile.CountFileLogicalReads"_sr, filename);

			countLogicalWrites.init("AsyncFile.CountLogicalWrites"_sr);
			countLogicalReads.init("AsyncFile.CountLogicalReads"_sr);
		}
	}

	static int openFlags(int flags) {
		int oflags = O_CLOEXEC;
		ASSERT(bool(flags & OPEN_READONLY) != bool(flags & OPEN_READWRITE)); // readonly xor readwrite
		if (flags & OPEN_EXCLUSIVE)
			oflags |= O_EXCL;
		if (flags & OPEN_CREATE)
			oflags |= O_CREAT;
		if (flags & OPEN_READONLY)
			oflags |= O_RDONLY;
		if (flags & OPEN_READWRITE)
			oflags |= O_RDWR;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE)
			oflags |= O_TRUNC;
#if defined(__linux__)
		if (flags & OPEN_UNBUFFERED && FLOW_KNOBS->EIO_USE_ODIRECT)
			oflags |= O_DIRECT;
#endif
		return oflags;
	}

	static void error(const char* context,
	                  int fd,
	                  eio_req* r,
	                  Reference<ErrorInfo> const& err = Reference<ErrorInfo>()) {
		Error e = io_error();
		errno = r->errorno;
		TraceEvent(context).error(e).detail("Fd", fd).detail("Result", r->result).GetLastError();
		if (err)
			err->set(e);
		else
			throw e;
	}

	ACTOR static void close_impl(int fd) {
		state Promise<Void> p;
		state eio_req* r = eio_close(fd, 0, eio_callback, &p);
		wait(p.getFuture());
		if (r->result)
			error("CloseError", fd, r);
		TraceEvent("AsyncFileClosed").suppressFor(1.0).detail("Fd", fd);
	}

	ACTOR static Future<int> read_impl(int fd, void* data, int length, int64_t offset) {
		state TaskPriority taskID = g_network->getCurrentTask();
		state Promise<Void> p;
		// fprintf(stderr, "eio_read (fd=%d length=%d offset=%lld)\n", fd, length, offset);
		state eio_req* r = eio_read(fd, data, length, offset, 0, eio_callback, &p);
		try {
			wait(p.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}
		try {
			state int result = r->result;
			// printf("eio read: %d/%d\n", r->result, length);
			if (result == -1) {
				error("ReadError", fd, r);
				throw internal_error();
			} else {
				wait(delay(0, taskID));
				return result;
			}
		} catch (Error& _e) {
			state Error e = _e;
			wait(delay(0, taskID));
			throw e;
		}
	}

	ACTOR static Future<Void> write_impl(int fd, Reference<ErrorInfo> err, StringRef data, int64_t offset) {
		state TaskPriority taskID = g_network->getCurrentTask();
		state Promise<Void> p;
		state eio_req* r = eio_write(fd, (void*)data.begin(), data.size(), offset, 0, eio_callback, &p);
		try {
			wait(p.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}
		if (r->result != data.size())
			error("WriteError", fd, r, err);
		wait(delay(0, taskID));
		return Void();
	}

	ACTOR static Future<Void> truncate_impl(int fd, Reference<ErrorInfo> err, int64_t size) {
		state TaskPriority taskID = g_network->getCurrentTask();
		state Promise<Void> p;
		state eio_req* r = eio_ftruncate(fd, size, 0, eio_callback, &p);
		try {
			wait(p.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}
		if (r->result)
			error("TruncateError", fd, r, err);
		wait(delay(0, taskID));
		return Void();
	}

	static eio_req* start_fsync(int fd, Promise<Void>& p, bool sync_metadata) {
#ifdef __APPLE__
		// Neither fsync() nor fdatasync() do the right thing on OS X!
		eio_req* req = (eio_req*)calloc(1, sizeof *req);
		req->type = EIO_CUSTOM;
		req->pri = 0;
		req->finish = eio_callback;
		req->data = &p;
		req->destroy = free_req;
		req->int1 = fd;
		req->feed = apple_fsync;
		eio_submit(req);
		return req;
#else
		if (sync_metadata)
			return eio_fsync(fd, 0, eio_callback, &p);
		else
			return eio_fdatasync(fd, 0, eio_callback, &p);
#endif
	}

	ACTOR static Future<Void> sync_impl(int fd, Reference<ErrorInfo> err, bool sync_metadata = false) {
		state TaskPriority taskID = g_network->getCurrentTask();
		state Promise<Void> p;
		state eio_req* r = start_fsync(fd, p, sync_metadata);

		try {
			wait(p.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}
		try {
			// Report any errors from prior write() or truncate() calls
			err->report();

			if (r->result)
				error("SyncError", fd, r);
			wait(delay(0, taskID));
			return Void();
		} catch (Error& _e) {
			state Error e = _e;
			wait(delay(0, taskID));
			throw e;
		}
	}

	ACTOR static Future<int64_t> size_impl(int fd) {
		state TaskPriority taskID = g_network->getCurrentTask();
		state Promise<Void> p;
		state eio_req* r = eio_fstat(fd, 0, eio_callback, &p);
		try {
			wait(p.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}
		if (r->result)
			error("FStatError", fd, r);
		EIO_STRUCT_STAT* statdata = (EIO_STRUCT_STAT*)r->ptr2;
		if (!statdata)
			error("FStatBufferError", fd, r);
		state int64_t size = statdata->st_size;
		wait(delay(0, taskID));
		return size;
	}

	ACTOR static Future<EIO_STRUCT_STAT> stat_impl(std::string filename) {
		state TaskPriority taskID = g_network->getCurrentTask();
		state Promise<Void> p;
		state EIO_STRUCT_STAT statdata;
		state eio_req* r = eio_stat(filename.c_str(), 0, eio_callback, &p);
		try {
			wait(p.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}
		if (r->result)
			error("StatError", 0, r);
		if (!r->ptr2)
			error("StatBufferError", 0, r);
		statdata = *EIO_STAT_BUF(r);
		wait(delay(0, taskID));
		return statdata;
	}

	ACTOR template <class R>
	static Future<R> dispatch_impl(std::function<R()> func) {
		state Dispatch<R> data(func);
		state TaskPriority taskID = g_network->getCurrentTask();

		state eio_req* r = eio_custom(
		    [](eio_req* req) {
			    // Runs on the eio thread pool
			    auto data = reinterpret_cast<Dispatch<R>*>(req->data);
			    try {
				    data->result = data->func();
				    req->result = 0;
			    } catch (Error& e) {
				    data->result = e;
				    req->result = -1;
			    } catch (...) {
				    data->result = unknown_error();
				    req->result = -1;
			    }
		    },
		    0,
		    [](eio_req* req) {
			    // Runs on the main thread, in eio_poll()
			    if (EIO_CANCELLED(req))
				    return 0;
			    auto data = reinterpret_cast<Dispatch<R>*>(req->data);
			    Promise<Void> p = std::move(data->done);
			    p.send(Void());
			    return 0;
		    },
		    &data);
		try {
			wait(data.done.getFuture());
		} catch (...) {
			g_network->setCurrentTask(taskID);
			eio_cancel(r);
			throw;
		}

		wait(delay(0, taskID));
		if (data.result.isError())
			throw data.result.getError();
		return data.result.get();
	}

	static std::atomic<int32_t> want_poll;

	ACTOR static void poll_eio() {
		while (eio_poll() == -1)
			wait(yield());
		want_poll = 0;
	}

	static void eio_want_poll() {
		want_poll = 1;
		// SOMEDAY: nullptr for deferred error, no analysis of correctness (itp)
		onMainThreadVoid([]() { poll_eio(); }, TaskPriority::PollEIO);
	}

	static int eio_callback(eio_req* req) {
		if (EIO_CANCELLED(req))
			return 0;
		Promise<Void> p = std::move(*(Promise<Void>*)req->data);
		p.send(Void());
		return 0;
	}

#ifdef __APPLE__
	static void apple_fsync(eio_req* req) { req->result = fcntl(req->int1, F_FULLFSYNC, 0); }
	static void free_req(eio_req* req) { free(req); }
#endif
};

#ifdef FILESYSTEM_IMPL
std::atomic<int32_t> AsyncFileEIO::want_poll = 0;
#endif

#include "flow/unactorcompiler.h"
#endif
#endif
