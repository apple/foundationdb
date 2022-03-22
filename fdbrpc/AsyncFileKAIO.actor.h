/*
 * AsyncFileKAIO.actor.h
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

#pragma once
#ifdef __linux__

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILEKAIO_ACTOR_G_H)
#define FLOW_ASYNCFILEKAIO_ACTOR_G_H
#include "fdbrpc/AsyncFileKAIO.actor.g.h"
#elif !defined(FLOW_ASYNCFILEKAIO_ACTOR_H)
#define FLOW_ASYNCFILEKAIO_ACTOR_H

#include "fdbrpc/IAsyncFile.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include "fdbrpc/linux_kaio.h"
#include "flow/Knobs.h"
#include "flow/UnitTest.h"
#include <stdio.h>
#include "flow/crc32c.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Set this to true to enable detailed KAIO request logging, which currently is written to a hardcoded location
// /data/v7/fdb/
#define KAIO_LOGGING 0

DESCR struct SlowAioSubmit {
	int64_t submitDuration; // ns
	int64_t truncateDuration; // ns
	int64_t numTruncates;
	int64_t truncateBytes;
	int64_t largestTruncate;
};

class AsyncFileKAIO final : public IAsyncFile, public ReferenceCounted<AsyncFileKAIO> {
public:

#if KAIO_LOGGING
private:
#pragma pack(push, 1)
	struct OpLogEntry {
		OpLogEntry() : result(0) {}
		enum EOperation{ READ = 1, WRITE = 2, SYNC = 3, TRUNCATE = 4 };
		enum EStage{ START = 1, LAUNCH = 2, REQUEUE = 3, COMPLETE = 4, READY = 5 };
		int64_t timestamp;
		uint32_t id;
		uint32_t checksum;
		uint32_t pageOffset;
		uint8_t pageCount;
		uint8_t op;
		uint8_t stage;
		uint32_t result;

		static uint32_t nextID() {
			static uint32_t last = 0;
			return ++last;
		}

		void log(FILE* file) {
			if (ftell(file) > (int64_t)50 * 1e9)
				fseek(file, 0, SEEK_SET);
			if (!fwrite(this, sizeof(OpLogEntry), 1, file))
				throw io_error();
		}
	};
#pragma pop

	FILE* logFile;
	struct IOBlock;
	static void KAIOLogBlockEvent(IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result = 0);
	static void KAIOLogBlockEvent(FILE* logFile, IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result = 0);
	static void KAIOLogEvent(FILE* logFile,
	                         uint32_t id,
	                         OpLogEntry::EOperation op,
	                         OpLogEntry::EStage stage,
	                         uint32_t pageOffset = 0,
	                         uint32_t result = 0);

public:
#else
#define KAIOLogBlockEvent(...)
#define KAIOLogEvent(...)
#endif

	static Future<Reference<IAsyncFile>> open(std::string filename, int flags, int mode, void* ignore) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		ASSERT(flags & OPEN_UNBUFFERED);

		if (flags & OPEN_LOCK)
			mode |= 02000; // Enable mandatory locking for this file if it is supported by the filesystem

		std::string open_filename = filename;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			ASSERT((flags & OPEN_CREATE) && (flags & OPEN_READWRITE) && !(flags & OPEN_EXCLUSIVE));
			open_filename = filename + ".part";
		}

		int fd = ::open(open_filename.c_str(), openFlags(flags), mode);
		if (fd < 0) {
			Error e = errno == ENOENT ? file_not_found() : io_error();
			int ecode = errno; // Save errno in case it is modified before it is used below
			TraceEvent ev("AsyncFileKAIOOpenFailed");
			ev.error(e)
			    .detail("Filename", filename)
			    .detailf("Flags", "%x", flags)
			    .detailf("OSFlags", "%x", openFlags(flags))
			    .detailf("Mode", "0%o", mode)
			    .GetLastError();
			if (ecode == EINVAL)
				ev.detail("Description", "Invalid argument - Does the target filesystem support KAIO?");
			return e;
		} else {
			TraceEvent("AsyncFileKAIOOpen")
			    .detail("Filename", filename)
			    .detail("Flags", flags)
			    .detail("Mode", mode)
			    .detail("Fd", fd);
		}

		Reference<AsyncFileKAIO> r(new AsyncFileKAIO(fd, flags, filename));

		if (flags & OPEN_LOCK) {
			// Acquire a "write" lock for the entire file
			flock lockDesc;
			lockDesc.l_type = F_WRLCK;
			lockDesc.l_whence = SEEK_SET;
			lockDesc.l_start = 0;
			lockDesc.l_len =
			    0; // "Specifying 0 for l_len has the special meaning: lock all bytes starting at the location specified
			       // by l_whence and l_start through to the end of file, no matter how large the file grows."
			lockDesc.l_pid = 0;
			if (fcntl(fd, F_SETLK, &lockDesc) == -1) {
				TraceEvent(SevError, "UnableToLockFile").detail("Filename", filename).GetLastError();
				return io_error();
			}
		}

		struct stat buf;
		if (fstat(fd, &buf)) {
			TraceEvent("AsyncFileKAIOFStatError").detail("Fd", fd).detail("Filename", filename).GetLastError();
			return io_error();
		}

		r->lastFileSize = r->nextFileSize = buf.st_size;
		return Reference<IAsyncFile>(std::move(r));
	}

	static void init(Reference<IEventFD> ev, double ioTimeout) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		if (!g_network->isSimulated()) {
			ctx.countAIOSubmit.init(LiteralStringRef("AsyncFile.CountAIOSubmit"));
			ctx.countAIOCollect.init(LiteralStringRef("AsyncFile.CountAIOCollect"));
			ctx.submitMetric.init(LiteralStringRef("AsyncFile.Submit"));
			ctx.countPreSubmitTruncate.init(LiteralStringRef("AsyncFile.CountPreAIOSubmitTruncate"));
			ctx.preSubmitTruncateBytes.init(LiteralStringRef("AsyncFile.PreAIOSubmitTruncateBytes"));
			ctx.slowAioSubmitMetric.init(LiteralStringRef("AsyncFile.SlowAIOSubmit"));
		}

		int rc = io_setup(FLOW_KNOBS->MAX_OUTSTANDING, &ctx.iocx);
		if (rc < 0) {
			TraceEvent("IOSetupError").GetLastError();
			throw io_error();
		}
		setTimeout(ioTimeout);
		ctx.evfd = ev->getFD();
		poll(ev);

		g_network->setGlobal(INetwork::enRunCycleFunc, (flowGlobalType)&AsyncFileKAIO::launch);
	}

	static int get_eventfd() { return ctx.evfd; }
	static void setTimeout(double ioTimeout) { ctx.setIOTimeout(ioTimeout); }

	void addref() override { ReferenceCounted<AsyncFileKAIO>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileKAIO>::delref(); }
	Future<int> read(void* data, int length, int64_t offset) override {
		++countFileLogicalReads;
		++countLogicalReads;
		// printf("%p Begin logical read\n", getCurrentCoro());

		if (failed) {
			return io_timeout();
		}

		IOBlock* io = new IOBlock(IO_CMD_PREAD, fd);
		io->buf = data;
		io->nbytes = length;
		io->offset = offset;

		enqueue(io, "read", this);
		Future<int> result = io->result.getFuture();

#if KAIO_LOGGING
		// result = map(result, [=](int r) mutable { KAIOLogBlockEvent(io, OpLogEntry::READY, r); return r; });
#endif

		return result;
	}
	Future<Void> write(void const* data, int length, int64_t offset) override {
		++countFileLogicalWrites;
		++countLogicalWrites;
		// printf("%p Begin logical write\n", getCurrentCoro());

		if (failed) {
			return io_timeout();
		}

		IOBlock* io = new IOBlock(IO_CMD_PWRITE, fd);
		io->buf = (void*)data;
		io->nbytes = length;
		io->offset = offset;

		nextFileSize = std::max(nextFileSize, offset + length);

		enqueue(io, "write", this);
		Future<int> result = io->result.getFuture();

#if KAIO_LOGGING
		// result = map(result, [=](int r) mutable { KAIOLogBlockEvent(io, OpLogEntry::READY, r); return r; });
#endif

		// auto& actorLineageSet = IAsyncFileSystem::filesystem()->getActorLineageSet();
		// auto index = actorLineageSet.insert(*currentLineage);
		// ASSERT(index != ActorLineageSet::npos);
		Future<Void> res = success(result);
		// actorLineageSet.erase(index);
		return res;
	}
// TODO(alexmiller): Remove when we upgrade the dev docker image to >14.10
#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE 0x10
#endif
	Future<Void> zeroRange(int64_t offset, int64_t length) override {
		bool success = false;
		if (ctx.fallocateZeroSupported) {
			int rc = fallocate(fd, FALLOC_FL_ZERO_RANGE, offset, length);
			if (rc == EOPNOTSUPP) {
				ctx.fallocateZeroSupported = false;
			}
			if (rc == 0) {
				success = true;
			}
		}
		return success ? Void() : IAsyncFile::zeroRange(offset, length);
	}
	Future<Void> truncate(int64_t size) override {
		++countFileLogicalWrites;
		++countLogicalWrites;

		if (failed) {
			return io_timeout();
		}

#if KAIO_LOGGING
		uint32_t id = OpLogEntry::nextID();
#endif
		int result = -1;
		KAIOLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::START, size / 4096);
		bool completed = false;
		double begin = timer_monotonic();

		if (ctx.fallocateSupported && size >= lastFileSize) {
			result = fallocate(fd, 0, 0, size);
			if (result != 0) {
				int fallocateErrCode = errno;
				TraceEvent("AsyncFileKAIOAllocateError")
				    .detail("Fd", fd)
				    .detail("Filename", filename)
				    .detail("Size", size)
				    .GetLastError();
				if (fallocateErrCode == EOPNOTSUPP) {
					// Mark fallocate as unsupported. Try again with truncate.
					ctx.fallocateSupported = false;
				} else {
					KAIOLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::COMPLETE, size / 4096, result);
					return io_error();
				}
			} else {
				completed = true;
			}
		}
		if (!completed)
			result = ftruncate(fd, size);

		double end = timer_monotonic();
		if (nondeterministicRandom()->random01() < end - begin) {
			TraceEvent("SlowKAIOTruncate")
			    .detail("TruncateTime", end - begin)
			    .detail("TruncateBytes", size - lastFileSize);
		}
		KAIOLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::COMPLETE, size / 4096, result);

		if (result != 0) {
			TraceEvent("AsyncFileKAIOTruncateError").detail("Fd", fd).detail("Filename", filename).GetLastError();
			return io_error();
		}

		lastFileSize = nextFileSize = size;

		return Void();
	}

	ACTOR static Future<Void> throwErrorIfFailed(Reference<AsyncFileKAIO> self, Future<Void> sync) {
		wait(sync);
		if (self->failed) {
			throw io_timeout();
		}
		return Void();
	}

	Future<Void> sync() override {
		++countFileLogicalWrites;
		++countLogicalWrites;

		if (failed) {
			return io_timeout();
		}

#if KAIO_LOGGING
		uint32_t id = OpLogEntry::nextID();
#endif

		KAIOLogEvent(logFile, id, OpLogEntry::SYNC, OpLogEntry::START);

		Future<Void> fsync = throwErrorIfFailed(
		    Reference<AsyncFileKAIO>::addRef(this),
		    AsyncFileEIO::async_fdatasync(fd)); // Don't close the file until the asynchronous thing is done
		// Alas, AIO f(data)sync doesn't seem to actually be implemented by the kernel
		/*IOBlock *io = new IOBlock(IO_CMD_FDSYNC, fd);
		submit(io, "write");
		fsync=success(io->result.getFuture());*/

#if KAIO_LOGGING
		fsync = map(fsync, [=](Void r) mutable {
			KAIOLogEvent(logFile, id, OpLogEntry::SYNC, OpLogEntry::COMPLETE);
			return r;
		});
#endif

		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;

			return AsyncFileEIO::waitAndAtomicRename(fsync, filename + ".part", filename);
		}

		return fsync;
	}
	Future<int64_t> size() const override { return nextFileSize; }
	int64_t debugFD() const override { return fd; }
	std::string getFilename() const override { return filename; }
	~AsyncFileKAIO() override {
		close(fd);

#if KAIO_LOGGING
		if (logFile != nullptr)
			fclose(logFile);
#endif
	}

	static void launch() {
		if (ctx.queue.size() && ctx.outstanding < FLOW_KNOBS->MAX_OUTSTANDING - FLOW_KNOBS->MIN_SUBMIT) {
			ctx.submitMetric = true;

			double begin = timer_monotonic();
			if (!ctx.outstanding)
				ctx.ioStallBegin = begin;

			IOBlock* toStart[FLOW_KNOBS->MAX_OUTSTANDING];
			int n = std::min<size_t>(FLOW_KNOBS->MAX_OUTSTANDING - ctx.outstanding, ctx.queue.size());

			int64_t previousTruncateCount = ctx.countPreSubmitTruncate;
			int64_t previousTruncateBytes = ctx.preSubmitTruncateBytes;
			int64_t largestTruncate = 0;

			for (int i = 0; i < n; i++) {
				auto io = ctx.queue.top();

				KAIOLogBlockEvent(io, OpLogEntry::LAUNCH);

				ctx.queue.pop();
				toStart[i] = io;
				io->startTime = now();

				if (ctx.ioTimeout > 0) {
					ctx.appendToRequestList(io);
				}

				if (io->owner->lastFileSize != io->owner->nextFileSize) {
					++ctx.countPreSubmitTruncate;
					int64_t truncateSize = io->owner->nextFileSize - io->owner->lastFileSize;
					ASSERT(truncateSize > 0);
					ctx.preSubmitTruncateBytes += truncateSize;
					largestTruncate = std::max(largestTruncate, truncateSize);
					io->owner->truncate(io->owner->nextFileSize);
				}
			}
			double truncateComplete = timer_monotonic();
			int rc = io_submit(ctx.iocx, n, (linux_iocb**)toStart);
			double end = timer_monotonic();

			if (end - begin > FLOW_KNOBS->SLOW_LOOP_CUTOFF) {
				ctx.slowAioSubmitMetric->submitDuration = end - truncateComplete;
				ctx.slowAioSubmitMetric->truncateDuration = truncateComplete - begin;
				ctx.slowAioSubmitMetric->numTruncates = ctx.countPreSubmitTruncate - previousTruncateCount;
				ctx.slowAioSubmitMetric->truncateBytes = ctx.preSubmitTruncateBytes - previousTruncateBytes;
				ctx.slowAioSubmitMetric->largestTruncate = largestTruncate;
				ctx.slowAioSubmitMetric->log();

				if (nondeterministicRandom()->random01() < end - begin) {
					TraceEvent("SlowKAIOLaunch")
					    .detail("IOSubmitTime", end - truncateComplete)
					    .detail("TruncateTime", truncateComplete - begin)
					    .detail("TruncateCount", ctx.countPreSubmitTruncate - previousTruncateCount)
					    .detail("TruncateBytes", ctx.preSubmitTruncateBytes - previousTruncateBytes)
					    .detail("LargestTruncate", largestTruncate);
				}
			}

			ctx.submitMetric = false;
			++ctx.countAIOSubmit;

			double elapsed = timer_monotonic() - begin;
			g_network->networkInfo.metrics.secSquaredSubmit += elapsed * elapsed / 2;

			//TraceEvent("Launched").detail("N", rc).detail("Queued", ctx.queue.size()).detail("Elapsed", elapsed).detail("Outstanding", ctx.outstanding+rc);
			// printf("launched: %d/%d in %f us (%d outstanding; lowest prio %d)\n", rc, ctx.queue.size(), elapsed*1e6,
			// ctx.outstanding + rc, toStart[n-1]->getTask());
			if (rc < 0) {
				if (errno == EAGAIN) {
					rc = 0;
				} else {
					KAIOLogBlockEvent(toStart[0], OpLogEntry::COMPLETE, errno ? -errno : -1000000);
					// Other errors are assumed to represent failure to issue the first I/O in the list
					toStart[0]->setResult(errno ? -errno : -1000000);
					rc = 1;
				}
			} else
				ctx.outstanding += rc;
			// Any unsubmitted I/Os need to be requeued
			for (int i = rc; i < n; i++) {
				KAIOLogBlockEvent(toStart[i], OpLogEntry::REQUEUE);
				ctx.queue.push(toStart[i]);
			}
		}
	}

	bool failed;

private:
	int fd, flags;
	int64_t lastFileSize, nextFileSize;
	std::string filename;
	Int64MetricHandle countFileLogicalWrites;
	Int64MetricHandle countFileLogicalReads;

	Int64MetricHandle countLogicalWrites;
	Int64MetricHandle countLogicalReads;

	struct IOBlock : linux_iocb, FastAllocated<IOBlock> {
		Promise<int> result;
		Reference<AsyncFileKAIO> owner;
		int64_t prio;
		IOBlock* prev;
		IOBlock* next;
		double startTime;
#if KAIO_LOGGING
		int32_t iolog_id;
#endif

		struct indirect_order_by_priority {
			bool operator()(IOBlock* a, IOBlock* b) { return a->prio < b->prio; }
		};

		IOBlock(int op, int fd) : prev(nullptr), next(nullptr), startTime(0) {
			memset((linux_iocb*)this, 0, sizeof(linux_iocb));
			aio_lio_opcode = op;
			aio_fildes = fd;
#if KAIO_LOGGING
			iolog_id = 0;
#endif
		}

		TaskPriority getTask() const { return static_cast<TaskPriority>((prio >> 32) + 1); }

		ACTOR static void deliver(Promise<int> result, bool failed, int r, TaskPriority task) {
			wait(delay(0, task));
			if (failed)
				result.sendError(io_timeout());
			else if (r < 0)
				result.sendError(io_error());
			else
				result.send(r);
		}

		void setResult(int r) {
			if (r < 0) {
				struct stat fst;
				fstat(aio_fildes, &fst);

				errno = -r;
				TraceEvent("AsyncFileKAIOIOError")
				    .GetLastError()
				    .detail("Fd", aio_fildes)
				    .detail("Op", aio_lio_opcode)
				    .detail("Nbytes", nbytes)
				    .detail("Offset", offset)
				    .detail("Ptr", int64_t(buf))
				    .detail("Size", fst.st_size)
				    .detail("Filename", owner->filename);
			}
			deliver(result, owner->failed, r, getTask());
			delete this;
		}

		void timeout(bool warnOnly) {
			TraceEvent(SevWarnAlways, "AsyncFileKAIOTimeout")
			    .detail("Fd", aio_fildes)
			    .detail("Op", aio_lio_opcode)
			    .detail("Nbytes", nbytes)
			    .detail("Offset", offset)
			    .detail("Ptr", int64_t(buf))
			    .detail("Filename", owner->filename);
			g_network->setGlobal(INetwork::enASIOTimedOut, (flowGlobalType) true);

			if (!warnOnly)
				owner->failed = true;
		}
	};

	struct Context {
		io_context_t iocx;
		int evfd;
		int outstanding;
		double ioStallBegin;
		bool fallocateSupported;
		bool fallocateZeroSupported;
		std::priority_queue<IOBlock*, std::vector<IOBlock*>, IOBlock::indirect_order_by_priority> queue;
		Int64MetricHandle countAIOSubmit;
		Int64MetricHandle countAIOCollect;
		Int64MetricHandle submitMetric;

		double ioTimeout;
		bool timeoutWarnOnly;
		IOBlock* submittedRequestList;

		Int64MetricHandle countPreSubmitTruncate;
		Int64MetricHandle preSubmitTruncateBytes;

		EventMetricHandle<SlowAioSubmit> slowAioSubmitMetric;

		uint32_t opsIssued;
		Context()
		  : iocx(0), evfd(-1), outstanding(0), ioStallBegin(0), fallocateSupported(true), fallocateZeroSupported(true),
		    submittedRequestList(nullptr), opsIssued(0) {
			setIOTimeout(0);
		}

		void setIOTimeout(double timeout) {
			ioTimeout = fabs(timeout);
			timeoutWarnOnly = timeout < 0;
		}

		void appendToRequestList(IOBlock* io) {
			ASSERT(!io->next && !io->prev);

			if (submittedRequestList) {
				io->prev = submittedRequestList->prev;
				io->prev->next = io;

				submittedRequestList->prev = io;
				io->next = submittedRequestList;
			} else {
				submittedRequestList = io;
				io->next = io->prev = io;
			}
		}

		void removeFromRequestList(IOBlock* io) {
			if (io->next == nullptr) {
				ASSERT(io->prev == nullptr);
				return;
			}

			ASSERT(io->prev != nullptr);

			if (io == io->next) {
				ASSERT(io == submittedRequestList && io == io->prev);
				submittedRequestList = nullptr;
			} else {
				io->next->prev = io->prev;
				io->prev->next = io->next;

				if (submittedRequestList == io) {
					submittedRequestList = io->next;
				}
			}

			io->next = io->prev = nullptr;
		}
	};
	static Context ctx;

	explicit AsyncFileKAIO(int fd, int flags, std::string const& filename)
	  : failed(false), fd(fd), flags(flags), filename(filename) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		if (!g_network->isSimulated()) {
			countFileLogicalWrites.init(LiteralStringRef("AsyncFile.CountFileLogicalWrites"), filename);
			countFileLogicalReads.init(LiteralStringRef("AsyncFile.CountFileLogicalReads"), filename);
			countLogicalWrites.init(LiteralStringRef("AsyncFile.CountLogicalWrites"));
			countLogicalReads.init(LiteralStringRef("AsyncFile.CountLogicalReads"));
		}

#if KAIO_LOGGING
		logFile = nullptr;
		// TODO:  Don't do this hacky investigation-specific thing
		StringRef fname(filename);
		if (fname.endsWith(LiteralStringRef(".sqlite")) || fname.endsWith(LiteralStringRef(".sqlite-wal"))) {
			std::string logFileName = basename(filename);
			while (logFileName.find("/") != std::string::npos)
				logFileName = logFileName.substr(logFileName.find("/") + 1);
			if (!logFileName.empty()) {
				// TODO: don't hardcode this path
				std::string logPath("/data/v7/fdb/");
				try {
					platform::createDirectory(logPath);
					logFileName = logPath + format("%s.iolog", logFileName.c_str());
					logFile = fopen(logFileName.c_str(), "r+");
					if (logFile == nullptr)
						logFile = fopen(logFileName.c_str(), "w");
					if (logFile != nullptr)
						TraceEvent("KAIOLogOpened").detail("File", filename).detail("LogFile", logFileName);
					else {
						TraceEvent(SevWarn, "KAIOLogOpenFailure")
						    .detail("File", filename)
						    .detail("LogFile", logFileName)
						    .detail("ErrorCode", errno)
						    .detail("ErrorDesc", strerror(errno));
					}
				} catch (Error& e) {
					TraceEvent(SevError, "KAIOLogOpenFailure").error(e);
				}
			}
		}
#endif
	}

	void enqueue(IOBlock* io, const char* op, AsyncFileKAIO* owner) {
		ASSERT(int64_t(io->buf) % 4096 == 0 && io->offset % 4096 == 0 && io->nbytes % 4096 == 0);

		KAIOLogBlockEvent(owner->logFile, io, OpLogEntry::START);

		io->flags |= 1;
		io->eventfd = ctx.evfd;
		io->prio = (int64_t(g_network->getCurrentTask()) << 32) - (++ctx.opsIssued);
		// io->prio = - (++ctx.opsIssued);
		io->owner = Reference<AsyncFileKAIO>::addRef(owner);

		ctx.queue.push(io);
	}

	static int openFlags(int flags) {
		int oflags = O_DIRECT | O_CLOEXEC;
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
		return oflags;
	}

	ACTOR static void poll(Reference<IEventFD> ev) {
		loop {
			wait(success(ev->read()));

			wait(delay(0, TaskPriority::DiskIOComplete));

			linux_ioresult ev[FLOW_KNOBS->MAX_OUTSTANDING];
			timespec tm;
			tm.tv_sec = 0;
			tm.tv_nsec = 0;

			int n;

			loop {
				n = io_getevents(ctx.iocx, 0, FLOW_KNOBS->MAX_OUTSTANDING, ev, &tm);
				if (n >= 0 || errno != EINTR)
					break;
			}

			++ctx.countAIOCollect;
			// printf("io_getevents: collected %d/%d in %f us (%d queued)\n", n, ctx.outstanding, (timer()-before)*1e6,
			// ctx.queue.size());
			if (n < 0) {
				// printf("io_getevents failed: %d\n", errno);
				TraceEvent("IOGetEventsError").GetLastError();
				throw io_error();
			}
			if (n) {
				double t = timer_monotonic();
				double elapsed = t - ctx.ioStallBegin;
				ctx.ioStallBegin = t;
				g_network->networkInfo.metrics.secSquaredDiskStall += elapsed * elapsed / 2;
			}

			ctx.outstanding -= n;

			if (ctx.ioTimeout > 0) {
				double currentTime = now();
				while (ctx.submittedRequestList && currentTime - ctx.submittedRequestList->startTime > ctx.ioTimeout) {
					ctx.submittedRequestList->timeout(ctx.timeoutWarnOnly);
					ctx.removeFromRequestList(ctx.submittedRequestList);
				}
			}

			for (int i = 0; i < n; i++) {
				IOBlock* iob = static_cast<IOBlock*>(ev[i].iocb);

				KAIOLogBlockEvent(iob, OpLogEntry::COMPLETE, ev[i].result);

				if (ctx.ioTimeout > 0) {
					ctx.removeFromRequestList(iob);
				}

				iob->setResult(ev[i].result);
			}
		}
	}
};

#if KAIO_LOGGING
// Call from contexts where only an ioblock is available, log if its owner is set
void AsyncFileKAIO::KAIOLogBlockEvent(IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result) {
	if (ioblock->owner)
		return KAIOLogBlockEvent(ioblock->owner->logFile, ioblock, stage, result);
}

void AsyncFileKAIO::KAIOLogBlockEvent(FILE* logFile, IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result) {
	if (logFile != nullptr) {
		// Figure out what type of operation this is
		OpLogEntry::EOperation op;
		if (ioblock->aio_lio_opcode == IO_CMD_PREAD)
			op = OpLogEntry::READ;
		else if (ioblock->aio_lio_opcode == IO_CMD_PWRITE)
			op = OpLogEntry::WRITE;
		else
			return;

		// Assign this IO operation an io log id number if it doesn't already have one
		if (ioblock->iolog_id == 0)
			ioblock->iolog_id = OpLogEntry::nextID();

		OpLogEntry e;
		e.timestamp = timer_int();
		e.op = (uint8_t)op;
		e.id = ioblock->iolog_id;
		e.stage = (uint8_t)stage;
		e.pageOffset = (uint32_t)(ioblock->offset / 4096);
		e.pageCount = (uint8_t)(ioblock->nbytes / 4096);
		e.result = result;

		// Log a checksum for Writes up to the Complete stage or Reads starting from the Complete stage
		if ((op == OpLogEntry::WRITE && stage <= OpLogEntry::COMPLETE) ||
		    (op == OpLogEntry::READ && stage >= OpLogEntry::COMPLETE))
			e.checksum = crc32c_append(0xab12fd93, ioblock->buf, ioblock->nbytes);
		else
			e.checksum = 0;

		e.log(logFile);
	}
}

void AsyncFileKAIO::KAIOLogEvent(FILE* logFile,
                                 uint32_t id,
                                 OpLogEntry::EOperation op,
                                 OpLogEntry::EStage stage,
                                 uint32_t pageOffset,
                                 uint32_t result) {
	if (logFile != nullptr) {
		OpLogEntry e;
		e.timestamp = timer_int();
		e.id = id;
		e.op = (uint8_t)op;
		e.stage = (uint8_t)stage;
		e.pageOffset = pageOffset;
		e.pageCount = 0;
		e.checksum = 0;
		e.result = result;
		e.log(logFile);
	}
}
#endif

ACTOR Future<Void> runTestOps(Reference<IAsyncFile> f, int numIterations, int fileSize, bool expectedToSucceed) {
	state void* buf =
	    FastAllocator<4096>::allocate(); // we leak this if there is an error, but that shouldn't be a big deal
	state int iteration = 0;

	state bool opTimedOut = false;

	for (; iteration < numIterations; ++iteration) {
		state std::vector<Future<Void>> futures;
		state int numOps = deterministicRandom()->randomInt(1, 20);
		for (; numOps > 0; --numOps) {
			if (deterministicRandom()->coinflip()) {
				futures.push_back(
				    success(f->read(buf, 4096, deterministicRandom()->randomInt(0, fileSize) / 4096 * 4096)));
			} else {
				futures.push_back(f->write(buf, 4096, deterministicRandom()->randomInt(0, fileSize) / 4096 * 4096));
			}
		}
		state int fIndex = 0;
		for (; fIndex < futures.size(); ++fIndex) {
			try {
				wait(futures[fIndex]);
			} catch (Error& e) {
				ASSERT(!expectedToSucceed);
				ASSERT(e.code() == error_code_io_timeout);
				opTimedOut = true;
			}
		}

		try {
			wait(f->sync() && delay(0.1));
			ASSERT(expectedToSucceed);
		} catch (Error& e) {
			ASSERT(!expectedToSucceed && e.code() == error_code_io_timeout);
		}
	}

	FastAllocator<4096>::release(buf);

	ASSERT(expectedToSucceed || opTimedOut);
	return Void();
}

TEST_CASE("/fdbrpc/AsyncFileKAIO/RequestList") {
	// This test does nothing in simulation because simulation doesn't support AsyncFileKAIO
	if (!g_network->isSimulated()) {
		state Reference<IAsyncFile> f;
		try {
			Reference<IAsyncFile> f_ = wait(
			    AsyncFileKAIO::open("/tmp/__KAIO_TEST_FILE__",
			                        IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
			                        0666,
			                        nullptr));
			f = f_;
			state int fileSize = 2 << 27; // ~100MB
			wait(f->truncate(fileSize));

			// Test that the request list works as intended with default timeout
			AsyncFileKAIO::setTimeout(0.0);
			wait(runTestOps(f, 100, fileSize, true));
			ASSERT(!((AsyncFileKAIO*)f.getPtr())->failed);

			// Test that the request list works as intended with long timeout
			AsyncFileKAIO::setTimeout(20.0);
			wait(runTestOps(f, 100, fileSize, true));
			ASSERT(!((AsyncFileKAIO*)f.getPtr())->failed);

			// Test that requests timeout correctly
			AsyncFileKAIO::setTimeout(0.0001);
			wait(runTestOps(f, 10, fileSize, false));
			ASSERT(((AsyncFileKAIO*)f.getPtr())->failed);
		} catch (Error& e) {
			state Error err = e;
			if (f) {
				wait(AsyncFileEIO::deleteFile(f->getFilename(), true));
			}
			throw err;
		}

		wait(AsyncFileEIO::deleteFile(f->getFilename(), true));
	}

	return Void();
}

AsyncFileKAIO::Context AsyncFileKAIO::ctx;

#include "flow/unactorcompiler.h"
#endif
#endif
