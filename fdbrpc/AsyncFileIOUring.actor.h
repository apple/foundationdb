/*
 * AsyncFileIOUring.actor.h
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

#pragma once
#ifdef __linux__

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILEIOUring_ACTOR_G_H)
#define FLOW_ASYNCFILEIOUring_ACTOR_G_H
#include "fdbrpc/AsyncFileIOUring.actor.g.h"
#elif !defined(FLOW_ASYNCFILEIOUring_ACTOR_H)
#define FLOW_ASYNCFILEIOUring_ACTOR_H

#include "fdbrpc/IAsyncFile.h"
#include <fcntl.h>
#include <atomic>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <liburing.h>
#include "flow/Knobs.h"
#include "flow/Histogram.h"
#include "fdbrpc/ContinuousSample.h"
#include "flow/UnitTest.h"
#include <stdio.h>
#include "flow/crc32c.h"
#include "flow/genericactors.actor.h"
#include "fdbrpc/AsyncFileKAIO.actor.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/flow.h"

// Test case
#include <algorithm>
#include <random>

#include "flow/actorcompiler.h" // This must be the last #include.

// Set this to true to enable detailed IOUring request logging, which currently is written to a hardcoded location
// /data/v7/fdb/
#define IOUring_LOGGING 0
#define IOUring_TRACING 0
#define AVOID_STALLS 0

enum { UIO_CMD_PREAD = 0, UIO_CMD_PWRITE = 1, UIO_CMD_FSYNC = 2, UIO_CMD_FDSYNC = 3 };

struct AsyncFileIOUringMetrics {
	Reference<Histogram> readLatencyDist;
	Reference<Histogram> writeLatencyDist;
	Reference<Histogram> syncLatencyDist;
	Reference<Histogram> ioSubmitLatencyDist;
	ContinuousSample<double> writeLatencySamples;
	ContinuousSample<double> ioSubmitLatencySamples;

	AsyncFileIOUringMetrics() : writeLatencySamples(10000), ioSubmitLatencySamples(10000) {}
} g_asyncFileIOUringMetrics;

Future<Void> g_asyncFileIOUringHistogramLogger;

DESCR struct SlowIOUringSubmit {
	int64_t submitDuration; // ns
	int64_t truncateDuration; // ns
	int64_t numTruncates;
	int64_t truncateBytes;
	int64_t largestTruncate;
};

typedef struct io_uring io_uring_t;
class AsyncFileIOUring : public IAsyncFile, public ReferenceCounted<AsyncFileIOUring> {
public:
	static Future<Reference<IAsyncFile>> open(std::string filename, int flags, int mode) {
		if (openFiles.find(filename) == openFiles.end()) {
			auto f = open_impl(filename, flags, mode);
			if (f.isReady() && f.isError())
				return f;
			if (!f.isReady())
				openFiles[filename].opened = f;
			else {
				return f.get();
			}
		}
		return openFiles[filename].get();
	}

	static Future<Reference<IAsyncFile>> open_impl(std::string filename, int flags, int mode) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		/* IOUring doesn't have to be unbuffered */
		// have to be both set or both unset
		ASSERT(0 == ((!!(flags & OPEN_UNBUFFERED)) ^ (!!(flags & OPEN_UNCACHED))));

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
			TraceEvent ev("AsyncFileIOUringOpenFailed");
			ev.error(e)
			    .detail("Filename", filename)
			    .detailf("Flags", "%x", flags)
			    .detailf("OSFlags", "%x", openFlags(flags))
			    .detailf("Mode", "0%o", mode)
			    .GetLastError();
			if (ecode == EINVAL)
				ev.detail("Description", "Invalid argument - Does the target filesystem support IOUring?");
			return e;
		} else {
			TraceEvent("AsyncFileIOUringOpen")
			    .detail("Filename", filename)
			    .detail("Flags", flags)
			    .detail("Mode", mode)
			    .detail("Fd", fd);
		}

		AsyncFileIOUring* const ptr = new AsyncFileIOUring(fd, flags, filename);
		auto& of = openFiles[filename];
		of.f = ptr;
		of.opened = Future<Reference<IAsyncFile>>();

		Reference<AsyncFileIOUring> r(ptr);

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
			TraceEvent("AsyncFileIOUringFStatError").detail("Fd", fd).detail("Filename", filename).GetLastError();
			return io_error();
		}

		r->lastFileSize = r->nextFileSize = buf.st_size;
		if (FLOW_KNOBS->IO_URING_POLL) {
			// TODO: FIX IO_URING_POLL BUG
			int ret __attribute__((unused)) = io_uring_register_files(&ctx.ring, &fd, 1);

			TraceEvent(SevDebug, "AsyncFileIOUringRegisterFile")
			    .detail("Filename", open_filename)
			    .detail("fd", fd)
			    .detail("ret", ret);
		}
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
			ctx.slowAioSubmitMetric.init(LiteralStringRef("AsyncFile.SlowIOUringSubmit"));
		}

		// Initialize io_uring
		int rc;
		if (FLOW_KNOBS->IO_URING_POLL) {
			struct io_uring_params params;
			memset(&params, 0, sizeof(params));
			// params.flags |= IORING_SETUP_SQPOLL;
			params.flags |= (IORING_SETUP_SQPOLL & IORING_SETUP_IOPOLL);
			params.sq_thread_idle = 2000;

			rc = io_uring_queue_init_params(FLOW_KNOBS->MAX_OUTSTANDING, &ctx.ring, &params);
		} else {
			rc = io_uring_queue_init(FLOW_KNOBS->MAX_OUTSTANDING, &ctx.ring, 0);
		}

		if (rc < 0) {
			TraceEvent("IOSetupError").GetLastError();
			printf("Error in iou setup %d %s\n", rc, strerror(-rc));
			throw io_error();
		}

		TraceEvent(SevDebug, "AsyncFileIOUringInit")
		    .detail("rc", rc)
		    .detail("eventfd", ev->getFD())
		    .detail("QD", FLOW_KNOBS->MAX_OUTSTANDING);

		// Register eventfd
		ctx.evfd = ev->getFD();
		io_uring_register_eventfd(&ctx.ring, ctx.evfd);

		// Start poll loop
		poll_batch(ev);

		setTimeout(ioTimeout);
		g_network->setGlobal(INetwork::enRunCycleFunc, (flowGlobalType)&AsyncFileIOUring::launch);
	}

	static int get_eventfd() { return ctx.evfd; }
	static void setTimeout(double ioTimeout) { ctx.setIOTimeout(ioTimeout); }

	void addref() override { ReferenceCounted<AsyncFileIOUring>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileIOUring>::delref(); }

	Future<int> read(void* data, int length, int64_t offset) override {
		++countFileLogicalReads;
		++countLogicalReads;

		if (failed) {
			return io_timeout();
		}

		IOBlock* io = new IOBlock(UIO_CMD_PREAD, fd);
		io->buf = data;
		io->nbytes = length;
		io->offset = offset;

		// We enqueue if > max events have already been pushed to the ring
		if (FLOW_KNOBS->IO_URING_DIRECT_SUBMIT && ctx.submitted < FLOW_KNOBS->MAX_OUTSTANDING) {

			double startT = now();
			struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.ring);
			if (nullptr == sqe) {
				enqueue(io, "read", this);
			} else {
				io->startTime = startT;

				struct iovec* iov = &io->iovec;
				iov->iov_base = io->buf;
				iov->iov_len = io->nbytes;
				if (FLOW_KNOBS->IO_URING_POLL) {
					io_uring_prep_readv(sqe, 0, iov, 1, io->offset);
					io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
				} else {
					io_uring_prep_readv(sqe, io->aio_fildes, iov, 1, io->offset);
				}

				io_uring_sqe_set_data(sqe, io);
				ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || int64_t(io->buf) % 4096 == 0);
				ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || io->offset % 4096 == 0);
				ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || io->nbytes % 4096 == 0);

				io->prio = (int64_t(g_network->getCurrentTask()) << 32) - (++ctx.opsIssued);
				io->owner = Reference<AsyncFileIOUring>::addRef(this);

				int rc = io_uring_submit(&ctx.ring);

				if (rc <= 0) {
					throw io_error();
				} else {
					ctx.submitted++;
				}
			}
		} else {
			enqueue(io, "read", this);
		}
		Future<int> result = io->result.getFuture();
		return result;
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		++countFileLogicalWrites;
		++countLogicalWrites;

		if (failed) {
			return io_timeout();
		}

		IOBlock* io = new IOBlock(UIO_CMD_PWRITE, fd);
		io->buf = (void*)data;
		io->nbytes = length;
		io->offset = offset;

		nextFileSize = std::max(nextFileSize, offset + length);

		if (FLOW_KNOBS->IO_URING_DIRECT_SUBMIT && ctx.submitted < FLOW_KNOBS->MAX_OUTSTANDING) {
			double startT = now();
			struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.ring);
			if (nullptr == sqe) {
				enqueue(io, "write", this);
			} else {
				io->startTime = startT;

				struct iovec* iov = &io->iovec;
				iov->iov_base = io->buf;
				iov->iov_len = io->nbytes;
				if (FLOW_KNOBS->IO_URING_POLL) {
					io_uring_prep_writev(sqe, 0, iov, 1, io->offset);
					io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
				} else {
					io_uring_prep_writev(sqe, io->aio_fildes, iov, 1, io->offset);
				}

				io_uring_sqe_set_data(sqe, io);

				ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || int64_t(io->buf) % 4096 == 0);
				ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || io->offset % 4096 == 0);
				ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || io->nbytes % 4096 == 0);

				io->prio = (int64_t(g_network->getCurrentTask()) << 32) - (++ctx.opsIssued);
				io->owner = Reference<AsyncFileIOUring>::addRef(this);

				if (this->lastFileSize != this->nextFileSize) {
					++ctx.countPreSubmitTruncate;
					int64_t truncateSize = this->nextFileSize - this->lastFileSize;
					ASSERT(truncateSize > 0);
					ctx.preSubmitTruncateBytes += truncateSize;
					this->truncate(io->owner->nextFileSize);
				}
				double start = timer_monotonic();
				int rc = io_uring_submit(&ctx.ring);
				g_asyncFileIOUringMetrics.ioSubmitLatencySamples.addSample((timer_monotonic() - start) * 1000000);

				if (rc <= 0) {
					throw io_error();
				} else {
					ctx.submitted++;
				}
			}
		} else {
			enqueue(io, "write", this);
		}
		Future<int> result = io->result.getFuture();
		return success(result);
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

		int result = -1;

		bool completed = false;
		double begin = timer_monotonic();

		if (ctx.fallocateSupported && size >= lastFileSize) {
			result = fallocate(fd, 0, 0, size);
			if (result != 0) {
				int fallocateErrCode = errno;
				TraceEvent("AsyncFileIOUringAllocateError")
				    .detail("Fd", fd)
				    .detail("Filename", filename)
				    .detail("Size", size)
				    .GetLastError();
				if (fallocateErrCode == EOPNOTSUPP) {
					// Mark fallocate as unsupported. Try again with truncate.
					ctx.fallocateSupported = false;
				} else {

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
			TraceEvent("SlowIOUringTruncate")
			    .detail("TruncateTime", end - begin)
			    .detail("TruncateBytes", size - lastFileSize);
		}

		if (result != 0) {
			TraceEvent("AsyncFileIOUringTruncateError").detail("Fd", fd).detail("Filename", filename).GetLastError();
			return io_error();
		}

		lastFileSize = nextFileSize = size;

		return Void();
	}

	ACTOR static Future<Void> throwErrorIfFailed(Reference<AsyncFileIOUring> self, Future<Void> sync) {
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

		IOBlock* io = new IOBlock(UIO_CMD_FSYNC, fd);
		if (FLOW_KNOBS->IO_URING_DIRECT_SUBMIT && ctx.submitted < FLOW_KNOBS->MAX_OUTSTANDING) {

			double startT = now();
			struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.ring);
			if (nullptr == sqe) {
				enqueue(io, "fsync", this);
			} else {
				io->startTime = startT;
				if (FLOW_KNOBS->IO_URING_POLL) {
					io_uring_prep_fsync(sqe, 0, 0);
					io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
				} else {
					io_uring_prep_fsync(sqe, io->aio_fildes, 0);
				}

				io_uring_sqe_set_data(sqe, io);

				io->prio = (int64_t(g_network->getCurrentTask()) << 32) - (++ctx.opsIssued);
				io->owner = Reference<AsyncFileIOUring>::addRef(this);

				int rc = io_uring_submit(&ctx.ring);
				if (rc <= 0) {
					throw io_error();
				} else {
					ctx.submitted++;
				}
			}

		} else {
			enqueue(io, "fsync", this);
		}
		Future<Void> fsync = success(io->result.getFuture());

		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;
			return AsyncFileEIO::waitAndAtomicRename(fsync, filename + ".part", filename);
		}
		return fsync;
	}
	Future<int64_t> size() const override { return nextFileSize; }
	int64_t debugFD() const override { return fd; }
	std::string getFilename() const override { return filename; }
	~AsyncFileIOUring() {
		close(fd);

		// TODO: unregister fixed file?
	}

	static void launch() {

		// We enter the loop if: 1) there's stuff to push and we can submit at least MIN_SUBMIT without overflowing
		// MAX_OUTSTANDING
		if (!(ctx.queue.size() && ctx.submitted < FLOW_KNOBS->MAX_OUTSTANDING - FLOW_KNOBS->MIN_SUBMIT))
			return;
		// W.r.t. KAIO, We don't enforce any min_submit. This reduces the variance in performance
		// We want to call "submit" if we have stuff in the ctx queue or in the ring queue
		int to_push = ctx.queue.size();
		if (to_push > 0) {
			// Cannot have more than a max of ops in the ring
			if (to_push + ctx.submitted > FLOW_KNOBS->MAX_OUTSTANDING)
				to_push = FLOW_KNOBS->MAX_OUTSTANDING - ctx.submitted;

			if (!to_push) {
				// The ring is full with submitted ops.
				return;
			}
			ctx.submitMetric = true;

			double begin = timer_monotonic();
			if (!ctx.submitted)
				ctx.ioStallBegin = begin;

			int64_t previousTruncateCount = ctx.countPreSubmitTruncate;
			int64_t previousTruncateBytes = ctx.preSubmitTruncateBytes;
			int64_t largestTruncate = 0;
			int i = 0;
			for (; i < to_push; i++) {
				auto io = ctx.queue.top();
				double startT = now();
				struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.ring);
				io->startTime = startT;

				switch (io->opcode) {
				case UIO_CMD_PREAD: {

					struct iovec* iov = &io->iovec;
					iov->iov_base = io->buf;
					iov->iov_len = io->nbytes;

					if (FLOW_KNOBS->IO_URING_POLL)
						io_uring_prep_readv(sqe, 0, iov, 1, io->offset);
					else
						io_uring_prep_readv(sqe, io->aio_fildes, iov, 1, io->offset);

					break;
				}
				case UIO_CMD_PWRITE: {

					struct iovec* iov = &io->iovec;
					iov->iov_base = io->buf;
					iov->iov_len = io->nbytes;
					if (FLOW_KNOBS->IO_URING_POLL)
						io_uring_prep_writev(sqe, 0, iov, 1, io->offset);
					else
						io_uring_prep_writev(sqe, io->aio_fildes, iov, 1, io->offset);

					break;
				}
				case UIO_CMD_FSYNC:
					if (FLOW_KNOBS->IO_URING_POLL)
						io_uring_prep_fsync(sqe, 0, 0);
					else
						io_uring_prep_fsync(sqe, io->aio_fildes, 0);

					break;
				default:
					UNSTOPPABLE_ASSERT(false);
				}
				ctx.queue.pop();

				if (FLOW_KNOBS->IO_URING_POLL)
					io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);

				io_uring_sqe_set_data(sqe, io);

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
			int rc = io_uring_submit(&ctx.ring);
			double end = timer_monotonic();
			if (rc <= 0 || rc < i) {
				// Error if rc<0 or if it undersubmits w.r.t. what we want to push
				printf("io_uring_submit error %d %s\n", rc, strerror(-rc));
				// If error is EAGAIN maybe we could just loop ocer and retry, but for now we crash on error
				// It is not clear how io_uring handles cqes that are prepared but not pushed
				throw io_error();
			}

			g_asyncFileIOUringMetrics.ioSubmitLatencyDist->sampleSeconds(end - truncateComplete);
			g_asyncFileIOUringMetrics.ioSubmitLatencySamples.addSample((end - truncateComplete) * 1000000);

			if (end - begin > FLOW_KNOBS->SLOW_LOOP_CUTOFF) {
				ctx.slowAioSubmitMetric->submitDuration = end - truncateComplete;
				ctx.slowAioSubmitMetric->truncateDuration = truncateComplete - begin;
				ctx.slowAioSubmitMetric->numTruncates = ctx.countPreSubmitTruncate - previousTruncateCount;
				ctx.slowAioSubmitMetric->truncateBytes = ctx.preSubmitTruncateBytes - previousTruncateBytes;
				ctx.slowAioSubmitMetric->largestTruncate = largestTruncate;
				ctx.slowAioSubmitMetric->log();

				if (nondeterministicRandom()->random01() < end - begin) {
					TraceEvent("SlowIOUringLaunch")
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
			ctx.submitted += rc;
		}
	}

	bool failed;

	static void getIOSubmitMetrics() {

		auto& metrics = g_asyncFileIOUringMetrics;

		printf("\n=== Write Latencies (us) === \n");
		printf("Mean Latency: %f\n", metrics.writeLatencySamples.mean());
		printf("Median Latency: %f\n", metrics.writeLatencySamples.median());
		printf("95%% Latency: %f\n", metrics.writeLatencySamples.percentile(0.95));
		printf("99%% Latency: %f\n", metrics.writeLatencySamples.percentile(0.99));
		printf("99.9%% Latency: %f\n", metrics.writeLatencySamples.percentile(0.999));

		printf("\n === io_submit Latencies (us) === \n");
		printf("Mean Latency: %f\n", metrics.ioSubmitLatencySamples.mean());
		printf("Median Latency: %f\n", metrics.ioSubmitLatencySamples.median());
		printf("95%% Latency: %f\n", metrics.ioSubmitLatencySamples.percentile(0.95));
		printf("99%% Latency: %f\n", metrics.ioSubmitLatencySamples.percentile(0.99));
		printf("99.9%% Latency: %f\n", metrics.ioSubmitLatencySamples.percentile(0.999));
	}

private:
	struct OpenFileInfo;
	static std::map<std::string, OpenFileInfo> openFiles;
	int fd, flags;
	int64_t lastFileSize, nextFileSize;
	std::string filename;
	Int64MetricHandle countFileLogicalWrites;
	Int64MetricHandle countFileLogicalReads;

	Int64MetricHandle countLogicalWrites;
	Int64MetricHandle countLogicalReads;

	struct OpenFileInfo : NonCopyable {
		IAsyncFile* f;
		Future<Reference<IAsyncFile>> opened; // Only valid until the file is fully opened

		OpenFileInfo() : f(0) {}
		OpenFileInfo(OpenFileInfo&& r) noexcept : f(r.f), opened(std::move(r.opened)) { r.f = 0; }

		Future<Reference<IAsyncFile>> get() {
			if (f)
				return Reference<IAsyncFile>::addRef(f);
			else
				return opened;
		}
	};

	struct IOBlock : FastAllocated<IOBlock> {
		Promise<int> result;
		Reference<AsyncFileIOUring> owner;
		int opcode;
		int64_t prio;
		int aio_fildes;
		void* buf;
		int64_t nbytes;
		int64_t offset;
		IOBlock* prev;
		IOBlock* next;
		struct iovec iovec;
		double startTime;
		// int buffer_index;
		// struct io_uring_cqe *batch_cqes[1024];
		struct indirect_order_by_priority {
			bool operator()(IOBlock* a, IOBlock* b) { return a->prio < b->prio; }
		};

		IOBlock(int op, int fd) : opcode(op), prio(0), aio_fildes(fd), buf(nullptr), nbytes(0), offset(0) {}

		TaskPriority getTask() const { return static_cast<TaskPriority>((prio >> 32) + 1); }

		static void deliver(Promise<int> result, bool failed, int r, TaskPriority task) {

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
				TraceEvent("AsyncFileIOUringIOError")
				    .GetLastError()
				    .detail("Fd", aio_fildes)
				    .detail("Op", opcode)
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
			TraceEvent(SevWarnAlways, "AsyncFileIOUringTimeout")
			    .detail("Fd", aio_fildes)
			    .detail("Op", opcode)
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
		std::thread thr;
		Promise<int> promise;
		Promise<int> waitPromise;
		struct io_uring_cqe* cqe;
		io_uring_t ring;
		int evfd;
		int outstanding;

		int submitted; // TODO if not eventfd this is going to brea
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

		EventMetricHandle<SlowIOUringSubmit> slowAioSubmitMetric;
		struct iovec* fixed_buffers;
		int* buffers_indices;
		int *buffer_head, *buffer_tail;
		struct io_uring_cqe* cqes_batch[1024];

		uint32_t opsIssued;

		Context()
		  : ring(), evfd(-1), outstanding(0), submitted(0), ioStallBegin(0), fallocateSupported(true),
		    fallocateZeroSupported(true), submittedRequestList(nullptr), opsIssued(0) {
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

	explicit AsyncFileIOUring(int fd, int flags, std::string const& filename)
	  : failed(false), fd(fd), flags(flags), filename(filename) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		if (!g_network->isSimulated()) {
			countFileLogicalWrites.init(LiteralStringRef("AsyncFile.CountFileLogicalWrites"), filename);
			countFileLogicalReads.init(LiteralStringRef("AsyncFile.CountFileLogicalReads"), filename);
			countLogicalWrites.init(LiteralStringRef("AsyncFile.CountLogicalWrites"));
			countLogicalReads.init(LiteralStringRef("AsyncFile.CountLogicalReads"));

			if (!g_asyncFileIOUringHistogramLogger.isValid()) {
				auto& metrics = g_asyncFileIOUringMetrics;
				metrics.readLatencyDist = Reference<Histogram>(new Histogram(
				    Reference<HistogramRegistry>(), "AsyncFileIOUring", "ReadLatency", Histogram::Unit::microseconds));
				metrics.writeLatencyDist = Reference<Histogram>(new Histogram(
				    Reference<HistogramRegistry>(), "AsyncFileIOUring", "WriteLatency", Histogram::Unit::microseconds));
				metrics.syncLatencyDist = Reference<Histogram>(new Histogram(
				    Reference<HistogramRegistry>(), "AsyncFileIOUring", "SyncLatency", Histogram::Unit::microseconds));
				metrics.ioSubmitLatencyDist = Reference<Histogram>(new Histogram(Reference<HistogramRegistry>(),
				                                                                 "AsyncFileIOUring",
				                                                                 "IoSubmitLatency",
				                                                                 Histogram::Unit::microseconds));
				g_asyncFileIOUringHistogramLogger = histogramLogger(SERVER_KNOBS->DISK_METRIC_LOGGING_INTERVAL);
			}
		}
	}

	void enqueue(IOBlock* io, const char* op, AsyncFileIOUring* owner) {
		if (io->opcode != UIO_CMD_FSYNC) {
			ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || int64_t(io->buf) % 4096 == 0);
			ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || io->offset % 4096 == 0);
			ASSERT(!bool(flags & IAsyncFile::OPEN_UNBUFFERED) || io->nbytes % 4096 == 0);
		}

		io->prio = (int64_t(g_network->getCurrentTask()) << 32) - (++ctx.opsIssued);

		io->owner = Reference<AsyncFileIOUring>::addRef(owner);

		ctx.queue.push(io);
	}

	static int openFlags(int flags) {
		int oflags = O_CLOEXEC;
		if (flags & OPEN_UNBUFFERED)
			oflags |= O_DIRECT;
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

	ACTOR static void poll_batch(Reference<IEventFD> ev) {
		state int rc = 0;
		state io_uring_cqe* cqe;
		state int64_t to_consume;
		state int r = 0;
		loop {

			int64_t ev_r = wait(ev->read());
			to_consume = ev_r;
			wait(delay(0, TaskPriority::DiskIOComplete));

			rc = io_uring_peek_batch_cqe(&ctx.ring, ctx.cqes_batch, to_consume);
			if (rc < to_consume) {
				TraceEvent("IOGetEventsError").GetLastError();
				throw io_error();
			}
			int e = 0;
			for (; e < to_consume; e++) {
				struct io_uring_cqe* cqe = ctx.cqes_batch[e];
				IOBlock* const iob = static_cast<IOBlock*>(io_uring_cqe_get_data(cqe));
				ASSERT(iob != nullptr);

				if (ctx.ioTimeout > 0 && !AVOID_STALLS) {
					ctx.removeFromRequestList(iob);
				}
				// printf("Op result %d %s\n", cqe->res, strerror(-cqe->res));

				auto& metrics = g_asyncFileIOUringMetrics;
				switch (iob->opcode) {
				case UIO_CMD_PREAD:
					metrics.readLatencyDist->sampleSeconds(now() - iob->startTime);
					break;
				case UIO_CMD_PWRITE:
					metrics.writeLatencyDist->sampleSeconds(now() - iob->startTime);
					metrics.writeLatencySamples.addSample((now() - iob->startTime) * 1000000);
					break;
				case UIO_CMD_FSYNC:
					metrics.syncLatencyDist->sampleSeconds(now() - iob->startTime);
					break;
				}

				iob->setResult(cqe->res);
				io_uring_cqe_seen(&ctx.ring, cqe);
			}

			if (1) {

				{
					++ctx.countAIOCollect;
					double t = timer_monotonic();
					double elapsed = t - ctx.ioStallBegin;
					ctx.ioStallBegin = t;
					g_network->networkInfo.metrics.secSquaredDiskStall += elapsed * elapsed / 2;
				}

				if (ctx.ioTimeout > 0 && !AVOID_STALLS) {
					double currentTime = now();
					while (ctx.submittedRequestList &&
					       currentTime - ctx.submittedRequestList->startTime > ctx.ioTimeout) {
						ctx.submittedRequestList->timeout(ctx.timeoutWarnOnly);
						ctx.removeFromRequestList(ctx.submittedRequestList);
					}
				}
				ctx.submitted -= to_consume;
			}
		}
	}

	ACTOR static Future<Void> histogramLogger(double interval) {
		state double currentTime;
		loop {
			currentTime = now();
			wait(delay(interval));
			double elapsed = now() - currentTime;
			auto& metrics = g_asyncFileIOUringMetrics;
			metrics.readLatencyDist->writeToLog(elapsed);
			metrics.writeLatencyDist->writeToLog(elapsed);
			metrics.syncLatencyDist->writeToLog(elapsed);
			metrics.ioSubmitLatencyDist->writeToLog(elapsed);
		}
	}
};

ACTOR Future<Void> runTestIOUringOps(Reference<IAsyncFile> f, int numIterations, int fileSize, bool expectedToSucceed) {
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

TEST_CASE("/fdbrpc/AsyncFileIOUring/RequestList") {
	// This test does nothing in simulation because simulation doesn't support AsyncFileIOUring
	if (!g_network->isSimulated()) {
		state Reference<IAsyncFile> f;
		try {
			Reference<IAsyncFile> f_ = wait(AsyncFileIOUring::open(
			    "/tmp/__IOUring_TEST_FILE__",
			    IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
			    0666));
			f = f_;
			state int fileSize = 2 << 27; // ~100MB
			wait(f->truncate(fileSize));

			// Test that the request list works as intended with default timeout
			AsyncFileIOUring::setTimeout(0.0);
			wait(runTestIOUringOps(f, 100, fileSize, true));
			ASSERT(!((AsyncFileIOUring*)f.getPtr())->failed);

			// Test that the request list works as intended with long timeout
			AsyncFileIOUring::setTimeout(20.0);
			wait(runTestIOUringOps(f, 100, fileSize, true));
			ASSERT(!((AsyncFileIOUring*)f.getPtr())->failed);

			// Test that requests timeout correctly
			AsyncFileIOUring::setTimeout(0.0001);
			wait(runTestIOUringOps(f, 10, fileSize, false));
			ASSERT(((AsyncFileIOUring*)f.getPtr())->failed);
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

ACTOR Future<double> writeBlock(int i, Reference<IAsyncFile> f, void* buf) {

	state double startTime = now();

	wait(f->write(buf, 4096, i * 4096));

	return now() - startTime;
}

TEST_CASE("/fdbrpc/AsyncFileIOUring/CallbackTest") {

	state int num_blocks = params.getInt("num_blocks").orDefault(1000);
	state std::string file_path = params.get("file_path").orDefault("");

	printf("num_blocks: %d\n", num_blocks);
	printf("file_path: %s\n", file_path.c_str());

	ASSERT(!file_path.empty());

	state Reference<IAsyncFile> f;
	try {
		Reference<IAsyncFile> f_ = wait(IAsyncFileSystem::filesystem()->open(
		    file_path, IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE, 0666));

		f = f_;

		state void* buf = FastAllocator<4096>::allocate();

		state std::vector<Future<double>> futures;
		for (int i = 0; i < num_blocks; ++i) {
			futures.push_back(writeBlock(i, f, buf));
		}

		state double sum = 0.0;
		state int i = 0;
		for (; i < num_blocks; ++i) {
			double time = wait(futures.at(i));
			sum += time;
		}

		FastAllocator<4096>::release(buf);

		printf("avg: %f\n", sum / num_blocks);

	} catch (Error& e) {
		state Error err = e;
		if (f) {
			wait(AsyncFileEIO::deleteFile(f->getFilename(), true));
		}

		throw err;
	}

	wait(AsyncFileEIO::deleteFile(f->getFilename(), true));

	return Void();
}

TEST_CASE("/fdbrpc/AsyncFileIOUring/metadata") {
	state std::string file_path = params.get("file_path").orDefault("");
	state int num_blocks = params.getInt("num_blocks").orDefault(1000);
	state int seed = params.getInt("seed").orDefault(getpid());
	state int fsync = params.getInt("fsync").orDefault(1);

	printf("Writing %d blocks to %s, using seed %d.\n", num_blocks, file_path.c_str(), seed);
	printf("Backend: %s\n", FLOW_KNOBS->ENABLE_IO_URING ? "io_uring" : "kaio");

	// Open file
	state Reference<IAsyncFile> f;

	Reference<IAsyncFile> f_ = wait(IAsyncFileSystem::filesystem()->open(
	    file_path,
	    IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
	    0666));

	f = f_;

	// Allocate buffer
	state char* buf = (char*)FastAllocator<4096>::allocate();

	for (int i = 0; i < 4096; i++) {
		buf[i] = 'A' + (i % 26);
	}

	// Randomize indexes
	state std::vector<int> indexes(num_blocks); // vector with 100 ints.
	std::iota(std::begin(indexes), std::end(indexes), 0);

	auto rng = std::default_random_engine{};
	rng.seed(seed);
	std::shuffle(std::begin(indexes), std::end(indexes), rng);

	// Write blocks
	state int i;
	for (i = 0; i < indexes.size(); ++i) {
		wait(f->write(buf, 4096, indexes.at(i) * 4096));
		if (i % fsync == 0)
			wait(f->sync());
	}

	// wait(f->sync());

	// Printing stats
	if (FLOW_KNOBS->ENABLE_IO_URING)
		AsyncFileIOUring::getIOSubmitMetrics();
	else
		AsyncFileKAIO::getIOSubmitMetrics();

	// Clean up
	FastAllocator<4096>::release(buf);

	wait(AsyncFileEIO::deleteFile(f->getFilename(), true));

	return Void();
}

AsyncFileIOUring::Context AsyncFileIOUring::ctx;
std::map<std::string, AsyncFileIOUring::OpenFileInfo> AsyncFileIOUring::openFiles;

#include "flow/unactorcompiler.h"
#endif
#endif
