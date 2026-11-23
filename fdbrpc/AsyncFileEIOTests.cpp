/*
 * AsyncFileEIOTests.cpp
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

#include "flow/Platform.h"

void forceLinkAsyncFileEIOTests() {}

#if defined(__unixish__)

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unistd.h>

#include "AsyncFileEIO.h"
#include "fdbrpc/Net2FileSystem.h"
#include "flow/UnitTest.h"

namespace {

struct EIOBlockingJob {
	std::mutex mutex;
	std::condition_variable condition;
	std::atomic<bool> started{ false };
	std::atomic<bool> finished{ false };
	bool released = false;

	void release() {
		std::lock_guard<std::mutex> lock(mutex);
		released = true;
		condition.notify_all();
	}

	Void run() {
		std::unique_lock<std::mutex> lock(mutex);
		started.store(true);
		// A failing test must never leave an EIO worker blocked indefinitely.
		bool wasReleased = condition.wait_for(lock, std::chrono::seconds(5), [this] { return released; });
		finished.store(true);
		if (!wasReleased)
			throw timed_out();
		return Void();
	}
};

struct EIOTestDirectory {
	explicit EIOTestDirectory(std::string path) : path(std::move(path)) { platform::createDirectory(this->path); }
	~EIOTestDirectory() {
		try {
			platform::eraseDirectoryRecursive(path);
		} catch (Error&) {
			// Preserve the original test error if cleanup also fails.
		}
	}
	std::string path;
};

Future<Void> waitForEIOJobStart(std::shared_ptr<EIOBlockingJob> job) {
	while (!job->started.load()) {
		co_await delay(0.001);
	}
	co_return;
}

Future<Void> waitForEIOJobDestruction(std::weak_ptr<EIOBlockingJob> job) {
	while (!job.expired()) {
		co_await delay(0.001);
	}
	co_return;
}

// These tests run in isolation. Drain cancelled operations before their temporary directories are removed.
Future<Void> waitForEIOIdle() {
	while (eio_nreqs() != 0) {
		co_await delay(0.001);
	}
	co_return;
}

// EIO serves equal-priority requests in order. Queue one blocker per configured slot.
// If the library starts fewer workers, the extra blockers can stay queued until release.
struct EIOBlockedPool {
	std::shared_ptr<EIOBlockingJob> blocker = std::make_shared<EIOBlockingJob>();
	std::vector<Future<Void>> operations;

	EIOBlockedPool() {
		ASSERT_GT(FLOW_KNOBS->EIO_MAX_PARALLELISM, 0);
		for (int i = 0; i < FLOW_KNOBS->EIO_MAX_PARALLELISM; ++i) {
			operations.push_back(AsyncFileEIO::dispatch<Void>([job = blocker] { return job->run(); }));
		}
	}
	~EIOBlockedPool() { release(); }

	void release() { blocker->release(); }
	Future<Void> done() { return waitForAll(operations); }
};

} // namespace

TEST_CASE("noSim/fileio/eio/cancelBlockedDispatch") {
	if (g_network->isSimulated())
		co_return;
	noUnseed = true;

	std::shared_ptr<EIOBlockingJob> job = std::make_shared<EIOBlockingJob>();
	std::weak_ptr<EIOBlockingJob> lifetime = job;
	Future<Void> operation = AsyncFileEIO::dispatch<Void>([job] { return job->run(); });
	co_await timeoutError(waitForEIOJobStart(job), 5.0);
	operation.cancel();
	ASSERT(operation.isError() && operation.getError().code() == error_code_actor_cancelled);
	job.reset();

	// The Flow timer must run while the syscall stand-in is still blocked, and cancellation must not free its job.
	co_await delay(0.01);
	{
		auto running = lifetime.lock();
		ASSERT(running);
		ASSERT(!running->finished.load());
		running->release();
	}
	co_await timeoutError(waitForEIOJobDestruction(lifetime), 5.0);
	co_return;
}

TEST_CASE("noSim/fileio/eio/atomicRenameOrdering") {
	if (g_network->isSimulated())
		co_return;
	noUnseed = true;

	std::unique_ptr<EIOTestDirectory> directory = std::make_unique<EIOTestDirectory>(
	    joinPath(params.getDataDir(), "eio-" + deterministicRandom()->randomUniqueID().toString()));
	std::string part = joinPath(directory->path, "backup.part");
	std::string final = joinPath(directory->path, "backup");
	Reference<IAsyncFile> file = co_await AsyncFileEIO::open(
	    part, IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED, 0600, nullptr);
	co_await file->write("backup", 6, 0);
	co_await file->sync();
	file.clear();

	Promise<Void> dataSynced;
	Future<Void> finished = AsyncFileEIO::waitAndAtomicRename(dataSynced.getFuture(), part, final, true);
	co_await delay(0.01);
	ASSERT(!finished.isReady());
	ASSERT(fileExists(part));
	ASSERT(!fileExists(final));
	TaskPriority callerPriority = g_network->getCurrentTask();
	dataSynced.send(Void());
	co_await timeoutError(finished, 5.0);
	ASSERT(g_network->getCurrentTask() == callerPriority);
	ASSERT(!fileExists(part));
	ASSERT(fileExists(final));
	ASSERT_EQ(fileSize(final), 6);

	// A failed data sync must leave the source untouched. A missing rename source retains the existing io_error API.
	Promise<Void> failedSync;
	Future<Void> failed = AsyncFileEIO::waitAndAtomicRename(failedSync.getFuture(), final, part, true);
	failedSync.sendError(io_error());
	try {
		co_await failed;
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_io_error);
	}
	ASSERT(fileExists(final));
	ASSERT(!fileExists(part));
	try {
		co_await timeoutError(AsyncFileEIO::waitAndAtomicRename(Void(), part, final, true), 5.0);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_io_error);
	}
	co_return;
}

TEST_CASE("noSim/fileio/eio/storageAtomicRenameCancellation") {
	if (g_network->isSimulated())
		co_return;
	noUnseed = true;

	std::unique_ptr<EIOTestDirectory> directory = std::make_unique<EIOTestDirectory>(
	    joinPath(params.getDataDir(), "eio-" + deterministicRandom()->randomUniqueID().toString()));
	std::string part = joinPath(directory->path, "storage.part");
	std::string final = joinPath(directory->path, "storage");
	Reference<IAsyncFile> file = co_await AsyncFileEIO::open(
	    part, IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED, 0600, nullptr);
	co_await file->sync();
	file.clear();

	EIOBlockedPool pool;
	Promise<Void> dataSynced;
	// Storage callers, including KAIO, use the default synchronous rename.
	Future<Void> finished = AsyncFileEIO::waitAndAtomicRename(dataSynced.getFuture(), part, final);
	dataSynced.send(Void());
	ASSERT(!finished.isReady()); // Parent directory sync is queued behind the blocked workers.
	ASSERT(!fileExists(part));
	ASSERT(fileExists(final)); // Rename must already be complete before cancellation/disposal can run.
	finished.cancel();
	::deleteFile(final);
	pool.release();
	co_await timeoutError(pool.done(), 5.0);
	co_await timeoutError(waitForEIOIdle(), 5.0);
	ASSERT(!fileExists(final));
	ASSERT(!fileExists(part));
	co_return;
}

TEST_CASE("noSim/fileio/eio/backupAtomicRenameUsesEIO") {
	if (g_network->isSimulated())
		co_return;
	noUnseed = true;

	std::unique_ptr<EIOTestDirectory> directory = std::make_unique<EIOTestDirectory>(
	    joinPath(params.getDataDir(), "eio-" + deterministicRandom()->randomUniqueID().toString()));
	std::string part = joinPath(directory->path, "backup.temp.part");
	std::string final = joinPath(directory->path, "backup.temp");
	Reference<IAsyncFile> file = co_await AsyncFileEIO::open(
	    part, IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED, 0600, nullptr);
	co_await file->sync();
	file.clear();

	EIOBlockedPool pool;
	Future<Void> finished = AsyncFileEIO::waitAndAtomicRename(Void(), part, final, true);
	co_await delay(0.01);
	ASSERT(!finished.isReady());
	ASSERT(fileExists(part));
	ASSERT(!fileExists(final)); // Opted-in backup renames wait for an EIO worker.
	pool.release();
	co_await timeoutError(pool.done(), 5.0);
	co_await timeoutError(finished, 5.0);
	ASSERT(!fileExists(part));
	ASSERT(fileExists(final));
	co_return;
}

TEST_CASE("noSim/fileio/eio/prepareBackupPath") {
	if (g_network->isSimulated())
		co_return;
	noUnseed = true;

	std::unique_ptr<EIOTestDirectory> directory = std::make_unique<EIOTestDirectory>(
	    joinPath(params.getDataDir(), "eio-" + deterministicRandom()->randomUniqueID().toString()));
	std::string real = joinPath(directory->path, "real");
	std::string link = joinPath(directory->path, "link");
	platform::createDirectory(real);
	ASSERT_EQ(::symlink("real", link.c_str()), 0);
	std::string filename = joinPath(link, "missing/child/backup");
	ASSERT(!directoryExists(joinPath(real, "missing/child")));
	EIOBlockedPool pool;
	Future<Void> prepare = Net2FileSystem::createParentDirectory(filename);
	ASSERT(!prepare.isReady());
	prepare.cancel();
	pool.release();
	co_await timeoutError(pool.done(), 5.0);
	co_await timeoutError(waitForEIOIdle(), 5.0);
	ASSERT(directoryExists(joinPath(real, "missing/child")));
	ASSERT(!fileExists(filename));
	// Repeated preparation must work for an existing directory too.
	co_await timeoutError(Net2FileSystem::createParentDirectory(filename), 5.0);
	ASSERT_EQ(::unlink(link.c_str()), 0);
	co_return;
}

#endif
