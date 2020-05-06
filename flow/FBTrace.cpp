/*
 * FBTrace.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "flow/FBTrace.h"
#include "flow/FileTraceLogWriter.h"
#include "flow/FastAlloc.h"
#include "flow/FileIdentifier.h"
#include "flow/IThreadPool.h"
#include "flow/Platform.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Trace.h"
#include "flow/network.h"
#include "flow/serialize.h"
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

struct TheChunkAllocator;

//  A special allocator that quickly allocates small objects
// and deallocates them roughly in the same order
struct Chunk {
	friend class TheChunkAllocator;
	// we'll use 1MB chunks
	static constexpr size_t size = ChunkAllocatorImpl::MAX_CHUNK_SIZE;
	//  mutable because it is thread safe
	// atomic because we ship these into
	// a thread pool
	mutable std::atomic<unsigned> refCount = 1;

	TheChunkAllocator& theAllocator;
	size_t freeSpace = size;

	constexpr static size_t beginOffset() {
		return sizeof(Chunk) % 8 == 0 ? sizeof(Chunk) : sizeof(Chunk) + (8 - (sizeof(Chunk) % 8));
	}
	void delref();
	void addref() { refCount.fetch_and(1); }

	static void* ptr_add(void* ptr, size_t offset) { return reinterpret_cast<uint8_t*>(ptr) + offset; }

	void* begin() const { return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(this) + Chunk::beginOffset()); }
	void* end() const { return reinterpret_cast<uint8_t*>(begin()) + (Chunk::size - freeSpace); }

	uint32_t calcOffset(void* ptr) {
		return uint32_t(reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(this));
	}
	static Chunk& getChunk(void* ptr) {
		auto addr = reinterpret_cast<uintptr_t>(ptr) - 4;
		unsigned offset = *reinterpret_cast<unsigned*>(addr);
		return *reinterpret_cast<Chunk*>(addr - offset);
	}

	void* allocate(size_t sz) {
		void* res = begin();
		res = std::align(4, sizeof(uint32_t), res, freeSpace);
		if (res == nullptr) {
			return nullptr;
		}
		if (sz > 16) {
			res = std::align(16, sz, res, freeSpace);
		} else if (sz > 8) {
			res = std::align(8, sz, res, freeSpace);
		}
		if (res == nullptr) {
			return nullptr;
		}
		auto offPtr = ptr_add(res, -4);
		*reinterpret_cast<uint32_t*>(offPtr) = calcOffset(offPtr);
		return res;
	}

private:
	// make sure nobody constructs a Chunk directly
	Chunk(TheChunkAllocator& theAllocator) : theAllocator(theAllocator) {}
};

struct ChunkDeleter {
	void operator()(Chunk* ptr) const { ptr->delref(); }
};

struct TheChunkAllocator {
	static constexpr size_t MAX_FREE_LIST_SIZE = 2;
	std::vector<Chunk*> freeList;
	std::mutex freeListMutex;
	std::unique_ptr<Chunk, ChunkDeleter> currentChunk;

	Chunk* createChunk() {
		void* buffer = aligned_alloc(8, Chunk::size + Chunk::beginOffset());
		return new (buffer) Chunk{ *this };
	}

	void freeChunk(Chunk* c) {
		c->~Chunk();
		aligned_free(c);
	}

	TheChunkAllocator() : currentChunk(createChunk()) {}

	void* allocate(size_t sz) {
		if (sz > ChunkAllocatorImpl::MAX_CHUNK_SIZE) {
			auto res = reinterpret_cast<uint32_t*>(aligned_alloc(8, sz));
			// I don't think this is necessary, but it help to debug because now this
			// means that the 8 bytes before will be 0x00000000ffffffff
			res[0] = 0;
			res[1] = std::numeric_limits<uint32_t>::max();
			return res + 2;
		}
		void* res = nullptr;
		if ((res = currentChunk->allocate(sz)) == nullptr) {
			currentChunk.reset(createChunk());
		}
		return currentChunk->allocate(sz);
	}

	void free(void* ptr) {
		auto i = reinterpret_cast<uint32_t*>(ptr);
		auto off = *(i - 1);
		if (off == std::numeric_limits<uint32_t>::max()) {
			aligned_free(i - 2);
		} else {
			auto addr = reinterpret_cast<std::ptrdiff_t>(ptr);
			reinterpret_cast<Chunk*>(addr - off)->delref();
		}
		std::unique_lock<std::mutex> _{ freeListMutex };
		if (freeList.size() > MAX_FREE_LIST_SIZE) {
			freeChunk(freeList.back());
			freeList.pop_back();
		}
	}
};

void Chunk::delref() {
	if (refCount.fetch_sub(1) == 1) {
		freeSpace = Chunk::size;
		std::unique_lock<std::mutex> _{ theAllocator.freeListMutex };
		theAllocator.freeList.push_back(this);
	}
}

bool mainThreadIsRunning = true;
struct MainThreadRunning {
	~MainThreadRunning() { mainThreadIsRunning = false; }
};
MainThreadRunning _mainThreadRunningHelper;

class FBFactoryState {
	using Lock = std::unique_lock<std::mutex>;
	FBFactoryState(FBFactoryState const&) = delete;
	FBFactoryState(FBFactoryState&&) = delete;
	FBFactoryState& operator=(FBFactoryState const&) = delete;
	FBFactoryState& operator=(FBFactoryState&&) = delete;
	std::mutex mutex;
	std::unordered_map<FileIdentifier, FBFactory*> factories;

public:
	FBFactoryState() {} // has to be public for std::make_shared

	void addFactory(FileIdentifier fId, FBFactory* f) {
		Lock _{ mutex };
		ASSERT(factories.emplace(fId, f).second);
	}

	static FBFactoryState& instance() {
		static std::mutex constructionMutex;
		static std::shared_ptr<FBFactoryState> myInstance;
		// this pointer makes sure that if the main thread has already
		// gone down, any other thread will still have a valid copy
		static thread_local std::shared_ptr<FBFactoryState> this_copy;
		if (this_copy) {
			return *this_copy;
		}
		// we can't construct anymore of the main thread shut down
		// throw an error instead - alternative is undefined behavior
		ASSERT(mainThreadIsRunning);
		if (myInstance) {
			this_copy = myInstance;
		} else {
			Lock _(constructionMutex);
			if (!myInstance) {
				// this means we will leak this memory
				// This prevents problems with multiple
				// threads shutting down
				myInstance = std::make_shared<FBFactoryState>();
			}
		}
		this_copy = myInstance;
		return *this_copy;
	}
};

TheChunkAllocator chunkAllocator;

template<class T, class U>
struct ZeroEstimateAction : ChunkAllocated<U>, TypedAction<T, U> {
	double getTimeEstimate() override { return 0; }
};

struct WriterThread : IThreadPoolReceiver {
	Reference<ITraceLogWriter> logWriter;
	unsigned rollsize;
	explicit WriterThread(Reference<ITraceLogWriter> logWriter, unsigned rollsize)
		: logWriter(logWriter), rollsize(rollsize)
	{}
	void init() override {}
	struct Open : ZeroEstimateAction<WriterThread, Open> {};
	void action(Open&) {
		logWriter->open();
	}
	struct Close : ZeroEstimateAction<WriterThread, Close> {};
	void action(Close&) {
		logWriter->close();
	}
	struct Role : ZeroEstimateAction<WriterThread, Role> {};
	void action(Role&) {
		logWriter->roll();
	}
	struct Write : ZeroEstimateAction<WriterThread, Write> {
		Reference<FBTraceImpl> msg;
		explicit Write(Reference<FBTraceImpl> const& msg) : msg(msg) {}
	};
	void action(Write& w) {
		ObjectWriter writer(Unversioned());
		w.msg->write(writer);
		logWriter->write(writer.toStringRef());
	}
};

using namespace std::literals;

struct FBTraceLog {
	using Clock = std::chrono::steady_clock;
	std::string directory;
	std::string processName;
	Reference<ITraceLogWriter> logWriter;
	Reference<IThreadPool> writer;
	Reference<IssuesList> issues;
	Clock::time_point latestTrace;
	bool opened = false;

	FBTraceLog() : issues(new IssuesList{}), latestTrace(Clock::now() - 1s) {}

	void traceIssues() {
		// we want to trace at most once per second
		if (Clock::now() < (latestTrace + 1s)) {
			return;
		}
		std::set<std::string> issueSet;
		issues->retrieveIssues(issueSet);
		onMainThread([&issueSet]() -> Future<Void> {
			TraceEvent evt(SevError, "FBTraceHasIssues");
			evt.detail("NumIssues", issueSet.size());
			int cnt = 1;
			for (auto const& s : issueSet) {
				evt.detail(format("Issue%d", cnt++).c_str(), s);
			}
			return Void();
		}).getBlocking();
	}

	void open(const std::string& directory, const std::string& processName, const std::string& basename,
	          unsigned rollsize, unsigned maxLogsSize) {
		if (g_network->isSimulated()) {
			writer = Reference<IThreadPool>{ new DummyThreadPool{} };
		} else {
			writer = createGenericThreadPool();
		}
		logWriter = Reference<ITraceLogWriter>(new FileTraceLogWriter(
		    directory, processName, basename, "fb", maxLogsSize, [this]() { traceIssues(); }, issues));
		writer->addThread(new WriterThread{logWriter, rollsize});
		writer->post(new WriterThread::Open{});
		opened = true;
	}

	void write(Reference<FBTraceImpl> const& msg) {
		if (!opened) {
			return;
		}
		writer->post(new WriterThread::Write{msg});
	}
};

thread_local FBTraceLog g_fbTraceLog;

} // namespace

namespace ChunkAllocatorImpl {

void* allocate(size_t sz) {
	return chunkAllocator.allocate(sz);
}

void free(void* ptr) {
	return chunkAllocator.free(ptr);
}

} // namespace ChunkAllocatorImpl

FBFactory::FBFactory(FileIdentifier fid) {
	FBFactoryState::instance().addFactory(fid, this);
}
FBFactory::~FBFactory() {}

void FBTraceImpl::addref() const {
	refCount.fetch_add(1);
}

void FBTraceImpl::delref() const {
	if (refCount.fetch_sub(1) == 1) {
		delete this;
	}
}

void* FBTraceImpl::operator new(std::size_t sz) {
	return ChunkAllocatorImpl::allocate(sz);
}
void FBTraceImpl::operator delete(void* ptr) {
	Chunk::getChunk(ptr).delref();
}

FBTraceImpl::~FBTraceImpl() {}

void FBTraceImpl::open(const std::string& directory, const std::string& processName, const std::string& basename,
                       unsigned rollsize, unsigned maxLogSize) {
	g_fbTraceLog.open(directory, processName, basename, rollsize, maxLogSize);
}

void fbTraceImpl(Reference<FBTraceImpl> const& traceLine) {
	g_fbTraceLog.write(traceLine);
}
