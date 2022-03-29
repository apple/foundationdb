/*
 * AsyncFileCached.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILECACHED_ACTOR_G_H)
#define FLOW_ASYNCFILECACHED_ACTOR_G_H
#include "fdbrpc/AsyncFileCached.actor.g.h"
#elif !defined(FLOW_ASYNCFILECACHED_ACTOR_H)
#define FLOW_ASYNCFILECACHED_ACTOR_H

#include <boost/intrusive/list.hpp>
#include <type_traits>

#include "flow/flow.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/Knobs.h"
#include "flow/TDMetric.actor.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace bi = boost::intrusive;
struct EvictablePage {
	void* data;
	int index;
	class Reference<struct EvictablePageCache> pageCache;
	bi::list_member_hook<> member_hook;

	virtual bool evict() = 0; // true if page was evicted, false if it isn't immediately evictable (but will be evicted
	                          // regardless if possible)

	EvictablePage(Reference<EvictablePageCache> pageCache) : data(0), index(-1), pageCache(pageCache) {}
	virtual ~EvictablePage();
};

struct EvictablePageCache : ReferenceCounted<EvictablePageCache> {
	using List =
	    bi::list<EvictablePage, bi::member_hook<EvictablePage, bi::list_member_hook<>, &EvictablePage::member_hook>>;
	enum CacheEvictionType { RANDOM = 0, LRU = 1 };

	static CacheEvictionType evictionPolicyStringToEnum(const std::string& policy) {
		std::string cep = policy;
		std::transform(cep.begin(), cep.end(), cep.begin(), ::tolower);
		if (cep != "random" && cep != "lru")
			throw invalid_cache_eviction_policy();

		if (cep == "random")
			return RANDOM;
		return LRU;
	}

	EvictablePageCache() : pageSize(0), maxPages(0), cacheEvictionType(RANDOM) {}

	explicit EvictablePageCache(int pageSize, int64_t maxSize)
	  : pageSize(pageSize), maxPages(maxSize / pageSize),
	    cacheEvictionType(evictionPolicyStringToEnum(FLOW_KNOBS->CACHE_EVICTION_POLICY)) {
		cacheEvictions.init(LiteralStringRef("EvictablePageCache.CacheEvictions"));
	}

	void allocate(EvictablePage* page) {
		try_evict();
		try_evict();

		page->data = allocateFast4kAligned(pageSize);

		if (RANDOM == cacheEvictionType) {
			page->index = pages.size();
			pages.push_back(page);
		} else {
			lruPages.push_back(*page); // new page is considered the most recently used (placed at LRU tail)
		}
	}

	void updateHit(EvictablePage* page) {
		if (RANDOM != cacheEvictionType) {
			// on a hit, update page's location in the LRU so that it's most recent (tail)
			lruPages.erase(List::s_iterator_to(*page));
			lruPages.push_back(*page);
		}
	}

	void try_evict() {
		if (RANDOM == cacheEvictionType) {
			if (pages.size() >= (uint64_t)maxPages && !pages.empty()) {
				for (int i = 0; i < FLOW_KNOBS->MAX_EVICT_ATTEMPTS;
				     i++) { // If we don't manage to evict anything, just go ahead and exceed the cache limit
					int toEvict = deterministicRandom()->randomInt(0, pages.size());
					if (pages[toEvict]->evict()) {
						++cacheEvictions;
						break;
					}
				}
			}
		} else {
			// For now, LRU is the only other CACHE_EVICTION option
			if (lruPages.size() >= (uint64_t)maxPages) {
				int i = 0;
				// try the least recently used pages first (starting at head of the LRU list)
				for (List::iterator it = lruPages.begin(); it != lruPages.end() && i < FLOW_KNOBS->MAX_EVICT_ATTEMPTS;
				     ++it, ++i) { // If we don't manage to evict anything, just go ahead and exceed the cache limit
					if (it->evict()) {
						++cacheEvictions;
						break;
					}
				}
			}
		}
	}

	std::vector<EvictablePage*> pages;
	List lruPages;
	int pageSize;
	int64_t maxPages;
	Int64MetricHandle cacheEvictions;
	const CacheEvictionType cacheEvictionType;
};

struct AFCPage;

class AsyncFileCached final : public IAsyncFile, public ReferenceCounted<AsyncFileCached> {
	friend struct AFCPage;

public:
	// Opens a file that uses the FDB in-memory page cache
	static Future<Reference<IAsyncFile>> open(std::string filename, int flags, int mode) {
		//TraceEvent("AsyncFileCachedOpen").detail("Filename", filename);
		auto itr = openFiles.find(filename);
		if (itr == openFiles.end()) {
			auto f = open_impl(filename, flags, mode);
			if (f.isReady() && f.isError())
				return f;

			auto result = openFiles.try_emplace(filename, f);

			// This should be inserting a new entry
			ASSERT(result.second);
			itr = result.first;

			// We return here instead of falling through to the outer scope so that we don't delete all references to
			// the underlying file before returning
			return itr->second.get();
		}
		return itr->second.get();
	}

	Future<int> read(void* data, int length, int64_t offset) override {
		++countFileCacheReads;
		++countCacheReads;
		if (offset + length > this->length) {
			length = int(this->length - offset);
			ASSERT(length >= 0);
		}
		auto f = read_write_impl<false>(this, static_cast<uint8_t*>(data), length, offset);
		if (f.isReady() && !f.isError())
			return length;
		++countFileCacheReadsBlocked;
		++countCacheReadsBlocked;
		return tag(f, length);
	}

	ACTOR static Future<Void> write_impl(AsyncFileCached* self, void const* data, int length, int64_t offset) {
		// If there is a truncate in progress before the the write position then we must
		// wait for it to complete.
		if (length + offset > self->currentTruncateSize)
			wait(self->currentTruncate);
		++self->countFileCacheWrites;
		++self->countCacheWrites;
		Future<Void> f = read_write_impl<true>(self, static_cast<const uint8_t*>(data), length, offset);
		if (!f.isReady()) {
			++self->countFileCacheWritesBlocked;
			++self->countCacheWritesBlocked;
		}
		wait(f);
		return Void();
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		return write_impl(this, data, length, offset);
	}

	Future<Void> readZeroCopy(void** data, int* length, int64_t offset) override;
	void releaseZeroCopy(void* data, int length, int64_t offset) override;

	// This waits for previously started truncates to finish and then truncates
	Future<Void> truncate(int64_t size) override { return truncate_impl(this, size); }

	// This is the 'real' truncate that does the actual removal of cache blocks and then shortens the file
	Future<Void> changeFileSize(int64_t size);

	// This wrapper for the actual truncation operation enforces ordering of truncates.
	// It maintains currentTruncate and currentTruncateSize so writers can wait behind truncates that would affect them.
	ACTOR static Future<Void> truncate_impl(AsyncFileCached* self, int64_t size) {
		wait(self->currentTruncate);
		self->currentTruncateSize = size;
		self->currentTruncate = self->changeFileSize(size);
		wait(self->currentTruncate);
		return Void();
	}

	Future<Void> sync() override { return waitAndSync(this, flush()); }

	Future<int64_t> size() const override { return length; }

	int64_t debugFD() const override { return uncached->debugFD(); }

	std::string getFilename() const override { return filename; }

	void setRateControl(Reference<IRateControl> const& rc) override { rateControl = rc; }

	Reference<IRateControl> const& getRateControl() override { return rateControl; }

	void addref() override {
		ReferenceCounted<AsyncFileCached>::addref();
		//TraceEvent("AsyncFileCachedAddRef").detail("Filename", filename).detail("Refcount", debugGetReferenceCount()).backtrace();
	}
	void delref() override {
		if (delref_no_destroy()) {
			// If this is ever ThreadSafeReferenceCounted...
			// setrefCountUnsafe(0);

			if (rateControl) {
				TraceEvent(SevDebug, "AsyncFileCachedKillWaiters").detail("Filename", filename);
				rateControl->killWaiters(io_error());
			}

			auto f = quiesce();
			TraceEvent("AsyncFileCachedDel")
			    .detail("Filename", filename)
			    .detail("Refcount", debugGetReferenceCount())
			    .detail("CanDie", f.isReady());
			// .backtrace();
			if (f.isReady())
				delete this;
			else
				uncancellable(holdWhile(Reference<AsyncFileCached>::addRef(this), f));
		}
	}

	~AsyncFileCached() override;

private:
	// A map of filename to the file handle for all opened cached files
	static std::map<std::string, UnsafeWeakFutureReference<IAsyncFile>> openFiles;

	std::string filename;
	Reference<IAsyncFile> uncached;
	int64_t length;
	int64_t prevLength;
	std::unordered_map<int64_t, AFCPage*> pages;
	std::vector<AFCPage*> flushable;
	Reference<EvictablePageCache> pageCache;
	Future<Void> currentTruncate;
	int64_t currentTruncateSize;
	Reference<IRateControl> rateControl;

	// Map of pointers which hold page buffers for pages which have been overwritten
	// but at the time of write there were still readZeroCopy holders.
	std::unordered_map<void*, int> orphanedPages;

	Int64MetricHandle countFileCacheFinds;
	Int64MetricHandle countFileCacheReads;
	Int64MetricHandle countFileCacheWrites;
	Int64MetricHandle countFileCacheReadsBlocked;
	Int64MetricHandle countFileCacheWritesBlocked;
	Int64MetricHandle countFileCachePageReadsHit;
	Int64MetricHandle countFileCachePageReadsMissed;
	Int64MetricHandle countFileCachePageReadsMerged;
	Int64MetricHandle countFileCacheReadBytes;

	Int64MetricHandle countCacheFinds;
	Int64MetricHandle countCacheReads;
	Int64MetricHandle countCacheWrites;
	Int64MetricHandle countCacheReadsBlocked;
	Int64MetricHandle countCacheWritesBlocked;
	Int64MetricHandle countCachePageReadsHit;
	Int64MetricHandle countCachePageReadsMissed;
	Int64MetricHandle countCachePageReadsMerged;
	Int64MetricHandle countCacheReadBytes;

	AsyncFileCached(Reference<IAsyncFile> uncached,
	                const std::string& filename,
	                int64_t length,
	                Reference<EvictablePageCache> pageCache)
	  : filename(filename), uncached(uncached), length(length), prevLength(length), pageCache(pageCache),
	    currentTruncate(Void()), currentTruncateSize(0), rateControl(nullptr) {
		if (!g_network->isSimulated()) {
			countFileCacheWrites.init(LiteralStringRef("AsyncFile.CountFileCacheWrites"), filename);
			countFileCacheReads.init(LiteralStringRef("AsyncFile.CountFileCacheReads"), filename);
			countFileCacheWritesBlocked.init(LiteralStringRef("AsyncFile.CountFileCacheWritesBlocked"), filename);
			countFileCacheReadsBlocked.init(LiteralStringRef("AsyncFile.CountFileCacheReadsBlocked"), filename);
			countFileCachePageReadsHit.init(LiteralStringRef("AsyncFile.CountFileCachePageReadsHit"), filename);
			countFileCachePageReadsMissed.init(LiteralStringRef("AsyncFile.CountFileCachePageReadsMissed"), filename);
			countFileCachePageReadsMerged.init(LiteralStringRef("AsyncFile.CountFileCachePageReadsMerged"), filename);
			countFileCacheFinds.init(LiteralStringRef("AsyncFile.CountFileCacheFinds"), filename);
			countFileCacheReadBytes.init(LiteralStringRef("AsyncFile.CountFileCacheReadBytes"), filename);

			countCacheWrites.init(LiteralStringRef("AsyncFile.CountCacheWrites"));
			countCacheReads.init(LiteralStringRef("AsyncFile.CountCacheReads"));
			countCacheWritesBlocked.init(LiteralStringRef("AsyncFile.CountCacheWritesBlocked"));
			countCacheReadsBlocked.init(LiteralStringRef("AsyncFile.CountCacheReadsBlocked"));
			countCachePageReadsHit.init(LiteralStringRef("AsyncFile.CountCachePageReadsHit"));
			countCachePageReadsMissed.init(LiteralStringRef("AsyncFile.CountCachePageReadsMissed"));
			countCachePageReadsMerged.init(LiteralStringRef("AsyncFile.CountCachePageReadsMerged"));
			countCacheFinds.init(LiteralStringRef("AsyncFile.CountCacheFinds"));
			countCacheReadBytes.init(LiteralStringRef("AsyncFile.CountCacheReadBytes"));
		}
	}

	static Future<Reference<IAsyncFile>> open_impl(std::string filename, int flags, int mode);

	// Opens a file that uses the FDB in-memory page cache
	ACTOR static Future<Reference<IAsyncFile>> open_impl(std::string filename,
	                                                     int flags,
	                                                     int mode,
	                                                     Reference<EvictablePageCache> pageCache) {
		try {
			TraceEvent("AFCUnderlyingOpenBegin").detail("Filename", filename);
			if (flags & IAsyncFile::OPEN_CACHED_READ_ONLY)
				flags = (flags & ~IAsyncFile::OPEN_READWRITE) | IAsyncFile::OPEN_READONLY;
			else
				flags = (flags & ~IAsyncFile::OPEN_READONLY) | IAsyncFile::OPEN_READWRITE;
			state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
			    filename, flags | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_UNBUFFERED, mode));
			TraceEvent("AFCUnderlyingOpenEnd").detail("Filename", filename);
			int64_t l = wait(f->size());
			TraceEvent("AFCUnderlyingSize").detail("Filename", filename).detail("Size", l);
			return new AsyncFileCached(f, filename, l, pageCache);
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				openFiles.erase(filename);
			throw e;
		}
	}

	Future<Void> flush() override;

	Future<Void> quiesce();

	ACTOR static Future<Void> waitAndSync(AsyncFileCached* self, Future<Void> flush) {
		wait(flush);
		wait(self->uncached->sync());
		return Void();
	}

	template <bool writing>
	static Future<Void> read_write_impl(AsyncFileCached* self,
	                                    typename std::conditional_t<writing, const uint8_t*, uint8_t*> data,
	                                    int length,
	                                    int64_t offset);

	void remove_page(AFCPage* page);
};

struct AFCPage : public EvictablePage, public FastAllocated<AFCPage> {
	bool evict() override {
		if (notReading.isReady() && notFlushing.isReady() && !dirty && !zeroCopyRefCount && !truncated) {
			owner->remove_page(this);
			delete this;
			return true;
		}

		if (dirty)
			flush();

		return false;
	}

	// Move this page's data into the orphanedPages set of the owner
	void orphan() {
		owner->orphanedPages[data] = zeroCopyRefCount;
		zeroCopyRefCount = 0;
		notReading = Void();
		data = allocateFast4kAligned(pageCache->pageSize);
	}

	Future<Void> write(void const* data, int length, int offset) {
		// If zero-copy reads are in progress, allow whole page writes to a new page buffer so the effects
		// are not seen by the prior readers who still hold zeroCopyRead pointers
		bool fullPage = offset == 0 && length == pageCache->pageSize;
		ASSERT(zeroCopyRefCount == 0 || fullPage);

		if (zeroCopyRefCount != 0) {
			ASSERT(fullPage);
			orphan();
		}

		setDirty();

		// If there are no active readers then if data is valid or we're replacing all of it we can write directly
		if (valid || fullPage) {
			if (!fullPage) {
				++owner->countFileCachePageReadsHit;
				++owner->countCachePageReadsHit;
			}
			valid = true;
			memcpy(static_cast<uint8_t*>(this->data) + offset, data, length);
			return yield();
		}

		++owner->countFileCachePageReadsMissed;
		++owner->countCachePageReadsMissed;

		// If data is not valid but no read is in progress, start reading
		if (notReading.isReady()) {
			notReading = readThrough(this);
		}

		notReading = waitAndWrite(this, data, length, offset);

		return notReading;
	}

	ACTOR static Future<Void> waitAndWrite(AFCPage* self, void const* data, int length, int offset) {
		wait(self->notReading);
		memcpy(static_cast<uint8_t*>(self->data) + offset, data, length);
		return Void();
	}

	Future<Void> readZeroCopy() {
		++zeroCopyRefCount;
		if (valid) {
			++owner->countFileCachePageReadsHit;
			++owner->countCachePageReadsHit;
			return yield();
		}

		++owner->countFileCachePageReadsMissed;
		++owner->countCachePageReadsMissed;

		if (notReading.isReady()) {
			notReading = readThrough(this);
		} else {
			++owner->countFileCachePageReadsMerged;
			++owner->countCachePageReadsMerged;
		}

		return notReading;
	}
	void releaseZeroCopy() {
		--zeroCopyRefCount;
		ASSERT(zeroCopyRefCount >= 0);
	}

	Future<Void> read(void* data, int length, int offset) {
		if (valid) {
			++owner->countFileCachePageReadsHit;
			++owner->countCachePageReadsHit;
			owner->countFileCacheReadBytes += length;
			owner->countCacheReadBytes += length;
			memcpy(data, static_cast<uint8_t const*>(this->data) + offset, length);
			return yield();
		}

		++owner->countFileCachePageReadsMissed;
		++owner->countCachePageReadsMissed;

		if (notReading.isReady()) {
			notReading = readThrough(this);
		} else {
			++owner->countFileCachePageReadsMerged;
			++owner->countCachePageReadsMerged;
		}

		notReading = waitAndRead(this, data, length, offset);

		return notReading;
	}

	ACTOR static Future<Void> waitAndRead(AFCPage* self, void* data, int length, int offset) {
		wait(self->notReading);
		memcpy(data, static_cast<uint8_t const*>(self->data) + offset, length);
		return Void();
	}

	ACTOR static Future<Void> readThrough(AFCPage* self) {
		ASSERT(!self->valid);
		state void* dst = self->data;
		if (self->pageOffset < self->owner->prevLength) {
			try {
				int _ = wait(self->owner->uncached->read(dst, self->pageCache->pageSize, self->pageOffset));
				if (_ != self->pageCache->pageSize)
					TraceEvent("ReadThroughShortRead")
					    .detail("ReadAmount", _)
					    .detail("PageSize", self->pageCache->pageSize)
					    .detail("PageOffset", self->pageOffset);
			} catch (Error& e) {
				self->zeroCopyRefCount = 0;
				TraceEvent("ReadThroughFailed").error(e);
				throw;
			}
		}
		// If the memory we read into wasn't orphaned while we were waiting on the read then set valid to true
		if (dst == self->data)
			self->valid = true;
		return Void();
	}

	ACTOR static Future<Void> writeThrough(AFCPage* self, Promise<Void> writing) {
		// writeThrough can be called on a page that is not dirty, just to wait for a previous writeThrough to finish.
		// In that case we don't want to do any disk I/O
		try {
			state bool dirty = self->dirty;
			++self->writeThroughCount;
			self->updateFlushableIndex();

			wait(self->notReading && self->notFlushing);

			if (dirty) {
				// Wait for rate control if it is set
				if (self->owner->getRateControl()) {
					int allowance = 1;
					// If I/O size is defined, wait for the calculated I/O quota
					if (FLOW_KNOBS->FLOW_CACHEDFILE_WRITE_IO_SIZE > 0) {
						allowance = (self->pageCache->pageSize + FLOW_KNOBS->FLOW_CACHEDFILE_WRITE_IO_SIZE - 1) /
						            FLOW_KNOBS->FLOW_CACHEDFILE_WRITE_IO_SIZE; // round up
						ASSERT(allowance > 0);
					}
					wait(self->owner->getRateControl()->getAllowance(allowance));
				}

				if (self->pageOffset + self->pageCache->pageSize > self->owner->length) {
					ASSERT(self->pageOffset < self->owner->length);
					memset(static_cast<uint8_t*>(self->data) + self->owner->length - self->pageOffset,
					       0,
					       self->pageCache->pageSize - (self->owner->length - self->pageOffset));
				}

				auto f = self->owner->uncached->write(self->data, self->pageCache->pageSize, self->pageOffset);

				wait(f);
			}
		} catch (Error& e) {
			--self->writeThroughCount;
			self->setDirty();
			writing.sendError(e);
			throw;
		}
		--self->writeThroughCount;
		self->updateFlushableIndex();

		writing.send(Void()); // FIXME: This could happen before the wait if AsyncFileKAIO dealt properly with
		                      // overlapping write and sync operations

		self->pageCache->try_evict();

		return Void();
	}

	Future<Void> flush() {
		if (!dirty && notFlushing.isReady())
			return Void();

		ASSERT(valid || !notReading.isReady() || notReading.isError());

		Promise<Void> writing;

		notFlushing = writeThrough(this, writing);

		clearDirty(); // Do this last so that if writeThrough immediately calls try_evict, we can't be evicted before
		              // assigning notFlushing
		return writing.getFuture();
	}

	Future<Void> quiesce() {
		if (dirty)
			flush();

		// If we are flushing, we will be quiescent when all flushes are finished
		// Returning flush() isn't right, because flush can return before notFlushing.isReady()
		if (!notFlushing.isReady()) {
			return notFlushing;
		}

		// else if we are reading, we will be quiescent when the read is finished
		if (!notReading.isReady())
			return notReading;

		return Void();
	}

	Future<Void> truncate() {
		// Allow truncatation during zero copy reads but orphan the previous buffer
		if (zeroCopyRefCount != 0)
			orphan();
		truncated = true;
		return truncate_impl(this);
	}

	ACTOR static Future<Void> truncate_impl(AFCPage* self) {
		wait(self->notReading && self->notFlushing && yield());
		delete self;
		return Void();
	}

	AFCPage(AsyncFileCached* owner, int64_t offset)
	  : EvictablePage(owner->pageCache), owner(owner), pageOffset(offset), notReading(Void()), notFlushing(Void()),
	    dirty(false), valid(false), truncated(false), writeThroughCount(0), flushableIndex(-1), zeroCopyRefCount(0) {
		pageCache->allocate(this);
	}

	~AFCPage() override {
		clearDirty();
		ASSERT_ABORT(flushableIndex == -1);
	}

	void setDirty() {
		dirty = true;
		updateFlushableIndex();
	}

	void clearDirty() {
		dirty = false;
		updateFlushableIndex();
	}

	void updateFlushableIndex() {
		bool flushable = dirty || writeThroughCount;
		if (flushable == (flushableIndex != -1))
			return;

		if (flushable) {
			flushableIndex = owner->flushable.size();
			owner->flushable.push_back(this);
		} else {
			ASSERT(owner->flushable[flushableIndex] == this);
			owner->flushable[flushableIndex] = owner->flushable.back();
			owner->flushable[flushableIndex]->flushableIndex = flushableIndex;
			owner->flushable.pop_back();
			flushableIndex = -1;
		}
	}

	AsyncFileCached* owner;
	int64_t pageOffset;

	Future<Void> notReading; // .isReady when a readThrough (or waitAndWrite) is not in progress
	Future<Void> notFlushing; // .isReady when a writeThrough is not in progress

	bool dirty; // write has been called more recently than flush
	bool valid; // data contains the file contents
	bool truncated; // true if this page has been truncated
	int writeThroughCount; // number of writeThrough actors that are in progress (potentially writing or waiting to
	                       // write)
	int flushableIndex; // index in owner->flushable[]
	int zeroCopyRefCount; // references held by "zero-copy" reads
};

#include "flow/unactorcompiler.h"
#endif
