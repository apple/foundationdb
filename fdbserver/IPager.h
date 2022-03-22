/*
 * IPager.h
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

#ifndef FDBSERVER_IPAGER_H
#define FDBSERVER_IPAGER_H
#pragma once

#include "fdbserver/IKeyValueStore.h"

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"
#include "flow/xxhash.h"

#ifndef VALGRIND
#define VALGRIND_MAKE_MEM_UNDEFINED(x, y)
#define VALGRIND_MAKE_MEM_DEFINED(x, y)
#endif

typedef uint32_t LogicalPageID;
typedef uint32_t PhysicalPageID;
#define invalidLogicalPageID std::numeric_limits<LogicalPageID>::max()

typedef uint32_t QueueID;
#define invalidQueueID std::numeric_limits<QueueID>::max()

// Pager Events
enum class PagerEvents { CacheLookup = 0, CacheHit, CacheMiss, PageWrite, MAXEVENTS };
static const char* const PagerEventsStrings[] = { "Lookup", "Hit", "Miss", "Write", "Unknown" };
// Reasons for page level events.
enum class PagerEventReasons { PointRead = 0, RangeRead, RangePrefetch, Commit, LazyClear, MetaData, MAXEVENTREASONS };
static const char* const PagerEventReasonsStrings[] = {
	"Get", "GetR", "GetRPF", "Commit", "LazyClr", "Meta", "Unknown"
};

static const unsigned int nonBtreeLevel = 0;
static const std::vector<std::pair<PagerEvents, PagerEventReasons>> possibleEventReasonPairs = {
	{ PagerEvents::CacheLookup, PagerEventReasons::Commit },
	{ PagerEvents::CacheLookup, PagerEventReasons::LazyClear },
	{ PagerEvents::CacheLookup, PagerEventReasons::PointRead },
	{ PagerEvents::CacheLookup, PagerEventReasons::RangeRead },
	{ PagerEvents::CacheHit, PagerEventReasons::Commit },
	{ PagerEvents::CacheHit, PagerEventReasons::LazyClear },
	{ PagerEvents::CacheHit, PagerEventReasons::PointRead },
	{ PagerEvents::CacheHit, PagerEventReasons::RangeRead },
	{ PagerEvents::CacheMiss, PagerEventReasons::Commit },
	{ PagerEvents::CacheMiss, PagerEventReasons::LazyClear },
	{ PagerEvents::CacheMiss, PagerEventReasons::PointRead },
	{ PagerEvents::CacheMiss, PagerEventReasons::RangeRead },
	{ PagerEvents::PageWrite, PagerEventReasons::Commit },
	{ PagerEvents::PageWrite, PagerEventReasons::LazyClear },
};
static const std::vector<std::pair<PagerEvents, PagerEventReasons>> L0PossibleEventReasonPairs = {
	{ PagerEvents::CacheLookup, PagerEventReasons::RangePrefetch },
	{ PagerEvents::CacheLookup, PagerEventReasons::MetaData },
	{ PagerEvents::CacheHit, PagerEventReasons::RangePrefetch },
	{ PagerEvents::CacheHit, PagerEventReasons::MetaData },
	{ PagerEvents::CacheMiss, PagerEventReasons::RangePrefetch },
	{ PagerEvents::CacheMiss, PagerEventReasons::MetaData },
	{ PagerEvents::PageWrite, PagerEventReasons::MetaData },
};

// Represents a block of memory in a 4096-byte aligned location held by an Arena.
class ArenaPage : public ReferenceCounted<ArenaPage>, public FastAllocated<ArenaPage> {
public:
	// The page's logical size includes an opaque checksum, use size() to get usable size
	ArenaPage(int logicalSize, int bufferSize) : logicalSize(logicalSize), bufferSize(bufferSize), userData(nullptr) {
		if (bufferSize > 0) {
			buffer = (uint8_t*)arena.allocate4kAlignedBuffer(bufferSize);

			// Mark any unused page portion defined
			VALGRIND_MAKE_MEM_DEFINED(buffer + logicalSize, bufferSize - logicalSize);
		} else {
			buffer = nullptr;
		}
	};

	~ArenaPage() {
		if (userData != nullptr && userDataDestructor != nullptr) {
			userDataDestructor(userData);
		}
	}

	uint8_t const* begin() const { return (uint8_t*)buffer; }

	uint8_t* mutate() { return (uint8_t*)buffer; }

	typedef XXH64_hash_t Checksum;

	// Usable size, without checksum
	int size() const { return logicalSize - sizeof(Checksum); }

	Standalone<StringRef> asStringRef() const { return Standalone<StringRef>(StringRef(begin(), size()), arena); }

	// Get an ArenaPage which is a copy of this page, in its own Arena
	Reference<ArenaPage> cloneContents() const {
		ArenaPage* p = new ArenaPage(logicalSize, bufferSize);
		memcpy(p->buffer, buffer, logicalSize);
		return Reference<ArenaPage>(p);
	}

	// Get an ArenaPage which depends on this page's Arena and references some of its memory
	Reference<ArenaPage> subPage(int offset, int len) const {
		ArenaPage* p = new ArenaPage(len, 0);
		p->buffer = buffer + offset;
		p->arena.dependsOn(arena);
		return Reference<ArenaPage>(p);
	}

	// Given a vector of pages with the same ->size(), create a new ArenaPage with a ->size() that is
	// equivalent to all of the input pages and has all of their contents copied into it.
	static Reference<ArenaPage> concatPages(const std::vector<Reference<const ArenaPage>>& pages) {
		int usableSize = pages.front()->size();
		int totalUsableSize = pages.size() * usableSize;
		int totalBufferSize = pages.front()->bufferSize * pages.size();
		ArenaPage* superpage = new ArenaPage(totalUsableSize + sizeof(Checksum), totalBufferSize);

		uint8_t* wptr = superpage->mutate();
		for (auto& p : pages) {
			ASSERT(p->size() == usableSize);
			memcpy(wptr, p->begin(), usableSize);
			wptr += usableSize;
		}

		return Reference<ArenaPage>(superpage);
	}

	Checksum& getChecksum() { return *(Checksum*)(buffer + size()); }

	Checksum calculateChecksum(LogicalPageID pageID) { return XXH3_64bits_withSeed(buffer, size(), pageID); }

	void updateChecksum(LogicalPageID pageID) { getChecksum() = calculateChecksum(pageID); }

	bool verifyChecksum(LogicalPageID pageID) { return getChecksum() == calculateChecksum(pageID); }

	const Arena& getArena() const { return arena; }

private:
	Arena arena;
	int logicalSize;
	int bufferSize;
	uint8_t* buffer;

public:
	mutable void* userData;
	mutable void (*userDataDestructor)(void*);
};

class IPagerSnapshot {
public:
	virtual Future<Reference<const ArenaPage>> getPhysicalPage(PagerEventReasons reason,
	                                                           unsigned int level,
	                                                           LogicalPageID pageID,
	                                                           int priority,
	                                                           bool cacheable,
	                                                           bool nohit) = 0;
	virtual Future<Reference<const ArenaPage>> getMultiPhysicalPage(PagerEventReasons reason,
	                                                                unsigned int level,
	                                                                VectorRef<LogicalPageID> pageIDs,
	                                                                int priority,
	                                                                bool cacheable,
	                                                                bool nohit) = 0;
	virtual Version getVersion() const = 0;

	virtual Key getMetaKey() const = 0;

	virtual ~IPagerSnapshot() {}

	virtual void addref() = 0;
	virtual void delref() = 0;
};

// This API is probably too customized to the behavior of DWALPager and probably needs some changes to be more generic.
class IPager2 : public IClosable {
public:
	// Returns an ArenaPage that can be passed to writePage. The data in the returned ArenaPage might not be zeroed.
	virtual Reference<ArenaPage> newPageBuffer(size_t size = 1) = 0;

	// Returns the usable size of pages returned by the pager (i.e. the size of the page that isn't pager overhead).
	// For a given pager instance, separate calls to this function must return the same value.
	// Only valid to call after recovery is complete.
	virtual int getUsablePageSize() const = 0;
	virtual int getPhysicalPageSize() const = 0;
	virtual int getLogicalPageSize() const = 0;
	virtual int getPagesPerExtent() const = 0;

	// Write detail fields with pager stats to a trace event
	virtual void toTraceEvent(TraceEvent& e) const = 0;

	// Allocate a new page ID for a subsequent write.  The page will be considered in-use after the next commit
	// regardless of whether or not it was written to.
	virtual Future<LogicalPageID> newPageID() = 0;

	virtual Future<LogicalPageID> newExtentPageID(QueueID queueID) = 0;
	virtual QueueID newLastQueueID() = 0;

	// Replace the contents of a page with new data across *all* versions.
	// Existing holders of a page reference for pageID, read from any version,
	// may see the effects of this write.
	virtual void updatePage(PagerEventReasons reason,
	                        unsigned int level,
	                        Standalone<VectorRef<LogicalPageID>> pageIDs,
	                        Reference<ArenaPage> data) = 0;
	// Try to atomically update the contents of a page as of version v in the next commit.
	// If the pager is unable to do this at this time, it may choose to write the data to a new page ID
	// instead and return the new page ID to the caller.  Otherwise the original pageID argument will be returned.
	// If a new page ID is returned, the old page ID will be freed as of version v
	virtual Future<LogicalPageID> atomicUpdatePage(PagerEventReasons reason,
	                                               unsigned int level,
	                                               LogicalPageID pageID,
	                                               Reference<ArenaPage> data,
	                                               Version v) = 0;

	// Free pageID to be used again after the commit that moves oldestVersion past v
	virtual void freePage(LogicalPageID pageID, Version v) = 0;

	virtual void freeExtent(LogicalPageID pageID) = 0;

	// If id is remapped, delete the original as of version v and return the page it was remapped to.  The caller
	// is then responsible for referencing and deleting the returned page ID.
	virtual LogicalPageID detachRemappedPage(LogicalPageID id, Version v) = 0;

	// Returns the latest data (regardless of version) for a page by LogicalPageID
	// The data returned will be the later of
	//   - the most recent committed atomic
	//   - the most recent non-atomic write
	// Cacheable indicates that the page should be added to the page cache (if applicable?) as a result of this read.
	// NoHit indicates that the read should not be considered a cache hit, such as when preloading pages that are
	// considered likely to be needed soon.
	virtual Future<Reference<ArenaPage>> readPage(PagerEventReasons reason,
	                                              unsigned int level,
	                                              PhysicalPageID pageIDs,
	                                              int priority,
	                                              bool cacheable,
	                                              bool noHit) = 0;
	virtual Future<Reference<ArenaPage>> readMultiPage(PagerEventReasons reason,
	                                                   unsigned int level,
	                                                   Standalone<VectorRef<PhysicalPageID>> pageIDs,
	                                                   int priority,
	                                                   bool cacheable,
	                                                   bool noHit) = 0;

	virtual Future<Reference<ArenaPage>> readExtent(LogicalPageID pageID) = 0;
	virtual void releaseExtentReadLock() = 0;

	// Temporary methods for testing
	virtual Future<Standalone<VectorRef<LogicalPageID>>> getUsedExtents(QueueID queueID) = 0;
	virtual void pushExtentUsedList(QueueID queueID, LogicalPageID extID) = 0;
	virtual void extentCacheClear() = 0;
	virtual int64_t getPageCacheCount() = 0;
	virtual int64_t getExtentCacheCount() = 0;

	// Get a snapshot of the metakey and all pages as of the version v which must be >= getOldestVersion()
	// Note that snapshots at any version may still see the results of updatePage() calls.
	// The snapshot shall be usable until setOldVersion() is called with a version > v.
	virtual Reference<IPagerSnapshot> getReadSnapshot(Version v) = 0;

	// Atomically make durable all pending page writes, page frees, and update the metadata string,
	// setting the committed version to v
	// v must be >= the highest versioned page write.
	virtual Future<Void> commit(Version v) = 0;

	// Get the latest meta key set or committed
	virtual Key getMetaKey() const = 0;

	// Set the metakey which will be stored in the next commit
	virtual void setMetaKey(KeyRef metaKey) = 0;

	virtual StorageBytes getStorageBytes() const = 0;

	virtual int64_t getPageCount() = 0;

	// Count of pages in use by the pager client (including retained old page versions)
	virtual Future<int64_t> getUserPageCount() = 0;

	// Future returned is ready when pager has been initialized from disk and is ready for reads and writes.
	// It is invalid to call most other functions until init() is ready.
	// TODO: Document further.
	virtual Future<Void> init() = 0;

	// Returns latest committed version
	virtual Version getLastCommittedVersion() const = 0;

	// Returns the oldest readable version as of the most recent committed version
	virtual Version getOldestReadableVersion() const = 0;

	// Sets the oldest readable version to be put into affect at the next commit.
	// The pager can reuse pages that were freed at a version less than v.
	// If any snapshots are in use at a version less than v, the pager can either forcefully
	// invalidate them or keep their versions around until the snapshots are no longer in use.
	virtual void setOldestReadableVersion(Version v) = 0;

	// Advance the commit version and the oldest readble version and commit until the remap queue is empty.
	virtual Future<Void> clearRemapQueue() = 0;

	// Get a pointer to an integer representing a byte count penalty the pager should apply against usable page cache
	// memory. This is used to track significant memory usage external to the pager.  Such usages should
	// increment/decrement the value at this pointer based on their memory footprint.
	virtual int64_t* getPageCachePenaltySource() = 0;

protected:
	~IPager2() {} // Destruction should be done using close()/dispose() from the IClosable interface
};

#endif
