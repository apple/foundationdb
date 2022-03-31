/*
 * AsyncFileCached.actor.cpp
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

#include "fdbrpc/AsyncFileCached.actor.h"

// Page caches used in non-simulated environments
Optional<Reference<EvictablePageCache>> pc4k, pc64k;

// The simulator needs to store separate page caches for each machine
static std::map<NetworkAddress, std::pair<Reference<EvictablePageCache>, Reference<EvictablePageCache>>>
    simulatorPageCaches;

EvictablePage::~EvictablePage() {
	if (data) {
		freeFast4kAligned(pageCache->pageSize, data);
	}
	if (EvictablePageCache::RANDOM == pageCache->cacheEvictionType) {
		if (index > -1) {
			pageCache->pages[index] = pageCache->pages.back();
			pageCache->pages[index]->index = index;
			pageCache->pages.pop_back();
		}
	} else {
		// remove it from the LRU
		pageCache->lruPages.erase(EvictablePageCache::List::s_iterator_to(*this));
	}
}

// A map of filename to the file handle for all opened cached files
std::map<std::string, UnsafeWeakFutureReference<IAsyncFile>> AsyncFileCached::openFiles;

void AsyncFileCached::remove_page(AFCPage* page) {
	pages.erase(page->pageOffset);
}

Future<Reference<IAsyncFile>> AsyncFileCached::open_impl(std::string filename, int flags, int mode) {
	Reference<EvictablePageCache> pageCache;

	// In a simulated environment, each machine needs its own caches
	if (g_network->isSimulated()) {
		auto cacheItr = simulatorPageCaches.find(g_network->getLocalAddress());
		if (cacheItr == simulatorPageCaches.end()) {
			int64_t pageCacheSize4k = (BUGGIFY) ? FLOW_KNOBS->BUGGIFY_SIM_PAGE_CACHE_4K : FLOW_KNOBS->SIM_PAGE_CACHE_4K;
			int64_t pageCacheSize64k =
			    (BUGGIFY) ? FLOW_KNOBS->BUGGIFY_SIM_PAGE_CACHE_64K : FLOW_KNOBS->SIM_PAGE_CACHE_64K;
			auto caches = std::make_pair(makeReference<EvictablePageCache>(4096, pageCacheSize4k),
			                             makeReference<EvictablePageCache>(65536, pageCacheSize64k));
			simulatorPageCaches[g_network->getLocalAddress()] = caches;
			pageCache = (flags & IAsyncFile::OPEN_LARGE_PAGES) ? caches.second : caches.first;
		} else
			pageCache = (flags & IAsyncFile::OPEN_LARGE_PAGES) ? cacheItr->second.second : cacheItr->second.first;
	} else {
		if (flags & IAsyncFile::OPEN_LARGE_PAGES) {
			if (!pc64k.present())
				pc64k = makeReference<EvictablePageCache>(65536, FLOW_KNOBS->PAGE_CACHE_64K);
			pageCache = pc64k.get();
		} else {
			if (!pc4k.present())
				pc4k = makeReference<EvictablePageCache>(4096, FLOW_KNOBS->PAGE_CACHE_4K);
			pageCache = pc4k.get();
		}
	}

	return open_impl(filename, flags, mode, pageCache);
}

template <bool writing>
Future<Void> AsyncFileCached::read_write_impl(AsyncFileCached* self,
                                              typename std::conditional_t<writing, const uint8_t*, uint8_t*> data,
                                              int length,
                                              int64_t offset) {
	if constexpr (writing) {
		if (offset + length > self->length)
			self->length = offset + length;
	}

	std::vector<Future<Void>> actors;

	int offsetInPage = offset % self->pageCache->pageSize;
	int64_t pageOffset = offset - offsetInPage;

	int remaining = length;

	while (remaining) {
		++self->countFileCacheFinds;
		++self->countCacheFinds;
		auto p = self->pages.find(pageOffset);
		if (p == self->pages.end()) {
			AFCPage* page = new AFCPage(self, pageOffset);
			p = self->pages.insert(std::make_pair(pageOffset, page)).first;
		} else {
			self->pageCache->updateHit(p->second);
		}

		int bytesInPage = std::min(self->pageCache->pageSize - offsetInPage, remaining);

		Future<Void> w;
		if constexpr (writing) {
			w = p->second->write(data, bytesInPage, offsetInPage);
		} else {
			w = p->second->read(data, bytesInPage, offsetInPage);
		}
		if (!w.isReady() || w.isError())
			actors.push_back(w);

		data += bytesInPage;
		pageOffset += self->pageCache->pageSize;
		offsetInPage = 0;

		remaining -= bytesInPage;
	}

	// This is susceptible to the introduction of waits on the read/write path: no wait can occur prior to
	// AFCPage::readThrough or prevLength will be set prematurely
	self->prevLength = self->length;

	return waitForAll(actors);
}

Future<Void> AsyncFileCached::readZeroCopy(void** data, int* length, int64_t offset) {
	++countFileCacheReads;
	++countCacheReads;

	// Only aligned page reads are zero-copy
	if (*length != pageCache->pageSize || (offset & (pageCache->pageSize - 1)) || offset + *length > this->length)
		return io_error();

	auto p = pages.find(offset);
	if (p == pages.end()) {
		AFCPage* page = new AFCPage(this, offset);
		p = pages.insert(std::make_pair(offset, page)).first;
	} else {
		p->second->pageCache->updateHit(p->second);
	}

	*data = p->second->data;

	return p->second->readZeroCopy();
}
void AsyncFileCached::releaseZeroCopy(void* data, int length, int64_t offset) {
	ASSERT(length == pageCache->pageSize && !(offset & (pageCache->pageSize - 1)) && offset + length <= this->length);
	auto p = pages.find(offset);
	// If the page is in the cache and the data pointer matches then release the page
	if (p != pages.end() && p->second->data == data) {
		p->second->releaseZeroCopy();
	} else {
		// Otherwise, the data pointer might exist in the orphaned pages map
		auto o = orphanedPages.find(data);
		if (o != orphanedPages.end()) {
			if (o->second == 1) {
				if (data) {
					freeFast4kAligned(length, data);
				}
			} else {
				--o->second;
			}
		}
	}
}

Future<Void> AsyncFileCached::changeFileSize(int64_t size) {
	++countFileCacheWrites;
	++countCacheWrites;

	std::vector<Future<Void>> actors;
	int64_t oldLength = length;

	int offsetInPage = size % pageCache->pageSize;
	int64_t pageOffset = size - offsetInPage;

	if (offsetInPage == 0 && size == length) {
		return Void();
	}

	length = size;
	prevLength = size;

	if (offsetInPage) {
		TEST(true); // Truncating to the middle of a page
		auto p = pages.find(pageOffset);
		if (p != pages.end()) {
			auto f = p->second->flush();
			if (!f.isReady() || f.isError())
				actors.push_back(f);
		} else {
			TEST(true); // Truncating to the middle of a page that isn't in cache
		}

		pageOffset += pageCache->pageSize;
	}

	// if this call to truncate results in a larger file, there is no
	// need to erase any pages
	if (oldLength > pageOffset) {
		// Iterating through all pages results in better cache locality than
		// looking up pages one by one in the hash table. However, if we only need
		// to truncate a small portion of data, looking up pages one by one should
		// be faster. So for now we do single key lookup for each page if it results
		// in less than a fixed percentage of the unordered map being accessed.
		int64_t numLookups = (oldLength + (pageCache->pageSize - 1) - pageOffset) / pageCache->pageSize;
		if (numLookups < pages.size() * FLOW_KNOBS->PAGE_CACHE_TRUNCATE_LOOKUP_FRACTION) {
			for (int64_t offset = pageOffset; offset < oldLength; offset += pageCache->pageSize) {
				auto iter = pages.find(offset);
				if (iter != pages.end()) {
					auto f = iter->second->truncate();
					if (!f.isReady() || f.isError()) {
						actors.push_back(f);
					}
					pages.erase(iter);
				}
			}
		} else {
			for (auto p = pages.begin(); p != pages.end();) {
				if (p->first >= pageOffset) {
					auto f = p->second->truncate();
					if (!f.isReady() || f.isError()) {
						actors.push_back(f);
					}
					auto last = p;
					++p;
					pages.erase(last);
				} else {
					++p;
				}
			}
		}
	}

	// Wait for the page truncations to finish, then truncate the underlying file
	// Template types are being provided explicitly because they can't be automatically deduced for some reason.
	return mapAsync<Void, std::function<Future<Void>(Void)>, Void>(
	    waitForAll(actors), [=](Void _) -> Future<Void> { return uncached->truncate(size); });
}

Future<Void> AsyncFileCached::flush() {
	++countFileCacheWrites;
	++countCacheWrites;

	std::vector<Future<Void>> unflushed;

	int debug_count = flushable.size();
	for (int i = 0; i < flushable.size();) {
		auto p = flushable[i];
		auto f = p->flush();
		if (!f.isReady() || f.isError())
			unflushed.push_back(f);
		ASSERT((i < flushable.size() && flushable[i] == p) != f.isReady());
		if (!f.isReady())
			i++;
	}
	ASSERT(flushable.size() <= debug_count);

	return waitForAll(unflushed);
}

Future<Void> AsyncFileCached::quiesce() {
	std::vector<Future<Void>> unquiescent;

	for (auto i = pages.begin(); i != pages.end(); ++i) {
		auto f = i->second->quiesce();
		if (!f.isReady())
			unquiescent.push_back(f);
	}

	// Errors are absorbed because we need everything to finish
	return waitForAllReady(unquiescent);
}

AsyncFileCached::~AsyncFileCached() {
	while (!pages.empty()) {
		auto ok = pages.begin()->second->evict();
		ASSERT_ABORT(ok);
	}
	openFiles.erase(filename);
}
