/*
 * AsyncFileCached.actor.cpp
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

#include "AsyncFileCached.actor.h"

//Page caches used in non-simulated environments
Optional<Reference<EvictablePageCache>> pc4k, pc64k;

//The simulator needs to store separate page caches for each machine
static std::map<NetworkAddress, std::pair<Reference<EvictablePageCache>, Reference<EvictablePageCache>>> simulatorPageCaches;

EvictablePage::~EvictablePage() {
	if (data) {
		if (pageCache->pageSize == 4096)
			FastAllocator<4096>::release(data);
		else
			aligned_free(data);
	}
	if (index > -1) {
		pageCache->pages[index] = pageCache->pages.back();
		pageCache->pages[index]->index = index;
		pageCache->pages.pop_back();
	}
}

std::map< std::string, OpenFileInfo > AsyncFileCached::openFiles;

void AsyncFileCached::remove_page( AFCPage* page ) {
	pages.erase( page->pageOffset );
}

Future<Reference<IAsyncFile>> AsyncFileCached::open_impl( std::string filename, int flags, int mode ) {
	Reference<EvictablePageCache> pageCache;

	//In a simulated environment, each machine needs its own caches
	if(g_network->isSimulated()) {
		auto cacheItr = simulatorPageCaches.find(g_network->getLocalAddress());
		if(cacheItr == simulatorPageCaches.end()) {
			int64_t pageCacheSize4k = (BUGGIFY) ? FLOW_KNOBS->BUGGIFY_SIM_PAGE_CACHE_4K : FLOW_KNOBS->SIM_PAGE_CACHE_4K;
			int64_t pageCacheSize64k = (BUGGIFY) ? FLOW_KNOBS->BUGGIFY_SIM_PAGE_CACHE_64K : FLOW_KNOBS->SIM_PAGE_CACHE_64K;
			auto caches = std::make_pair(Reference<EvictablePageCache>(new EvictablePageCache(4096, pageCacheSize4k)), Reference<EvictablePageCache>(new EvictablePageCache(65536, pageCacheSize64k)));
			simulatorPageCaches[g_network->getLocalAddress()] = caches;
			pageCache = (flags & IAsyncFile::OPEN_LARGE_PAGES) ? caches.second : caches.first;
		}
		else
			pageCache = (flags & IAsyncFile::OPEN_LARGE_PAGES) ? cacheItr->second.second : cacheItr->second.first;
	}
	else {
		if(flags & IAsyncFile::OPEN_LARGE_PAGES) {
			if(!pc64k.present()) pc64k = Reference<EvictablePageCache>(new EvictablePageCache(65536, FLOW_KNOBS->PAGE_CACHE_64K));
			pageCache = pc64k.get();
		} else {
			if(!pc4k.present()) pc4k = Reference<EvictablePageCache>(new EvictablePageCache(4096, FLOW_KNOBS->PAGE_CACHE_4K));
			pageCache = pc4k.get();
		}
	}

	return open_impl(filename, flags, mode, pageCache);
}

Future<Void> AsyncFileCached::read_write_impl( AsyncFileCached* self, void* data, int length, int64_t offset, bool writing ) {
	if (writing) {
		if (offset + length > self->length)
			self->length = offset + length;
	}

	std::vector<Future<Void>> actors;

	uint8_t* cdata = static_cast<uint8_t*>(data);

	int offsetInPage = offset % self->pageCache->pageSize;
	int64_t pageOffset = offset - offsetInPage;

	int remaining = length;

	while (remaining) {
		++self->countFileCacheFinds;
		++self->countCacheFinds;
		auto p = self->pages.find( pageOffset );
		if ( p == self->pages.end() ) {
			AFCPage* page = new AFCPage( self, pageOffset );
			p = self->pages.insert( std::make_pair(pageOffset, page) ).first;
		}

		int bytesInPage = std::min(self->pageCache->pageSize - offsetInPage, remaining);

		auto w = writing
			? p->second->write( cdata, bytesInPage, offsetInPage )
			: p->second->read( cdata, bytesInPage, offsetInPage );
		if (!w.isReady() || w.isError())
			actors.push_back( w );

		cdata += bytesInPage;
		pageOffset += self->pageCache->pageSize;
		offsetInPage = 0;

		remaining -= bytesInPage;
	}

	//This is susceptible to the introduction of waits on the read/write path: no wait can occur prior to AFCPage::readThrough
	//or prevLength will be set prematurely
	self->prevLength = self->length;

	return waitForAll( actors );
}

Future<Void> AsyncFileCached::readZeroCopy( void** data, int* length, int64_t offset ) {
	++countFileCacheReads;
	++countCacheReads;

	// Only aligned page reads are zero-copy
	if (*length != pageCache->pageSize || (offset & (pageCache->pageSize-1)) || offset + *length > this->length)
		return io_error();

	auto p = pages.find( offset );
	if ( p == pages.end() ) {
		AFCPage* page = new AFCPage( this, offset );
		p = pages.insert( std::make_pair(offset, page) ).first;
	}

	*data = p->second->data;

	return p->second->readZeroCopy();
}
void AsyncFileCached::releaseZeroCopy( void* data, int length, int64_t offset ) {
	ASSERT( length == pageCache->pageSize && !(offset & (pageCache->pageSize-1)) && offset + length <= this->length);
	auto p = pages.find( offset );
	ASSERT( p != pages.end() && p->second->data == data );
	p->second->releaseZeroCopy();
}

Future<Void> AsyncFileCached::truncate( int64_t size ) {
	++countFileCacheWrites;
	++countCacheWrites;

	std::vector<Future<Void>> actors;

	int offsetInPage = size % pageCache->pageSize;
	int64_t pageOffset = size - offsetInPage;

	if(offsetInPage == 0 && size == length) {
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
		}
		else {
			TEST(true); // Truncating to the middle of a page that isn't in cache
		}

		pageOffset += pageCache->pageSize;
	}
	/*
	for ( auto p = pages.lower_bound( pageOffset ); p != pages.end(); p = pages.erase(p) ) {
		auto f = p->second->truncate();
		if ( !f.isReady() || f.isError())
			actors.push_back( f );
	}
	*/

	for ( auto p = pages.begin(); p != pages.end(); ) {
		if ( p->first >= pageOffset ) {
			auto f = p->second->truncate();
			if ( !f.isReady() || f.isError() )
				actors.push_back( f );
			auto last = p;
			++p;
			pages.erase(last);
		} else
			++p;
	}

	return truncate_impl( this, size, waitForAll( actors ) );
}

Future<Void> AsyncFileCached::flush() {
	++countFileCacheWrites;
	++countCacheWrites;

	std::vector<Future<Void>> unflushed;

	int debug_count = flushable.size();
	for(int i=0; i<flushable.size(); ) {
		auto p = flushable[i];
		auto f = p->flush();
		if (!f.isReady() || f.isError()) unflushed.push_back( f );
		ASSERT( (i<flushable.size() && flushable[i] == p) != f.isReady() );
		if (!f.isReady()) i++;
	}
	ASSERT( flushable.size() <= debug_count );

	return waitForAll(unflushed);
}

Future<Void> AsyncFileCached::quiesce() {
	std::vector<Future<Void>> unquiescent;

	for( auto i = pages.begin(); i != pages.end(); ++i ) {
		auto f = i->second->quiesce();
		if( !f.isReady() ) unquiescent.push_back( f );
	}
	
	//Errors are absorbed because we need everything to finish
	return waitForAllReady(unquiescent);
}

AsyncFileCached::~AsyncFileCached() {
	while ( !pages.empty() ) {
		auto ok = pages.begin()->second->evict();
		ASSERT( ok );
	}
	openFiles.erase( filename );
}
