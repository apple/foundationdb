/*
 * IndirectShadowPager.actor.cpp
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

#include "IndirectShadowPager.h"
#include "Knobs.h"

#include "flow/UnitTest.h"

IndirectShadowPage::IndirectShadowPage() : allocated(true) {
	data = (uint8_t*)FastAllocator<4096>::allocate();
}

IndirectShadowPage::IndirectShadowPage(uint8_t *data) : data(data), allocated(false) {}

IndirectShadowPage::~IndirectShadowPage() {
	if(allocated) {
		FastAllocator<4096>::release(data);
	}
}

uint8_t const* IndirectShadowPage::begin() const {
	return data;
}

uint8_t* IndirectShadowPage::mutate() {
	return data;
}

int IndirectShadowPage::size() const {
	return PAGE_BYTES - PAGE_OVERHEAD_BYTES;
}

const int IndirectShadowPage::PAGE_BYTES = 4096;
const int IndirectShadowPage::PAGE_OVERHEAD_BYTES = 0;

Future<Reference<const IPage>> IndirectShadowPagerSnapshot::getPhysicalPage(LogicalPageID pageID) {
	return pager->getPage(Reference<IndirectShadowPagerSnapshot>::addRef(this), pageID, version);
}

void IndirectShadowPagerSnapshot::invalidateReturnedPages() {
	arena = Arena();
}

template <class T>
T bigEndian(T val) {
	static_assert(sizeof(T) <= 8, "Can't compute bigEndian on integers larger than 8 bytes");
	uint64_t b = bigEndian64(val);
	return *(T*)((uint8_t*)&b+8-sizeof(T));
}

ACTOR Future<Void> recover(IndirectShadowPager *pager) {
	try {
		TraceEvent("PagerRecovering").detail("Basename", pager->basename);
		pager->pageTableLog = openKVStore(KeyValueStoreType::MEMORY, pager->basename, UID(), 1e9);

		// TODO: this can be done synchronously with the log recovery
		int64_t flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_LOCK;
		if(!fileExists(pager->basename + ".redwood")) {
			flags |= IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE;
		}

		Reference<IAsyncFile> dataFile = wait(IAsyncFileSystem::filesystem()->open(pager->basename + ".redwood", flags, 0600));
		pager->dataFile = dataFile;

		TraceEvent("PagerOpenedDataFile").detail("Filename", pager->basename + ".redwood");

		Void _ = wait(pager->dataFile->sync());

		state int64_t fileSize = wait(pager->dataFile->size());
		TraceEvent("PagerGotFileSize").detail("Size", fileSize);

		if(fileSize > 0) {
			TraceEvent("PagerRecoveringFromLogs");
			Optional<Value> pagesAllocatedValue = wait(pager->pageTableLog->readValue(IndirectShadowPager::PAGES_ALLOCATED_KEY));
			if(pagesAllocatedValue.present()) {
				BinaryReader pr(pagesAllocatedValue.get(), Unversioned());
				uint32_t pagesAllocated;
				pr >> pagesAllocated;
				pager->pagerFile.init(fileSize, pagesAllocated);

				debug_printf("Recovered pages allocated: %d\n", pager->pagerFile.pagesAllocated);
				ASSERT(pager->pagerFile.pagesAllocated != PagerFile::INVALID_PAGE);

				Optional<Value> latestVersionValue = wait(pager->pageTableLog->readValue(IndirectShadowPager::LATEST_VERSION_KEY));
				ASSERT(latestVersionValue.present());

				BinaryReader vr(latestVersionValue.get(), Unversioned());
				vr >> pager->latestVersion;

				Optional<Value> oldestVersionValue = wait(pager->pageTableLog->readValue(IndirectShadowPager::OLDEST_VERSION_KEY));

				if(oldestVersionValue.present()) {
					BinaryReader vr(oldestVersionValue.get(), Unversioned());
					vr >> pager->oldestVersion;
				}

				debug_printf("Recovered version info: %d - %d\n", pager->oldestVersion, pager->latestVersion);
				pager->committedVersion = pager->latestVersion;

				Standalone<VectorRef<KeyValueRef>> tableEntries = wait(pager->pageTableLog->readRange(KeyRangeRef(IndirectShadowPager::TABLE_ENTRY_PREFIX, strinc(IndirectShadowPager::TABLE_ENTRY_PREFIX))));

				if(tableEntries.size() > 0) {
					BinaryReader kr(tableEntries.back().key, Unversioned());

					uint8_t prefix;
					LogicalPageID logicalPageID;

					kr >> prefix;
					ASSERT(prefix == IndirectShadowPager::TABLE_ENTRY_PREFIX.begin()[0]);

					kr >> logicalPageID;
					logicalPageID = bigEndian(logicalPageID);

					LogicalPageID pageTableSize = std::max<LogicalPageID>(logicalPageID+1, SERVER_KNOBS->PAGER_RESERVED_PAGES);
					pager->pageTable.resize(pageTableSize);
					debug_printf("Recovered page table size: %d\n", pageTableSize);
				}
				else {
					debug_printf("Recovered no page table entries\n");
				}

				LogicalPageID nextPageID = SERVER_KNOBS->PAGER_RESERVED_PAGES;
				std::set<PhysicalPageID> allocatedPhysicalPages;
				for(auto entry : tableEntries) {
					BinaryReader kr(entry.key, Unversioned());
					BinaryReader vr(entry.value, Unversioned());

					uint8_t prefix;
					LogicalPageID logicalPageID;
					Version version;
					PhysicalPageID physicalPageID;

					kr >> prefix;
					ASSERT(prefix == IndirectShadowPager::TABLE_ENTRY_PREFIX.begin()[0]);

					kr >> logicalPageID;
					logicalPageID = bigEndian(logicalPageID);

					kr >> version;
					version = bigEndian(version);

					vr >> physicalPageID;

					pager->pageTable[logicalPageID].push_back(std::make_pair(version, physicalPageID));
					
					if(physicalPageID != PagerFile::INVALID_PAGE) {
						allocatedPhysicalPages.insert(physicalPageID);
						pager->pagerFile.markPageAllocated(logicalPageID, version, physicalPageID);
					}

					while(nextPageID < logicalPageID) {
						pager->logicalFreeList.push_back(nextPageID++);
					}
					if(logicalPageID == nextPageID) {
						++nextPageID;
					}

					debug_printf("Recovered page table entry %d -> (%d, %d)\n", logicalPageID, version, physicalPageID);
				}

				debug_printf("Building physical free list\n");
				// TODO: can we do this better? does it require storing extra info in the log?
				PhysicalPageID nextPhysicalPageID = 0;
				for(auto itr = allocatedPhysicalPages.begin(); itr != allocatedPhysicalPages.end(); ++itr) {
					while(nextPhysicalPageID < *itr) {
						pager->pagerFile.freePage(nextPhysicalPageID++);
					}
					++nextPhysicalPageID;
				}

				while(nextPhysicalPageID < pager->pagerFile.pagesAllocated) {
					pager->pagerFile.freePage(nextPhysicalPageID++);
				}
			}
		}

		if(pager->pageTable.size() < SERVER_KNOBS->PAGER_RESERVED_PAGES) {
			pager->pageTable.resize(SERVER_KNOBS->PAGER_RESERVED_PAGES);
		}

		pager->pagerFile.finishedMarkingPages();
		pager->pagerFile.startVacuuming();

		debug_printf("Finished recovery\n", pager->oldestVersion);
		TraceEvent("PagerFinishedRecovery");
	}
	catch(Error &e) {
		TraceEvent(SevError, "PagerRecoveryFailed").error(e, true);
		throw e;
	}

	return Void();
}

ACTOR Future<Void> housekeeper(IndirectShadowPager *pager) {
	Void _ = wait(pager->recovery);

	loop {
		state LogicalPageID pageID = 0;
		for(; pageID < pager->pageTable.size(); ++pageID) {
			// TODO: pick an appropriate rate for this loop and determine the right way to implement it
			// Right now, this delays 10ms every 400K pages, which means we have 1s of delay for every
			// 40M pages. In total, we introduce 100s delay for a max size 4B page file.
			if(pageID % 400000 == 0) {
				Void _ = wait(delay(0.01)); 
			}
			else {
				Void _ = wait(yield());
			}

			auto& pageVersionMap = pager->pageTable[pageID];

			if(pageVersionMap.size() > 0) {
				auto itr = pageVersionMap.begin();
				for(auto prev = itr; prev != pageVersionMap.end() && prev->first < pager->oldestVersion; prev=itr) {
					pager->pagerFile.markPageAllocated(pageID, itr->first, itr->second);
					++itr;
					if(prev->second != PagerFile::INVALID_PAGE && (itr == pageVersionMap.end() || itr->first <= pager->oldestVersion)) {
						pager->freePhysicalPageID(prev->second);
					}
					if(itr == pageVersionMap.end() || itr->first >= pager->oldestVersion) {
						//debug_printf("Updating oldest version for logical page %d: %d\n", pageID, pager->oldestVersion);
						pager->logPageTableClear(pageID, 0, pager->oldestVersion);

						if(itr != pageVersionMap.end() && itr->first > pager->oldestVersion) {
							//debug_printf("Erasing pages to prev from pageVersionMap for %d (itr=%d, prev=%d)\n", pageID, itr->first, prev->first);
							prev->first = pager->oldestVersion;
							pager->logPageTableUpdate(pageID, pager->oldestVersion, prev->second);
							itr = pageVersionMap.erase(pageVersionMap.begin(), prev);
						}
						else {
							//debug_printf("Erasing pages to itr from pageVersionMap for %d (%d) (itr=%d, prev=%d)\n", pageID, itr == pageVersionMap.end(), itr==pageVersionMap.end() ? -1 : itr->first, prev->first);
							itr = pageVersionMap.erase(pageVersionMap.begin(), itr);
						}
					}
				}

				for(; itr != pageVersionMap.end(); ++itr) {
					pager->pagerFile.markPageAllocated(pageID, itr->first, itr->second);
				}

				if(pageVersionMap.size() == 0) {
					pager->freeLogicalPageID(pageID);
				}
			}
		}

		pager->pagerFile.finishedMarkingPages();
	}
}

ACTOR Future<Void> forwardError(Future<Void> f, Promise<Void> target) {
	try {
		Void _ = wait(f);
	}
	catch(Error &e) {
		if(target.canBeSet()) {
			target.sendError(e);
		}

		throw e;
	}

	return Void();
}

IndirectShadowPager::IndirectShadowPager(std::string basename) 
	: basename(basename), latestVersion(0), committedVersion(0), committing(Void()), oldestVersion(0), pagerFile(this)
{
	recovery = forwardError(recover(this), errorPromise);
	housekeeping = forwardError(housekeeper(this), errorPromise);
}

Reference<IPage> IndirectShadowPager::newPageBuffer() {
	return Reference<IPage>(new IndirectShadowPage());
}

int IndirectShadowPager::getUsablePageSize() {
	return IndirectShadowPage::PAGE_BYTES - IndirectShadowPage::PAGE_OVERHEAD_BYTES;
}

Reference<IPagerSnapshot> IndirectShadowPager::getReadSnapshot(Version version) {
	ASSERT(recovery.isReady());
	ASSERT(version <= latestVersion);
	ASSERT(version >= oldestVersion);

	return Reference<IPagerSnapshot>(new IndirectShadowPagerSnapshot(this, version));
}

LogicalPageID IndirectShadowPager::allocateLogicalPage() {
	ASSERT(recovery.isReady());

	LogicalPageID allocatedPage;
	if(logicalFreeList.size() > 0) {
		allocatedPage = logicalFreeList.front();
		logicalFreeList.pop_front();
	}
	else {
		ASSERT(pageTable.size() < std::numeric_limits<LogicalPageID>::max()); // TODO: different error?
		allocatedPage = pageTable.size();
		pageTable.push_back(PageVersionMap());
	}

	ASSERT(allocatedPage >= SERVER_KNOBS->PAGER_RESERVED_PAGES);
	return allocatedPage;
}

void IndirectShadowPager::freeLogicalPage(LogicalPageID pageID, Version version) {
	ASSERT(recovery.isReady());
	ASSERT(committing.isReady());

	ASSERT(pageID < pageTable.size());

	PageVersionMap &pageVersionMap = pageTable[pageID];
	ASSERT(!pageVersionMap.empty());

	auto itr = pageVersionMapLowerBound(pageVersionMap, version);
	for(auto i = itr; i != pageVersionMap.end(); ++i) {
		freePhysicalPageID(i->second);
	}

	if(itr != pageVersionMap.end()) {
		//debug_printf("Clearing newest versions for logical page %d: %d\n", pageID, version);
		logPageTableClearToEnd(pageID, version);
		pageVersionMap.erase(itr, pageVersionMap.end());
	}
	
	if(pageVersionMap.size() == 0) {
		debug_printf("Freeing logical page %d (freeLogicalPage)\n", pageID);
		logicalFreeList.push_back(pageID);
	}
	else if(pageVersionMap.back().second != PagerFile::INVALID_PAGE) {
		pageVersionMap.push_back(std::make_pair(version, PagerFile::INVALID_PAGE));
		logPageTableUpdate(pageID, version, PagerFile::INVALID_PAGE);
	}
}

ACTOR Future<Void> waitAndFreePhysicalPageID(IndirectShadowPager *pager, PhysicalPageID pageID, Future<Void> canFree) {
	Void _ = wait(canFree);
	pager->pagerFile.freePage(pageID);
	return Void();
}

// TODO: we need to be more careful about making sure the action that caused the page to be freed is durable before freeing the page
// Otherwise, the page could be rewritten prematurely.
void IndirectShadowPager::freePhysicalPageID(PhysicalPageID pageID) {
	debug_printf("Freeing physical page: %u\n", pageID);
	auto itr = readCounts.find(pageID);
	if(itr != readCounts.end()) {
		operations.add(waitAndFreePhysicalPageID(this, pageID, itr->second.second.getFuture()));
	}
	else {
		pagerFile.freePage(pageID);
	}
}

void IndirectShadowPager::writePage(LogicalPageID pageID, Reference<IPage> contents, Version updateVersion) {
	ASSERT(recovery.isReady());
	ASSERT(committing.isReady());

	ASSERT(updateVersion > latestVersion || updateVersion == 0);
	ASSERT(pageID < pageTable.size());

	PageVersionMap &pageVersionMap = pageTable[pageID];

	ASSERT(pageVersionMap.empty() || pageVersionMap.back().second != PagerFile::INVALID_PAGE);

	// TODO: should this be conditional on the write succeeding?
	bool updateExisting = updateVersion == 0;
	if(updateExisting) {
		ASSERT(pageVersionMap.size());
		updateVersion = pageVersionMap.back().first;
	}

	PhysicalPageID physicalPageID = pagerFile.allocatePage(pageID, updateVersion);

	if(updateExisting) {
		freePhysicalPageID(pageVersionMap.back().second);
		pageVersionMap.back().second = physicalPageID;
	}
	else {
		ASSERT(pageVersionMap.empty() || pageVersionMap.back().first < updateVersion);
		pageVersionMap.push_back(std::make_pair(updateVersion, physicalPageID));
	}

	logPageTableUpdate(pageID, updateVersion, physicalPageID);
	Future<Void> write = dataFile->write(contents->begin(), IndirectShadowPage::PAGE_BYTES, physicalPageID * IndirectShadowPage::PAGE_BYTES);

	if(write.isError()) {
		if(errorPromise.canBeSet()) {
			errorPromise.sendError(write.getError());
		}
		throw write.getError();
	}
	writeActors.add(forwardError(write, errorPromise));
}

void IndirectShadowPager::forgetVersions(Version begin, Version end) {
	ASSERT(recovery.isReady());
	ASSERT(begin <= end);
	ASSERT(end <= latestVersion);

	// TODO: support forgetting arbitrary ranges
	if(begin <= oldestVersion) {
		oldestVersion = std::max(end, oldestVersion);
		logVersion(OLDEST_VERSION_KEY, oldestVersion);
	}
}

ACTOR Future<Void> commitImpl(IndirectShadowPager *pager, Future<Void> previousCommit) {
	state Future<Void> outstandingWrites = pager->writeActors.signalAndCollapse();
	state Version commitVersion = pager->latestVersion;

	Void _ = wait(previousCommit);

	pager->logVersion(IndirectShadowPager::LATEST_VERSION_KEY, commitVersion);

	// TODO: we need to prevent writes that happen now from being committed in the subsequent log commit
	// This is probably best done once we have better control of the log, where we can write a commit entry
	// here without syncing the file.

	Void _ = wait(outstandingWrites);

	Void _ = wait(pager->dataFile->sync());
	Void _ = wait(pager->pageTableLog->commit());
	
	pager->committedVersion = std::max(pager->committedVersion, commitVersion);

	return Void();
}

Future<Void> IndirectShadowPager::commit() {
	ASSERT(recovery.isReady());
	Future<Void> f = commitImpl(this, committing);
	committing = f;
	return committing;
}

void IndirectShadowPager::setLatestVersion(Version version) {
	ASSERT(recovery.isReady());
	latestVersion = version;
}

ACTOR Future<Version> getLatestVersionImpl(IndirectShadowPager *pager) {
	Void _ = wait(pager->recovery);
	return pager->latestVersion;
}

Future<Version> IndirectShadowPager::getLatestVersion() {
	return getLatestVersionImpl(this);
}

Future<Void> IndirectShadowPager::getError() {
	return errorPromise.getFuture();
}

Future<Void> IndirectShadowPager::onClosed() {
	return closed.getFuture();
}

ACTOR void shutdown(IndirectShadowPager *pager, bool dispose) {
	pager->recovery = Never(); // TODO: this is a hacky way to prevent users from performing operations after calling shutdown. Implement a better mechanism 

	Void _ = wait(pager->writeActors.signal());
	Void _ = wait(pager->operations.signal());
	Void _ = wait(pager->committing);

	pager->housekeeping.cancel();
	pager->pagerFile.shutdown();

	state Future<Void> pageTableClosed = pager->pageTableLog->onClosed();
	if(dispose) {
		Void _ = wait(IAsyncFileSystem::filesystem()->deleteFile(pager->basename + ".redwood", true));
		pager->pageTableLog->dispose();
	}
	else {
		pager->pageTableLog->close();
	}

	Void _ = wait(pageTableClosed);

	pager->closed.send(Void());
	delete pager;
}

void IndirectShadowPager::dispose() {
	shutdown(this, true);
}

void IndirectShadowPager::close() {
	shutdown(this, false);
}

ACTOR Future<Reference<const IPage>> getPageImpl(IndirectShadowPager *pager, Reference<IndirectShadowPagerSnapshot> snapshot, LogicalPageID pageID, Version version) {
	ASSERT(pageID < pager->pageTable.size());
	PageVersionMap &pageVersionMap = pager->pageTable[pageID];

	auto itr = IndirectShadowPager::pageVersionMapUpperBound(pageVersionMap, version);
	if(itr == pageVersionMap.begin()) {
		ASSERT(false);
	}

	--itr;

	state PhysicalPageID physicalPageID = itr->second;
	//debug_printf("Reading %d at v%d from %d\n", pageID, version, physicalPageID);
	
	ASSERT(physicalPageID != PagerFile::INVALID_PAGE);
	++pager->readCounts[physicalPageID].first;

	// We are relying on the use of AsyncFileCached for performance here. We expect that all write actors will complete immediately (with a possible yield()),
	// so this wait should either be nonexistent or just a yield.
	Void _ = wait(pager->writeActors.signalAndCollapse());

	state uint8_t *buf = new (snapshot->arena) uint8_t[IndirectShadowPage::PAGE_BYTES];
	// TODO:  Use readZeroCopy but fall back ton read().  Releasing pages should releaseZeroCopy for successful zero copy reads
	int read = wait(pager->dataFile->read(buf, IndirectShadowPage::PAGE_BYTES, physicalPageID * IndirectShadowPage::PAGE_BYTES));
	ASSERT(read == IndirectShadowPage::PAGE_BYTES);

	auto readCountItr = pager->readCounts.find(physicalPageID);
	ASSERT(readCountItr != pager->readCounts.end());
	if(readCountItr->second.first == 1) {
		readCountItr->second.second.send(Void());
		pager->readCounts.erase(readCountItr);
	}
	else {
		--readCountItr->second.first;
	}

	return Reference<const IPage>(new IndirectShadowPage(buf));
}

Future<Reference<const IPage>> IndirectShadowPager::getPage(Reference<IndirectShadowPagerSnapshot> snapshot, LogicalPageID pageID, Version version) {
	ASSERT(recovery.isReady());

	Future<Reference<const IPage>> f = getPageImpl(this, snapshot, pageID, version);
	operations.add(success(f));
	return f;
}

PageVersionMap::iterator IndirectShadowPager::pageVersionMapLowerBound(PageVersionMap &pageVersionMap, Version version) {
	return std::lower_bound(pageVersionMap.begin(), pageVersionMap.end(), version, [](std::pair<Version, PhysicalPageID> p, Version v) {
		return p.first < v;
	});
}

PageVersionMap::iterator IndirectShadowPager::pageVersionMapUpperBound(PageVersionMap &pageVersionMap, Version version) {
	return std::upper_bound(pageVersionMap.begin(), pageVersionMap.end(), version, [](Version v, std::pair<Version, PhysicalPageID> p) {
		return v < p.first;
	});
}

void IndirectShadowPager::freeLogicalPageID(LogicalPageID pageID) {
	if(pageID >= SERVER_KNOBS->PAGER_RESERVED_PAGES) {
		debug_printf("Freeing logical page: %u\n", pageID);
		logicalFreeList.push_back(pageID);
	}
}

void IndirectShadowPager::logVersion(StringRef versionKey, Version version) {
	BinaryWriter v(Unversioned());
	v << version;

	pageTableLog->set(KeyValueRef(versionKey, v.toStringRef()));
}

void IndirectShadowPager::logPagesAllocated() {
	BinaryWriter v(Unversioned());
	v << pagerFile.getPagesAllocated();

	pageTableLog->set(KeyValueRef(PAGES_ALLOCATED_KEY, v.toStringRef()));
}

void IndirectShadowPager::logPageTableUpdate(LogicalPageID logicalPageID, Version version, PhysicalPageID physicalPageID) {
	BinaryWriter k(Unversioned());
	k << TABLE_ENTRY_PREFIX.begin()[0] << bigEndian(logicalPageID) << bigEndian(version);

	BinaryWriter v(Unversioned());
	v << physicalPageID;

	pageTableLog->set(KeyValueRef(k.toStringRef(), v.toStringRef()));
}

void IndirectShadowPager::logPageTableClearToEnd(LogicalPageID logicalPageID, Version start) {
	BinaryWriter b(Unversioned());
	b << TABLE_ENTRY_PREFIX.begin()[0] << bigEndian(logicalPageID) << bigEndian(start);

	BinaryWriter e(Unversioned());
	e << TABLE_ENTRY_PREFIX.begin()[0] << bigEndian(logicalPageID);

	pageTableLog->clear(KeyRangeRef(b.toStringRef(), strinc(e.toStringRef())));
}

void IndirectShadowPager::logPageTableClear(LogicalPageID logicalPageID, Version start, Version end) {
	BinaryWriter b(Unversioned());
	b << TABLE_ENTRY_PREFIX.begin()[0] << bigEndian(logicalPageID) << bigEndian(start);

	BinaryWriter e(Unversioned());
	e << TABLE_ENTRY_PREFIX.begin()[0] << bigEndian(logicalPageID) << bigEndian(end);

	pageTableLog->clear(KeyRangeRef(b.toStringRef(), e.toStringRef()));
}

const StringRef IndirectShadowPager::LATEST_VERSION_KEY = LiteralStringRef("\xff/LatestVersion");
const StringRef IndirectShadowPager::OLDEST_VERSION_KEY = LiteralStringRef("\xff/OldestVersion");
const StringRef IndirectShadowPager::PAGES_ALLOCATED_KEY = LiteralStringRef("\xff/PagesAllocated");
const StringRef IndirectShadowPager::TABLE_ENTRY_PREFIX = LiteralStringRef("\x00");

ACTOR Future<Void> copyPage(IndirectShadowPager *pager, Reference<IPage> page, PhysicalPageID from, PhysicalPageID to) {
	state bool zeroCopied = true;
	state int bytes = IndirectShadowPage::PAGE_BYTES;
	state void *data = nullptr;

	try {
		try {
			Void _ = wait(pager->dataFile->readZeroCopy(&data, &bytes, from * IndirectShadowPage::PAGE_BYTES));
		}
		catch(Error &e) {
			zeroCopied = false;
			data = page->mutate();
			int _bytes = wait(pager->dataFile->read(data, page->size(), from * IndirectShadowPage::PAGE_BYTES));
			bytes = _bytes;
		}

		ASSERT(bytes == IndirectShadowPage::PAGE_BYTES);
		Void _ = wait(pager->dataFile->write(data, bytes, to * IndirectShadowPage::PAGE_BYTES));
		if(zeroCopied) {
			pager->dataFile->releaseZeroCopy(data, bytes, from * IndirectShadowPage::PAGE_BYTES);
		}
	}
	catch(Error &e) {
		if(zeroCopied) {
			pager->dataFile->releaseZeroCopy(data, bytes, from * IndirectShadowPage::PAGE_BYTES);
		}
		pager->pagerFile.freePage(to);
		throw e;
	}

	return Void();
}

ACTOR Future<Void> vacuumer(IndirectShadowPager *pager, PagerFile *pagerFile) {
	state Reference<IPage> page(new IndirectShadowPage());

	loop {
		state double start = now();
		while(!pagerFile->canVacuum()) {
			Void _ = wait(delay(1.0));
		}

		ASSERT(!pagerFile->freePages.empty());

		if(!pagerFile->vacuumQueue.empty()) {
			state PhysicalPageID lastUsedPage = pagerFile->vacuumQueue.rbegin()->first;
			PhysicalPageID lastFreePage = *pagerFile->freePages.rbegin();
			//debug_printf("Vacuuming: evaluating (free list size=%u, lastFreePage=%u, lastUsedPage=%u, pagesAllocated=%u)\n", pagerFile->freePages.size(), lastFreePage, lastUsedPage, pagerFile->pagesAllocated);
			ASSERT(lastFreePage < pagerFile->pagesAllocated);
			ASSERT(lastUsedPage < pagerFile->pagesAllocated);
			ASSERT(lastFreePage != lastUsedPage);

			if(lastFreePage < lastUsedPage) {
				state std::pair<LogicalPageID, Version> logicalPageInfo = pagerFile->vacuumQueue[lastUsedPage];
				state PhysicalPageID newPage = pagerFile->allocatePage(logicalPageInfo.first, logicalPageInfo.second);

				debug_printf("Vacuuming: copying page %u to %u\n", lastUsedPage, newPage);
				Void _ = wait(copyPage(pager, page, lastUsedPage, newPage));

				auto &pageVersionMap = pager->pageTable[logicalPageInfo.first];
				auto itr = IndirectShadowPager::pageVersionMapLowerBound(pageVersionMap, logicalPageInfo.second);
				if(itr != pageVersionMap.end() && itr->second == lastUsedPage) {
					itr->second = newPage;
					pager->logPageTableUpdate(logicalPageInfo.first, itr->first, newPage);
					pagerFile->freePage(lastUsedPage);
				}
				else {
					TEST(true); // page was freed while vacuuming
					pagerFile->freePage(newPage);
				}
			}
		}

		PhysicalPageID firstFreePage = pagerFile->vacuumQueue.empty() ? pagerFile->minVacuumQueuePage : (pagerFile->vacuumQueue.rbegin()->first + 1);
		ASSERT(pagerFile->pagesAllocated >= firstFreePage);

		uint64_t pagesToErase = 0;
		if(pagerFile->freePages.size() >= SERVER_KNOBS->FREE_PAGE_VACUUM_THRESHOLD) {
			pagesToErase = std::min<uint64_t>(pagerFile->freePages.size() - SERVER_KNOBS->FREE_PAGE_VACUUM_THRESHOLD + 1, pagerFile->pagesAllocated - firstFreePage);
		}

		//debug_printf("Vacuuming: got %u pages to erase (freePages=%u, pagesAllocated=%u, vacuumQueueEmpty=%u, minVacuumQueuePage=%u, firstFreePage=%u)\n", pagesToErase, pagerFile->freePages.size(), pagerFile->pagesAllocated, pagerFile->vacuumQueue.empty(), pagerFile->minVacuumQueuePage, firstFreePage);

		if(pagesToErase > 0) {
			PhysicalPageID eraseStartPage = pagerFile->pagesAllocated - pagesToErase;
			debug_printf("Vacuuming: truncating last %u pages starting at %u\n", pagesToErase, eraseStartPage);

			ASSERT(pagesToErase <= pagerFile->pagesAllocated);

			pagerFile->pagesAllocated = eraseStartPage;
			pager->logPagesAllocated();

			auto freePageItr = pagerFile->freePages.find(eraseStartPage);
			ASSERT(freePageItr != pagerFile->freePages.end());

			pagerFile->freePages.erase(freePageItr, pagerFile->freePages.end());
			ASSERT(pagerFile->vacuumQueue.empty() || pagerFile->vacuumQueue.rbegin()->first < eraseStartPage);

			Void _ = wait(pager->dataFile->truncate(pagerFile->pagesAllocated * IndirectShadowPage::PAGE_BYTES));
		}

		Void _ = wait(delayUntil(start + (double)IndirectShadowPage::PAGE_BYTES / SERVER_KNOBS->VACUUM_BYTES_PER_SECOND)); // TODO: figure out the correct mechanism here
	}
}

PagerFile::PagerFile(IndirectShadowPager *pager) : fileSize(0), pagesAllocated(0), pager(pager), vacuumQueueReady(false), minVacuumQueuePage(0) {}

PhysicalPageID PagerFile::allocatePage(LogicalPageID logicalPageID, Version version) {
	ASSERT(pagesAllocated * IndirectShadowPage::PAGE_BYTES <= fileSize);
	ASSERT(fileSize % IndirectShadowPage::PAGE_BYTES == 0);

	PhysicalPageID allocatedPage;
	if(!freePages.empty()) {
		allocatedPage = *freePages.begin();
		freePages.erase(freePages.begin());
	}
	else {
		if(pagesAllocated * IndirectShadowPage::PAGE_BYTES == fileSize) {
			fileSize += (1 << 24);
			// TODO: extend the file before writing beyond the end.
		}

		ASSERT(pagesAllocated < INVALID_PAGE); // TODO: we should throw a better error here
		allocatedPage = pagesAllocated++;
		pager->logPagesAllocated();
	}

	markPageAllocated(logicalPageID, version, allocatedPage);

	debug_printf("Allocated physical page %u\n", allocatedPage);
	return allocatedPage;
}

void PagerFile::freePage(PhysicalPageID pageID) {
	freePages.insert(pageID);

	if(pageID >= minVacuumQueuePage) {
		vacuumQueue.erase(pageID);
	}
}

void PagerFile::markPageAllocated(LogicalPageID logicalPageID, Version version, PhysicalPageID physicalPageID) {
	if(physicalPageID != INVALID_PAGE && physicalPageID >= minVacuumQueuePage) {
		vacuumQueue[physicalPageID] = std::make_pair(logicalPageID, version);
	}
}

void PagerFile::finishedMarkingPages() {
	if(minVacuumQueuePage >= pagesAllocated) {
		minVacuumQueuePage = pagesAllocated >= SERVER_KNOBS->VACUUM_QUEUE_SIZE ? pagesAllocated - SERVER_KNOBS->VACUUM_QUEUE_SIZE : 0;
		vacuumQueueReady = false;
	}
	else {
		if(!vacuumQueueReady) {
			vacuumQueueReady = true;
		}
		if(pagesAllocated > SERVER_KNOBS->VACUUM_QUEUE_SIZE && minVacuumQueuePage < pagesAllocated - SERVER_KNOBS->VACUUM_QUEUE_SIZE) {
			minVacuumQueuePage = pagesAllocated - SERVER_KNOBS->VACUUM_QUEUE_SIZE;
			auto itr = vacuumQueue.lower_bound(minVacuumQueuePage);
			vacuumQueue.erase(vacuumQueue.begin(), itr);
		}
	}
}

uint64_t PagerFile::size() {
	return fileSize;
}

uint32_t PagerFile::getPagesAllocated() {
	return pagesAllocated;
}

void PagerFile::init(uint64_t fileSize, uint32_t pagesAllocated) {
	this->fileSize = fileSize;
	this->pagesAllocated = pagesAllocated;
	this->minVacuumQueuePage = pagesAllocated >= SERVER_KNOBS->VACUUM_QUEUE_SIZE ? pagesAllocated - SERVER_KNOBS->VACUUM_QUEUE_SIZE : 0;
}

void PagerFile::startVacuuming() {
	vacuuming = vacuumer(pager, this);
}

void PagerFile::shutdown() {
	vacuuming.cancel();
}

bool PagerFile::canVacuum() {
	if(freePages.size() < SERVER_KNOBS->FREE_PAGE_VACUUM_THRESHOLD // Not enough free pages
		|| minVacuumQueuePage >= pagesAllocated // We finished processing all pages in the vacuum queue
		|| !vacuumQueueReady) // Populating vacuum queue
	{
		//debug_printf("Vacuuming: waiting for vacuumable pages (free list size=%u, minVacuumQueuePage=%u, pages allocated=%u, vacuumQueueReady=%d)\n", freePages.size(), minVacuumQueuePage, pagesAllocated, vacuumQueueReady);
		return false;
	}

	return true;
}

const PhysicalPageID PagerFile::INVALID_PAGE = std::numeric_limits<PhysicalPageID>::max();

extern Future<Void> simplePagerTest(IPager* const& pager);

TEST_CASE("fdbserver/indirectshadowpager/simple") {
	state IPager *pager = new IndirectShadowPager("data/test");

	Void _ = wait(simplePagerTest(pager));

	Future<Void> closedFuture = pager->onClosed();
	pager->close();
	Void _ = wait(closedFuture);

	return Void();
}
