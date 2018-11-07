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

#include "fdbserver/IndirectShadowPager.h"
#include "fdbserver/Knobs.h"

#include "flow/UnitTest.h"
#include "flow/actorcompiler.h"

struct SumType {
	bool operator==(const SumType &rhs) const { return part1 == rhs.part1 && part2 == rhs.part2; }
	uint32_t part1;
	uint32_t part2;
	std::string toString() { return format("0x%08x%08x", part1, part2); }
};

bool checksum(IAsyncFile *file, uint8_t *page, int pageSize, LogicalPageID logical, PhysicalPageID physical, bool write) {
	// Calculates and then stores or verifies the checksum at the end of the page.
	// If write is true then the checksum is written into the page
	// If write is false then the checksum is compared to the in-page sum and
	// and error will be thrown if they do not match.
	ASSERT(sizeof(SumType) == IndirectShadowPage::PAGE_OVERHEAD_BYTES);
	// Adjust pageSize to refer to only usable storage bytes
	pageSize -= IndirectShadowPage::PAGE_OVERHEAD_BYTES;
	SumType sum;
	SumType *pSumInPage = (SumType *)(page + pageSize);

	// Write sum directly to page or to sum variable based on mode
	SumType *sumOut = write ? pSumInPage : &sum;
	sumOut->part1 = physical;
	sumOut->part2 = logical; 
	hashlittle2(page, pageSize, &sumOut->part1, &sumOut->part2);

	debug_printf("checksum %s%s logical %d physical %d size %d checksums page %s calculated %s data at %p %s\n",
		write ? "write" : "read", (!write && sum != *pSumInPage) ? " MISMATCH" : "", logical, physical, pageSize, write ? "NA" : pSumInPage->toString().c_str(), sumOut->toString().c_str(), page, "" /*StringRef((uint8_t *)page, pageSize).toHexString().c_str()*/);

	// Verify if not in write mode
	if(!write && sum != *pSumInPage) {
		TraceEvent (SevError, "IndirectShadowPagerPageChecksumFailure")
			.detail("UserPageSize", pageSize)
			.detail("Filename", file->getFilename())
			.detail("LogicalPage", logical)
			.detail("PhysicalPage", physical)
			.detail("ChecksumInPage", pSumInPage->toString())
			.detail("ChecksumCalculated", sum.toString());
		return false;
	}
	return true;
}

inline bool checksumRead(IAsyncFile *file, uint8_t *page, int pageSize, LogicalPageID logical, PhysicalPageID physical) {
	return checksum(file, page, pageSize, logical, physical, false);
}

inline void checksumWrite(IAsyncFile *file, uint8_t *page, int pageSize, LogicalPageID logical, PhysicalPageID physical) {
	checksum(file, page, pageSize, logical, physical, true);
}

IndirectShadowPage::IndirectShadowPage() : fastAllocated(true) {
	data = (uint8_t*)FastAllocator<4096>::allocate();
#if VALGRIND
	// Prevent valgrind errors caused by writing random unneeded bytes to disk.
	memset(data, 0, size());
#endif
}

IndirectShadowPage::~IndirectShadowPage() {
	if(fastAllocated) {
		FastAllocator<4096>::release(data);
	}
	else if(file) {
		file->releaseZeroCopy(data, PAGE_BYTES, (int64_t) physicalPageID * PAGE_BYTES);
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
const int IndirectShadowPage::PAGE_OVERHEAD_BYTES = sizeof(SumType);

IndirectShadowPagerSnapshot::IndirectShadowPagerSnapshot(IndirectShadowPager *pager, Version version)
	: pager(pager), version(version), pagerError(pager->getError())
{
}

Future<Reference<const IPage>> IndirectShadowPagerSnapshot::getPhysicalPage(LogicalPageID pageID) {
	if(pagerError.isReady())
		pagerError.get();
	return pager->getPage(Reference<IndirectShadowPagerSnapshot>::addRef(this), pageID, version);
}

template <class T>
T bigEndian(T val) {
	static_assert(sizeof(T) <= 8, "Can't compute bigEndian on integers larger than 8 bytes");
	uint64_t b = bigEndian64(val);
	return *(T*)((uint8_t*)&b+8-sizeof(T));
}

ACTOR Future<Void> recover(IndirectShadowPager *pager) {
	try {
		TraceEvent("PagerRecovering").detail("Filename", pager->pageFileName);
		pager->pageTableLog = keyValueStoreMemory(pager->basename, UID(), 1e9, "pagerlog");

		// TODO: this can be done synchronously with the log recovery
		int64_t flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_LOCK;
		state bool exists = fileExists(pager->pageFileName);
		if(!exists) {
			flags |= IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE;
		}

		Reference<IAsyncFile> dataFile = wait(IAsyncFileSystem::filesystem()->open(pager->pageFileName, flags, 0600));
		pager->dataFile = dataFile;

		TraceEvent("PagerOpenedDataFile").detail("Filename", pager->pageFileName);

		if(!exists) {
			wait(pager->dataFile->sync());
		}
		TraceEvent("PagerSyncdDataFile").detail("Filename", pager->pageFileName);

		state int64_t fileSize = wait(pager->dataFile->size());
		TraceEvent("PagerGotFileSize").detail("Size", fileSize).detail("Filename", pager->pageFileName);

		if(fileSize > 0) {
			TraceEvent("PagerRecoveringFromLogs").detail("Filename", pager->pageFileName);
			Optional<Value> pagesAllocatedValue = wait(pager->pageTableLog->readValue(IndirectShadowPager::PAGES_ALLOCATED_KEY));
			if(pagesAllocatedValue.present()) {
				BinaryReader pr(pagesAllocatedValue.get(), Unversioned());
				uint32_t pagesAllocated;
				pr >> pagesAllocated;
				pager->pagerFile.init(fileSize, pagesAllocated);

				debug_printf("%s: Recovered pages allocated: %d\n", pager->pageFileName.c_str(), pager->pagerFile.pagesAllocated);
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

				debug_printf("%s: Recovered version info: earliest v%lld  latest v%lld\n", pager->pageFileName.c_str(), pager->oldestVersion, pager->latestVersion);
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
					debug_printf("%s: Recovered page table size: %d\n", pager->pageFileName.c_str(), pageTableSize);
				}
				else {
					debug_printf("%s: Recovered no page table entries\n", pager->pageFileName.c_str());
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

					ASSERT(version <= pager->latestVersion);

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

					debug_printf("%s: Recovered page table entry logical %d -> (v%lld, physical %d)\n", pager->pageFileName.c_str(), logicalPageID, version, physicalPageID);
				}

				debug_printf("%s: Building physical free list\n", pager->pageFileName.c_str());
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

		debug_printf("%s: Finished recovery at v%lld\n", pager->pageFileName.c_str(), pager->latestVersion);
		TraceEvent("PagerFinishedRecovery").detail("LatestVersion", pager->latestVersion).detail("OldestVersion", pager->oldestVersion).detail("Filename", pager->pageFileName);
	}
	catch(Error &e) {
		if(e.code() != error_code_actor_cancelled) {
			TraceEvent(SevError, "PagerRecoveryFailed").error(e, true).detail("Filename", pager->pageFileName);
		}
		throw;
	}

	return Void();
}

ACTOR Future<Void> housekeeper(IndirectShadowPager *pager) {
	wait(pager->recovery);

	loop {
		state LogicalPageID pageID = 0;
		for(; pageID < pager->pageTable.size(); ++pageID) {
			// TODO: pick an appropriate rate for this loop and determine the right way to implement it
			// Right now, this delays 10ms every 400K pages, which means we have 1s of delay for every
			// 40M pages. In total, we introduce 100s delay for a max size 4B page file.
			if(pageID % 400000 == 0) {
				wait(delay(0.01)); 
			}
			else {
				wait(yield());
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
						debug_printf("%s: Updating oldest version for logical %u: v%lld\n", pager->pageFileName.c_str(), pageID, pager->oldestVersion);
						pager->logPageTableClear(pageID, 0, pager->oldestVersion);

						if(itr != pageVersionMap.end() && itr->first > pager->oldestVersion) {
							debug_printf("%s: Erasing pages to prev from pageVersionMap for %d (itr=%lld, prev=%lld)\n", pager->pageFileName.c_str(), pageID, itr->first, prev->first);
							prev->first = pager->oldestVersion;
							pager->logPageTableUpdate(pageID, pager->oldestVersion, prev->second);
							itr = pageVersionMap.erase(pageVersionMap.begin(), prev);
						}
						else {
							debug_printf("%s: Erasing pages to itr from pageVersionMap for %d (%d) (itr=%lld, prev=%lld)\n", pager->pageFileName.c_str(), pageID, itr == pageVersionMap.end(), itr==pageVersionMap.end() ? -1 : itr->first, prev->first);
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
		wait(f);
	}
	catch(Error &e) {
		if(e.code() != error_code_actor_cancelled && target.canBeSet()) {
			target.sendError(e);
		}

		throw e;
	}

	return Void();
}

IndirectShadowPager::IndirectShadowPager(std::string basename) 
	: basename(basename), latestVersion(0), committedVersion(0), committing(Void()), oldestVersion(0), pagerFile(this)
{
	pageFileName = basename;
	recovery = forwardError(recover(this), errorPromise);
	housekeeping = forwardError(housekeeper(this), errorPromise);
}

StorageBytes IndirectShadowPager::getStorageBytes() {
	int64_t free;
	int64_t total;
	g_network->getDiskBytes(parentDirectory(basename), free, total);
	return StorageBytes(free, total, pagerFile.size(), free + IndirectShadowPage::PAGE_BYTES * pagerFile.getFreePages());
}

Reference<IPage> IndirectShadowPager::newPageBuffer() {
	return Reference<IPage>(new IndirectShadowPage());
}

int IndirectShadowPager::getUsablePageSize() {
	return IndirectShadowPage::PAGE_BYTES - IndirectShadowPage::PAGE_OVERHEAD_BYTES;
}

Reference<IPagerSnapshot> IndirectShadowPager::getReadSnapshot(Version version) {
	debug_printf("%s: Getting read snapshot v%lld  latest v%lld  oldest v%lld\n", pageFileName.c_str(), version, latestVersion, oldestVersion);
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
	debug_printf("%s: op=allocate id=%u\n", pageFileName.c_str(), allocatedPage);
	return allocatedPage;
}

void IndirectShadowPager::freeLogicalPage(LogicalPageID pageID, Version version) {
	ASSERT(recovery.isReady());
	ASSERT(committing.isReady());

	ASSERT(pageID < pageTable.size());

	PageVersionMap &pageVersionMap = pageTable[pageID];
	ASSERT(!pageVersionMap.empty());

	// 0 will mean delete as of latest version, similar to write at latest version
	if(version == 0) {
		version = pageVersionMap.back().first;
	}

	auto itr = pageVersionMapLowerBound(pageVersionMap, version);
	// TODO:  Is this correct, that versions from the past *forward* can be deleted?
	for(auto i = itr; i != pageVersionMap.end(); ++i) {
		freePhysicalPageID(i->second);
	}

	if(itr != pageVersionMap.end()) {
		debug_printf("%s: Clearing newest versions for logical %u: v%lld\n", pageFileName.c_str(), pageID, version);
		logPageTableClearToEnd(pageID, version);
		pageVersionMap.erase(itr, pageVersionMap.end());
	}
	
	if(pageVersionMap.size() == 0) {
		debug_printf("%s: Freeing logical %u (freeLogicalPage)\n", pageFileName.c_str(), pageID);
		logicalFreeList.push_back(pageID);
	}
	else if(pageVersionMap.back().second != PagerFile::INVALID_PAGE) {
		pageVersionMap.push_back(std::make_pair(version, PagerFile::INVALID_PAGE));
		logPageTableUpdate(pageID, version, PagerFile::INVALID_PAGE);
	}
}

ACTOR Future<Void> waitAndFreePhysicalPageID(IndirectShadowPager *pager, PhysicalPageID pageID, Future<Void> canFree) {
	wait(canFree);
	pager->pagerFile.freePage(pageID);
	return Void();
}

// TODO: Freeing physical pages must be done *after* committing the page map changes that cause the physical page to no longer be used.
// Otherwise, the physical page could be reused by a write followed by a power loss in which case the mapping change would not
// have been committed and so the physical page should still contain its previous data but it's been overwritten.
void IndirectShadowPager::freePhysicalPageID(PhysicalPageID pageID) {
	debug_printf("%s: Freeing physical %u\n", pageFileName.c_str(), pageID);
	auto itr = busyPages.find(pageID);
	pagerFile.freePage(pageID);
}

void IndirectShadowPager::writePage(LogicalPageID pageID, Reference<IPage> contents, Version updateVersion, LogicalPageID referencePageID) {
	ASSERT(recovery.isReady());
	ASSERT(committing.isReady());

	ASSERT(updateVersion > latestVersion || updateVersion == 0);
	ASSERT(pageID < pageTable.size());

	PageVersionMap &pageVersionMap = pageTable[pageID];

	ASSERT(pageVersionMap.empty() || pageVersionMap.back().second != PagerFile::INVALID_PAGE);

	// TODO: should this be conditional on the write succeeding?
	bool updateExisting = updateVersion == 0;
	if(updateExisting) {
		// If there is no existing latest version to update then there must be a referencePageID from which to get a latest version
		// so get that version and change this to a normal update
		if(pageVersionMap.empty()) {
			ASSERT(referencePageID != invalidLogicalPageID);
			PageVersionMap &rpv = pageTable[referencePageID];
			ASSERT(!rpv.empty());
			updateVersion = rpv.back().first;
			updateExisting = false;
		}
		else {
			ASSERT(pageVersionMap.size());
			updateVersion = pageVersionMap.back().first;
		}
	}

	PhysicalPageID physicalPageID = pagerFile.allocatePage(pageID, updateVersion);

	debug_printf("%s: Writing logical %d v%lld physical %d\n", pageFileName.c_str(), pageID, updateVersion, physicalPageID);

	if(updateExisting) {
		// TODO:  Physical page cannot be freed now, it must be done after the page mapping change above is committed
		//freePhysicalPageID(pageVersionMap.back().second);
		pageVersionMap.back().second = physicalPageID;
	}
	else {
		ASSERT(pageVersionMap.empty() || pageVersionMap.back().first < updateVersion);
		pageVersionMap.push_back(std::make_pair(updateVersion, physicalPageID));
	}

	logPageTableUpdate(pageID, updateVersion, physicalPageID);

	checksumWrite(dataFile.getPtr(), contents->mutate(), IndirectShadowPage::PAGE_BYTES, pageID, physicalPageID);

	Future<Void> write = holdWhile(contents, dataFile->write(contents->begin(), IndirectShadowPage::PAGE_BYTES, (int64_t) physicalPageID * IndirectShadowPage::PAGE_BYTES));

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

	wait(previousCommit);

	pager->logVersion(IndirectShadowPager::LATEST_VERSION_KEY, commitVersion);

	// TODO: we need to prevent writes that happen now from being committed in the subsequent log commit
	// This is probably best done once we have better control of the log, where we can write a commit entry
	// here without syncing the file.

	wait(outstandingWrites);

	wait(pager->dataFile->sync());
	wait(pager->pageTableLog->commit());
	
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
	wait(pager->recovery);
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
	if(pager->errorPromise.canBeSet())
		pager->errorPromise.sendError(actor_cancelled());  // Ideally this should be shutdown_in_progress

	// Cancel all outstanding reads
	auto i = pager->busyPages.begin();
	auto iEnd = pager->busyPages.end();

	while(i != iEnd) {
		// Advance before calling cancel as the rawRead cancel will destroy the map entry it lives in
		(i++)->second.read.cancel();
	}
	ASSERT(pager->busyPages.empty());

	wait(ready(pager->writeActors.signal()));
	wait(ready(pager->operations.signal()));
	wait(ready(pager->committing));

	pager->housekeeping.cancel();
	pager->pagerFile.shutdown();

	state Future<Void> pageTableClosed = pager->pageTableLog->onClosed();
	if(dispose) {
		wait(ready(IAsyncFileSystem::filesystem()->deleteFile(pager->pageFileName, true)));
		pager->pageTableLog->dispose();
	}
	else {
		pager->pageTableLog->close();
	}

	wait(ready(pageTableClosed));

	pager->closed.send(Void());
	delete pager;
}

void IndirectShadowPager::dispose() {
	shutdown(this, true);
}

void IndirectShadowPager::close() {
	shutdown(this, false);
}

ACTOR Future<Reference<const IPage>> rawRead(IndirectShadowPager *pager, LogicalPageID logicalPageID, PhysicalPageID physicalPageID) {
	state void *data;
	state int len = IndirectShadowPage::PAGE_BYTES;
	state bool readSuccess = false;

	try {
		wait(pager->dataFile->readZeroCopy(&data, &len, (int64_t) physicalPageID * IndirectShadowPage::PAGE_BYTES));
		readSuccess = true;

		if(!checksumRead(pager->dataFile.getPtr(), (uint8_t *)data, len, logicalPageID, physicalPageID)) {
			throw checksum_failed();
		}

		pager->busyPages.erase(physicalPageID);
		return Reference<const IPage>(new IndirectShadowPage((uint8_t *)data, pager->dataFile, physicalPageID));
	}
	catch(Error &e) {
		pager->busyPages.erase(physicalPageID);
		if(readSuccess || e.code() == error_code_actor_cancelled) {
			pager->dataFile->releaseZeroCopy(data, len, (int64_t) physicalPageID * IndirectShadowPage::PAGE_BYTES);
		}
		throw;
	}
}

Future<Reference<const IPage>> getPageImpl(IndirectShadowPager *pager, Reference<IndirectShadowPagerSnapshot> snapshot, LogicalPageID logicalPageID, Version version) {
	ASSERT(logicalPageID < pager->pageTable.size());
	PageVersionMap &pageVersionMap = pager->pageTable[logicalPageID];

	auto itr = IndirectShadowPager::pageVersionMapUpperBound(pageVersionMap, version);
	if(itr == pageVersionMap.begin()) {
		debug_printf("%s: Page version map empty! op=error id=%u @%lld\n", pager->pageFileName.c_str(), logicalPageID, version);
		ASSERT(false);
	}
	--itr;
	PhysicalPageID physicalPageID = itr->second;
	ASSERT(physicalPageID != PagerFile::INVALID_PAGE);

	debug_printf("%s: Reading logical %d v%lld physical %d mapSize %lu\n", pager->pageFileName.c_str(), logicalPageID, version, physicalPageID, pageVersionMap.size());

	IndirectShadowPager::BusyPage &bp = pager->busyPages[physicalPageID];
	if(!bp.read.isValid()) {
		Future<Reference<const IPage>> get = rawRead(pager, logicalPageID, physicalPageID);
		if(!get.isReady()) {
			bp.read = get;
		}
		return get;
	}
	return bp.read;
}

Future<Reference<const IPage>> IndirectShadowPager::getPage(Reference<IndirectShadowPagerSnapshot> snapshot, LogicalPageID pageID, Version version) {
	if(!recovery.isReady()) {
		debug_printf("%s: getPage failure, recovery not ready - op=error id=%u @%lld\n", pageFileName.c_str(), pageID, version);
		ASSERT(false);
	}

	Future<Reference<const IPage>> f = getPageImpl(this, snapshot, pageID, version);
	operations.add(forwardError(ready(f), errorPromise));  // For some reason if success is ready() then shutdown hangs when waiting on operations
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
		debug_printf("%s: Freeing logical %u\n", pageFileName.c_str(), pageID);
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

ACTOR Future<Void> copyPage(IndirectShadowPager *pager, Reference<IPage> page, LogicalPageID logical, PhysicalPageID from, PhysicalPageID to) {
	state bool zeroCopied = true;
	state int bytes = IndirectShadowPage::PAGE_BYTES;
	state void *data = nullptr;

	try {
		try {
			wait(pager->dataFile->readZeroCopy(&data, &bytes, (int64_t)from * IndirectShadowPage::PAGE_BYTES));
		}
		catch(Error &e) {
			zeroCopied = false;
			data = page->mutate();
			int _bytes = wait(pager->dataFile->read(data, page->size(), (int64_t)from * IndirectShadowPage::PAGE_BYTES));
			bytes = _bytes;
		}

		ASSERT(bytes == IndirectShadowPage::PAGE_BYTES);
		checksumWrite(pager->dataFile.getPtr(), page->mutate(), bytes, logical, to);
		wait(pager->dataFile->write(data, bytes, (int64_t)to * IndirectShadowPage::PAGE_BYTES));
		if(zeroCopied) {
			pager->dataFile->releaseZeroCopy(data, bytes, (int64_t)from * IndirectShadowPage::PAGE_BYTES);
		}
	}
	catch(Error &e) {
		if(zeroCopied) {
			pager->dataFile->releaseZeroCopy(data, bytes, (int64_t)from * IndirectShadowPage::PAGE_BYTES);
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
			wait(delay(1.0));
		}

		ASSERT(!pagerFile->freePages.empty());

		if(!pagerFile->vacuumQueue.empty()) {
			state PhysicalPageID lastUsedPage = pagerFile->vacuumQueue.rbegin()->first;
			PhysicalPageID lastFreePage = *pagerFile->freePages.rbegin();
			debug_printf("%s: Vacuuming: evaluating (free list size=%lu, lastFreePage=%u, lastUsedPage=%u, pagesAllocated=%u)\n", pager->pageFileName.c_str(), pagerFile->freePages.size(), lastFreePage, lastUsedPage, pagerFile->pagesAllocated);
			ASSERT(lastFreePage < pagerFile->pagesAllocated);
			ASSERT(lastUsedPage < pagerFile->pagesAllocated);
			ASSERT(lastFreePage != lastUsedPage);

			if(lastFreePage < lastUsedPage) {
				state std::pair<LogicalPageID, Version> logicalPageInfo = pagerFile->vacuumQueue[lastUsedPage];
				state PhysicalPageID newPage = pagerFile->allocatePage(logicalPageInfo.first, logicalPageInfo.second);

				debug_printf("%s: Vacuuming: copying page %u to %u\n", pager->pageFileName.c_str(), lastUsedPage, newPage);
				wait(copyPage(pager, page, logicalPageInfo.first, lastUsedPage, newPage));

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

		debug_printf("%s: Vacuuming: got %llu pages to erase (freePages=%lu, pagesAllocated=%u, vacuumQueueEmpty=%u, minVacuumQueuePage=%u, firstFreePage=%u)\n", pager->pageFileName.c_str(), pagesToErase, pagerFile->freePages.size(), pagerFile->pagesAllocated, pagerFile->vacuumQueue.empty(), pagerFile->minVacuumQueuePage, firstFreePage);

		if(pagesToErase > 0) {
			PhysicalPageID eraseStartPage = pagerFile->pagesAllocated - pagesToErase;
			debug_printf("%s: Vacuuming: truncating last %llu pages starting at %u\n", pager->pageFileName.c_str(), pagesToErase, eraseStartPage);

			ASSERT(pagesToErase <= pagerFile->pagesAllocated);

			pagerFile->pagesAllocated = eraseStartPage;
			pager->logPagesAllocated();

			auto freePageItr = pagerFile->freePages.find(eraseStartPage);
			ASSERT(freePageItr != pagerFile->freePages.end());

			pagerFile->freePages.erase(freePageItr, pagerFile->freePages.end());
			ASSERT(pagerFile->vacuumQueue.empty() || pagerFile->vacuumQueue.rbegin()->first < eraseStartPage);

			wait(pager->dataFile->truncate((int64_t)pagerFile->pagesAllocated * IndirectShadowPage::PAGE_BYTES));
		}

		wait(delayUntil(start + (double)IndirectShadowPage::PAGE_BYTES / SERVER_KNOBS->VACUUM_BYTES_PER_SECOND)); // TODO: figure out the correct mechanism here
	}
}

PagerFile::PagerFile(IndirectShadowPager *pager) : fileSize(0), pagesAllocated(0), pager(pager), vacuumQueueReady(false), minVacuumQueuePage(0) {}

PhysicalPageID PagerFile::allocatePage(LogicalPageID logicalPageID, Version version) {
	ASSERT((int64_t)pagesAllocated * IndirectShadowPage::PAGE_BYTES <= fileSize);
	ASSERT(fileSize % IndirectShadowPage::PAGE_BYTES == 0);

	PhysicalPageID allocatedPage;
	if(!freePages.empty()) {
		allocatedPage = *freePages.begin();
		freePages.erase(freePages.begin());
	}
	else {
		if((int64_t)pagesAllocated * IndirectShadowPage::PAGE_BYTES == fileSize) {
			fileSize += (1 << 24);
			// TODO: extend the file before writing beyond the end.
		}

		ASSERT(pagesAllocated < INVALID_PAGE); // TODO: we should throw a better error here
		allocatedPage = pagesAllocated++;
		pager->logPagesAllocated();
	}

	markPageAllocated(logicalPageID, version, allocatedPage);

	debug_printf("%s: Allocated physical %u\n", pager->pageFileName.c_str(), allocatedPage);
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

uint32_t PagerFile::getFreePages() {
	return freePages.size();
}

void PagerFile::init(uint64_t fileSize, uint32_t pagesAllocated) {
	this->fileSize = fileSize;
	this->pagesAllocated = pagesAllocated;
	this->minVacuumQueuePage = pagesAllocated >= SERVER_KNOBS->VACUUM_QUEUE_SIZE ? pagesAllocated - SERVER_KNOBS->VACUUM_QUEUE_SIZE : 0;
}

void PagerFile::startVacuuming() {
	vacuuming = Never(); //vacuumer(pager, this);
}

void PagerFile::shutdown() {
	vacuuming.cancel();
}

bool PagerFile::canVacuum() {
	if(freePages.size() < SERVER_KNOBS->FREE_PAGE_VACUUM_THRESHOLD // Not enough free pages
		|| minVacuumQueuePage >= pagesAllocated // We finished processing all pages in the vacuum queue
		|| !vacuumQueueReady) // Populating vacuum queue
	{
		debug_printf("%s: Vacuuming: waiting for vacuumable pages (free list size=%lu, minVacuumQueuePage=%u, pages allocated=%u, vacuumQueueReady=%d)\n", pager->pageFileName.c_str(), freePages.size(), minVacuumQueuePage, pagesAllocated, vacuumQueueReady);
		return false;
	}

	return true;
}

const PhysicalPageID PagerFile::INVALID_PAGE = std::numeric_limits<PhysicalPageID>::max();

extern Future<Void> simplePagerTest(IPager* const& pager);

TEST_CASE("/fdbserver/indirectshadowpager/simple") {
	state IPager *pager = new IndirectShadowPager("unittest_pageFile");

	wait(simplePagerTest(pager));

	Future<Void> closedFuture = pager->onClosed();
	pager->close();
	wait(closedFuture);

	return Void();
}
