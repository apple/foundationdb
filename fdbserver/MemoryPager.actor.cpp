/*
 * MemoryPager.actor.cpp
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

#include "fdbserver/MemoryPager.h"
#include "fdbserver/Knobs.h"

#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h"

typedef uint8_t* PhysicalPageID;
typedef std::vector<std::pair<Version, PhysicalPageID>> PageVersionMap;
typedef std::vector<PageVersionMap> LogicalPageTable;

class MemoryPager;

class MemoryPage : public IPage, ReferenceCounted<MemoryPage> {
public:
	MemoryPage();
	MemoryPage(uint8_t *data);
	virtual ~MemoryPage();

	virtual void addref() const {
		ReferenceCounted<MemoryPage>::addref();
	}

	virtual void delref() const {
		ReferenceCounted<MemoryPage>::delref();
	}

	virtual int size() const;
	virtual uint8_t const* begin() const;
	virtual uint8_t* mutate();

private:
	friend class MemoryPager;
	uint8_t *data;
	bool allocated;

	static const int PAGE_BYTES;
};

class MemoryPagerSnapshot : public IPagerSnapshot, ReferenceCounted<MemoryPagerSnapshot> {
public:
	MemoryPagerSnapshot(MemoryPager *pager, Version version) : pager(pager), version(version) {}
	virtual Future<Reference<const IPage>> getPhysicalPage(LogicalPageID pageID);
	virtual Version getVersion() const {
		return version;
	}

	virtual void addref() {
		ReferenceCounted<MemoryPagerSnapshot>::addref();
	}

	virtual void delref() {
		ReferenceCounted<MemoryPagerSnapshot>::delref();
	}

private:
	MemoryPager *pager;
	Version version;
};

class MemoryPager : public IPager, ReferenceCounted<MemoryPager> {
public:
	MemoryPager();

	virtual Reference<IPage> newPageBuffer();
	virtual int getUsablePageSize();

	virtual Reference<IPagerSnapshot> getReadSnapshot(Version version);

	virtual LogicalPageID allocateLogicalPage();
	virtual void freeLogicalPage(LogicalPageID pageID, Version version);
	virtual void writePage(LogicalPageID pageID, Reference<IPage> contents, Version updateVersion, LogicalPageID referencePageID);
	virtual void forgetVersions(Version begin, Version end);
	virtual Future<Void> commit();

	virtual StorageBytes getStorageBytes() {
		// TODO:  Get actual values for used and free memory
		return StorageBytes();
	}

	virtual void setLatestVersion(Version version);
	virtual Future<Version> getLatestVersion();	
	
	virtual Future<Void> getError();
	virtual Future<Void> onClosed();
	virtual void dispose();
	virtual void close();

	virtual Reference<const IPage> getPage(LogicalPageID pageID, Version version);

private:
	Version latestVersion;
	Version committedVersion;
	Standalone<VectorRef<VectorRef<uint8_t>>> data;
	LogicalPageTable pageTable;

	Promise<Void> closed;

	std::vector<PhysicalPageID> freeList; // TODO: is this good enough for now?

	PhysicalPageID allocatePage(Reference<IPage> contents);
	void extendData();	

	static const PhysicalPageID INVALID_PAGE;
};

IPager * createMemoryPager() {
	return new MemoryPager();
}

MemoryPage::MemoryPage() : allocated(true) {
	data = (uint8_t*)FastAllocator<4096>::allocate();
}

MemoryPage::MemoryPage(uint8_t *data) : data(data), allocated(false) {}

MemoryPage::~MemoryPage() {
	if(allocated) {
		FastAllocator<4096>::release(data);
	}
}

uint8_t const* MemoryPage::begin() const {
	return data;
}

uint8_t* MemoryPage::mutate() {
	return data;
}

int MemoryPage::size() const {
	return PAGE_BYTES;
}

const int MemoryPage::PAGE_BYTES = 4096;

Future<Reference<const IPage>> MemoryPagerSnapshot::getPhysicalPage(LogicalPageID pageID) {
	return pager->getPage(pageID, version);
}

MemoryPager::MemoryPager() : latestVersion(0), committedVersion(0) {
	extendData();
	pageTable.resize(SERVER_KNOBS->PAGER_RESERVED_PAGES);
}

Reference<IPage> MemoryPager::newPageBuffer() {
	return Reference<IPage>(new MemoryPage());
}

int MemoryPager::getUsablePageSize() {
	return MemoryPage::PAGE_BYTES;
}

Reference<IPagerSnapshot> MemoryPager::getReadSnapshot(Version version) {
	ASSERT(version <= latestVersion);
	return Reference<IPagerSnapshot>(new MemoryPagerSnapshot(this, version));
}

LogicalPageID MemoryPager::allocateLogicalPage() {
	ASSERT(pageTable.size() >= SERVER_KNOBS->PAGER_RESERVED_PAGES);
	pageTable.push_back(PageVersionMap());
	return pageTable.size() - 1;
}

void MemoryPager::freeLogicalPage(LogicalPageID pageID, Version version) {
	ASSERT(pageID < pageTable.size());

	PageVersionMap &pageVersionMap = pageTable[pageID];
	ASSERT(!pageVersionMap.empty());

	auto itr = std::lower_bound(pageVersionMap.begin(), pageVersionMap.end(), version, [](std::pair<Version, PhysicalPageID> p, Version v) {
		return p.first < v;
	});

	pageVersionMap.erase(itr, pageVersionMap.end());
	if(pageVersionMap.size() > 0 && pageVersionMap.back().second != INVALID_PAGE) {
		pageVersionMap.push_back(std::make_pair(version, INVALID_PAGE));
	}
}

void MemoryPager::writePage(LogicalPageID pageID, Reference<IPage> contents, Version updateVersion, LogicalPageID referencePageID) {
	ASSERT(updateVersion > latestVersion || updateVersion == 0);
	ASSERT(pageID < pageTable.size());

	if(referencePageID != invalidLogicalPageID) {
		PageVersionMap &rpv = pageTable[referencePageID];
		ASSERT(!rpv.empty());
		updateVersion = rpv.back().first;
	}

	PageVersionMap &pageVersionMap = pageTable[pageID];

	ASSERT(updateVersion >= committedVersion || updateVersion == 0);
	PhysicalPageID physicalPageID = allocatePage(contents);

	ASSERT(pageVersionMap.empty() || pageVersionMap.back().second != INVALID_PAGE);

	if(updateVersion == 0) {
		ASSERT(pageVersionMap.size());
		updateVersion = pageVersionMap.back().first;
		pageVersionMap.back().second = physicalPageID;
		// TODO: what to do with old page?
	}
	else {
		ASSERT(pageVersionMap.empty() || pageVersionMap.back().first < updateVersion);
		pageVersionMap.push_back(std::make_pair(updateVersion, physicalPageID));
	}

}

void MemoryPager::forgetVersions(Version begin, Version end) {
	ASSERT(begin <= end);
	ASSERT(end <= latestVersion);
	// TODO
}

Future<Void> MemoryPager::commit() { 
	ASSERT(committedVersion < latestVersion);
	committedVersion = latestVersion;
	return Void();
}

void MemoryPager::setLatestVersion(Version version) {
	ASSERT(version > latestVersion);
	latestVersion = version;
}

Future<Version> MemoryPager::getLatestVersion() {
	return latestVersion;
}

Reference<const IPage> MemoryPager::getPage(LogicalPageID pageID, Version version) {
	ASSERT(pageID < pageTable.size());
	PageVersionMap const& pageVersionMap = pageTable[pageID];

	auto itr = std::upper_bound(pageVersionMap.begin(), pageVersionMap.end(), version, [](Version v, std::pair<Version, PhysicalPageID> p) {
		return v < p.first;
	});

	if(itr == pageVersionMap.begin()) {
		return Reference<IPage>(); // TODO: should this be an error?
	}

	--itr;
	
	ASSERT(itr->second != INVALID_PAGE);
	return Reference<const IPage>(new MemoryPage(itr->second)); // TODO: Page memory owned by the pager. Change this?
}

Future<Void> MemoryPager::getError() {
	return Void();
}

Future<Void> MemoryPager::onClosed() {
	return closed.getFuture();
}

void MemoryPager::dispose() {
	closed.send(Void());
	delete this;
}

void MemoryPager::close() {
	dispose();
}

PhysicalPageID MemoryPager::allocatePage(Reference<IPage> contents) {
	if(freeList.size()) {
		PhysicalPageID pageID = freeList.back();
		freeList.pop_back();

		memcpy(pageID, contents->begin(), contents->size());
		return pageID;
	}
	else {
		ASSERT(data.size() && data.back().capacity() - data.back().size() >= contents->size());
		PhysicalPageID pageID = data.back().end();

		data.back().append(data.arena(), contents->begin(), contents->size());
		if(data.back().size() == data.back().capacity()) {
			extendData();
		}
		else {
			ASSERT(data.back().size() <= data.back().capacity() - 4096);
		}

		return pageID;
	}
}

void MemoryPager::extendData() {
	if(data.size() > 1000) { // TODO: is this an ok way to handle large data size?
		throw io_error();
	}

	VectorRef<uint8_t> d;
	d.reserve(data.arena(), 1 << 22);
	data.push_back(data.arena(), d);
}

// TODO: these tests are not MemoryPager specific, we should make them more general

void fillPage(Reference<IPage> page, LogicalPageID pageID, Version version) {
	ASSERT(page->size() > sizeof(LogicalPageID) + sizeof(Version));

	memset(page->mutate(), 0, page->size());
	memcpy(page->mutate(), (void*)&pageID, sizeof(LogicalPageID));
	memcpy(page->mutate() + sizeof(LogicalPageID), (void*)&version, sizeof(Version));
}

bool validatePage(Reference<const IPage> page, LogicalPageID pageID, Version version) {
	bool valid = true;

	LogicalPageID readPageID = *(LogicalPageID*)page->begin();
	if(readPageID != pageID) {
		fprintf(stderr, "Invalid PageID detected: %u (expected %u)\n", readPageID, pageID);
		valid = false;
	}

	Version readVersion = *(Version*)(page->begin()+sizeof(LogicalPageID));
	if(readVersion != version) {
		fprintf(stderr, "Invalid Version detected on page %u: %lld (expected %lld)\n", pageID, readVersion, version);
		valid = false;
	}

	return valid;
}

void writePage(IPager *pager, Reference<IPage> page, LogicalPageID pageID, Version version, bool updateVersion=true) {
	fillPage(page, pageID, version);
	pager->writePage(pageID, page, updateVersion ? version : 0);
}

ACTOR Future<Void> commit(IPager *pager) {
	static int commitNum = 1;
	state int myCommit = commitNum++;

	debug_printf("Commit%d\n", myCommit);
	wait(pager->commit());
	debug_printf("FinishedCommit%d\n", myCommit);
	return Void();
}

ACTOR Future<Void> read(IPager *pager, LogicalPageID pageID, Version version, Version expectedVersion=-1) {
	static int readNum = 1;
	state int myRead = readNum++;
	state Reference<IPagerSnapshot> readSnapshot = pager->getReadSnapshot(version);
	debug_printf("Read%d\n", myRead);
	Reference<const IPage> readPage = wait(readSnapshot->getPhysicalPage(pageID));
	debug_printf("FinishedRead%d\n", myRead);
	ASSERT(validatePage(readPage, pageID, expectedVersion >= 0 ? expectedVersion : version));
	return Void();
}

ACTOR Future<Void> simplePagerTest(IPager *pager) {
	state Reference<IPage> page = pager->newPageBuffer();

	Version latestVersion = wait(pager->getLatestVersion());
	debug_printf("Got latest version: %lld\n", latestVersion);

	state Version version = latestVersion+1;
	state Version v1 = version;

	state LogicalPageID pageID1 = pager->allocateLogicalPage();

	writePage(pager, page, pageID1, v1);
	pager->setLatestVersion(v1);
	wait(commit(pager));

	state LogicalPageID pageID2 = pager->allocateLogicalPage();

	state Version v2 = ++version;

	writePage(pager, page, pageID1, v2);
	writePage(pager, page, pageID2, v2);
	pager->setLatestVersion(v2);
	wait(commit(pager));

	wait(read(pager, pageID1, v2));
	wait(read(pager, pageID1, v1));

	state Version v3 = ++version;
	writePage(pager, page, pageID1, v3, false);
	pager->setLatestVersion(v3);

	wait(read(pager, pageID1, v2, v3));
	wait(read(pager, pageID1, v3, v3));

	state LogicalPageID pageID3 = pager->allocateLogicalPage();

	state Version v4 = ++version;
	writePage(pager, page, pageID2, v4);
	writePage(pager, page, pageID3, v4);
	pager->setLatestVersion(v4);
	wait(commit(pager));

	wait(read(pager, pageID2, v4, v4));

	state Version v5 = ++version;
	writePage(pager, page, pageID2, v5);

	state LogicalPageID pageID4 = pager->allocateLogicalPage();
	writePage(pager, page, pageID4, v5);
	
	state Version v6 = ++version;
	pager->freeLogicalPage(pageID2, v5);
	pager->freeLogicalPage(pageID3, v3);
	pager->setLatestVersion(v6);
	wait(commit(pager));

	pager->forgetVersions(0, v4);
	wait(commit(pager));

	wait(delay(3.0));

	wait(commit(pager));

	return Void();
}

/*
TEST_CASE("/fdbserver/memorypager/simple") {
	state IPager *pager = new MemoryPager();

	wait(simplePagerTest(pager));

	Future<Void> closedFuture = pager->onClosed();
	pager->dispose();

	wait(closedFuture);
	return Void();
}
*/

const PhysicalPageID MemoryPager::INVALID_PAGE = nullptr;
