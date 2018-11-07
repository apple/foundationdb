/*
 * IndirectShadowPager.h
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

#ifndef FDBSERVER_INDIRECTSHADOWPAGER_H
#define FDBSERVER_INDIRECTSHADOWPAGER_H
#pragma once

#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/IPager.h"

#include "flow/ActorCollection.h"
#include "fdbclient/Notified.h"

#include "fdbrpc/IAsyncFile.h"

typedef uint32_t PhysicalPageID;
typedef std::vector<std::pair<Version, PhysicalPageID>> PageVersionMap;
typedef std::vector<PageVersionMap> LogicalPageTable;

class IndirectShadowPager;

class IndirectShadowPage : public IPage, ReferenceCounted<IndirectShadowPage> {
public:
	IndirectShadowPage();
	IndirectShadowPage(uint8_t *data, Reference<IAsyncFile> file, PhysicalPageID pageID)
	 : file(file), physicalPageID(pageID), fastAllocated(false), data(data) {}
	virtual ~IndirectShadowPage();

	virtual void addref() const {
		ReferenceCounted<IndirectShadowPage>::addref();
	}

	virtual void delref() const {
		ReferenceCounted<IndirectShadowPage>::delref();
	}

	virtual int size() const;
	virtual uint8_t const* begin() const;
	virtual uint8_t* mutate();

//private:
	static const int PAGE_BYTES;
	static const int PAGE_OVERHEAD_BYTES;

private:
	Reference<IAsyncFile> file;
	PhysicalPageID physicalPageID;
	bool fastAllocated;
	uint8_t *data;
};

class IndirectShadowPagerSnapshot : public IPagerSnapshot, ReferenceCounted<IndirectShadowPagerSnapshot> {
public:
	IndirectShadowPagerSnapshot(IndirectShadowPager *pager, Version version);

	virtual Future<Reference<const IPage>> getPhysicalPage(LogicalPageID pageID);

	virtual Version getVersion() const {
		return version;
	}

	virtual ~IndirectShadowPagerSnapshot() {
	}

	virtual void addref() {
		ReferenceCounted<IndirectShadowPagerSnapshot>::addref();
	}

	virtual void delref() {
		ReferenceCounted<IndirectShadowPagerSnapshot>::delref();
	}

private:
	IndirectShadowPager *pager;
	Version version;
	Future<Void> pagerError;
};

class PagerFile {
public:
	PagerFile(IndirectShadowPager *pager);

	PhysicalPageID allocatePage(LogicalPageID logicalPageID, Version version);
	void freePage(PhysicalPageID physicalPageID);
	void markPageAllocated(LogicalPageID logicalPageID, Version version, PhysicalPageID physicalPageID);

	void finishedMarkingPages();

	uint64_t size();
	uint32_t getPagesAllocated();
	uint32_t getFreePages();

	void init(uint64_t fileSize, uint32_t pagesAllocated);
	void startVacuuming();
	void shutdown();

//private:
	Future<Void> vacuuming;
	IndirectShadowPager *pager;

	uint32_t pagesAllocated;
	uint64_t fileSize;

	std::set<PhysicalPageID> freePages;

	PhysicalPageID minVacuumQueuePage;
	bool vacuumQueueReady;
	std::map<PhysicalPageID, std::pair<LogicalPageID, Version>> vacuumQueue;

	bool canVacuum();

	static const PhysicalPageID INVALID_PAGE;
};

class IndirectShadowPager : public IPager {
public:
	IndirectShadowPager(std::string basename);
	virtual ~IndirectShadowPager() {
	}

	virtual Reference<IPage> newPageBuffer();
	virtual int getUsablePageSize();

	virtual Reference<IPagerSnapshot> getReadSnapshot(Version version);

	virtual LogicalPageID allocateLogicalPage();
	virtual void freeLogicalPage(LogicalPageID pageID, Version version);
	virtual void writePage(LogicalPageID pageID, Reference<IPage> contents, Version updateVersion, LogicalPageID referencePageID);
	virtual void forgetVersions(Version begin, Version end);
	virtual Future<Void> commit();

	virtual void setLatestVersion(Version version);
	virtual Future<Version> getLatestVersion();	

	virtual StorageBytes getStorageBytes();

	virtual Future<Void> getError();
	virtual Future<Void> onClosed();
	virtual void dispose();
	virtual void close();

	Future<Reference<const IPage>> getPage(Reference<IndirectShadowPagerSnapshot> snapshot, LogicalPageID pageID, Version version);

//private:
	std::string basename;
	std::string pageFileName;

	Version latestVersion;
	Version committedVersion;

	LogicalPageTable pageTable;
	IKeyValueStore *pageTableLog;

	Reference<IAsyncFile> dataFile;
	Future<Void> recovery;

	Future<Void> housekeeping;
	Future<Void> vacuuming;
	Version oldestVersion;

	// TODO: This structure maybe isn't needed
	struct BusyPage {
		Future<Reference<const IPage>> read;
	};

	typedef std::map<PhysicalPageID, BusyPage> BusyPageMapT;
	BusyPageMapT busyPages;

	SignalableActorCollection operations;
	SignalableActorCollection writeActors;
	Future<Void> committing;

	Promise<Void> closed;
	Promise<Void> errorPromise;

	std::deque<LogicalPageID> logicalFreeList;
	PagerFile pagerFile;

	static PageVersionMap::iterator pageVersionMapLowerBound(PageVersionMap &pageVersionMap, Version v);
	static PageVersionMap::iterator pageVersionMapUpperBound(PageVersionMap &pageVersionMap, Version v);

	void freeLogicalPageID(LogicalPageID pageID);
	void freePhysicalPageID(PhysicalPageID pageID);

	void logVersion(StringRef versionKey, Version version);
	void logPagesAllocated();
	void logPageTableUpdate(LogicalPageID logicalPageID, Version version, PhysicalPageID physicalPageID);
	void logPageTableClearToEnd(LogicalPageID logicalPageID, Version start);
	void logPageTableClear(LogicalPageID logicalPageID, Version start, Version end);

	static const StringRef LATEST_VERSION_KEY;
	static const StringRef OLDEST_VERSION_KEY;
	static const StringRef PAGES_ALLOCATED_KEY;
	static const StringRef TABLE_ENTRY_PREFIX;

};

#endif
