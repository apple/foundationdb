/*
 * MemoryPager.h
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

#ifndef FDBSERVER_MEMORYPAGER_H
#define FDBSERVER_MEMORYPAGER_H
#pragma once

#include "IPager.h"

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
	virtual void invalidateReturnedPages() {}

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
	virtual void writePage(LogicalPageID pageID, Reference<IPage> contents, Version updateVersion);
	virtual void forgetVersions(Version begin, Version end);
	virtual Future<Void> commit();

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

#endif