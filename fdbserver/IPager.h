/*
 * IPager.h
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

#ifndef FDBSERVER_IPAGER_H
#define FDBSERVER_IPAGER_H
#pragma once

#include "fdbserver/IKeyValueStore.h"

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"

#define REDWOOD_DEBUG 0

#define debug_printf_always(...) { fprintf(stdout, "%s %f (%s:%d) ", g_network->getLocalAddress().toString().c_str(), now(), __FUNCTION__, __LINE__), fprintf(stdout, __VA_ARGS__); fflush(stdout); }

#define debug_printf_noop(...)

#if defined(NO_INTELLISENSE)
	#if REDWOOD_DEBUG
		#define debug_printf debug_printf_always
	#else
		#define debug_printf debug_printf_noop
	#endif
#else
	// To get error-checking on debug_printf statements in IDE
	#define debug_printf printf
#endif

#define BEACON fprintf(stderr, "%s: %s line %d \n", __FUNCTION__, __FILE__, __LINE__)
#define TRACE fprintf(stderr, "%s: %s line %d %s\n", __FUNCTION__, __FILE__, __LINE__, platform::get_backtrace().c_str());

#ifndef VALGRIND
#define VALGRIND_MAKE_MEM_UNDEFINED(x, y)
#define VALGRIND_MAKE_MEM_DEFINED(x, y)
#endif

typedef uint32_t LogicalPageID; // uint64_t?
static const int invalidLogicalPageID = LogicalPageID(-1);

class IPage {
public:
	IPage() : userData(nullptr) {}

	virtual uint8_t const* begin() const = 0;
	virtual uint8_t* mutate() = 0;

	// Must return the same size for all pages created by the same pager instance
	virtual int size() const = 0;

	StringRef asStringRef() const {
		return StringRef(begin(), size());
	}

	virtual ~IPage() {
		if(userData != nullptr && userDataDestructor != nullptr) {
			userDataDestructor(userData);
		}
	}

	virtual void addref() const = 0;
	virtual void delref() const = 0;

	mutable void *userData;
	mutable void (*userDataDestructor)(void *);
};

class IPagerSnapshot {
public:
	virtual Future<Reference<const IPage>> getPhysicalPage(LogicalPageID pageID, bool cacheable) = 0;
	virtual Version getVersion() const = 0;

	virtual Key getMetaKey() const {
		return Key();
	}

	virtual ~IPagerSnapshot() {}

	virtual void addref() = 0;
	virtual void delref() = 0;
};

class IPager : public IClosable {
public:
	// Returns an IPage that can be passed to writePage. The data in the returned IPage might not be zeroed.
	virtual Reference<IPage> newPageBuffer() = 0;

	// Returns the usable size of pages returned by the pager (i.e. the size of the page that isn't pager overhead).
	// For a given pager instance, separate calls to this function must return the same value.
	virtual int getUsablePageSize() = 0;
	
	virtual StorageBytes getStorageBytes() = 0;

	// Permitted to fail (ASSERT) during recovery.
	virtual Reference<IPagerSnapshot> getReadSnapshot(Version version) = 0;

	// Returns an unused LogicalPageID. 
	// LogicalPageIDs in the range [0, SERVER_KNOBS->PAGER_RESERVED_PAGES) do not need to be allocated.
	// Permitted to fail (ASSERT) during recovery.
	virtual LogicalPageID allocateLogicalPage() = 0;

	// Signals that the page will no longer be used as of the specified version. Versions prior to the specified version must be kept.
	// Permitted to fail (ASSERT) during recovery.
	virtual void freeLogicalPage(LogicalPageID pageID, Version version) = 0;

	// Writes a page with the given LogicalPageID at the specified version. LogicalPageIDs in the range [0, SERVER_KNOBS->PAGER_RESERVED_PAGES)
	// can be written without being allocated. All other LogicalPageIDs must be allocated using allocateLogicalPage before writing them.
	//
	// If updateVersion is 0, we are signalling to the pager that we are reusing the LogicalPageID entry at the current latest version of pageID.
	// 
	// Otherwise, we will add a new entry for LogicalPageID at the specified version. In that case, updateVersion must be larger than any version 
	// written to this page previously, and it must be larger than any version committed.  If referencePageID is given, the latest version of that
	// page will be used for the write, which *can* be less than the latest committed version.
	//
	// Permitted to fail (ASSERT) during recovery.
	virtual void writePage(LogicalPageID pageID, Reference<IPage> contents, Version updateVersion, LogicalPageID referencePageID = invalidLogicalPageID) = 0;

	// Signals to the pager that no more reads will be performed in the range [begin, end). 
	// Permitted to fail (ASSERT) during recovery.
	virtual void forgetVersions(Version begin, Version end) = 0;

	// Makes durable all writes and any data structures used for recovery.
	// Permitted to fail (ASSERT) during recovery.
	virtual Future<Void> commit() = 0;

	// Returns the latest version of the pager. Permitted to block until recovery is complete, at which point it should always be set immediately.
	// Some functions in the IPager interface are permitted to fail (ASSERT) during recovery, so users should wait for getLatestVersion to complete 
	// before doing anything else.
	virtual Future<Version> getLatestVersion() = 0;

	// Sets the latest version of the pager. Must be monotonically increasing. 
	// 
	// Must be called prior to reading the specified version. SOMEDAY: It may be desirable in the future to relax this constraint for performance reasons.
	//
	// Permitted to fail (ASSERT) during recovery.
	virtual void setLatestVersion(Version version) = 0;

protected:
	~IPager() {} // Destruction should be done using close()/dispose() from the IClosable interface
};

class IPager2 : public IClosable {
public:
	// Returns an IPage that can be passed to writePage. The data in the returned IPage might not be zeroed.
	virtual Reference<IPage> newPageBuffer() = 0;

	// Returns the usable size of pages returned by the pager (i.e. the size of the page that isn't pager overhead).
	// For a given pager instance, separate calls to this function must return the same value.
	// Only valid to call after recovery is complete.
	virtual int getUsablePageSize() = 0;

	// Allocate a new page ID for a subsequent write.  The page will be considered in-use after the next commit
	// regardless of whether or not it was written to.
	virtual Future<LogicalPageID> newPageID() = 0;

	// Replace the contents of a page with new data across *all* versions.
	// Existing holders of a page reference for pageID, read from any version,
	// may see the effects of this write.
	virtual void updatePage(LogicalPageID pageID, Reference<IPage> data) = 0;

	// Try to atomically update the contents of a page as of version v in the next commit.
	// If the pager is unable to do this at this time, it may choose to write the data to a new page ID
	// instead and return the new page ID to the caller.  Otherwise the original pageID argument will be returned.
	// If a new page ID is returned, the old page ID will be freed as of version v
	virtual Future<LogicalPageID> atomicUpdatePage(LogicalPageID pageID, Reference<IPage> data, Version v) = 0;

	// Free pageID to be used again after the commit that moves oldestVersion past v
	virtual void freePage(LogicalPageID pageID, Version v) = 0;

	// Returns the latest data (regardless of version) for a page by LogicalPageID
	// The data returned will be the later of
	//   - the most recent committed atomic
	//   - the most recent non-atomic write
	virtual Future<Reference<IPage>> readPage(LogicalPageID pageID, bool cacheable) = 0;

	// Get a snapshot of the metakey and all pages as of the version v which must be >= getOldestVersion()
	// Note that snapshots at any version may still see the results of updatePage() calls.
	// The snapshot shall be usable until setOldVersion() is called with a version > v.
	virtual Reference<IPagerSnapshot> getReadSnapshot(Version v) = 0;

	// Atomically make durable all pending page writes, page frees, and update the metadata string.
	virtual Future<Void> commit() = 0;

	// Get the latest meta key set or committed
	virtual Key getMetaKey() const = 0;

	// Set the metakey which will be stored in the next commit
	virtual void setMetaKey(KeyRef metaKey) = 0;

	// Sets the next commit version
	virtual void setCommitVersion(Version v) = 0;

	virtual StorageBytes getStorageBytes() = 0;

	// Future returned is ready when pager has been initialized from disk and is ready for reads and writes.
	// It is invalid to call most other functions until init() is ready.
	// TODO: Document further.
	virtual Future<Void> init() = 0;

	// Returns latest committed version
	virtual Version getLatestVersion() = 0;

	// Returns the oldest readable version as of the most recent committed version
	virtual Version getOldestVersion() = 0;

	// Sets the oldest readable version to be put into affect at the next commit.
	// The pager can reuse pages that were freed at a version less than v.
	// If any snapshots are in use at a version less than v, the pager can either forcefully
	// invalidate them or keep their versions around until the snapshots are no longer in use.
	virtual void setOldestVersion(Version v) = 0;

protected:
	~IPager2() {} // Destruction should be done using close()/dispose() from the IClosable interface
};

#endif
