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

#define debug_printf_always(...) { fprintf(stdout, "%s %f ", g_network->getLocalAddress().toString().c_str(), now()), fprintf(stdout, __VA_ARGS__); fflush(stdout); }

#if REDWOOD_DEBUG
  #define debug_printf debug_printf_always
#else
  #define debug_printf(...)
#endif

#define BEACON fprintf(stderr, "%s: %s line %d \n", __FUNCTION__, __FILE__, __LINE__)

typedef uint32_t LogicalPageID; // uint64_t?
static const int invalidLogicalPageID = LogicalPageID(-1);

class IPage {
public:
	virtual uint8_t const* begin() const = 0;
	virtual uint8_t* mutate() = 0;

	// Must return the same size for all pages created by the same pager instance
	virtual int size() const = 0;

	StringRef asStringRef() const {
		return StringRef(begin(), size());
	}

	virtual ~IPage() {}

	virtual void addref() const = 0;
	virtual void delref() const = 0;
};

class IPagerSnapshot {
public:
	virtual Future<Reference<const IPage>> getPhysicalPage(LogicalPageID pageID) = 0;
	virtual Version getVersion() const = 0;

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

#endif
