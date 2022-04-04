/*
 * IClientApi.h
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

#ifndef FDBCLIENT_ICLIENTAPI_H
#define FDBCLIENT_ICLIENTAPI_H
#pragma once

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Tenant.h"

#include "flow/ThreadHelper.actor.h"

struct VersionVector;

// An interface that represents a transaction created by a client
class ITransaction {
public:
	virtual ~ITransaction() {}

	virtual void cancel() = 0;
	virtual void setVersion(Version v) = 0;
	virtual ThreadFuture<Version> getReadVersion() = 0;

	// These functions that read data return Standalone<...> objects, but these objects are not required to manage their
	// own memory. It is guaranteed, however, that the ThreadFuture will hold a reference to the memory. It will persist
	// until the ThreadFuture's ThreadSingleAssignmentVar has its memory released or it is destroyed.
	virtual ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot = false) = 0;
	virtual ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot = false) = 0;
	virtual ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                           const KeySelectorRef& end,
	                                           int limit,
	                                           bool snapshot = false,
	                                           bool reverse = false) = 0;
	virtual ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                           const KeySelectorRef& end,
	                                           GetRangeLimits limits,
	                                           bool snapshot = false,
	                                           bool reverse = false) = 0;
	virtual ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                           int limit,
	                                           bool snapshot = false,
	                                           bool reverse = false) = 0;
	virtual ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                           GetRangeLimits limits,
	                                           bool snapshot = false,
	                                           bool reverse = false) = 0;
	virtual ThreadFuture<MappedRangeResult> getMappedRange(const KeySelectorRef& begin,
	                                                       const KeySelectorRef& end,
	                                                       const StringRef& mapper,
	                                                       GetRangeLimits limits,
	                                                       bool snapshot = false,
	                                                       bool reverse = false) = 0;
	virtual ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) = 0;
	virtual ThreadFuture<Standalone<StringRef>> getVersionstamp() = 0;

	virtual void addReadConflictRange(const KeyRangeRef& keys) = 0;
	virtual ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) = 0;
	virtual ThreadFuture<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRangeRef& range,
	                                                                        int64_t chunkSize) = 0;

	virtual ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRangeRef& keyRange) = 0;

	virtual ThreadResult<RangeResult> readBlobGranules(const KeyRangeRef& keyRange,
	                                                   Version beginVersion,
	                                                   Optional<Version> readVersion,
	                                                   ReadBlobGranuleContext granuleContext) = 0;

	virtual void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) = 0;
	virtual void set(const KeyRef& key, const ValueRef& value) = 0;
	virtual void clear(const KeyRef& begin, const KeyRef& end) = 0;
	virtual void clear(const KeyRangeRef& range) = 0;
	virtual void clear(const KeyRef& key) = 0;

	virtual ThreadFuture<Void> watch(const KeyRef& key) = 0;

	virtual void addWriteConflictRange(const KeyRangeRef& keys) = 0;

	virtual ThreadFuture<Void> commit() = 0;
	virtual Version getCommittedVersion() = 0;
	// @todo This API and the "getSpanID()" API may help with debugging simulation
	// test failures. (These APIs are not currently invoked anywhere.) Remove them
	// later if they are not really needed.
	virtual VersionVector getVersionVector() = 0;
	virtual UID getSpanID() = 0;
	virtual ThreadFuture<int64_t> getApproximateSize() = 0;

	virtual void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) = 0;

	virtual ThreadFuture<Void> onError(Error const& e) = 0;
	virtual void reset() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;

	// used in template functions as returned Future type
	template <class Type>
	using FutureT = ThreadFuture<Type>;
	// internal use only, return true by default
	// Only if it's a MultiVersionTransaction and the underlying transaction handler is null,
	// it will return false
	virtual bool isValid() { return true; }

	virtual Optional<TenantName> getTenant() = 0;
};

class ITenant {
public:
	virtual ~ITenant() {}

	virtual Reference<ITransaction> createTransaction() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

// An interface that represents a connection to a cluster made by a client
class IDatabase {
public:
	virtual ~IDatabase() {}

	virtual Reference<ITenant> openTenant(TenantNameRef tenantName) = 0;
	virtual Reference<ITransaction> createTransaction() = 0;
	virtual void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) = 0;
	virtual double getMainThreadBusyness() = 0;

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	virtual ThreadFuture<ProtocolVersion> getServerProtocol(
	    Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>()) = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;

	// Management API, attempt to kill or suspend a process, return 1 for request sent out, 0 for failure
	virtual ThreadFuture<int64_t> rebootWorker(const StringRef& address, bool check, int duration) = 0;
	// Management API, force the database to recover into DCID, causing the database to lose the most recently committed
	// mutations
	virtual ThreadFuture<Void> forceRecoveryWithDataLoss(const StringRef& dcid) = 0;
	// Management API, create snapshot
	virtual ThreadFuture<Void> createSnapshot(const StringRef& uid, const StringRef& snapshot_command) = 0;

	// used in template functions as the Transaction type that can be created through createTransaction()
	using TransactionT = ITransaction;
};

// An interface that presents the top-level FDB client API as exposed through the C bindings
//
// This interface and its associated objects are intended to live outside the network thread, so its asynchronous
// operations use ThreadFutures and implementations should be thread safe.
class IClientApi {
public:
	virtual ~IClientApi() {}

	virtual void selectApiVersion(int apiVersion) = 0;
	virtual const char* getClientVersion() = 0;

	virtual void setNetworkOption(FDBNetworkOptions::Option option,
	                              Optional<StringRef> value = Optional<StringRef>()) = 0;
	virtual void setupNetwork() = 0;
	virtual void runNetwork() = 0;
	virtual void stopNetwork() = 0;

	virtual Reference<IDatabase> createDatabase(const char* clusterFilePath) = 0;

	virtual void addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) = 0;
};

#endif
