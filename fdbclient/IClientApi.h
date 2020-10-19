/*
 * IClientApi.h
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

#ifndef FDBCLIENT_ICLIENTAPI_H
#define FDBCLIENT_ICLIENTAPI_H
#pragma once

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"

#include "flow/ThreadHelper.actor.h"

class ITransaction {
public:
	virtual ~ITransaction() {}

	virtual void cancel() = 0;
	virtual void setVersion(Version v) = 0;
	virtual ThreadFuture<Version> getReadVersion() = 0;

	// These functions that read data return Standalone<...> objects, but these objects are not required to manage their own memory.
	// It is guaranteed, however, that the ThreadFuture will hold a reference to the memory. It will persist until the ThreadFuture's 
	// ThreadSingleAssignmentVar has its memory released or it is destroyed.
	virtual ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot=false) = 0;
	virtual ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot=false) = 0;
	virtual ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot=false, bool reverse=false) = 0;
	virtual ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot=false, bool reverse=false) = 0;
	virtual ThreadFuture<Standalone<RangeResultRef>> getRange(const KeyRangeRef& keys, int limit, bool snapshot=false, bool reverse=false) = 0;
	virtual ThreadFuture<Standalone<RangeResultRef>> getRange( const KeyRangeRef& keys, GetRangeLimits limits, bool snapshot=false, bool reverse=false) = 0;
	virtual ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) = 0;
	virtual ThreadFuture<Standalone<StringRef>> getVersionstamp() = 0;

	virtual void addReadConflictRange(const KeyRangeRef& keys) = 0;
	virtual ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) = 0;
	virtual ThreadFuture<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRangeRef& range,
	                                                                        int64_t chunkSize) = 0;

	virtual void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) = 0;
	virtual void set(const KeyRef& key, const ValueRef& value) = 0;
	virtual void clear(const KeyRef& begin, const KeyRef& end) = 0;
	virtual void clear(const KeyRangeRef& range) = 0;
	virtual void clear(const KeyRef& key) = 0;

	virtual ThreadFuture<Void> watch(const KeyRef& key) = 0;

	virtual void addWriteConflictRange(const KeyRangeRef& keys) = 0;

	virtual ThreadFuture<Void> commit() = 0;
	virtual Version getCommittedVersion() = 0;
	virtual ThreadFuture<int64_t> getApproximateSize() = 0;

	virtual void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value=Optional<StringRef>()) = 0;

	virtual ThreadFuture<Void> onError(Error const& e) = 0;
	virtual void reset() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

class IDatabase {
public:
	virtual ~IDatabase() {}

	virtual Reference<ITransaction> createTransaction() = 0;
	virtual void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

class IClientApi {
public:
	virtual ~IClientApi() {}

	virtual void selectApiVersion(int apiVersion) = 0;
	virtual const char* getClientVersion() = 0;
	virtual ThreadFuture<uint64_t> getServerProtocol(const char* clusterFilePath) = 0;

	virtual void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) = 0;
	virtual void setupNetwork() = 0;
	virtual void runNetwork() = 0;
	virtual void stopNetwork() = 0;

	virtual Reference<IDatabase> createDatabase(const char *clusterFilePath) = 0;

	virtual void addNetworkThreadCompletionHook(void (*hook)(void*), void *hookParameter) = 0;
};

#endif
