/*
 * ISingleThreadTransaction.h
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

#pragma once

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/Error.h"
#include "flow/FastRef.h"

/*
 * Used by ThreadSafeTransaction to execute normal or configuration transaction operations on the network thread
 */
class ISingleThreadTransaction : public ReferenceCounted<ISingleThreadTransaction> {
protected:
	ISingleThreadTransaction() = default;
	ISingleThreadTransaction(Error const& deferredError) : deferredError(deferredError) {}

public:
	virtual ~ISingleThreadTransaction() = default;

	enum class Type {
		RYW,
		SIMPLE_CONFIG,
		PAXOS_CONFIG,
	};

	static ISingleThreadTransaction* allocateOnForeignThread(Type);

	static Reference<ISingleThreadTransaction> create(Type, Database const&);
	static Reference<ISingleThreadTransaction> create(Type, Database const&, TenantName const&);

	virtual void construct(Database const&) = 0;
	virtual void construct(Database const&, TenantName const&) {
		// By default, a transaction implementation does not support tenants.
		ASSERT(false);
	}

	virtual void setVersion(Version v) = 0;
	virtual Future<Version> getReadVersion() = 0;
	virtual Optional<Version> getCachedReadVersion() const = 0;
	virtual Future<Optional<Value>> get(const Key& key, Snapshot = Snapshot::False) = 0;
	virtual Future<Key> getKey(const KeySelector& key, Snapshot = Snapshot::False) = 0;
	virtual Future<RangeResult> getRange(const KeySelector& begin,
	                                     const KeySelector& end,
	                                     int limit,
	                                     Snapshot = Snapshot::False,
	                                     Reverse = Reverse::False) = 0;
	virtual Future<RangeResult> getRange(KeySelector begin,
	                                     KeySelector end,
	                                     GetRangeLimits limits,
	                                     Snapshot = Snapshot::False,
	                                     Reverse = Reverse::False) = 0;
	virtual Future<MappedRangeResult> getMappedRange(KeySelector begin,
	                                                 KeySelector end,
	                                                 Key mapper,
	                                                 GetRangeLimits limits,
	                                                 Snapshot = Snapshot::False,
	                                                 Reverse = Reverse::False) = 0;
	virtual Future<Standalone<VectorRef<const char*>>> getAddressesForKey(Key const& key) = 0;
	virtual Future<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(KeyRange const& range, int64_t chunkSize) = 0;
	virtual Future<int64_t> getEstimatedRangeSizeBytes(KeyRange const& keys) = 0;
	virtual Future<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(KeyRange const& range) = 0;
	virtual Future<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranules(KeyRange const& range,
	                                                                            Version begin,
	                                                                            Optional<Version> readVersion,
	                                                                            Version* readVersionOut = nullptr) = 0;
	virtual void addReadConflictRange(KeyRangeRef const& keys) = 0;
	virtual void makeSelfConflicting() = 0;
	virtual void atomicOp(KeyRef const& key, ValueRef const& operand, uint32_t operationType) = 0;
	virtual void set(KeyRef const& key, ValueRef const& value) = 0;
	virtual void clear(const KeyRangeRef& range) = 0;
	virtual void clear(KeyRef const& key) = 0;
	virtual Future<Void> watch(Key const& key) = 0;
	virtual void addWriteConflictRange(KeyRangeRef const& keys) = 0;
	virtual Future<Void> commit() = 0;
	virtual Version getCommittedVersion() const = 0;
	virtual VersionVector getVersionVector() const = 0;
	virtual UID getSpanID() const = 0;
	virtual int64_t getApproximateSize() const = 0;
	virtual Future<Standalone<StringRef>> getVersionstamp() = 0;
	virtual void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) = 0;
	virtual Future<Void> onError(Error const& e) = 0;
	virtual void cancel() = 0;
	virtual void reset() = 0;
	virtual void debugTransaction(UID dID) = 0;
	virtual void checkDeferredError() const = 0;
	virtual void getWriteConflicts(KeyRangeMap<bool>* result) = 0;

	// Used by ThreadSafeTransaction for exceptions thrown in void methods
	Error deferredError;
};
