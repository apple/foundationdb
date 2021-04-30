/*
 * ISingleThreadTransaction.h
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

#pragma once

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "flow/Error.h"
#include "flow/FastRef.h"

class ISingleThreadTransaction : public ReferenceCounted<ISingleThreadTransaction> {
public:
	virtual ~ISingleThreadTransaction() = default;

	virtual void setVersion(Version v) = 0;
	virtual Future<Version> getReadVersion() = 0;
	virtual Optional<Version> getCachedReadVersion() = 0;
	virtual Future<Optional<Value>> get(const Key& key, bool snapshot = false) = 0;
	virtual Future<Key> getKey(const KeySelector& key, bool snapshot = false) = 0;
	virtual Future<Standalone<RangeResultRef>> getRange(const KeySelector& begin,
	                                                    const KeySelector& end,
	                                                    int limit,
	                                                    bool snapshot = false,
	                                                    bool reverse = false) = 0;
	virtual Future<Standalone<RangeResultRef>> getRange(KeySelector begin,
	                                                    KeySelector end,
	                                                    GetRangeLimits limits,
	                                                    bool snapshot = false,
	                                                    bool reverse = false) = 0;
	virtual Future<Standalone<VectorRef<const char*>>> getAddressesForKey(const Key& key) = 0;
	virtual Future<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRange& range, int64_t chunkSize) = 0;
	virtual Future<int64_t> getEstimatedRangeSizeBytes(const KeyRange& keys) = 0;
	virtual void addReadConflictRange(KeyRangeRef const& keys) = 0;
	virtual void makeSelfConflicting() = 0;
	virtual void atomicOp(const KeyRef& key, const ValueRef& operand, uint32_t operationType) = 0;
	virtual void set(const KeyRef& key, const ValueRef& value) = 0;
	virtual void clear(const KeyRangeRef& range) = 0;
	virtual void clear(const KeyRef& key) = 0;
	virtual Future<Void> watch(const Key& key) = 0;
	virtual void addWriteConflictRange(KeyRangeRef const& keys) = 0;
	virtual Future<Void> commit() = 0;
	virtual Version getCommittedVersion() const = 0;
	virtual int64_t getApproximateSize() const = 0;
	virtual Future<Standalone<StringRef>> getVersionstamp() = 0;
	virtual void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) = 0;
	virtual Future<Void> onError(Error const& e) = 0;
	virtual void cancel() = 0;
	virtual void reset() = 0;
	virtual void debugTransaction(UID dID) = 0;
	virtual void checkDeferredError() = 0;
	virtual void getWriteConflicts(KeyRangeMap<bool>* result) = 0;

	// Used by ThreadSafeTransaction for exceptions thrown in void methods
	virtual Error& getMutableDeferredError() = 0;
};
