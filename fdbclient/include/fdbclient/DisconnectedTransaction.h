/*
 * DisconnectedTransaction.h
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

#ifndef FDBCLIENT_DISCONNECTEDTRANSACTION_H
#define FDBCLIENT_DISCONNECTEDTRANSACTION_H
#pragma once

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"

class DisconnectedTransaction : public ITransaction, ThreadSafeReferenceCounted<DisconnectedTransaction> {
public:
	DisconnectedTransaction(Optional<TenantName> tenant);
	~DisconnectedTransaction() override;

	void cancel() override;
	void setVersion(Version v) override;
	ThreadFuture<Version> getReadVersion() override;

	ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot = false) override;
	ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot = false) override;
	ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                   const KeySelectorRef& end,
	                                   int limit,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                   const KeySelectorRef& end,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   int limit,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<MappedRangeResult> getMappedRange(const KeySelectorRef& begin,
	                                               const KeySelectorRef& end,
	                                               const StringRef& mapper,
	                                               GetRangeLimits limits,
	                                               int matchIndex,
	                                               bool snapshot,
	                                               bool reverse) override;
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) override;
	ThreadFuture<Standalone<StringRef>> getVersionstamp() override;

	void addReadConflictRange(const KeyRangeRef& keys) override;
	ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) override;

	ThreadFuture<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRangeRef& range,
	                                                                int64_t chunkSize) override;
	ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRangeRef& keyRange,
	                                                                      int rangeLimit) override;

	ThreadResult<RangeResult> readBlobGranules(const KeyRangeRef& keyRange,
	                                           Version beginVersion,
	                                           Optional<Version> readVersion,
	                                           ReadBlobGranuleContext granuleContext) override;

	void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) override;
	void set(const KeyRef& key, const ValueRef& value) override;
	void clear(const KeyRef& begin, const KeyRef& end) override;
	void clear(const KeyRangeRef& range) override;
	void clear(const KeyRef& key) override;

	ThreadFuture<Void> watch(const KeyRef& key) override;

	void addWriteConflictRange(const KeyRangeRef& keys) override;

	ThreadFuture<Void> commit() override;
	Version getCommittedVersion() override;
	ThreadFuture<VersionVector> getVersionVector() override;
	ThreadFuture<SpanContext> getSpanContext() override;
	ThreadFuture<int64_t> getApproximateSize() override;

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	ThreadFuture<Void> onError(Error const& e) override;
	void reset() override;

	Optional<TenantName> getTenant() override;

	void addref() override { ThreadSafeReferenceCounted<DisconnectedTransaction>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<DisconnectedTransaction>::delref(); }

	// return true if the underlying transaction pointer is not empty
	bool isValid() override;

	bool isReplayable() override { return true; }
	void replay(Reference<ITransaction> tr) override;

private:
	// The time when the MultiVersionTransaction was last created or reset
	std::atomic<double> startTime;

	// A lock that needs to be held if using timeoutTsav or currentTimeout
	ThreadSpinLock timeoutLock;

	// A single assignment var (i.e. promise) that gets set with an error when the timeout elapses or the transaction
	// is reset or destroyed.
	Reference<ThreadSingleAssignmentVar<Void>> timeoutTsav;

	// A reference to the current actor waiting for the timeout. This actor will set the timeoutTsav promise.
	ThreadFuture<Void> currentTimeout;

	// Configure a timeout based on the options set for this transaction. This timeout only applies
	// if we don't have an underlying database object to connect with.
	void setTimeout(Optional<StringRef> value);

	// Creates a ThreadFuture that will be set (to Void) when the transaction times out.
	ThreadFuture<Void> makeTimeout();

	template <class... Args>
	void storeOperation(void (ITransaction::*func)(Args...), Args&&... args);

	template <class T, class... Args>
	ThreadFuture<T> storeOperation(ThreadFuture<T> (ITransaction::*func)(Args...), Args&&... args);

	template <class T, class... Args>
	ThreadResult<T> storeOperation(ThreadResult<T> (ITransaction::*func)(Args...), Args&&... args);

	Arena arena;
	Mutex mutex;
	std::vector<std::function<void(Reference<ITransaction>)>> operations;
	Optional<TenantName> tenant;
};

#endif
