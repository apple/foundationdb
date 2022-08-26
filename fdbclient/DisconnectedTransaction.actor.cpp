/*
 * DisconnectedTransaction.actor.cpp
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

#ifdef ADDRESS_SANITIZER
#include <sanitizer/lsan_interface.h>
#endif

#include "fdbclient/DisconnectedTransaction.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/MultiVersionAssignmentVars.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/VersionVector.h"

#include "flow/network.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#ifdef FDBCLIENT_NATIVEAPI_ACTOR_H
#error "MVC should not depend on the Native API"
#endif

DisconnectedTransaction::DisconnectedTransaction(Optional<TenantName> tenant)
  : startTime(timer_monotonic()), timeoutTsav(new ThreadSingleAssignmentVar<Void>()), tenant(tenant) {}

DisconnectedTransaction::~DisconnectedTransaction() {
	// timeoutTsav->trySendError(transaction_cancelled());
}

void DisconnectedTransaction::cancel() {
	storeOperation(&ITransaction::cancel);
}

void DisconnectedTransaction::setVersion(Version v) {
	storeOperation(&ITransaction::setVersion, std::forward<Version>(v));
}

ThreadFuture<Version> DisconnectedTransaction::getReadVersion() {
	return storeOperation(&ITransaction::getReadVersion);
}

ThreadFuture<Optional<Value>> DisconnectedTransaction::get(const KeyRef& key, bool snapshot) {
	return storeOperation(&ITransaction::get, key, std::forward<bool>(snapshot));
}

ThreadFuture<Key> DisconnectedTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	return storeOperation(&ITransaction::getKey, key, std::forward<bool>(snapshot));
}

ThreadFuture<RangeResult> DisconnectedTransaction::getRange(const KeySelectorRef& begin,
                                                            const KeySelectorRef& end,
                                                            int limit,
                                                            bool snapshot,
                                                            bool reverse) {
	return storeOperation<RangeResult>(&ITransaction::getRange,
	                                   begin,
	                                   end,
	                                   std::forward<int>(limit),
	                                   std::forward<bool>(snapshot),
	                                   std::forward<bool>(reverse));
}

ThreadFuture<RangeResult> DisconnectedTransaction::getRange(const KeySelectorRef& begin,
                                                            const KeySelectorRef& end,
                                                            GetRangeLimits limits,
                                                            bool snapshot,
                                                            bool reverse) {
	return storeOperation<RangeResult>(&ITransaction::getRange,
	                                   begin,
	                                   end,
	                                   std::forward<GetRangeLimits>(limits),
	                                   std::forward<bool>(snapshot),
	                                   std::forward<bool>(reverse));
}

ThreadFuture<RangeResult> DisconnectedTransaction::getRange(const KeyRangeRef& keys,
                                                            int limit,
                                                            bool snapshot,
                                                            bool reverse) {
	return storeOperation<RangeResult>(&ITransaction::getRange,
	                                   keys,
	                                   std::forward<int>(limit),
	                                   std::forward<bool>(snapshot),
	                                   std::forward<bool>(reverse));
}

ThreadFuture<RangeResult> DisconnectedTransaction::getRange(const KeyRangeRef& keys,
                                                            GetRangeLimits limits,
                                                            bool snapshot,
                                                            bool reverse) {
	return storeOperation<RangeResult>(&ITransaction::getRange,
	                                   keys,
	                                   std::forward<GetRangeLimits>(limits),
	                                   std::forward<bool>(snapshot),
	                                   std::forward<bool>(reverse));
}

ThreadFuture<MappedRangeResult> DisconnectedTransaction::getMappedRange(const KeySelectorRef& begin,
                                                                        const KeySelectorRef& end,
                                                                        const StringRef& mapper,
                                                                        GetRangeLimits limits,
                                                                        int matchIndex,
                                                                        bool snapshot,
                                                                        bool reverse) {
	return storeOperation(&ITransaction::getMappedRange,
	                      begin,
	                      end,
	                      mapper,
	                      std::forward<GetRangeLimits>(limits),
	                      std::forward<int>(matchIndex),
	                      std::forward<bool>(snapshot),
	                      std::forward<bool>(reverse));
}

ThreadFuture<Standalone<VectorRef<const char*>>> DisconnectedTransaction::getAddressesForKey(const KeyRef& key) {
	return storeOperation(&ITransaction::getAddressesForKey, key);
}

ThreadFuture<Standalone<StringRef>> DisconnectedTransaction::getVersionstamp() {
	return storeOperation(&ITransaction::getVersionstamp);
}

void DisconnectedTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	storeOperation(&ITransaction::addReadConflictRange, keys);
}

ThreadFuture<int64_t> DisconnectedTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	return storeOperation(&ITransaction::getEstimatedRangeSizeBytes, keys);
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> DisconnectedTransaction::getRangeSplitPoints(const KeyRangeRef& range,
                                                                                         int64_t chunkSize) {
	return storeOperation(&ITransaction::getRangeSplitPoints, range, std::forward<int64_t>(chunkSize));
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> DisconnectedTransaction::getBlobGranuleRanges(
    const KeyRangeRef& keyRange,
    int rangeLimit) {
	return storeOperation(&ITransaction::getBlobGranuleRanges, keyRange, std::forward<int>(rangeLimit));
}

ThreadResult<RangeResult> DisconnectedTransaction::readBlobGranules(const KeyRangeRef& keyRange,
                                                                    Version beginVersion,
                                                                    Optional<Version> readVersion,
                                                                    ReadBlobGranuleContext granuleContext) {
	return storeOperation(&ITransaction::readBlobGranules,
	                      keyRange,
	                      std::forward<Version>(beginVersion),
	                      std::forward<Optional<Version>>(readVersion),
	                      std::forward<ReadBlobGranuleContext>(granuleContext));
}

void DisconnectedTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	storeOperation(&ITransaction::atomicOp, key, value, std::forward<uint32_t>(operationType));
}

void DisconnectedTransaction::set(const KeyRef& key, const ValueRef& value) {
	storeOperation(&ITransaction::set, key, value);
}

void DisconnectedTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	storeOperation(&ITransaction::clear, begin, end);
}

void DisconnectedTransaction::clear(const KeyRangeRef& range) {
	storeOperation(&ITransaction::clear, range);
}

void DisconnectedTransaction::clear(const KeyRef& key) {
	storeOperation(&ITransaction::clear, key);
}

ThreadFuture<Void> DisconnectedTransaction::watch(const KeyRef& key) {
	return storeOperation(&ITransaction::watch, key);
}

void DisconnectedTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	storeOperation(&ITransaction::addWriteConflictRange, keys);
}

ThreadFuture<Void> DisconnectedTransaction::commit() {
	return storeOperation(&ITransaction::commit);
}

Version DisconnectedTransaction::getCommittedVersion() {
	return invalidVersion;
}

ThreadFuture<VersionVector> DisconnectedTransaction::getVersionVector() {
	return storeOperation(&ITransaction::getVersionVector);
}

ThreadFuture<SpanContext> DisconnectedTransaction::getSpanContext() {
	return storeOperation(&ITransaction::getSpanContext);
}

ThreadFuture<int64_t> DisconnectedTransaction::getApproximateSize() {
	return storeOperation(&ITransaction::getApproximateSize);
}

void DisconnectedTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	if (option == FDBTransactionOptions::TIMEOUT) {
		setTimeout(value);
	}

	return storeOperation(&ITransaction::setOption,
	                      std::forward<FDBTransactionOptions::Option>(option),
	                      std::forward<Optional<StringRef>>(value));
}

ThreadFuture<Void> DisconnectedTransaction::onError(Error const& e) {
	return storeOperation(&ITransaction::onError, std::forward<const Error&>(e));
}

void DisconnectedTransaction::reset() {
	operations.clear();

	startTime = timer_monotonic();
	Reference<ThreadSingleAssignmentVar<Void>> prevTimeoutTsav;
	ThreadFuture<Void> prevTimeout;

	{ // lock scope
		ThreadSpinLockHolder holder(timeoutLock);

		prevTimeoutTsav = timeoutTsav;
		timeoutTsav = makeReference<ThreadSingleAssignmentVar<Void>>();

		prevTimeout = currentTimeout;
		currentTimeout = ThreadFuture<Void>();
	}

	// Cancel any outstanding operations if they don't have an underlying transaction object to cancel them
	prevTimeoutTsav->trySendError(transaction_cancelled());
	if (prevTimeout.isValid()) {
		prevTimeout.cancel();
	}
}

Optional<TenantName> DisconnectedTransaction::getTenant() {
	return tenant;
}

bool DisconnectedTransaction::isValid() {
	return false;
}

// Waits for the specified duration and signals the assignment variable. This will be canceled if a new timeout is set,
// in which case the tsav will not be signaled.
ACTOR Future<Void> timeoutImpl(Reference<ThreadSingleAssignmentVar<Void>> tsav, double duration) {
	wait(delay(duration));
	tsav->trySend(Void());
	return Void();
}

// Configure a timeout based on the options set for this transaction. This timeout only applies
// if we don't have an underlying database object to connect with.
void DisconnectedTransaction::setTimeout(Optional<StringRef> value) {
	double timeoutDuration = extractIntOption(value, 0, std::numeric_limits<int>::max()) / 1000.0;

	ThreadFuture<Void> prevTimeout;
	double transactionStartTime = startTime;

	{ // lock scope
		ThreadSpinLockHolder holder(timeoutLock);

		Reference<ThreadSingleAssignmentVar<Void>> tsav = timeoutTsav;
		ThreadFuture<Void> newTimeout = onMainThread([transactionStartTime, tsav, timeoutDuration]() {
			return timeoutImpl(tsav, timeoutDuration - std::max(0.0, now() - transactionStartTime));
		});

		prevTimeout = currentTimeout;
		currentTimeout = newTimeout;
	}

	// Cancel the previous timeout now that we have a new one. This means that changing the timeout
	// affects in-flight operations, which is consistent with the behavior in RYW.
	if (prevTimeout.isValid()) {
		prevTimeout.cancel();
	}
}

// Creates a ThreadFuture<T> that will signal an error if the transaction times out.
ThreadFuture<Void> DisconnectedTransaction::makeTimeout() {
	ThreadFuture<Void> f;

	{ // lock scope
		ThreadSpinLockHolder holder(timeoutLock);

		// Our ThreadFuture holds a reference to this TSAV,
		// but the ThreadFuture does not increment the ref count
		timeoutTsav->addref();
		f = ThreadFuture<Void>(timeoutTsav.getPtr());
	}

	return f;
}

void DisconnectedTransaction::replay(Reference<ITransaction> tr) {
	mutex.enter();
	std::vector<std::function<void(Reference<ITransaction>)>> opsToApply = operations;
	mutex.leave();

	for (auto f : opsToApply) {
		f(tr);
	}
}

template <class... Args>
void DisconnectedTransaction::storeOperation(void (ITransaction::*func)(Args...), Args&&... args) {
	auto copiedArgs = std::make_tuple(copyRefType(arena, args)...);
	MutexHolder holder(mutex);
	operations.push_back([copiedArgs, func](Reference<ITransaction> tr) {
		std::apply([tr, func](Args... a) { (tr.getPtr()->*func)(std::forward<Args>(a)...); }, copiedArgs);
	});
}

template <class T, class... Args>
ThreadFuture<T> DisconnectedTransaction::storeOperation(ThreadFuture<T> (ITransaction::*func)(Args...),
                                                        Args&&... args) {
	Reference<ThreadSingleAssignmentVar<T>> sav = makeReference<ThreadSingleAssignmentVar<T>>();

	ThreadFuture<T> f(sav.getPtr());
	sav->addref();

	auto copiedArgs = std::make_tuple(copyRefType(arena, args)...);

	{
		MutexHolder holder(mutex);
		operations.push_back([copiedArgs, func, sav](Reference<ITransaction> tr) {
			ThreadFuture<T> result = std::apply(
			    [tr, func](Args... a) { return (tr.getPtr()->*func)(std::forward<Args>(a)...); }, copiedArgs);
			mapThreadFuture<T, T>(result, [sav](ErrorOr<T> val) {
				if (val.isError()) {
					sav->sendError(val.getError());
				} else {
					sav->send(val.get());
				}

				return val;
			});
		});
	}

	return abortableFuture(f, makeTimeout(), error_code_transaction_timed_out);
}

template <class T, class... Args>
ThreadResult<T> DisconnectedTransaction::storeOperation(ThreadResult<T> (ITransaction::*func)(Args...),
                                                        Args&&... args) {
	try {
		Reference<ThreadSingleAssignmentVar<T>> sav;

		ThreadFuture<T> f(sav.getPtr());
		sav->addref();

		auto copiedArgs = std::make_tuple(copyRefType(arena, args)...);

		{
			MutexHolder holder(mutex);
			operations.push_back([copiedArgs, func](Reference<ITransaction> tr) {
				ThreadResult<T> result = std::apply(
				    [tr, func](Args... a) { return (tr.getPtr()->*func)(std::forward<Args>(a)...); }, copiedArgs);
			});
		}

		f = abortableFuture(f, makeTimeout(), error_code_transaction_timed_out);
		f.blockUntilReadyCheckOnMainThread();

		return ThreadResult<T>(sav.extractPtr());
	} catch (Error& e) {
		return ThreadResult<T>(e);
	}
}
