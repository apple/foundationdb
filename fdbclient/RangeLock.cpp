/*
 * RangeLock.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/RangeLock.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "flow/Trace.h"

// Persist a new owner if input ownerUniqueID is not existing; Update description if input ownerUniqueID exists
Future<Void> registerRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID, std::string description) {
	if (ownerUniqueID.empty() || description.empty()) {
		throw range_lock_failed();
	}
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<Value> res = co_await tr.get(rangeLockOwnerKeyFor(ownerUniqueID));
			RangeLockOwner owner;
			if (res.present()) {
				owner = decodeRangeLockOwner(res.get());
				ASSERT(owner.isValid());
				if (owner.getDescription() == description) {
					co_return;
				}
				owner.setDescription(description);
			} else {
				owner = RangeLockOwner(ownerUniqueID, description);
			}
			tr.set(rangeLockOwnerKeyFor(ownerUniqueID), rangeLockOwnerValue(owner));
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Void> removeRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID) {
	if (ownerUniqueID.empty()) {
		throw range_lock_failed();
	}
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<Value> res = co_await tr.get(rangeLockOwnerKeyFor(ownerUniqueID));
			if (!res.present()) {
				co_return;
			}
			RangeLockOwner owner = decodeRangeLockOwner(res.get());
			ASSERT(owner.isValid());
			tr.clear(rangeLockOwnerKeyFor(ownerUniqueID));
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> res = co_await tr.get(rangeLockOwnerKeyFor(ownerUniqueID));
			if (!res.present()) {
				co_return Optional<RangeLockOwner>();
			}
			RangeLockOwner owner = decodeRangeLockOwner(res.get());
			ASSERT(owner.isValid());
			co_return owner;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

AsyncResult<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx) {
	std::vector<RangeLockOwner> res;
	Key beginKey = rangeLockOwnerKeys.begin;
	Key endKey = rangeLockOwnerKeys.end;
	Transaction tr(cx);
	while (beginKey < endKey) {
		KeyRange rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult result = co_await tr.getRange(rangeToRead, CLIENT_KNOBS->TOO_MANY);
			for (const auto& kv : result) {
				RangeLockOwner owner = decodeRangeLockOwner(kv.value);
				ASSERT(owner.isValid());
				res.push_back(owner);
				beginKey = keyAfter(kv.key);
			}
			if (!result.more) {
				break;
			}
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	co_return res;
}

// Not transactional
Future<std::vector<std::pair<KeyRange, RangeLockState>>>
findExclusiveReadLockOnRange(Database cx, KeyRange range, Optional<RangeLockOwnerName> ownerName) {
	if (range.end > normalKeys.end) {
		throw range_lock_failed();
	}
	std::vector<std::pair<KeyRange, RangeLockState>> lockedRanges;
	Key beginKey = range.begin;
	Key endKey = range.end;
	Transaction tr(cx);
	while (beginKey < endKey) {
		KeyRange rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult result = co_await krmGetRanges(&tr, rangeLockPrefix, rangeToRead);
			if (result.empty()) {
				break;
			}
			for (int i = 0; i < static_cast<int>(result.size()) - 1; i++) {
				if (result[i].value.empty()) {
					continue;
				}
				RangeLockStateSet rangeLockStateSet = decodeRangeLockStateSet(result[i].value);
				ASSERT(rangeLockStateSet.isValid());
				if (rangeLockStateSet.isLockedFor(RangeLockType::ExclusiveReadLock) &&
				    (!ownerName.present() ||
				     ownerName.get() == rangeLockStateSet.getAllLockStats()[0].getOwnerUniqueId())) {
					// Exclusive lock can only have one lock in the set, so we just check the first lock in the set
					lockedRanges.push_back(std::make_pair(Standalone(KeyRangeRef(result[i].key, result[i + 1].key)),
					                                      rangeLockStateSet.getAllLockStats()[0]));
				}
			}
			beginKey = result.back().key;
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	co_return lockedRanges;
}

namespace {

// Validate the input range and owner.
// If invalid, reject the request by throwing range_lock_failed error.
// If the range has been locked, reject the request by throwing range_lock_reject error.
Future<Void> prepareExclusiveRangeLockOperation(Transaction* tr, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	// Check input range
	if (range.end > normalKeys.end) {
		TraceEvent(SevDebug, "PrepareExclusiveRangeLockOperationFailed")
		    .detail("Reason", "Range out of scope")
		    .detail("Range", range);
		throw range_lock_failed();
	}
	// Check owner
	Optional<Value> ownerValue = co_await tr->get(rangeLockOwnerKeyFor(ownerUniqueID));
	if (!ownerValue.present()) {
		TraceEvent(SevDebug, "PrepareExclusiveRangeLockOperationFailed")
		    .detail("Reason", "Owner not found")
		    .detail("Owner", ownerUniqueID)
		    .detail("Range", range);
		throw range_lock_failed();
	}
	RangeLockOwner owner = decodeRangeLockOwner(ownerValue.get());
	ASSERT(owner.isValid());
	// Check lock state on the entire input range. Throw exception if the range has been locked by a different owner.
	Key beginKey = range.begin;
	Key endKey = range.end;
	KeyRange rangeToRead;
	while (beginKey < endKey) {
		rangeToRead = KeyRangeRef(beginKey, endKey);
		RangeResult res = co_await krmGetRanges(tr, rangeLockPrefix, rangeToRead);
		if (res.empty()) {
			break;
		}
		for (int i = 0; i < static_cast<int>(res.size()) - 1; i++) {
			if (res[i].value.empty()) {
				continue;
			}
			RangeLockStateSet rangeLockStateSet = decodeRangeLockStateSet(res[i].value);
			ASSERT(rangeLockStateSet.isValid());
			auto lockSet = rangeLockStateSet.getLocks();
			if (!lockSet.empty() && (!rangeLockStateSet.isLockedFor(RangeLockType::ExclusiveReadLock) ||
			                         lockSet.find(RangeLockState(RangeLockType::ExclusiveReadLock, ownerUniqueID, range)
			                                          .getLockUniqueString()) == lockSet.end())) {
				TraceEvent(SevDebug, "PrepareExclusiveRangeLockOperationFailed")
				    .detail("Reason", "Locked")
				    .detail("NewLockType", RangeLockType::ExclusiveReadLock)
				    .detail("NewLockRange", range)
				    .detail("NewLockOwner", ownerUniqueID)
				    .detail("ExistingLocks", rangeLockStateSet.toString());
				throw range_lock_reject(); // Has been locked
			}
		}
		beginKey = res.back().key;
	}
}

Future<Void> prepareExclusiveRangeUnlockOperation(Transaction* tr, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	// Check input range
	if (range.end > normalKeys.end) {
		TraceEvent(SevDebug, "PrepareExclusiveRangeUnlockOperationFailed")
		    .detail("Reason", "Range out of scope")
		    .detail("Range", range);
		throw range_lock_failed();
	}
	// Check owner
	Optional<Value> ownerValue = co_await tr->get(rangeLockOwnerKeyFor(ownerUniqueID));
	if (!ownerValue.present()) {
		TraceEvent(SevDebug, "PrepareExclusiveRangeUnlockOperationFailed")
		    .detail("Reason", "Owner not found")
		    .detail("Owner", ownerUniqueID)
		    .detail("Range", range);
		throw range_lock_failed();
	}
	RangeLockOwner owner = decodeRangeLockOwner(ownerValue.get());
	ASSERT(owner.isValid());

	// Check lock state on the entire input range. Throw exception if the range has been locked by a different owner.
	Key beginKey = range.begin;
	Key endKey = range.end;
	KeyRange rangeToRead;
	while (beginKey < endKey) {
		rangeToRead = KeyRangeRef(beginKey, endKey);
		RangeResult res = co_await krmGetRanges(tr, rangeLockPrefix, rangeToRead);
		if (res.empty()) {
			break;
		}
		for (int i = 0; i < static_cast<int>(res.size()) - 1; i++) {
			if (res[i].value.empty()) {
				continue;
			}
			RangeLockStateSet rangeLockStateSet = decodeRangeLockStateSet(res[i].value);
			ASSERT(rangeLockStateSet.isValid());
			auto lockSet = rangeLockStateSet.getLocks();
			if (!lockSet.empty() && (!rangeLockStateSet.isLockedFor(RangeLockType::ExclusiveReadLock) ||
			                         lockSet.find(RangeLockState(RangeLockType::ExclusiveReadLock, ownerUniqueID, range)
			                                          .getLockUniqueString()) == lockSet.end())) {
				TraceEvent(SevDebug, "PrepareExclusiveRangeUnlockOperationFailed")
				    .detail("Reason", "Has been locked by a different user or the same user with a different range")
				    .detail("UnLockOwner", ownerUniqueID)
				    .detail("UnLockRange", range)
				    .detail("ExistingLocks", rangeLockStateSet.toString());
				throw range_unlock_reject();
			}
		}
		beginKey = res.back().key;
	}
}

} // namespace

// Transactional. One transaction can call takeExclusiveReadLockOnRange at most for one time.
// This is the limitation of the krmSetRangeCoalescing.
Future<Void> takeExclusiveReadLockOnRange(Transaction* tr, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	// Add conflict range
	tr->addWriteConflictRange(range);
	co_await prepareExclusiveRangeLockOperation(tr, range, ownerUniqueID);
	// At this point, no lock presents on the range.
	// Lock range by writting the range.
	RangeLockStateSet rangeLockStateSet;
	rangeLockStateSet.insertIfNotExist(RangeLockState(RangeLockType::ExclusiveReadLock, ownerUniqueID, range));
	co_await krmSetRange(tr, rangeLockPrefix, range, rangeLockStateSetValue(rangeLockStateSet));
	TraceEvent(SevInfo, "TakeExclusiveReadLockTransactionOnRange").detail("Range", range);
}

// Transactional. One transaction can call releaseExclusiveReadLockOnRange at most for one time.
// This is the limitation of the krmSetRangeCoalescing.
Future<Void> releaseExclusiveReadLockOnRange(Transaction* tr, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	co_await prepareExclusiveRangeUnlockOperation(tr, range, ownerUniqueID);
	// At this point, no lock presents on the range.
	// Unlock by overwiting the range.
	co_await krmSetRangeCoalescing(tr, rangeLockPrefix, range, normalKeys, rangeLockStateSetValue(RangeLockStateSet()));
	TraceEvent(SevInfo, "ReleaseExclusiveReadLockTransactionOnRange").detail("Range", range);
}

Future<Void> releaseExclusiveReadLockByUser(Database cx, RangeLockOwnerName ownerUniqueID) {
	Key beginKey = normalKeys.begin;
	Key endKey = normalKeys.end;
	Transaction tr(cx);
	int i = 0;
	RangeResult result;
	KeyRange rangeToRead;
	RangeLockStateSet currentRangeLockStateSet;
	KeyRange currentRange;
	Key beginKeyToClear;
	Key endKeyToClear;
	while (beginKey < endKey) {
		rangeToRead = Standalone(KeyRangeRef(beginKey, endKey));
		Error err;
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			result.clear();
			result = co_await krmGetRanges(&tr, rangeLockPrefix, rangeToRead);
			if (result.empty()) {
				break;
			}
			i = 0;
			beginKeyToClear = result[0].key;
			endKeyToClear = result[0].key; // Expanding when currentRange is valid to clear
			for (; i < static_cast<int>(result.size()) - 1; i++) {
				currentRange = KeyRangeRef(result[i].key, result[i + 1].key);
				if (result[i].value.empty()) {
					endKeyToClear = currentRange.end;
					continue;
				}
				currentRangeLockStateSet = decodeRangeLockStateSet(result[i].value);
				ASSERT(currentRangeLockStateSet.isValid());
				if (currentRangeLockStateSet.isLockedFor(RangeLockType::ExclusiveReadLock) &&
				    currentRangeLockStateSet.getAllLockStats()[0].getOwnerUniqueId() == ownerUniqueID) {
					// If this range is exclusively locked by the input owner, we will clear it.
					endKeyToClear = currentRange.end;
					continue;
				}
				break;
			}
			if (beginKeyToClear != endKeyToClear) {
				ASSERT(endKeyToClear > beginKeyToClear);
				co_await krmSetRangeCoalescing(&tr,
				                               rangeLockPrefix,
				                               KeyRangeRef(beginKeyToClear, endKeyToClear),
				                               normalKeys,
				                               rangeLockStateSetValue(RangeLockStateSet()));
				co_await tr.commit();
			}
			beginKey = currentRange.end; // We skip the currentRange if it is not locked by the input owner.
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Transactional
Future<Void> takeExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			co_await takeExclusiveReadLockOnRange(&tr, range, ownerUniqueID);
			co_await tr.commit();
			TraceEvent(SevInfo, "TakeExclusiveReadLockOnRange").detail("Range", range);
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Transactional
Future<Void> releaseExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			co_await releaseExclusiveReadLockOnRange(&tr, range, ownerUniqueID);
			co_await tr.commit();
			TraceEvent(SevInfo, "ReleaseExclusiveReadLockOnRange").detail("Range", range);
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}
