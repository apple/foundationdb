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
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

namespace {

void validateRangeLockRange(const KeyRangeRef& range) {
	if (range.empty() || !normalKeys.contains(range)) {
		throw range_lock_failed();
	}
}

void validateRangeLockOwnerName(const RangeLockOwnerName& owner) {
	if (owner.empty()) {
		throw range_lock_failed();
	}
}

} // namespace

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

namespace {

Future<Void> removeRangeLockOwnerImpl(Database cx, RangeLockOwnerName ownerUniqueID, Optional<UID> expectedGeneration) {
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
			if (!expectedGeneration.present()) {
				expectedGeneration = owner.getGeneration();
			} else if (owner.getGeneration() != expectedGeneration.get()) {
				throw range_lock_failed();
			}
			// Acquisition reads the registration key; retirement also conflicts with every lock-map change.
			// Together these reads prevent a concurrent take from leaving an unregistered owner with held locks.
			tr.addReadConflictRange(rangeLockKeys);
			Key beginKey = normalKeys.begin;
			while (beginKey < normalKeys.end) {
				RangeResult locks = co_await krmGetRanges(&tr, rangeLockPrefix, KeyRangeRef(beginKey, normalKeys.end));
				if (locks.empty()) {
					break;
				}
				for (int i = 0; i < static_cast<int>(locks.size()) - 1; ++i) {
					if (locks[i].value.empty()) {
						continue;
					}
					for (const auto& lock : decodeRangeLockStateSet(locks[i].value).getAllLockStats()) {
						if (lock.getOwnerUniqueId() == ownerUniqueID) {
							throw range_lock_reject();
						}
					}
				}
				ASSERT(locks.back().key > beginKey);
				beginKey = locks.back().key;
			}
			tr.clear(rangeLockOwnerKeyFor(ownerUniqueID));
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

} // namespace

Future<Void> removeRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID) {
	return removeRangeLockOwnerImpl(cx, ownerUniqueID, Optional<UID>());
}

Future<Void> removeRangeLockOwner(Database cx, RangeLockOwner expectedOwner) {
	if (!expectedOwner.isValid() || !expectedOwner.getGeneration().isValid()) {
		throw range_lock_failed();
	}
	co_await removeRangeLockOwnerImpl(cx, expectedOwner.getOwnerUniqueId(), expectedOwner.getGeneration());
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
	validateRangeLockRange(range);
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

void validateFencedRangeLock(const RangeLockState& lock) {
	if (!lock.isValid() || !lock.isLockedFor(RangeLockType::ExclusiveReadLock) || lock.getLockId().empty()) {
		throw range_lock_failed();
	}
	validateRangeLockRange(lock.getRange());
}

Future<Void> checkRangeLockOwner(Transaction* tr,
                                 RangeLockOwnerName ownerUniqueID,
                                 Optional<UID> expectedGeneration = Optional<UID>()) {
	Optional<Value> ownerValue = co_await tr->get(rangeLockOwnerKeyFor(ownerUniqueID));
	if (!ownerValue.present()) {
		throw range_lock_failed();
	}
	RangeLockOwner owner = decodeRangeLockOwner(ownerValue.get());
	if (!owner.isValid() || (expectedGeneration.present() && owner.getGeneration() != expectedGeneration.get())) {
		throw range_lock_failed();
	}
}

struct ExclusiveRangeLockScan {
	bool fullyHeld = true;
	bool conflicting = false;
	Optional<RangeLockState> lock;
};

Future<ExclusiveRangeLockScan> scanExclusiveRangeLock(Transaction* tr, RangeLockState requestedLock) {
	ExclusiveRangeLockScan result;
	Key beginKey = requestedLock.getRange().begin;
	const Key endKey = requestedLock.getRange().end;
	while (beginKey < endKey) {
		RangeResult ranges = co_await krmGetRanges(tr, rangeLockPrefix, KeyRangeRef(beginKey, endKey));
		if (ranges.size() < 2) {
			result.fullyHeld = false;
			break;
		}
		for (int i = 0; i < static_cast<int>(ranges.size()) - 1; ++i) {
			RangeLockStateSet locks =
			    ranges[i].value.empty() ? RangeLockStateSet() : decodeRangeLockStateSet(ranges[i].value);
			if (locks.empty()) {
				result.fullyHeld = false;
				continue;
			}
			if (locks.getLocks().size() != 1 || !locks.containsLogicalLock(requestedLock)) {
				result.conflicting = true;
				co_return result;
			}
			const RangeLockState& current = locks.getLocks().begin()->second;
			if (result.lock.present() && !result.lock.get().hasSameAcquisition(current)) {
				result.conflicting = true;
				co_return result;
			}
			result.lock = current;
		}
		ASSERT(ranges.back().key > beginKey);
		beginKey = ranges.back().key;
	}
	co_return result;
}

Future<Optional<RangeLockState>> prepareExclusiveRangeLockOperation(Transaction* tr,
                                                                    RangeLockState requestedLock,
                                                                    Optional<UID> expectedGeneration = Optional<UID>(),
                                                                    bool adoptLegacy = false) {
	co_await checkRangeLockOwner(tr, requestedLock.getOwnerUniqueId(), expectedGeneration);
	ExclusiveRangeLockScan scan = co_await scanExclusiveRangeLock(tr, requestedLock);
	bool wrongAcquisition =
	    scan.lock.present() && !requestedLock.getLockId().empty() && !scan.lock.get().hasSameAcquisition(requestedLock);
	if (adoptLegacy) {
		if (!scan.fullyHeld || !scan.lock.present() || !scan.lock.get().getLockId().empty()) {
			throw range_lock_reject();
		}
		wrongAcquisition = false;
	}
	if (scan.conflicting || wrongAcquisition) {
		throw range_lock_reject();
	}
	co_return scan.lock;
}

Future<Void> prepareExclusiveRangeUnlockOperation(Transaction* tr, RangeLockState expectedLock) {
	co_await checkRangeLockOwner(tr, expectedLock.getOwnerUniqueId());
	ExclusiveRangeLockScan scan = co_await scanExclusiveRangeLock(tr, expectedLock);
	if (scan.conflicting || (scan.lock.present() && !expectedLock.getLockId().empty() &&
	                         !scan.lock.get().hasSameAcquisition(expectedLock))) {
		throw range_unlock_reject();
	}
}

Future<Void> checkNoActiveBulkLoadFence(Transaction* tr, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	if (ownerUniqueID != rangeLockNameForBulkLoad) {
		co_return;
	}
	tr->addReadConflictRange(bulkLoadJobKeys);
	if ((co_await getSubmittedBulkLoadJob(tr, range)).present()) {
		throw range_unlock_reject();
	}
}

} // namespace

// Transactional. One transaction can call takeExclusiveReadLockOnRange at most for one time.
// This is the limitation of the krmSetRangeCoalescing.
Future<Void> takeExclusiveReadLockOnRange(Transaction* tr, KeyRange range, RangeLockOwnerName ownerUniqueID) {
	validateRangeLockRange(range);
	validateRangeLockOwnerName(ownerUniqueID);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	// Add conflict range
	tr->addWriteConflictRange(range);
	RangeLockState requestedLock(RangeLockType::ExclusiveReadLock, ownerUniqueID, range);
	Optional<RangeLockState> existing = co_await prepareExclusiveRangeLockOperation(tr, requestedLock);
	// Administrative idempotent takes must not strip a live acquisition's fencing token.
	if (existing.present()) {
		requestedLock = existing.get();
	}
	RangeLockStateSet rangeLockStateSet;
	rangeLockStateSet.insertIfNotExist(requestedLock);
	co_await krmSetRange(tr, rangeLockPrefix, range, rangeLockStateSetValue(rangeLockStateSet));
	TraceEvent(SevInfo, "TakeExclusiveReadLockTransactionOnRange").detail("Range", range);
}

Future<Void> takeExclusiveReadLockOnRange(Transaction* tr, RangeLockState requestedLock, UID expectedOwnerGeneration) {
	validateFencedRangeLock(requestedLock);
	if (!expectedOwnerGeneration.isValid()) {
		throw range_lock_failed();
	}
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->addWriteConflictRange(requestedLock.getRange());
	co_await prepareExclusiveRangeLockOperation(tr, requestedLock, expectedOwnerGeneration);
	RangeLockStateSet locks;
	locks.insertIfNotExist(requestedLock);
	co_await krmSetRange(tr, rangeLockPrefix, requestedLock.getRange(), rangeLockStateSetValue(locks));
}

Future<Void> adoptExclusiveReadLockOnRange(Transaction* tr, RangeLockState requestedLock, UID expectedOwnerGeneration) {
	validateFencedRangeLock(requestedLock);
	if (!expectedOwnerGeneration.isValid()) {
		throw range_lock_failed();
	}
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->addWriteConflictRange(requestedLock.getRange());
	co_await prepareExclusiveRangeLockOperation(tr, requestedLock, expectedOwnerGeneration, true);
	RangeLockStateSet locks;
	locks.insertIfNotExist(requestedLock);
	co_await krmSetRange(tr, rangeLockPrefix, requestedLock.getRange(), rangeLockStateSetValue(locks));
}

// Transactional. One transaction can call releaseExclusiveReadLockOnRange at most for one time.
// This is the limitation of the krmSetRangeCoalescing.
Future<Void> releaseExclusiveReadLockOnRange(Transaction* tr,
                                             KeyRange range,
                                             RangeLockOwnerName ownerUniqueID,
                                             bool allowActiveBulkLoad) {
	validateRangeLockRange(range);
	validateRangeLockOwnerName(ownerUniqueID);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	if (!allowActiveBulkLoad) {
		co_await checkNoActiveBulkLoadFence(tr, range, ownerUniqueID);
	}
	co_await prepareExclusiveRangeUnlockOperation(
	    tr, RangeLockState(RangeLockType::ExclusiveReadLock, ownerUniqueID, range));
	co_await krmSetRangeCoalescing(tr, rangeLockPrefix, range, normalKeys, rangeLockStateSetValue(RangeLockStateSet()));
	TraceEvent(SevInfo, "ReleaseExclusiveReadLockTransactionOnRange").detail("Range", range);
}

Future<Void> releaseExclusiveReadLockOnRange(Transaction* tr, RangeLockState expectedLock) {
	validateFencedRangeLock(expectedLock);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	co_await prepareExclusiveRangeUnlockOperation(tr, expectedLock);
	co_await krmSetRangeCoalescing(
	    tr, rangeLockPrefix, expectedLock.getRange(), normalKeys, rangeLockStateSetValue(RangeLockStateSet()));
}

Future<bool> isExclusiveReadLockHeld(Transaction* tr, RangeLockState expectedLock) {
	validateFencedRangeLock(expectedLock);
	tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	ExclusiveRangeLockScan scan = co_await scanExclusiveRangeLock(tr, expectedLock);
	co_return !scan.conflicting && scan.fullyHeld && scan.lock.present() &&
	    scan.lock.get().hasSameAcquisition(expectedLock);
}

Future<Void> releaseExclusiveReadLockByUser(Database cx, RangeLockOwnerName ownerUniqueID, bool allowActiveBulkLoad) {
	validateRangeLockOwnerName(ownerUniqueID);
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
			if (!allowActiveBulkLoad) {
				co_await checkNoActiveBulkLoadFence(&tr, normalKeys, ownerUniqueID);
			}
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
	validateRangeLockRange(range);
	validateRangeLockOwnerName(ownerUniqueID);
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

Future<Void> takeExclusiveReadLockOnRange(Database cx, RangeLockState requestedLock, UID expectedOwnerGeneration) {
	validateFencedRangeLock(requestedLock);
	if (!expectedOwnerGeneration.isValid()) {
		throw range_lock_failed();
	}
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			co_await takeExclusiveReadLockOnRange(&tr, requestedLock, expectedOwnerGeneration);
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Transactional
Future<Void> releaseExclusiveReadLockOnRange(Database cx,
                                             KeyRange range,
                                             RangeLockOwnerName ownerUniqueID,
                                             bool allowActiveBulkLoad) {
	validateRangeLockRange(range);
	validateRangeLockOwnerName(ownerUniqueID);
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			co_await releaseExclusiveReadLockOnRange(&tr, range, ownerUniqueID, allowActiveBulkLoad);
			co_await tr.commit();
			TraceEvent(SevInfo, "ReleaseExclusiveReadLockOnRange").detail("Range", range);
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Void> releaseExclusiveReadLockOnRange(Database cx, RangeLockState expectedLock) {
	validateFencedRangeLock(expectedLock);
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			co_await releaseExclusiveReadLockOnRange(&tr, expectedLock);
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

TEST_CASE("/RangeLock/LogicalIdentity") {
	const auto checkCollision = [](const RangeLockState& first, const RangeLockState& colliding) {
		ASSERT(first != colliding);
		ASSERT(first.getLockUniqueString() == colliding.getLockUniqueString());

		RangeLockStateSet locks;
		locks.insertIfNotExist(first);
		const Value encoded = rangeLockStateSetValue(locks);
		RangeLockStateSet decoded = decodeRangeLockStateSet(encoded);
		ASSERT(decoded == locks);
		ASSERT(decoded.containsLogicalLock(first));
		ASSERT(!decoded.containsLogicalLock(colliding));
		decoded.insertIfNotExist(first);
		ASSERT(rangeLockStateSetValue(decoded) == encoded);

		bool rejected = false;
		try {
			decoded.insertIfNotExist(colliding);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_range_lock_failed);
			rejected = true;
		}
		ASSERT(rejected);
		decoded.remove(colliding);
		ASSERT(decoded == locks);
		decoded.remove(first);
		ASSERT(decoded.empty());
	};

	checkCollision(
	    RangeLockState(RangeLockType::ExclusiveReadLock, "A", KeyRangeRef("XExclusiveReadLock{ begin=a"_sr, "z"_sr)),
	    RangeLockState(RangeLockType::ExclusiveReadLock, "AExclusiveReadLock{ begin=X", KeyRangeRef("a"_sr, "z"_sr)));
	checkCollision(RangeLockState(RangeLockType::ExclusiveReadLock, "owner", KeyRangeRef("a"_sr, "b  end=c"_sr)),
	               RangeLockState(RangeLockType::ExclusiveReadLock, "owner", KeyRangeRef("a  end=b"_sr, "c"_sr)));
	co_return;
}

TEST_CASE("/RangeLock/AcquisitionIdentity") {
	const RangeLockState first(RangeLockType::ExclusiveReadLock, "owner", KeyRangeRef("a"_sr, "z"_sr), "first");
	const RangeLockState replacement(
	    RangeLockType::ExclusiveReadLock, "owner", KeyRangeRef("a"_sr, "z"_sr), "replacement");
	ASSERT(first.hasSameLogicalIdentity(replacement));
	ASSERT(!first.hasSameAcquisition(replacement));
	ASSERT(first != replacement);
	ASSERT(first.getLockUniqueString() == replacement.getLockUniqueString());
	RangeLockStateSet firstSet;
	RangeLockStateSet replacementSet;
	firstSet.insertIfNotExist(first);
	replacementSet.insertIfNotExist(replacement);
	ASSERT(firstSet != replacementSet);
	ASSERT(rangeLockStateSetValue(firstSet) != rangeLockStateSetValue(replacementSet));
	ASSERT(firstSet.containsLogicalLock(replacement));
	ASSERT(!firstSet.containsExactLock(replacement));
	firstSet.remove(replacement);
	ASSERT(firstSet.containsExactLock(first));
	ASSERT(decodeRangeLockState(rangeLockStateValue(first)).hasSameAcquisition(first));
	co_return;
}

TEST_CASE("/RangeLock/InvalidRanges") {
	const auto checkRejected = [](const auto& result) {
		ASSERT(result.isReady());
		ASSERT(result.isError());
		ASSERT_EQ(result.getError().code(), error_code_range_lock_failed);
	};
	const std::vector<KeyRange> invalidRanges = {
		KeyRangeRef(normalKeys.begin, normalKeys.begin), KeyRangeRef("a"_sr, "a"_sr),
		KeyRangeRef(normalKeys.end, normalKeys.end),     KeyRangeRef(normalKeys.begin, allKeys.end),
		KeyRangeRef(normalKeys.end, allKeys.end),
	};
	for (const auto& range : invalidRanges) {
		// Invalid input must fail before either overload accesses a transaction or database.
		checkRejected(takeExclusiveReadLockOnRange(static_cast<Transaction*>(nullptr), range, "owner"));
		checkRejected(releaseExclusiveReadLockOnRange(static_cast<Transaction*>(nullptr), range, "owner"));
		checkRejected(takeExclusiveReadLockOnRange(Database(), range, "owner"));
		checkRejected(releaseExclusiveReadLockOnRange(Database(), range, "owner"));
		checkRejected(findExclusiveReadLockOnRange(Database(), range));
		const RangeLockState fenced(RangeLockType::ExclusiveReadLock, "owner", range, "acquisition");
		checkRejected(takeExclusiveReadLockOnRange(static_cast<Transaction*>(nullptr), fenced, UID(1, 2)));
		checkRejected(takeExclusiveReadLockOnRange(Database(), fenced, UID(1, 2)));
		checkRejected(releaseExclusiveReadLockOnRange(static_cast<Transaction*>(nullptr), fenced));
		checkRejected(releaseExclusiveReadLockOnRange(Database(), fenced));
		checkRejected(isExclusiveReadLockHeld(static_cast<Transaction*>(nullptr), fenced));
	}
	const RangeLockState fenced(RangeLockType::ExclusiveReadLock, "owner", normalKeys, "acquisition");
	checkRejected(takeExclusiveReadLockOnRange(static_cast<Transaction*>(nullptr), fenced, UID()));
	checkRejected(takeExclusiveReadLockOnRange(Database(), fenced, UID()));
	checkRejected(removeRangeLockOwner(Database(), RangeLockOwner()));
	checkRejected(takeExclusiveReadLockOnRange(static_cast<Transaction*>(nullptr), normalKeys, ""));
	checkRejected(releaseExclusiveReadLockOnRange(static_cast<Transaction*>(nullptr), normalKeys, ""));
	checkRejected(takeExclusiveReadLockOnRange(Database(), normalKeys, ""));
	checkRejected(releaseExclusiveReadLockOnRange(Database(), normalKeys, ""));
	checkRejected(releaseExclusiveReadLockByUser(Database(), ""));
	validateRangeLockRange(normalKeys);
	validateRangeLockRange(KeyRangeRef("a"_sr, normalKeys.end));
	co_return;
}
