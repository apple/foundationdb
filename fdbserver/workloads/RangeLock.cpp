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

#include "fdbclient/AuditUtils.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"

struct RangeLocking : TestWorkload {
	static constexpr auto NAME = "RangeLocking";
	const bool enabled;
	bool pass;
	bool shouldExit = false;
	bool verboseLogging = false; // enable to log range lock and commit history
	std::string rangeLockOwnerName = "RangeLockingTest";

	// This workload is not compatible with RandomRangeLock workload because they will race in locked range
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomRangeLock" });
	}

	struct KVOperation {
		std::variant<KeyRange, KeyValue> params;

		explicit KVOperation(KeyRange range) : params(range) {}
		explicit KVOperation(KeyValue keyValue) : params(keyValue) {}

		std::string toString() const {
			std::string res = "KVOperation: ";
			if (std::holds_alternative<KeyRange>(params)) {
				res = res + "[ClearRange]: " + std::get<KeyRange>(params).toString();
			} else {
				res = res + "[SetKeyValue]: key: " + std::get<KeyValue>(params).key.toString() +
				      ", value: " + std::get<KeyValue>(params).value.toString();
			}
			return res;
		}
	};

	struct LockRangeOperation {
		KeyRange range;
		bool lock;
		LockRangeOperation(KeyRange range, bool lock) : range(range), lock(lock) {}

		std::string toString() const {
			std::string res = "LockRangeOperation: ";
			if (lock) {
				res = res + "[LockRange]: " + range.toString();
			} else {
				res = res + "[UnlockRange]: " + range.toString();
			}
			return res;
		}
	};

	KeyRangeMap<bool> lockedRangeMap;
	std::vector<LockRangeOperation> lockRangeOperations;
	std::vector<KVOperation> kvOperations;
	std::map<Key, Value> kvs;

	explicit RangeLocking(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {
		lockedRangeMap.insert(allKeys, false);
	}

	Future<Void> setup(Database const& cx) override {
		return registerRangeLockOwner(cx, rangeLockOwnerName, rangeLockOwnerName);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> setKey(Database cx, Key key, Value value) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.set(key, value);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> clearKey(Database cx, Key key) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.clear(key);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> clearRange(Database cx, KeyRange range) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.clear(range);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Optional<Value>> getKey(Database cx, Key key) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				Optional<Value> value = co_await tr.get(key);
				co_return value;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	std::string getLockRangesString(const std::vector<std::pair<KeyRange, RangeLockState>>& locks) {
		std::string res = "";
		int count = 0;
		for (const auto& [lockedRange, _lockState] : locks) {
			res = res + lockedRange.toString();
			if (count < locks.size()) {
				res = res + ", ";
			}
			count = count + 1;
		}
		return res;
	}

	KeyValue getRandomKeyValue() const {
		Key key = StringRef(std::to_string(deterministicRandom()->randomInt(0, 10)));
		Value value = key;
		return Standalone(KeyValueRef(key, value));
	}

	KeyRange getRandomRange() const {
		int startPoint = deterministicRandom()->randomInt(0, 9);
		Key beginKey = StringRef(std::to_string(startPoint));
		Key endKey = StringRef(std::to_string(deterministicRandom()->randomInt(startPoint + 1, 10)));
		return Standalone(KeyRangeRef(beginKey, endKey));
	}

	Future<Void> updateDBWithRandomOperations(RangeLocking* self, Database cx) {
		self->kvOperations.clear();
		int iterationCount = deterministicRandom()->randomInt(1, 10);
		for (int i = 0; i < iterationCount; ++i) {
			bool acceptedByDB = true;
			if (deterministicRandom()->coinflip()) {
				KeyValue kv = self->getRandomKeyValue();
				try {
					co_await self->setKey(cx, kv.key, kv.value);
				} catch (Error& e) {
					if (e.code() != error_code_transaction_rejected_range_locked) {
						throw e;
					}
					acceptedByDB = false;
				}
				self->kvOperations.push_back(KVOperation(kv));
				if (self->verboseLogging) {
					TraceEvent("RangeLockWorkLoadHistory")
					    .detail("Ops", "SetKey")
					    .detail("Key", kv.key)
					    .detail("Value", kv.value)
					    .detail("Accepted", acceptedByDB);
				}
			} else {
				KeyRange range = self->getRandomRange();
				try {
					co_await self->clearRange(cx, range);
				} catch (Error& e) {
					if (e.code() != error_code_transaction_rejected_range_locked) {
						throw e;
					}
					acceptedByDB = false;
				}
				self->kvOperations.push_back(KVOperation(range));
				if (self->verboseLogging) {
					TraceEvent("RangeLockWorkLoadHistory")
					    .detail("Ops", "ClearRange")
					    .detail("Range", range)
					    .detail("Accepted", acceptedByDB);
				}
			}
		}
	}

	Future<Void> updateLockMapWithRandomOperation(RangeLocking* self, Database cx) {
		self->lockRangeOperations.clear();
		int iterationCount = deterministicRandom()->randomInt(1, 10);
		for (int i = 0; i < iterationCount; ++i) {
			KeyRange range = self->getRandomRange();
			bool lock = deterministicRandom()->coinflip();
			if (lock) {
				try {
					co_await takeExclusiveReadLockOnRange(cx, range, self->rangeLockOwnerName);
					if (self->verboseLogging) {
						TraceEvent("RangeLockWorkLoadHistory").detail("Ops", "Lock").detail("Range", range);
					}
				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw e;
					}
					TraceEvent("RangeLockWorkLoadHistory")
					    .errorUnsuppressed(e)
					    .detail("Ops", "LockFailed")
					    .detail("Range", range);
					ASSERT(e.code() == error_code_range_lock_reject);
					continue; // Do not add the operation to lockRangeOperations.
				}
			} else {
				try {
					co_await releaseExclusiveReadLockOnRange(cx, range, self->rangeLockOwnerName);
					if (self->verboseLogging) {
						TraceEvent("RangeLockWorkLoadHistory").detail("Ops", "Unlock").detail("Range", range);
					}
				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw e;
					}
					TraceEvent("RangeLockWorkLoadHistory")
					    .errorUnsuppressed(e)
					    .detail("Ops", "UnlockFailed")
					    .detail("Range", range);
					ASSERT(e.code() == error_code_range_unlock_reject);
					continue; // Do not add the operation to lockRangeOperations.
				}
			}
			self->lockRangeOperations.push_back(LockRangeOperation(range, lock));
		}
	}

	bool operationRejectByLocking(RangeLocking* self, const KVOperation& kvOperation) {
		KeyRange rangeToCheck;
		if (std::holds_alternative<KeyRange>(kvOperation.params)) {
			rangeToCheck = std::get<KeyRange>(kvOperation.params);
		} else {
			rangeToCheck = singleKeyRange(std::get<KeyValue>(kvOperation.params).key);
		}
		for (auto lockRange : self->lockedRangeMap.intersectingRanges(rangeToCheck)) {
			if (lockRange.value() == true) {
				return true;
			}
		}
		return false;
	}

	void updateInMemoryKVSStatus(RangeLocking* self) {
		for (const auto& operation : self->kvOperations) {
			if (self->operationRejectByLocking(self, operation)) {
				continue;
			}
			if (std::holds_alternative<KeyValue>(operation.params)) {
				Key key = std::get<KeyValue>(operation.params).key;
				Value value = std::get<KeyValue>(operation.params).value;
				self->kvs[key] = value;
			} else {
				KeyRange clearRange = std::get<KeyRange>(operation.params);
				std::vector<Key> keysToClear;
				for (const auto& [key, value] : self->kvs) {
					if (clearRange.contains(key)) {
						keysToClear.push_back(key);
					}
				}
				for (const auto& key : keysToClear) {
					self->kvs.erase(key);
				}
			}
		}
		return;
	}

	void updateInMemoryLockStatus(RangeLocking* self) {
		for (const auto& operation : self->lockRangeOperations) {
			self->lockedRangeMap.insert(operation.range, operation.lock);
		}
		return;
	}

	Future<std::map<Key, Value>> getKVSFromDB(RangeLocking* self, Database cx) {
		std::map<Key, Value> kvsFromDB;
		Key beginKey = normalKeys.begin;
		Key endKey = normalKeys.end;
		while (true) {
			Transaction tr(cx);
			KeyRange rangeToRead = KeyRangeRef(beginKey, endKey);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult res = co_await tr.getRange(rangeToRead, GetRangeLimits());
				for (int i = 0; i < res.size(); i++) {
					kvsFromDB[res[i].key] = res[i].value;
				}
				if (!res.empty()) {
					beginKey = keyAfter(res.end()[-1].key);
				} else {
					break;
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await tr.onError(err);
			}
		}
		co_return kvsFromDB;
	}

	Future<std::vector<KeyRange>> getLockedRangesFromDB(Database cx) {
		std::vector<std::pair<KeyRange, RangeLockState>> res;
		res = co_await findExclusiveReadLockOnRange(cx, normalKeys);
		std::vector<KeyRange> ranges;
		for (const auto& [lockedRange, _lockState] : res) {
			ranges.push_back(lockedRange);
		}
		co_return coalesceRangeList(ranges);
	}

	std::vector<KeyRange> getLockedRangesFromMemory(RangeLocking* self) {
		std::vector<KeyRange> res;
		for (auto range : self->lockedRangeMap.ranges()) {
			if (range.value() == true) {
				res.push_back(range.range());
			}
		}
		return coalesceRangeList(res);
	}

	Future<Void> checkKVCorrectness(RangeLocking* self, Database cx) {
		std::map<Key, Value> currentKvsInDB;
		currentKvsInDB = co_await self->getKVSFromDB(self, cx);
		for (const auto& [key, value] : currentKvsInDB) {
			if (self->kvs.find(key) == self->kvs.end()) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckDBUniqueKey")
				    .detail("Key", key)
				    .detail("Value", value);
				self->shouldExit = true;
				co_return;
			} else if (self->kvs[key] != value) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckMismatchValue")
				    .detail("Key", key)
				    .detail("MemValue", self->kvs[key])
				    .detail("DBValue", value);
				self->shouldExit = true;
				co_return;
			}
		}
		for (const auto& [key, value] : self->kvs) {
			if (currentKvsInDB.find(key) == currentKvsInDB.end()) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckMemoryUniqueKey")
				    .detail("Key", key)
				    .detail("Value", value);
				self->shouldExit = true;
				co_return;
			}
		}
		if (self->verboseLogging) {
			TraceEvent e("RangeLockWorkLoadHistory");
			e.setMaxEventLength(-1);
			e.setMaxFieldLength(-1);
			e.detail("Ops", "CheckAllKVCorrect");
			int i = 0;
			for (const auto& [key, value] : currentKvsInDB) {
				e.detail("Key" + std::to_string(i), key);
				e.detail("Value" + std::to_string(i), value);
				i++;
			}
		}
	}

	Future<Void> checkLockCorrectness(RangeLocking* self, Database cx) {
		std::vector<KeyRange> currentLockRangesInDB;
		currentLockRangesInDB = co_await self->getLockedRangesFromDB(cx);
		std::vector<KeyRange> currentLockRangesInMemory = self->getLockedRangesFromMemory(self);
		for (int i = 0; i < currentLockRangesInDB.size(); i++) {
			if (i >= currentLockRangesInMemory.size()) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckDBUniqueLockedRange")
				    .detail("Range", currentLockRangesInDB[i]);
				self->shouldExit = true;
				co_return;
			}
			if (currentLockRangesInDB[i] != currentLockRangesInMemory[i]) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckMismatchLockedRange")
				    .detail("RangeMemory", currentLockRangesInMemory[i])
				    .detail("RangeDB", currentLockRangesInDB[i]);
				self->shouldExit = true;
				co_return;
			}
		}
		for (int i = currentLockRangesInDB.size(); i < currentLockRangesInMemory.size(); i++) {
			TraceEvent(SevError, "RangeLockWorkLoadHistory")
			    .detail("Ops", "CheckMemoryUniqueLockedRange")
			    .detail("Key", currentLockRangesInMemory[i]);
			self->shouldExit = true;
			co_return;
		}
		if (self->verboseLogging) {
			TraceEvent e("RangeLockWorkLoadHistory");
			e.setMaxEventLength(-1);
			e.setMaxFieldLength(-1);
			e.detail("Ops", "CheckAllLockCorrect");
			int i = 0;
			for (const auto& range : currentLockRangesInDB) {
				e.detail("Range" + std::to_string(i), range);
				i++;
			}
		}
	}

	Future<Void> complexTest(RangeLocking* self, Database cx) {
		int iterationCount = 100;
		int iteration = 0;
		while (true) {
			if (iteration > iterationCount || self->shouldExit) {
				break;
			}
			if (deterministicRandom()->coinflip()) {
				co_await self->updateLockMapWithRandomOperation(self, cx);
				self->updateInMemoryLockStatus(self);
			}
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Iteration", iteration)
			    .detail("IterationCount", iterationCount)
			    .detail("Phase", "UpdateLock");
			co_await self->checkLockCorrectness(self, cx);
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Iteration", iteration)
			    .detail("IterationCount", iterationCount)
			    .detail("Phase", "CheckLockCorrectness");
			if (deterministicRandom()->coinflip()) {
				try {
					co_await self->updateDBWithRandomOperations(self, cx);
					self->updateInMemoryKVSStatus(self);
				} catch (Error& e) {
					ASSERT(e.code() == error_code_transaction_rejected_range_locked);
					self->kvOperations.clear();
				}
			}
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Iteration", iteration)
			    .detail("IterationCount", iterationCount)
			    .detail("Phase", "UpdateDB");
			co_await self->checkKVCorrectness(self, cx);
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Iteration", iteration)
			    .detail("IterationCount", iterationCount)
			    .detail("Phase", "CheckDBCorrectness");
			iteration++;
		}
		std::vector<std::pair<KeyRange, RangeLockState>> locks2 = co_await findExclusiveReadLockOnRange(cx, normalKeys);
		for (const auto& [lockedRange, lockState] : locks2) {
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Phase", "BeforeLockRelease")
			    .detail("Range", lockedRange)
			    .detail("State", lockState.toString());
		}
		co_await releaseExclusiveReadLockByUser(cx, self->rangeLockOwnerName);
		std::vector<std::pair<KeyRange, RangeLockState>> locks = co_await findExclusiveReadLockOnRange(cx, normalKeys);
		for (const auto& [lockedRange, lockState] : locks) {
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Phase", "AfterLockRelease")
			    .detail("Range", lockedRange)
			    .detail("State", lockState.toString());
		}
		ASSERT(locks.empty());

		TraceEvent("RangeLockWorkloadProgress").detail("Phase", "End");
	}

	bool sameRangeList(const std::vector<KeyRange>& rangesA,
	                   const std::vector<KeyRange>& rangesB,
	                   const RangeLockOwnerName& owner) {
		if (rangesA.size() != rangesB.size()) {
			TraceEvent(SevError, "RangeLockWorkloadTestUnlockRangeByUserMismatch")
			    .detail("RangesA", describe(rangesA))
			    .detail("RangesB", describe(rangesB))
			    .detail("Owner", owner);
			return false;
		}
		for (const auto& rangeA : rangesA) {
			if (std::find(rangesB.begin(), rangesB.end(), rangeA) == rangesB.end()) {
				TraceEvent(SevError, "RangeLockWorkloadTestUnlockRangeByUserMismatch")
				    .detail("RangesA", describe(rangesA))
				    .detail("RangesB", describe(rangesB))
				    .detail("Owner", owner);
				return false;
			}
		}
		for (const auto& rangeB : rangesB) {
			if (std::find(rangesA.begin(), rangesA.end(), rangeB) == rangesA.end()) {
				TraceEvent(SevError, "RangeLockWorkloadTestUnlockRangeByUserMismatch")
				    .detail("RangesA", describe(rangesA))
				    .detail("RangesB", describe(rangesB))
				    .detail("Owner", owner);
				return false;
			}
		}
		return true;
	}

	Future<Void> testUnlockByUser(RangeLocking* self, Database cx) {
		int i = 0;
		int j = 0;
		std::unordered_map<RangeLockOwnerName, std::vector<KeyRange>> rangeLocks;
		RangeLockOwnerName rangeLockOwnerName;
		std::vector<RangeLockOwnerName> candidates;
		KeyRange rangeToLock;
		std::vector<KeyRange> lockedRanges;
		std::vector<RangeLockOwnerName> usersToUnlock; // can contain duplicated users
		std::vector<std::pair<KeyRange, RangeLockState>> locksPerUser;
		for (; i < 100; i++) {
			rangeLockOwnerName = "TestUnlockByUser" + std::to_string(i);
			co_await registerRangeLockOwner(cx, rangeLockOwnerName, rangeLockOwnerName);
			lockedRanges.clear();
			for (; j < 2; j++) {
				try {
					rangeToLock = self->getRandomRange();
					co_await takeExclusiveReadLockOnRange(cx, rangeToLock, rangeLockOwnerName);
					lockedRanges.push_back(rangeToLock);
					TraceEvent("RangeLockWorkloadTestUnlockRangeByUser")
					    .detail("Ops", "LockRange")
					    .detail("Range", rangeToLock)
					    .detail("User", rangeLockOwnerName);
				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw e;
					}
					ASSERT(e.code() == error_code_range_lock_reject);
				}
			}
			auto res = rangeLocks.insert({ rangeLockOwnerName, lockedRanges });
			ASSERT(res.second);
			candidates.push_back(rangeLockOwnerName);
		}
		for (i = 0; i < 10; i++) {
			usersToUnlock.push_back(deterministicRandom()->randomChoice(candidates));
		}
		for (i = 0; i < usersToUnlock.size(); i++) {
			co_await releaseExclusiveReadLockByUser(cx, usersToUnlock[i]);
			TraceEvent("RangeLockWorkloadTestUnlockRangeByUser")
			    .detail("Ops", "Unlock by user")
			    .detail("User", usersToUnlock[i]);
		}
		for (i = 0; i < candidates.size(); i++) {
			locksPerUser.clear();
			locksPerUser = co_await findExclusiveReadLockOnRange(cx, normalKeys, candidates[i]);
			if (std::find(usersToUnlock.begin(), usersToUnlock.end(), candidates[i]) != usersToUnlock.end()) {
				TraceEvent("RangeLockWorkloadTestUnlockRangeByUser")
				    .detail("Ops", "Find unlocked user")
				    .detail("User", candidates[i])
				    .detail("LockCount", locksPerUser.size());
				ASSERT(locksPerUser.empty());
			} else {
				std::vector<KeyRange> lockedRangeFromMetadata;
				for (const auto& [lockedRange, lockState] : locksPerUser) {
					ASSERT(lockedRange == lockState.getRange());
					lockedRangeFromMetadata.push_back(lockedRange);
				}
				TraceEvent("RangeLockWorkloadTestUnlockRangeByUser")
				    .detail("Ops", "Find locked user")
				    .detail("User", candidates[i])
				    .detail("LockCount", locksPerUser.size());
				ASSERT(self->sameRangeList(coalesceRangeList(lockedRangeFromMetadata),
				                           coalesceRangeList(rangeLocks[candidates[i]]),
				                           candidates[i]));
			}
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}
		co_await complexTest(this, cx);
		co_await testUnlockByUser(this, cx);
	}
};

WorkloadFactory<RangeLocking> RangeLockingFactory;
