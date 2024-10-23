/*
 * RangeLock.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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

		KVOperation(KeyRange range) : params(range) {}
		KVOperation(KeyValue keyValue) : params(keyValue) {}

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

	RangeLocking(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {
		lockedRangeMap.insert(allKeys, false);
	}

	Future<Void> setup(Database const& cx) override {
		return registerRangeLockOwner(cx, rangeLockOwnerName, rangeLockOwnerName);
	}

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> setKey(Database cx, Key key, Value value) {
		state Transaction tr(cx);
		loop {
			try {
				tr.set(key, value);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> clearKey(Database cx, Key key) {
		state Transaction tr(cx);
		loop {
			try {
				tr.clear(key);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> clearRange(Database cx, KeyRange range) {
		state Transaction tr(cx);
		loop {
			try {
				tr.clear(range);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Optional<Value>> getKey(Database cx, Key key) {
		state Transaction tr(cx);
		loop {
			try {
				Optional<Value> value = wait(tr.get(key));
				return value;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> simpleTest(RangeLocking* self, Database cx) {
		state Key keyUpdate = "11"_sr;
		state KeyRange keyToClear = KeyRangeRef("1"_sr, "3"_sr);
		state KeyRange rangeLock = KeyRangeRef("1"_sr, "2"_sr);
		state Optional<Value> value;
		state std::vector<KeyRange> lockedRanges;

		wait(self->setKey(cx, keyUpdate, "1"_sr));

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(value.present() && value.get() == "1"_sr);

		wait(self->clearKey(cx, keyUpdate));

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(!value.present());

		wait(takeReadLockOnRange(cx, rangeLock, self->rangeLockOwnerName));
		TraceEvent("RangeLockWorkLoadLockRange").detail("Range", rangeLock);

		wait(store(lockedRanges, getReadLockOnRange(cx, normalKeys)));
		TraceEvent("RangeLockWorkLoadGetLockedRange")
		    .detail("Range", rangeLock)
		    .detail("LockState", describe(lockedRanges));

		try {
			wait(self->setKey(cx, keyUpdate, "2"_sr));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_transaction_rejected_range_locked);
		}

		try {
			wait(self->clearRange(cx, keyToClear));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_transaction_rejected_range_locked);
		}

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(!value.present());

		wait(releaseReadLockOnRange(cx, rangeLock, self->rangeLockOwnerName));
		TraceEvent("RangeLockWorkLoadUnlockRange").detail("Range", rangeLock);

		lockedRanges.clear();
		wait(store(lockedRanges, getReadLockOnRange(cx, normalKeys)));
		TraceEvent("RangeLockWorkLoadGetLockedRange")
		    .detail("Range", rangeLock)
		    .detail("LockState", describe(lockedRanges));

		wait(self->setKey(cx, keyUpdate, "3"_sr));

		wait(store(value, self->getKey(cx, keyUpdate)));
		ASSERT(value.present() && value.get() == "3"_sr);

		return Void();
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

	ACTOR Future<Void> updateDBWithRandomOperations(RangeLocking* self, Database cx) {
		self->kvOperations.clear();
		state int iterationCount = deterministicRandom()->randomInt(1, 10);
		state int i = 0;
		for (; i < iterationCount; i++) {
			state bool acceptedByDB = true;
			if (deterministicRandom()->coinflip()) {
				state KeyValue kv = self->getRandomKeyValue();
				try {
					wait(self->setKey(cx, kv.key, kv.value));
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
				state KeyRange range = self->getRandomRange();
				try {
					wait(self->clearRange(cx, range));
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
		return Void();
	}

	ACTOR Future<Void> updateLockMapWithRandomOperation(RangeLocking* self, Database cx) {
		self->lockRangeOperations.clear();
		state int i = 0;
		state int iterationCount = deterministicRandom()->randomInt(1, 10);
		for (; i < iterationCount; i++) {
			state KeyRange range = self->getRandomRange();
			state bool lock = deterministicRandom()->coinflip();
			if (lock) {
				wait(takeReadLockOnRange(cx, range, self->rangeLockOwnerName));
				if (self->verboseLogging) {
					TraceEvent("RangeLockWorkLoadHistory").detail("Ops", "Lock").detail("Range", range);
				}
			} else {
				wait(releaseReadLockOnRange(cx, range, self->rangeLockOwnerName));
				if (self->verboseLogging) {
					TraceEvent("RangeLockWorkLoadHistory").detail("Ops", "Unlock").detail("Range", range);
				}
			}
			self->lockRangeOperations.push_back(LockRangeOperation(range, lock));
		}
		return Void();
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

	ACTOR Future<std::map<Key, Value>> getKVSFromDB(RangeLocking* self, Database cx) {
		state std::map<Key, Value> kvsFromDB;
		state Key beginKey = normalKeys.begin;
		state Key endKey = normalKeys.end;
		loop {
			state Transaction tr(cx);
			KeyRange rangeToRead = KeyRangeRef(beginKey, endKey);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult res = wait(tr.getRange(rangeToRead, GetRangeLimits()));
				for (int i = 0; i < res.size(); i++) {
					kvsFromDB[res[i].key] = res[i].value;
				}
				if (res.size() > 0) {
					beginKey = keyAfter(res.end()[-1].key);
				} else {
					break;
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return kvsFromDB;
	}

	ACTOR Future<std::vector<KeyRange>> getLockedRangesFromDB(Database cx) {
		state std::vector<KeyRange> res;
		wait(store(res, getReadLockOnRange(cx, normalKeys)));
		return coalesceRangeList(res);
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

	ACTOR Future<Void> checkKVCorrectness(RangeLocking* self, Database cx) {
		state std::map<Key, Value> currentKvsInDB;
		wait(store(currentKvsInDB, self->getKVSFromDB(self, cx)));
		for (const auto& [key, value] : currentKvsInDB) {
			if (self->kvs.find(key) == self->kvs.end()) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckDBUniqueKey")
				    .detail("Key", key)
				    .detail("Value", value);
				self->shouldExit = true;
				return Void();
			} else if (self->kvs[key] != value) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckMismatchValue")
				    .detail("Key", key)
				    .detail("MemValue", self->kvs[key])
				    .detail("DBValue", value);
				self->shouldExit = true;
				return Void();
			}
		}
		for (const auto& [key, value] : self->kvs) {
			if (currentKvsInDB.find(key) == currentKvsInDB.end()) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckMemoryUniqueKey")
				    .detail("Key", key)
				    .detail("Value", value);
				self->shouldExit = true;
				return Void();
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

		return Void();
	}

	ACTOR Future<Void> checkLockCorrectness(RangeLocking* self, Database cx) {
		state std::vector<KeyRange> currentLockRangesInDB;
		wait(store(currentLockRangesInDB, self->getLockedRangesFromDB(cx)));
		std::vector<KeyRange> currentLockRangesInMemory = self->getLockedRangesFromMemory(self);
		for (int i = 0; i < currentLockRangesInDB.size(); i++) {
			if (i >= currentLockRangesInMemory.size()) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckDBUniqueLockedRange")
				    .detail("Range", currentLockRangesInDB[i]);
				self->shouldExit = true;
				return Void();
			}
			if (currentLockRangesInDB[i] != currentLockRangesInMemory[i]) {
				TraceEvent(SevError, "RangeLockWorkLoadHistory")
				    .detail("Ops", "CheckMismatchLockedRange")
				    .detail("RangeMemory", currentLockRangesInMemory[i])
				    .detail("RangeDB", currentLockRangesInDB[i]);
				self->shouldExit = true;
				return Void();
			}
		}
		for (int i = currentLockRangesInDB.size(); i < currentLockRangesInMemory.size(); i++) {
			TraceEvent(SevError, "RangeLockWorkLoadHistory")
			    .detail("Ops", "CheckMemoryUniqueLockedRange")
			    .detail("Key", currentLockRangesInMemory[i]);
			self->shouldExit = true;
			return Void();
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

		return Void();
	}

	ACTOR Future<Void> complexTest(RangeLocking* self, Database cx) {
		state int iterationCount = 100;
		state int iteration = 0;
		state std::string rangeLockOwnerName = "RangeLockingSimpleTest";
		wait(registerRangeLockOwner(cx, rangeLockOwnerName, rangeLockOwnerName));
		loop {
			if (iteration > iterationCount || self->shouldExit) {
				break;
			}
			if (deterministicRandom()->coinflip()) {
				wait(self->updateLockMapWithRandomOperation(self, cx));
				self->updateInMemoryLockStatus(self);
			}
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Iteration", iteration)
			    .detail("IterationCount", iterationCount)
			    .detail("Phase", "UpdateLock");
			wait(self->checkLockCorrectness(self, cx));
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Iteration", iteration)
			    .detail("IterationCount", iterationCount)
			    .detail("Phase", "CheckLockCorrectness");
			if (deterministicRandom()->coinflip()) {
				try {
					wait(self->updateDBWithRandomOperations(self, cx));
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
			wait(self->checkKVCorrectness(self, cx));
			TraceEvent("RangeLockWorkloadProgress")
			    .detail("Iteration", iteration)
			    .detail("IterationCount", iterationCount)
			    .detail("Phase", "CheckDBCorrectness");
			iteration++;
		}
		wait(releaseReadLockOnRange(cx, normalKeys, self->rangeLockOwnerName));
		TraceEvent("RangeLockWorkloadProgress").detail("Phase", "End");
		return Void();
	}

	ACTOR Future<Void> _start(RangeLocking* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}
		// wait(self->simpleTest(self, cx));
		wait(self->complexTest(self, cx));
		return Void();
	}
};

WorkloadFactory<RangeLocking> RangeLockingFactory;
