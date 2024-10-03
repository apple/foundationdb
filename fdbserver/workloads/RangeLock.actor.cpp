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

#include "fdbclient/RangeLock.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>

struct RangeLocking : TestWorkload {
	static constexpr auto NAME = "RangeLocking";
	const bool enabled;
	bool pass;

	struct KVOperation {
		Optional<KeyRange> range;
		Optional<KeyValue> keyValue;

		KVOperation(KeyRange range) : range(range) {}
		KVOperation(KeyValue keyValue) : keyValue(keyValue) {}

		std::string toString() const {
			std::string res = "KVOperation: ";
			if (range.present()) {
				res = res + "[ClearRange]: " + range.get().toString();
			} else {
				res = res + "[SetKeyValue]: key: " + keyValue.get().key.toString() +
				      ", value: " + keyValue.get().value.toString();
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

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> setKey(Database cx, Key key, Value value) {
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.set(key, value);
				wait(tr.commit());
				TraceEvent("RangeLockWorkLoadSetKey").detail("Key", key).detail("Value", value);
				return Void();
			} catch (Error& e) {
				TraceEvent("RangeLockWorkLoadSetKeyError")
				    .errorUnsuppressed(e)
				    .detail("Key", key)
				    .detail("Value", value);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> clearKey(Database cx, Key key) {
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.clear(key);
				wait(tr.commit());
				TraceEvent("RangeLockWorkLoadClearKey").detail("Key", key);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> clearRange(Database cx, KeyRange range) {
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.clear(range);
				wait(tr.commit());
				TraceEvent("RangeLockWorkLoadClearRange").detail("Range", range);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Optional<Value>> getKey(Database cx, Key key) {
		loop {
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = wait(tr.get(key));
				TraceEvent("RangeLockWorkLoadGetKey")
				    .detail("Key", key)
				    .detail("Value", value.present() ? value.get() : Value());
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

		wait(lockCommitUserRange(cx, rangeLock));
		TraceEvent("RangeLockWorkLoadLockRange").detail("Range", rangeLock);

		wait(store(lockedRanges, getCommitLockedUserRanges(cx, normalKeys)));
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

		wait(unlockCommitUserRange(cx, rangeLock));
		TraceEvent("RangeLockWorkLoadUnlockRange").detail("Range", rangeLock);

		lockedRanges.clear();
		wait(store(lockedRanges, getCommitLockedUserRanges(cx, normalKeys)));
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
		loop {
			self->kvOperations.clear();
			state Transaction tr(cx);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				int iterationCount = deterministicRandom()->randomInt(1, 10);
				for (int i = 0; i < iterationCount; i++) {
					if (deterministicRandom()->coinflip()) {
						KeyValue kv = self->getRandomKeyValue();
						tr.set(kv.key, kv.value);
						self->kvOperations.push_back(KVOperation(kv));
					} else {
						KeyRange range = self->getRandomRange();
						tr.clear(range);
						self->kvOperations.push_back(KVOperation(range));
					}
				}
				wait(tr.commit());
				TraceEvent("RangeLockWorkLoadRandomKVS");
				break;
			} catch (Error& e) {
				TraceEvent("RangeLockWorkLoadRandomKVSError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> updateLockMapWithRandomOperation(RangeLocking* self, Database cx) {
		loop {
			self->lockRangeOperations.clear();
			try {
				state int i = 0;
				state int iterationCount = deterministicRandom()->randomInt(1, 10);
				for (; i < iterationCount; i++) {
					state KeyRange range = self->getRandomRange();
					state bool lock = deterministicRandom()->coinflip();
					if (lock) {
						wait(lockCommitUserRange(cx, range));
					} else {
						wait(unlockCommitUserRange(cx, range));
					}
					self->lockRangeOperations.push_back(LockRangeOperation(range, lock));
				}
				TraceEvent("RangeLockWorkLoadRandomLockRange").detail("Ranges", describe(self->lockRangeOperations));
				break;
			} catch (Error& e) {
				TraceEvent("RangeLockWorkLoadRandomLockRangeError")
				    .errorUnsuppressed(e)
				    .detail("Ranges", describe(self->lockRangeOperations));
			}
		}
		return Void();
	}

	void updateInMemoryKVSStatus(RangeLocking* self) {
		for (const auto& operation : self->kvOperations) {
			if (operation.keyValue.present()) {
				Key key = operation.keyValue.get().key;
				Value value = operation.keyValue.get().value;
				self->kvs[key] = value;
			} else {
				KeyRange clearRange = operation.range.get();
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

	ACTOR Future<Void> checkCorrectness(RangeLocking* self, Database cx) { return Void(); }

	ACTOR Future<Void> complexTest(RangeLocking* self, Database cx) {
		state int iterationCount = 1000;
		state int iteration = 0;
		loop {
			if (iteration > iterationCount) {
				break;
			}
			if (deterministicRandom()->coinflip()) {
				wait(self->updateLockMapWithRandomOperation(self, cx));
				self->updateInMemoryLockStatus(self);
			}
			if (deterministicRandom()->coinflip()) {
				try {
					wait(self->updateDBWithRandomOperations(self, cx));
					self->updateInMemoryKVSStatus(self);
				} catch (Error& e) {
					ASSERT(e.code() == error_code_transaction_rejected_range_locked);
					self->kvOperations.clear();
				}
			}
			wait(self->checkCorrectness(self, cx));
			iteration++;
		}
		wait(unlockCommitUserRange(cx, normalKeys));
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
