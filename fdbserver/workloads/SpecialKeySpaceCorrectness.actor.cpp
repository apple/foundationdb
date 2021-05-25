/*
 * SpecialKeySpaceCorrectness.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

class SKSCTestImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit SKSCTestImpl(KeyRangeRef kr) : SpecialKeyRangeBaseImpl(kr) {}
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const {
		ASSERT(range.contains(kr));
		auto resultFuture = ryw->getRange(kr, CLIENT_KNOBS->TOO_MANY);
		// all keys are written to RYW, since GRV is set, the read should happen locally
		ASSERT(resultFuture.isReady());
		auto result = resultFuture.getValue();
		ASSERT(!result.more);
		// To make the test more complext, instead of simply returning the k-v pairs, we reverse all the value strings
		auto kvs = resultFuture.getValue();
		for (int i = 0; i < kvs.size(); ++i) {
			std::string valStr(kvs[i].value.toString());
			std::reverse(valStr.begin(), valStr.end());
			kvs[i].value = ValueRef(kvs.arena(), valStr);
		}
		return kvs;
	}
};

struct SpecialKeySpaceCorrectnessWorkload : TestWorkload {

	int actorCount, minKeysPerRange, maxKeysPerRange, rangeCount, keyBytes, valBytes, conflictRangeSizeFactor;
	double testDuration, absoluteRandomProb, transactionsPerSecond;
	PerfIntCounter wrongResults, keysCount;
	Reference<ReadYourWritesTransaction> ryw; // used to store all populated data
	std::vector<std::shared_ptr<SKSCTestImpl>> impls;
	Standalone<VectorRef<KeyRangeRef>> keys;

	SpecialKeySpaceCorrectnessWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), wrongResults("Wrong Results"), keysCount("Number of generated keys") {
		minKeysPerRange = getOption(options, LiteralStringRef("minKeysPerRange"), 1);
		maxKeysPerRange = getOption(options, LiteralStringRef("maxKeysPerRange"), 100);
		rangeCount = getOption(options, LiteralStringRef("rangeCount"), 10);
		keyBytes = getOption(options, LiteralStringRef("keyBytes"), 16);
		valBytes = getOption(options, LiteralStringRef("valueBytes"), 16);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 100.0);
		actorCount = getOption(options, LiteralStringRef("actorCount"), 1);
		absoluteRandomProb = getOption(options, LiteralStringRef("absoluteRandomProb"), 0.5);
		// Controls the relative size of read/write conflict ranges and the number of random getranges
		conflictRangeSizeFactor = getOption(options, LiteralStringRef("conflictRangeSizeFactor"), 10);
		ASSERT(conflictRangeSizeFactor >= 1);
	}

	virtual std::string description() { return "SpecialKeySpaceCorrectness"; }
	virtual Future<Void> setup(Database const& cx) { return _setup(cx, this); }
	virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
	virtual Future<bool> check(Database const& cx) { return wrongResults.getValue() == 0; }
	virtual void getMetrics(std::vector<PerfMetric>& m) {}

	// disable the default timeout setting
	double getCheckTimeout() override { return std::numeric_limits<double>::max(); }

	Future<Void> _setup(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		cx->specialKeySpace = std::make_unique<SpecialKeySpace>();
		self->ryw = Reference(new ReadYourWritesTransaction(cx));
		self->ryw->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED);
		self->ryw->setVersion(100);
		self->ryw->clear(normalKeys);
		// generate key ranges
		for (int i = 0; i < self->rangeCount; ++i) {
			std::string baseKey = deterministicRandom()->randomAlphaNumeric(i + 1);
			Key startKey(baseKey + "/");
			Key endKey(baseKey + "/\xff");
			self->keys.push_back_deep(self->keys.arena(), KeyRangeRef(startKey, endKey));
			self->impls.push_back(std::make_shared<SKSCTestImpl>(KeyRangeRef(startKey, endKey)));
			// Although there are already ranges registered, the testing range will replace them
			cx->specialKeySpace->registerKeyRange(
			    SpecialKeySpace::MODULE::TESTONLY, self->keys.back(), self->impls.back().get());
			// generate keys in each key range
			int keysInRange = deterministicRandom()->randomInt(self->minKeysPerRange, self->maxKeysPerRange + 1);
			self->keysCount += keysInRange;
			for (int j = 0; j < keysInRange; ++j) {
				self->ryw->set(Key(deterministicRandom()->randomAlphaNumeric(self->keyBytes)).withPrefix(startKey),
				               Value(deterministicRandom()->randomAlphaNumeric(self->valBytes)));
			}
		}
		return Void();
	}
	ACTOR Future<Void> _start(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		testRywLifetime(cx);
		wait(timeout(self->testModuleRangeReadErrors(cx, self) && self->getRangeCallActor(cx, self) &&
		                 testConflictRanges(cx, /*read*/ true, self) && testConflictRanges(cx, /*read*/ false, self),
		             self->testDuration,
		             Void()));
		return Void();
	}

	// This would be a unit test except we need a Database to create an ryw transaction
	static void testRywLifetime(Database cx) {
		Future<Void> f;
		{
			ReadYourWritesTransaction ryw{ cx->clone() };
			if (!ryw.getDatabase()->apiVersionAtLeast(630)) {
				// This test is not valid for API versions smaller than 630
				return;
			}
			f = success(ryw.get(LiteralStringRef("\xff\xff/status/json")));
			TEST(!f.isReady());
		}
		ASSERT(f.isError());
		ASSERT(f.getError().code() == error_code_transaction_cancelled);
	}

	ACTOR Future<Void> getRangeCallActor(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		state double lastTime = now();
		loop {
			wait(poisson(&lastTime, 1.0 / self->transactionsPerSecond));
			state bool reverse = deterministicRandom()->coinflip();
			state GetRangeLimits limit = self->randomLimits();
			state KeySelector begin = self->randomKeySelector();
			state KeySelector end = self->randomKeySelector();
			auto correctResultFuture = self->ryw->getRange(begin, end, limit, false, reverse);
			ASSERT(correctResultFuture.isReady());
			auto correctResult = correctResultFuture.getValue();
			auto testResultFuture = cx->specialKeySpace->getRange(self->ryw.getPtr(), begin, end, limit, reverse);
			ASSERT(testResultFuture.isReady());
			auto testResult = testResultFuture.getValue();

			// check the consistency of results
			if (!self->compareRangeResult(correctResult, testResult)) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Results from getRange are inconsistent")
				    .detail("Begin", begin.toString())
				    .detail("End", end.toString())
				    .detail("LimitRows", limit.rows)
				    .detail("LimitBytes", limit.bytes)
				    .detail("Reverse", reverse);
				++self->wrongResults;
			}
		}
	}

	bool compareRangeResult(Standalone<RangeResultRef>& res1, Standalone<RangeResultRef>& res2) {
		if ((res1.more != res2.more) || (res1.readToBegin != res2.readToBegin) ||
		    (res1.readThroughEnd != res2.readThroughEnd)) {
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "RangeResultRef flags are inconsistent")
			    .detail("More", res1.more)
			    .detail("ReadToBegin", res1.readToBegin)
			    .detail("ReadThroughEnd", res1.readThroughEnd)
			    .detail("More2", res2.more)
			    .detail("ReadToBegin2", res2.readToBegin)
			    .detail("ReadThroughEnd2", res2.readThroughEnd);
			return false;
		}
		if (res1.size() != res2.size()) {
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "Results' sizes are inconsistent")
			    .detail("CorrestResultSize", res1.size())
			    .detail("TestResultSize", res2.size());
			return false;
		}
		for (int i = 0; i < res1.size(); ++i) {
			if (res1[i].key != res2[i].key) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Keys are inconsistent")
				    .detail("Index", i)
				    .detail("CorrectKey", printable(res1[i].key))
				    .detail("TestKey", printable(res2[i].key));
				return false;
			}
			// Value strings should be reversed pairs
			std::string valStr(res2[i].value.toString());
			std::reverse(valStr.begin(), valStr.end());
			Value valReversed(valStr);
			if (res1[i].value != valReversed) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Values are inconsistent, CorrectValue should be the reverse of the TestValue")
				    .detail("Index", i)
				    .detail("CorrectValue", printable(res1[i].value))
				    .detail("TestValue", printable(res2[i].value));
				return false;
			}
			TEST(true); // Special key space keys equal
		}
		return true;
	}

	KeySelector randomKeySelector() {
		Key randomKey;
		if (deterministicRandom()->random01() < absoluteRandomProb) {
			Key prefix;
			if (deterministicRandom()->random01() < absoluteRandomProb)
				// prefix length is randomly generated
				prefix =
				    Key(deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, rangeCount + 1)) +
				        "/");
			else
				// pick up an existing prefix
				prefix = keys[deterministicRandom()->randomInt(0, rangeCount)].begin;
			randomKey = Key(deterministicRandom()->randomAlphaNumeric(keyBytes)).withPrefix(prefix);
		} else {
			// pick up existing keys from registered key ranges
			KeyRangeRef randomKeyRangeRef = keys[deterministicRandom()->randomInt(0, keys.size())];
			randomKey = deterministicRandom()->coinflip() ? randomKeyRangeRef.begin : randomKeyRangeRef.end;
		}
		// covers corner cases where offset points outside the key space
		int offset = deterministicRandom()->randomInt(-keysCount.getValue() - 1, keysCount.getValue() + 2);
		return KeySelectorRef(randomKey, deterministicRandom()->coinflip(), offset);
	}

	GetRangeLimits randomLimits() {
		// TODO : fix knobs for row_unlimited
		int rowLimits = deterministicRandom()->randomInt(1, keysCount.getValue() + 1);
		// The largest key's bytes is longest prefix bytes + 1(for '/') + generated key bytes
		// 8 here refers to bytes of KeyValueRef
		int byteLimits = deterministicRandom()->randomInt(
		    1, keysCount.getValue() * (keyBytes + (rangeCount + 1) + valBytes + 8) + 1);

		return GetRangeLimits(rowLimits, byteLimits);
	}

	ACTOR Future<Void> testModuleRangeReadErrors(Database cx_, SpecialKeySpaceCorrectnessWorkload* self) {
		Database cx = cx_->clone();
		state Reference<ReadYourWritesTransaction> tx = Reference(new ReadYourWritesTransaction(cx));
		// begin key outside module range
		try {
			wait(success(tx->getRange(
			    KeyRangeRef(LiteralStringRef("\xff\xff/transactio"), LiteralStringRef("\xff\xff/transaction0")),
			    CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_cross_module_read);
			tx->reset();
		}
		// end key outside module range
		try {
			wait(success(tx->getRange(
			    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/"), LiteralStringRef("\xff\xff/transaction1")),
			    CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_cross_module_read);
			tx->reset();
		}
		// both begin and end outside module range
		try {
			wait(success(tx->getRange(
			    KeyRangeRef(LiteralStringRef("\xff\xff/transaction"), LiteralStringRef("\xff\xff/transaction1")),
			    CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_cross_module_read);
			tx->reset();
		}
		// legal range read using the module range
		try {
			wait(success(tx->getRange(
			    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/"), LiteralStringRef("\xff\xff/transaction0")),
			    CLIENT_KNOBS->TOO_MANY)));
			TEST(true);
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// cross module read with option turned on
		try {
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED);
			const KeyRef startKey = LiteralStringRef("\xff\xff/transactio");
			const KeyRef endKey = LiteralStringRef("\xff\xff/transaction1");
			Standalone<RangeResultRef> result =
			    wait(tx->getRange(KeyRangeRef(startKey, endKey), GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
			// The whole transaction module should be empty
			ASSERT(!result.size());
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// end keySelector inside module range, *** a tricky corner case ***
		try {
			tx->addReadConflictRange(singleKeyRange(LiteralStringRef("testKey")));
			KeySelector begin = KeySelectorRef(readConflictRangeKeysRange.begin, false, 1);
			KeySelector end = KeySelectorRef(LiteralStringRef("\xff\xff/transaction0"), false, 0);
			wait(success(tx->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY))));
			TEST(true);
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// No module found error case with keys
		try {
			wait(success(tx->getRange(KeyRangeRef(LiteralStringRef("\xff\xff/A_no_module_related_prefix"),
			                                      LiteralStringRef("\xff\xff/I_am_also_not_in_any_module")),
			                          CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_no_module_found);
			tx->reset();
		}
		// No module found error with KeySelectors, *** a tricky corner case ***
		try {
			KeySelector begin = KeySelectorRef(LiteralStringRef("\xff\xff/zzz_i_am_not_a_module"), false, 1);
			KeySelector end = KeySelectorRef(LiteralStringRef("\xff\xff/zzz_to_be_the_final_one"), false, 2);
			wait(success(tx->getRange(begin, end, CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_no_module_found);
			tx->reset();
		}
		// begin and end keySelectors clamp up to the boundary of the module
		try {
			const KeyRef key = LiteralStringRef("\xff\xff/cluster_file_path");
			KeySelector begin = KeySelectorRef(key, false, 0);
			KeySelector end = KeySelectorRef(keyAfter(key), false, 2);
			Standalone<RangeResultRef> result = wait(tx->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
			ASSERT(result.readToBegin && result.readThroughEnd);
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		try {
			tx->addReadConflictRange(singleKeyRange(LiteralStringRef("readKey")));
			const KeyRef key = LiteralStringRef("\xff\xff/transaction/a_to_be_the_first");
			KeySelector begin = KeySelectorRef(key, false, 0);
			KeySelector end = KeySelectorRef(key, false, 2);
			Standalone<RangeResultRef> result = wait(tx->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
			ASSERT(result.readToBegin && !result.readThroughEnd);
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// base key of the end key selector not in (\xff\xff, \xff\xff\xff), throw key_outside_legal_range()
		try {
			const KeySelector startKeySelector = KeySelectorRef(LiteralStringRef("\xff\xff/test"), true, -200);
			const KeySelector endKeySelector = KeySelectorRef(LiteralStringRef("test"), true, -10);
			Standalone<RangeResultRef> result =
			    wait(tx->getRange(startKeySelector, endKeySelector, GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_key_outside_legal_range);
			tx->reset();
		}
		// test case when registered range is the same as the underlying module
		try {
			state Standalone<RangeResultRef> result =
			    wait(tx->getRange(KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces/"),
			                                  LiteralStringRef("\xff\xff/worker_interfaces0")),
			                      CLIENT_KNOBS->TOO_MANY));
			// Note: there's possibility we get zero workers
			if (result.size()) {
				state KeyValueRef entry = deterministicRandom()->randomChoice(result);
				Optional<Value> singleRes = wait(tx->get(entry.key));
				if (singleRes.present())
					ASSERT(singleRes.get() == entry.value);
			}
		} catch (Error& e) {
			wait(tx->onError(e));
		}

		return Void();
	}

	ACTOR static Future<Void> testConflictRanges(Database cx_, bool read, SpecialKeySpaceCorrectnessWorkload* self) {
		state StringRef prefix = read ? readConflictRangeKeysRange.begin : writeConflictRangeKeysRange.begin;
		TEST(read); // test read conflict range special key implementation
		TEST(!read); // test write conflict range special key implementation
		// Get a default special key range instance
		Database cx = cx_->clone();
		state Reference<ReadYourWritesTransaction> tx = Reference(new ReadYourWritesTransaction(cx));
		state Reference<ReadYourWritesTransaction> referenceTx = Reference(new ReadYourWritesTransaction(cx));
		state bool ryw = deterministicRandom()->coinflip();
		if (!ryw) {
			tx->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
		}
		referenceTx->setVersion(100); // Prevent this from doing a GRV or committing
		referenceTx->clear(normalKeys);
		referenceTx->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		int numKeys = deterministicRandom()->randomInt(1, self->conflictRangeSizeFactor) * 4;
		state std::vector<std::string> keys; // Must all be distinct
		keys.resize(numKeys);
		int lastKey = 0;
		for (auto& key : keys) {
			key = std::to_string(lastKey++);
		}
		if (deterministicRandom()->coinflip()) {
			// Include beginning of keyspace
			keys.push_back("");
		}
		if (deterministicRandom()->coinflip()) {
			// Include end of keyspace
			keys.push_back("\xff");
		}
		std::mt19937 g(deterministicRandom()->randomUInt32());
		std::shuffle(keys.begin(), keys.end(), g);
		// First half of the keys will be ranges, the other keys will mix in some read boundaries that aren't range
		// boundaries
		std::sort(keys.begin(), keys.begin() + keys.size() / 2);
		for (auto iter = keys.begin(); iter + 1 < keys.begin() + keys.size() / 2; iter += 2) {
			Standalone<KeyRangeRef> range = KeyRangeRef(*iter, *(iter + 1));
			if (read) {
				tx->addReadConflictRange(range);
				// Add it twice so that we can observe the de-duplication that should get done
				tx->addReadConflictRange(range);
			} else {
				tx->addWriteConflictRange(range);
				tx->addWriteConflictRange(range);
			}
			// TODO test that fails if we don't wait on tx->pendingReads()
			referenceTx->set(range.begin, LiteralStringRef("1"));
			referenceTx->set(range.end, LiteralStringRef("0"));
		}
		if (!read && deterministicRandom()->coinflip()) {
			try {
				wait(tx->commit());
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				return Void();
			}
			TEST(true); // Read write conflict range of committed transaction
		}
		try {
			wait(success(tx->get(LiteralStringRef("\xff\xff/1314109/i_hope_this_isn't_registered"))));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_no_module_found);
		}
		for (int i = 0; i < self->conflictRangeSizeFactor; ++i) {
			GetRangeLimits limit;
			KeySelector begin;
			KeySelector end;
			loop {
				begin = firstGreaterOrEqual(deterministicRandom()->randomChoice(keys));
				end = firstGreaterOrEqual(deterministicRandom()->randomChoice(keys));
				if (begin.getKey() < end.getKey())
					break;
			}
			bool reverse = deterministicRandom()->coinflip();

			auto correctResultFuture = referenceTx->getRange(begin, end, limit, false, reverse);
			ASSERT(correctResultFuture.isReady());
			begin.setKey(begin.getKey().withPrefix(prefix, begin.arena()));
			end.setKey(end.getKey().withPrefix(prefix, begin.arena()));
			auto testResultFuture = tx->getRange(begin, end, limit, false, reverse);
			ASSERT(testResultFuture.isReady());
			auto correct_iter = correctResultFuture.get().begin();
			auto test_iter = testResultFuture.get().begin();
			bool had_error = false;
			while (correct_iter != correctResultFuture.get().end() && test_iter != testResultFuture.get().end()) {
				if (correct_iter->key != test_iter->key.removePrefix(prefix) ||
				    correct_iter->value != test_iter->value) {
					TraceEvent(SevError, "TestFailure")
					    .detail("Reason", "Mismatched keys")
					    .detail("ConflictType", read ? "read" : "write")
					    .detail("CorrectKey", correct_iter->key)
					    .detail("TestKey", test_iter->key)
					    .detail("CorrectValue", correct_iter->value)
					    .detail("TestValue", test_iter->value)
					    .detail("Begin", begin.toString())
					    .detail("End", end.toString())
					    .detail("Ryw", ryw);
					had_error = true;
					++self->wrongResults;
				}
				++correct_iter;
				++test_iter;
			}
			while (correct_iter != correctResultFuture.get().end()) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Extra correct key")
				    .detail("ConflictType", read ? "read" : "write")
				    .detail("CorrectKey", correct_iter->key)
				    .detail("CorrectValue", correct_iter->value)
				    .detail("Begin", begin.toString())
				    .detail("End", end.toString())
				    .detail("Ryw", ryw);
				++correct_iter;
				had_error = true;
				++self->wrongResults;
			}
			while (test_iter != testResultFuture.get().end()) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Extra test key")
				    .detail("ConflictType", read ? "read" : "write")
				    .detail("TestKey", test_iter->key)
				    .detail("TestValue", test_iter->value)
				    .detail("Begin", begin.toString())
				    .detail("End", end.toString())
				    .detail("Ryw", ryw);
				++test_iter;
				had_error = true;
				++self->wrongResults;
			}
			if (had_error)
				break;
		}
		return Void();
	}
};

WorkloadFactory<SpecialKeySpaceCorrectnessWorkload> SpecialKeySpaceCorrectnessFactory("SpecialKeySpaceCorrectness");
