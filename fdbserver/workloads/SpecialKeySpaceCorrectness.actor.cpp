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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

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
		self->ryw->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
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
			cx->specialKeySpace->registerKeyRange(SpecialKeySpace::MODULE::TESTONLY,
			                                      SpecialKeySpace::IMPLTYPE::READWRITE, self->keys.back(),
			                                      self->impls.back().get());
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
		wait(timeout(self->testSpecialKeySpaceErrors(cx, self) && self->getRangeCallActor(cx, self) &&
		                 testConflictRanges(cx, /*read*/ true, self) && testConflictRanges(cx, /*read*/ false, self),
		             self->testDuration, Void()));
		// Only use one client to avoid potential conflicts on changing cluster configuration
		if (self->clientId == 0) wait(self->managementApiCorrectnessActor(cx, self));
		return Void();
	}

	// This would be a unit test except we need a Database to create an ryw transaction
	static void testRywLifetime(Database cx) {
		Future<Void> f;
		{
			ReadYourWritesTransaction ryw{ cx->clone() };
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

			// check ryw result consistency
			KeyRange rkr = self->randomKeyRange();
			KeyRef rkey1 = rkr.begin;
			KeyRef rkey2 = rkr.end;
			// randomly set/clear two keys or clear a key range
			if (deterministicRandom()->coinflip()) {
				Value rvalue1 = self->randomValue();
				cx->specialKeySpace->set(self->ryw.getPtr(), rkey1, rvalue1);
				self->ryw->set(rkey1, rvalue1);
				Value rvalue2 = self->randomValue();
				cx->specialKeySpace->set(self->ryw.getPtr(), rkey2, rvalue2);
				self->ryw->set(rkey2, rvalue2);
			} else if (deterministicRandom()->coinflip()) {
				cx->specialKeySpace->clear(self->ryw.getPtr(), rkey1);
				self->ryw->clear(rkey1);
				cx->specialKeySpace->clear(self->ryw.getPtr(), rkey2);
				self->ryw->clear(rkey2);
			} else {
				cx->specialKeySpace->clear(self->ryw.getPtr(), rkr);
				self->ryw->clear(rkr);
			}
			// use the same key selectors again to test consistency of ryw
			auto correctRywResultFuture = self->ryw->getRange(begin, end, limit, false, reverse);
			ASSERT(correctRywResultFuture.isReady());
			auto correctRywResult = correctRywResultFuture.getValue();
			auto testRywResultFuture = cx->specialKeySpace->getRange(self->ryw.getPtr(), begin, end, limit, reverse);
			ASSERT(testRywResultFuture.isReady());
			auto testRywResult = testRywResultFuture.getValue();

			// check the consistency of results
			if (!self->compareRangeResult(correctRywResult, testRywResult)) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Results from getRange(ryw) are inconsistent")
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
			if (res1[i].value != res2[i].value) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Values are inconsistent")
				    .detail("Index", i)
				    .detail("CorrectValue", printable(res1[i].value))
				    .detail("TestValue", printable(res2[i].value));
				return false;
			}
			TEST(true); // Special key space keys equal
		}
		return true;
	}

	KeyRange randomKeyRange() {
		Key prefix = keys[deterministicRandom()->randomInt(0, rangeCount)].begin;
		Key rkey1 = Key(deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(0, keyBytes)))
		                .withPrefix(prefix);
		Key rkey2 = Key(deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(0, keyBytes)))
		                .withPrefix(prefix);
		return rkey1 <= rkey2 ? KeyRangeRef(rkey1, rkey2) : KeyRangeRef(rkey2, rkey1);
	}

	Key randomKey() {
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
		return randomKey;
	}

	Value randomValue() { return Value(deterministicRandom()->randomAlphaNumeric(valBytes)); }

	KeySelector randomKeySelector() {
		// covers corner cases where offset points outside the key space
		int offset = deterministicRandom()->randomInt(-keysCount.getValue() - 1, keysCount.getValue() + 2);
		return KeySelectorRef(randomKey(), deterministicRandom()->coinflip(), offset);
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

	ACTOR Future<Void> testSpecialKeySpaceErrors(Database cx_, SpecialKeySpaceCorrectnessWorkload* self) {
		Database cx = cx_->clone();
		state Reference<ReadYourWritesTransaction> tx = Reference(new ReadYourWritesTransaction(cx));
		// begin key outside module range
		try {
			wait(success(tx->getRange(
			    KeyRangeRef(LiteralStringRef("\xff\xff/transactio"), LiteralStringRef("\xff\xff/transaction0")),
			    CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) throw;
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
			if (e.code() == error_code_actor_cancelled) throw;
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
			if (e.code() == error_code_actor_cancelled) throw;
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
			if (e.code() == error_code_actor_cancelled) throw;
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
			if (e.code() == error_code_actor_cancelled) throw;
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
		// Errors introduced by SpecialKeyRangeRWImpl
		// Writes are disabled by default
		try {
			tx->set(LiteralStringRef("\xff\xff/I_am_not_a_range_can_be_written"), ValueRef());
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) throw;
			ASSERT(e.code() == error_code_special_keys_write_disabled);
			tx->reset();
		}
		// The special key is not in a range that can be called with set
		try {
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->set(LiteralStringRef("\xff\xff/I_am_not_a_range_can_be_written"), ValueRef());
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) throw;
			ASSERT(e.code() == error_code_special_keys_no_write_module_found);
			tx->reset();
		}
		// A clear cross two ranges are forbidden
		try {
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->clear(KeyRangeRef(SpecialKeySpace::getManamentApiCommandRange("exclude").begin,
			                      SpecialKeySpace::getManamentApiCommandRange("failed").end));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) throw;
			ASSERT(e.code() == error_code_special_keys_cross_module_clear);
			tx->reset();
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
				if (e.code() == error_code_actor_cancelled) throw;
				return Void();
			}
			TEST(true); // Read write conflict range of committed transaction
		}
		try {
			wait(success(tx->get(LiteralStringRef("\xff\xff/1314109/i_hope_this_isn't_registered"))));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) throw;
			ASSERT(e.code() == error_code_special_keys_no_module_found);
		}
		for (int i = 0; i < self->conflictRangeSizeFactor; ++i) {
			GetRangeLimits limit;
			KeySelector begin;
			KeySelector end;
			loop {
				begin = firstGreaterOrEqual(deterministicRandom()->randomChoice(keys));
				end = firstGreaterOrEqual(deterministicRandom()->randomChoice(keys));
				if (begin.getKey() < end.getKey()) break;
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
			if (had_error) break;
		}
		return Void();
	}

	bool getRangeResultInOrder(const Standalone<RangeResultRef>& result) {
		for (int i = 0; i < result.size() - 1; ++i) {
			if (result[i].key >= result[i + 1].key) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "GetRangeResultNotInOrder")
				    .detail("Index", i)
				    .detail("Key1", result[i].key)
				    .detail("Key2", result[i + 1].key);
				return false;
			}
		}
		return true;
	}

	ACTOR Future<Void> managementApiCorrectnessActor(Database cx_, SpecialKeySpaceCorrectnessWorkload* self) {
		// All management api related tests
		Database cx = cx_->clone();
		state Reference<ReadYourWritesTransaction> tx = Reference(new ReadYourWritesTransaction(cx));
		// test ordered option keys
		{
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (const std::string& option : SpecialKeySpace::getManagementApiOptionsSet()) {
				tx->set(LiteralStringRef("options/")
				            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
				            .withSuffix(option),
				        ValueRef());
			}
			Standalone<RangeResultRef> result = wait(tx->getRange(
			    KeyRangeRef(LiteralStringRef("options/"), LiteralStringRef("options0"))
			        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin),
			    CLIENT_KNOBS->TOO_MANY));
			ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(result.size() == SpecialKeySpace::getManagementApiOptionsSet().size());
			ASSERT(self->getRangeResultInOrder(result));
			tx->reset();
		}
		// "exclude" error message shema check
		try {
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->set(LiteralStringRef("Invalid_Network_Address")
			            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("exclude")),
			        ValueRef());
			wait(tx->commit());
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) throw;
			if (e.code() == error_code_special_keys_api_failure) {
				Optional<Value> errorMsg =
				    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
				ASSERT(errorMsg.present());
				std::string errorStr;
				auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
				auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
				// special_key_space_management_api_error_msg schema validation
				ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
				ASSERT(valueObj["command"].get_str() == "exclude" && !valueObj["retriable"].get_bool());
			} else {
				TraceEvent(SevDebug, "UnexpectedError").detail("Command", "Exclude").error(e);
				wait(tx->onError(e));
			}
			tx->reset();
		}
		// "setclass"
		{
			try {
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// test getRange
				state Standalone<RangeResultRef> result = wait(tx->getRange(
				    KeyRangeRef(LiteralStringRef("process/class_type/"), LiteralStringRef("process/class_type0"))
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(result));
				// check correctness of classType of each process
				vector<ProcessData> workers = wait(getWorkers(&tx->getTransaction()));
				if (workers.size()) {
					for (const auto& worker : workers) {
						Key addr =
						    Key("process/class_type/" + formatIpPort(worker.address.ip, worker.address.port))
						        .withPrefix(
						            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
						bool found = false;
						for (const auto& kv : result) {
							if (kv.key == addr) {
								ASSERT(kv.value.toString() == worker.processClass.toString());
								found = true;
								break;
							}
						}
						// Each process should find its corresponding element
						ASSERT(found);
					}
					state ProcessData worker = deterministicRandom()->randomChoice(workers);
					state Key addr =
					    Key("process/class_type/" + formatIpPort(worker.address.ip, worker.address.port))
					        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
					tx->set(addr, LiteralStringRef("InvalidProcessType"));
					// test ryw
					Optional<Value> processType = wait(tx->get(addr));
					ASSERT(processType.present() && processType.get() == LiteralStringRef("InvalidProcessType"));
					// test ryw disabled
					tx->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					Optional<Value> originalProcessType = wait(tx->get(addr));
					ASSERT(originalProcessType.present() &&
					       originalProcessType.get() == worker.processClass.toString());
					// test error handling (invalid value type)
					wait(tx->commit());
					ASSERT(false);
				} else {
					// If no worker process returned, skip the test
					TraceEvent(SevDebug, "EmptyWorkerListInSetClassTest");
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) throw;
				if (e.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
					ASSERT(errorMsg.present());
					std::string errorStr;
					auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
					auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
					// special_key_space_management_api_error_msg schema validation
					ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
					ASSERT(valueObj["command"].get_str() == "setclass" && !valueObj["retriable"].get_bool());
				} else {
					TraceEvent(SevDebug, "UnexpectedError").detail("Command", "Setclass").error(e);
					wait(tx->onError(e));
				}
				tx->reset();
			}
		}
		// read class_source
		{
			try {
				// test getRange
				state Standalone<RangeResultRef> class_source_result = wait(tx->getRange(
				    KeyRangeRef(LiteralStringRef("process/class_source/"), LiteralStringRef("process/class_source0"))
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY));
				ASSERT(!class_source_result.more && class_source_result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(class_source_result));
				// check correctness of classType of each process
				vector<ProcessData> workers = wait(getWorkers(&tx->getTransaction()));
				if (workers.size()) {
					for (const auto& worker : workers) {
						Key addr =
						    Key("process/class_source/" + formatIpPort(worker.address.ip, worker.address.port))
						        .withPrefix(
						            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
						bool found = false;
						for (const auto& kv : class_source_result) {
							if (kv.key == addr) {
								ASSERT(kv.value.toString() == worker.processClass.sourceString());
								// Default source string is command_line
								ASSERT(kv.value == LiteralStringRef("command_line"));
								found = true;
								break;
							}
						}
						// Each process should find its corresponding element
						ASSERT(found);
					}
					ProcessData worker = deterministicRandom()->randomChoice(workers);
					state std::string address = formatIpPort(worker.address.ip, worker.address.port);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set(
					    Key("process/class_type/" + address)
					        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
					    Value(worker.processClass.toString())); // Set it as the same class type as before, thus only
					                                            // class source will be changed
					wait(tx->commit());
					Optional<Value> class_source = wait(tx->get(
					    Key("process/class_source/" + address)
					        .withPrefix(
					            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
					ASSERT(class_source.present() && class_source.get() == LiteralStringRef("set_class"));
					tx->reset();
				} else {
					// If no worker process returned, skip the test
					TraceEvent(SevDebug, "EmptyWorkerListInSetClassTest");
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) throw;
				wait(tx->onError(e));
			}
		}
		return Void();
	}
};

WorkloadFactory<SpecialKeySpaceCorrectnessWorkload> SpecialKeySpaceCorrectnessFactory("SpecialKeySpaceCorrectness");
