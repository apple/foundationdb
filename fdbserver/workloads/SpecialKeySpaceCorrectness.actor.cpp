/*
 * SpecialKeySpaceCorrectness.actor.cpp
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

#include "boost/lexical_cast.hpp"
#include "boost/algorithm/string.hpp"

#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
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

	std::string description() const override { return "SpecialKeySpaceCorrectness"; }
	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	Future<bool> check(Database const& cx) override { return wrongResults.getValue() == 0; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// disable the default timeout setting
	double getCheckTimeout() const override { return std::numeric_limits<double>::max(); }

	Future<Void> _setup(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		cx->specialKeySpace = std::make_unique<SpecialKeySpace>();
		self->ryw = makeReference<ReadYourWritesTransaction>(cx);
		self->ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
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
			                                      SpecialKeySpace::IMPLTYPE::READWRITE,
			                                      self->keys.back(),
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
		             self->testDuration,
		             Void()));
		// Only use one client to avoid potential conflicts on changing cluster configuration
		if (self->clientId == 0)
			wait(self->managementApiCorrectnessActor(cx, self));
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
			TEST(!f.isReady()); // status json not ready
		}
		ASSERT(f.isError());
		ASSERT(f.getError().code() == error_code_transaction_cancelled);
	}

	ACTOR Future<Void> getRangeCallActor(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		state double lastTime = now();
		state Reverse reverse = Reverse::False;
		loop {
			wait(poisson(&lastTime, 1.0 / self->transactionsPerSecond));
			reverse.set(deterministicRandom()->coinflip());
			state GetRangeLimits limit = self->randomLimits();
			state KeySelector begin = self->randomKeySelector();
			state KeySelector end = self->randomKeySelector();
			auto correctResultFuture = self->ryw->getRange(begin, end, limit, Snapshot::False, reverse);
			ASSERT(correctResultFuture.isReady());
			auto correctResult = correctResultFuture.getValue();
			auto testResultFuture = cx->specialKeySpace->getRange(self->ryw.getPtr(), begin, end, limit, reverse);
			ASSERT(testResultFuture.isReady());
			auto testResult = testResultFuture.getValue();

			// check the consistency of results
			if (!self->compareRangeResult(correctResult, testResult)) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Results from getRange are inconsistent")
				    .detail("Begin", begin)
				    .detail("End", end)
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
			auto correctRywResultFuture = self->ryw->getRange(begin, end, limit, Snapshot::False, reverse);
			ASSERT(correctRywResultFuture.isReady());
			auto correctRywResult = correctRywResultFuture.getValue();
			auto testRywResultFuture = cx->specialKeySpace->getRange(self->ryw.getPtr(), begin, end, limit, reverse);
			ASSERT(testRywResultFuture.isReady());
			auto testRywResult = testRywResultFuture.getValue();

			// check the consistency of results
			if (!self->compareRangeResult(correctRywResult, testRywResult)) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Results from getRange(ryw) are inconsistent")
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LimitRows", limit.rows)
				    .detail("LimitBytes", limit.bytes)
				    .detail("Reverse", reverse);
				++self->wrongResults;
			}
		}
	}

	bool compareRangeResult(RangeResult const& res1, RangeResult const& res2) {
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
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
		// begin key outside module range
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
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
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
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
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
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
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			wait(success(tx->getRange(
			    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/"), LiteralStringRef("\xff\xff/transaction0")),
			    CLIENT_KNOBS->TOO_MANY)));
			TEST(true); // read transaction special keyrange
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// cross module read with option turned on
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED);
			const KeyRef startKey = LiteralStringRef("\xff\xff/transactio");
			const KeyRef endKey = LiteralStringRef("\xff\xff/transaction1");
			RangeResult result =
			    wait(tx->getRange(KeyRangeRef(startKey, endKey), GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
			// The whole transaction module should be empty
			ASSERT(!result.size());
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// end keySelector inside module range, *** a tricky corner case ***
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->addReadConflictRange(singleKeyRange(LiteralStringRef("testKey")));
			KeySelector begin = KeySelectorRef(readConflictRangeKeysRange.begin, false, 1);
			KeySelector end = KeySelectorRef(LiteralStringRef("\xff\xff/transaction0"), false, 0);
			wait(success(tx->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY))));
			TEST(true); // end key selector inside module range
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// No module found error case with keys
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
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
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
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
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			const KeyRef key = LiteralStringRef("\xff\xff/cluster_file_path");
			KeySelector begin = KeySelectorRef(key, false, 0);
			KeySelector end = KeySelectorRef(keyAfter(key), false, 2);
			RangeResult result = wait(tx->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
			ASSERT(result.readToBegin && result.readThroughEnd);
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->addReadConflictRange(singleKeyRange(LiteralStringRef("readKey")));
			const KeyRef key = LiteralStringRef("\xff\xff/transaction/a_to_be_the_first");
			KeySelector begin = KeySelectorRef(key, false, 0);
			KeySelector end = KeySelectorRef(key, false, 2);
			RangeResult result = wait(tx->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
			ASSERT(result.readToBegin && !result.readThroughEnd);
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// Errors introduced by SpecialKeyRangeRWImpl
		// Writes are disabled by default
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->set(LiteralStringRef("\xff\xff/I_am_not_a_range_can_be_written"), ValueRef());
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_write_disabled);
			tx->reset();
		}
		// The special key is not in a range that can be called with set
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->set(LiteralStringRef("\xff\xff/I_am_not_a_range_can_be_written"), ValueRef());
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_no_write_module_found);
			tx->reset();
		}
		// A clear cross two ranges are forbidden
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->clear(KeyRangeRef(SpecialKeySpace::getManagementApiCommandRange("exclude").begin,
			                      SpecialKeySpace::getManagementApiCommandRange("failed").end));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_special_keys_cross_module_clear);
			tx->reset();
		}
		// base key of the end key selector not in (\xff\xff, \xff\xff\xff), throw key_outside_legal_range()
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			const KeySelector startKeySelector = KeySelectorRef(LiteralStringRef("\xff\xff/test"), true, -200);
			const KeySelector endKeySelector = KeySelectorRef(LiteralStringRef("test"), true, -10);
			RangeResult result =
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
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			state RangeResult result = wait(tx->getRange(KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces/"),
			                                                         LiteralStringRef("\xff\xff/worker_interfaces0")),
			                                             CLIENT_KNOBS->TOO_MANY));
			// Note: there's possibility we get zero workers
			if (result.size()) {
				state KeyValueRef entry = deterministicRandom()->randomChoice(result);
				Optional<Value> singleRes = wait(tx->get(entry.key));
				if (singleRes.present())
					ASSERT(singleRes.get() == entry.value);
			}
			tx->reset();
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
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
		state Reference<ReadYourWritesTransaction> referenceTx = makeReference<ReadYourWritesTransaction>(cx);
		state bool ryw = deterministicRandom()->coinflip();
		tx->setOption(FDBTransactionOptions::RAW_ACCESS);
		if (!ryw) {
			tx->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
		}
		referenceTx->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		referenceTx->setVersion(100); // Prevent this from doing a GRV or committing
		referenceTx->clear(normalKeys);
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
			Reverse reverse{ deterministicRandom()->coinflip() };

			auto correctResultFuture = referenceTx->getRange(begin, end, limit, Snapshot::False, reverse);
			ASSERT(correctResultFuture.isReady());
			begin.setKey(begin.getKey().withPrefix(prefix, begin.arena()));
			end.setKey(end.getKey().withPrefix(prefix, begin.arena()));
			auto testResultFuture = tx->getRange(begin, end, limit, Snapshot::False, reverse);
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
					    .detail("Begin", begin)
					    .detail("End", end)
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
				    .detail("Begin", begin)
				    .detail("End", end)
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
				    .detail("Begin", begin)
				    .detail("End", end)
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

	bool getRangeResultInOrder(const RangeResult& result) {
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
		state Database cx = cx_->clone();
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
		// test ordered option keys
		{
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (const std::string& option : SpecialKeySpace::getManagementApiOptionsSet()) {
				tx->set(
				    "options/"_sr.withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
				        .withSuffix(option),
				    ValueRef());
			}
			RangeResult result = wait(tx->getRange(
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
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->set(LiteralStringRef("Invalid_Network_Address")
			            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("exclude")),
			        ValueRef());
			wait(tx->commit());
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
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
				TraceEvent(SevDebug, "UnexpectedError").error(e).detail("Command", "Exclude");
				wait(tx->onError(e));
			}
			tx->reset();
		}
		// "setclass"
		{
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// test getRange
				state RangeResult result = wait(tx->getRange(
				    KeyRangeRef(LiteralStringRef("process/class_type/"), LiteralStringRef("process/class_type0"))
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(result));
				// check correctness of classType of each process
				std::vector<ProcessData> workers = wait(getWorkers(&tx->getTransaction()));
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
					TraceEvent(SevDebug, "EmptyWorkerListInSetClassTest").log();
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
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
					TraceEvent(SevDebug, "UnexpectedError").error(e).detail("Command", "Setclass");
					wait(tx->onError(e));
				}
				tx->reset();
			}
		}
		// read class_source
		{
			try {
				// test getRange
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				state RangeResult class_source_result = wait(tx->getRange(
				    KeyRangeRef(LiteralStringRef("process/class_source/"), LiteralStringRef("process/class_source0"))
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY));
				ASSERT(!class_source_result.more && class_source_result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(class_source_result));
				// check correctness of classType of each process
				std::vector<ProcessData> workers = wait(getWorkers(&tx->getTransaction()));
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
					tx->reset();
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					Optional<Value> class_source = wait(tx->get(
					    Key("process/class_source/" + address)
					        .withPrefix(
					            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
					TraceEvent(SevDebug, "SetClassSourceDebug")
					    .detail("Present", class_source.present())
					    .detail("ClassSource", class_source.present() ? class_source.get().toString() : "__Nothing");
					// Very rarely, we get an empty worker list, thus no class_source data
					if (class_source.present())
						ASSERT(class_source.get() == LiteralStringRef("set_class"));
					tx->reset();
				} else {
					// If no worker process returned, skip the test
					TraceEvent(SevDebug, "EmptyWorkerListInSetClassTest").log();
				}
			} catch (Error& e) {
				wait(tx->onError(e));
			}
		}
		// test lock and unlock
		// maske sure we lock the database
		loop {
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// lock the database
				UID uid = deterministicRandom()->randomUniqueID();
				tx->set(SpecialKeySpace::getManagementApiCommandPrefix("lock"), uid.toString());
				// commit
				wait(tx->commit());
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "DatabaseLockFailure").error(e);
				// In case commit_unknown_result is thrown by buggify, we may try to lock more than once
				// The second lock commit will throw special_keys_api_failure error
				if (e.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
					ASSERT(errorMsg.present());
					std::string errorStr;
					auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
					auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
					// special_key_space_management_api_error_msg schema validation
					ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
					ASSERT(valueObj["command"].get_str() == "lock" && !valueObj["retriable"].get_bool());
					break;
				} else if (e.code() == error_code_database_locked) {
					// Database is already locked. This can happen if a previous attempt
					// failed with unknown_result.
					break;
				} else {
					wait(tx->onError(e));
				}
			}
		}
		TraceEvent(SevDebug, "DatabaseLocked").log();
		// if database locked, fdb read should get database_locked error
		try {
			tx->reset();
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			RangeResult res = wait(tx->getRange(normalKeys, 1));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_database_locked);
		}
		// make sure we unlock the database
		// unlock is idempotent, thus we can commit many times until successful
		loop {
			try {
				tx->reset();
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// unlock the database
				tx->clear(SpecialKeySpace::getManagementApiCommandPrefix("lock"));
				wait(tx->commit());
				TraceEvent(SevDebug, "DatabaseUnlocked").log();
				tx->reset();
				// read should be successful
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				RangeResult res = wait(tx->getRange(normalKeys, 1));
				tx->reset();
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "DatabaseUnlockFailure").error(e);
				ASSERT(e.code() != error_code_database_locked);
				wait(tx->onError(e));
			}
		}
		// test consistencycheck which only used by ConsistencyCheck Workload
		// Note: we have exclusive ownership of fdbShouldConsistencyCheckBeSuspended,
		// no existing workloads can modify the key
		{
			try {
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> val1 = wait(tx->get(fdbShouldConsistencyCheckBeSuspended));
				state bool ccSuspendSetting =
				    val1.present() ? BinaryReader::fromStringRef<bool>(val1.get(), Unversioned()) : false;
				Optional<Value> val2 =
				    wait(tx->get(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck")));
				// Make sure the read result from special key consistency with the system key
				ASSERT(ccSuspendSetting ? val2.present() : !val2.present());
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// Make sure by default, consistencycheck is enabled
				ASSERT(!ccSuspendSetting);
				// Disable consistencycheck
				tx->set(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"), ValueRef());
				wait(tx->commit());
				tx->reset();
				// Read system key to make sure it is disabled
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> val3 = wait(tx->get(fdbShouldConsistencyCheckBeSuspended));
				bool ccSuspendSetting2 =
				    val3.present() ? BinaryReader::fromStringRef<bool>(val3.get(), Unversioned()) : false;
				ASSERT(ccSuspendSetting2);
				tx->reset();
			} catch (Error& e) {
				wait(tx->onError(e));
			}
		}
		// make sure we enable consistencycheck by the end
		{
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->clear(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"));
					wait(tx->commit());
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
		}
		// coordinators
		// test read, makes sure it's the same as reading from coordinatorsKey
		loop {
			try {
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> res = wait(tx->get(coordinatorsKey));
				ASSERT(res.present()); // Otherwise, database is in a bad state
				state ClusterConnectionString cs(res.get().toString());
				Optional<Value> coordinator_processes_key =
				    wait(tx->get(LiteralStringRef("processes")
				                     .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators"))));
				ASSERT(coordinator_processes_key.present());
				state std::vector<std::string> process_addresses;
				boost::split(
				    process_addresses, coordinator_processes_key.get().toString(), [](char c) { return c == ','; });
				ASSERT(process_addresses.size() == cs.coordinators().size() + cs.hostnames.size());
				wait(cs.resolveHostnames());
				// compare the coordinator process network addresses one by one
				for (const auto& network_address : cs.coordinators()) {
					ASSERT(std::find(process_addresses.begin(), process_addresses.end(), network_address.toString()) !=
					       process_addresses.end());
				}
				tx->reset();
				break;
			} catch (Error& e) {
				wait(tx->onError(e));
			}
		}
		// test change coordinators and cluster description
		// we randomly pick one process(not coordinator) and add it, in this case, it should always succeed
		{
			state std::string new_cluster_description;
			state std::string new_coordinator_process;
			state std::vector<std::string> old_coordinators_processes;
			state bool possible_to_add_coordinator;
			state KeyRange coordinators_key_range =
			    KeyRangeRef(LiteralStringRef("process/"), LiteralStringRef("process0"))
			        .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators"));
			loop {
				try {
					tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					Optional<Value> ccStrValue = wait(tx->get(coordinatorsKey));
					ASSERT(ccStrValue.present()); // Otherwise, database is in a bad state
					ClusterConnectionString ccStr(ccStrValue.get().toString());
					// choose a new description if configuration allows transactions across differently named clusters
					new_cluster_description = SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT
					                              ? deterministicRandom()->randomAlphaNumeric(8)
					                              : ccStr.clusterKeyName().toString();
					// get current coordinators
					Optional<Value> processes_key =
					    wait(tx->get(LiteralStringRef("processes")
					                     .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators"))));
					ASSERT(processes_key.present());
					boost::split(
					    old_coordinators_processes, processes_key.get().toString(), [](char c) { return c == ','; });
					// pick up one non-coordinator process if possible
					std::vector<ProcessData> workers = wait(getWorkers(&tx->getTransaction()));
					std::string old_coordinators_processes_string = describe(old_coordinators_processes);
					TraceEvent(SevDebug, "CoordinatorsManualChange")
					    .detail("OldCoordinators", old_coordinators_processes_string)
					    .detail("WorkerSize", workers.size());
					if (workers.size() > old_coordinators_processes.size()) {
						loop {
							auto worker = deterministicRandom()->randomChoice(workers);
							new_coordinator_process = worker.address.toString();
							if (old_coordinators_processes_string.find(new_coordinator_process) == std::string::npos) {
								break;
							}
						}
						possible_to_add_coordinator = true;
					} else {
						possible_to_add_coordinator = false;
					}
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				}
			}
			TraceEvent(SevDebug, "CoordinatorsManualChange")
			    .detail("NewCoordinator", possible_to_add_coordinator ? new_coordinator_process : "")
			    .detail("NewClusterDescription", new_cluster_description);
			if (possible_to_add_coordinator) {
				loop {
					try {
						std::string new_processes_key(new_coordinator_process);
						tx->setOption(FDBTransactionOptions::RAW_ACCESS);
						tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
						for (const auto& address : old_coordinators_processes) {
							new_processes_key += "," + address;
						}
						tx->set(LiteralStringRef("processes")
						            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
						        Value(new_processes_key));
						// update cluster description
						tx->set(LiteralStringRef("cluster_description")
						            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
						        Value(new_cluster_description));
						wait(tx->commit());
						ASSERT(false);
					} catch (Error& e) {
						TraceEvent(SevDebug, "CoordinatorsManualChange").error(e);
						// if we repeat doing the change, we will get the error:
						// CoordinatorsResult::SAME_NETWORK_ADDRESSES
						if (e.code() == error_code_special_keys_api_failure) {
							Optional<Value> errorMsg =
							    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
							ASSERT(errorMsg.present());
							std::string errorStr;
							auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
							auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
							// special_key_space_management_api_error_msg schema validation
							ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
							TraceEvent(SevDebug, "CoordinatorsManualChange")
							    .detail("ErrorMessage", valueObj["message"].get_str());
							ASSERT(valueObj["command"].get_str() == "coordinators");
							if (valueObj["retriable"].get_bool()) { // coordinators not reachable, retry
								tx->reset();
							} else {
								ASSERT(valueObj["message"].get_str() ==
								       "No change (existing configuration satisfies request)");
								tx->reset();
								break;
							}
						} else {
							wait(tx->onError(e));
						}
						wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
					}
				}
				// change successful, now check it is already changed
				try {
					tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					Optional<Value> res = wait(tx->get(coordinatorsKey));
					ASSERT(res.present()); // Otherwise, database is in a bad state
					state ClusterConnectionString csNew(res.get().toString());
					wait(csNew.resolveHostnames());
					ASSERT(csNew.coordinators().size() == old_coordinators_processes.size() + 1);
					// verify the coordinators' addresses
					for (const auto& network_address : csNew.coordinators()) {
						std::string address_str = network_address.toString();
						ASSERT(std::find(old_coordinators_processes.begin(),
						                 old_coordinators_processes.end(),
						                 address_str) != old_coordinators_processes.end() ||
						       new_coordinator_process == address_str);
					}
					// verify the cluster decription
					ASSERT(new_cluster_description == csNew.clusterKeyName().toString());
					tx->reset();
				} catch (Error& e) {
					wait(tx->onError(e));
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				}
				// change back to original settings
				loop {
					try {
						std::string new_processes_key;
						tx->setOption(FDBTransactionOptions::RAW_ACCESS);
						tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
						for (const auto& address : old_coordinators_processes) {
							new_processes_key += new_processes_key.size() ? "," : "";
							new_processes_key += address;
						}
						tx->set(LiteralStringRef("processes")
						            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
						        Value(new_processes_key));
						wait(tx->commit());
						ASSERT(false);
					} catch (Error& e) {
						TraceEvent(SevDebug, "CoordinatorsManualChangeRevert").error(e);
						if (e.code() == error_code_special_keys_api_failure) {
							Optional<Value> errorMsg =
							    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
							ASSERT(errorMsg.present());
							std::string errorStr;
							auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
							auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
							// special_key_space_management_api_error_msg schema validation
							ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
							TraceEvent(SevDebug, "CoordinatorsManualChangeRevert")
							    .detail("ErrorMessage", valueObj["message"].get_str());
							ASSERT(valueObj["command"].get_str() == "coordinators");
							if (valueObj["retriable"].get_bool()) {
								tx->reset();
							} else if (valueObj["message"].get_str() ==
							           "No change (existing configuration satisfies request)") {
								tx->reset();
								break;
							} else {
								TraceEvent(SevError, "CoordinatorsManualChangeRevert")
								    .detail("UnexpectedError", valueObj["message"].get_str());
								throw special_keys_api_failure();
							}
						} else {
							wait(tx->onError(e));
						}
						wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
					}
				}
			}
		}
		// advanceversion
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			Version v1 = wait(tx->getReadVersion());
			TraceEvent(SevDebug, "InitialReadVersion").detail("Version", v1);
			state Version v2 = 2 * v1;
			loop {
				try {
					// loop until the grv is larger than the set version
					Version v3 = wait(tx->getReadVersion());
					if (v3 > v2) {
						TraceEvent(SevDebug, "AdvanceVersionSuccess").detail("Version", v3);
						break;
					}
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					// force the cluster to recover at v2
					tx->set(SpecialKeySpace::getManagementApiCommandPrefix("advanceversion"), std::to_string(v2));
					wait(tx->commit());
					ASSERT(false); // Should fail with commit_unknown_result
				} catch (Error& e) {
					TraceEvent(SevDebug, "AdvanceVersionCommitFailure").error(e);
					wait(tx->onError(e));
				}
			}
			tx->reset();
		} catch (Error& e) {
			wait(tx->onError(e));
		}
		// profile client get
		loop {
			try {
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				// client_txn_sample_rate
				state Optional<Value> txnSampleRate =
				    wait(tx->get(LiteralStringRef("client_txn_sample_rate")
				                     .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("profile"))));
				ASSERT(txnSampleRate.present());
				Optional<Value> txnSampleRateKey = wait(tx->get(fdbClientInfoTxnSampleRate));
				if (txnSampleRateKey.present()) {
					const double sampleRateDbl =
					    BinaryReader::fromStringRef<double>(txnSampleRateKey.get(), Unversioned());
					if (!std::isinf(sampleRateDbl)) {
						ASSERT(txnSampleRate.get().toString() == boost::lexical_cast<std::string>(sampleRateDbl));
					} else {
						ASSERT(txnSampleRate.get().toString() == "default");
					}
				} else {
					ASSERT(txnSampleRate.get().toString() == "default");
				}
				// client_txn_size_limit
				state Optional<Value> txnSizeLimit =
				    wait(tx->get(LiteralStringRef("client_txn_size_limit")
				                     .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("profile"))));
				ASSERT(txnSizeLimit.present());
				Optional<Value> txnSizeLimitKey = wait(tx->get(fdbClientInfoTxnSizeLimit));
				if (txnSizeLimitKey.present()) {
					const int64_t sizeLimit =
					    BinaryReader::fromStringRef<int64_t>(txnSizeLimitKey.get(), Unversioned());
					if (sizeLimit != -1) {
						ASSERT(txnSizeLimit.get().toString() == boost::lexical_cast<std::string>(sizeLimit));
					} else {
						ASSERT(txnSizeLimit.get().toString() == "default");
					}
				} else {
					ASSERT(txnSizeLimit.get().toString() == "default");
				}
				tx->reset();
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "ProfileClientGet").error(e);
				wait(tx->onError(e));
			}
		}
		{
			state double r_sample_rate = deterministicRandom()->random01();
			state int64_t r_size_limit = deterministicRandom()->randomInt64(1e3, 1e6);
			// update the sample rate and size limit
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set(LiteralStringRef("client_txn_sample_rate")
					            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("profile")),
					        Value(boost::lexical_cast<std::string>(r_sample_rate)));
					tx->set(LiteralStringRef("client_txn_size_limit")
					            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("profile")),
					        Value(boost::lexical_cast<std::string>(r_size_limit)));
					wait(tx->commit());
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
			// commit successfully, verify the system key changed
			loop {
				try {
					tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					Optional<Value> sampleRate = wait(tx->get(fdbClientInfoTxnSampleRate));
					ASSERT(sampleRate.present());
					ASSERT(r_sample_rate == BinaryReader::fromStringRef<double>(sampleRate.get(), Unversioned()));
					Optional<Value> sizeLimit = wait(tx->get(fdbClientInfoTxnSizeLimit));
					ASSERT(sizeLimit.present());
					ASSERT(r_size_limit == BinaryReader::fromStringRef<int64_t>(sizeLimit.get(), Unversioned()));
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
			// Change back to default
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set(LiteralStringRef("client_txn_sample_rate")
					            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("profile")),
					        LiteralStringRef("default"));
					tx->set(LiteralStringRef("client_txn_size_limit")
					            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("profile")),
					        LiteralStringRef("default"));
					wait(tx->commit());
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
			// Test invalid values
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set((deterministicRandom()->coinflip() ? LiteralStringRef("client_txn_sample_rate")
					                                           : LiteralStringRef("client_txn_size_limit"))
					            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("profile")),
					        LiteralStringRef("invalid_value"));
					wait(tx->commit());
					ASSERT(false);
				} catch (Error& e) {
					if (e.code() == error_code_special_keys_api_failure) {
						Optional<Value> errorMsg =
						    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
						ASSERT(errorMsg.present());
						std::string errorStr;
						auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
						auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
						// special_key_space_management_api_error_msg schema validation
						ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
						ASSERT(valueObj["command"].get_str() == "profile" && !valueObj["retriable"].get_bool());
						tx->reset();
						break;
					} else {
						wait(tx->onError(e));
					}
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				}
			}
		}
		// data_distribution & maintenance get
		loop {
			try {
				// maintenance
				RangeResult maintenanceKVs = wait(
				    tx->getRange(SpecialKeySpace::getManagementApiCommandRange("maintenance"), CLIENT_KNOBS->TOO_MANY));
				// By default, no maintenance is going on
				ASSERT(!maintenanceKVs.more && !maintenanceKVs.size());
				// datadistribution
				RangeResult ddKVs = wait(tx->getRange(SpecialKeySpace::getManagementApiCommandRange("datadistribution"),
				                                      CLIENT_KNOBS->TOO_MANY));
				// By default, data_distribution/mode := "-1"
				ASSERT(!ddKVs.more && ddKVs.size() == 1);
				ASSERT(ddKVs[0].key == LiteralStringRef("mode").withPrefix(
				                           SpecialKeySpace::getManagementApiCommandPrefix("datadistribution")));
				ASSERT(ddKVs[0].value == Value(boost::lexical_cast<std::string>(-1)));
				tx->reset();
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "MaintenanceGet").error(e);
				wait(tx->onError(e));
			}
		}
		// maintenance set
		{
			// Make sure setting more than one zone as maintenance will fail
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set(Key(deterministicRandom()->randomAlphaNumeric(8))
					            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("maintenance")),
					        Value(boost::lexical_cast<std::string>(deterministicRandom()->randomInt(1, 100))));
					// make sure this is a different zone id
					tx->set(Key(deterministicRandom()->randomAlphaNumeric(9))
					            .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("maintenance")),
					        Value(boost::lexical_cast<std::string>(deterministicRandom()->randomInt(1, 100))));
					wait(tx->commit());
					ASSERT(false);
				} catch (Error& e) {
					TraceEvent(SevDebug, "MaintenanceSetMoreThanOneZone").error(e);
					if (e.code() == error_code_special_keys_api_failure) {
						Optional<Value> errorMsg =
						    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
						ASSERT(errorMsg.present());
						std::string errorStr;
						auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
						auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
						// special_key_space_management_api_error_msg schema validation
						ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
						ASSERT(valueObj["command"].get_str() == "maintenance" && !valueObj["retriable"].get_bool());
						TraceEvent(SevDebug, "MaintenanceSetMoreThanOneZone")
						    .detail("ErrorMessage", valueObj["message"].get_str());
						tx->reset();
						break;
					} else {
						wait(tx->onError(e));
					}
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				}
			}
			// Disable DD for SS failures
			state int ignoreSSFailuresRetry = 0;
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set(ignoreSSFailuresZoneString.withPrefix(
					            SpecialKeySpace::getManagementApiCommandPrefix("maintenance")),
					        Value(boost::lexical_cast<std::string>(0)));
					wait(tx->commit());
					tx->reset();
					ignoreSSFailuresRetry++;
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				} catch (Error& e) {
					TraceEvent(SevDebug, "MaintenanceDDIgnoreSSFailures").error(e);
					// the second commit will fail since maintenance not allowed to use while DD disabled for SS
					// failures
					if (e.code() == error_code_special_keys_api_failure) {
						Optional<Value> errorMsg =
						    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
						ASSERT(errorMsg.present());
						std::string errorStr;
						auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
						auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
						// special_key_space_management_api_error_msg schema validation
						ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
						ASSERT(valueObj["command"].get_str() == "maintenance" && !valueObj["retriable"].get_bool());
						ASSERT(ignoreSSFailuresRetry > 0);
						TraceEvent(SevDebug, "MaintenanceDDIgnoreSSFailures")
						    .detail("Retry", ignoreSSFailuresRetry)
						    .detail("ErrorMessage", valueObj["message"].get_str());
						tx->reset();
						break;
					} else {
						wait(tx->onError(e));
					}
					ignoreSSFailuresRetry++;
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				}
			}
			// set dd mode to 0 and disable DD for rebalance
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					KeyRef ddPrefix = SpecialKeySpace::getManagementApiCommandPrefix("datadistribution");
					tx->set(LiteralStringRef("mode").withPrefix(ddPrefix), LiteralStringRef("0"));
					tx->set(LiteralStringRef("rebalance_ignored").withPrefix(ddPrefix), Value());
					wait(tx->commit());
					tx->reset();
					break;
				} catch (Error& e) {
					TraceEvent(SevDebug, "DataDistributionDisableModeAndRebalance").error(e);
					wait(tx->onError(e));
				}
			}
			// verify underlying system keys are consistent with the change
			loop {
				try {
					tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					// check DD disabled for SS failures
					Optional<Value> val1 = wait(tx->get(healthyZoneKey));
					ASSERT(val1.present());
					auto healthyZone = decodeHealthyZoneValue(val1.get());
					ASSERT(healthyZone.first == ignoreSSFailuresZoneString);
					// check DD mode
					Optional<Value> val2 = wait(tx->get(dataDistributionModeKey));
					ASSERT(val2.present());
					// mode should be set to 0
					ASSERT(BinaryReader::fromStringRef<int>(val2.get(), Unversioned()) == 0);
					// check DD disabled for rebalance
					Optional<Value> val3 = wait(tx->get(rebalanceDDIgnoreKey));
					// default value "on"
					ASSERT(val3.present() && val3.get() == LiteralStringRef("on"));
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
			// then, clear all changes
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->clear(ignoreSSFailuresZoneString.withPrefix(
					    SpecialKeySpace::getManagementApiCommandPrefix("maintenance")));
					KeyRef ddPrefix = SpecialKeySpace::getManagementApiCommandPrefix("datadistribution");
					tx->clear(LiteralStringRef("mode").withPrefix(ddPrefix));
					tx->clear(LiteralStringRef("rebalance_ignored").withPrefix(ddPrefix));
					wait(tx->commit());
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
			// verify all changes are cleared
			loop {
				try {
					tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					// check DD SSFailures key
					Optional<Value> val1 = wait(tx->get(healthyZoneKey));
					ASSERT(!val1.present());
					// check DD mode
					Optional<Value> val2 = wait(tx->get(dataDistributionModeKey));
					ASSERT(!val2.present());
					// check DD rebalance key
					Optional<Value> val3 = wait(tx->get(rebalanceDDIgnoreKey));
					ASSERT(!val3.present());
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
		}
		// make sure when we change dd related special keys, we grab the two system keys,
		// i.e. moveKeysLockOwnerKey and moveKeysLockWriteKey
		{
			state Reference<ReadYourWritesTransaction> tr1(new ReadYourWritesTransaction(cx));
			state Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(cx));
			loop {
				try {
					tr1->setOption(FDBTransactionOptions::RAW_ACCESS);
					tr1->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tr2->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

					Version readVersion = wait(tr1->getReadVersion());
					tr2->setVersion(readVersion);
					KeyRef ddPrefix = SpecialKeySpace::getManagementApiCommandPrefix("datadistribution");
					tr1->set(LiteralStringRef("mode").withPrefix(ddPrefix), LiteralStringRef("1"));
					wait(tr1->commit());
					// randomly read the moveKeysLockOwnerKey/moveKeysLockWriteKey
					// both of them should be grabbed when changing dd mode
					wait(success(
					    tr2->get(deterministicRandom()->coinflip() ? moveKeysLockOwnerKey : moveKeysLockWriteKey)));
					// tr2 shoulde never succeed, just write to a key to make it not a read-only transaction
					tr2->set(LiteralStringRef("unused_key"), LiteralStringRef(""));
					wait(tr2->commit());
					ASSERT(false); // commit should always fail due to conflict
				} catch (Error& e) {
					if (e.code() != error_code_not_committed) {
						// when buggify is enabled, it's possible we get other retriable errors
						wait(tr2->onError(e));
						tr1->reset();
					} else {
						// loop until we get conflict error
						break;
					}
				}
			}
		}
		return Void();
	}
};

WorkloadFactory<SpecialKeySpaceCorrectnessWorkload> SpecialKeySpaceCorrectnessFactory("SpecialKeySpaceCorrectness");
