/*
 * SpecialKeySpaceCorrectness.actor.cpp
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

#include "boost/lexical_cast.hpp"
#include "boost/algorithm/string.hpp"

#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h"

struct SpecialKeySpaceCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "SpecialKeySpaceCorrectness";

	int actorCount, minKeysPerRange, maxKeysPerRange, rangeCount, keyBytes, valBytes, conflictRangeSizeFactor;
	double testDuration, absoluteRandomProb, transactionsPerSecond;
	PerfIntCounter wrongResults, keysCount;
	Reference<ReadYourWritesTransaction> ryw; // used to store all populated data
	std::vector<std::shared_ptr<SKSCTestRWImpl>> rwImpls;
	std::vector<std::shared_ptr<SKSCTestAsyncReadImpl>> asyncReadImpls;
	Standalone<VectorRef<KeyRangeRef>> keys;
	Standalone<VectorRef<KeyRangeRef>> rwKeys;

	SpecialKeySpaceCorrectnessWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), wrongResults("Wrong Results"), keysCount("Number of generated keys") {
		minKeysPerRange = getOption(options, "minKeysPerRange"_sr, 1);
		maxKeysPerRange = getOption(options, "maxKeysPerRange"_sr, 100);
		rangeCount = getOption(options, "rangeCount"_sr, 10);
		keyBytes = getOption(options, "keyBytes"_sr, 16);
		valBytes = getOption(options, "valueBytes"_sr, 16);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 100.0);
		actorCount = getOption(options, "actorCount"_sr, 1);
		absoluteRandomProb = getOption(options, "absoluteRandomProb"_sr, 0.5);
		// Controls the relative size of read/write conflict ranges and the number of random getranges
		conflictRangeSizeFactor = getOption(options, "conflictRangeSizeFactor"_sr, 10);
		ASSERT(conflictRangeSizeFactor >= 1);
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	Future<bool> check(Database const& cx) override { return wrongResults.getValue() == 0; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
	// disable the default timeout setting
	double getCheckTimeout() const override { return std::numeric_limits<double>::max(); }

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		// Failure injection workloads like Rollback, Attrition and so on are interfering with the test.
		// In particular, the test aims to test special keys' functions on monitoring and managing the cluster.
		// It expects the FDB cluster is healthy and not doing unexpected configuration changes.
		// All changes should come from special keys' operations' outcome.
		// Consequently, we disable all failure injection workloads in background for this test
		out.insert("all");
	}

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
			if (deterministicRandom()->random01() < 0.2 && !self->rwImpls.empty()) {
				self->asyncReadImpls.push_back(std::make_shared<SKSCTestAsyncReadImpl>(KeyRangeRef(startKey, endKey)));
				cx->specialKeySpace->registerKeyRange(SpecialKeySpace::MODULE::TESTONLY,
				                                      SpecialKeySpace::IMPLTYPE::READONLY,
				                                      self->keys.back(),
				                                      self->asyncReadImpls.back().get());
			} else {
				self->rwImpls.push_back(std::make_shared<SKSCTestRWImpl>(KeyRangeRef(startKey, endKey)));
				// Although there are already ranges registered, the testing range will replace them
				cx->specialKeySpace->registerKeyRange(SpecialKeySpace::MODULE::TESTONLY,
				                                      SpecialKeySpace::IMPLTYPE::READWRITE,
				                                      self->keys.back(),
				                                      self->rwImpls.back().get());
			}
			// generate keys in each key range
			int keysInRange = deterministicRandom()->randomInt(self->minKeysPerRange, self->maxKeysPerRange + 1);
			self->keysCount += keysInRange;
			for (int j = 0; j < keysInRange; ++j) {
				self->ryw->set(Key(deterministicRandom()->randomAlphaNumeric(self->keyBytes)).withPrefix(startKey),
				               Value(deterministicRandom()->randomAlphaNumeric(self->valBytes)));
			}
		}
		ASSERT(rwImpls.size() > 0);

		return Void();
	}
	ACTOR Future<Void> _start(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		testRywLifetime(cx);
		wait(timeout(self->testSpecialKeySpaceErrors(cx, self) && self->getRangeCallActor(cx, self) &&
		                 testConflictRanges(cx, /*read*/ true, self) && testConflictRanges(cx, /*read*/ false, self) &&
		                 self->metricsApiCorrectnessActor(cx, self),
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
			f = success(ryw.get("\xff\xff/status/json"_sr));
			CODE_PROBE(!f.isReady(), "status json not ready");
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
			KeyRange rkr = self->randomRWKeyRange();
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
			CODE_PROBE(true, "Special key space keys equal");
		}
		return true;
	}

	KeyRange randomRWKeyRange() {
		ASSERT(rwImpls.size() > 0);
		Key prefix = rwImpls[deterministicRandom()->randomInt(0, rwImpls.size())]->getKeyRange().begin;
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
		int rowLimits = deterministicRandom()->randomInt(0, keysCount.getValue() + 1);
		// The largest key's bytes is longest prefix bytes + 1(for '/') + generated key bytes
		// 8 here refers to bytes of KeyValueRef
		int byteLimits = deterministicRandom()->randomInt(
		    1, keysCount.getValue() * (keyBytes + (rangeCount + 1) + valBytes + 8) + 1);

		auto limit = GetRangeLimits(rowLimits, byteLimits);
		// minRows is always initialized to 1
		if (limit.rows == 0)
			limit.minRows = 0;
		return limit;
	}

	ACTOR Future<Void> testSpecialKeySpaceErrors(Database cx_, SpecialKeySpaceCorrectnessWorkload* self) {
		state Database cx = cx_->clone();
		state int64_t tenantId;
		try {
			Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), TenantName("foo"_sr)));
			ASSERT(entry.present());
			tenantId = entry.get().id;
		} catch (Error& e) {
			ASSERT(e.code() == error_code_tenant_already_exists || e.code() == error_code_actor_cancelled);
		}
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
		state Reference<ReadYourWritesTransaction> tenantTx =
		    makeReference<ReadYourWritesTransaction>(cx, makeReference<Tenant>(tenantId));
		// Use new transactions that may use default tenant rather than re-use tx
		// This is because tx will reject raw access for later tests if default tenant is set
		state Reference<ReadYourWritesTransaction> defaultTx1 = makeReference<ReadYourWritesTransaction>(cx);
		state Reference<ReadYourWritesTransaction> defaultTx2 = makeReference<ReadYourWritesTransaction>(cx);
		state bool disableRyw = deterministicRandom()->coinflip();
		// tenant transaction accessing modules that do not support tenants
		// tenant getRange
		try {
			wait(success(tenantTx->getRange(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT),
			                                CLIENT_KNOBS->TOO_MANY)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_illegal_tenant_access);
			tenantTx->reset();
		}
		// tenant set + commit
		try {
			tenantTx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tenantTx->set(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"), ValueRef());
			wait(tenantTx->commit());
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_illegal_tenant_access);
			tenantTx->reset();
		}
		// tenant clear
		try {
			tenantTx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tenantTx->clear(SpecialKeySpace::getManagementApiCommandRange("exclude"));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			ASSERT(e.code() == error_code_illegal_tenant_access);
			tenantTx->reset();
		}
		// tenant check that conflict ranges stay the same after commit
		// and depending on if RYW is disabled
		{
			state RangeResult readresult1;
			state RangeResult readresult2;
			state RangeResult writeResult1;
			state RangeResult writeResult2;
			try {
				if (disableRyw) {
					defaultTx1->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
				}
				defaultTx1->addReadConflictRange(singleKeyRange("testKeylll"_sr));
				defaultTx1->addWriteConflictRange(singleKeyRange("testKeylll"_sr));
				wait(store(readresult1, defaultTx1->getRange(readConflictRangeKeysRange, CLIENT_KNOBS->TOO_MANY)));
				wait(store(writeResult1, defaultTx1->getRange(writeConflictRangeKeysRange, CLIENT_KNOBS->TOO_MANY)));
				wait(defaultTx1->commit());
				CODE_PROBE(true, "conflict range tenant commit succeeded");
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				CODE_PROBE(true, "conflict range tenant commit error thrown");
			}
			wait(store(readresult2, defaultTx1->getRange(readConflictRangeKeysRange, CLIENT_KNOBS->TOO_MANY)));
			wait(store(writeResult2, defaultTx1->getRange(writeConflictRangeKeysRange, CLIENT_KNOBS->TOO_MANY)));
			ASSERT(readresult1 == readresult2);
			ASSERT(writeResult1 == writeResult2);
			defaultTx1->reset();
		}
		// proper conflict ranges
		loop {
			try {
				if (disableRyw) {
					defaultTx1->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					defaultTx2->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
				}
				defaultTx1->setOption(FDBTransactionOptions::REPORT_CONFLICTING_KEYS);
				defaultTx2->setOption(FDBTransactionOptions::REPORT_CONFLICTING_KEYS);
				wait(success(defaultTx1->getReadVersion()));
				wait(success(defaultTx2->getReadVersion()));
				defaultTx1->addReadConflictRange(singleKeyRange("foo"_sr));
				defaultTx1->addWriteConflictRange(singleKeyRange("foo"_sr));
				defaultTx2->addWriteConflictRange(singleKeyRange("foo"_sr));
				wait(defaultTx2->commit());
				try {
					wait(defaultTx1->commit());
					ASSERT(false);
				} catch (Error& e) {
					state Error err = e;
					if (err.code() != error_code_not_committed) {
						wait(defaultTx1->onError(err));
						wait(defaultTx2->onError(err));
						continue;
					}
					// Read conflict ranges of defaultTx1 and check for "foo" with no tenant prefix
					state RangeResult readConflictRange =
					    wait(defaultTx1->getRange(readConflictRangeKeysRange, CLIENT_KNOBS->TOO_MANY));
					state RangeResult writeConflictRange =
					    wait(defaultTx1->getRange(writeConflictRangeKeysRange, CLIENT_KNOBS->TOO_MANY));
					state RangeResult conflictKeys =
					    wait(defaultTx1->getRange(conflictingKeysRange, CLIENT_KNOBS->TOO_MANY));

					// size is 2 because singleKeyRange includes the key after
					ASSERT(readConflictRange.size() == 2 &&
					       readConflictRange.begin()->key == readConflictRangeKeysRange.begin.withSuffix("foo"_sr));
					ASSERT(writeConflictRange.size() == 2 &&
					       writeConflictRange.begin()->key == writeConflictRangeKeysRange.begin.withSuffix("foo"_sr));
					ASSERT(conflictKeys.size() == 2 &&
					       conflictKeys.begin()->key == conflictingKeysRange.begin.withSuffix("foo"_sr));
					defaultTx1->reset();
					defaultTx2->reset();
					break;
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				wait(defaultTx2->onError(e));
			}
		}
		// begin key outside module range
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			wait(success(tx->getRange(KeyRangeRef("\xff\xff/transactio"_sr, "\xff\xff/transaction0"_sr),
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
			wait(success(tx->getRange(KeyRangeRef("\xff\xff/transaction/"_sr, "\xff\xff/transaction1"_sr),
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
			wait(success(tx->getRange(KeyRangeRef("\xff\xff/transaction"_sr, "\xff\xff/transaction1"_sr),
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
			wait(success(tx->getRange(KeyRangeRef("\xff\xff/transaction/"_sr, "\xff\xff/transaction0"_sr),
			                          CLIENT_KNOBS->TOO_MANY)));
			CODE_PROBE(true, "read transaction special keyrange");
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// cross module read with option turned on
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED);
			const KeyRef startKey = "\xff\xff/transactio"_sr;
			const KeyRef endKey = "\xff\xff/transaction1"_sr;
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
			tx->addReadConflictRange(singleKeyRange("testKey"_sr));
			KeySelector begin = KeySelectorRef(readConflictRangeKeysRange.begin, false, 1);
			KeySelector end = KeySelectorRef("\xff\xff/transaction0"_sr, false, 0);
			wait(success(tx->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY))));
			CODE_PROBE(true, "end key selector inside module range");
			tx->reset();
		} catch (Error& e) {
			throw;
		}
		// No module found error case with keys
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			wait(success(tx->getRange(
			    KeyRangeRef("\xff\xff/A_no_module_related_prefix"_sr, "\xff\xff/I_am_also_not_in_any_module"_sr),
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
			KeySelector begin = KeySelectorRef("\xff\xff/zzz_i_am_not_a_module"_sr, false, 1);
			KeySelector end = KeySelectorRef("\xff\xff/zzz_to_be_the_final_one"_sr, false, 2);
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
			const KeyRef key = "\xff\xff/cluster_file_path"_sr;
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
			tx->addReadConflictRange(singleKeyRange("readKey"_sr));
			const KeyRef key = "\xff\xff/transaction/a_to_be_the_first"_sr;
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
			tx->set("\xff\xff/I_am_not_a_range_can_be_written"_sr, ValueRef());
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
			tx->set("\xff\xff/I_am_not_a_range_can_be_written"_sr, ValueRef());
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
			const KeySelector startKeySelector = KeySelectorRef("\xff\xff/test"_sr, true, -200);
			const KeySelector endKeySelector = KeySelectorRef("test"_sr, true, -10);
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
			state RangeResult result =
			    wait(tx->getRange(KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr),
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
		CODE_PROBE(read, "test read conflict range special key implementation");
		CODE_PROBE(!read, "test write conflict range special key implementation");
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
			referenceTx->set(range.begin, "1"_sr);
			referenceTx->set(range.end, "0"_sr);
		}
		if (!read && deterministicRandom()->coinflip()) {
			try {
				wait(tx->commit());
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				return Void();
			}
			CODE_PROBE(true, "Read write conflict range of committed transaction");
		}
		try {
			wait(success(tx->get("\xff\xff/1314109/i_hope_this_isn't_registered"_sr)));
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
		// All management api related tests that cannot run with failure injections
		state Database cx = cx_->clone();
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
		// test change coordinators and cluster description
		// we randomly pick one process(not coordinator) and add it, in this case, it should always succeed
		{
			state std::string new_cluster_description;
			state std::string new_coordinator_process;
			state std::vector<std::string> old_coordinators_processes;
			state bool possible_to_add_coordinator;
			state KeyRange coordinators_key_range =
			    KeyRangeRef("process/"_sr, "process0"_sr)
			        .withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators"));
			state unsigned retries = 0;
			state bool changeCoordinatorsSucceeded = true;
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
					Optional<Value> processes_key = wait(tx->get(
					    "processes"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators"))));
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
						tx->set(
						    "processes"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
						    Value(new_processes_key));
						// update cluster description
						tx->set("cluster_description"_sr.withPrefix(
						            SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
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
								if (++retries >= 10) {
									CODE_PROBE(
									    true, "ChangeCoordinators Exceeded retry limit", probe::decoration::rare);
									changeCoordinatorsSucceeded = false;
									tx->reset();
									break;
								}
								tx->reset();
							} else {
								ASSERT(valueObj["message"].get_str() ==
								       "No change (existing configuration satisfies request)");
								tx->reset();
								CODE_PROBE(true, "Successfully changed coordinators");
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
					ClusterConnectionString csNew(res.get().toString());
					// verify the cluster description
					ASSERT(!changeCoordinatorsSucceeded ||
					       new_cluster_description == csNew.clusterKeyName().toString());
					ASSERT(!changeCoordinatorsSucceeded ||
					       csNew.hostnames.size() + csNew.coords.size() == old_coordinators_processes.size() + 1);
					std::vector<NetworkAddress> newCoordinators = wait(csNew.tryResolveHostnames());
					// verify the coordinators' addresses
					for (const auto& network_address : newCoordinators) {
						std::string address_str = network_address.toString();
						ASSERT(std::find(old_coordinators_processes.begin(),
						                 old_coordinators_processes.end(),
						                 address_str) != old_coordinators_processes.end() ||
						       new_coordinator_process == address_str);
					}
					tx->reset();
				} catch (Error& e) {
					wait(tx->onError(e));
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				}
				// change back to original settings
				while (changeCoordinatorsSucceeded) {
					try {
						std::string new_processes_key;
						tx->setOption(FDBTransactionOptions::RAW_ACCESS);
						tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
						for (const auto& address : old_coordinators_processes) {
							new_processes_key += new_processes_key.size() ? "," : "";
							new_processes_key += address;
						}
						tx->set(
						    "processes"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
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
				ASSERT(ddKVs[0].key ==
				       "mode"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("datadistribution")));
				TraceEvent("DDKVsValue").detail("Value", ddKVs[0].value);
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
			state uint8_t ddIgnoreValue = DDIgnore::NONE;
			if (deterministicRandom()->coinflip()) {
				ddIgnoreValue |= DDIgnore::REBALANCE_READ;
			}
			if (deterministicRandom()->coinflip()) {
				ddIgnoreValue |= DDIgnore::REBALANCE_DISK;
			}
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					KeyRef ddPrefix = SpecialKeySpace::getManagementApiCommandPrefix("datadistribution");
					tx->set("mode"_sr.withPrefix(ddPrefix), "0"_sr);
					tx->set("rebalance_ignored"_sr.withPrefix(ddPrefix),
					        BinaryWriter::toValue(ddIgnoreValue, Unversioned()));
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
					ASSERT(val3.present() &&
					       BinaryReader::fromStringRef<uint8_t>(val3.get(), Unversioned()) == ddIgnoreValue);
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
					tx->clear("mode"_sr.withPrefix(ddPrefix));
					tx->clear("rebalance_ignored"_sr.withPrefix(ddPrefix));
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
		return Void();
	}

	ACTOR Future<Void> metricsApiCorrectnessActor(Database cx_, SpecialKeySpaceCorrectnessWorkload* self) {
		state Database cx = cx_->clone();
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
		tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		{
			Optional<Value> metrics = wait(tx->get("fault_tolerance_metrics_json"_sr.withPrefix(
			    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::METRICS).begin)));
			ASSERT(metrics.present());
			auto metricsObj = readJSONStrictly(metrics.get().toString()).get_obj();
			auto schema = readJSONStrictly(JSONSchemas::faultToleranceStatusSchema.toString()).get_obj();
			std::string errorStr;
			ASSERT(schemaMatch(schema, metricsObj, errorStr, SevError, true));
		}
		return Void();
	}
};

WorkloadFactory<SpecialKeySpaceCorrectnessWorkload> SpecialKeySpaceCorrectnessFactory;
