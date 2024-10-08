/*
 * FuzzApiCorrectness.actor.cpp
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

#include <limits.h>
#include <mutex>
#include <functional>
#include <sstream>

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "flow/ActorCollection.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace ph = std::placeholders;

// This allows us to dictate which exceptions we SHOULD get.
// We can use this to suppress expected exceptions, and take action
// if we don't get an exception wqe should have gotten.
struct ExceptionContract {
	enum occurance_t { Never = 0, Possible = 1, Always = 2 };

	std::string func;
	std::map<int, occurance_t> expected;
	std::function<void(TraceEvent&)> augment;

	ExceptionContract(const char* func_, const std::function<void(TraceEvent&)>& augment_)
	  : func(func_), augment(augment_) {}
	ExceptionContract& operator=(const std::map<int, occurance_t>& e) {
		expected = e;
		return *this;
	}

	static occurance_t possibleButRequiredIf(bool in) { return in ? Always : Possible; }
	static occurance_t requiredIf(bool in) { return in ? Always : Never; }
	static occurance_t possibleIf(bool in) { return in ? Possible : Never; }

	void handleException(const Error& e, Reference<ITransaction> tr) const {
		// We should always ignore these.
		if (e.code() == error_code_used_during_commit || e.code() == error_code_transaction_too_old ||
		    e.code() == error_code_future_version || e.code() == error_code_transaction_cancelled ||
		    e.code() == error_code_key_too_large || e.code() == error_code_value_too_large ||
		    e.code() == error_code_process_behind || e.code() == error_code_batch_transaction_throttled ||
		    e.code() == error_code_tag_throttled || e.code() == error_code_grv_proxy_memory_limit_exceeded) {
			return;
		}

		auto i = expected.find(e.code());
		if (i != expected.end() && i->second != Never) {
			Severity s = (i->second == Possible) ? SevWarn : SevInfo;
			TraceEvent evt(s, func.c_str());
			evt.error(e)
			    .detail("Thrown", true)
			    .detail("Expected", i->second == Possible ? "possible" : "always")
			    .detail("Tenant", tr->getTenant())
			    .backtrace();
			if (augment)
				augment(evt);
			return;
		}

		TraceEvent evt(SevError, func.c_str());
		evt.error(e).detail("Thrown", true).detail("Expected", "never").detail("Tenant", tr->getTenant()).backtrace();
		if (augment)
			augment(evt);
		throw e;
	}

	// Return true if we should have thrown, but didn't.
	void handleNotThrown(Reference<ITransaction> tr) const {
		for (auto i : expected) {
			if (i.second == Always) {
				TraceEvent evt(SevError, func.c_str());
				evt.error(Error::fromUnvalidatedCode(i.first))
				    .detail("Thrown", false)
				    .detail("Expected", "always")
				    .detail("Tenant", tr->getTenant())
				    .backtrace();
				if (augment)
					augment(evt);
			}
		}
	}
};

struct FuzzApiCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "FuzzApiCorrectness";
	static std::once_flag onceFlag;
	static std::vector<std::function<
	    Future<Void>(unsigned int const&, FuzzApiCorrectnessWorkload* const&, Reference<ITransaction> const&)>>
	    testCases;

	double testDuration;
	int numOps;
	bool rarelyCommit, adjacentKeys;
	int minNode, nodes;
	std::pair<int, int> valueSizeRange;
	int maxClearSize;
	double initialKeyDensity;
	bool useSystemKeys;
	bool writeSystemKeys = false; // whether we really write to a system key in the workload
	KeyRange conflictRange;
	unsigned int operationId;
	int64_t maximumTotalData;
	bool specialKeysRelaxed;
	bool specialKeysWritesEnabled;

	bool success;

	Reference<IDatabase> db;

	std::vector<Reference<ITenant>> tenants;
	std::set<TenantName> createdTenants;
	int numTenants;
	int numTenantGroups;
	int minTenantNum = -1;

	bool illegalTenantAccess = false;

	// Map from tenant number to key prefix
	std::map<int, std::string> keyPrefixes;

	FuzzApiCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), operationId(0), success(true) {
		std::call_once(onceFlag, [&]() { addTestCases(); });

		testDuration = getOption(options, "testDuration"_sr, 60.0);
		numOps = getOption(options, "numOps"_sr, 21);
		rarelyCommit = getOption(options, "rarelyCommit"_sr, false);
		maximumTotalData = getOption(options, "maximumTotalData"_sr, 15e6);
		minNode = getOption(options, "minNode"_sr, 0);
		adjacentKeys = deterministicRandom()->coinflip();
		useSystemKeys = deterministicRandom()->coinflip();
		initialKeyDensity = deterministicRandom()->random01(); // This fraction of keys are present before the first
		                                                       // transaction (and after an unknown result)
		specialKeysRelaxed = deterministicRandom()->coinflip();
		// Only enable special keys writes when allowed to access system keys
		specialKeysWritesEnabled = useSystemKeys && deterministicRandom()->coinflip();

		int maxTenants = getOption(options, "numTenants"_sr, 4);
		numTenants = deterministicRandom()->randomInt(0, maxTenants + 1);

		int maxTenantGroups = getOption(options, "numTenantGroups"_sr, numTenants);
		numTenantGroups = deterministicRandom()->randomInt(0, maxTenantGroups + 1);

		// See https://github.com/apple/foundationdb/issues/2424
		if (BUGGIFY) {
			enableClientBuggify();
		}

		if (adjacentKeys) {
			nodes = std::min<int64_t>(deterministicRandom()->randomInt(1, 4 << deterministicRandom()->randomInt(0, 14)),
			                          CLIENT_KNOBS->KEY_SIZE_LIMIT * 1.2);
		} else {
			nodes = deterministicRandom()->randomInt(1, 4 << deterministicRandom()->randomInt(0, 20));
		}

		int newNodes =
		    std::min<int>(nodes, maximumTotalData / (getKeyForIndex(-1, nodes).size() + valueSizeRange.second));
		minNode = std::max(minNode, nodes - newNodes);
		nodes = newNodes;

		if (useSystemKeys && deterministicRandom()->coinflip()) {
			keyPrefixes[-1] = "\xff\x01";
			writeSystemKeys = true;
		}

		maxClearSize = 1 << deterministicRandom()->randomInt(0, 20);
		conflictRange = KeyRangeRef("\xfe"_sr, "\xfe\x00"_sr);
		TraceEvent("FuzzApiCorrectnessConfiguration")
		    .detail("Nodes", nodes)
		    .detail("NumTenants", numTenants)
		    .detail("InitialKeyDensity", initialKeyDensity)
		    .detail("AdjacentKeys", adjacentKeys)
		    .detail("ValueSizeMin", valueSizeRange.first)
		    .detail("ValueSizeRange", valueSizeRange.second)
		    .detail("MaxClearSize", maxClearSize)
		    .detail("UseSystemKeys", useSystemKeys)
		    .detail("SpecialKeysRelaxed", specialKeysRelaxed)
		    .detail("SpecialKeysWritesEnabled", specialKeysWritesEnabled);

		TraceEvent("RemapEventSeverity")
		    .detail("TargetEvent", "LargePacketSent")
		    .detail("OriginalSeverity", SevWarnAlways)
		    .detail("NewSeverity", SevInfo);
		TraceEvent("RemapEventSeverity")
		    .detail("TargetEvent", "LargePacketReceived")
		    .detail("OriginalSeverity", SevWarnAlways)
		    .detail("NewSeverity", SevInfo);
		TraceEvent("RemapEventSeverity")
		    .detail("TargetEvent", "LargeTransaction")
		    .detail("OriginalSeverity", SevWarnAlways)
		    .detail("NewSeverity", SevInfo);
	}

	static TenantName getTenant(int num) { return TenantNameRef(format("tenant_%d", num)); }
	Optional<TenantGroupName> getTenantGroup(int num) {
		int groupNum = num % (numTenantGroups + 1);
		if (groupNum == numTenantGroups - 1) {
			return Optional<TenantGroupName>();
		} else {
			return TenantGroupNameRef(format("tenantgroup_%d", groupNum));
		}
	}
	bool canUseTenant(Optional<TenantName> tenant) { return !tenant.present() || createdTenants.count(tenant.get()); }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		}

		return Void();
	}

	ACTOR Future<Void> _setup(Database cx, FuzzApiCorrectnessWorkload* self) {
		Reference<IDatabase> db = wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));
		self->db = db;

		std::vector<Future<Void>> tenantFutures;
		// The last tenant will not be created
		for (int i = 0; i < self->numTenants; ++i) {
			TenantName tenantName = getTenant(i);
			TenantMapEntry entry;
			entry.tenantGroup = self->getTenantGroup(i);
			tenantFutures.push_back(::success(TenantAPI::createTenant(cx.getReference(), tenantName, entry)));
			self->createdTenants.insert(tenantName);
		}
		wait(waitForAll(tenantFutures));

		// Open one extra tenant to test the failure of using a tenant that doesn't exist
		for (int i = 0; i < self->numTenants + 1; ++i) {
			TenantName tenantName = getTenant(i);
			self->tenants.push_back(self->db->openTenant(tenantName));
		}

		// When domain-aware encryption is enabled, writing random keys without specifying tenant may cause Redwood to
		// create too many pages.
		DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		if (config.encryptionAtRestMode.mode == EncryptionAtRestMode::DOMAIN_AWARE) {
			self->minTenantNum = 0;
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return loadAndRun(this, cx);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (!writeSystemKeys) { // there must be illegal access during data load
			return illegalTenantAccess;
		}
		return success;
	}

	Key getKeyForIndex(int tenantNum, int idx) {
		idx += minNode;
		if (adjacentKeys) {
			return Key(keyPrefixes[tenantNum] + std::string(idx, '\x00'));
		} else {
			return Key(keyPrefixes[tenantNum] + format("%010d", idx));
		}
	}

	KeyRef getMaxKey(Reference<ITransaction> tr) const {
		if (useSystemKeys && !tr->getTenant().present()) {
			return systemKeys.end;
		} else {
			return normalKeys.end;
		}
	}

	Value getRandomValue() const {
		return Value(
		    std::string(deterministicRandom()->randomInt(valueSizeRange.first, valueSizeRange.second + 1), 'x'));
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		// m.push_back( transactions.getMetric() );
		// m.push_back( retries.getMetric() );
	}

	// Prevent a write only transaction whose commit was previously cancelled from being reordered after this
	// transaction
	ACTOR Future<Void> writeBarrier(Reference<IDatabase> db) {
		state Reference<ITransaction> tr = db->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				// Write-only transactions have a self-conflict in the system keys
				tr->addWriteConflictRange(allKeys);
				tr->clear(normalKeys);
				wait(unsafeThreadFutureToFuture(tr->commit()));
				return Void();
			} catch (Error& e) {
				wait(unsafeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR Future<Void> loadAndRun(FuzzApiCorrectnessWorkload* self, Database cx) {
		state double startTime = now();
		state int nodesPerTenant = std::max<int>(1, self->nodes / (self->numTenants + 1));
		state int keysPerBatch =
		    std::min<int64_t>(1000,
		                      1 + CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT / 2 /
		                              (self->getKeyForIndex(-1, nodesPerTenant).size() + self->valueSizeRange.second));
		try {
			loop {
				state int tenantNum = self->minTenantNum;
				for (; tenantNum < self->numTenants; ++tenantNum) {
					state int i = 0;
					wait(self->writeBarrier(self->db));
					for (; i < nodesPerTenant; i += keysPerBatch) {
						state Reference<ITransaction> tr = tenantNum < 0
						                                       ? self->db->createTransaction()
						                                       : self->tenants[tenantNum]->createTransaction();
						loop {
							if (now() - startTime > self->testDuration)
								return Void();
							try {
								if (self->useSystemKeys && tenantNum == -1) {
									tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
								}
								if (self->specialKeysRelaxed)
									tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED);
								if (self->specialKeysWritesEnabled)
									tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);

								if (i == 0) {
									tr->clear(normalKeys);
								}

								int end = std::min(nodesPerTenant, i + keysPerBatch);
								tr->clear(KeyRangeRef(self->getKeyForIndex(tenantNum, i),
								                      self->getKeyForIndex(tenantNum, end)));

								for (int j = i; j < end; j++) {
									if (deterministicRandom()->random01() < self->initialKeyDensity) {
										Key key = self->getKeyForIndex(tenantNum, j);
										if (key.size() <= getMaxWriteKeySize(key, false)) {
											Value value = self->getRandomValue();
											value = value.substr(
											    0, std::min<int>(value.size(), CLIENT_KNOBS->VALUE_SIZE_LIMIT));
											tr->set(key, value);
										}
									}
								}
								wait(unsafeThreadFutureToFuture(tr->commit()));
								//TraceEvent("WDRInitBatch").detail("I", i).detail("CommittedVersion", tr->getCommittedVersion());
								break;
							} catch (Error& e) {
								if (e.code() == error_code_illegal_tenant_access) {
									ASSERT(!self->writeSystemKeys);
									ASSERT_EQ(tenantNum, -1);
									self->illegalTenantAccess = true;
									break;
								}
								wait(unsafeThreadFutureToFuture(tr->onError(e)));
							}
						}

						if (self->illegalTenantAccess) {
							// no need to do the non-system writes
							break;
						}
					}
				}

				loop {
					try {
						wait(self->randomTransaction(self, cx) && delay(self->numOps * .001));
					} catch (Error& e) {
						if (e.code() != error_code_not_committed)
							throw e;
						break;
					}
					if (now() - startTime > self->testDuration)
						return Void();
				}
			}
		} catch (Error& e) {
			TraceEvent("FuzzLoadAndRunError").error(e).backtrace();
			throw e;
		}
	}

	ACTOR Future<Void> randomTransaction(FuzzApiCorrectnessWorkload* self, Database cx) {
		state Reference<ITransaction> tr;
		state bool readYourWritesDisabled = deterministicRandom()->coinflip();
		state bool readAheadDisabled = deterministicRandom()->coinflip();
		state std::vector<Future<Void>> operations;
		state int waitLocation = 0;

		state int tenantNum = deterministicRandom()->randomInt(self->minTenantNum, self->tenants.size());
		if (tenantNum == -1) {
			tr = self->db->createTransaction();
		} else {
			tr = self->tenants[tenantNum]->createTransaction();
		}

		state bool rawAccess = tenantNum == -1 && deterministicRandom()->coinflip();

		loop {
			state bool cancelled = false;
			if (readYourWritesDisabled)
				tr->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
			if (readAheadDisabled)
				tr->setOption(FDBTransactionOptions::READ_AHEAD_DISABLE);
			if (self->useSystemKeys && tenantNum == -1) {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			}
			if (self->specialKeysRelaxed) {
				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED);
			}
			if (self->specialKeysWritesEnabled) {
				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			}
			if (rawAccess) {
				tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			}
			tr->addWriteConflictRange(self->conflictRange);

			try {
				state int numWaits = deterministicRandom()->randomInt(1, 5);
				state int i = 0;

				// Code path here will create a bunch of ops, wait for ONE of them to complete,
				// then add more ops, wait for another one, and so on.
				for (; i < numWaits; i++) {
					state int numOps = deterministicRandom()->randomInt(1, self->numOps);
					state int j = 0;
					for (; j < numOps; j++) {
						state int operationType = deterministicRandom()->randomInt(0, testCases.size());
						printf("%d: Selected Operation %d\n", self->operationId + 1, operationType);
						try {
							operations.push_back(testCases[operationType](++self->operationId, self, tr));
						} catch (Error& e) {
							TraceEvent(SevWarn, "IgnoredOperation")
							    .error(e)
							    .detail("Operation", operationType)
							    .detail("Id", self->operationId);
						}
					}

					// Wait for a random op to complete.
					if (waitLocation < operations.size()) {
						int waitOp = deterministicRandom()->randomInt(waitLocation, operations.size());
						wait(operations[waitOp]);
						wait(delay(0.000001)); // to ensure errors have propagated from reads to commits
						waitLocation = operations.size();
					}
				}
				wait(waitForAll(operations));
				try {
					wait(timeoutError(unsafeThreadFutureToFuture(tr->commit()), 30));
				} catch (Error& e) {
					if (e.code() == error_code_client_invalid_operation ||
					    e.code() == error_code_transaction_too_large || e.code() == error_code_invalid_option) {
						throw not_committed();
					}
				}
				break;
			} catch (Error& e) {
				operations.clear();
				waitLocation = 0;

				if (e.code() == error_code_not_committed || e.code() == error_code_commit_unknown_result || cancelled) {
					throw not_committed();
				} else if (e.code() == error_code_illegal_tenant_access) {
					ASSERT_EQ(tenantNum, -1);
					ASSERT_EQ(cx->getTenantMode(), TenantMode::REQUIRED);
					return Void();
				}

				try {
					wait(unsafeThreadFutureToFuture(tr->onError(e)));
				} catch (Error& e) {
					if (e.code() == error_code_transaction_timed_out) {
						throw not_committed();
					}
					throw e;
				}
			}
		}
		return Void();
	}

	template <typename Subclass, typename T>
	struct BaseTest {
		typedef T value_type;

		ACTOR static Future<Void> runTest(unsigned int id, FuzzApiCorrectnessWorkload* wl, Reference<ITransaction> tr) {
			state Subclass self(id, wl, tr);

			try {
				value_type result = wait(timeoutError(BaseTest::runTest2(tr, &self), 1000));
				self.contract.handleNotThrown(tr);
				return self.errorCheck(tr, result);
			} catch (Error& e) {
				self.contract.handleException(e, tr);
			}
			return Void();
		}

		ACTOR static Future<value_type> runTest2(Reference<ITransaction> tr, Subclass* self) {
			state Future<value_type> future = unsafeThreadFutureToFuture(self->createFuture(tr));

			// Create may have added some other things to do before
			// we can get the result.
			state std::vector<ThreadFuture<Void>>::iterator i;
			for (i = self->pre_steps.begin(); i != self->pre_steps.end(); ++i) {
				wait(unsafeThreadFutureToFuture(*i));
			}

			value_type result = wait(future);
			if (future.isError()) {
				self->contract.handleException(future.getError(), tr);
			} else {
				ASSERT(future.isValid());
			}

			return result;
		}

		virtual ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) = 0;
		virtual Void errorCheck(Reference<ITransaction> tr, value_type result) { return Void(); }
		virtual void augmentTrace(TraceEvent& e) const { e.detail("Id", id); }

	protected:
		unsigned int id;
		FuzzApiCorrectnessWorkload* workload;
		ExceptionContract contract;
		std::vector<ThreadFuture<Void>> pre_steps;

		BaseTest(unsigned int id_, FuzzApiCorrectnessWorkload* wl, const char* func)
		  : id(id_), workload(wl), contract(func, std::bind(&BaseTest::augmentTrace, this, ph::_1)) {}

		static Key makeKey() {
			double ksrv = deterministicRandom()->random01();

			// 25% of the time it's empty, 25% it's above range.
			if (ksrv < 0.25)
				return Key();

			int64_t key_size;
			if (ksrv < 0.5)
				key_size = deterministicRandom()->randomInt64(1, CLIENT_KNOBS->KEY_SIZE_LIMIT + 1) +
				           CLIENT_KNOBS->KEY_SIZE_LIMIT;
			else
				key_size = deterministicRandom()->randomInt64(1, CLIENT_KNOBS->KEY_SIZE_LIMIT + 1);

			std::string skey;
			skey.reserve(key_size);
			for (size_t j = 0; j < key_size; ++j)
				skey.append(1, (char)deterministicRandom()->randomInt(0, 256));

			// 15% (= 20% * 75%) of the time generating keys after \xff\xff to test special keys code
			if (deterministicRandom()->random01() < 0.2)
				return Key(skey).withPrefix(specialKeys.begin);
			else
				return Key(skey);
		}

		static Value makeValue() {
			double vrv = deterministicRandom()->random01();

			// 25% of the time it's empty, 25% it's above range.
			if (vrv < 0.25)
				return Value();

			int64_t value_size;
			if (vrv < 0.5)
				value_size = deterministicRandom()->randomInt64(1, CLIENT_KNOBS->VALUE_SIZE_LIMIT + 1) +
				             CLIENT_KNOBS->VALUE_SIZE_LIMIT;
			else
				value_size = deterministicRandom()->randomInt64(1, CLIENT_KNOBS->VALUE_SIZE_LIMIT + 1);

			std::string svalue;
			svalue.reserve(value_size);
			for (size_t j = 0; j < value_size; ++j)
				svalue.append(1, (char)deterministicRandom()->randomInt(0, 256));

			return Value(svalue);
		}

		static KeySelector makeKeySel() {
			// 40% of the time no offset, 30% it's a positive or negative.
			double orv = deterministicRandom()->random01();
			int offs = 0;
			if (orv >= 0.4) {
				if (orv < 0.7)
					offs = deterministicRandom()->randomInt(INT_MIN, 0);
				else
					offs = deterministicRandom()->randomInt(0, INT_MAX) + 1;
			}
			return KeySelectorRef(makeKey(), deterministicRandom()->coinflip(), offs);
		}

		static GetRangeLimits makeRangeLimits() {
			double lrv = deterministicRandom()->random01();
			int rowlimit = 0;
			if (lrv >= 0.2) {
				if (lrv < 0.4)
					rowlimit = deterministicRandom()->randomInt(INT_MIN, 0);
				else
					rowlimit = deterministicRandom()->randomInt(0, INT_MAX) + 1;
			}

			if (deterministicRandom()->coinflip())
				return GetRangeLimits(rowlimit);

			lrv = deterministicRandom()->random01();
			int bytelimit = 0;
			if (lrv >= 0.2) {
				if (lrv < 0.4)
					bytelimit = deterministicRandom()->randomInt(INT_MIN, 0);
				else
					bytelimit = deterministicRandom()->randomInt(0, INT_MAX) + 1;
			}

			// Try again if both are 0.
			if (!rowlimit && !bytelimit)
				return makeRangeLimits();

			return GetRangeLimits(rowlimit, bytelimit);
		}

		static bool isOverlapping(const Key& begin, const Key& end, const KeyRangeRef& range) {
			return ((range.begin >= begin && range.begin < end) || (range.end >= begin && range.end < end) ||
			        (begin >= range.begin && begin < range.end) || (end >= range.begin && end < range.end));
		}
		static bool isOverlapping(const Key& begin, const Key& end, const KeyRef& key) {
			return (key >= begin && key < end);
		}

		static std::string slashToEnd(const KeyRef& key) { return key.toString().replace(key.size() - 1, 1, 1, '0'); }

		static KeyRangeRef& getServerKeys() {
			static std::string serverKeysEnd = slashToEnd(serverKeysPrefix);
			static KeyRangeRef serverKeys(serverKeysPrefix, StringRef(serverKeysEnd));
			return serverKeys;
		}

		static KeyRangeRef& getGlobalKeys() {
			static std::string globalKeysPrefix2 = globalKeysPrefix.toString() + "/";
			static std::string globalKeysEnd = slashToEnd(globalKeysPrefix2);
			static KeyRangeRef globalKeys(globalKeysPrefix2, StringRef(globalKeysEnd));
			return globalKeys;
		}

		static bool isProtectedKey(const Key& begin, const Key& end) {
			if (end < begin)
				return false;

			return (isOverlapping(begin, end, keyServersKeys) || isOverlapping(begin, end, serverListKeys) ||
			        isOverlapping(begin, end, processClassKeys) || isOverlapping(begin, end, configKeys) ||
			        isOverlapping(begin, end, workerListKeys) || isOverlapping(begin, end, backupLogKeys) ||
			        isOverlapping(begin, end, getServerKeys()) || isOverlapping(begin, end, getGlobalKeys()) ||
			        isOverlapping(begin, end, coordinatorsKey) || isOverlapping(begin, end, backupEnabledKey) ||
			        isOverlapping(begin, end, backupVersionKey));
		}

		static bool isProtectedKey(const Key& k) {
			return (isOverlapping(keyServersKeys.begin, keyServersKeys.end, k) ||
			        isOverlapping(serverListKeys.begin, serverListKeys.end, k) ||
			        isOverlapping(processClassKeys.begin, processClassKeys.end, k) ||
			        isOverlapping(configKeys.begin, configKeys.end, k) ||
			        isOverlapping(workerListKeys.begin, workerListKeys.end, k) ||
			        isOverlapping(backupLogKeys.begin, backupLogKeys.end, k) ||
			        isOverlapping(getServerKeys().begin, getServerKeys().end, k) ||
			        isOverlapping(getGlobalKeys().begin, getGlobalKeys().end, k) || coordinatorsKey == k ||
			        backupEnabledKey == k || backupVersionKey == k);
		}
	};

	template <typename Subclass>
	struct BaseTestCallback : BaseTest<Subclass, Void> {
		typedef typename BaseTest<Subclass, Void>::value_type value_type;

		BaseTestCallback(unsigned int id, FuzzApiCorrectnessWorkload* wl, const char* func)
		  : BaseTest<Subclass, Void>(id, wl, func) {}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			callback(tr);
			return tr.castTo<ThreadSafeTransaction>()->checkDeferredError();
		}

		Void errorCheck(Reference<ITransaction> tr, value_type result) override {
			callbackErrorCheck(tr);
			return Void();
		}

		virtual void callback(Reference<ITransaction> tr) = 0;
		virtual void callbackErrorCheck(Reference<ITransaction> tr) {}
	};

	struct TestSetVersion : public BaseTest<TestSetVersion, Version> {
		typedef BaseTest<TestSetVersion, Version> base_type;
		Version v;

		TestSetVersion(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestSetVersion") {
			if (deterministicRandom()->coinflip())
				v = deterministicRandom()->randomInt64(INT64_MIN, 0);
			else
				v = deterministicRandom()->randomInt64(0, INT64_MAX);

			contract = { std::make_pair(error_code_read_version_already_set, ExceptionContract::Possible),
				         std::make_pair(error_code_version_invalid, ExceptionContract::requiredIf(v <= 0)) };
		}

		ThreadFuture<Version> createFuture(Reference<ITransaction> tr) override {
			tr->setVersion(v);
			pre_steps.push_back(tr.castTo<ThreadSafeTransaction>()->checkDeferredError());
			return tr->getReadVersion();
		}

		Void errorCheck(Reference<ITransaction> tr, value_type result) override {
			ASSERT(v == result);
			return Void();
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Version", v);
		}
	};

	struct TestGet : public BaseTest<TestGet, Optional<Value>> {
		typedef BaseTest<TestGet, Optional<Value>> base_type;
		Key key;

		TestGet(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestGet") {
			key = makeKey();
			contract = {
				std::make_pair(
				    error_code_key_outside_legal_range,
				    ExceptionContract::requiredIf((key >= workload->getMaxKey(tr)) && !specialKeys.contains(key))),
				std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				std::make_pair(error_code_accessed_unreadable, ExceptionContract::Possible),
				std::make_pair(
				    error_code_special_keys_no_module_found,
				    ExceptionContract::possibleIf(specialKeys.contains(key) && !workload->specialKeysRelaxed)),
				// Read this particular special key may throw timed_out
				std::make_pair(error_code_timed_out, ExceptionContract::possibleIf(key == "\xff\xff/status/json"_sr)),
				// Read this particular special key may throw special_keys_api_failure
				std::make_pair(
				    error_code_special_keys_api_failure,
				    ExceptionContract::possibleIf(
				        key == "auto_coordinators"_sr.withPrefix(
				                   SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin))),
				std::make_pair(error_code_tenant_not_found,
				               ExceptionContract::possibleIf(!workload->canUseTenant(tr->getTenant()))),
				std::make_pair(error_code_invalid_option,
				               ExceptionContract::possibleIf(tr->getTenant().present() && specialKeys.contains(key))),
				std::make_pair(error_code_illegal_tenant_access,
				               ExceptionContract::possibleIf(tr->getTenant().present() && specialKeys.contains(key)))
			};
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			return tr->get(key, deterministicRandom()->coinflip());
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key", key);
			e.detail("Size", key.size());
		}
	};

	struct TestGetKey : public BaseTest<TestGetKey, Key> {
		typedef BaseTest<TestGetKey, Key> base_type;
		KeySelector keysel;

		TestGetKey(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestGetKey") {
			keysel = makeKeySel();
			contract = { std::make_pair(error_code_key_outside_legal_range,
				                        ExceptionContract::requiredIf((keysel.getKey() > workload->getMaxKey(tr)))),
				         std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				         std::make_pair(error_code_accessed_unreadable, ExceptionContract::Possible),
				         std::make_pair(error_code_tenant_not_found,
				                        ExceptionContract::possibleIf(!workload->canUseTenant(tr->getTenant()))) };
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			return tr->getKey(keysel, deterministicRandom()->coinflip());
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("KeySel", keysel);
		}
	};

	struct TestGetRange0 : public BaseTest<TestGetRange0, RangeResult> {
		typedef BaseTest<TestGetRange0, RangeResult> base_type;
		KeySelector keysel1, keysel2;
		int limit;

		TestGetRange0(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestGetRange0") {
			keysel1 = makeKeySel();
			keysel2 = makeKeySel();
			limit = 0;

			double lrv = deterministicRandom()->random01();
			if (lrv > 0.20) {
				if (lrv < 0.4)
					limit = deterministicRandom()->randomInt(INT_MIN, 0);
				else
					limit = deterministicRandom()->randomInt(0, INT_MAX) + 1;
			}

			bool isSpecialKeyRange = specialKeys.contains(keysel1.getKey()) && specialKeys.begin <= keysel2.getKey() &&
			                         keysel2.getKey() <= specialKeys.end;

			contract = {
				std::make_pair(error_code_range_limits_invalid, ExceptionContract::possibleButRequiredIf(limit < 0)),
				std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				std::make_pair(error_code_key_outside_legal_range,
				               ExceptionContract::requiredIf(((keysel1.getKey() > workload->getMaxKey(tr)) ||
				                                              (keysel2.getKey() > workload->getMaxKey(tr))) &&
				                                             !isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_cross_module_read,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				std::make_pair(error_code_special_keys_no_module_found,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				// Read some special keys, e.g. status/json, can throw timed_out
				std::make_pair(error_code_timed_out, ExceptionContract::possibleIf(isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_api_failure, ExceptionContract::possibleIf(isSpecialKeyRange)),
				std::make_pair(error_code_accessed_unreadable, ExceptionContract::Possible),
				std::make_pair(error_code_tenant_not_found,
				               ExceptionContract::possibleIf(!workload->canUseTenant(tr->getTenant()))),
				std::make_pair(error_code_invalid_option,
				               ExceptionContract::possibleIf(tr->getTenant().present() && isSpecialKeyRange)),
				std::make_pair(error_code_illegal_tenant_access,
				               ExceptionContract::possibleIf(tr->getTenant().present() && isSpecialKeyRange))
			};
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			return tr->getRange(
			    keysel1, keysel2, limit, deterministicRandom()->coinflip(), deterministicRandom()->coinflip());
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("KeySel1", keysel1).detail("KeySel2", keysel2).detail("Limit", limit);
		}
	};

	struct TestGetRange1 : public BaseTest<TestGetRange1, RangeResult> {
		typedef BaseTest<TestGetRange1, RangeResult> base_type;
		KeySelector keysel1, keysel2;
		GetRangeLimits limits;

		TestGetRange1(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestGetRange1") {
			keysel1 = makeKeySel();
			keysel2 = makeKeySel();
			limits = makeRangeLimits();

			bool isSpecialKeyRange = specialKeys.contains(keysel1.getKey()) && specialKeys.begin <= keysel2.getKey() &&
			                         keysel2.getKey() <= specialKeys.end;

			contract = {
				std::make_pair(error_code_range_limits_invalid,
				               ExceptionContract::possibleButRequiredIf(!limits.isReached() && !limits.isValid())),
				std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				std::make_pair(error_code_key_outside_legal_range,
				               ExceptionContract::requiredIf(((keysel1.getKey() > workload->getMaxKey(tr)) ||
				                                              (keysel2.getKey() > workload->getMaxKey(tr))) &&
				                                             !isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_cross_module_read,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				std::make_pair(error_code_special_keys_no_module_found,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				std::make_pair(error_code_timed_out, ExceptionContract::possibleIf(isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_api_failure, ExceptionContract::possibleIf(isSpecialKeyRange)),
				std::make_pair(error_code_accessed_unreadable, ExceptionContract::Possible),
				std::make_pair(error_code_tenant_not_found,
				               ExceptionContract::possibleIf(!workload->canUseTenant(tr->getTenant()))),
				std::make_pair(error_code_illegal_tenant_access,
				               ExceptionContract::possibleIf(tr->getTenant().present() && isSpecialKeyRange))
			};
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			return tr->getRange(
			    keysel1, keysel2, limits, deterministicRandom()->coinflip(), deterministicRandom()->coinflip());
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("KeySel1", keysel1).detail("KeySel2", keysel2);
			std::stringstream ss;
			ss << "(" << limits.rows << ", " << limits.minRows << ", " << limits.bytes << ")";
			e.detail("Limits", ss.str());
		}
	};

	struct TestGetRange2 : public BaseTest<TestGetRange2, RangeResult> {
		typedef BaseTest<TestGetRange2, RangeResult> base_type;
		Key key1, key2;
		int limit;

		TestGetRange2(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestGetRange2") {
			key1 = makeKey();
			key2 = makeKey();
			limit = 0;

			double lrv = deterministicRandom()->random01();
			if (lrv > 0.20) {
				if (lrv < 0.4)
					limit = deterministicRandom()->randomInt(INT_MIN, 0);
				else
					limit = deterministicRandom()->randomInt(0, INT_MAX) + 1;
			}

			bool isSpecialKeyRange = specialKeys.contains(key1) && specialKeys.begin <= key2 && key2 <= specialKeys.end;
			// Read this particular special key may throw special_keys_api_failure
			Key autoCoordinatorSpecialKey = "auto_coordinators"_sr.withPrefix(
			    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
			KeyRangeRef actorLineageRange = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ACTORLINEAGE);
			// Read this particular special key may throw timed_out
			Key statusJsonSpecialKey = "\xff\xff/status/json"_sr;

			contract = {
				std::make_pair(error_code_inverted_range, ExceptionContract::requiredIf(key1 > key2)),
				std::make_pair(error_code_range_limits_invalid, ExceptionContract::possibleButRequiredIf(limit < 0)),
				std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				std::make_pair(
				    error_code_key_outside_legal_range,
				    ExceptionContract::requiredIf(
				        ((key1 > workload->getMaxKey(tr)) || (key2 > workload->getMaxKey(tr))) && !isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_cross_module_read,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				std::make_pair(error_code_special_keys_no_module_found,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				std::make_pair(
				    error_code_timed_out,
				    ExceptionContract::possibleIf(key1 <= statusJsonSpecialKey && statusJsonSpecialKey < key2)),
				std::make_pair(error_code_special_keys_api_failure,
				               ExceptionContract::possibleIf(
				                   (key1 <= autoCoordinatorSpecialKey && autoCoordinatorSpecialKey < key2) ||
				                   actorLineageRange.intersects(KeyRangeRef(key1, key2)))),
				std::make_pair(error_code_accessed_unreadable, ExceptionContract::Possible),
				std::make_pair(error_code_tenant_not_found,
				               ExceptionContract::possibleIf(!workload->canUseTenant(tr->getTenant()))),
				std::make_pair(error_code_invalid_option,
				               ExceptionContract::possibleIf(tr->getTenant().present() && isSpecialKeyRange)),
				std::make_pair(error_code_illegal_tenant_access,
				               ExceptionContract::possibleIf(tr->getTenant().present() && isSpecialKeyRange))
			};
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			return tr->getRange(
			    KeyRangeRef(key1, key2), limit, deterministicRandom()->coinflip(), deterministicRandom()->coinflip());
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key1", key1).detail("Key2", key2).detail("Limit", limit);
		}
	};

	struct TestGetRange3 : public BaseTest<TestGetRange3, RangeResult> {
		typedef BaseTest<TestGetRange3, RangeResult> base_type;
		Key key1, key2;
		GetRangeLimits limits;

		TestGetRange3(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestGetRange3") {
			key1 = makeKey();
			key2 = makeKey();
			limits = makeRangeLimits();

			bool isSpecialKeyRange = specialKeys.contains(key1) && specialKeys.begin <= key2 && key2 <= specialKeys.end;
			Key autoCoordinatorSpecialKey = "auto_coordinators"_sr.withPrefix(
			    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
			KeyRangeRef actorLineageRange = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ACTORLINEAGE);
			Key statusJsonSpecialKey = "\xff\xff/status/json"_sr;

			contract = {
				std::make_pair(error_code_inverted_range, ExceptionContract::requiredIf(key1 > key2)),
				std::make_pair(error_code_range_limits_invalid,
				               ExceptionContract::possibleButRequiredIf(!limits.isReached() && !limits.isValid())),
				std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				std::make_pair(
				    error_code_key_outside_legal_range,
				    ExceptionContract::requiredIf(
				        ((key1 > workload->getMaxKey(tr)) || (key2 > workload->getMaxKey(tr))) && !isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_cross_module_read,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				std::make_pair(error_code_special_keys_no_module_found,
				               ExceptionContract::possibleIf(isSpecialKeyRange && !workload->specialKeysRelaxed)),
				std::make_pair(
				    error_code_timed_out,
				    ExceptionContract::possibleIf(key1 <= statusJsonSpecialKey && statusJsonSpecialKey < key2)),
				std::make_pair(error_code_special_keys_api_failure,
				               ExceptionContract::possibleIf(
				                   (key1 <= autoCoordinatorSpecialKey && autoCoordinatorSpecialKey < key2) ||
				                   actorLineageRange.intersects(KeyRangeRef(key1, key2)))),
				std::make_pair(error_code_accessed_unreadable, ExceptionContract::Possible),
				std::make_pair(error_code_tenant_not_found,
				               ExceptionContract::possibleIf(!workload->canUseTenant(tr->getTenant()))),
				std::make_pair(error_code_invalid_option,
				               ExceptionContract::possibleIf(tr->getTenant().present() && isSpecialKeyRange)),
				std::make_pair(error_code_illegal_tenant_access,
				               ExceptionContract::possibleIf(tr->getTenant().present() && isSpecialKeyRange))
			};
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			return tr->getRange(
			    KeyRangeRef(key1, key2), limits, deterministicRandom()->coinflip(), deterministicRandom()->coinflip());
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key1", key1).detail("Key2", key2);
			std::stringstream ss;
			ss << "(" << limits.rows << ", " << limits.minRows << ", " << limits.bytes << ")";
			e.detail("Limits", ss.str());
		}
	};

	struct TestGetAddressesForKey : public BaseTest<TestGetAddressesForKey, Standalone<VectorRef<const char*>>> {
		typedef BaseTest<TestGetAddressesForKey, Standalone<VectorRef<const char*>>> base_type;
		Key key;

		TestGetAddressesForKey(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestGetAddressesForKey") {
			key = makeKey();
			contract = { std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				         std::make_pair(error_code_tenant_not_found,
				                        ExceptionContract::requiredIf(!workload->canUseTenant(tr->getTenant()))) };
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override {
			return tr->getAddressesForKey(key);
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key", key);
		}
	};

	struct TestAddReadConflictRange : public BaseTestCallback<TestAddReadConflictRange> {
		typedef BaseTest<TestAddReadConflictRange, Void> base_type;
		Key key1, key2;

		TestAddReadConflictRange(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestAddReadConflictRange") {
			key1 = makeKey();
			key2 = makeKey();
			contract = { std::make_pair(error_code_inverted_range, ExceptionContract::requiredIf(key1 > key2)),
				         std::make_pair(error_code_key_outside_legal_range,
				                        ExceptionContract::requiredIf((key1 > workload->getMaxKey(tr)) ||
				                                                      (key2 > workload->getMaxKey(tr)))) };
		}

		void callback(Reference<ITransaction> tr) override { tr->addReadConflictRange(KeyRangeRef(key1, key2)); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key1", key1).detail("Key2", key2);
		}
	};

	struct TestAtomicOp : public BaseTestCallback<TestAtomicOp> {
		typedef BaseTest<TestAtomicOp, Void> base_type;
		Key key;
		Value value;
		uint8_t op;
		int32_t pos;

		TestAtomicOp(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestAtomicOp") {
			key = makeKey();
			while (isProtectedKey(key)) {
				key = makeKey();
			}
			value = makeValue();
			double arv = deterministicRandom()->random01();
			if (arv < 0.25) {
				int val = UINT8_MAX;
				for (auto i = FDBMutationTypes::optionInfo.begin(); i != FDBMutationTypes::optionInfo.end(); ++i)
					if (i->first < val)
						val = i->first;
				op = deterministicRandom()->randomInt(0, val);
			} else if (arv < 0.50) {
				int val = 0;
				for (auto i = FDBMutationTypes::optionInfo.begin(); i != FDBMutationTypes::optionInfo.end(); ++i)
					if (i->first > val)
						val = i->first;
				op = deterministicRandom()->randomInt(val + 1, UINT8_MAX);
			} else {
				int minval = UINT8_MAX, maxval = 0;
				for (auto i = FDBMutationTypes::optionInfo.begin(); i != FDBMutationTypes::optionInfo.end(); ++i) {
					if (i->first < minval)
						minval = i->first;
					if (i->first > maxval)
						maxval = i->first;
				}
				op = deterministicRandom()->randomInt(minval, maxval + 1);
			}

			pos = -1;
			if (op == MutationRef::SetVersionstampedKey && key.size() >= 4) {
				pos = littleEndian32(*(int32_t*)&key.end()[-4]);
			}
			if (op == MutationRef::SetVersionstampedValue && value.size() >= 4) {
				pos = littleEndian32(*(int32_t*)&value.end()[-4]);
			}

			contract = { std::make_pair(error_code_key_too_large,
				                        key.size() > getMaxWriteKeySize(key, true)    ? ExceptionContract::Always
				                        : key.size() > getMaxWriteKeySize(key, false) ? ExceptionContract::Possible
				                                                                      : ExceptionContract::Never),
				         std::make_pair(error_code_value_too_large,
				                        ExceptionContract::requiredIf(value.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)),
				         std::make_pair(error_code_invalid_mutation_type,
				                        ExceptionContract::requiredIf(!isValidMutationType(op) ||
				                                                      !isAtomicOp((MutationRef::Type)op))),
				         std::make_pair(error_code_key_outside_legal_range,
				                        ExceptionContract::requiredIf((key >= workload->getMaxKey(tr)))),
				         std::make_pair(error_code_client_invalid_operation,
				                        ExceptionContract::requiredIf((op == MutationRef::SetVersionstampedKey &&
				                                                       (pos < 0 || pos + 10 > key.size() - 4)) ||
				                                                      (op == MutationRef::SetVersionstampedValue &&
				                                                       (pos < 0 || pos + 10 > value.size() - 4)))) };
		}

		void callback(Reference<ITransaction> tr) override { tr->atomicOp(key, value, (FDBMutationTypes::Option)op); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key", key).detail("Value", value).detail("Op", op).detail("Pos", pos);
		}
	};

	struct TestSet : public BaseTestCallback<TestSet> {
		typedef BaseTest<TestSet, Void> base_type;
		Key key;
		Value value;

		TestSet(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestSet") {
			key = makeKey();
			while (isProtectedKey(key)) {
				key = makeKey();
			}
			value = makeValue();
			contract = { std::make_pair(error_code_key_too_large,
				                        key.size() > getMaxWriteKeySize(key, true)    ? ExceptionContract::Always
				                        : key.size() > getMaxWriteKeySize(key, false) ? ExceptionContract::Possible
				                                                                      : ExceptionContract::Never),
				         std::make_pair(error_code_value_too_large,
				                        ExceptionContract::requiredIf(value.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)),
				         std::make_pair(error_code_key_outside_legal_range,
				                        ExceptionContract::requiredIf((key >= workload->getMaxKey(tr)) &&
				                                                      !specialKeys.contains(key))),
				         std::make_pair(error_code_special_keys_write_disabled,
				                        ExceptionContract::requiredIf(specialKeys.contains(key) &&
				                                                      !workload->specialKeysWritesEnabled)),
				         std::make_pair(error_code_special_keys_no_write_module_found,
				                        ExceptionContract::possibleIf(specialKeys.contains(key) &&
				                                                      workload->specialKeysWritesEnabled)) };
		}

		void callback(Reference<ITransaction> tr) override { tr->set(key, value); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key", key).detail("Value", value);
		}
	};

	struct TestClear0 : public BaseTestCallback<TestClear0> {
		typedef BaseTest<TestClear0, Void> base_type;
		Key key1, key2;

		TestClear0(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestClear0") {
			key1 = makeKey();
			key2 = makeKey();
			while (isProtectedKey(key1, key2)) {
				key1 = makeKey();
				key2 = makeKey();
			}

			bool isSpecialKeyRange = specialKeys.contains(key1) && key2 <= specialKeys.end;

			contract = {
				std::make_pair(error_code_inverted_range, ExceptionContract::requiredIf(key1 > key2)),
				std::make_pair(
				    error_code_key_outside_legal_range,
				    ExceptionContract::requiredIf(
				        ((key1 > workload->getMaxKey(tr)) || (key2 > workload->getMaxKey(tr))) && !isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_write_disabled,
				               ExceptionContract::requiredIf(isSpecialKeyRange && !workload->specialKeysWritesEnabled)),
				std::make_pair(error_code_special_keys_cross_module_clear,
				               ExceptionContract::possibleIf(isSpecialKeyRange && workload->specialKeysWritesEnabled)),
				std::make_pair(error_code_special_keys_no_write_module_found,
				               ExceptionContract::possibleIf(isSpecialKeyRange && workload->specialKeysWritesEnabled))
			};
		}

		void callback(Reference<ITransaction> tr) override { tr->clear(key1, key2); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key1", key1).detail("Key2", key2);
		}
	};

	struct TestClear1 : public BaseTestCallback<TestClear1> {
		typedef BaseTest<TestClear1, Void> base_type;
		Key key1, key2;

		TestClear1(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestClear1") {
			key1 = makeKey();
			key2 = makeKey();
			while (isProtectedKey(key1, key2)) {
				key1 = makeKey();
				key2 = makeKey();
			}

			bool isSpecialKeyRange = specialKeys.contains(key1) && key2 <= specialKeys.end;

			contract = {
				std::make_pair(error_code_inverted_range, ExceptionContract::requiredIf(key1 > key2)),
				std::make_pair(
				    error_code_key_outside_legal_range,
				    ExceptionContract::requiredIf(
				        ((key1 > workload->getMaxKey(tr)) || (key2 > workload->getMaxKey(tr))) && !isSpecialKeyRange)),
				std::make_pair(error_code_special_keys_write_disabled,
				               ExceptionContract::requiredIf(isSpecialKeyRange && !workload->specialKeysWritesEnabled)),
				std::make_pair(error_code_special_keys_cross_module_clear,
				               ExceptionContract::possibleIf(isSpecialKeyRange && workload->specialKeysWritesEnabled)),
				std::make_pair(error_code_special_keys_no_write_module_found,
				               ExceptionContract::possibleIf(isSpecialKeyRange && workload->specialKeysWritesEnabled))
			};
		}

		void callback(Reference<ITransaction> tr) override { tr->clear(KeyRangeRef(key1, key2)); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key1", key1).detail("Key2", key2);
		}
	};

	struct TestClear2 : public BaseTestCallback<TestClear2> {
		typedef BaseTest<TestClear2, Void> base_type;
		Key key;

		TestClear2(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestClear2") {
			key = makeKey();
			while (isProtectedKey(key)) {
				key = makeKey();
			}
			contract = { std::make_pair(error_code_key_outside_legal_range,
				                        ExceptionContract::requiredIf(key >= workload->getMaxKey(tr))),
				         std::make_pair(error_code_special_keys_write_disabled,
				                        ExceptionContract::requiredIf(specialKeys.contains(key) &&
				                                                      !workload->specialKeysWritesEnabled)),
				         std::make_pair(error_code_special_keys_no_write_module_found,
				                        ExceptionContract::possibleIf(specialKeys.contains(key) &&
				                                                      workload->specialKeysWritesEnabled)) };
		}

		void callback(Reference<ITransaction> tr) override { tr->clear(key); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key", key);
		}
	};

	struct TestWatch : public BaseTest<TestWatch, Void> {
		typedef BaseTest<TestWatch, Void> base_type;
		Key key;

		TestWatch(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTest(id, workload, "TestWatch") {
			key = makeKey();
			printf("Watching: %d %s\n", key.size(), printable(key.substr(0, std::min(key.size(), 20))).c_str());
			contract = { std::make_pair(error_code_key_too_large,
				                        key.size() > getMaxWriteKeySize(key, true)    ? ExceptionContract::Always
				                        : key.size() > getMaxWriteKeySize(key, false) ? ExceptionContract::Possible
				                                                                      : ExceptionContract::Never),
				         std::make_pair(error_code_watches_disabled, ExceptionContract::Possible),
				         std::make_pair(error_code_key_outside_legal_range,
				                        ExceptionContract::requiredIf((key >= workload->getMaxKey(tr)))),
				         std::make_pair(error_code_client_invalid_operation, ExceptionContract::Possible),
				         std::make_pair(error_code_timed_out, ExceptionContract::Possible),
				         std::make_pair(error_code_accessed_unreadable, ExceptionContract::Possible),
				         std::make_pair(error_code_tenant_not_found,
				                        ExceptionContract::possibleIf(!workload->canUseTenant(tr->getTenant()))) };
		}

		ThreadFuture<value_type> createFuture(Reference<ITransaction> tr) override { return tr->watch(key); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key", key);
		}
	};

	struct TestAddWriteConflictRange : public BaseTestCallback<TestAddWriteConflictRange> {
		typedef BaseTest<TestAddWriteConflictRange, Void> base_type;
		Key key1, key2;

		TestAddWriteConflictRange(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestAddWriteConflictRange") {
			key1 = makeKey();
			key2 = makeKey();
			contract = { std::make_pair(error_code_inverted_range, ExceptionContract::requiredIf(key1 > key2)),
				         std::make_pair(error_code_key_outside_legal_range,
				                        ExceptionContract::requiredIf((key1 > workload->getMaxKey(tr)) ||
				                                                      (key2 > workload->getMaxKey(tr)))) };
		}

		void callback(Reference<ITransaction> tr) override { tr->addWriteConflictRange(KeyRangeRef(key1, key2)); }

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Key1", key1).detail("Key2", key2);
		}
	};

	struct TestSetOption : public BaseTestCallback<TestSetOption> {
		typedef BaseTest<TestSetOption, Void> base_type;
		int op;
		Optional<Standalone<StringRef>> val;

		TestSetOption(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestSetOption") {
			double arv = deterministicRandom()->random01();
			if (arv < 0.25) {
				int val = INT_MAX;
				for (auto i = FDBTransactionOptions::optionInfo.begin(); i != FDBTransactionOptions::optionInfo.end();
				     ++i)
					if (i->first < val)
						val = i->first;
				op = deterministicRandom()->randomInt(INT_MIN, val);
			} else if (arv < 0.50) {
				int val = INT_MIN;
				for (auto i = FDBTransactionOptions::optionInfo.begin(); i != FDBTransactionOptions::optionInfo.end();
				     ++i)
					if (i->first > val)
						val = i->first;
				op = deterministicRandom()->randomInt(val + 1, INT_MAX);
			} else {
				int minval = INT_MAX, maxval = INT_MIN;
				for (auto i = FDBTransactionOptions::optionInfo.begin(); i != FDBTransactionOptions::optionInfo.end();
				     ++i) {
					if (i->first < minval)
						minval = i->first;
					if (i->first > maxval)
						maxval = i->first;
				}
				op = deterministicRandom()->randomInt(minval, maxval + 1);
			}

			// do not test the following options since they are actually used by the workload
			if (op == FDBTransactionOptions::ACCESS_SYSTEM_KEYS || op == FDBTransactionOptions::READ_SYSTEM_KEYS ||
			    op == FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES ||
			    op == FDBTransactionOptions::RAW_ACCESS) {
				op = -1;
			}

			// do not test the option since it's already used by the workload
			if (op == FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED)
				op = -1;
			// disable for now(see issue#3934, pr#3930)
			if (op == FDBTransactionOptions::CHECK_WRITES_ENABLE)
				op = -1;

			double orv = deterministicRandom()->random01();
			if (orv >= 0.25) {
				int64_t value_size = deterministicRandom()->randomInt64(0, CLIENT_KNOBS->VALUE_SIZE_LIMIT + 1) +
				                     CLIENT_KNOBS->VALUE_SIZE_LIMIT;

				std::string svalue;
				svalue.reserve(value_size);
				for (size_t j = 0; j < value_size; ++j)
					svalue.append(1, (char)deterministicRandom()->randomInt(0, 256));

				val = Standalone<StringRef>(StringRef(svalue));
			}

			contract = { std::make_pair(error_code_invalid_option, ExceptionContract::Possible),
				         std::make_pair(error_code_invalid_option_value, ExceptionContract::Possible),
				         std::make_pair(error_code_tag_too_long, ExceptionContract::Possible),
				         std::make_pair(error_code_too_many_tags, ExceptionContract::Possible),
				         std::make_pair(
				             error_code_client_invalid_operation,
				             ExceptionContract::possibleIf(
				                 (FDBTransactionOptions::Option)op == FDBTransactionOptions::READ_YOUR_WRITES_DISABLE ||
				                 (FDBTransactionOptions::Option)op == FDBTransactionOptions::LOG_TRANSACTION)),
				         std::make_pair(
				             error_code_read_version_already_set,
				             ExceptionContract::possibleIf((FDBTransactionOptions::Option)op ==
				                                           FDBTransactionOptions::INITIALIZE_NEW_DATABASE)) };
		}

		void callback(Reference<ITransaction> tr) override {
			tr->setOption((FDBTransactionOptions::Option)op, val.castTo<StringRef>());
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("Op", op).detail("Val", val);
		}
	};

	struct TestOnError : public BaseTestCallback<TestOnError> {
		typedef BaseTest<TestOnError, Void> base_type;
		int errorcode;

		TestOnError(unsigned int id, FuzzApiCorrectnessWorkload* workload, Reference<ITransaction> tr)
		  : BaseTestCallback(id, workload, "TestOnError") {
			errorcode = 0;
			double erv = deterministicRandom()->random01();
			if (erv >= 0.2) {
				if (erv < 0.6)
					errorcode = deterministicRandom()->randomInt(INT_MIN, 0);
				else
					errorcode = deterministicRandom()->randomInt(0, INT_MAX) + 1;
			}
		}

		void callback(Reference<ITransaction> tr) override {
			tr->onError(Error::fromUnvalidatedCode(errorcode));
			// This is necessary here, as onError will have reset this
			// value, we will be looking at the wrong thing.
			if (workload->useSystemKeys && !tr->getTenant().present())
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		}

		void augmentTrace(TraceEvent& e) const override {
			base_type::augmentTrace(e);
			e.detail("ErrorCode", errorcode);
		}
	};

	static void addTestCases() {
		testCases.push_back(std::bind(&TestSetVersion::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestGet::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestGetKey::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestGetRange0::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestGetRange1::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestGetRange2::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestGetRange3::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestGetAddressesForKey::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestAddReadConflictRange::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestAtomicOp::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestSet::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestClear0::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestClear1::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestClear2::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestWatch::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestAddWriteConflictRange::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestSetOption::runTest, ph::_1, ph::_2, ph::_3));
		testCases.push_back(std::bind(&TestOnError::runTest, ph::_1, ph::_2, ph::_3));
	}
};

std::once_flag FuzzApiCorrectnessWorkload::onceFlag;
std::vector<std::function<
    Future<Void>(unsigned int const&, FuzzApiCorrectnessWorkload* const&, Reference<ITransaction> const&)>>
    FuzzApiCorrectnessWorkload::testCases;
WorkloadFactory<FuzzApiCorrectnessWorkload> FuzzApiCorrectnessWorkloadFactory;
