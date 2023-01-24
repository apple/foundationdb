/*
 * RawTenantAccessWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h"

struct RawTenantAccessWorkload : TestWorkload {
	static constexpr auto NAME = "RawTenantAccess";

	const Key specialKeysTenantMapPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                           .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                           .withSuffix(TenantRangeImpl::mapSubRange.begin);
	const KeyRef writeKey = "key"_sr;
	const ValueRef writeValue = "value"_sr;

	int tenantCount;
	double testDuration;

	std::set<int> lastCreatedTenants; // the index of tenant to be created if the last transaction succeed
	std::set<int> lastDeletedTenants; // the index of tenant to be deleted if the last transaction succeed
	std::map<int, int64_t> idx2Tid; // workload tenant idx to tenantId
	std::map<int64_t, int> tid2Idx; // tenant id to tenant index in this workload

	RawTenantAccessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		tenantCount = std::min(getOption(options, "tenantCount"_sr, 1000), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
		testDuration = getOption(options, "testDuration"_sr, 120.0);
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		}
		return Void();
	}

	TenantName indexToTenantName(int index) {
		auto name = fmt::format("tenant_idx_{:06d}", index);
		return TenantName(StringRef(name));
	}

	ACTOR static Future<Void> _setup(Database cx, RawTenantAccessWorkload* self) {
		RawTenantAccessWorkload* workload = self;
		// create N tenant through special key space
		wait(runRYWTransaction(cx, [workload](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (int i = 0; i < workload->tenantCount ; i += 2) {
				tr->set(workload->specialKeysTenantMapPrefix.withSuffix(workload->indexToTenantName(i)), ""_sr);
			}
			return Future<Void>(Void());
		}));

		for (int i = 0; i < self->tenantCount; i += 2) {
			self->lastCreatedTenants.insert(i);
		}
		return Void();
	}

	bool hasNonexistentTenant() const {
		return lastCreatedTenants.size() + idx2Tid.size() < tenantCount;
	}

	bool hasExistingTenant() const {
		return idx2Tid.size() - lastDeletedTenants.size() > 0;
	}

	int64_t extractTenantId(ValueRef value) {
		int64_t id;
		json_spirit::mValue jsonObject;
		json_spirit::read_string(value.toString(), jsonObject);
		JSONDoc jsonDoc(jsonObject);
		jsonDoc.get("id", id);
		return id;
	}

	void eraseDeletedTenants() {
		for (auto idx : lastDeletedTenants) {
			auto tid = idx2Tid.at(idx);
			tid2Idx.erase(tid);
			idx2Tid.erase(idx);
		}
	}

	void addCreatedTenants(std::unordered_map<int, int64_t> const& newTenantIds) {
		for (auto idx : lastCreatedTenants) {
			auto tid = newTenantIds.at(idx);
			tid2Idx[tid] = idx;
			idx2Tid[idx] = tid;
		}
	}

	ACTOR static Future<Void> checkAndApplyTenantChanges(Database cx,
	                                                     RawTenantAccessWorkload* self,
	                                                     bool lastCommitted) {
		state std::unordered_map<int, int64_t> newTenantIds;
		// check tenant existence, and load tenantId
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			tr->reset();
			newTenantIds.clear();
			try {
				state std::set<int>::const_iterator it = self->lastDeletedTenants.cbegin();
				// check tenant deletion
				while (it != self->lastDeletedTenants.end()) {
					Key key = self->specialKeysTenantMapPrefix.withSuffix(self->indexToTenantName(*it));
					Optional<Value> value = wait(tr->get(key));
					// the commit proxies should have the same view of tenant map
					ASSERT_EQ(value.present(), !lastCommitted);
					++it;
				}

				// check tenant creation
				it = self->lastCreatedTenants.cbegin();
				while (it != self->lastCreatedTenants.end()) {
					Key key = self->specialKeysTenantMapPrefix.withSuffix(self->indexToTenantName(*it));
					Optional<Value> value = wait(tr->get(key));
					// the commit proxies should have the same view of tenant map
					ASSERT_EQ(value.present(), lastCommitted || (self->idx2Tid.count(*it) > 0));

					if (value.present()) {
						auto id = self->extractTenantId(value.get());
						newTenantIds[*it] = id;
						if (!lastCommitted) {
							ASSERT_EQ(id, self->idx2Tid.at(*it));
						}
					}

					++it;
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		TraceEvent("RawTenantAccess_CheckTenantChanges")
		    .detail("CurrentTenantCount", self->idx2Tid.size())
		    .detail("NewTenantIds", newTenantIds.size());

		if (lastCommitted) {
			self->eraseDeletedTenants();
			self->addCreatedTenants(newTenantIds);
			TraceEvent("RawTenantAccess_ApplyTenantChanges").detail("CurrentTenantCount", self->idx2Tid.size());
		}

		self->lastDeletedTenants.clear();
		self->lastCreatedTenants.clear();

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return ready(timeout(_start(cx, this), testDuration));
		}
		return Void();
	}

	void createNewTenant(Reference<ReadYourWritesTransaction> tr, UID traceId) {
		ASSERT(hasNonexistentTenant());
		int tenantIdx = deterministicRandom()->randomInt(0, tenantCount);
		// find the nearest nonexistent tenant
		while (idx2Tid.count(tenantIdx) || lastCreatedTenants.count(tenantIdx)) {
			tenantIdx++;
			if (tenantIdx == tenantCount) {
				tenantIdx = 0;
			}
		}

		tr->set(specialKeysTenantMapPrefix.withSuffix(indexToTenantName(tenantIdx)), ""_sr);
		lastCreatedTenants.insert(tenantIdx);
		lastDeletedTenants.erase(tenantIdx);

		TraceEvent("RawTenantAccess_CreateNewTenant", traceId)
		    .detail("TenantIndex", tenantIdx)
		    .detail("LastCreatedTenants", lastCreatedTenants.size())
		    .detail("LastDeletedTenants", lastDeletedTenants.size());
	}

	void deleteExistingTenant(Reference<ReadYourWritesTransaction> tr, UID traceId) {
		ASSERT(hasExistingTenant());
		int tenantIdx = deterministicRandom()->randomInt(0, tenantCount);
		// find the nearest existing tenant
		while (true) {
			if (idx2Tid.count(tenantIdx) && !lastDeletedTenants.count(tenantIdx)) {
				break;
			}
			tenantIdx++;
			if (tenantIdx == tenantCount) {
				tenantIdx = 0;
			}
		}

		Key key = specialKeysTenantMapPrefix.withSuffix(indexToTenantName(tenantIdx));
		tr->clear(key);
		lastCreatedTenants.erase(tenantIdx);
		lastDeletedTenants.insert(tenantIdx);
		TraceEvent("RawTenantAccess_DeleteExistingTenant", traceId)
		    .detail("TenantIndex", tenantIdx)
		    .detail("TenantId", idx2Tid.at(tenantIdx))
		    .detail("LastCreatedTenants", lastCreatedTenants.size())
		    .detail("LastDeletedTenants", lastDeletedTenants.size());
	}

	void writeToExistingTenant(Reference<ReadYourWritesTransaction> tr, UID traceId) {
		ASSERT_GT(idx2Tid.size(), 0);
		// determine the tenant to write
		int tenantIdx = deterministicRandom()->randomInt(0, tenantCount);
		auto it = idx2Tid.lower_bound(tenantIdx);
		if (it == idx2Tid.end()) {
			tenantIdx = idx2Tid.begin()->first;
		} else {
			tenantIdx = it->first;
		}

		// write the raw data
		int64_t tenantId = idx2Tid.at(tenantIdx);
		Key prefix = TenantAPI::idToPrefix(tenantId);
		tr->set(prefix.withSuffix(writeKey), writeValue);
		TraceEvent("RawTenantAccess_WriteToExistingTenant", traceId)
		    .detail("TenantIndex", tenantIdx)
		    .detail("TenantId", tenantId);
	}

	void writeToInvalidTenant(Reference<ReadYourWritesTransaction> tr, UID traceId) {
		ASSERT(hasNonexistentTenant());
		// determine the invalid tenant id
		int64_t tenantId = TenantInfo::INVALID_TENANT;
		if (deterministicRandom()->coinflip() && lastDeletedTenants.size() > 0) {
			// choose the tenant deleted in the same transaction
			tenantId = idx2Tid.at(*lastDeletedTenants.begin());
		} else {
			// randomly generate a tenant id
			do {
				tenantId = deterministicRandom()->randomInt64(0, std::numeric_limits<int64_t>::max());
			} while (tid2Idx.count(tenantId));
		}
		ASSERT_GE(tenantId, 0);

		// write to invalid tenant
		Key prefix = TenantAPI::idToPrefix(tenantId);
		tr->set(prefix.withSuffix(writeKey), writeValue);
		TraceEvent("RawTenantAccess_WriteToInvalidTenant", traceId).detail("TenantId", tenantId);
	}

	// return whether the transaction is committed
	ACTOR static Future<bool> randomTenantTransaction(Database cx, RawTenantAccessWorkload* self) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state UID traceId = deterministicRandom()->randomUniqueID();
		// 1. create/delete tenant and write to a tenant is an illegal operation for now. (tenantMapChange &&
		// normalKeyWriteOp == true)
		// 2. write a nonexistent tenant is illegal. (invalidTenantWriteOp == true)
		state bool legalTxnOnly = deterministicRandom()->coinflip(); // whether allow generating illegal transaction
		state bool validTenantWriteOnly = deterministicRandom()->coinflip(); // whether only write to existing tenants

		state bool illegalAccessCaught = false;
		state bool normalKeyWriteOp = false;
		state bool validTenantWriteOp = false;
		state bool invalidTenantWriteOp = false;
		state bool tenantMapChangeOp = false;
		state bool committed = false;

		loop {
			tr->reset();
			tr->debugTransaction(traceId);
			try {
				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr->setOption(FDBTransactionOptions::RAW_ACCESS);
				// the transaction will randomly run 10 ops
				state int i = 0;
				for (; i < 10; ++i) {
					int op = deterministicRandom()->randomInt(0, 4);
					if (op == 0 && self->hasNonexistentTenant() && !(legalTxnOnly && normalKeyWriteOp)) {
						// whether to create a new Tenant
						self->createNewTenant(tr, traceId);
						tenantMapChangeOp = true;
					} else if (op == 1 && self->hasExistingTenant() && !(legalTxnOnly && normalKeyWriteOp)) {
						// whether to delete an existing tenant
						self->deleteExistingTenant(tr, traceId);
						tenantMapChangeOp = true;
					} else if (op == 2 && self->hasNonexistentTenant() && !legalTxnOnly && !validTenantWriteOnly) {
						// whether to write to a nonexistent tenant
						self->writeToInvalidTenant(tr, traceId);
						invalidTenantWriteOp = true;
						normalKeyWriteOp = true;
					} else if (op == 3 && self->idx2Tid.size() > 0 && !(legalTxnOnly && tenantMapChangeOp)) {
						// whether to write to an existing tenant
						self->writeToExistingTenant(tr, traceId);
						validTenantWriteOp = true;
						normalKeyWriteOp = true;
					}
				}

				wait(tr->commit());
				committed = true;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_illegal_tenant_access) {
					illegalAccessCaught = true;
					break;
				}
				TraceEvent("RawTenantAccess_TransactionError", traceId).error(e);
				wait(tr->onError(e));
			}
		}

		// check whether we caught illegal transaction when running illegal transactions
		if (invalidTenantWriteOp) {
			ASSERT(illegalAccessCaught);
			CODE_PROBE(true, "Caught invalid tenant write op.");
		} else if (tenantMapChangeOp && normalKeyWriteOp) {
			ASSERT(illegalAccessCaught);
			CODE_PROBE(true, "Caught tenant map changing and normal key writing in the same transaction.");
		} else {
			ASSERT(!illegalAccessCaught);
		}
		TraceEvent("RawTenantAccess_TransactionResult", traceId).detail("Committed", committed);
		return committed;
	}

	// clear tenant data to make sure the random tenant deletions are success
	ACTOR static Future<Void> clearAllTenantData(Database cx, RawTenantAccessWorkload* self) {
		RawTenantAccessWorkload* workload = self;
		wait(runRYWTransaction(cx, [workload](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			for (auto [tid, _] : workload->tid2Idx) {
				Key prefix = TenantAPI::idToPrefix(tid);
				tr->clear(prefix.withSuffix(workload->writeKey));
			}
			return Future<Void>(Void());
		}));
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, RawTenantAccessWorkload* self) {
		state bool lastCommitted = true;
		loop {
			wait(checkAndApplyTenantChanges(cx, self, lastCommitted));
			wait(clearAllTenantData(cx, self));
			wait(store(lastCommitted, randomTenantTransaction(cx, self)));
			wait(delay(0.5));
		}
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RawTenantAccessWorkload> RawTenantAccessWorkload;