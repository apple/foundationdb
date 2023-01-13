/*
 * GetEstimatedRangeSize.actor.cpp
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

#include <cstring>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/TokenSign.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct GetEstimatedRangeSizeWorkload : TestWorkload {
	static constexpr auto NAME = "GetEstimatedRangeSize";
	int nodeCount;
	double testDuration;
	Key keyPrefix;

	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, retries, tooOldRetries, commitFailedRetries;
	PerfDoubleCounter totalLatency;

	GetEstimatedRangeSizeWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"), tooOldRetries("Retries.too_old"),
	    commitFailedRetries("Retries.commit_failed"), totalLatency("Latency") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		nodeCount = getOption(options, "nodeCount"_sr, 10000.0);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		hasTenant = hasOption(options, "tenant"_sr);

		ASSERT(g_network->isSimulated());
		auto k = g_simulator.authKeys.begin();
		this->tenant = getOption(options, "tenant"_sr, "DefaultTenant"_sr);
		// make it comfortably longer than the timeout of the workload
		auto currentTime = uint64_t(lround(g_network->timer()));
		this->token.algorithm = authz::Algorithm::ES256;
		this->token.issuedAtUnixTime = currentTime;
		this->token.expiresAtUnixTime =
		    currentTime + uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100;
		this->token.keyId = k->first;
		this->token.notBeforeUnixTime = currentTime - 10;
		VectorRef<StringRef> tenants;
		tenants.push_back_deep(this->arena, this->tenant);
		this->token.tenants = tenants;
		// we currently don't support this workload to be run outside of simulation
		this->signedToken = authz::jwt::signToken(this->arena, this->token, k->second);
	}

	std::string description() const override { return "GetEstimatedRangeSizeWorkload"; }

	Future<Void> setup(Database const& cx) override {
		if (!hasTenant) {
			return Void();
		}
		// Use default values for arguments between (and including) postSetupWarming and endNodeIdx params
		return bulkSetup(cx,
		                 this,
		                 nodeCount,
		                 Promise<double>(),
		                 true,
		                 0.0,
		                 1e12,
		                 std::vector<uint64_t>(),
		                 Promise<std::vector<std::pair<uint64_t, double>>>(),
		                 0,
		                 0.1,
		                 0,
		                 0,
		                 { tenant });
	}

	Future<Void> start(Database const& cx) override {
		cx->defaultTenant = this->tenant;
		return checkSize(this, cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	StringRef getAuthToken() const { return this->signedToken; }

	void setAuthToken(ReadYourWritesTransaction& tr) {
		tr.setOption(FDBTransactionOptions::AUTHORIZATION_TOKEN, this->signedToken);
		tr.setOption(FDBTransactionOptions::RAW_ACCESS);
	}

	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }
	int fromValue(const ValueRef& v) { return testKeyToDouble(v, keyPrefix); }
	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(key(n), value((n + 1) % nodeCount)); }

	ACTOR static Future<Void> checkSize(GetEstimatedRangeSizeWorkload* self, Database cx) {
		// TraceEvent(SevWarnAlways, "AKDebug").detail("Status", "checkSize-1").detail("Tenant", cx->defaultTenant.get());
		int64_t size = wait(getSize(self, cx));
		// TraceEvent(SevWarnAlways, "AKDebug").detail("Status", "checkSize-2").detail("Tenant", cx->defaultTenant.get());
		TraceEvent(SevWarnAlways, "AKGetEstimatedRangeSizeResults")
		    .detail("Tenant", cx->defaultTenant.get())
		    .detail("TenantSize", size);
		ASSERT_LT(size, 0);
		return Void();
	}

	ACTOR static Future<int64_t> getSize(GetEstimatedRangeSizeWorkload* self, Database cx) {
		// TraceEvent(SevWarnAlways, "AKDebug").detail("Status", "getSize-1").detail("Tenant", cx->defaultTenant.get());
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				self->setAuthToken(tr);
				state int64_t size = wait(tr.getEstimatedRangeSizeBytes(normalKeys));
				// TraceEvent(SevWarnAlways, "AKDebug")
				//     .detail("Status", "getSize-2")
				//     .detail("Tenant", cx->defaultTenant.get());
				tr.reset();
				return size;
			} catch (Error& e) {
				// TraceEvent(SevWarnAlways, "AKDebugError").detail("Status", "getSize-3").detail("Error", e.name());
				wait(tr.onError(e));
			}
		}
	}
};

WorkloadFactory<GetEstimatedRangeSizeWorkload> GetEstimatedRangeSizeWorkloadFactory;
