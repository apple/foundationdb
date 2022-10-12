/*
 * AuthzSecurity.actor.cpp
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

#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/TokenSign.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct AuthzSecurityWorkload : TestWorkload {
	int actorCount;
	double testDuration;

	std::vector<Future<Void>> clients;
	Arena arena;
	TenantName tenant;
	authz::jwt::TokenRef token;
	Standalone<StringRef> signedToken;

	AuthzSecurityWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		actorCount = getOption(options, "actorsPerClient"_sr, 1);
		tenant = getOption(options, "tenant"_sr, "authzSecurityTestTenant"_sr);
		ASSERT(g_network->isSimulated());
		// make it comfortably longer than the timeout of the workload
		signedToken = g_simulator.makeToken(
				tenant,
				uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100);
	}

	std::string description() const override {
		return "AuthzSecurityWorkload";
	}

	Future<Void> setup(Database const& cx) override {
		//cx->defaultTenant = tenant;
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		//cx->defaultTenant = tenant;
		for (int c = 0; c < actorCount; c++)
			clients.push_back(timeout(runTestClient(cx, this), testDuration, Void()));
		return waitForAll(clients);
	}
	Future<bool> check(Database const& cx) override {
		int errors = 0;
		for (int c = 0; c < clients.size(); c++)
			errors += clients[c].isError();
		if (errors)
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were client errors.");
		clients.clear();
		return errors == 0;
	}
	void getMetrics(std::vector<PerfMetric>& m) override {
		//m.push_back(transactions.getMetric());
		//m.push_back(retries.getMetric());
		//m.push_back(tooOldRetries.getMetric());
		//m.push_back(commitFailedRetries.getMetric());
		//m.emplace_back("Avg Latency (ms)", 1000 * totalLatency.getValue() / transactions.getValue(), Averaged::True);
		//m.emplace_back("Read rows/simsec (approx)", transactions.getValue() * 3 / testDuration, Averaged::False);
		//m.emplace_back("Write rows/simsec (approx)", transactions.getValue() * 4 / testDuration, Averaged::False);
	}

	void setAuthToken(Transaction& tr) {
		tr.setOption(FDBTransactionOptions::AUTHORIZATION_TOKEN, signedToken);
	}

	ACTOR Future<Void> testCrossTenantGet(Database cx, AuthzSecurityWorkload* self) {
		state Transaction tr(cx, self->tenant);
		self->setAuthToken(tr);
		tr.set("abc"_sr, "def"_sr);
		wait(tr.commit());
		auto const committedVersion = tr.getCommittedVersion();
		auto loc = cx->getCachedLocation(tenant, "abc"_sr);
		ASSERT(loc.present());
		auto const& tenantEntry = loc.get().tenantEntry;
		ASSERT(!tenantEntry.prefix.empty());
		GetValueRequest req;
		req.key = "abc"_sr;
		state bool exceptionHit = true;
		try {
			GetValueReply reply = wait(loadBalance(
						cx.getPtr(),
						loc.get().locations,
						&StorageServerInterface::getValue,
						req,
						TaskPriority::DefaultPromiseEndpoint,
						AtMostOnce::False,
						nullptr));
			exceptionHit = false;
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_permission_denied);
		}
		ASSERT(exceptionHit);
		return Void();
		/*
		Reference<CommitProxyInfo> cpInfo =
			wait(cx->getCommitProxiesFuture(UseProvisionalProxies::False));
		auto const& commitProxy = cpInfo->getInterface(cpInfo->getBest());
		state CommitTransactionRequest req;
		state MutationRef mut;
		auto& arena = req.arena;
		mut.type = MutationRef::SetValue;
		mut.param1 = Str
		req.transaction.mutations = VectorRef<MutationRef>(&mut, 1);
		commitProxy.commit.send(req);
		req.reply
		clientInfo
		*/
	}

	ACTOR Future<Void> runTestClient(Database cx, AuthzSecurityWorkload* self) {
		try {
			wait(testCrossTenantGet(cx, self));
		} catch (Error& e) {
			TraceEvent(SevError, "AuthzSecurityClient").error(e);
			throw;
		}
	}
};

WorkloadFactory<AuthzSecurityWorkload> AuthzSecurityWorkloadFactory("AuthzSecurity", true);
