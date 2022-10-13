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
#include <unordered_set>

#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

FDB_BOOLEAN_PARAM(PositiveTestcase);

struct AuthzSecurityWorkload : TestWorkload {
	int actorCount;
	double testDuration;

	std::vector<Future<Void>> clients;
	Arena arena;
	TenantName tenant;
	TenantName anotherTenant;
	Standalone<StringRef> signedToken;
	Standalone<StringRef> signedTokenAnotherTenant;
	Standalone<StringRef> tLogConfigKey;
	PerfIntCounter crossTenantGetPositive, crossTenantGetNegative, crossTenantCommitPositive, crossTenantCommitNegative,
	    publicNonTenantRequestPositive, tLogReadNegative;
	std::vector<std::function<Future<Void>(Database cx)>> testFunctions;

	AuthzSecurityWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), crossTenantGetPositive("CrossTenantGetPositive"),
	    crossTenantGetNegative("CrossTenantGetNegative"), crossTenantCommitPositive("CrossTenantCommitPositive"),
	    crossTenantCommitNegative("CrossTenantCommitNegative"),
	    publicNonTenantRequestPositive("PublicNonTenantRequestPositive"), tLogReadNegative("TLogReadNegative") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		actorCount = getOption(options, "actorsPerClient"_sr, 1);
		tenant = getOption(options, "tenantA"_sr, "authzSecurityTestTenant"_sr);
		anotherTenant = getOption(options, "tenantB"_sr, "authzSecurityTestTenant"_sr);
		tLogConfigKey = getOption(options, "tLogConfigKey"_sr, "TLogInterface"_sr);
		ASSERT(g_network->isSimulated());
		// make it comfortably longer than the timeout of the workload
		signedToken = g_simulator->makeToken(
		    tenant, uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100);
		signedTokenAnotherTenant = g_simulator->makeToken(
		    anotherTenant, uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100);
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantGetDisallowed(PositiveTestcase::True, cx, this); });
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantGetDisallowed(PositiveTestcase::False, cx, this); });
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantCommitDisallowed(PositiveTestcase::True, cx, this); });
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantCommitDisallowed(PositiveTestcase::False, cx, this); });
		testFunctions.push_back(
		    [this](Database cx) { return testPublicNonTenantRequestsAllowedWithoutTokens(cx, this); });
	}

	std::string description() const override { return "AuthzSecurityWorkload"; }

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
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
		return errors == 0 && crossTenantGetPositive.getValue() > 0 && crossTenantGetNegative.getValue() > 0 &&
		       crossTenantCommitPositive.getValue() > 0 && crossTenantCommitNegative.getValue() > 0 &&
		       publicNonTenantRequestPositive.getValue() > 0;
	}
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(crossTenantGetPositive.getMetric());
		m.push_back(crossTenantGetNegative.getMetric());
		m.push_back(crossTenantCommitPositive.getMetric());
		m.push_back(crossTenantCommitNegative.getMetric());
		m.push_back(publicNonTenantRequestPositive.getMetric());
		m.push_back(tLogReadNegative.getMetric());
	}

	void setAuthToken(Transaction& tr, Standalone<StringRef> token) {
		tr.setOption(FDBTransactionOptions::AUTHORIZATION_TOKEN, token);
	}

	ACTOR Future<Void> setKeyValue(Transaction* tr, StringRef key, StringRef value) {
		loop {
			try {
				tr->set(key, value);
				wait(tr->commit());
				tr->reset();
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		return Void();
	}

	Standalone<StringRef> randomString() {
		auto const minLength = tLogConfigKey.size() + 1;
		return StringRef(
		    deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(minLength, minLength + 100)));
	}

	ACTOR Future<Optional<Error>> tryGetValue(TenantName tenant,
	                                          Version committedVersion,
	                                          Standalone<StringRef> key,
	                                          Optional<Standalone<StringRef>> expectedValue,
	                                          Standalone<StringRef> token,
	                                          Database cx,
	                                          AuthzSecurityWorkload* self) {
		loop {
			auto loc = cx->getCachedLocation(tenant, key);
			ASSERT(loc.present());
			auto const& tenantEntry = loc.get().tenantEntry;
			ASSERT(!tenantEntry.prefix.empty());
			GetValueRequest req;
			req.key = key;
			req.version = committedVersion;
			req.tenantInfo.tenantId = tenantEntry.id;
			req.tenantInfo.name = tenant;
			req.tenantInfo.token = token;
			try {
				GetValueReply reply = wait(loadBalance(loc.get().locations->locations(),
				                                       &StorageServerInterface::getValue,
				                                       req,
				                                       TaskPriority::DefaultPromiseEndpoint,
				                                       AtMostOnce::False,
				                                       nullptr));
				// test may fail before here, but if it does, the value should match
				if (reply.value != expectedValue) {
					TraceEvent(SevError, "AuthzSecurityUnmatchedValue")
					    .detail("Expected", expectedValue)
					    .detail("Actual", reply.value)
					    .log();
				}
				break;
			} catch (Error& e) {
				CODE_PROBE(e.code() == error_code_permission_denied, "Cross tenant get meets permission_denied");
				return e;
			}
		}
		return Optional<Error>();
	}

	ACTOR Future<Void> testCrossTenantGetDisallowed(PositiveTestcase positive,
	                                                Database cx,
	                                                AuthzSecurityWorkload* self) {
		state Transaction tr(cx, self->tenant);
		state Key key = self->randomString();
		state Value value = self->randomString();
		self->setAuthToken(tr, self->signedToken);
		wait(self->setKeyValue(&tr, key, value));
		Optional<Error> outcome = wait(self->tryGetValue(
		    self->tenant, tr.getCommittedVersion(), key, value, self->signedTokenAnotherTenant, cx, self));
		if (positive) {
			// Supposed to succeed. Expected to occasionally fail because of buggify, faultInjection, or data
			// distribution, but should not return permission_denied
			Optional<Error> outcome = wait(
			    self->tryGetValue(self->tenant, tr.getCommittedVersion(), key, value, self->signedToken, cx, self));
			if (!outcome.present()) {
				++self->crossTenantGetPositive;
			} else if (outcome.get().code() == error_code_permission_denied) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenantGetDisallowed")
				    .detail("Subcase", "Positive")
				    .log();
			}
		} else {
			// Should always fail. Expected to return permission_denied, but expected to occasionally fail with
			// different errors
			if (!outcome.present()) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenantGetDisallowed")
				    .detail("Subcase", "Negative")
				    .log();
			} else if (outcome.get().code() == error_code_permission_denied) {
				++self->crossTenantGetNegative;
			}
		}
		return Void();
	}

	ACTOR Future<Optional<Error>> tryCommit(TenantName tenant,
	                                        Standalone<StringRef> token,
	                                        Key key,
	                                        Value newValue,
	                                        Version readVersion,
	                                        Database cx,
	                                        AuthzSecurityWorkload* self) {
		loop {
			state Transaction tr(cx);
			Version freshReadVersion = wait(tr.getReadVersion());
			auto loc = cx->getCachedLocation(tenant, key);
			ASSERT(loc.present());
			auto const& tenantEntry = loc.get().tenantEntry;
			ASSERT(!tenantEntry.prefix.empty());
			state Key prefixedKey = key.withPrefix(tenantEntry.prefix);
			CommitTransactionRequest req;
			req.transaction.mutations.push_back(req.arena, MutationRef(MutationRef::SetValue, prefixedKey, newValue));
			req.transaction.read_snapshot = std::max(readVersion, freshReadVersion);
			req.tenantInfo.name = tenant;
			req.tenantInfo.token = token;
			req.tenantInfo.tenantId = tenantEntry.id;
			try {
				CommitID reply = wait(basicLoadBalance(cx->getCommitProxies(UseProvisionalProxies::False),
				                                       &CommitProxyInterface::commit,
				                                       req,
				                                       TaskPriority::DefaultPromiseEndpoint,
				                                       AtMostOnce::False));
				return Optional<Error>();
			} catch (Error& e) {
				CODE_PROBE(e.code() == error_code_permission_denied, "Cross tenant commit meets permission_denied");
				return e;
			}
		}
	}

	ACTOR Future<Void> testCrossTenantCommitDisallowed(PositiveTestcase positive,
	                                                   Database cx,
	                                                   AuthzSecurityWorkload* self) {
		state Transaction tr(cx, self->tenant);
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Value newValue = self->randomString();
		self->setAuthToken(tr, self->signedToken);
		wait(self->setKeyValue(&tr, key, value));
		if (positive) {
			// Expected to succeed, may occasionally fail
			Optional<Error> outcome = wait(
			    self->tryCommit(self->tenant, self->signedToken, key, newValue, tr.getCommittedVersion(), cx, self));
			if (!outcome.present()) {
				++self->crossTenantCommitPositive;
			} else if (outcome.get().code() == error_code_permission_denied) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenantGetDisallowed")
				    .detail("Subcase", "Positive")
				    .log();
			}
		} else {
			Optional<Error> outcome = wait(self->tryCommit(
			    self->tenant, self->signedTokenAnotherTenant, key, newValue, tr.getCommittedVersion(), cx, self));
			if (!outcome.present()) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenantGetDisallowed")
				    .detail("Subcase", "Negative")
				    .log();
			} else if (outcome.get().code() == error_code_permission_denied) {
				++self->crossTenantCommitNegative;
			}
		}
		return Void();
	}

	ACTOR Future<Void> testPublicNonTenantRequestsAllowedWithoutTokens(Database cx, AuthzSecurityWorkload* self) {
		state Transaction tr(cx, self->tenant);
		Version version = wait(tr.getReadVersion());
		(void)version;
		++self->publicNonTenantRequestPositive;
		return Void();
	}

	ACTOR Future<Void> testTLogReadDisallowed(Database cx, AuthzSecurityWorkload* self) {
		state Transaction tr(cx, self->tenant);
		state Key key = self->randomString();
		state Value value = self->randomString();
		self->setAuthToken(tr, self->signedToken);
		wait(self->setKeyValue(&tr, key, value));
		state Version committedVersion = tr.getCommittedVersion();
		tr.reset();
		self->setAuthToken(tr, self->signedToken);
		state Optional<Value> tLogConfigString = wait(tr.get(self->tLogConfigKey));
		ASSERT(tLogConfigString.present());
		state LogSystemConfig logSystemConfig =
		    ObjectReader::fromStringRef<LogSystemConfig>(tLogConfigString.get(), IncludeVersion());
		state std::vector<TLogInterface> logs = logSystemConfig.allPresentLogs();
		state std::vector<Future<TLogPeekReply>> replies;
		for (const auto& log : logs) {
			replies.push_back(
			    log.peekMessages.getReply(TLogPeekRequest(committedVersion, Tag(0, committedVersion), false, false)));
		}
		waitForAll(replies);
		for (auto i = 0u; i < logs.size(); i++) {
			if (!replies[i].isError()) {
				TraceEvent(SevError, "AuthzExpectedErrorNotFound").detail("TLogIndex", i).log();
			} else {
				Error const& e = replies[i].getError();
				if (e.code() != error_code_unauthorized_attempt && e.code() != error_code_actor_cancelled) {
					TraceEvent(SevError, "AuthzSecurityUnexpectedError").detail("Error", e.name()).log();
				}
			}
		}
		++self->tLogReadNegative;
		return Void();
	}

	ACTOR Future<Void> runTestClient(Database cx, AuthzSecurityWorkload* self) {
		try {
			loop { wait(deterministicRandom()->randomChoice(self->testFunctions)(cx)); }
			// loop { wait(self->testTLogReadDisallowed(cx, self)); }
		} catch (Error& e) {
			TraceEvent(SevError, "AuthzSecurityClient").error(e);
			throw;
		}
	}
};

WorkloadFactory<AuthzSecurityWorkload> AuthzSecurityWorkloadFactory("AuthzSecurity", true);
