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
	static constexpr auto NAME = "AuthzSecurity";
	int actorCount;
	double testDuration, transactionsPerSecond;

	std::vector<Future<Void>> clients;
	Arena arena;
	Reference<Tenant> tenant;
	TenantName tenantName;
	TenantName anotherTenantName;
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
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 500.0) / clientCount;
		actorCount = getOption(options, "actorsPerClient"_sr, transactionsPerSecond / 5);
		tenantName = getOption(options, "tenantA"_sr, "authzSecurityTestTenant"_sr);
		anotherTenantName = getOption(options, "tenantB"_sr, "authzSecurityTestTenant"_sr);
		tLogConfigKey = getOption(options, "tLogConfigKey"_sr, "TLogInterface"_sr);
		ASSERT(g_network->isSimulated());
		// make it comfortably longer than the timeout of the workload
		signedToken = g_simulator->makeToken(
		    tenantName, uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100);
		signedTokenAnotherTenant = g_simulator->makeToken(
		    anotherTenantName, uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100);
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantGetDisallowed(this, cx, PositiveTestcase::True); });
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantGetDisallowed(this, cx, PositiveTestcase::False); });
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantCommitDisallowed(this, cx, PositiveTestcase::True); });
		testFunctions.push_back(
		    [this](Database cx) { return testCrossTenantCommitDisallowed(this, cx, PositiveTestcase::False); });
		testFunctions.push_back(
		    [this](Database cx) { return testPublicNonTenantRequestsAllowedWithoutTokens(this, cx); });
		testFunctions.push_back([this](Database cx) { return testTLogReadDisallowed(this, cx); });
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		for (int c = 0; c < actorCount; c++)
			clients.push_back(timeout(runTestClient(this, cx->clone()), testDuration, Void()));
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
		       publicNonTenantRequestPositive.getValue() > 0 && tLogReadNegative.getValue() > 0;
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

	ACTOR static Future<Version> setAndCommitKeyValueAndGetVersion(AuthzSecurityWorkload* self,
	                                                               Database cx,
	                                                               TenantName tenant,
	                                                               Standalone<StringRef> token,
	                                                               StringRef key,
	                                                               StringRef value) {
		state Transaction tr(cx, tenant);
		self->setAuthToken(tr, token);
		loop {
			try {
				tr.set(key, value);
				wait(tr.commit());
				return tr.getCommittedVersion();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<KeyRangeLocationInfo> refreshAndGetCachedLocation(AuthzSecurityWorkload* self,
	                                                                      Database cx,
	                                                                      Reference<Tenant> tenant,
	                                                                      Standalone<StringRef> token,
	                                                                      StringRef key) {
		state Transaction tr(cx, tenant);
		self->setAuthToken(tr, token);
		loop {
			try {
				// trigger GetKeyServerLocationsRequest and subsequent cache update
				wait(success(tr.get(key)));
				auto loc = cx->getCachedLocation(tr.trState->getTenantInfo(), key);
				if (loc.present()) {
					return loc.get();
				} else {
					wait(delay(0.1));
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	Standalone<StringRef> randomString() {
		auto const minLength = tLogConfigKey.size() + 1;
		return StringRef(
		    deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(minLength, minLength + 100)));
	}

	ACTOR static Future<Optional<Error>> tryGetValue(AuthzSecurityWorkload* self,
	                                                 Reference<Tenant> tenant,
	                                                 Version committedVersion,
	                                                 Standalone<StringRef> key,
	                                                 Optional<Standalone<StringRef>> expectedValue,
	                                                 Standalone<StringRef> token,
	                                                 Database cx,
	                                                 KeyRangeLocationInfo loc) {
		loop {
			GetValueRequest req;
			req.key = key;
			req.version = committedVersion;
			req.tenantInfo.tenantId = tenant->id();
			req.tenantInfo.name = tenant->name.get();
			req.tenantInfo.token = token;
			try {
				GetValueReply reply = wait(loadBalance(loc.locations->locations(),
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

	ACTOR static Future<Void> testCrossTenantGetDisallowed(AuthzSecurityWorkload* self,
	                                                       Database cx,
	                                                       PositiveTestcase positive) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Version committedVersion =
		    wait(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value));
		// refresh key location cache via get()
		KeyRangeLocationInfo loc = wait(refreshAndGetCachedLocation(self, cx, self->tenant, self->signedToken, key));
		if (positive) {
			// Supposed to succeed. Expected to occasionally fail because of buggify, faultInjection, or data
			// distribution, but should not return permission_denied
			Optional<Error> outcome = wait(tryGetValue(self,
			                                           self->tenant,
			                                           committedVersion,
			                                           key,
			                                           value,
			                                           self->signedToken /* passing correct token */,
			                                           cx,
			                                           loc));
			if (!outcome.present()) {
				++self->crossTenantGetPositive;
			} else if (outcome.get().code() == error_code_permission_denied) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenantGetDisallowed")
				    .detail("Subcase", "Positive")
				    .log();
			}
		} else {
			Optional<Error> outcome =
			    wait(tryGetValue(self,
			                     self->tenant,
			                     committedVersion,
			                     key,
			                     value,
			                     self->signedTokenAnotherTenant /* deliberately passing bad token */,
			                     cx,
			                     loc));
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

	ACTOR static Future<Optional<Error>> tryCommit(AuthzSecurityWorkload* self,
	                                               Reference<Tenant> tenant,
	                                               Standalone<StringRef> token,
	                                               Key key,
	                                               Value newValue,
	                                               Version readVersion,
	                                               Database cx) {
		loop {
			state Key prefixedKey = key.withPrefix(tenant->prefix());
			CommitTransactionRequest req;
			req.transaction.mutations.push_back(req.arena, MutationRef(MutationRef::SetValue, prefixedKey, newValue));
			req.transaction.read_snapshot = readVersion;
			req.tenantInfo.name = tenant->name.get();
			req.tenantInfo.token = token;
			req.tenantInfo.tenantId = tenant->id();
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

	ACTOR static Future<Void> testCrossTenantCommitDisallowed(AuthzSecurityWorkload* self,
	                                                          Database cx,
	                                                          PositiveTestcase positive) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Value newValue = self->randomString();
		state Version committedVersion =
		    wait(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value));
		if (positive) {
			// Expected to succeed, may occasionally fail
			Optional<Error> outcome =
			    wait(tryCommit(self, self->tenant, self->signedToken, key, newValue, committedVersion, cx));
			if (!outcome.present()) {
				++self->crossTenantCommitPositive;
			} else if (outcome.get().code() == error_code_permission_denied) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenantGetDisallowed")
				    .detail("Subcase", "Positive")
				    .log();
			}
		} else {
			Optional<Error> outcome = wait(
			    tryCommit(self, self->tenant, self->signedTokenAnotherTenant, key, newValue, committedVersion, cx));
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

	ACTOR static Future<Void> testPublicNonTenantRequestsAllowedWithoutTokens(AuthzSecurityWorkload* self,
	                                                                          Database cx) {
		state Transaction tr(cx, self->tenant);
		loop {
			try {
				Version version = wait(tr.getReadVersion());
				(void)version;
				++self->publicNonTenantRequestPositive;
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> testTLogReadDisallowed(AuthzSecurityWorkload* self, Database cx) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Version committedVersion =
		    wait(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value));
		state Transaction tr(cx, self->tenant);
		self->setAuthToken(tr, self->signedToken);
		state Optional<Value> tLogConfigString;
		loop {
			try {
				Optional<Value> value = wait(tr.get(self->tLogConfigKey));
				ASSERT(value.present());
				tLogConfigString = value;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		ASSERT(tLogConfigString.present());
		state LogSystemConfig logSystemConfig =
		    ObjectReader::fromStringRef<LogSystemConfig>(tLogConfigString.get(), IncludeVersion());
		state std::vector<TLogInterface> logs = logSystemConfig.allPresentLogs();
		state std::vector<Future<ErrorOr<TLogPeekReply>>> replies;
		for (const auto& log : logs) {
			replies.push_back(log.peekMessages.tryGetReply(
			    TLogPeekRequest(committedVersion, Tag(0, committedVersion), false, false)));
		}
		wait(waitForAllReady(replies));
		for (auto i = 0u; i < logs.size(); i++) {
			const auto& reply = replies[i];
			ASSERT(reply.isValid());
			if (reply.canGet()) {
				ErrorOr<TLogPeekReply> r = reply.getValue();
				if (!r.isError()) {
					const TLogPeekReply& rpcReply = r.get();
					TraceEvent(SevError, "AuthzExpectedErrorNotFound")
					    .detail("TLogIndex", i)
					    .detail("Messages", rpcReply.messages.toString())
					    .detail("End", rpcReply.end)
					    .detail("Popped", rpcReply.popped)
					    .detail("MaxKnownVersion", rpcReply.maxKnownVersion)
					    .detail("MinKnownCommitVersion", rpcReply.minKnownCommittedVersion)
					    .detail("Begin", rpcReply.begin)
					    .detail("OnlySpilled", rpcReply.onlySpilled)
					    .log();
				} else {
					Error e = r.getError();
					if (e.code() == error_code_unauthorized_attempt) {
						++self->tLogReadNegative;
					} else if (e.code() != error_code_actor_cancelled &&
					           e.code() != error_code_request_maybe_delivered) {
						TraceEvent(SevError, "AuthzSecurityUnexpectedError").detail("Error", e.name()).log();
					}
				}
			} else {
				TraceEvent(SevError, "AuthzSecurityUnexpectedError").detail("Error", reply.getError().name()).log();
			}
		}
		return Void();
	}

	ACTOR static Future<Void> runTestClient(AuthzSecurityWorkload* self, Database cx) {
		state double lastTime = now();
		state double delay = self->actorCount / self->transactionsPerSecond;
		try {
			loop {
				wait(poisson(&lastTime, delay));
				wait(deterministicRandom()->randomChoice(self->testFunctions)(cx));
			}
		} catch (Error& e) {
			TraceEvent(SevError, "AuthzSecurityClient").error(e);
			throw;
		}
	}
};

WorkloadFactory<AuthzSecurityWorkload> AuthzSecurityWorkloadFactory(UntrustedMode::True);
