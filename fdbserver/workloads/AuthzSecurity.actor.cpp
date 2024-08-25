/*
 * AuthzSecurity.actor.cpp
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

#include <cstring>
#include <unordered_set>

#include "fdbclient/BlobWorkerInterface.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/WipedString.h"
#include "flow/serialize.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

FDB_BOOLEAN_PARAM(PositiveTestcase);

bool checkGranuleLocations(ErrorOr<GetBlobGranuleLocationsReply> rep, TenantInfo tenant) {
	if (rep.isError()) {
		if (rep.getError().code() == error_code_permission_denied) {
			TraceEvent(SevError, "AuthzSecurityError")
			    .detail("Case", "CrossTenantGranuleLocationCheckDisallowed")
			    .log();
		}
		return false;
	} else {
		ASSERT(!rep.get().results.empty());
		for (auto const& [range, bwIface] : rep.get().results) {
			if (!range.begin.startsWith(tenant.prefix.get())) {
				TraceEvent(SevError, "AuthzSecurityBlobGranuleRangeLeak")
				    .detail("TenantId", tenant.tenantId)
				    .detail("LeakingRangeBegin", range.begin.printable());
			}
			if (!range.end.startsWith(tenant.prefix.get())) {
				TraceEvent(SevError, "AuthzSecurityBlobGranuleRangeLeak")
				    .detail("TenantId", tenant.tenantId)
				    .detail("LeakingRangeEnd", range.end.printable());
			}
		}
		return true;
	}
}

struct AuthzSecurityWorkload : TestWorkload {
	static constexpr auto NAME = "AuthzSecurity";
	int actorCount;
	double testDuration, transactionsPerSecond;

	std::vector<Future<Void>> clients;
	Arena arena;
	Reference<Tenant> tenant;
	Reference<Tenant> anotherTenant;
	TenantName tenantName;
	TenantName anotherTenantName;
	WipedString signedToken;
	WipedString signedTokenAnotherTenant;
	Standalone<StringRef> tLogConfigKey;
	PerfIntCounter crossTenantGetPositive, crossTenantGetNegative, crossTenantCommitPositive, crossTenantCommitNegative,
	    publicNonTenantRequestPositive, tLogReadNegative, keyLocationLeakNegative, bgLocationLeakNegative,
	    crossTenantBGLocPositive, crossTenantBGLocNegative, crossTenantBGReqPositive, crossTenantBGReqNegative,
	    crossTenantBGReadPositive, crossTenantBGReadNegative, crossTenantGetGranulesPositive,
	    crossTenantGetGranulesNegative, blobbifyNegative, unblobbifyNegative, listBlobNegative, verifyBlobNegative,
	    flushBlobNegative, purgeBlobNegative;
	std::vector<std::function<Future<Void>(Database cx)>> testFunctions;
	bool checkBlobGranules, checkBlobManagement;

	AuthzSecurityWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), crossTenantGetPositive("CrossTenantGetPositive"),
	    crossTenantGetNegative("CrossTenantGetNegative"), crossTenantCommitPositive("CrossTenantCommitPositive"),
	    crossTenantCommitNegative("CrossTenantCommitNegative"),
	    publicNonTenantRequestPositive("PublicNonTenantRequestPositive"), tLogReadNegative("TLogReadNegative"),
	    keyLocationLeakNegative("KeyLocationLeakNegative"), bgLocationLeakNegative("BGLocationLeakNegative"),
	    crossTenantBGLocPositive("CrossTenantBGLocPositive"), crossTenantBGLocNegative("CrossTenantBGLocNegative"),
	    crossTenantBGReqPositive("CrossTenantBGReqPositive"), crossTenantBGReqNegative("CrossTenantBGReqNegative"),
	    crossTenantBGReadPositive("CrossTenantBGReadPositive"), crossTenantBGReadNegative("CrossTenantBGReadNegative"),
	    crossTenantGetGranulesPositive("CrossTenantGetGranulesPositive"),
	    crossTenantGetGranulesNegative("CrossTenantGetGranulesNegative"), blobbifyNegative("BlobbifyNegative"),
	    unblobbifyNegative("UnblobbifyNegative"), listBlobNegative("ListBlobNegative"),
	    verifyBlobNegative("VerifyBlobNegative"), flushBlobNegative("FlushBlobNegative"),
	    purgeBlobNegative("PurgeBlobNegative") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 500.0) / clientCount;
		actorCount = getOption(options, "actorsPerClient"_sr, transactionsPerSecond / 5);
		tenantName = getOption(options, "tenantA"_sr, "authzSecurityTestTenant"_sr);
		anotherTenantName = getOption(options, "tenantB"_sr, "authzSecurityTestTenant"_sr);
		tLogConfigKey = getOption(options, "tLogConfigKey"_sr, "TLogInterface"_sr);
		checkBlobGranules = getOption(options, "checkBlobGranules"_sr, false);
		checkBlobManagement =
		    checkBlobGranules && getOption(options, "checkBlobManagement"_sr, sharedRandomNumber % 2 == 0);
		sharedRandomNumber /= 2;

		ASSERT(g_network->isSimulated());
		// make it comfortably longer than the timeout of the workload
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
		testFunctions.push_back([this](Database cx) { return testKeyLocationLeakDisallowed(this, cx); });

		if (checkBlobGranules) {
			testFunctions.push_back([this](Database cx) { return testBlobGranuleLocationLeakDisallowed(this, cx); });
			testFunctions.push_back(
			    [this](Database cx) { return testCrossTenantBGLocDisallowed(this, cx, PositiveTestcase::True); });
			testFunctions.push_back(
			    [this](Database cx) { return testCrossTenantBGLocDisallowed(this, cx, PositiveTestcase::False); });
			testFunctions.push_back(
			    [this](Database cx) { return testCrossTenantBGRequestDisallowed(this, cx, PositiveTestcase::True); });
			testFunctions.push_back(
			    [this](Database cx) { return testCrossTenantBGRequestDisallowed(this, cx, PositiveTestcase::False); });
			testFunctions.push_back(
			    [this](Database cx) { return testCrossTenantBGReadDisallowed(this, cx, PositiveTestcase::True); });
			testFunctions.push_back(
			    [this](Database cx) { return testCrossTenantBGReadDisallowed(this, cx, PositiveTestcase::False); });
			testFunctions.push_back(
			    [this](Database cx) { return testCrossTenantGetGranulesDisallowed(this, cx, PositiveTestcase::True); });
			testFunctions.push_back([this](Database cx) {
				return testCrossTenantGetGranulesDisallowed(this, cx, PositiveTestcase::False);
			});
		}
		if (checkBlobManagement) {
			testFunctions.push_back([this](Database cx) { return testBlobbifyDisallowed(this, cx); });
			testFunctions.push_back([this](Database cx) { return testUnblobbifyDisallowed(this, cx); });
			testFunctions.push_back([this](Database cx) { return testListBlobDisallowed(this, cx); });
			testFunctions.push_back([this](Database cx) { return testVerifyBlobDisallowed(this, cx); });
			testFunctions.push_back([this](Database cx) { return testFlushBlobDisallowed(this, cx); });
			testFunctions.push_back([this](Database cx) { return testPurgeBlobDisallowed(this, cx); });
		}
	}

	Future<Void> setup(Database const& cx) override {
		tenant = makeReference<Tenant>(cx, tenantName);
		anotherTenant = makeReference<Tenant>(cx, anotherTenantName);
		return tenant->ready() && anotherTenant->ready();
	}

	Future<Void> start(Database const& cx) override {
		signedToken = g_simulator->makeToken(
		    tenant->id(), uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100);
		signedTokenAnotherTenant = g_simulator->makeToken(
		    anotherTenant->id(), uint64_t(std::lround(getCheckTimeout())) + uint64_t(std::lround(testDuration)) + 100);
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
		bool success = errors == 0 && crossTenantGetPositive.getValue() > 0 && crossTenantGetNegative.getValue() > 0 &&
		               crossTenantCommitPositive.getValue() > 0 && crossTenantCommitNegative.getValue() > 0 &&
		               publicNonTenantRequestPositive.getValue() > 0 && tLogReadNegative.getValue() > 0 &&
		               keyLocationLeakNegative.getValue() > 0;
		if (checkBlobGranules) {
			success &= bgLocationLeakNegative.getValue() > 0 && crossTenantBGLocPositive.getValue() > 0 &&
			           crossTenantBGLocNegative.getValue() > 0 && crossTenantBGReqPositive.getValue() > 0 &&
			           crossTenantBGReqNegative.getValue() > 0 && crossTenantBGReadPositive.getValue() > 0 &&
			           crossTenantBGReadNegative.getValue() > 0 && crossTenantGetGranulesPositive.getValue() > 0 &&
			           crossTenantGetGranulesNegative.getValue() > 0;
		}
		if (checkBlobManagement) {
			success &= blobbifyNegative.getValue() > 0 && unblobbifyNegative.getValue() > 0 &&
			           listBlobNegative.getValue() > 0 && verifyBlobNegative.getValue() > 0 &&
			           flushBlobNegative.getValue() > 0 && purgeBlobNegative.getValue() > 0;
		}
		return success;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(crossTenantGetPositive.getMetric());
		m.push_back(crossTenantGetNegative.getMetric());
		m.push_back(crossTenantCommitPositive.getMetric());
		m.push_back(crossTenantCommitNegative.getMetric());
		m.push_back(publicNonTenantRequestPositive.getMetric());
		m.push_back(tLogReadNegative.getMetric());
		m.push_back(keyLocationLeakNegative.getMetric());
		if (checkBlobGranules) {
			m.push_back(bgLocationLeakNegative.getMetric());
			m.push_back(crossTenantBGLocPositive.getMetric());
			m.push_back(crossTenantBGLocNegative.getMetric());
			m.push_back(crossTenantBGReqPositive.getMetric());
			m.push_back(crossTenantBGReqNegative.getMetric());
			m.push_back(crossTenantBGReadPositive.getMetric());
			m.push_back(crossTenantBGReadNegative.getMetric());
			m.push_back(crossTenantGetGranulesPositive.getMetric());
			m.push_back(crossTenantGetGranulesNegative.getMetric());
		}
		if (checkBlobManagement) {
			m.push_back(blobbifyNegative.getMetric());
			m.push_back(unblobbifyNegative.getMetric());
			m.push_back(listBlobNegative.getMetric());
			m.push_back(verifyBlobNegative.getMetric());
			m.push_back(flushBlobNegative.getMetric());
			m.push_back(purgeBlobNegative.getMetric());
		}
	}

	void setAuthToken(Transaction& tr, StringRef token) {
		tr.setOption(FDBTransactionOptions::AUTHORIZATION_TOKEN, token);
	}

	ACTOR static Future<Version> setAndCommitKeyValueAndGetVersion(AuthzSecurityWorkload* self,
	                                                               Database cx,
	                                                               Reference<Tenant> tenant,
	                                                               WipedString token,
	                                                               StringRef key,
	                                                               StringRef value) {
		state Transaction tr(cx, tenant);
		loop {
			try {
				self->setAuthToken(tr, token);
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
	                                                                      WipedString token,
	                                                                      StringRef key) {
		state Transaction tr(cx, tenant);
		loop {
			try {
				// trigger GetKeyServerLocationsRequest and subsequent cache update
				self->setAuthToken(tr, token);
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
	                                                 WipedString token,
	                                                 Database cx,
	                                                 KeyRangeLocationInfo loc) {
		loop {
			GetValueRequest req;
			req.key = key;
			req.version = committedVersion;
			req.tenantInfo.tenantId = tenant->id();
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
	                                               WipedString token,
	                                               Key key,
	                                               Value newValue,
	                                               Version readVersion,
	                                               Database cx) {
		loop {
			state Key prefixedKey = key.withPrefix(tenant->prefix());
			CommitTransactionRequest req;
			req.transaction.mutations.push_back(req.arena, MutationRef(MutationRef::SetValue, prefixedKey, newValue));
			req.transaction.read_snapshot = readVersion;
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
				    .detail("Case", "CrossTenantCommitDisallowed")
				    .detail("Subcase", "Positive")
				    .log();
			}
		} else {
			Optional<Error> outcome = wait(
			    tryCommit(self, self->tenant, self->signedTokenAnotherTenant, key, newValue, committedVersion, cx));
			if (!outcome.present()) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenantCommitDisallowed")
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
		state Optional<Value> tLogConfigString;
		loop {
			try {
				self->setAuthToken(tr, self->signedToken);
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

	ACTOR static Future<Void> testKeyLocationLeakDisallowed(AuthzSecurityWorkload* self, Database cx) {
		state Key key = self->randomString();
		state Value value = self->randomString();

		wait(success(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value)));
		state Version v2 = wait(setAndCommitKeyValueAndGetVersion(
		    self, cx, self->anotherTenant, self->signedTokenAnotherTenant, key, value));

		try {
			GetKeyServerLocationsReply rep =
			    wait(basicLoadBalance(cx->getCommitProxies(UseProvisionalProxies::False),
			                          &CommitProxyInterface::getKeyServersLocations,
			                          GetKeyServerLocationsRequest(SpanContext(),
			                                                       TenantInfo(self->tenant->id(), self->signedToken),
			                                                       key,
			                                                       Optional<KeyRef>(),
			                                                       100,
			                                                       false,
			                                                       v2,
			                                                       Arena())));
			for (auto const& [range, ssIfaces] : rep.results) {
				if (!range.begin.startsWith(self->tenant->prefix())) {
					TraceEvent(SevError, "AuthzSecurityKeyRangeLeak")
					    .detail("TenantId", self->tenant->id())
					    .detail("LeakingRangeBegin", range.begin.printable());
				}
				if (!range.end.startsWith(self->tenant->prefix())) {
					TraceEvent(SevError, "AuthzSecurityKeyRangeLeak")
					    .detail("TenantId", self->tenant->id())
					    .detail("LeakingRangeEnd", range.end.printable());
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			ASSERT(e.code() == error_code_commit_proxy_memory_limit_exceeded);
		}
		try {
			GetKeyServerLocationsReply rep = wait(basicLoadBalance(
			    cx->getCommitProxies(UseProvisionalProxies::False),
			    &CommitProxyInterface::getKeyServersLocations,
			    GetKeyServerLocationsRequest(SpanContext(),
			                                 TenantInfo(self->anotherTenant->id(), self->signedTokenAnotherTenant),
			                                 key,
			                                 Optional<KeyRef>(),
			                                 100,
			                                 false,
			                                 v2,
			                                 Arena())));
			for (auto const& [range, ssIfaces] : rep.results) {
				if (!range.begin.startsWith(self->anotherTenant->prefix())) {
					TraceEvent(SevError, "AuthzSecurityKeyRangeLeak")
					    .detail("TenantId", self->anotherTenant->id())
					    .detail("LeakingRangeBegin", range.begin.printable());
				}
				if (!range.end.startsWith(self->anotherTenant->prefix())) {
					TraceEvent(SevError, "AuthzSecurityKeyRangeLeak")
					    .detail("TenantId", self->anotherTenant->id())
					    .detail("LeakingRangeEnd", range.end.printable());
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			ASSERT(e.code() == error_code_commit_proxy_memory_limit_exceeded);
		}
		++self->keyLocationLeakNegative;

		return Void();
	}

	ACTOR static Future<ErrorOr<GetBlobGranuleLocationsReply>> getGranuleLocations(AuthzSecurityWorkload* self,
	                                                                               Database cx,
	                                                                               TenantInfo tenant,
	                                                                               Version v) {
		try {
			GetBlobGranuleLocationsReply reply = wait(
			    basicLoadBalance(cx->getCommitProxies(UseProvisionalProxies::False),
			                     &CommitProxyInterface::getBlobGranuleLocations,
			                     GetBlobGranuleLocationsRequest(
			                         SpanContext(), tenant, ""_sr, Optional<KeyRef>(), 100, false, false, v, Arena())));
			return reply;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			CODE_PROBE(e.code() == error_code_permission_denied,
			           "Cross tenant blob granule locations meets permission_denied");
			return e;
		}
	}

	ACTOR static Future<Void> testBlobGranuleLocationLeakDisallowed(AuthzSecurityWorkload* self, Database cx) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Version v1 =
		    wait(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value));
		state Version v2 = wait(setAndCommitKeyValueAndGetVersion(
		    self, cx, self->anotherTenant, self->signedTokenAnotherTenant, key, value));

		state bool success = true;
		state TenantInfo tenantInfo;

		{
			tenantInfo = TenantInfo(self->tenant->id(), self->signedToken);
			ErrorOr<GetBlobGranuleLocationsReply> rep = wait(getGranuleLocations(self, cx, tenantInfo, v2));
			bool checkSuccess = checkGranuleLocations(rep, tenantInfo);
			success &= checkSuccess;
		}
		{
			tenantInfo = TenantInfo(self->anotherTenant->id(), self->signedTokenAnotherTenant);
			ErrorOr<GetBlobGranuleLocationsReply> rep = wait(getGranuleLocations(self, cx, tenantInfo, v2));
			bool checkSuccess = checkGranuleLocations(rep, tenantInfo);
			success &= checkSuccess;
		}
		if (success) {
			++self->bgLocationLeakNegative;
		}

		return Void();
	}

	ACTOR static Future<Optional<Error>> tryBlobGranuleRequest(AuthzSecurityWorkload* self,
	                                                           Database cx,
	                                                           Reference<Tenant> tenant,
	                                                           WipedString locToken,
	                                                           WipedString reqToken,
	                                                           Version committedVersion) {
		try {
			ErrorOr<GetBlobGranuleLocationsReply> rep =
			    wait(getGranuleLocations(self, cx, TenantInfo(tenant->id(), locToken), committedVersion));
			if (rep.isError()) {
				if (rep.getError().code() == error_code_permission_denied) {
					TraceEvent(SevError, "AuthzSecurityError")
					    .detail("Case", "GranuleLocBeforeRequestDisallowed")
					    .log();
				}
				throw rep.getError();
			}

			int locIdx = deterministicRandom()->randomInt(0, rep.get().results.size());

			ASSERT(!rep.get().results.empty());
			ASSERT(!rep.get().bwInterfs.empty());
			BlobGranuleFileRequest req;
			req.arena.dependsOn(rep.get().arena);
			req.keyRange = rep.get().results[locIdx].first;
			req.tenantInfo = TenantInfo(tenant->id(), reqToken);
			req.readVersion = committedVersion;

			UID bwId = rep.get().results[locIdx].second;
			ASSERT(bwId != UID());
			int bwInterfIdx;
			for (bwInterfIdx = 0; bwInterfIdx < rep.get().bwInterfs.size(); bwInterfIdx++) {
				if (rep.get().bwInterfs[bwInterfIdx].id() == bwId) {
					break;
				}
			}
			ASSERT(bwInterfIdx < rep.get().bwInterfs.size());
			auto& bwInterf = rep.get().bwInterfs[bwInterfIdx];
			ErrorOr<BlobGranuleFileReply> fileRep = wait(bwInterf.blobGranuleFileRequest.tryGetReply(req));
			if (fileRep.isError()) {
				throw fileRep.getError();
			}
			ASSERT(!fileRep.get().chunks.empty());

			return Optional<Error>();
		} catch (Error& e) {
			CODE_PROBE(e.code() == error_code_permission_denied,
			           "Cross tenant blob granule read meets permission_denied");
			return e;
		}
	}

	ACTOR static Future<Optional<Error>> tryBlobGranuleRead(AuthzSecurityWorkload* self,
	                                                        Database cx,
	                                                        Reference<Tenant> tenant,
	                                                        Key key,
	                                                        WipedString token,
	                                                        Version committedVersion) {
		state Transaction tr(cx, tenant);
		self->setAuthToken(tr, token);
		KeyRange range(KeyRangeRef(key, keyAfter(key)));
		try {
			wait(success(tr.readBlobGranules(range, 0, committedVersion)));
			return Optional<Error>();
		} catch (Error& e) {
			CODE_PROBE(e.code() == error_code_permission_denied,
			           "Cross tenant blob granule read meets permission_denied");
			return e;
		}
	}

	static void checkCrossTenantOutcome(std::string testcase,
	                                    PerfIntCounter& positiveCounter,
	                                    PerfIntCounter& negativeCounter,
	                                    Optional<Error> outcome,
	                                    PositiveTestcase positive) {
		if (positive) {
			// Supposed to succeed. Expected to occasionally fail because of buggify, faultInjection, or data
			// distribution, but should not return permission_denied
			if (!outcome.present()) {
				++positiveCounter;
			} else if (outcome.get().code() == error_code_permission_denied) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenant" + testcase + "Disallowed")
				    .detail("Subcase", "Positive")
				    .log();
			}
		} else {
			// Should always fail. Expected to return permission_denied, but expected to occasionally fail with
			// different errors
			if (!outcome.present()) {
				TraceEvent(SevError, "AuthzSecurityError")
				    .detail("Case", "CrossTenant" + testcase + "Disallowed")
				    .detail("Subcase", "Negative")
				    .log();
			} else if (outcome.get().code() == error_code_permission_denied) {
				++negativeCounter;
			}
		}
	}

	ACTOR static Future<Void> testCrossTenantBGLocDisallowed(AuthzSecurityWorkload* self,
	                                                         Database cx,
	                                                         PositiveTestcase positive) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Version committedVersion =
		    wait(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value));
		TenantInfo tenantInfo(self->tenant->id(), positive ? self->signedToken : self->signedTokenAnotherTenant);
		ErrorOr<GetBlobGranuleLocationsReply> rep = wait(getGranuleLocations(self, cx, tenantInfo, committedVersion));
		Optional<Error> outcome = rep.isError() ? rep.getError() : Optional<Error>();
		checkCrossTenantOutcome(
		    "BGLoc", self->crossTenantBGLocPositive, self->crossTenantBGLocNegative, outcome, positive);
		return Void();
	}

	ACTOR static Future<Void> testCrossTenantBGRequestDisallowed(AuthzSecurityWorkload* self,
	                                                             Database cx,
	                                                             PositiveTestcase positive) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Version committedVersion =
		    wait(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value));
		Optional<Error> outcome =
		    wait(tryBlobGranuleRequest(self,
		                               cx,
		                               self->tenant,
		                               self->signedToken,
		                               positive ? self->signedToken : self->signedTokenAnotherTenant,
		                               committedVersion));
		checkCrossTenantOutcome(
		    "BGRequest", self->crossTenantBGReqPositive, self->crossTenantBGReqNegative, outcome, positive);
		return Void();
	}

	ACTOR static Future<Void> testCrossTenantBGReadDisallowed(AuthzSecurityWorkload* self,
	                                                          Database cx,
	                                                          PositiveTestcase positive) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		state Version committedVersion =
		    wait(setAndCommitKeyValueAndGetVersion(self, cx, self->tenant, self->signedToken, key, value));
		Optional<Error> outcome = wait(tryBlobGranuleRead(self,
		                                                  cx,
		                                                  self->tenant,
		                                                  key,
		                                                  positive ? self->signedToken : self->signedTokenAnotherTenant,
		                                                  committedVersion));
		checkCrossTenantOutcome(
		    "BGRead", self->crossTenantBGReadPositive, self->crossTenantBGReadNegative, outcome, positive);
		return Void();
	}

	ACTOR static Future<Optional<Error>> tryGetGranules(AuthzSecurityWorkload* self,
	                                                    Database cx,
	                                                    Reference<Tenant> tenant,
	                                                    Key key,
	                                                    WipedString token) {
		state Transaction tr(cx, tenant);
		self->setAuthToken(tr, token);
		KeyRange range(KeyRangeRef(key, keyAfter(key)));
		try {
			Standalone<VectorRef<KeyRangeRef>> result = wait(tr.getBlobGranuleRanges(range, 1000));

			ASSERT(result.size() <= 1);
			if (!result.empty()) {
				ASSERT(result[0].contains(key));
			}
			return Optional<Error>();
		} catch (Error& e) {
			CODE_PROBE(e.code() == error_code_permission_denied, "Cross tenant get granules meets permission_denied");
			return e;
		}
	}

	ACTOR static Future<Void> testCrossTenantGetGranulesDisallowed(AuthzSecurityWorkload* self,
	                                                               Database cx,
	                                                               PositiveTestcase positive) {
		state Key key = self->randomString();
		state Value value = self->randomString();
		Optional<Error> outcome = wait(
		    tryGetGranules(self, cx, self->tenant, key, positive ? self->signedToken : self->signedTokenAnotherTenant));
		checkCrossTenantOutcome("GetGranules",
		                        self->crossTenantGetGranulesPositive,
		                        self->crossTenantGetGranulesNegative,
		                        outcome,
		                        positive);
		return Void();
	}

	ACTOR static Future<Void> checkBlobManagementNegative(AuthzSecurityWorkload* self,
	                                                      std::string opName,
	                                                      Future<Void> op,
	                                                      PerfIntCounter* counter) {
		try {
			wait(op);
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			if (e.code() == error_code_permission_denied) {
				++(*counter);
			} else {
				TraceEvent(SevError, "AuthzSecurityBlobManagementAllowed")
				    .detail("OpType", opName)
				    .detail("TenantId", self->tenant->id());
			}
		}
		return Void();
	}

	ACTOR static Future<Void> testBlobbifyDisallowed(AuthzSecurityWorkload* self, Database cx) {
		Future<Void> op;
		if (deterministicRandom()->coinflip()) {
			op = success(cx->blobbifyRange(normalKeys, self->tenant));
		} else {
			op = success(cx->blobbifyRangeBlocking(normalKeys, self->tenant));
		}
		wait(checkBlobManagementNegative(self, "Blobbify", op, &self->blobbifyNegative));
		return Void();
	}

	ACTOR static Future<Void> testUnblobbifyDisallowed(AuthzSecurityWorkload* self, Database cx) {
		wait(checkBlobManagementNegative(
		    self, "Unblobbify", success(cx->unblobbifyRange(normalKeys, self->tenant)), &self->unblobbifyNegative));
		return Void();
	}

	ACTOR static Future<Void> testListBlobDisallowed(AuthzSecurityWorkload* self, Database cx) {
		wait(checkBlobManagementNegative(self,
		                                 "ListBlob",
		                                 success(cx->listBlobbifiedRanges(normalKeys, 1000, self->tenant)),
		                                 &self->listBlobNegative));
		return Void();
	}

	ACTOR static Future<Void> testVerifyBlobDisallowed(AuthzSecurityWorkload* self, Database cx) {
		wait(checkBlobManagementNegative(
		    self, "VerifyBlob", success(cx->verifyBlobRange(normalKeys, {}, self->tenant)), &self->verifyBlobNegative));
		return Void();
	}

	ACTOR static Future<Void> testFlushBlobDisallowed(AuthzSecurityWorkload* self, Database cx) {
		wait(checkBlobManagementNegative(
		    self,
		    "FlushBlob",
		    success(cx->flushBlobRange(normalKeys, {}, deterministicRandom()->coinflip(), self->tenant)),
		    &self->flushBlobNegative));
		return Void();
	}

	ACTOR static Future<Void> testPurgeBlobDisallowed(AuthzSecurityWorkload* self, Database cx) {
		wait(checkBlobManagementNegative(
		    self,
		    "PurgeBlob",
		    success(cx->purgeBlobGranules(normalKeys, 1, self->tenant, deterministicRandom()->coinflip())),
		    &self->purgeBlobNegative));
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
