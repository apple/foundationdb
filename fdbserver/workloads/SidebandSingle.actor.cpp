/*
 * SidebandSingle.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/Knobs.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SidebandMessage {
	constexpr static FileIdentifier file_identifier = 11862047;
	uint64_t key;
	Version commitVersion;

	SidebandMessage() {}
	SidebandMessage(uint64_t key, Version commitVersion) : key(key), commitVersion(commitVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, commitVersion);
	}
};

struct SidebandInterface {
	constexpr static FileIdentifier file_identifier = 15950545;
	RequestStream<SidebandMessage> updates;

	UID id() const { return updates.getEndpoint().token; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, updates);
	}
};

struct SidebandSingleWorkload : TestWorkload {
	double testDuration, operationsPerSecond;
	SidebandInterface interf;

	std::vector<Future<Void>> clients;
	PerfIntCounter messages, consistencyErrors, keysUnexpectedlyPresent;

	SidebandSingleWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), messages("Messages"), consistencyErrors("Causal Consistency Errors"),
	    keysUnexpectedlyPresent("KeysUnexpectedlyPresent") {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		operationsPerSecond = getOption(options, LiteralStringRef("operationsPerSecond"), 50.0);
	}

	std::string description() const override { return "SidebandSingleWorkload"; }
	Future<Void> setup(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return persistInterface(this, cx);
	}
	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();

		clients.push_back(mutator(this, cx));
		clients.push_back(checker(this, cx));
		return delay(testDuration);
	}
	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		int errors = 0;
		for (int c = 0; c < clients.size(); c++) {
			errors += clients[c].isError();
		}
		if (errors)
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were client errors.");
		clients.clear();
		if (consistencyErrors.getValue())
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were causal consistency errors.");
		return !errors && !consistencyErrors.getValue();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(messages.getMetric());
		m.push_back(consistencyErrors.getMetric());
		m.push_back(keysUnexpectedlyPresent.getMetric());
	}

	ACTOR Future<Void> persistInterface(SidebandSingleWorkload* self, Database cx) {
		state Transaction tr(cx);
		BinaryWriter wr(IncludeVersion());
		wr << self->interf;
		state Standalone<StringRef> serializedInterface = wr.toValue();
		loop {
			try {
				Optional<Value> val = wait(tr.get(StringRef(format("Sideband/Client/%d", self->clientId))));
				if (val.present()) {
					if (val.get() != serializedInterface)
						throw operation_failed();
					break;
				}
				tr.set(format("Sideband/Client/%d", self->clientId), serializedInterface);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		TraceEvent("SidebandPersisted", self->interf.id()).detail("ClientIdx", self->clientId);
		return Void();
	}

	ACTOR Future<SidebandInterface> fetchSideband(SidebandSingleWorkload* self, Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				Optional<Value> val = wait(tr.get(StringRef(format("Sideband/Client/%d", self->clientId))));
				if (!val.present()) {
					throw operation_failed();
				}
				SidebandInterface sideband;
				BinaryReader br(val.get(), IncludeVersion());
				br >> sideband;
				TraceEvent("SidebandFetched", sideband.id()).detail("ClientIdx", self->clientId);
				return sideband;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> mutator(SidebandSingleWorkload* self, Database cx) {
		state SidebandInterface checker = wait(self->fetchSideband(self, cx));
		state double lastTime = now();
		state Version commitVersion;
		state bool unknown = false;

		loop {
			wait(poisson(&lastTime, 1.0 / self->operationsPerSecond));
			state Transaction tr0(cx);
			state Transaction tr(cx);
			state uint64_t key = deterministicRandom()->randomUniqueID().hash();

			state Standalone<StringRef> messageKey(format("Sideband/Message/%llx", key));
			// first set, this is the "old" value, always retry
			loop {
				try {
					tr0.set(messageKey, LiteralStringRef("oldbeef"));
					wait(tr0.commit());
					break;
				} catch (Error& e) {
					wait(tr0.onError(e));
				}
			}
			// second set, the checker should see this, no retries on unknown result
			loop {
				try {
					// tr.setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
					tr.set(messageKey, LiteralStringRef("deadbeef"));
					TraceEvent("DebugSidebandBeforeCommit");
					wait(tr.commit());
					commitVersion = tr.getCommittedVersion();
					TraceEvent("DebugSidebandAfterCommit").detail("CommitVersion", commitVersion);
					break;
				} catch (Error& e) {
					if (e.code() == error_code_commit_unknown_result) {
						TraceEvent("DebugSidebandUnknownResult");
						unknown = true;
						++self->messages;
						checker.updates.send(SidebandMessage(key, invalidVersion));
						break;
					}
					wait(tr.onError(e));
				}
			}
			if (unknown) {
				unknown = false;
				continue;
			}
			++self->messages;
			checker.updates.send(SidebandMessage(key, commitVersion));
		}
	}

	ACTOR Future<Void> checker(SidebandSingleWorkload* self, Database cx) {
		loop {
			state SidebandMessage message = waitNext(self->interf.updates.getFuture());
			state Standalone<StringRef> messageKey(format("Sideband/Message/%llx", message.key));
			state Transaction tr(cx);
			loop {
				try {
					TraceEvent("DebugSidebandCacheGetBefore");
					tr.setOption(FDBTransactionOptions::USE_GRV_CACHE);
					state Optional<Value> val = wait(tr.get(messageKey));
					TraceEvent("DebugSidebandCacheGetAfter");
					if (!val.present()) {
						TraceEvent(SevError, "CausalConsistencyError1", self->interf.id())
						    .detail("MessageKey", messageKey.toString().c_str())
						    .detail("RemoteCommitVersion", message.commitVersion)
						    .detail("LocalReadVersion",
						            tr.getReadVersion().get()); // will assert that ReadVersion is set
						++self->consistencyErrors;
					} else if (val.get() != LiteralStringRef("deadbeef")) {
						TraceEvent("DebugSidebandOldBeef");
						// check again without cache, and if it's the same, that's expected
						state Transaction tr2(cx);
						state Optional<Value> val2;
						loop {
							try {
								TraceEvent("DebugSidebandNoCacheGetBefore");
								Optional<Value> val2 = wait(tr2.get(messageKey));
								TraceEvent("DebugSidebandNoCacheGetAfter");
								break;
							} catch (Error& e) {
								TraceEvent("DebugSidebandNoCacheError").error(e, true);
								wait(tr2.onError(e));
							}
						}
						if (val != val2) {
							TraceEvent(SevError, "CausalConsistencyError2", self->interf.id())
							    .detail("MessageKey", messageKey.toString().c_str())
							    .detail("Val1", val)
							    .detail("Val2", val2)
							    .detail("RemoteCommitVersion", message.commitVersion)
							    .detail("LocalReadVersion",
							            tr.getReadVersion().get()); // will assert that ReadVersion is set
							++self->consistencyErrors;
						}
					}
					break;
				} catch (Error& e) {
					TraceEvent("DebugSidebandCheckError").error(e, true);
					wait(tr.onError(e));
				}
			}
		}
	}
};

WorkloadFactory<SidebandSingleWorkload> SidebandSingleWorkloadFactory("SidebandSingle");
