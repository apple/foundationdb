/*
 * Sideband.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"

struct SidebandMessage {
	constexpr static FileIdentifier file_identifier = 11862046;
	uint64_t key;
	Version commitVersion;

	SidebandMessage() = default;
	SidebandMessage(uint64_t key, Version commitVersion) : key(key), commitVersion(commitVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, commitVersion);
	}
};

struct SidebandInterface {
	constexpr static FileIdentifier file_identifier = 15950544;
	RequestStream<SidebandMessage> updates;

	UID id() const { return updates.getEndpoint().token; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, updates);
	}
};

struct SidebandWorkload : TestWorkload {
	static constexpr auto NAME = "Sideband";

	double testDuration, operationsPerSecond;
	SidebandInterface interf;

	std::vector<Future<Void>> clients;
	PerfIntCounter messages, consistencyErrors, keysUnexpectedlyPresent;

	explicit SidebandWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), messages("Messages"), consistencyErrors("Causal Consistency Errors"),
	    keysUnexpectedlyPresent("KeysUnexpectedlyPresent") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		operationsPerSecond = getOption(options, "operationsPerSecond"_sr, 50.0);
	}

	Future<Void> setup(Database const& cx) override { return persistInterface(this, cx->clone()); }
	Future<Void> start(Database const& cx) override {
		clients.push_back(mutator(this, cx->clone()));
		clients.push_back(checker(this, cx->clone()));
		return delay(testDuration);
	}
	Future<bool> check(Database const& cx) override {
		int errors = 0;
		for (int c = 0; c < clients.size(); c++)
			errors += clients[c].isError();
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

	Future<Void> persistInterface(SidebandWorkload* self, Database cx) {
		Transaction tr(cx);
		BinaryWriter wr(IncludeVersion());
		wr << self->interf;
		Standalone<StringRef> serializedInterface = wr.toValue();
		while (true) {
			Error err;
			try {
				Optional<Value> val = co_await tr.get(StringRef(format("Sideband/Client/%d", self->clientId)));
				if (val.present()) {
					if (val.get() != serializedInterface)
						throw operation_failed();
					break;
				}
				tr.set(format("Sideband/Client/%d", self->clientId), serializedInterface);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
		TraceEvent("SidebandPersisted", self->interf.id()).detail("ClientIdx", self->clientId);
	}

	Future<SidebandInterface> fetchSideband(SidebandWorkload* self, Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				Optional<Value> val =
				    co_await tr.get(StringRef(format("Sideband/Client/%d", (self->clientId + 1) % self->clientCount)));
				if (!val.present()) {
					throw operation_failed();
				}
				SidebandInterface sideband;
				BinaryReader br(val.get(), IncludeVersion());
				br >> sideband;
				TraceEvent("SidebandFetched", sideband.id()).detail("ClientIdx", self->clientId);
				co_return sideband;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> mutator(SidebandWorkload* self, Database cx) {
		SidebandInterface checker = co_await self->fetchSideband(self, cx->clone());
		double lastTime = now();
		Version commitVersion{ 0 };

		while (true) {
			co_await poisson(&lastTime, 1.0 / self->operationsPerSecond);
			Transaction tr(cx);
			uint64_t key = deterministicRandom()->randomUniqueID().hash();

			Standalone<StringRef> messageKey(format("Sideband/Message/%llx", key));
			while (true) {
				Error err;
				try {
					Optional<Value> val = co_await tr.get(messageKey);
					if (val.present()) {
						commitVersion = tr.getReadVersion().get();
						++self->keysUnexpectedlyPresent;
						break;
					}
					tr.set(messageKey, "deadbeef"_sr);
					co_await tr.commit();
					commitVersion = tr.getCommittedVersion();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
			++self->messages;
			checker.updates.send(SidebandMessage(key, commitVersion));
		}
	}

	Future<Void> checker(SidebandWorkload* self, Database cx) {
		while (true) {
			SidebandMessage message = co_await self->interf.updates.getFuture();
			Standalone<StringRef> messageKey(format("Sideband/Message/%llx", message.key));
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					Optional<Value> val = co_await tr.get(messageKey);
					if (!val.present()) {
						TraceEvent(SevError, "CausalConsistencyError", self->interf.id())
						    .detail("MessageKey", messageKey.toString().c_str())
						    .detail("RemoteCommitVersion", message.commitVersion)
						    .detail("LocalReadVersion",
						            tr.getReadVersion().get()); // will assert that ReadVersion is set
						++self->consistencyErrors;
					}
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}
	}
};

WorkloadFactory<SidebandWorkload> SidebandWorkloadFactory;
