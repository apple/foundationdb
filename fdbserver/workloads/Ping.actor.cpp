/*
 * Ping.actor.cpp
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

#include "flow/ActorCollection.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct PingWorkloadInterface {
	RequestStream<LoadedPingRequest> payloadPing;

	UID id() const { return payloadPing.getEndpoint().token; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, payloadPing);
	}
};

struct PingWorkload : TestWorkload {
	static constexpr auto NAME = "Ping";

	double testDuration, operationsPerSecond;
	PingWorkloadInterface interf;
	bool logging, pingWorkers, registerInterface, broadcastTest, usePayload, parallelBroadcast, workerBroadcast;
	Standalone<StringRef> payloadOut, payloadBack;
	int actorCount;

	std::vector<Future<Void>> clients;
	PerfIntCounter messages;
	PerfDoubleCounter totalMessageLatency;
	PerfDoubleCounter maxMessageLatency;

	PingWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), messages("Messages"), totalMessageLatency("TotalLatency"),
	    maxMessageLatency("Max Latency (ms)") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		operationsPerSecond = getOption(options, "operationsPerSecondPerClient"_sr, 50.0);
		usePayload = getOption(options, "usePayload"_sr, false);
		logging = getOption(options, "logging"_sr, false);
		pingWorkers = getOption(options, "pingWorkers"_sr, false);
		registerInterface = getOption(options, "registerInterface"_sr, true);
		broadcastTest = getOption(options, "broadcastTest"_sr, false);
		parallelBroadcast = getOption(options, "parallelBroadcast"_sr, false);
		workerBroadcast = getOption(options, "workerBroadcast"_sr, false);
		int payloadSize = getOption(options, "payloadSizeOut"_sr, 1024);
		payloadOut = std::string(payloadSize, '.');
		payloadSize = getOption(options, "payloadSizeBack"_sr, 1024);
		payloadBack = std::string(payloadSize, '.');
		actorCount = getOption(options, "actorCount"_sr, 1);
	}

	Future<Void> setup(Database const& cx) override {
		if (pingWorkers || !registerInterface)
			return Void();
		return persistInterface(this, cx);
	}
	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> clients;
		if (pingWorkers) {
			clients.push_back(workerPinger(this));
		} else if (broadcastTest) {
			if (parallelBroadcast || !clientId)
				clients.push_back(payloadSender(this, cx));
			// clients.push_back( payloadPinger( this, cx ) );
		} else if (!broadcastTest) {
			clients.push_back(pinger(this, cx));
		}
		clients.push_back(ponger(this));
		return timeout(waitForAll(clients), testDuration, Void()); // delay( testDuration );
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(messages.getMetric());
		m.emplace_back("Avg Latency (ms)", 1000 * totalMessageLatency.getValue() / messages.getValue(), Averaged::True);
		m.push_back(maxMessageLatency.getMetric());
	}

	ACTOR Future<Void> persistInterface(PingWorkload* self, Database cx) {
		state Transaction tr(cx);
		BinaryWriter wr(IncludeVersion());
		wr << self->interf;
		state Standalone<StringRef> serializedInterface = wr.toValue();
		loop {
			try {
				Optional<Value> val = wait(tr.get(StringRef(format("Ping/Client/%d", self->clientId))));
				if (val.present()) {
					if (val.get() != serializedInterface)
						throw operation_failed();
					break;
				}
				tr.set(format("Ping/Client/%d", self->clientId), serializedInterface);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<std::vector<PingWorkloadInterface>> fetchInterfaces(PingWorkload* self, Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				state std::vector<PingWorkloadInterface> result;
				state int i;
				for (i = 0; i < self->clientCount; i++) {
					Optional<Value> val = wait(tr.get(StringRef(format("Ping/Client/%d", i))));
					if (!val.present()) {
						throw operation_failed();
					}
					PingWorkloadInterface interf;
					BinaryReader br(val.get(), IncludeVersion());
					br >> interf;
					result.push_back(interf);
				}
				return result;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> pinger(PingWorkload* self, std::vector<RequestStream<LoadedPingRequest>> peers) {
		state double lastTime = now();

		loop {
			wait(poisson(&lastTime, self->actorCount / self->operationsPerSecond));
			auto& peer = deterministicRandom()->randomChoice(peers);
			state NetworkAddress addr = peer.getEndpoint().getPrimaryAddress();
			state double before = now();

			LoadedPingRequest req;
			req.id = deterministicRandom()->randomUniqueID();
			req.payload = self->usePayload ? self->payloadOut : ""_sr;
			req.loadReply = self->usePayload;
			LoadedReply rep = wait(peer.getReply(req));

			double elapsed = now() - before;
			self->totalMessageLatency += elapsed;
			self->maxMessageLatency += std::max(0.0, elapsed * 1000.0 - self->maxMessageLatency.getValue());
			++self->messages;
			if (self->logging)
				TraceEvent("Ping").detail("Elapsed", elapsed).detail("To", addr);
		}
	}

	ACTOR Future<Void> pinger(PingWorkload* self, Database cx) {
		std::vector<PingWorkloadInterface> testers = wait(self->fetchInterfaces(self, cx));
		std::vector<RequestStream<LoadedPingRequest>> peers;
		peers.reserve(testers.size());
		for (int i = 0; i < testers.size(); i++)
			peers.push_back(testers[i].payloadPing);
		std::vector<Future<Void>> pingers;
		pingers.reserve(self->actorCount);
		for (int i = 0; i < self->actorCount; i++)
			pingers.push_back(self->pinger(self, peers));
		wait(waitForAll(pingers));
		return Void();
	}

	ACTOR Future<Void> workerPinger(PingWorkload* self) {
		std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));
		std::vector<RequestStream<LoadedPingRequest>> peers;
		peers.reserve(workers.size());
		for (int i = 0; i < workers.size(); i++)
			peers.push_back(workers[i].interf.debugPing);
		std::vector<Future<Void>> pingers;
		pingers.reserve(self->actorCount);
		for (int i = 0; i < self->actorCount; i++)
			pingers.push_back(self->pinger(self, peers));
		wait(waitForAll(pingers));
		return Void();
	}

	// ACTOR Future<Void> poisson_spin( double *last, double meanInterval ) {
	// 	*last += meanInterval*-log( deterministicRandom()->random01() );
	// 	wait( delay( std::max( *last - timer() - 0.01, 0.0 ) ) );
	// 	if( timer() >= *last )
	// 		TraceEvent(SevWarnAlways, "SpinPoissonInaccurateTime").detail("Diff", timer() - *last);
	// 	while( timer() < *last )
	// 		_mm_pause();
	// 	return Void();
	// }

	ACTOR Future<Void> payloadSender(PingWorkload* self, Database cx) {
		state std::vector<RequestStream<LoadedPingRequest>> endpoints;
		state double lastTime = timer();
		state PromiseStream<Future<Void>> addActor;
		state Future<Void> collection = actorCollection(addActor.getFuture());

		if (self->workerBroadcast) {
			std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));
			for (int i = 0; i < workers.size(); i++)
				endpoints.push_back(workers[i].interf.debugPing);
		} else {
			std::vector<PingWorkloadInterface> peers = wait(self->fetchInterfaces(self, cx));
			for (int i = 0; i < peers.size(); i++)
				endpoints.push_back(peers[i].payloadPing);
		}

		// std::random_shuffle( peers.begin(), peers.end() );
		loop {
			wait(poisson(&lastTime, 1.0 / 6.0));
			addActor.send(self->payloadPinger(self, cx, endpoints));
		}
	}

	// ACTOR Future<Void> receptionLogger( PingWorkload* self, Future<PingReply> done, NetworkAddress to, UID id ) {
	// 	wait(success( done ));
	// 	if( now() > self->testStart + 29 && now() < self->testStart + 31 )
	// 		TraceEvent("PayloadReplyReceived", id).detail("To", to);
	// 	return Void();
	// }

	// ACTOR Future<Void> payloadDelayer( PingRequest req, PromiseStream<PingRequest> stream ) {
	// 	wait( delay( deterministicRandom()->random01() * 0.100 ) );
	// 	PingReply rep = wait( stream.getReply( req ) );
	// 	return Void();
	// }

	ACTOR Future<Void> payloadPinger(PingWorkload* self,
	                                 Database cx,
	                                 std::vector<RequestStream<LoadedPingRequest>> peers) {
		// state std::vector<PingWorkloadInterface> peers = wait( self->fetchInterfaces( self, cx ) );

		// loop {
		state double start = now();
		state UID pingId(deterministicRandom()->randomUniqueID());
		std::vector<Future<Void>> replies;
		for (int i = 0; i < peers.size(); i++) {
			LoadedPingRequest req;
			req.id = pingId;
			req.payload = self->payloadOut;
			req.loadReply = true;
			replies.push_back(success(peers[i].getReply(req)));
			// replies.push_back( self->receptionLogger( self, peers[i].payloadPing.getReply( req ),
			// peers[i].payloadPing.getEndpoint().getPrimaryAddress(), pingId ) ); peers[i].payloadPing.send( req );
			// replies.push_back( self->payloadDelayer( req, peers[i].payloadPing ) );
		}
		TraceEvent("PayloadPingSent", pingId).log();
		wait(waitForAll(replies));
		double elapsed = now() - start;
		TraceEvent("PayloadPingDone", pingId).detail("Elapsed", elapsed);
		// 	wait( delay( deterministicRandom()->random01() / 100 ) );
		// }
		return Void();
	}

	// ACTOR Future<Void> packetPonger( PingWorkload* self, LoadedPingRequest req ) {
	// 	wait( delay( deterministicRandom()->random01() * 0.100 ) );

	// 	LoadedReply rep;
	// 	rep.id = req.id;
	// 	rep.payload = req.loadReply ? self->payloadBack : ""_sr;
	// 	req.reply.send( rep );

	// 	return Void();
	// }

	ACTOR Future<Void> ponger(PingWorkload* self) {
		// state PromiseStream<Future<Void>> addActor;
		// state Future<Void> pongCollection = actorCollection( addActor.getFuture() );

		loop {
			LoadedPingRequest req = waitNext(self->interf.payloadPing.getFuture());
			// double end = timer() + (deterministicRandom()->random01() / 200);
			// while( timer() < end )
			// 	_mm_pause();

			// if( now() > self->testStart + 29 && now() < self->testStart + 31 )
			// 	TraceEvent("PayloadPingReceived", req.id);

			LoadedReply rep;
			rep.id = req.id;
			rep.payload = req.loadReply ? self->payloadBack : ""_sr;
			req.reply.send(rep);

			// addActor.send( self->packetPonger( self, req ) );
		}
	}
};

WorkloadFactory<PingWorkload> PingWorkloadFactory;
