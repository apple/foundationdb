/*
 * networktest.actor.cpp
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

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbserver/NetworkTest.h"
#include "flow/Knobs.h"
#include "flow/ActorCollection.h"
#include "flow/UnitTest.h"
#include <inttypes.h>

#include "flow/actorcompiler.h" // This must be the last #include.

constexpr int WLTOKEN_NETWORKTEST = WLTOKEN_FIRST_AVAILABLE;

struct LatencyStats {
	using sample = double;
	double x = 0;
	double x2 = 0;
	double n = 0;

	sample tick() {
		// now() returns the timestamp when we were scheduled; count
		// all that time against this sample.
		return now();
	}

	void tock(sample tick) {
		// time_monotonic returns the timestamp when it was called;
		// count the time it took us to be dispatched and invoke
		// timer_monotonic
		double delta = timer_monotonic() - tick;
		x += delta;
		x2 += (delta * delta);
		n++;
	}

	void reset() { *this = LatencyStats(); }
	double count() { return n; }
	double mean() { return x / n; }
	double stddev() { return sqrt(x2 / n - (x / n) * (x / n)); }
};

NetworkTestInterface::NetworkTestInterface(NetworkAddress remote)
  : test(Endpoint::wellKnown({ remote }, WLTOKEN_NETWORKTEST)) {}

NetworkTestInterface::NetworkTestInterface(INetwork* local) {
	test.makeWellKnownEndpoint(WLTOKEN_NETWORKTEST, TaskPriority::DefaultEndpoint);
}

ACTOR Future<Void> networkTestServer() {
	state NetworkTestInterface interf(g_network);
	state Future<Void> logging = delay(1.0);
	state double lastTime = now();
	state int sent = 0;
	state LatencyStats latency;

	loop {
		choose {
			when(NetworkTestRequest req = waitNext(interf.test.getFuture())) {
				LatencyStats::sample sample = latency.tick();
				req.reply.send(NetworkTestReply(Value(std::string(req.replySize, '.'))));
				latency.tock(sample);
				sent++;
			}
			when(wait(logging)) {
				auto spd = sent / (now() - lastTime);
				if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
					fprintf(stderr, "%f\t%.3f\t%.3f\n", spd, latency.mean() * 1e6, latency.stddev() * 1e6);
				} else {
					fprintf(stderr, "responses per second: %f (%f us)\n", spd, latency.mean() * 1e6);
				}
				latency.reset();
				lastTime = now();
				sent = 0;
				logging = delay(1.0);
			}
		}
	}
}

ACTOR Future<Void> networkTestStreamingServer() {
	state NetworkTestInterface interf(g_network);
	state Future<Void> logging = delay(1.0);
	state double lastTime = now();
	state int sent = 0;
	state LatencyStats latency;

	loop {
		try {
			choose {
				when(state NetworkTestStreamingRequest req = waitNext(interf.testStream.getFuture())) {
					state LatencyStats::sample sample = latency.tick();
					state int i = 0;
					for (; i < 100; ++i) {
						wait(req.reply.onReady());
						req.reply.send(NetworkTestStreamingReply{ i });
					}
					req.reply.sendError(end_of_stream());
					latency.tock(sample);
					sent++;
				}
				when(wait(logging)) {
					auto spd = sent / (now() - lastTime);
					if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
						fprintf(stderr, "%f\t%.3f\t%.3f\n", spd, latency.mean() * 1e6, latency.stddev() * 1e6);
					} else {
						fprintf(stderr, "responses per second: %f (%f us)\n", spd, latency.mean() * 1e6);
					}
					latency.reset();
					lastTime = now();
					sent = 0;
					logging = delay(1.0);
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_operation_obsolete) {
				throw e;
			}
		}
	}
}

static bool moreRequestsPending(int count) {
	if (count == -1) {
		return false;
	} else {
		int request_count = FLOW_KNOBS->NETWORK_TEST_REQUEST_COUNT;
		return (!request_count) || count < request_count;
	}
}

static bool moreLoggingNeeded(int count, int iteration) {
	if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
		return iteration <= 2;
	} else {
		return moreRequestsPending(count);
	}
}

ACTOR Future<Void> testClient(std::vector<NetworkTestInterface> interfs,
                              int* sent,
                              int* completed,
                              LatencyStats* latency) {
	state std::string request_payload(FLOW_KNOBS->NETWORK_TEST_REQUEST_SIZE, '.');
	state LatencyStats::sample sample;

	while (moreRequestsPending(*sent)) {
		(*sent)++;
		sample = latency->tick();
		NetworkTestReply rep = wait(
		    retryBrokenPromise(interfs[deterministicRandom()->randomInt(0, interfs.size())].test,
		                       NetworkTestRequest(StringRef(request_payload), FLOW_KNOBS->NETWORK_TEST_REPLY_SIZE)));
		latency->tock(sample);
		(*completed)++;
	}
	return Void();
}

ACTOR Future<Void> testClientStream(std::vector<NetworkTestInterface> interfs,
                                    int* sent,
                                    int* completed,
                                    LatencyStats* latency) {
	state std::string request_payload(FLOW_KNOBS->NETWORK_TEST_REQUEST_SIZE, '.');
	state LatencyStats::sample sample;

	while (moreRequestsPending(*sent)) {
		(*sent)++;
		sample = latency->tick();
		state ReplyPromiseStream<NetworkTestStreamingReply> stream =
		    interfs[deterministicRandom()->randomInt(0, interfs.size())].testStream.getReplyStream(
		        NetworkTestStreamingRequest{});
		state int j = 0;
		try {
			loop {
				NetworkTestStreamingReply rep = waitNext(stream.getFuture());
				ASSERT(rep.index == j++);
			}
		} catch (Error& e) {
			ASSERT(e.code() == error_code_end_of_stream || e.code() == error_code_connection_failed);
		}
		latency->tock(sample);
		(*completed)++;
	}
	return Void();
}

ACTOR Future<Void> logger(int* sent, int* completed, LatencyStats* latency) {
	state double lastTime = now();
	state int logged = 0;
	state int iteration = 0;
	while (moreLoggingNeeded(logged, ++iteration)) {
		wait(delay(1.0));
		auto spd = (*completed - logged) / (now() - lastTime);
		if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
			if (iteration == 2) {
				// We don't report the first iteration because of warm-up effects.
				printf("%f\t%.3f\t%.3f\n", spd, latency->mean() * 1e6, latency->stddev() * 1e6);
			}
		} else {
			fprintf(stderr, "messages per second: %f (%6.3f us)\n", spd, latency->mean() * 1e6);
		}
		latency->reset();
		lastTime = now();
		logged = *completed;
	}
	// tell the clients to shut down
	*sent = -1;
	return Void();
}

static void networkTestnanosleep() {
	printf("nanosleep speed test\n");

#ifdef __linux__
	printf("\nnanosleep(10) latencies:");
	for (int i = 0; i < 10; i++) {

		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 10;
		nanosleep(&tv, nullptr);
		double after = timer_monotonic();

		printf(" %0.3lf", (after - before) * 1e6);
	}

	printf("\nnanosleep(10) latency after 5ms spin:");
	for (int i = 0; i < 10; i++) {
		double a = timer_monotonic() + 5e-3;
		while (timer_monotonic() < a) {
		}

		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 10;
		nanosleep(&tv, nullptr);
		double after = timer_monotonic();
		printf(" %0.3lf", (after - before) * 1e6);
	}

	printf("\nnanosleep(20000) latency:");
	for (int i = 0; i < 10; i++) {
		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 20000;
		nanosleep(&tv, nullptr);
		double after = timer_monotonic();

		printf(" %0.3lf", (after - before) * 1e6);
	}
	printf("\n");

	printf("nanosleep(20000) loop\n");
	while (true) {
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 20000;
		nanosleep(&tv, nullptr);
	}
#endif

	return;
}

ACTOR Future<Void> networkTestClient(std::string testServers) {
	if (testServers == "nanosleep") {
		networkTestnanosleep();
		// return Void();
	}

	state std::vector<NetworkTestInterface> interfs;
	state std::vector<NetworkAddress> servers = NetworkAddress::parseList(testServers);
	state int sent = 0;
	state int completed = 0;
	state LatencyStats latency;

	interfs.reserve(servers.size());
	for (int i = 0; i < servers.size(); i++) {
		interfs.push_back(NetworkTestInterface(servers[i]));
	}

	state std::vector<Future<Void>> clients;
	clients.reserve(FLOW_KNOBS->NETWORK_TEST_CLIENT_COUNT);
	for (int i = 0; i < FLOW_KNOBS->NETWORK_TEST_CLIENT_COUNT; i++) {
		clients.push_back(testClient(interfs, &sent, &completed, &latency));
	}
	clients.push_back(logger(&sent, &completed, &latency));

	wait(waitForAll(clients));
	return Void();
}

struct RandomIntRange {
	int min;
	int max;

	RandomIntRange(int low = 0, int high = 0) : min(low), max(high) {}

	// Accepts strings of the form "min:max" or "N"
	// where N will be used for both min and max
	RandomIntRange(std::string str) {
		StringRef high = str;
		StringRef low = high.eat(":");
		if (high.size() == 0) {
			high = low;
		}
		min = low.size() == 0 ? 0 : atol(low.toString().c_str());
		max = high.size() == 0 ? 0 : atol(high.toString().c_str());
		if (min > max) {
			std::swap(min, max);
		}
	}

	int get() const { return (max == 0) ? 0 : nondeterministicRandom()->randomInt(min, max + 1); }

	std::string toString() const { return format("%d:%d", min, max); }
};

struct P2PNetworkTest {
	// Addresses to listen on
	std::vector<Reference<IListener>> listeners;
	// Addresses to randomly connect to
	std::vector<NetworkAddress> remotes;
	// Number of outgoing connections to maintain
	int connectionsOut;
	// Message size range to send on outgoing established connections
	RandomIntRange requestBytes;
	// Message size to reply with on incoming established connections
	RandomIntRange replyBytes;
	// Number of requests/replies per session
	RandomIntRange requests;
	// Delay after message send and receive are complete before closing connection
	RandomIntRange idleMilliseconds;
	// Random delay before socket reads
	RandomIntRange waitReadMilliseconds;
	// Random delay before socket writes
	RandomIntRange waitWriteMilliseconds;

	double startTime;
	int64_t bytesSent;
	int64_t bytesReceived;
	int sessionsIn;
	int sessionsOut;
	int connectErrors;
	int acceptErrors;
	int sessionErrors;

	Standalone<StringRef> msgBuffer;

	std::string statsString() {
		double elapsed = now() - startTime;
		std::string s = format(
		    "%.2f MB/s bytes in  %.2f MB/s bytes out  %.2f/s completed sessions in  %.2f/s completed sessions out  ",
		    bytesReceived / elapsed / 1e6,
		    bytesSent / elapsed / 1e6,
		    sessionsIn / elapsed,
		    sessionsOut / elapsed);
		s += format("Total Errors %d  connect=%d  accept=%d  session=%d",
		            connectErrors + acceptErrors + sessionErrors,
		            connectErrors,
		            acceptErrors,
		            sessionErrors);
		bytesSent = 0;
		bytesReceived = 0;
		sessionsIn = 0;
		sessionsOut = 0;
		startTime = now();
		return s;
	}

	P2PNetworkTest() {}

	P2PNetworkTest(std::string listenerAddresses,
	               std::string remoteAddresses,
	               int connectionsOut,
	               RandomIntRange sendMsgBytes,
	               RandomIntRange recvMsgBytes,
	               RandomIntRange requests,
	               RandomIntRange idleMilliseconds,
	               RandomIntRange waitReadMilliseconds,
	               RandomIntRange waitWriteMilliseconds)
	  : connectionsOut(connectionsOut), requestBytes(sendMsgBytes), replyBytes(recvMsgBytes), requests(requests),
	    idleMilliseconds(idleMilliseconds), waitReadMilliseconds(waitReadMilliseconds),
	    waitWriteMilliseconds(waitWriteMilliseconds) {
		bytesSent = 0;
		bytesReceived = 0;
		sessionsIn = 0;
		sessionsOut = 0;
		connectErrors = 0;
		acceptErrors = 0;
		sessionErrors = 0;
		msgBuffer = makeString(std::max(sendMsgBytes.max, recvMsgBytes.max));

		if (!remoteAddresses.empty()) {
			remotes = NetworkAddress::parseList(remoteAddresses);
		}

		if (!listenerAddresses.empty()) {
			for (auto a : NetworkAddress::parseList(listenerAddresses)) {
				listeners.push_back(INetworkConnections::net()->listen(a));
			}
		}
	}

	NetworkAddress randomRemote() { return remotes[nondeterministicRandom()->randomInt(0, remotes.size())]; }

	ACTOR static Future<Standalone<StringRef>> readMsg(P2PNetworkTest* self, Reference<IConnection> conn) {
		state Standalone<StringRef> buffer = makeString(sizeof(int));
		state int writeOffset = 0;
		state bool gotHeader = false;

		// Fill buffer sequentially until the initial bytesToRead is read (or more), then read
		// intended message size and add it to bytesToRead, continue if needed until bytesToRead is 0.
		loop {
			int stutter = self->waitReadMilliseconds.get();
			if (stutter > 0) {
				wait(delay(stutter / 1e3));
			}

			int len = conn->read((uint8_t*)buffer.begin() + writeOffset, (uint8_t*)buffer.end());
			writeOffset += len;
			self->bytesReceived += len;

			// If buffer is complete, either process it as a header or return it
			if (writeOffset == buffer.size()) {
				if (gotHeader) {
					return buffer;
				} else {
					gotHeader = true;
					int msgSize = *(int*)buffer.begin();
					if (msgSize == 0) {
						return Standalone<StringRef>();
					}
					buffer = makeString(msgSize);
					writeOffset = 0;
				}
			}

			if (len == 0) {
				wait(conn->onReadable());
				wait(delay(0, TaskPriority::ReadSocket));
			}
		}
	}

	ACTOR static Future<Void> writeMsg(P2PNetworkTest* self, Reference<IConnection> conn, StringRef msg) {
		state UnsentPacketQueue packets;
		PacketWriter writer(packets.getWriteBuffer(msg.size()), nullptr, Unversioned());
		writer.serializeBinaryItem((int)msg.size());
		writer.serializeBytes(msg);

		loop {
			int stutter = self->waitWriteMilliseconds.get();
			if (stutter > 0) {
				wait(delay(stutter / 1e3));
			}
			int sent = conn->write(packets.getUnsent(), FLOW_KNOBS->MAX_PACKET_SEND_BYTES);

			if (sent != 0) {
				self->bytesSent += sent;
				packets.sent(sent);
			}

			if (packets.empty()) {
				break;
			}

			wait(conn->onWritable());
			wait(yield(TaskPriority::WriteSocket));
		}

		return Void();
	}

	ACTOR static Future<Void> doSession(P2PNetworkTest* self, Reference<IConnection> conn, bool incoming) {
		state int numRequests;

		try {
			if (incoming) {
				wait(conn->acceptHandshake());

				// Read the number of requests for the session
				Standalone<StringRef> buf = wait(readMsg(self, conn));
				ASSERT(buf.size() == sizeof(int));
				numRequests = *(int*)buf.begin();
			} else {
				wait(conn->connectHandshake());

				// Pick the number of requests for the session and send it to remote
				numRequests = self->requests.get();
				wait(writeMsg(self, conn, StringRef((const uint8_t*)&numRequests, sizeof(int))));
			}

			while (numRequests > 0) {
				if (incoming) {
					// Wait for a request
					wait(success(readMsg(self, conn)));
					// Send a reply
					wait(writeMsg(self, conn, self->msgBuffer.substr(0, self->replyBytes.get())));
				} else {
					// Send a request
					wait(writeMsg(self, conn, self->msgBuffer.substr(0, self->requestBytes.get())));
					// Wait for a reply
					wait(success(readMsg(self, conn)));
				}

				if (--numRequests == 0) {
					break;
				}
			}

			wait(delay(self->idleMilliseconds.get() / 1e3));
			conn->close();

			if (incoming) {
				++self->sessionsIn;
			} else {
				++self->sessionsOut;
			}
		} catch (Error& e) {
			++self->sessionErrors;
			TraceEvent(SevError, incoming ? "P2PIncomingSessionError" : "P2POutgoingSessionError")
			    .error(e)
			    .detail("Remote", conn->getPeerAddress());
		}

		return Void();
	}

	ACTOR static Future<Void> outgoing(P2PNetworkTest* self) {
		loop {
			wait(delay(0, TaskPriority::WriteSocket));
			state NetworkAddress remote = self->randomRemote();

			try {
				state Reference<IConnection> conn = wait(INetworkConnections::net()->connect(remote));
				// printf("Connected to %s\n", remote.toString().c_str());
				wait(doSession(self, conn, false));
			} catch (Error& e) {
				++self->connectErrors;
				TraceEvent(SevError, "P2POutgoingError").error(e).detail("Remote", remote);
				wait(delay(1));
			}
		}
	}

	ACTOR static Future<Void> incoming(P2PNetworkTest* self, Reference<IListener> listener) {
		state ActorCollection sessions(false);

		loop {
			wait(delay(0, TaskPriority::AcceptSocket));

			try {
				state Reference<IConnection> conn = wait(listener->accept());
				// printf("Connected from %s\n", conn->getPeerAddress().toString().c_str());
				sessions.add(doSession(self, conn, true));
			} catch (Error& e) {
				++self->acceptErrors;
				TraceEvent(SevError, "P2PIncomingError").error(e).detail("Listener", listener->getListenAddress());
			}
		}
	}

	ACTOR static Future<Void> run_impl(P2PNetworkTest* self) {
		state ActorCollection actors(false);

		self->startTime = now();

		fmt::print("{0} listeners, {1} remotes, {2} outgoing connections\n",
		           self->listeners.size(),
		           self->remotes.size(),
		           self->connectionsOut);

		for (auto n : self->remotes) {
			printf("Remote: %s\n", n.toString().c_str());
		}

		for (auto el : self->listeners) {
			printf("Listener: %s\n", el->getListenAddress().toString().c_str());
			actors.add(incoming(self, el));
		}

		printf("Request size: %s\n", self->requestBytes.toString().c_str());
		printf("Response size: %s\n", self->replyBytes.toString().c_str());
		printf("Requests per outgoing session: %s\n", self->requests.toString().c_str());
		printf("Delay before socket read: %s\n", self->waitReadMilliseconds.toString().c_str());
		printf("Delay before socket write: %s\n", self->waitWriteMilliseconds.toString().c_str());
		printf("Delay before session close: %s\n", self->idleMilliseconds.toString().c_str());
		printf("Send/Recv size %d bytes\n", FLOW_KNOBS->MAX_PACKET_SEND_BYTES);

		if ((self->remotes.empty() || self->connectionsOut == 0) && self->listeners.empty()) {
			printf("No listeners and no remotes or connectionsOut, so there is nothing to do!\n");
			ASSERT((!self->remotes.empty() && (self->connectionsOut > 0)) || !self->listeners.empty());
		}

		if (!self->remotes.empty()) {
			for (int i = 0; i < self->connectionsOut; ++i) {
				actors.add(outgoing(self));
			}
		}

		loop {
			wait(delay(1.0, TaskPriority::Max));
			printf("%s\n", self->statsString().c_str());
		}
	}

	Future<Void> run() { return run_impl(this); }
};

// Peer-to-Peer network test.
// One or more instances can be run and set to talk to each other.
// Each instance
//   - listens on 0 or more listenerAddresses
//   - maintains 0 or more connectionsOut at a time, each to a random choice from remoteAddresses
// Address lists are a string of comma-separated IP:port[:tls] strings.
//
// The other arguments can be specified as "fixedValue" or "minValue:maxValue".
// Each outgoing connection will live for a random requests count.
// Each request will
//   - send a random requestBytes sized message
//   - wait for a random replyBytes sized response.
// The client will close the connection after a random idleMilliseconds.
// Reads and writes can optionally preceded by random delays, waitReadMilliseconds and waitWriteMilliseconds.
TEST_CASE(":/network/p2ptest") {
	state P2PNetworkTest p2p(params.get("listenerAddresses").orDefault(""),
	                         params.get("remoteAddresses").orDefault(""),
	                         params.getInt("connectionsOut").orDefault(1),
	                         params.get("requestBytes").orDefault("50:100"),
	                         params.get("replyBytes").orDefault("500:1000"),
	                         params.get("requests").orDefault("10:10000"),
	                         params.get("idleMilliseconds").orDefault("0"),
	                         params.get("waitReadMilliseconds").orDefault("0"),
	                         params.get("waitWriteMilliseconds").orDefault("0"));

	wait(p2p.run());
	return Void();
}
