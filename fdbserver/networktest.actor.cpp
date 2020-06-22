/*
 * networktest.actor.cpp
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

#include "fdbserver/NetworkTest.h"
#include "flow/Knobs.h"
#include "flow/actorcompiler.h"  // This must be the last #include.
#include "flow/ActorCollection.h"
#include "flow/UnitTest.h"
#include <inttypes.h>

UID WLTOKEN_NETWORKTEST( -1, 2 );

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

NetworkTestInterface::NetworkTestInterface( NetworkAddress remote )
	: test( Endpoint({remote}, WLTOKEN_NETWORKTEST) )
{
}

NetworkTestInterface::NetworkTestInterface( INetwork* local )
{
	test.makeWellKnownEndpoint( WLTOKEN_NETWORKTEST, TaskPriority::DefaultEndpoint );
}

ACTOR Future<Void> networkTestServer() {
	state NetworkTestInterface interf( g_network );
	state Future<Void> logging = delay( 1.0 );
	state double lastTime = now();
	state int sent = 0;
	state LatencyStats latency;

	loop {
		choose {
			when( NetworkTestRequest req = waitNext( interf.test.getFuture() ) ) {
				LatencyStats::sample sample = latency.tick();
				req.reply.send( NetworkTestReply( Value( std::string( req.replySize, '.' ) ) ) );
				latency.tock(sample);
				sent++;
			}
			when( wait( logging ) ) {
				auto spd = sent / (now() - lastTime);
				if (FLOW_KNOBS->NETWORK_TEST_SCRIPT_MODE) {
					fprintf(stderr, "%f\t%.3f\t%.3f\n", spd, latency.mean() * 1e6, latency.stddev() * 1e6);
				} else {
					fprintf(stderr, "responses per second: %f (%f us)\n", spd, latency.mean() * 1e6);
				}
				latency.reset();
				lastTime = now();
				sent = 0;
				logging = delay( 1.0 );
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

ACTOR Future<Void> testClient(std::vector<NetworkTestInterface> interfs, int* sent, int* completed,
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

ACTOR Future<Void> logger(int* sent, int* completed, LatencyStats* latency) {
	state double lastTime = now();
	state int logged = 0;
	state int iteration = 0;
	while (moreLoggingNeeded(logged, ++iteration)) {
		wait( delay(1.0) );
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

static void networkTestnanosleep()
{
	printf("nanosleep speed test\n");

#ifdef __linux__
	printf("\nnanosleep(10) latencies:");
	for (int i = 0; i < 10; i++) {

		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 10;
		nanosleep(&tv, NULL);
		double after = timer_monotonic();

		printf(" %0.3lf", (after - before)*1e6);
	}

	printf("\nnanosleep(10) latency after 5ms spin:");
	for (int i = 0; i < 10; i++) {
		double a = timer_monotonic() + 5e-3;
		while (timer_monotonic() < a) {}

		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 10;
		nanosleep(&tv, NULL);
		double after = timer_monotonic();
		printf(" %0.3lf", (after - before)*1e6);
	}

	printf("\nnanosleep(20000) latency:");
	for (int i = 0; i < 10; i++) {
		double before = timer_monotonic();
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 20000;
		nanosleep(&tv, NULL);
		double after = timer_monotonic();

		printf(" %0.3lf", (after - before)*1e6);
	}
	printf("\n");

	printf("nanosleep(20000) loop\n");
	while (true) {
		timespec tv;
		tv.tv_sec = 0;
		tv.tv_nsec = 20000;
		nanosleep(&tv, NULL);
	}
#endif

	return;
}

ACTOR Future<Void> networkTestClient( std:: string testServers ) {
	if (testServers == "nanosleep") {
		networkTestnanosleep();
		//return Void();
	}

	state std::vector<NetworkTestInterface> interfs;
	state std::vector<NetworkAddress> servers = NetworkAddress::parseList(testServers);
	state int sent = 0;
	state int completed = 0;
	state LatencyStats latency;

	for( int i = 0; i < servers.size(); i++ ) {
		interfs.push_back( NetworkTestInterface( servers[i] ) );
	}

	state std::vector<Future<Void>> clients;
	for (int i = 0; i < FLOW_KNOBS->NETWORK_TEST_CLIENT_COUNT; i++) {
		clients.push_back(testClient(interfs, &sent, &completed, &latency));
	}
	clients.push_back(logger(&sent, &completed, &latency));

	wait( waitForAll( clients ) );
	return Void();
}

struct RandomIntRange {
	int min;
	int max;
	int get() const {
		return nondeterministicRandom()->randomInt(min, max + 1);
	}
};

struct P2PNetworkTest {
	std::vector<Reference<IListener>> listeners;
	std::vector<NetworkAddress> remotes;
	int connectionsOut;
	RandomIntRange msgBytes;
	RandomIntRange idleMilliseconds;

	double startTime;
	int64_t bytesSent;
	int64_t bytesReceived;
	int sessionsIn;
	int sessionsOut;
	Standalone<StringRef> msgBuffer;
	int sendRecvSize;

	std::string statsString() const {
		double elapsed = now() - startTime;
		return format("%.2f MB/s bytes in  %.2f MB/s bytes out  %.2f/s sessions in  %.2f/s sessions out",
			bytesReceived / elapsed / 1e6, bytesSent / elapsed / 1e6, sessionsIn / elapsed, sessionsOut / elapsed);
	}

	P2PNetworkTest() {}

	P2PNetworkTest(std::string listenerAddresses, std::string remoteAddresses, int connectionsOut, RandomIntRange msgBytes, RandomIntRange idleMilliseconds, int sendRecvSize)
	  : connectionsOut(connectionsOut), msgBytes(msgBytes), idleMilliseconds(idleMilliseconds), sendRecvSize(sendRecvSize) {
		bytesSent = 0;
		bytesReceived = 0;
		sessionsIn = 0;
		sessionsOut = 0;
		msgBuffer = nondeterministicRandom()->randomAlphaNumeric(msgBytes.max);

		if(!remoteAddresses.empty()) {
			remotes = NetworkAddress::parseList(remoteAddresses);
		}

		if(!listenerAddresses.empty()) {
			for(auto a : NetworkAddress::parseList(listenerAddresses)) {
				listeners.push_back(INetworkConnections::net()->listen(a));
			}
		}
	}

	NetworkAddress randomRemote() {
		return remotes[nondeterministicRandom()->randomInt(0, remotes.size())];
	}

	ACTOR static Future<Void> readMsg(P2PNetworkTest *self, Reference<IConnection> conn) {
		state Standalone<StringRef> buffer = makeString(self->sendRecvSize);
		state int bytesToRead = sizeof(int);
		state int writeOffset = 0;
		state bool gotHeader = false;

		// Fill buffer sequentially until the initial bytesToRead is read (or more), then read
		// intended message size and add it to bytesToRead, continue if needed until bytesToRead is 0.
		loop {
			int len = conn->read((uint8_t *)buffer.begin() + writeOffset, (uint8_t *)buffer.end());
			bytesToRead -= len;
			self->bytesReceived += len;
			writeOffset += len;

			// If no size header yet but there are enough bytes, read the size
			if(!gotHeader && bytesToRead <= 0) {
					gotHeader = true;
					bytesToRead += *(int *)buffer.begin();
			}

			if(gotHeader) {
				if(bytesToRead == 0) {
					break;
				}
				ASSERT(bytesToRead > 0);
				writeOffset = 0;
			}

			if(len == 0) {
				wait(conn->onReadable());
				wait( delay( 0, TaskPriority::ReadSocket ) );
			}
		}

		return Void();
	}

	ACTOR static Future<Void> writeMsg(P2PNetworkTest *self, Reference<IConnection> conn) {
		state UnsentPacketQueue packets;
		state int msgSize = self->msgBytes.get();

		PacketWriter writer(packets.getWriteBuffer(), nullptr, Unversioned());
		writer.serializeBinaryItem(msgSize);
		writer.serializeBytes(self->msgBuffer.substr(0, msgSize));

		loop {
			wait(conn->onWritable());
			wait( delay( 0, TaskPriority::WriteSocket ) );
			int len = conn->write(packets.getUnsent(), self->sendRecvSize);
			self->bytesSent += len;
			packets.sent(len);

			if(packets.empty())
				break;
		}

		return Void();
	}

	ACTOR static Future<Void> doSession(P2PNetworkTest *self, Reference<IConnection> conn) {
		try {
			wait(readMsg(self, conn) && writeMsg(self, conn));
			wait(delay(self->idleMilliseconds.get() / 1e3));
			conn->close();
		} catch(Error &e) {
			printf("doSession: error %s on remote %s\n", e.what(), conn->getPeerAddress().toString().c_str());
		}

		return Void();
	}

	ACTOR static Future<Void> outgoing(P2PNetworkTest *self) {
		loop {
			wait(delay(0, TaskPriority::WriteSocket));
			state NetworkAddress remote = self->randomRemote();

			try {
				state Reference<IConnection> conn = wait(INetworkConnections::net()->connect(remote));
				wait(conn->connectHandshake());
				//printf("Connected to %s\n", remote.toString().c_str());
				++self->sessionsOut;
				wait(doSession(self, conn));
			} catch(Error &e) {
				printf("outgoing: error %s on remote %s\n", e.what(), remote.toString().c_str());
			}
		}
	}

	ACTOR static Future<Void> incoming(P2PNetworkTest *self, Reference<IListener> listener) {
		state ActorCollection sessions(false);

		loop {
			wait(delay(0, TaskPriority::AcceptSocket));

			try {
				state Reference<IConnection> conn = wait(listener->accept());
				wait(conn->acceptHandshake());
				//printf("Connected from %s\n", conn->getPeerAddress().toString().c_str());
				++self->sessionsIn;
				sessions.add(doSession(self, conn));
			} catch(Error &e) {
				printf("incoming: error %s on listener %s\n", e.what(), listener->getListenAddress().toString().c_str());
			}
		}
	}

	ACTOR static Future<Void> run_impl(P2PNetworkTest *self) {
		state ActorCollection actors(false);
		g_network->initTLS();

		self->startTime = now();

		printf("%d listeners, %d remotes, %d outgoing connections\n", self->listeners.size(), self->remotes.size(), self->connectionsOut);
		printf("Message size %d to %d bytes\n", self->msgBytes.min, self->msgBytes.max);
		printf("Post exchange delay %f to %f seconds\n", self->idleMilliseconds.min / 1e3, self->idleMilliseconds.max / 1e3);
		printf("Send/Recv size %d bytes\n", self->sendRecvSize);

		for(auto n : self->remotes) {
			printf("Remote: %s\n", n.toString().c_str());
		}

		for(auto el : self->listeners) {
			printf("Listener: %s\n", el->getListenAddress().toString().c_str());
			actors.add(incoming(self, el));
		}

		if(!self->remotes.empty()) {
			for(int i = 0; i < self->connectionsOut; ++i) {
				actors.add(outgoing(self));
			}
		}

		loop {
			wait(delay(1.0));
			printf("%s\n", self->statsString().c_str());
		}
	}

	Future<Void> run() {
		return run_impl(this);
	}

};

int getEnvInt(const char *name, int defaultValue = 0) {
	const char *val = getenv(name);
	return val != nullptr ? atol(val) : defaultValue;
}

std::string getEnvStr(const char *name, std::string defaultValue = "") {
	const char *val = getenv(name);
	return val != nullptr ? val : defaultValue;
}

// TODO: Remove this hacky thing and make a "networkp2ptest" role in fdbserver
TEST_CASE("!p2ptest") {
	state P2PNetworkTest p2p(
		getEnvStr("listenerAddresses", "127.0.0.1:5000,127.0.0.1:5001:tls"),
		getEnvStr("remoteAddresses", "127.0.0.1:5000,127.0.0.1:5001:tls"),
		getEnvInt("connectionsOut", 2),
		{getEnvInt("minMsgBytes", 0), getEnvInt("maxMsgBytes", 1000000)},
		{getEnvInt("minIdleMilliseconds", 500), getEnvInt("maxIdleMilliseconds", 1000)},
		getEnvInt("sendRecvSize", 32000)
	);

	wait(p2p.run());
	return Void();
}
