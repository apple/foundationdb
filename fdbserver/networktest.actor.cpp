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
