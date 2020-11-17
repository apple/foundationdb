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

#include <iostream>
#include <fstream>

#include "fdbserver/NetworkTest.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

uint32_t NetworkTestReply::accumulativeIndex = 0;
uint32_t NetworkTestRequest::accumulativeIndex = 0;

UID WLTOKEN_NETWORKTEST( -1, 2 );

NetworkTestInterface::NetworkTestInterface( NetworkAddress remote )
	: test( Endpoint({remote}, WLTOKEN_NETWORKTEST) )
{
}

NetworkTestInterface::NetworkTestInterface( INetwork* local )
{
	test.makeWellKnownEndpoint( WLTOKEN_NETWORKTEST, TaskPriority::DefaultEndpoint );
}

constexpr double startingDelay = 5.0;
constexpr int packetSize = 155 * 1024;	// slightly larger than 150K limit
constexpr int NUM_PARALLEL_CLIENT = 32;

void generateReplyString(std::string& output, const int length) {
	std::ifstream ifs("/dev/random");
	output.resize(length);
	ifs.read(&output[0], length);
}

ACTOR Future<Void> networkTestServer() {
	state NetworkTestInterface interf( g_network );
	state Future<Void> logging = delay( 1.0 );
	state double lastTime = now();
	state int sent = 0;

	wait(delay(startingDelay));

	loop {
		choose {
			when( NetworkTestRequest req = waitNext( interf.test.getFuture() ) ) {
				std::string reply;
				generateReplyString(reply, req.replySize);
				auto networkTestReply = NetworkTestReply(Value(reply));
				req.reply.send(networkTestReply);
				sent++;
				std::cout << "Req index = " << req.index << " Resp index = " << networkTestReply.index << std::endl;
			}

			when( wait( logging ) ) {
				auto spd = sent / (now() - lastTime);
				std::cerr << "responses per second: " << spd << " (" << 1e6 / spd << " us)" << std::endl;
				lastTime = now();
				sent = 0;
				logging = delay( 1.0 );
			}
		}
	}
}

ACTOR Future<Void> testClient( std::vector<NetworkTestInterface> interfs, int* sent ) {
	loop {
		state NetworkTestRequest request = NetworkTestRequest(LiteralStringRef("."), packetSize);
		state NetworkTestReply rep = wait(retryBrokenPromise(
			interfs[deterministicRandom()->randomInt(0, interfs.size())].test,
			request));

		std::cout << "Req index = " << request.index << " Resp index = " << rep.index << std::endl;

		(*sent)++;
	}
}

ACTOR Future<Void> logger( int* sent ) {
	state double lastTime = now();
	loop {
		wait( delay(1.0) );
		auto spd = *sent / (now() - lastTime);
		std::cerr << "messages per second: " << spd << std::endl;
		lastTime = now();
		*sent = 0;
	}
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
	wait(delay(startingDelay));

	state std::vector<NetworkTestInterface> interfs;
	state std::vector<NetworkAddress> servers = NetworkAddress::parseList(testServers);
	state int sent = 0;

	for( int i = 0; i < servers.size(); i++ ) {
		interfs.push_back( NetworkTestInterface( servers[i] ) );
	}

	state std::vector<Future<Void>> clients;
	for( int i = 0; i < NUM_PARALLEL_CLIENT; i++ )
		clients.push_back( testClient( interfs, &sent ) );
	clients.push_back( logger( &sent ) );

	wait( waitForAll( clients ) );
	return Void();
}
