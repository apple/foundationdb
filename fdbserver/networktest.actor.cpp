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

#include "flow/actorcompiler.h"
#include "NetworkTest.h"

UID WLTOKEN_NETWORKTEST( -1, 2 );

NetworkTestInterface::NetworkTestInterface( NetworkAddress remote )
	: test( Endpoint(remote, WLTOKEN_NETWORKTEST) )
{
}

NetworkTestInterface::NetworkTestInterface( INetwork* local )
{
	test.makeWellKnownEndpoint( WLTOKEN_NETWORKTEST, TaskDefaultEndpoint );
}

ACTOR Future<Void> networkTestServer() {
	state NetworkTestInterface interf( g_network );
	state Future<Void> logging = delay( 1.0 );
	state double lastTime = now();
	state int sent = 0;

	loop {
		choose {
			when( NetworkTestRequest req = waitNext( interf.test.getFuture() ) ) {
				req.reply.send( NetworkTestReply( Value( std::string( req.replySize, '.' ) ) ) );
				sent++;
			}
			when( Void _ = wait( logging ) ) {
				auto spd = sent / (now() - lastTime);
				fprintf( stderr, "responses per second: %f (%f us)\n", spd, 1e6/spd );
				lastTime = now();
				sent = 0;
				logging = delay( 1.0 );
			}
		}
	}
}

ACTOR Future<Void> testClient( std::vector<NetworkTestInterface> interfs, int* sent ) {
	state double lastTime = now();

	loop {
		NetworkTestReply rep = wait(  retryBrokenPromise(interfs[g_random->randomInt(0, interfs.size())].test, NetworkTestRequest( LiteralStringRef("."), 600000 ) ) );
		(*sent)++;
	}
}

ACTOR Future<Void> logger( int* sent ) {
	state double lastTime = now();
	loop {
		Void _ = wait( delay(1.0) );
		auto spd = *sent / (now() - lastTime);
		fprintf( stderr, "messages per second: %f\n", spd);
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
		while (timer_monotonic() < a) 0;

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

	for( int i = 0; i < servers.size(); i++ ) {
		interfs.push_back( NetworkTestInterface( servers[i] ) );
	}

	state std::vector<Future<Void>> clients;
	for( int i = 0; i < 30; i++ )
		clients.push_back( testClient( interfs, &sent ) );
	clients.push_back( logger( &sent ) );

	Void _ = wait( waitForAll( clients ) );
	return Void();
}