/*
 * print_in_order.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fmt/format.h"
#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/DeterministicRandom.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

// Solution to https://leetcode.com/problems/print-in-order/description/
//
// This is a super basic concurrency exercise useful as a day 1
// exercise in a new environment. To try this yourself, delete the
// next two functions, then write a solution from scratch.

ACTOR Future<Void> print_msg_when_ready(Future<Void> ready, std::string msg) {
	int delay_msec = deterministicRandom()->randomInt(0, 1000);
	double delay_sec = static_cast<double>(delay_msec) / 1000.0;
	wait(delay(delay_sec));

	wait(ready);
	std::cout << msg << std::endl;
	wait(delay(0.1));
	return Void();
}

ACTOR Future<Void> orchestrate() {
	state Promise<Void> p_first, p_second, p_third;
	state Future<Void> first_ready = p_first.getFuture();
	state Future<Void> second_ready = p_second.getFuture();
	state Future<Void> third_ready = p_third.getFuture();

	state Future<Void> first = print_msg_when_ready(first_ready, "First");
	state Future<Void> second = print_msg_when_ready(second_ready, "Second");
	state Future<Void> third = print_msg_when_ready(third_ready, "Third");

	// If we do the following, the order of output varies from run to run based on
	// random seed chosen.  This is expected.
	// p_first.send(0);
	// p_second.send(0);
	// p_third.send(0);

	// So what we have to do is signal in order and wait before signaling the
	// next.
	p_first.send(Void());
	wait(first);
	p_second.send(Void());
	wait(second);
	p_third.send(Void());
	wait(third);

	return Void();
}

int main(int argc, char** argv) {
	// Cargo-culted from tutorial.actor.cpp.
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	std::vector<Future<Void>> all;

	all.emplace_back(orchestrate());

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
