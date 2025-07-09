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

ACTOR Future<Void> print_msg_when_ready(Future<int> ready, std::string msg) {
	int delay_msec = deterministicRandom()->randomInt(0, 1000);
	double delay_sec = static_cast<double>(delay_msec)/1000.0;
	wait(delay(delay_sec));

	loop choose {
		when(int b = wait(ready)) {
			std::cout << msg << std::endl;
			wait(delay(0.1));
			return Void();
		}
	}
}

ACTOR Future<Void> orchestrate() {
	state Promise<int> p_first, p_second, p_third;
	state Future<int> first_ready = p_first.getFuture();
	state Future<int> second_ready = p_second.getFuture();
	state Future<int> third_ready = p_third.getFuture();

	state Future<Void> first = print_msg_when_ready(first_ready, "First");
	state Future<Void> second = print_msg_when_ready(second_ready, "Second");
	state Future<Void> third = print_msg_when_ready(third_ready, "Third");

	// If we do the following, the order of output varies from run to run based on
	// random seed chosen.  This is expected.
	//p_first.send(0);
	//p_second.send(0);
	//p_third.send(0);

	// So what we have to do is signal in order and wait before signaling the
	// next.
	p_first.send(0); wait(first);
	p_second.send(0); wait(second);
	p_third.send(0); wait(third);

	return Void();
}

int main(int argc, char **argv) {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	std::vector<Future<Void>> all;

	all.emplace_back(orchestrate());

	auto f = stopAfter(waitForAll(all));
	g_network->run();
	return 0;


	return 0;
}
