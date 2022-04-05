#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

ACTOR Future<Void> unwaitDelay() {
	state int rep = 0;

	loop {
		wait(delay(0.5));
		if (++rep == 10) break;
		std::cout << "unwaitDelay: " << rep << std::endl;
	}

	std::cout << "This should not happen as unwaitDelay should be cancelled after waitDelay finishes." << std::endl;

	return Void();
}


ACTOR Future<Void> waitDelay() {
	state int rep = 0;

	loop {
		wait(delay(0.5));
		if (++rep == 5) break;
		std::cout << "waitDelay: " << rep << std::endl;
	}

	std::cout << "waitDelay: completed." << std::endl;

	return Void();
}


int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _1 = unwaitDelay();	// NOTE: This actor is not being waited. It *must* be assigned to a variable
	auto _2 = stopAfter(waitForAll<Void>({waitDelay()}));
	g_network->run();

	return 0;
}

