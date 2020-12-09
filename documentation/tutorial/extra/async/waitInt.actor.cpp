#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

ACTOR Future<int> returnInt() {
    wait(delay(1));
    return 1;
}

ACTOR Future<Void> waitInt() {
    int intValue = wait(returnInt());
    std::cout << "Received " << intValue << std::endl;

	return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({waitInt()}));
	g_network->run();

	return 0;
}

