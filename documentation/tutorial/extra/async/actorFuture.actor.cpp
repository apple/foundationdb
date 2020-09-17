#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

ACTOR Future<int> returnInt() {
    std::cout << "returnInt is triggered" << std::endl;
    wait(delay(1));
    std::cout << "returnInt is returning" << std::endl;
    return 1;
}

ACTOR Future<Void> waitDelay() {
    std::cout << "Assigning returnInt() call to a Future<int> variable" << std::endl;
    Future<int> intValueFuture = returnInt();

    std::cout << "Waiting for returnInt returns" << std::endl;
    int value = wait(intValueFuture);

    std::cout << "Received " << value << std::endl;

	return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({waitDelay()}));
	g_network->run();

	return 0;
}

