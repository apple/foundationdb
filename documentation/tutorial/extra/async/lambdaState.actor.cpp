#include <iostream>
#include <functional>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"


ACTOR Future<Void> lambdaCapturing() {
	// The variable marked as state will be accessible everywhere in the ACTOR
	state int stateValue = 0;
    // NOTE: it is necessary to capture `this` pointer in order to access
    // stateVariable!
    state std::function<void()> lambda1 = [this]() {
        std::cout << "stateValue = " << stateValue << std::endl;
    };

    int nonStateVariable = 10;
    auto lambda2 = [=]() -> void {
        std::cout << "nonStateVariable = " << nonStateVariable << std::endl;

        // It is not possible to access `stateVariable` since the lambda is
        // not caputring `this`
    };

    std::cout << "Before delay: " << std::endl;

    lambda1();
    lambda2();

    wait(delay(0.1));

    std::cout << std::endl << "After delay: " << std::endl;
    lambda1();
    // lambda2 is not accessible as it is out of scope

    return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({lambdaCapturing()}));
	g_network->run();

	return 0;
}

