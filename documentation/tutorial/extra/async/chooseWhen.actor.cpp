#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

ACTOR Future<int> returnValue(double delayTime) {
    wait(delay(delayTime));
    return 1;
}

ACTOR Future<Void> chooseWhen() {
    choose {
        when(wait(delay(1.0))) {
            std::cout << "Timeout!" << std::endl;
        }
        when(int val = wait(returnValue(2.0))) {
            std:: cout << "Received: " << val << std::endl;
        }
    }

    return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({chooseWhen()}));
	g_network->run();

	return 0;
}

