#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

ACTOR Future<int> echoValue(double delayTime, int value) {
    wait(delay(delayTime));
    return value;
}

ACTOR Future<Void> loopChooseWhen() {
    state int secondTicker = 0;
    state Future<int> echoValue1 = echoValue(1.5, 1);
    state Future<int> echoValue2 = echoValue(2.5, 2);
    state Future<Void> delayer = delay(1.0);

    loop choose {
        when(wait(delayer)) {
            std::cout << ++secondTicker << " second(s) has passed" << std::endl;
            if (secondTicker > 10) {
                break;
            }
            delayer = delay(1.0);
        }
        when(int val = wait(echoValue1)) {
            std::cout << "echoValue 1 Received: " << val << std::endl;
            echoValue1 = echoValue(1.5, 1);
        }
        when(int val = wait(echoValue2)) {
            std::cout << "echoValue 2 Received: " << val << std::endl;
            echoValue2 = echoValue(2.5, 2);
        }
    }

    return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({loopChooseWhen()}));
	g_network->run();

	return 0;
}

