#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

PromiseStream<int> promise;

ACTOR Future<Void> producer() {
    state int counter = 0;
    state int i = 0;

    for (i = 0; i < 10; ++i) {
        promise.send(counter);
        ++counter;
        wait(delay(0.8));
    }

    return Void();
}

ACTOR Future<Void> loopForPromise() {
    state Future<Void> delayer = delay(1.0);
    state int counter = 0;

    loop {
        choose {
            when(wait(delayer)) {
                ++counter;
                std::cout << counter << " second(s) has passed." << std::endl;
                if (counter == 11) {
                    break;
                }
                delayer = delay(1.0);
            }
            when(int value = waitNext(promise.getFuture())) {
                std::cout << "Received " << value << std::endl;
            }
        }
    }

    return Void();
}


int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({loopForPromise(), producer()}));
	g_network->run();

	return 0;
}

