#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

const int VALUE = 42;
Promise<int> promise;

ACTOR Future<Void> waitForPromise(int id) {
    state int value = wait(promise.getFuture());

    std::cout << "waitForPromise id=" << id << " received " << value << std::endl;

	return Void();
}

ACTOR Future<Void> loopForPromise() {
    state Future<Void> delayer = delay(1.0);
    state Future<Void> future1 = waitForPromise(1);
    state Future<Void> future2 = waitForPromise(2);
    state int counter = 0;

    loop {
        choose {
            when(wait(delayer)) {
                ++counter;
                std::cout << counter << " second(s) has passed." << std::endl;
                if (counter == 5) {
                    break;
                }
                if (counter == 2) {
                    promise.send(3);
                }
                delayer = delay(1.0);
            }
            when(wait(future1)) {
                std::cout << "Future1 is done" << std::endl;
                // It is important to mute the future1 to be Never() so it will
                // not be triggered anymore in the loop; otherwise, since the
                // condition will be fulfilled in the 2th second, future1 will
                // *always* be triggered and other conditions will be ignored.
                future1 = Never();
            }
            when(wait(future2)) {
                std::cout << "Future2 is done" << std::endl;
                future2 = Never();
            }
        }
    }

    return Void();
}


int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({loopForPromise()}));
	g_network->run();

	return 0;
}

