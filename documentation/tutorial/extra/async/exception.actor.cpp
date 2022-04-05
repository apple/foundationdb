#include <iostream>

#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"

#include "flow/actorcompiler.h"

ACTOR Future<Void> throwException(double delayTime) {
    wait(delay(delayTime));
    throw internal_error();
    // The return is unreachable
    // return Void();
}

ACTOR Future<Void> catchException() {
    state int secondTicker = 0;
    state Future<Void> exception = throwException(2.5);
    state Future<Void> delayer = delay(1.0);

    loop {
        try {
            choose {
                when(wait(delayer)) {
                    std::cout << ++secondTicker << " second(s) has passed" << std::endl;
                    if (secondTicker > 10) {
                        break;
                    }
                    delayer = delay(1.0);
                }
                when(wait(exception)) {
                    std::cout << "This should not happen." << std::endl;
                }
            }
        } catch(Error& ex) {
            std::cout << "Caught exception: " << ex.code() << " -- " << ex.what() << std::endl;
            break;
        }
    }

    return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({catchException()}));
	g_network->run();

	return 0;
}

