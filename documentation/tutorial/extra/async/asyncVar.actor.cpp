#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"

#include "flow/actorcompiler.h"

AsyncVar<int> integerTrigger(0);
AsyncTrigger terminateTrigger;

ACTOR Future<Void> loopFunc() {
    state int i = 0;

    for (i = 0; i < 10; ++i) {
        integerTrigger.set(integerTrigger.get() + 1);

        wait(delay(0));
    }

    terminateTrigger.trigger();
    return Void();
}

ACTOR Future<Void> asyncVarLoop() {
    loop {
        choose {
            when(wait(integerTrigger.onChange())) {
                std::cout << "integerTrigger value: " << integerTrigger.get() << std::endl;
            }
            when(wait(terminateTrigger.onTrigger())) {
                std::cout << "terminateTrigger triggered!" << std::endl;
                break;
            }
        }
    };

	return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({loopFunc(), asyncVarLoop()}));
	g_network->run();

	return 0;
}

