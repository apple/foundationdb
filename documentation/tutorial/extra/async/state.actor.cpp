#include <iostream>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"


ACTOR Future<Void> state_1() {
	// The variable marked as state will be accessible everywhere in the ACTOR
	state int stateValue = 0;

	// The variable without state decorator will have scope between the
	// declaration of itself and the first wait call. Thus it is possible to
	// redefine it after the wait call.
	int value = 0;

	std::cout << "The stateValue is " << stateValue << " while value is " << value << std::endl;

	wait(delay(1.0));

	int value = -1;
	++stateValue;
	std::cout << "The stateValue is " << stateValue << " while value is " << value << std::endl;

	return Void();
}

ACTOR Future<Void> state_2() {
	// Even the state variable is defined after
	std::cout << "State variable defined later can be accessed before its definition: stateVariable = " << stateVariable << std::endl;

	wait(delay(0.1));

	state int stateVariable = 10;

	return Void();
}


int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({state_1(), state_2()}));
	g_network->run();

	return 0;
}

