#include "flow/flow.h"
#include "fdbclient/StackLineage.h"
#include <csignal>
#include <iostream>
#include <string_view>

// This is not yet correct, as this is not async safe
// However, this should be good enough for an initial
// proof of concept.
extern "C" void stackSignalHandler(int sig) {
	auto stack = getActorStackTrace();
	int i = 0;
	while (!stack.empty()) {
		auto s = stack.back();
		stack.pop_back();
		std::string_view n(reinterpret_cast<const char*>(s.begin()), s.size());
		std::cout << i << ": " << n << std::endl;
		++i;
	}
}

void setupStackSignal() {
	std::signal(SIGUSR1, &stackSignalHandler);
}
