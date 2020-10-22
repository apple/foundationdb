#include <iostream>
#include <string>
#include <vector>

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

ACTOR
template <typename T>
Future<T> reportLatency(Future<T> future, std::string tag) {
    state double startTime = now();
    state T value = wait(future);
    std::cout << tag << " delay: " << now() - startTime << std::endl;
    return value;
}

ACTOR Future<Void> reportLatency(Future<Void> future, std::string tag) {
    state double startTime = now();
    wait(future);
    std::cout << tag << " delay: " << now() - startTime << std::endl;
    return Void();
}

ACTOR Future<Void> job1() {
    wait(delay(2.0));
    return Void();
}

ACTOR Future<int> job2(int n) {
    wait(delay(1.0));
    return n;
}

ACTOR Future<Void> waitDelay() {
    wait(reportLatency(job1(), "job1"));
    state int n = wait(reportLatency(job2(1), "job2"));

	return Void();
}


int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _ = stopAfter(waitForAll<Void>({waitDelay()}));
	g_network->run();

	return 0;
}

