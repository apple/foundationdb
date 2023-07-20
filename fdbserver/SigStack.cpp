/*
 * SigStack.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

#ifdef _WIN32
#define SIGUSR1 10
#define SIGUSR2 12
#endif

void setupStackSignal() {
	std::signal(SIGUSR1, &stackSignalHandler);
}
