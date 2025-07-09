/*
 * make_h2o.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fmt/format.h"
#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/DeterministicRandom.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

// https://leetcode.com/problems/building-h2o/



ACTOR Future<Void> orchestrate() {

	return Void();
}

int main(int argc, char **argv) {
	// Cargo-culted from tutorial.actor.cpp.
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	std::vector<Future<Void>> all;

	all.emplace_back(orchestrate());

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
