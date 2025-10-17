/*
 * play.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "flow/Arena.h"
#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h"

#include <iostream>
#include <vector>

/// Use this file as means to play with flow code
/// The goal is to give you a starting point with a boilerplate template
/// Don't expect frequent changes to this file unless we want to change the base template
/// The use-case would be for people to have ephemeral code (on top of this template) that never gets checked in

ACTOR Future<Void> foo() {
	std::cout << "foo enter" << std::endl;
	wait(delay(1));
	std::cout << "foo exit" << std::endl;
	return Void();
}

int main(int argc, char** argv) {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	std::vector<Future<Void>> all;
	all.emplace_back(foo());

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
