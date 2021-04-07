/*
 * AnnotateActor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include "flow/flow.h"
#include "flow/network.h"

// Used to manually instrument waiting actors to collect samples for the
// sampling profiler.
struct AnnotateActor {
	unsigned index;

	AnnotateActor() {}

	// This API isn't great. Ideally, no cleanup call is needed. I ran into an
	// issue around the destructor being called twice because an instance of
	// this class has to be stored as a class member (otherwise it goes away
	// when wait is called), and due to how Flow does code generation the
	// member will be default initialized and then initialized again when it is
	// initially set. Then, the destructor will be called twice, causing issues
	// when the WriteOnlySet tries to erase the same index twice. I'm working
	// on this :)

	void start() {
		index = g_network->getActorLineageSet().insert(currentLineage);
	}
	
	void complete() {
		g_network->getActorLineageSet().erase(index);
	}
};

