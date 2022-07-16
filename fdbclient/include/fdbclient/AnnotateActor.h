/*
 * AnnotateActor.h
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

#pragma once

#include "flow/flow.h"
#include "flow/network.h"

#include <string_view>

// Used to manually instrument waiting actors to collect samples for the
// sampling profiler.
struct AnnotateActor {
	unsigned index;
	bool set;

	AnnotateActor() : set(false) {}

	AnnotateActor(LineageReference* lineage) : set(false) {
#ifdef ENABLE_SAMPLING
		if (lineage->getPtr() != 0) {
			index = g_network->getActorLineageSet().insert(*lineage);
			set = (index != ActorLineageSet::npos);
		}
#endif
	}

	AnnotateActor(const AnnotateActor& other) = delete;
	AnnotateActor(AnnotateActor&& other) = delete;
	AnnotateActor& operator=(const AnnotateActor& other) = delete;

	AnnotateActor& operator=(AnnotateActor&& other) {
		if (this == &other) {
			return *this;
		}

		this->index = other.index;
		this->set = other.set;

		other.set = false;

		return *this;
	}

	~AnnotateActor() {
#ifdef ENABLE_SAMPLING
		if (set) {
			g_network->getActorLineageSet().erase(index);
		}
#endif
	}
};

enum class WaitState { Disk, Network, Running };
// usually we shouldn't use `using namespace` in a header file, but literals should be safe as user defined literals
// need to be prefixed with `_`
using namespace std::literals;

constexpr std::string_view to_string(WaitState st) {
	switch (st) {
	case WaitState::Disk:
		return "Disk"sv;
	case WaitState::Network:
		return "Network"sv;
	case WaitState::Running:
		return "Running"sv;
	default:
		return ""sv;
	}
}

#ifdef ENABLE_SAMPLING
extern std::map<WaitState, std::function<std::vector<Reference<ActorLineage>>()>> samples;
#endif
