/*
 * ActorLineageProfiler.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-20201 Apple Inc. and the FoundationDB project authors
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
#include <optional>
#include <string>
#include <any>
#include <vector>
#include "flow/singleton.h"
#include "flow/flow.h"

struct IALPCollectorBase {
	virtual std::optional<std::any> collect(ActorLineage*) = 0;
	virtual const std::string_view& name() = 0;
	IALPCollectorBase();
};

template <class T>
struct IALPCollector : IALPCollectorBase {
	const std::string_view& name() override {
		static std::string_view res;
		if (res == "") {
			res = T::name;
		}
		return res;
	}
};

enum class WaitState { Running, DiskIO };

std::string_view to_string(WaitState w) {
	switch (w) {
	case WaitState::Running:
		return "Running";
	case WaitState::DiskIO:
		return "DiskIO";
	}
}

struct Sample : std::enable_shared_from_this<Sample> {
	double time = 0.0;
	unsigned size = 0u;
	char* data = nullptr;
	~Sample() { ::free(data); }
};

class SampleCollectorT {
public: // Types
	friend class crossbow::singleton<SampleCollectorT>;
	using Getter = std::function<std::vector<Reference<ActorLineage>>()>;

private:
	std::vector<IALPCollectorBase*> collectors;
	std::map<WaitState, Getter> getSamples;
	SampleCollectorT() {}

public:
	void addCollector(IALPCollectorBase* collector) { collectors.push_back(collector); }
	std::map<std::string_view, std::any> collect(ActorLineage* lineage);
	std::shared_ptr<Sample> collect();
};

using SampleCollector = crossbow::singleton<SampleCollectorT>;
