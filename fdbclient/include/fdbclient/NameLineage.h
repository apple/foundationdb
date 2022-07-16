/*
 * NameLineage.h
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

#include <string_view>

#include "fdbclient/ActorLineageProfiler.h"

struct NameLineage : LineageProperties<NameLineage> {
	static constexpr std::string_view name = "Actor"sv;
	const char* actorName;
};

struct NameLineageCollector : IALPCollector<NameLineage> {
	NameLineageCollector() : IALPCollector() {}
	std::optional<std::any> collect(ActorLineage* lineage) override {
		auto str = lineage->get(&NameLineage::actorName);
		if (str.has_value()) {
			return std::string_view(*str, std::strlen(*str));
		} else {
			return {};
		}
	}
};
