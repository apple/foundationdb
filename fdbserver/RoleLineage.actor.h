/*
 * RoleLineage.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_ROLE_LINEAGE_ACTOR_G_H)
#define FDBSERVER_ROLE_LINEAGE_ACTOR_G_H
#include "fdbserver/RoleLineage.actor.g.h"
#elif !defined(FDBSERVER_ROLE_LINEAGE_ACTOR_H)
#define FDBSERVER_ROLE_LINEAGE_ACTOR_H

#include "flow/singleton.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/ActorLineageProfiler.h"
#include "fdbserver/WorkerInterface.actor.h"

#include <string_view>
#include <msgpack.hpp>
#include <any>
#include "flow/actorcompiler.h" // This must be the last include

struct RoleLineage : LineageProperties<RoleLineage> {
	static std::string_view name;
	ProcessClass::ClusterRole role = ProcessClass::NoRole;

	bool isSet(ProcessClass::ClusterRole RoleLineage::*member) const { return this->*member != ProcessClass::NoRole; }
};

struct RoleLineageCollector : IALPCollector<RoleLineage> {
	RoleLineageCollector() : IALPCollector() {}
	std::optional<std::any> collect(ActorLineage* lineage) override {
		auto res = lineage->get(&RoleLineage::role);
		if (res.has_value()) {
			return Role::get(res.value()).abbreviation;
		} else {
			return std::optional<std::any>();
		}
	}
};

// creates a new root and sets the role lineage
ACTOR template <class Fun>
Future<decltype(std::declval<Fun>()())> runInRole(Fun fun, ProcessClass::ClusterRole role) {
	getCurrentLineage()->makeRoot();
	getCurrentLineage()->modify(&RoleLineage::role) = role;
	decltype(std::declval<Fun>()()) res = wait(fun());
	return res;
}

#endif
