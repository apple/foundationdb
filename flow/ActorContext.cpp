/*
 * ActorContext.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "flow/ActorContext.h"
#include "flow/ActorUID.h"

#if ACTOR_MONITORING != ACTOR_MONITORING_DISABLED

#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>

#include "libb64/encode.h"
#include "libb64/decode.h"

#include "flow/ActorUID.h"
#include "flow/flow.h"
#include "flow/GUID.h"

namespace ActorMonitoring {

namespace {

std::vector<ActorExecutionContext> g_currentExecutionContext;

std::unordered_map<ActorID, ActiveActor> g_activeActors;

inline ActorID getActorID() {
	static thread_local ActorID actorID = ActorMonitoring::INIT_ACTOR_ID;
	return ++actorID;
}

inline ActorID getActorSpawnerID() {
	if (g_currentExecutionContext.empty()) {
		return INIT_ACTOR_ID;
	}
	return g_currentExecutionContext.back().actorID;
}

inline bool isActorOnMainThread() {
	// The INetwork framework behaves differently in Net2 and Sim2.
	// For Net2, when Net2::run() is called, the N2::thread_network is set to be the current Net2 instance.
	// For Sim2, it tests if on main thread by calling the underlying Net2 instance, however, since Net2::run()
	// is never called, the N2::thread_network will always be nullptr. In this case, Sim2::isOnMainThread will always
	// return false and not reliable.
	if (g_network) [[likely]] {
		return g_network->isSimulated() || g_network->isOnMainThread();
	} else {
		return false;
	}
}

inline double gn_now() {
	if (g_network == nullptr) [[unlikely]] {
		return 0.0;
	}
	return g_network->now();
}

} // anonymous namespace

ActorInfoMinimal::ActorInfoMinimal() : identifier(ActorIdentifier()), id(INVALID_ACTOR_ID), spawner(INVALID_ACTOR_ID) {}

ActorInfoMinimal::ActorInfoMinimal(const ActorIdentifier& identifier_, const ActorID id_, const ActorID spawner_)
  : identifier(identifier_), id(id_), spawner(spawner_) {}

ActorInfoFull::ActorInfoFull()
  : ActorInfoMinimal(), spawnTime(-1), lastResumeTime(-1), lastYieldTime(-1), numResumes(0) {}

ActorInfoFull::ActorInfoFull(const ActorIdentifier& identifier_, const ActorID id_, const ActorID spawner_)
  : ActorInfoMinimal(identifier_, id_, spawner_), spawnTime(-1), lastResumeTime(-1), lastYieldTime(-1), numResumes(-1) {
}

using ActiveActorsCount_t = uint32_t;

ActiveActorHelper::ActiveActorHelper(const ActorIdentifier& actorIdentifier) {
	if (!isActorOnMainThread()) [[unlikely]] {
		// Only capture ACTORs on the main thread
		return;
	}
	const auto actorID_ = getActorID();
	const auto spawnerActorID = getActorSpawnerID();
	actorID = actorID_;
	g_activeActors[actorID] = ActiveActor(actorIdentifier, actorID, spawnerActorID);
}

ActiveActorHelper::~ActiveActorHelper() {
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	g_activeActors.erase(actorID);
}

ActorExecutionContextHelper::ActorExecutionContextHelper(const ActorID& actorID_,
                                                         const ActorBlockIdentifier& blockIdentifier_) {
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	g_currentExecutionContext.emplace_back(actorID_, blockIdentifier_);
#if ACTOR_MONITORING == ACTOR_MONITORING_FULL
	g_activeActors[actorID_].lastResumeTime = gn_now();
	++g_activeActors[actorID_].numResumes;
#endif
}

ActorExecutionContextHelper::~ActorExecutionContextHelper() {
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	if (g_currentExecutionContext.empty()) [[unlikely]] {
		// This should not happen, abort the program if it happens.
		std::abort();
	}
	g_currentExecutionContext.pop_back();
}

ActorYieldHelper::ActorYieldHelper(const ActorID& actorID_, const ActorBlockIdentifier& blockIdentifier_) {
#if ACTOR_MONITORING == ACTOR_MONITORING_FULL
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	g_activeActors[actorID_].lastYieldTime = gn_now();
	g_activeActors[actorID_].yieldBlockID = blockIdentifier_;
#endif
}

namespace {

void encodeBinaryGUID(BinaryWriter& writer) {
	writer << BINARY_GUID;
}

} // namespace

std::string encodeActorContext(const ActorContextDumpType dumpType) {
	BinaryWriter writer(Unversioned());

	encodeBinaryGUID(writer);

	return "";
}

DecodedActorContext decodeActorContext(const std::string& caller) {
	return DecodedActorContext();
}

namespace {

auto getActorInfoFromActorID(const ActorID& actorID) -> std::optional<ActorInfo> {
	std::optional<ActorInfo> result;

	if (auto iter = g_activeActors.find(actorID); iter != std::end(g_activeActors)) {
		result.emplace(iter->second);
	}

	return result;
}

auto getActorDebuggingDataFromIdentifier(const ActorIdentifier& actorIdentifier) -> std::optional<ActorDebuggingData> {
	std::optional<ActorDebuggingData> result;

	if (auto iter = ACTOR_DEBUGGING_DATA.find(actorIdentifier); iter != std::end(ACTOR_DEBUGGING_DATA)) {
		result.emplace(iter->second);
	}

	return result;
}

} // namespace

void dumpActorCallBacktrace() {
	std::cout << "Length of ACTOR stack: " << g_currentExecutionContext.size() << std::endl;
	std::cout << "NumActors=" << g_activeActors.size() << std::endl;
	std::cout << "NumDebugDatas=" <<  ACTOR_DEBUGGING_DATA.size() << std::endl;
	for (const auto& block : g_currentExecutionContext) {
		std::cout << std::setw(10) << block.actorID << "\t";
		if (const auto info = getActorInfoFromActorID(block.actorID); info.has_value()) {
			if (const auto debugData = getActorDebuggingDataFromIdentifier(info->identifier); debugData.has_value()) {
				std::cout << std::setw(30) << debugData->actorName << "\t" << debugData->path << ":"
				          << debugData->lineNumber << std::endl;
			} else {
				std::cout << "No debug data available" << std::endl;
			}
		} else {
			std::cout << "No ACTOR info" << std::endl;
		}
	}
}

} // namespace ActorMonitoring

#endif
