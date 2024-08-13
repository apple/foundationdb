/*
 * ActorContext.h
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

#ifndef FLOW_ACTOR_CONTEXT_H
#define FLOW_ACTOR_CONTEXT_H

#ifdef WITH_ACAC

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "flow/FastAlloc.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"

using ActorIdentifier = UID;
using ActorID = uint64_t;

static constexpr ActorID INVALID_ACTOR_ID = std::numeric_limits<ActorID>::max();
static constexpr ActorID INIT_ACTOR_ID = 0;

struct ActiveActor {
	ActorIdentifier identifier = ActorIdentifier();
	ActorID id = ActorID();
	double spawnTime = double();

	ActorID spawner = INVALID_ACTOR_ID;

	ActiveActor();
	explicit ActiveActor(const ActorIdentifier& identifier_,
	                     const ActorID& id_,
	                     const ActorID& spawnerID_ = INVALID_ACTOR_ID);

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, identifier, id, spawnTime, spawner);
	}
};

using ActorBlockIdentifier = UID;

struct ActorExecutionContext {
	ActorID actorID;

	ActorBlockIdentifier blockIdentifier;

	explicit ActorExecutionContext(const ActorID actorID_, const ActorBlockIdentifier blockIdentifier_)
	  : actorID(actorID_), blockIdentifier(blockIdentifier_) {}
};

// Dumps the current ACTORs to a given stream
extern void dumpActors(std::ostream& stream);

// A helper class that register/unregister the Actor
class ActiveActorHelper {
public:
	ActorID actorID;

	ActiveActorHelper(const ActorIdentifier& actorIdentifier);
	~ActiveActorHelper();
};

class ActorExecutionContextHelper {
public:
	ActorExecutionContextHelper(const ActorID& actorID_, const ActorBlockIdentifier& blockIdentifier_);
	~ActorExecutionContextHelper();
};

enum class ActorContextDumpType : uint8_t {
	FULL_CONTEXT,
	CURRENT_STACK,
	CURRENT_CALL_BACKTRACE,
};

// Encode the current actor context into a string
extern std::string encodeActorContext(const ActorContextDumpType dumpType = ActorContextDumpType::FULL_CONTEXT);

// Encode the current actor call backtrace
extern void dumpActorCallBacktrace();

struct DecodedActorContext {
	struct ActorInfo {
		ActorID id;
		ActorIdentifier identifier;
		ActorID spawnerID;

		ActorInfo(const ActorID& _id, const ActorIdentifier& _identifier, const ActorID& _spawnerID)
		  : id(_id), identifier(_identifier), spawnerID(_spawnerID) {}
	};
	ActorID currentRunningActor;
	std::vector<ActorInfo> context;
	ActorContextDumpType dumpType;
};

// Decode the serialized actor context to DecodedActorContext
DecodedActorContext decodeActorContext(const std::string& caller);

#else // WITH_ACAC

#include <memory>

#include "flow/IRandom.h"

using ActorID = uint64_t;
using ActorIdentifier = UID;
using ActorBlockIdentifier = UID;

struct ActorExecutionContext {};
struct ActiveActor {};
struct ActiveActorHelper {
	ActiveActorHelper(const ActorIdentifier&) {}
};
struct ActorExecutionContextHelper {
	ActorExecutionContextHelper(const ActorID&, const ActorBlockIdentifier&);
};

#endif // WITH_ACAC

#endif // FLOW_ACTOR_CONTEXT_H
