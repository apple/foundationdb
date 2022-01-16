/*
 * TeamPartitionedLogSystem.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TEAMPARTITIONEDLOGSYSTEM_ACTOR_G_H)
#define FDBSERVER_TEAMPARTITIONEDLOGSYSTEM_ACTOR_G_H
#include "fdbserver/TeamPartitionedLogSystem.actor.g.h"
#elif !defined(FDBSERVER_TEAMPARTITIONEDLOGSYSTEM_ACTOR_H)
#define FDBSERVER_TEAMPARTITIONEDLOGSYSTEM_ACTOR_H

#pragma once

#include "fdbserver/TagPartitionedLogSystem.actor.h"
#include "fdbserver/ptxn/TLogPeekCursor.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace ptxn {

// FIXME create base class for both LogSystems rather than inheritence
class TeamPartitionedLogSystem : public TagPartitionedLogSystem {
public:
	TeamPartitionedLogSystem(UID dbgid,
	                         LocalityData locality,
	                         LogEpoch e,
	                         Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>())
	  : TagPartitionedLogSystem(dbgid, locality, e, addActor) {}

	// FIXME this should be in some kind of base class
	static Reference<ILogSystem> fromLogSystemConfig(UID const& dbgid,
	                                                 LocalityData const& locality,
	                                                 LogSystemConfig const& lsConf,
	                                                 bool excludeRemote,
	                                                 bool useRecoveredAt,
	                                                 Optional<PromiseStream<Future<Void>>> addActor);
};

} // namespace ptxn

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_TEAMPARTITIONEDLOGSYSTEM_ACTOR_G_H
