/*
 * LogSystem.cpp
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

#include "fdbserver/LogSystem.h"
#include "fdbserver/TagPartitionedLogSystem.actor.h"
#include "fdbserver/MockLogSystem.h"

#include "flow/actorcompiler.h" // This must be the last #include.

Future<Void> ILogSystem::recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem,
                                            UID const& dbgid,
                                            DBCoreState const& oldState,
                                            FutureStream<TLogRejoinRequest> const& rejoins,
                                            LocalityData const& locality,
                                            bool* forceRecovery) {
	return TagPartitionedLogSystem::recoverAndEndEpoch(outLogSystem, dbgid, oldState, rejoins, locality, forceRecovery);
}

Reference<ILogSystem> ILogSystem::fromLogSystemConfig(UID const& dbgid,
                                                      struct LocalityData const& locality,
                                                      struct LogSystemConfig const& conf,
                                                      bool excludeRemote,
                                                      bool useRecoveredAt,
                                                      Optional<PromiseStream<Future<Void>>> addActor) {
	if (conf.logSystemType == LogSystemType::empty)
		return Reference<ILogSystem>();
	else if (conf.logSystemType == LogSystemType::tagPartitioned)
		return TagPartitionedLogSystem::fromLogSystemConfig(
		    dbgid, locality, conf, excludeRemote, useRecoveredAt, addActor);
	else if (conf.logSystemType == LogSystemType::mock) {
		return Reference<ILogSystem>(static_cast<ILogSystem*>(conf.mockLogSystem));
	} else
		throw internal_error();
}

Reference<ILogSystem> ILogSystem::fromOldLogSystemConfig(UID const& dbgid,
                                                         struct LocalityData const& locality,
                                                         struct LogSystemConfig const& conf) {
	if (conf.logSystemType == LogSystemType::empty)
		return Reference<ILogSystem>();
	else if (conf.logSystemType == LogSystemType::tagPartitioned)
		return TagPartitionedLogSystem::fromOldLogSystemConfig(dbgid, locality, conf);
	else
		throw internal_error();
}

Reference<ILogSystem> ILogSystem::fromServerDBInfo(UID const& dbgid,
                                                   ServerDBInfo const& dbInfo,
                                                   bool useRecoveredAt,
                                                   Optional<PromiseStream<Future<Void>>> addActor) {
	return fromLogSystemConfig(dbgid, dbInfo.myLocality, dbInfo.logSystemConfig, false, useRecoveredAt, addActor);
}
