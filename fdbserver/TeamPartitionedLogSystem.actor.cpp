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

#include "fdbserver/TeamPartitionedLogSystem.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace ptxn {

Reference<ILogSystem> TeamPartitionedLogSystem::fromLogSystemConfig(UID const& dbgid,
                                                                    LocalityData const& locality,
                                                                    LogSystemConfig const& lsConf,
                                                                    bool excludeRemote,
                                                                    bool useRecoveredAt,
                                                                    Optional<PromiseStream<Future<Void>>> addActor) {
	ASSERT(lsConf.logSystemType == LogSystemType::teamPartitioned ||
	       (lsConf.logSystemType == LogSystemType::empty && !lsConf.tLogs.size()));
	// ASSERT(lsConf.epoch == epoch);  //< FIXME
	auto logSystem = makeReference<TeamPartitionedLogSystem>(dbgid, locality, lsConf.epoch, addActor);

	logSystem->tLogs.reserve(lsConf.tLogs.size());
	logSystem->expectedLogSets = lsConf.expectedLogSets;
	logSystem->logRouterTags = lsConf.logRouterTags;
	logSystem->txsTags = lsConf.txsTags;
	logSystem->recruitmentID = lsConf.recruitmentID;
	logSystem->stopped = lsConf.stopped;
	if (useRecoveredAt) {
		logSystem->recoveredAt = lsConf.recoveredAt;
	}
	logSystem->pseudoLocalities = lsConf.pseudoLocalities;
	for (const TLogSet& tLogSet : lsConf.tLogs) {
		if (!excludeRemote || tLogSet.isLocal) {
			logSystem->tLogs.push_back(makeReference<LogSet>(tLogSet));
		}
	}

	for (const auto& oldTlogConf : lsConf.oldTLogs) {
		logSystem->oldLogData.emplace_back(oldTlogConf);
	}

	logSystem->logSystemType = lsConf.logSystemType;
	logSystem->oldestBackupEpoch = lsConf.oldestBackupEpoch;
	return logSystem;
}

} // namespace ptxn