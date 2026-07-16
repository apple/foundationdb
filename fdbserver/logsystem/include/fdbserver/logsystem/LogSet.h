/*
 * LogSet.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_LOGSYSTEM_LOGSET_H
#define FDBSERVER_LOGSYSTEM_LOGSET_H
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "fdbrpc/Locality.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/core/DBCoreState.h"
#include "fdbserver/core/LogSystemConfig.h"
#include "fdbserver/core/TLogInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "flow/Histogram.h"

struct ConnectionResetInfo : public ReferenceCounted<ConnectionResetInfo> {
	double lastReset;
	Future<Void> resetCheck;
	int slowReplies;
	int fastReplies;

	ConnectionResetInfo() : lastReset(now()), resetCheck(Void()), slowReplies(0), fastReplies(0) {}
};

class LogSet : NonCopyable, public ReferenceCounted<LogSet> {
public:
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers;
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logRouters;
	std::vector<Reference<AsyncVar<OptionalInterface<BackupInterface>>>> backupWorkers;
	std::vector<Reference<ConnectionResetInfo>> connectionResetTrackers;
	std::vector<Reference<Histogram>> tlogPushDistTrackers;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	std::vector<LocalityData> tLogLocalities;
	TLogVersion tLogVersion;
	Reference<IReplicationPolicy> tLogPolicy;
	Reference<LocalitySet> logServerSet;
	std::vector<int> logIndexArray;
	std::vector<LocalityEntry> logEntryArray;
	bool isLocal;
	int8_t locality;
	Version startVersion;
	std::vector<Future<TLogLockResult>> replies;
	std::vector<std::vector<int>> satelliteTagLocations;

	LogSet()
	  : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), isLocal(true), locality(tagLocalityInvalid),
	    startVersion(invalidVersion) {}
	explicit LogSet(const TLogSet& tlogSet);
	explicit LogSet(const CoreTLogSet& coreSet);

	std::string logRouterString();
	bool hasLogRouter(UID id) const;
	bool hasBackupWorker(UID id) const;
	std::string logServerString();
	void populateSatelliteTagLocations(int logRouterTags,
	                                   int oldLogRouterTags,
	                                   int txsTags,
	                                   int oldTxsTags,
	                                   int cdcTags);
	void checkSatelliteTagLocations();
	int bestLocationFor(Tag tag);
	void updateLocalitySet(std::vector<LocalityData> const& localities);
	bool satisfiesPolicy(const std::vector<LocalityEntry>& locations);
	void getPushLocations(
	    VectorRef<Tag> tags,
	    std::vector<int>& locations,
	    int locationOffset,
	    bool allLocations = false,
	    const Optional<Reference<LocalitySet>>& restrictedLogSet = Optional<Reference<LocalitySet>>());

private:
	int satelliteTagLocationIndex(Tag tag) const;
	std::vector<LocalityEntry> alsoServers, resultEntries;
	std::vector<int> newLocations;
};

#endif // FDBSERVER_LOGSYSTEM_LOGSET_H
