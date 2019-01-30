/*
 * DBCoreState.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_DBCORESTATE_H
#define FDBSERVER_DBCORESTATE_H
#pragma once

#include "fdbrpc/ReplicationPolicy.h"

// This structure is stored persistently in CoordinatedState and must be versioned carefully!
// It records a synchronous replication topology which can be used in the absence of faults (or under a limited
//   number of failures, in the case of less than full write quorums) to durably commit transactions.  When faults or
//   configuration changes require the topology to be changed, (a read quorum of) the old topology is locked, a new
//   topology is constructed, and then committed to this coordinated state before becoming active.  This process
//   is called 'recovery'.
// At the moment, transaction logs are replicated but not partitioned, so the topology is as simple as a list of
//   transaction log replicas and the write quorum that was used to commit to them.  The read quorum required to
//   ensure durability of locking and recovery is therefore tLogWriteAntiQuorum + 1.

struct CoreTLogSet {
	std::vector< UID > tLogs;
	int32_t tLogWriteAntiQuorum; // The write anti quorum previously used to write to tLogs, which might be different from the anti quorum suggested by the current configuration going forward!
	int32_t tLogReplicationFactor; // The replication factor previously used to write to tLogs, which might be different from the current configuration
	std::vector< LocalityData > tLogLocalities; // Stores the localities of the log servers
	IRepPolicyRef tLogPolicy;
	bool isLocal;
	int8_t locality;
	Version startVersion;
	std::vector<std::vector<int>> satelliteTagLocations;

	CoreTLogSet() : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), isLocal(true), locality(tagLocalityUpgraded), startVersion(invalidVersion) {}

	bool operator == (CoreTLogSet const& rhs) const { 
		return tLogs == rhs.tLogs && tLogWriteAntiQuorum == rhs.tLogWriteAntiQuorum && tLogReplicationFactor == rhs.tLogReplicationFactor && isLocal == rhs.isLocal && satelliteTagLocations == rhs.satelliteTagLocations &&
			locality == rhs.locality && startVersion == rhs.startVersion && ((!tLogPolicy && !rhs.tLogPolicy) || (tLogPolicy && rhs.tLogPolicy && (tLogPolicy->info() == rhs.tLogPolicy->info()))); 
	}

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, tLogs, tLogWriteAntiQuorum, tLogReplicationFactor, tLogPolicy, tLogLocalities, isLocal, locality, startVersion, satelliteTagLocations);
	}
};

struct OldTLogCoreData {
	std::vector<CoreTLogSet> tLogs;
	int32_t logRouterTags;
	Version epochEnd;

	OldTLogCoreData() : epochEnd(0), logRouterTags(0) {}

	bool operator == (OldTLogCoreData const& rhs) const { 
		return tLogs == rhs.tLogs && logRouterTags == rhs.logRouterTags && epochEnd == rhs.epochEnd;
	}

	template <class Archive>
	void serialize(Archive& ar) {
		if( ar.protocolVersion() >= 0x0FDB00A560010001LL) {
			serializer(ar, tLogs, logRouterTags, epochEnd);
		}
		else if(ar.isDeserializing) {
			tLogs.push_back(CoreTLogSet());
			serializer(ar, tLogs[0].tLogs, tLogs[0].tLogWriteAntiQuorum, tLogs[0].tLogReplicationFactor, tLogs[0].tLogPolicy, epochEnd, tLogs[0].tLogLocalities);
		}
	}
};

struct DBCoreState {
	std::vector<CoreTLogSet> tLogs;
	int32_t logRouterTags;
	std::vector<OldTLogCoreData> oldTLogData;
	DBRecoveryCount recoveryCount;  // Increases with sequential successful recoveries.
	int logSystemType;
	
	DBCoreState() : logRouterTags(0), recoveryCount(0), logSystemType(0) {}

	vector<UID> getPriorCommittedLogServers() {
		vector<UID> priorCommittedLogServers;
		for(auto& it : tLogs) {
			for(auto& log : it.tLogs) {
				priorCommittedLogServers.push_back(log);
			}
		}
		for(int i = 0; i < oldTLogData.size(); i++) {
			for(auto& it : oldTLogData[i].tLogs) {
				for(auto& log : it.tLogs) {
					priorCommittedLogServers.push_back(log);
				}
			}
		}
		return priorCommittedLogServers;
	}

	bool isEqual(DBCoreState const& r) const {
		return logSystemType == r.logSystemType && recoveryCount == r.recoveryCount && tLogs == r.tLogs && oldTLogData == r.oldTLogData && logRouterTags == r.logRouterTags;
	}
	bool operator == ( const DBCoreState& rhs ) const { return isEqual(rhs); }

	template <class Archive>
	void serialize(Archive& ar) {
		//FIXME: remove when we no longer need to test upgrades from 4.X releases
		if(g_network->isSimulated() && ar.protocolVersion() < 0x0FDB00A460010001LL) {
			TraceEvent("ElapsedTime").detail("SimTime", now()).detail("RealTime", 0).detail("RandomUnseed", 0);
			flushAndExit(0);
		}
		
		ASSERT(ar.protocolVersion() >= 0x0FDB00A460010001LL);
		if(ar.protocolVersion() >= 0x0FDB00A560010001LL) {
			serializer(ar, tLogs, logRouterTags, oldTLogData, recoveryCount, logSystemType);
		} else if(ar.isDeserializing) {
			tLogs.push_back(CoreTLogSet());
			serializer(ar, tLogs[0].tLogs, tLogs[0].tLogWriteAntiQuorum, recoveryCount, tLogs[0].tLogReplicationFactor, logSystemType);

			uint64_t tLocalitySize = (uint64_t)tLogs[0].tLogLocalities.size();
			serializer(ar, oldTLogData, tLogs[0].tLogPolicy, tLocalitySize);
			if (ar.isDeserializing) {
				tLogs[0].tLogLocalities.reserve(tLocalitySize);
				for (size_t i = 0; i < tLocalitySize; i++) {
					LocalityData locality;
					serializer(ar, locality);
					tLogs[0].tLogLocalities.push_back(locality);
				}

				if(oldTLogData.size()) {
					tLogs[0].startVersion = oldTLogData[0].epochEnd;
					for(int i = 0; i < oldTLogData.size() - 1; i++) {
						oldTLogData[i].tLogs[0].startVersion = oldTLogData[i+1].epochEnd;
					}
				}
			}
		}
	}
};

#endif
