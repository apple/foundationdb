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

struct OldTLogCoreData {
	vector< UID > tLogs;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	std::vector< LocalityData > tLogLocalities; // Stores the localities of the log servers
	IRepPolicyRef	tLogPolicy;
	Version epochEnd;

	OldTLogCoreData() : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), epochEnd(0) {}

	bool operator == (OldTLogCoreData const& rhs) const { return tLogs == rhs.tLogs && tLogWriteAntiQuorum == rhs.tLogWriteAntiQuorum && tLogReplicationFactor == rhs.tLogReplicationFactor && epochEnd == rhs.epochEnd && ((!tLogPolicy && !rhs.tLogPolicy) || (tLogPolicy && rhs.tLogPolicy && (tLogPolicy->info() == rhs.tLogPolicy->info()))); }

	template <class Archive>
	void serialize(Archive& ar) {
		ar & tLogs & tLogWriteAntiQuorum & tLogReplicationFactor & tLogPolicy & epochEnd & tLogLocalities;
	}
};

struct DBCoreState {
	vector< UID > tLogs;
	int32_t tLogWriteAntiQuorum;        // The write anti quorum previously used to write to tLogs, which might be different from the anti quorum suggested by the current configuration going forward!
	int32_t tLogReplicationFactor;  // The replication factor previously used to write to tLogs, which might be different from the current configuration

	IRepPolicyRef	tLogPolicy;
	DBRecoveryCount recoveryCount;  // Increases with sequential successful recoveries.
	int logSystemType;
	std::vector< LocalityData > tLogLocalities; // Stores the localities of the log servers

	std::vector<OldTLogCoreData> oldTLogData;

	DBCoreState() : recoveryCount(0), tLogWriteAntiQuorum(0), tLogReplicationFactor(0), logSystemType(0) {}

	vector<UID> getPriorCommittedLogServers() {
		vector<UID> priorCommittedLogServers;
		for(int i = 0; i < oldTLogData.size(); i++) {
			for(auto it : oldTLogData[i].tLogs) {
				priorCommittedLogServers.push_back(it);
			}
		}
		return priorCommittedLogServers;
	}

	bool isEqual(DBCoreState const& r) const {
		if (logSystemType != r.logSystemType || recoveryCount != r.recoveryCount || tLogWriteAntiQuorum != r.tLogWriteAntiQuorum || tLogReplicationFactor != r.tLogReplicationFactor || tLogs.size() != r.tLogs.size() || oldTLogData.size() != r.oldTLogData.size() || tLogLocalities != r.tLogLocalities || tLogs != r.tLogs)
			return false;
		for(int i = 0; i < oldTLogData.size(); i++ ) {
			if (oldTLogData[i] != r.oldTLogData[i])
				return false;
		}
		return true;
	}
	bool operator == ( const DBCoreState& rhs ) const { return isEqual(rhs); }

	template <class Archive>
	void serialize(Archive& ar) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A320050001LL );
		ar & tLogs & tLogWriteAntiQuorum & recoveryCount & tLogReplicationFactor & logSystemType;
		if(ar.protocolVersion() >= 0x0FDB00A460010001LL) {
			uint64_t tLocalitySize = (uint64_t)tLogLocalities.size();
			ar & oldTLogData & tLogPolicy & tLocalitySize;
			if (ar.isDeserializing) {
				tLogLocalities.reserve(tLocalitySize);
				for (size_t i = 0; i < tLocalitySize; i++) {
					LocalityData locality;
					ar & locality;
					tLogLocalities.push_back(locality);
				}
			}
			else {
				for (auto& locality : tLogLocalities) {
					ar & locality;
				}
			}
		}
		else if(ar.isDeserializing) {	
			oldTLogData.clear();
			tLogPolicy = IRepPolicyRef(new PolicyAcross(tLogReplicationFactor, "zoneid", IRepPolicyRef(new PolicyOne())));

			if(ar.protocolVersion() >= 0x0FDB00A400000001LL) {
				oldTLogData.push_back(OldTLogCoreData());
				ar & oldTLogData[0].tLogs & oldTLogData[0].epochEnd & oldTLogData[0].tLogWriteAntiQuorum & oldTLogData[0].tLogReplicationFactor;
				if(!oldTLogData[0].tLogs.size()) {
					oldTLogData.pop_back();
				} else {
					for(int i = 0; i < oldTLogData.size(); i++ ) {
						oldTLogData[i].tLogPolicy = IRepPolicyRef(new PolicyAcross(oldTLogData[i].tLogReplicationFactor, "zoneid", IRepPolicyRef(new PolicyOne())));
						if (oldTLogData[i].tLogs.size()) {
							oldTLogData[i].tLogLocalities.reserve(oldTLogData[i].tLogs.size());
							for (auto& tLog : oldTLogData[i].tLogs) {
								LocalityData locality;
								locality.set(LocalityData::keyZoneId, g_random->randomUniqueID().toString());
								locality.set(LocalityData::keyDataHallId, LiteralStringRef("0"));
								oldTLogData[i].tLogLocalities.push_back(locality);
							}
						}
					}
				}
			}

			tLogLocalities.reserve(tLogs.size());
			for (auto& tLog : tLogs) {
				LocalityData locality;
				locality.set(LocalityData::keyZoneId, g_random->randomUniqueID().toString());
				locality.set(LocalityData::keyDataHallId, LiteralStringRef("0"));
				tLogLocalities.push_back(locality);
			}
		}

		TraceEvent("CoreStateSerialize").detail("AntiQuorum", tLogWriteAntiQuorum)
			.detail("logSystemType", logSystemType).detail("recoveryCount", recoveryCount)
			.detail("tLogReplicationFactor", tLogReplicationFactor)
			.detail("tLogPolicy", (tLogPolicy.getPtr()) ? tLogPolicy->info() : "[unset]")
			.detail("logs", describe(tLogs)).detail("procotol", ar.protocolVersion())
			.detail("oldTLogData", oldTLogData.size())
			.detail("deserializing", ar.isDeserializing);
	}

	std::string toString() const { return format("type: %d anti: %d replication: %d policy: %s tLogs: %s oldGenerations: %d  tlocalities: %s",
		logSystemType, tLogWriteAntiQuorum, tLogReplicationFactor, (tLogPolicy ? tLogPolicy->info().c_str() : "[unset]"), describe(tLogs).c_str(), oldTLogData.size(), describe(tLogLocalities).c_str()); }
};

#endif
