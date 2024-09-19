/*
 * DBCoreState.h
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

#ifndef FDBSERVER_DBCORESTATE_H
#define FDBSERVER_DBCORESTATE_H

#include <set>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/MasterInterface.h"
#include "flow/ObjectSerializerTraits.h"
#include "fdbserver/Knobs.h"

class LogSet;
struct OldLogData;

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
	std::vector<UID> tLogs;
	int32_t tLogWriteAntiQuorum; // The write anti quorum previously used to write to tLogs, which might be different
	                             // from the anti quorum suggested by the current configuration going forward!
	int32_t tLogReplicationFactor; // The replication factor previously used to write to tLogs, which might be different
	                               // from the current configuration
	std::vector<LocalityData> tLogLocalities; // Stores the localities of the log servers
	Reference<IReplicationPolicy> tLogPolicy;
	bool isLocal;
	int8_t locality;
	Version startVersion;
	std::vector<std::vector<int>> satelliteTagLocations;
	TLogVersion tLogVersion;

	CoreTLogSet()
	  : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), isLocal(true), locality(tagLocalityInvalid),
	    startVersion(invalidVersion) {}
	explicit CoreTLogSet(const LogSet& logset);

	bool operator==(CoreTLogSet const& rhs) const {
		return tLogs == rhs.tLogs && tLogWriteAntiQuorum == rhs.tLogWriteAntiQuorum &&
		       tLogReplicationFactor == rhs.tLogReplicationFactor && isLocal == rhs.isLocal &&
		       satelliteTagLocations == rhs.satelliteTagLocations && locality == rhs.locality &&
		       startVersion == rhs.startVersion &&
		       ((!tLogPolicy && !rhs.tLogPolicy) ||
		        (tLogPolicy && rhs.tLogPolicy && (tLogPolicy->info() == rhs.tLogPolicy->info())));
	}

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar,
		           tLogs,
		           tLogWriteAntiQuorum,
		           tLogReplicationFactor,
		           tLogPolicy,
		           tLogLocalities,
		           isLocal,
		           locality,
		           startVersion,
		           satelliteTagLocations,
		           tLogVersion);
	}
};

struct OldTLogCoreData {
	std::vector<CoreTLogSet> tLogs;
	int32_t logRouterTags;
	int32_t txsTags;
	Version epochBegin, epochEnd;
	Version recoverAt;
	std::set<int8_t> pseudoLocalities;
	LogEpoch epoch;

	OldTLogCoreData() : logRouterTags(0), txsTags(0), epochBegin(0), epochEnd(0), recoverAt(0), epoch(0) {}
	explicit OldTLogCoreData(const OldLogData&);

	bool operator==(const OldTLogCoreData& rhs) const {
		if (SERVER_KNOBS->RECORD_RECOVER_AT_IN_CSTATE) {
			return tLogs == rhs.tLogs && logRouterTags == rhs.logRouterTags && txsTags == rhs.txsTags &&
			       epochBegin == rhs.epochBegin && epochEnd == rhs.epochEnd && recoverAt == rhs.recoverAt &&
			       pseudoLocalities == rhs.pseudoLocalities && epoch == rhs.epoch;
		} else {
			return tLogs == rhs.tLogs && logRouterTags == rhs.logRouterTags && txsTags == rhs.txsTags &&
			       epochBegin == rhs.epochBegin && epochEnd == rhs.epochEnd &&
			       pseudoLocalities == rhs.pseudoLocalities && epoch == rhs.epoch;
		}
	}

	template <class Archive>
	void serialize(Archive& ar) {
		ASSERT_WE_THINK(ar.protocolVersion().hasBackupWorker());
		serializer(ar,
		           tLogs,
		           logRouterTags,
		           epochEnd,
		           pseudoLocalities, // since 6.1
		           txsTags, // since 6.1
		           epoch, // since 6.3
		           epochBegin // since 6.3
		);
		if (ar.protocolVersion().hasGcTxnGenerations()) {
			serializer(ar, recoverAt); // since 7.3
		}
	}
};

struct DBCoreState {
	std::vector<CoreTLogSet> tLogs;
	int32_t logRouterTags;
	int32_t txsTags;
	std::vector<OldTLogCoreData> oldTLogData;
	DBRecoveryCount recoveryCount; // Increases with sequential successful recoveries.
	LogSystemType logSystemType;
	std::set<int8_t> pseudoLocalities;
	ProtocolVersion newestProtocolVersion;
	ProtocolVersion lowestCompatibleProtocolVersion;
	EncryptionAtRestMode encryptionAtRestMode; // cluster encryption data at-rest mode

	DBCoreState()
	  : logRouterTags(0), txsTags(0), recoveryCount(0), logSystemType(LogSystemType::empty),
	    newestProtocolVersion(ProtocolVersion::invalidProtocolVersion),
	    lowestCompatibleProtocolVersion(ProtocolVersion::invalidProtocolVersion),
	    encryptionAtRestMode(EncryptionAtRestMode()) {}

	std::vector<UID> getPriorCommittedLogServers() {
		std::vector<UID> priorCommittedLogServers;
		for (auto& it : tLogs) {
			for (auto& log : it.tLogs) {
				priorCommittedLogServers.push_back(log);
			}
		}
		for (int i = 0; i < oldTLogData.size(); i++) {
			for (auto& it : oldTLogData[i].tLogs) {
				for (auto& log : it.tLogs) {
					priorCommittedLogServers.push_back(log);
				}
			}
		}
		return priorCommittedLogServers;
	}

	bool isEqual(const DBCoreState& r) const {
		return logSystemType == r.logSystemType && recoveryCount == r.recoveryCount && tLogs == r.tLogs &&
		       oldTLogData == r.oldTLogData && logRouterTags == r.logRouterTags && txsTags == r.txsTags &&
		       pseudoLocalities == r.pseudoLocalities && encryptionAtRestMode == r.encryptionAtRestMode;
	}
	bool operator==(const DBCoreState& rhs) const { return isEqual(rhs); }
	bool operator!=(const DBCoreState& rhs) const { return !isEqual(rhs); }

	template <class Archive>
	void serialize(Archive& ar) {
		ASSERT_WE_THINK(ar.protocolVersion().hasShardedTxsTags()); // 6.1
		serializer(ar,
		           tLogs,
		           logRouterTags,
		           oldTLogData,
		           recoveryCount,
		           logSystemType,
		           pseudoLocalities, // since 6.1
		           txsTags); // since 6.1

		if (ar.protocolVersion().hasSWVersionTracking()) {
			serializer(ar, newestProtocolVersion, lowestCompatibleProtocolVersion); // 7.2
		}
		if (ar.protocolVersion().hasEncryptionAtRest()) {
			serializer(ar, encryptionAtRestMode); // 7.2
		}
	}
};

#endif
