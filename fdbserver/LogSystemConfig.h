/*
 * LogSystemConfig.h
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

#ifndef FDBSERVER_LOGSYSTEMCONFIG_H
#define FDBSERVER_LOGSYSTEMCONFIG_H
#pragma once

#include "fdbserver/BackupInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbclient/DatabaseConfiguration.h"

template <class Interface>
struct OptionalInterface {
	friend struct serializable_traits<OptionalInterface<Interface>>;
	// Represents an interface with a known id() and possibly known actual endpoints.
	// For example, an OptionalInterface<TLogInterface> represents a particular tlog by id, which you might or might not presently know how to communicate with

	UID id() const { return ident; }
	bool present() const { return iface.present(); }
	Interface const& interf() const { return iface.get(); }

	explicit OptionalInterface( UID id ) : ident(id) {}
	explicit OptionalInterface( Interface const& i ) : ident(i.id()), iface(i) {}
	OptionalInterface() {}

	std::string toString() const { return ident.toString(); }

	bool operator==(UID const& r) const { return ident == r; }

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, iface);
		if( !iface.present() ) serializer(ar, ident);
		else ident = iface.get().id();
	}

protected:
	UID ident;
	Optional<Interface> iface;
};

class LogSet;
struct OldLogData;

template <class Interface>
struct serializable_traits<OptionalInterface<Interface>> : std::true_type {
	template <class Archiver>
	static void serialize(Archiver& ar, OptionalInterface<Interface>& m) {
		if constexpr (!Archiver::isDeserializing) {
			if (m.iface.present()) {
				m.ident = m.iface.get().id();
			}
		}
		::serializer(ar, m.iface, m.ident);
		if constexpr (Archiver::isDeserializing) {
			if (m.iface.present()) {
				m.ident = m.iface.get().id();
			}
		}
	}
};


struct TLogSet {
	constexpr static FileIdentifier file_identifier = 6302317;
	std::vector<OptionalInterface<TLogInterface>> tLogs;
	std::vector<OptionalInterface<TLogInterface>> logRouters;
	std::vector<OptionalInterface<BackupInterface>> backupWorkers;
	int32_t tLogWriteAntiQuorum, tLogReplicationFactor;
	std::vector< LocalityData > tLogLocalities; // Stores the localities of the log servers
	TLogVersion tLogVersion;
	Reference<IReplicationPolicy> tLogPolicy;
	bool isLocal;
	int8_t locality;
	Version startVersion;
	std::vector<std::vector<int>> satelliteTagLocations;

	TLogSet() : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), isLocal(true), locality(tagLocalityInvalid), startVersion(invalidVersion) {}
	explicit TLogSet(const LogSet& rhs);

	std::string toString() const {
		return format("anti: %d replication: %d local: %d routers: %d tLogs: %s backupWorkers: %s locality: %d",
		              tLogWriteAntiQuorum, tLogReplicationFactor, isLocal, logRouters.size(), describe(tLogs).c_str(),
		              describe(backupWorkers).c_str(), locality);
	}

	bool operator == ( const TLogSet& rhs ) const {
		if (tLogWriteAntiQuorum != rhs.tLogWriteAntiQuorum || tLogReplicationFactor != rhs.tLogReplicationFactor ||
		    isLocal != rhs.isLocal || satelliteTagLocations != rhs.satelliteTagLocations ||
		    startVersion != rhs.startVersion || tLogs.size() != rhs.tLogs.size() || locality != rhs.locality ||
		    logRouters.size() != rhs.logRouters.size() || backupWorkers.size() != rhs.backupWorkers.size()) {
			return false;
		}
		if ((tLogPolicy && !rhs.tLogPolicy) || (!tLogPolicy && rhs.tLogPolicy) || (tLogPolicy && (tLogPolicy->info() != rhs.tLogPolicy->info()))) {
			return false;
		}
		for(int j = 0; j < tLogs.size(); j++ ) {
			if (tLogs[j].id() != rhs.tLogs[j].id() || tLogs[j].present() != rhs.tLogs[j].present() || ( tLogs[j].present() && tLogs[j].interf().commit.getEndpoint().token != rhs.tLogs[j].interf().commit.getEndpoint().token ) ) {
				return false;
			}
		}
		for(int j = 0; j < logRouters.size(); j++ ) {
			if (logRouters[j].id() != rhs.logRouters[j].id() || logRouters[j].present() != rhs.logRouters[j].present() || ( logRouters[j].present() && logRouters[j].interf().commit.getEndpoint().token != rhs.logRouters[j].interf().commit.getEndpoint().token ) ) {
				return false;
			}
		}
		for (int j = 0; j < backupWorkers.size(); j++) {
			if (backupWorkers[j].id() != rhs.backupWorkers[j].id() ||
			    backupWorkers[j].present() != rhs.backupWorkers[j].present() ||
			    (backupWorkers[j].present() &&
			     backupWorkers[j].interf().getToken() != rhs.backupWorkers[j].interf().getToken())) {
				return false;
			}
		}
		return true;
	}

	bool isEqualIds(TLogSet const& r) const {
		if (tLogWriteAntiQuorum != r.tLogWriteAntiQuorum || tLogReplicationFactor != r.tLogReplicationFactor || isLocal != r.isLocal || satelliteTagLocations != r.satelliteTagLocations ||
			startVersion != r.startVersion || tLogs.size() != r.tLogs.size() || locality != r.locality) {
			return false;
		}
		if ((tLogPolicy && !r.tLogPolicy) || (!tLogPolicy && r.tLogPolicy) || (tLogPolicy && (tLogPolicy->info() != r.tLogPolicy->info()))) {
			return false;
		}
		for(int i = 0; i < tLogs.size(); i++) {
			if( tLogs[i].id() != r.tLogs[i].id() ) {
				return false;
			}
		}
		return true;
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, tLogs, logRouters, tLogWriteAntiQuorum, tLogReplicationFactor, tLogPolicy, tLogLocalities,
		           isLocal, locality, startVersion, satelliteTagLocations, tLogVersion, backupWorkers);
	}
};

struct OldTLogConf {
	constexpr static FileIdentifier file_identifier = 16233772;
	std::vector<TLogSet> tLogs;
	Version epochBegin, epochEnd;
	int32_t logRouterTags;
	int32_t txsTags;
	std::set<int8_t> pseudoLocalities; // Tracking pseudo localities, e.g., tagLocalityLogRouterMapped, used in the old epoch.
	LogEpoch epoch;

	OldTLogConf() : epochBegin(0), epochEnd(0), logRouterTags(0), txsTags(0), epoch(0) {}
	explicit OldTLogConf(const OldLogData&);

	std::string toString() const {
		return format("end: %d tags: %d %s", epochEnd, logRouterTags, describe(tLogs).c_str());
	}

	bool operator==(const OldTLogConf& rhs) const {
		return tLogs == rhs.tLogs && epochBegin == rhs.epochBegin && epochEnd == rhs.epochEnd &&
		       logRouterTags == rhs.logRouterTags && txsTags == rhs.txsTags &&
		       pseudoLocalities == rhs.pseudoLocalities && epoch == rhs.epoch;
	}

	bool isEqualIds(OldTLogConf const& r) const {
		if(tLogs.size() != r.tLogs.size()) {
			return false;
		}
		for(int i = 0; i < tLogs.size(); i++) {
			if(!tLogs[i].isEqualIds(r.tLogs[i])) {
				return false;
			}
		}
		return true;
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, tLogs, epochBegin, epochEnd, logRouterTags, pseudoLocalities, txsTags, epoch);
	}
};

// LogSystemType is always 2 (tagPartitioned). There is no other tag partitioned system.
// This type is supposed to be removed. However, because the serialized value of the type is stored in coordinators,
// removing it is complex in order to support forward and backward compatibility.
enum class LogSystemType {
	empty = 0, // Never used.
	tagPartitioned = 2,
};
BINARY_SERIALIZABLE(LogSystemType);

struct LogSystemConfig {
	constexpr static FileIdentifier file_identifier = 16360847;
	LogSystemType logSystemType;
	std::vector<TLogSet> tLogs;
	int32_t logRouterTags;
	int32_t txsTags;
	std::vector<OldTLogConf> oldTLogs;
	int32_t expectedLogSets;
	UID recruitmentID;
	bool stopped;
	Optional<Version> recoveredAt;
	std::set<int8_t> pseudoLocalities;
	LogEpoch epoch;
	LogEpoch oldestBackupEpoch;

	LogSystemConfig(LogEpoch e = 0)
	  : logSystemType(LogSystemType::empty), logRouterTags(0), txsTags(0), expectedLogSets(0), stopped(false), epoch(e),
	    oldestBackupEpoch(e) {}

	std::string toString() const {
		return format("type: %d oldGenerations: %d tags: %d %s", logSystemType, oldTLogs.size(), logRouterTags, describe(tLogs).c_str());
	}

	Optional<Key> getRemoteDcId() const {
		for( int i = 0; i < tLogs.size(); i++ ) {
			if(!tLogs[i].isLocal) {
				for( int j = 0; j < tLogs[i].tLogs.size(); j++ ) {
					if( tLogs[i].tLogs[j].present() ) {
						return tLogs[i].tLogs[j].interf().filteredLocality.dcId();
					}
				}
			}
		}
		return Optional<Key>();
	}

	std::vector<TLogInterface> allLocalLogs(bool includeSatellite = true) const {
		std::vector<TLogInterface> results;
		for( int i = 0; i < tLogs.size(); i++ ) {
			// skip satellite TLogs, if it was not needed
			if (!includeSatellite && tLogs[i].locality == tagLocalitySatellite) {
				continue;
			}
			if(tLogs[i].isLocal) {
				for( int j = 0; j < tLogs[i].tLogs.size(); j++ ) {
					if( tLogs[i].tLogs[j].present() ) {
						results.push_back(tLogs[i].tLogs[j].interf());
					}
				}
			}
		}
		return results;
	}

	std::vector<TLogInterface> allPresentLogs() const {
		std::vector<TLogInterface> results;
		for( int i = 0; i < tLogs.size(); i++ ) {
			for( int j = 0; j < tLogs[i].tLogs.size(); j++ ) {
				if( tLogs[i].tLogs[j].present() ) {
					results.push_back(tLogs[i].tLogs[j].interf());
				}
			}
		}
		return results;
	}

	std::pair<int8_t,int8_t> getLocalityForDcId(Optional<Key> dcId) const {
		std::map<int8_t, int> matchingLocalities;
		std::map<int8_t, int> allLocalities;
		for( auto& tLogSet : tLogs ) {
			for( auto& tLog : tLogSet.tLogs ) {
				if( tLogSet.locality >= 0 ) {
					if( tLog.present() && tLog.interf().filteredLocality.dcId() == dcId ) {
						matchingLocalities[tLogSet.locality]++;
					} else {
						allLocalities[tLogSet.locality]++;
					}
				}
			}
		}

		for(auto& oldLog : oldTLogs) {
			for( auto& tLogSet : oldLog.tLogs ) {
				for( auto& tLog : tLogSet.tLogs ) {
					if( tLogSet.locality >= 0 ) {
						if( tLog.present() && tLog.interf().filteredLocality.dcId() == dcId ) {
							matchingLocalities[tLogSet.locality]++;
						} else {
							allLocalities[tLogSet.locality]++;
						}
					}
				}
			}
		}

		int8_t bestLoc = tagLocalityInvalid;
		int bestLocalityCount = -1;
		for(auto& it : matchingLocalities) {
			if(it.second > bestLocalityCount) {
				bestLoc = it.first;
				bestLocalityCount = it.second;
			}
		}

		int8_t secondLoc = tagLocalityInvalid;
		int8_t thirdLoc = tagLocalityInvalid;
		int secondLocalityCount = -1;
		int thirdLocalityCount = -1;
		for(auto& it : allLocalities) {
			if(bestLoc != it.first) {
				if(it.second > secondLocalityCount) {
					thirdLoc = secondLoc;
					thirdLocalityCount = secondLocalityCount;
					secondLoc = it.first;
					secondLocalityCount = it.second;
				} else if(it.second > thirdLocalityCount) {
					thirdLoc = it.first;
					thirdLocalityCount = it.second;
				}
			}
		}

		if(bestLoc != tagLocalityInvalid) {
			return std::make_pair(bestLoc, secondLoc);
		}
		return std::make_pair(secondLoc, thirdLoc);
	}

	std::vector<std::pair<UID, NetworkAddress>> allSharedLogs() const {
		typedef std::pair<UID, NetworkAddress> IdAddrPair;
		std::vector<IdAddrPair> results;
		for (auto &tLogSet : tLogs) {
			for (auto &tLog : tLogSet.tLogs) {
				if (tLog.present())
					results.push_back(IdAddrPair(tLog.interf().getSharedTLogID(), tLog.interf().address()));
			}
		}

		for (auto &oldLog : oldTLogs) {
			for (auto &tLogSet : oldLog.tLogs) {
				for (auto &tLog : tLogSet.tLogs) {
					if (tLog.present())
						results.push_back(IdAddrPair(tLog.interf().getSharedTLogID(), tLog.interf().address()));
				}
			}
		}
		uniquify(results);
		// This assert depends on the fact that uniquify will sort the elements based on <UID, NetworkAddr> order
		ASSERT_WE_THINK(std::unique(results.begin(), results.end(), [](IdAddrPair &x, IdAddrPair &y) { return x.first == y.first; }) == results.end());
		return results;
	}

	bool operator == ( const LogSystemConfig& rhs ) const { return isEqual(rhs); }

	bool isEqual(LogSystemConfig const& r) const {
		return logSystemType == r.logSystemType && tLogs == r.tLogs && oldTLogs == r.oldTLogs &&
		       expectedLogSets == r.expectedLogSets && logRouterTags == r.logRouterTags && txsTags == r.txsTags &&
		       recruitmentID == r.recruitmentID && stopped == r.stopped && recoveredAt == r.recoveredAt &&
		       pseudoLocalities == r.pseudoLocalities && epoch == r.epoch && oldestBackupEpoch == r.oldestBackupEpoch;
	}

	bool isEqualIds(LogSystemConfig const& r) const {
		for( auto& i : r.tLogs ) {
			for( auto& j : tLogs ) {
				if( i.isEqualIds(j) ) {
					return true;
				}
			}
		}
		return false;
	}

	bool isNextGenerationOf(LogSystemConfig const& r) const {
		if( !oldTLogs.size() ) {
			return false;
		}

		for( auto& i : r.tLogs ) {
			for( auto& j : oldTLogs[0].tLogs ) {
				if( i.isEqualIds(j) ) {
					return true;
				}
			}
		}
		return false;
	}

	bool hasTLog(UID tid) const {
		for (const auto& log : tLogs) {
			if (std::count(log.tLogs.begin(), log.tLogs.end(), tid) > 0) {
				return true;
			}
		}
		for (const auto& old : oldTLogs) {
			for (const auto& log : old.tLogs) {
				if (std::count(log.tLogs.begin(), log.tLogs.end(), tid) > 0) {
					return true;
				}
			}
		}
		return false;
	}

	bool hasLogRouter(UID rid) const {
		for (const auto& log : tLogs) {
			if (std::count(log.logRouters.begin(), log.logRouters.end(), rid) > 0) {
				return true;
			}
		}
		for (const auto& old : oldTLogs) {
			for (const auto& log : old.tLogs) {
				if (std::count(log.logRouters.begin(), log.logRouters.end(), rid) > 0) {
					return true;
				}
			}
		}
		return false;
	}

	bool hasBackupWorker(UID bid) const {
		for (const auto& log : tLogs) {
			if (std::count(log.backupWorkers.begin(), log.backupWorkers.end(), bid) > 0) {
				return true;
			}
		}
		for (const auto& old : oldTLogs) {
			for (const auto& log : old.tLogs) {
				if (std::count(log.backupWorkers.begin(), log.backupWorkers.end(), bid) > 0) {
					return true;
				}
			}
		}
		return false;
	}

	Version getEpochEndVersion(LogEpoch epoch) const {
		for (const auto& old : oldTLogs) {
			if (old.epoch == epoch) {
				return old.epochEnd;
			}
		}
		return invalidVersion;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, logSystemType, tLogs, logRouterTags, oldTLogs, expectedLogSets, recruitmentID, stopped,
		           recoveredAt, pseudoLocalities, txsTags, epoch, oldestBackupEpoch);
	}
};

#endif
