/*
 * LogSystemConfig.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/LogSystemConfig.h"

std::string TLogSet::toString() const {
	return format("anti: %d replication: %d local: %d routers: %d tLogs: %s backupWorkers: %s locality: %d",
	              tLogWriteAntiQuorum,
	              tLogReplicationFactor,
	              isLocal,
	              logRouters.size(),
	              describe(tLogs).c_str(),
	              describe(backupWorkers).c_str(),
	              locality);
}

bool TLogSet::operator==(const TLogSet& rhs) const {
	if (tLogWriteAntiQuorum != rhs.tLogWriteAntiQuorum || tLogReplicationFactor != rhs.tLogReplicationFactor ||
	    isLocal != rhs.isLocal || satelliteTagLocations != rhs.satelliteTagLocations ||
	    startVersion != rhs.startVersion || tLogs.size() != rhs.tLogs.size() || locality != rhs.locality ||
	    logRouters.size() != rhs.logRouters.size() || backupWorkers.size() != rhs.backupWorkers.size()) {
		return false;
	}
	if ((tLogPolicy && !rhs.tLogPolicy) || (!tLogPolicy && rhs.tLogPolicy) ||
	    (tLogPolicy && (tLogPolicy->info() != rhs.tLogPolicy->info()))) {
		return false;
	}
	for (int j = 0; j < tLogs.size(); j++) {
		if (tLogs[j].id() != rhs.tLogs[j].id() || tLogs[j].present() != rhs.tLogs[j].present() ||
		    (tLogs[j].present() &&
		     tLogs[j].interf().commit.getEndpoint().token != rhs.tLogs[j].interf().commit.getEndpoint().token)) {
			return false;
		}
	}
	for (int j = 0; j < logRouters.size(); j++) {
		if (logRouters[j].id() != rhs.logRouters[j].id() || logRouters[j].present() != rhs.logRouters[j].present() ||
		    (logRouters[j].present() && logRouters[j].interf().commit.getEndpoint().token !=
		                                    rhs.logRouters[j].interf().commit.getEndpoint().token)) {
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

bool TLogSet::isEqualIds(TLogSet const& r) const {
	if (tLogWriteAntiQuorum != r.tLogWriteAntiQuorum || tLogReplicationFactor != r.tLogReplicationFactor ||
	    isLocal != r.isLocal || satelliteTagLocations != r.satelliteTagLocations || startVersion != r.startVersion ||
	    tLogs.size() != r.tLogs.size() || locality != r.locality) {
		return false;
	}
	if ((tLogPolicy && !r.tLogPolicy) || (!tLogPolicy && r.tLogPolicy) ||
	    (tLogPolicy && (tLogPolicy->info() != r.tLogPolicy->info()))) {
		return false;
	}
	for (int i = 0; i < tLogs.size(); i++) {
		if (tLogs[i].id() != r.tLogs[i].id()) {
			return false;
		}
	}
	return true;
}

bool OldTLogConf::operator==(const OldTLogConf& rhs) const {
	return tLogs == rhs.tLogs && epochBegin == rhs.epochBegin && epochEnd == rhs.epochEnd &&
	       logRouterTags == rhs.logRouterTags && txsTags == rhs.txsTags && pseudoLocalities == rhs.pseudoLocalities &&
	       epoch == rhs.epoch;
}

bool OldTLogConf::isEqualIds(OldTLogConf const& r) const {
	if (tLogs.size() != r.tLogs.size()) {
		return false;
	}
	for (int i = 0; i < tLogs.size(); i++) {
		if (!tLogs[i].isEqualIds(r.tLogs[i])) {
			return false;
		}
	}
	return true;
}

std::string LogSystemConfig::toString() const {
	return format("type: %d oldGenerations: %d tags: %d %s",
	              logSystemType,
	              oldTLogs.size(),
	              logRouterTags,
	              describe(tLogs).c_str());
}

Optional<Key> LogSystemConfig::getRemoteDcId() const {
	for (int i = 0; i < tLogs.size(); i++) {
		if (!tLogs[i].isLocal) {
			for (int j = 0; j < tLogs[i].tLogs.size(); j++) {
				if (tLogs[i].tLogs[j].present()) {
					return tLogs[i].tLogs[j].interf().filteredLocality.dcId();
				}
			}
		}
	}
	return Optional<Key>();
}

std::vector<TLogInterface> LogSystemConfig::allLocalLogs(bool includeSatellite) const {
	std::vector<TLogInterface> results;
	for (int i = 0; i < tLogs.size(); i++) {
		// skip satellite TLogs, if it was not needed
		if (!includeSatellite && tLogs[i].locality == tagLocalitySatellite) {
			continue;
		}
		if (tLogs[i].isLocal) {
			for (int j = 0; j < tLogs[i].tLogs.size(); j++) {
				if (tLogs[i].tLogs[j].present()) {
					results.push_back(tLogs[i].tLogs[j].interf());
				}
			}
		}
	}
	return results;
}

int LogSystemConfig::numLogs() const {
	int numLogs = 0;
	for (auto& tLogSet : tLogs) {
		if (tLogSet.isLocal == true) {
			numLogs += tLogSet.tLogs.size();
		}
	}
	return numLogs;
}

std::vector<TLogInterface> LogSystemConfig::allPresentLogs() const {
	std::vector<TLogInterface> results;
	for (int i = 0; i < tLogs.size(); i++) {
		for (int j = 0; j < tLogs[i].tLogs.size(); j++) {
			if (tLogs[i].tLogs[j].present()) {
				results.push_back(tLogs[i].tLogs[j].interf());
			}
		}
	}
	return results;
}

std::pair<int8_t, int8_t> LogSystemConfig::getLocalityForDcId(Optional<Key> dcId) const {
	std::map<int8_t, int> matchingLocalities;
	std::map<int8_t, int> allLocalities;
	for (auto& tLogSet : tLogs) {
		for (auto& tLog : tLogSet.tLogs) {
			if (tLogSet.locality >= 0) {
				if (tLog.present() && tLog.interf().filteredLocality.dcId() == dcId) {
					matchingLocalities[tLogSet.locality]++;
				} else {
					allLocalities[tLogSet.locality]++;
				}
			}
		}
	}

	for (auto& oldLog : oldTLogs) {
		for (auto& tLogSet : oldLog.tLogs) {
			for (auto& tLog : tLogSet.tLogs) {
				if (tLogSet.locality >= 0) {
					if (tLog.present() && tLog.interf().filteredLocality.dcId() == dcId) {
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
	for (auto& it : matchingLocalities) {
		if (it.second > bestLocalityCount) {
			bestLoc = it.first;
			bestLocalityCount = it.second;
		}
	}

	int8_t secondLoc = tagLocalityInvalid;
	int8_t thirdLoc = tagLocalityInvalid;
	int secondLocalityCount = -1;
	int thirdLocalityCount = -1;
	for (auto& it : allLocalities) {
		if (bestLoc != it.first) {
			if (it.second > secondLocalityCount) {
				thirdLoc = secondLoc;
				thirdLocalityCount = secondLocalityCount;
				secondLoc = it.first;
				secondLocalityCount = it.second;
			} else if (it.second > thirdLocalityCount) {
				thirdLoc = it.first;
				thirdLocalityCount = it.second;
			}
		}
	}

	if (bestLoc != tagLocalityInvalid) {
		return std::make_pair(bestLoc, secondLoc);
	}
	return std::make_pair(secondLoc, thirdLoc);
}

std::vector<std::pair<UID, NetworkAddress>> LogSystemConfig::allSharedLogs() const {
	typedef std::pair<UID, NetworkAddress> IdAddrPair;
	std::vector<IdAddrPair> results;
	for (auto& tLogSet : tLogs) {
		for (auto& tLog : tLogSet.tLogs) {
			if (tLog.present())
				results.push_back(IdAddrPair(tLog.interf().getSharedTLogID(), tLog.interf().address()));
		}
	}

	for (auto& oldLog : oldTLogs) {
		for (auto& tLogSet : oldLog.tLogs) {
			for (auto& tLog : tLogSet.tLogs) {
				if (tLog.present())
					results.push_back(IdAddrPair(tLog.interf().getSharedTLogID(), tLog.interf().address()));
			}
		}
	}
	uniquify(results);
	// This assert depends on the fact that uniquify will sort the elements based on <UID, NetworkAddr> order
	ASSERT_WE_THINK(std::unique(results.begin(), results.end(), [](IdAddrPair& x, IdAddrPair& y) {
		                return x.first == y.first;
	                }) == results.end());
	return results;
}

bool LogSystemConfig::isEqual(LogSystemConfig const& r) const {
	return logSystemType == r.logSystemType && tLogs == r.tLogs && oldTLogs == r.oldTLogs &&
	       expectedLogSets == r.expectedLogSets && logRouterTags == r.logRouterTags && txsTags == r.txsTags &&
	       recruitmentID == r.recruitmentID && stopped == r.stopped && recoveredAt == r.recoveredAt &&
	       pseudoLocalities == r.pseudoLocalities && epoch == r.epoch && oldestBackupEpoch == r.oldestBackupEpoch;
}

bool LogSystemConfig::isEqualIds(LogSystemConfig const& r) const {
	for (auto& i : r.tLogs) {
		for (auto& j : tLogs) {
			if (i.isEqualIds(j)) {
				return true;
			}
		}
	}
	return false;
}

bool LogSystemConfig::isNextGenerationOf(LogSystemConfig const& r) const {
	if (!oldTLogs.size()) {
		return false;
	}

	for (auto& i : r.tLogs) {
		for (auto& j : oldTLogs[0].tLogs) {
			if (i.isEqualIds(j)) {
				return true;
			}
		}
	}
	return false;
}

bool LogSystemConfig::hasTLog(UID tid) const {
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

bool LogSystemConfig::hasLogRouter(UID rid) const {
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

bool LogSystemConfig::hasBackupWorker(UID bid) const {
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

Version LogSystemConfig::getEpochEndVersion(LogEpoch epoch) const {
	for (const auto& old : oldTLogs) {
		if (old.epoch == epoch) {
			return old.epochEnd;
		}
	}
	return invalidVersion;
}
