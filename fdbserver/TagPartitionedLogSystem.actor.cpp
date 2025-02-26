/*
 * TagPartitionedLogSystem.actor.cpp
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

#include "fdbserver/TagPartitionedLogSystem.actor.h"
#include <boost/dynamic_bitset.hpp>

#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Version> minVersionWhenReady(Future<Void> f,
                                          std::vector<std::pair<UID, Future<TLogCommitReply>>> replies) {
	try {
		wait(f);
		Version minVersion = std::numeric_limits<Version>::max();
		for (const auto& [_tlogID, reply] : replies) {
			if (reply.isReady() && !reply.isError()) {
				minVersion = std::min(minVersion, reply.get().version);
			}
		}
		return minVersion;
	} catch (Error& err) {
		if (err.code() == error_code_operation_cancelled) {
			TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "TLogPushCancelled");
			int index = 0;
			for (const auto& [tlogID, reply] : replies) {
				if (reply.isReady()) {
					continue;
				}
				std::string message;
				if (reply.isError()) {
					// FIXME Use C++20 format when it is available
					message = format("TLogPushRespondError%04d", index++);
				} else {
					message = format("TLogPushNoResponse%04d", index++);
				}
				TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, message.c_str())
				    .detail("TLogID", tlogID);
			}
		}
		throw;
	}
}

LogSet::LogSet(const TLogSet& tLogSet)
  : tLogWriteAntiQuorum(tLogSet.tLogWriteAntiQuorum), tLogReplicationFactor(tLogSet.tLogReplicationFactor),
    tLogLocalities(tLogSet.tLogLocalities), tLogVersion(tLogSet.tLogVersion), tLogPolicy(tLogSet.tLogPolicy),
    isLocal(tLogSet.isLocal), locality(tLogSet.locality), startVersion(tLogSet.startVersion),
    satelliteTagLocations(tLogSet.satelliteTagLocations) {
	for (const auto& log : tLogSet.tLogs) {
		logServers.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(log));
	}
	for (const auto& log : tLogSet.logRouters) {
		logRouters.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(log));
	}
	for (const auto& log : tLogSet.backupWorkers) {
		backupWorkers.push_back(makeReference<AsyncVar<OptionalInterface<BackupInterface>>>(log));
	}
	filterLocalityDataForPolicy(tLogPolicy, &tLogLocalities);
	updateLocalitySet(tLogLocalities);
}

LogSet::LogSet(const CoreTLogSet& coreSet)
  : tLogWriteAntiQuorum(coreSet.tLogWriteAntiQuorum), tLogReplicationFactor(coreSet.tLogReplicationFactor),
    tLogLocalities(coreSet.tLogLocalities), tLogVersion(coreSet.tLogVersion), tLogPolicy(coreSet.tLogPolicy),
    isLocal(coreSet.isLocal), locality(coreSet.locality), startVersion(coreSet.startVersion),
    satelliteTagLocations(coreSet.satelliteTagLocations) {
	for (const auto& log : coreSet.tLogs) {
		logServers.push_back(
		    makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(OptionalInterface<TLogInterface>(log)));
	}
	// Do NOT recover coreSet.backupWorkers, because master will recruit new ones.
	filterLocalityDataForPolicy(tLogPolicy, &tLogLocalities);
	updateLocalitySet(tLogLocalities);
}

TLogSet::TLogSet(const LogSet& rhs)
  : tLogWriteAntiQuorum(rhs.tLogWriteAntiQuorum), tLogReplicationFactor(rhs.tLogReplicationFactor),
    tLogLocalities(rhs.tLogLocalities), tLogVersion(rhs.tLogVersion), tLogPolicy(rhs.tLogPolicy), isLocal(rhs.isLocal),
    locality(rhs.locality), startVersion(rhs.startVersion), satelliteTagLocations(rhs.satelliteTagLocations) {
	for (const auto& tlog : rhs.logServers) {
		tLogs.push_back(tlog->get());
	}
	for (const auto& logRouter : rhs.logRouters) {
		logRouters.push_back(logRouter->get());
	}
	for (const auto& worker : rhs.backupWorkers) {
		backupWorkers.push_back(worker->get());
	}
}

OldTLogConf::OldTLogConf(const OldLogData& oldLogData)
  : epochBegin(oldLogData.epochBegin), epochEnd(oldLogData.epochEnd), recoverAt(oldLogData.recoverAt),
    logRouterTags(oldLogData.logRouterTags), txsTags(oldLogData.txsTags), pseudoLocalities(oldLogData.pseudoLocalities),
    epoch(oldLogData.epoch) {
	for (const Reference<LogSet>& logSet : oldLogData.tLogs) {
		tLogs.emplace_back(*logSet);
	}
}

CoreTLogSet::CoreTLogSet(const LogSet& logset)
  : tLogWriteAntiQuorum(logset.tLogWriteAntiQuorum), tLogReplicationFactor(logset.tLogReplicationFactor),
    tLogLocalities(logset.tLogLocalities), tLogPolicy(logset.tLogPolicy), isLocal(logset.isLocal),
    locality(logset.locality), startVersion(logset.startVersion), satelliteTagLocations(logset.satelliteTagLocations),
    tLogVersion(logset.tLogVersion) {
	for (const auto& log : logset.logServers) {
		tLogs.push_back(log->get().id());
	}
	// Do NOT store logset.backupWorkers, because master will recruit new ones.
}

OldTLogCoreData::OldTLogCoreData(const OldLogData& oldData)
  : logRouterTags(oldData.logRouterTags), txsTags(oldData.txsTags), epochBegin(oldData.epochBegin),
    epochEnd(oldData.epochEnd), recoverAt(oldData.recoverAt), pseudoLocalities(oldData.pseudoLocalities),
    epoch(oldData.epoch) {
	for (const Reference<LogSet>& logSet : oldData.tLogs) {
		if (logSet->logServers.size()) {
			tLogs.emplace_back(*logSet);
		}
	}
}

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
	else
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

void TagPartitionedLogSystem::stopRejoins() {
	rejoins = Future<Void>();
}

void TagPartitionedLogSystem::addref() {
	ReferenceCounted<TagPartitionedLogSystem>::addref();
}

void TagPartitionedLogSystem::delref() {
	ReferenceCounted<TagPartitionedLogSystem>::delref();
}

std::string TagPartitionedLogSystem::describe() const {
	std::string result;
	for (int i = 0; i < tLogs.size(); i++) {
		result += format("%d: ", i);
		for (int j = 0; j < tLogs[i]->logServers.size(); j++) {
			result +=
			    tLogs[i]->logServers[j]->get().id().toString() + ((j == tLogs[i]->logServers.size() - 1) ? " " : ", ");
		}
	}
	return result;
}

UID TagPartitionedLogSystem::getDebugID() const {
	return dbgid;
}

void TagPartitionedLogSystem::addPseudoLocality(int8_t locality) {
	ASSERT(locality < 0);
	pseudoLocalities.insert(locality);
	for (uint16_t i = 0; i < logRouterTags; i++) {
		pseudoLocalityPopVersion[Tag(locality, i)] = 0;
	}
}

Tag TagPartitionedLogSystem::getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const {
	switch (type) {
	case ProcessClass::LogRouterClass:
		if (tag.locality == tagLocalityLogRouter) {
			ASSERT(pseudoLocalities.contains(tagLocalityLogRouterMapped));
			tag.locality = tagLocalityLogRouterMapped;
		}
		break;

	case ProcessClass::BackupClass:
		if (tag.locality == tagLocalityLogRouter) {
			ASSERT(pseudoLocalities.contains(tagLocalityBackup));
			tag.locality = tagLocalityBackup;
		}
		break;

	default: // This should be an error at caller site.
		break;
	}
	return tag;
}

bool TagPartitionedLogSystem::hasPseudoLocality(int8_t locality) const {
	return pseudoLocalities.contains(locality);
}

Version TagPartitionedLogSystem::popPseudoLocalityTag(Tag tag, Version upTo) {
	ASSERT(isPseudoLocality(tag.locality) && hasPseudoLocality(tag.locality));

	Version& localityVersion = pseudoLocalityPopVersion[tag];
	localityVersion = std::max(localityVersion, upTo);
	Version minVersion = localityVersion;
	// Why do we need to use the minimum popped version among all tags? Reason: for example,
	// 2 pseudo tags pop 100 or 150, respectively. It's only safe to pop min(100, 150),
	// because [101,150) is needed by another pseudo tag.
	for (const int8_t locality : pseudoLocalities) {
		minVersion = std::min(minVersion, pseudoLocalityPopVersion[Tag(locality, tag.id)]);
	}
	// TraceEvent("TLogPopPseudoTag", dbgid).detail("Tag", tag.toString()).detail("Version", upTo).detail("PopVersion", minVersion);
	return minVersion;
}

Future<Void> TagPartitionedLogSystem::recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem,
                                                         UID const& dbgid,
                                                         DBCoreState const& oldState,
                                                         FutureStream<TLogRejoinRequest> const& rejoins,
                                                         LocalityData const& locality,
                                                         bool* forceRecovery) {
	return epochEnd(outLogSystem, dbgid, oldState, rejoins, locality, forceRecovery);
}

Reference<ILogSystem> TagPartitionedLogSystem::fromLogSystemConfig(UID const& dbgid,
                                                                   LocalityData const& locality,
                                                                   LogSystemConfig const& lsConf,
                                                                   bool excludeRemote,
                                                                   bool useRecoveredAt,
                                                                   Optional<PromiseStream<Future<Void>>> addActor) {
	ASSERT(lsConf.logSystemType == LogSystemType::tagPartitioned ||
	       (lsConf.logSystemType == LogSystemType::empty && !lsConf.tLogs.size()));
	// ASSERT(lsConf.epoch == epoch);  //< FIXME
	auto logSystem = makeReference<TagPartitionedLogSystem>(dbgid, locality, lsConf.epoch, addActor);

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
		//TraceEvent("BWFromLSConf")
		//    .detail("Epoch", logSystem->oldLogData.back().epoch)
		//    .detail("Version", logSystem->oldLogData.back().epochEnd);
	}

	logSystem->logSystemType = lsConf.logSystemType;
	logSystem->oldestBackupEpoch = lsConf.oldestBackupEpoch;
	return logSystem;
}

Reference<ILogSystem> TagPartitionedLogSystem::fromOldLogSystemConfig(UID const& dbgid,
                                                                      LocalityData const& locality,
                                                                      LogSystemConfig const& lsConf) {
	ASSERT(lsConf.logSystemType == LogSystemType::tagPartitioned ||
	       (lsConf.logSystemType == LogSystemType::empty && !lsConf.tLogs.size()));
	// ASSERT(lsConf.epoch == epoch);  //< FIXME
	const LogEpoch e = lsConf.oldTLogs.size() > 0 ? lsConf.oldTLogs[0].epoch : 0;
	auto logSystem = makeReference<TagPartitionedLogSystem>(dbgid, locality, e);

	if (lsConf.oldTLogs.size()) {
		for (const TLogSet& tLogSet : lsConf.oldTLogs[0].tLogs) {
			logSystem->tLogs.push_back(makeReference<LogSet>(tLogSet));
		}
		logSystem->logRouterTags = lsConf.oldTLogs[0].logRouterTags;
		logSystem->txsTags = lsConf.oldTLogs[0].txsTags;
		// logSystem->epochEnd = lsConf.oldTLogs[0].epochEnd;

		for (int i = 1; i < lsConf.oldTLogs.size(); i++) {
			logSystem->oldLogData.emplace_back(lsConf.oldTLogs[i]);
		}
	}
	logSystem->logSystemType = lsConf.logSystemType;
	logSystem->stopped = true;
	logSystem->pseudoLocalities = lsConf.pseudoLocalities;

	return logSystem;
}

void TagPartitionedLogSystem::purgeOldRecoveredGenerations() {
	Version oldestGenerationRecoverAtVersion = std::min(recoveredVersion->get(), remoteRecoveredVersion->get());
	TraceEvent("ToCoreStateOldestGenerationRecoverAtVersion")
	    .detail("RecoveredVersion", recoveredVersion->get())
	    .detail("RemoteRecoveredVersion", remoteRecoveredVersion->get())
	    .detail("OldestBackupEpoch", oldestBackupEpoch);
	for (int i = 0; i < oldLogData.size(); ++i) {
		const auto& oldData = oldLogData[i];
		// Remove earlier generation that TLog data are
		//  - consumed by all storage servers
		//  - no longer used by backup workers
		if (oldData.recoverAt < oldestGenerationRecoverAtVersion && oldData.epoch < oldestBackupEpoch) {
			if (g_network->isSimulated()) {
				for (int j = 0; j < oldLogData.size(); ++j) {
					TraceEvent("AllOldGenerations")
					    .detail("Index", j)
					    .detail("Purge", i + 1)
					    .detail("Begin", oldLogData[j].epochBegin)
					    .detail("RecoverAt", oldLogData[j].recoverAt);
				}
				for (int j = i + 1; j < oldLogData.size(); ++j) {
					ASSERT(oldLogData[j].recoverAt < oldestGenerationRecoverAtVersion);
					ASSERT(oldData.tLogs[0]->backupWorkers.size() == 0 || oldLogData[j].epoch < oldestBackupEpoch);
				}
			}
			for (int j = i; j < oldLogData.size(); ++j) {
				TraceEvent("PurgeOldTLogGeneration")
				    .detail("Begin", oldLogData[j].epochBegin)
				    .detail("End", oldLogData[j].epochEnd)
				    .detail("Epoch", oldLogData[j].epoch)
				    .detail("RecoverAt", oldLogData[j].recoverAt)
				    .detail("Index", j);
			}
			oldLogData.resize(i);
			break;
		}
	}
}

void TagPartitionedLogSystem::toCoreState(DBCoreState& newState) const {
	if (recoveryComplete.isValid() && recoveryComplete.isError())
		throw recoveryComplete.getError();

	if (remoteRecoveryComplete.isValid() && remoteRecoveryComplete.isError())
		throw remoteRecoveryComplete.getError();

	newState.tLogs.clear();
	newState.logRouterTags = logRouterTags;
	newState.txsTags = txsTags;
	newState.pseudoLocalities = pseudoLocalities;
	for (const auto& t : tLogs) {
		if (t->logServers.size()) {
			newState.tLogs.emplace_back(*t);
			newState.tLogs.back().tLogLocalities.clear();
			for (const auto& log : t->logServers) {
				newState.tLogs.back().tLogLocalities.push_back(log->get().interf().filteredLocality);
			}
		}
	}

	newState.oldTLogData.clear();
	if (!recoveryComplete.isValid() || !recoveryComplete.isReady() ||
	    (repopulateRegionAntiQuorum == 0 && (!remoteRecoveryComplete.isValid() || !remoteRecoveryComplete.isReady())) ||
	    epoch != oldestBackupEpoch) {
		for (const auto& oldData : oldLogData) {
			newState.oldTLogData.emplace_back(oldData);
			TraceEvent("BWToCore")
			    .detail("Epoch", newState.oldTLogData.back().epoch)
			    .detail("TotalTags", newState.oldTLogData.back().logRouterTags)
			    .detail("BeginVersion", newState.oldTLogData.back().epochBegin)
			    .detail("EndVersion", newState.oldTLogData.back().epochEnd);
		}
	}

	newState.logSystemType = logSystemType;
}

bool TagPartitionedLogSystem::remoteStorageRecovered() const {
	return remoteRecoveryComplete.isValid() && remoteRecoveryComplete.isReady();
}

Future<Void> TagPartitionedLogSystem::onCoreStateChanged() const {
	std::vector<Future<Void>> changes;
	changes.push_back(Never());
	if (recoveryComplete.isValid() && !recoveryComplete.isReady()) {
		changes.push_back(recoveryComplete);
	}
	if (remoteRecovery.isValid() && !remoteRecovery.isReady()) {
		changes.push_back(remoteRecovery);
	}
	if (remoteRecoveryComplete.isValid() && !remoteRecoveryComplete.isReady()) {
		changes.push_back(remoteRecoveryComplete);
	}
	changes.push_back(backupWorkerChanged.onTrigger()); // changes to oldestBackupEpoch
	changes.push_back(recoveredVersion->onChange());
	changes.push_back(remoteRecoveredVersion->onChange());
	return waitForAny(changes);
}

void TagPartitionedLogSystem::coreStateWritten(DBCoreState const& newState) {
	if (!newState.oldTLogData.size()) {
		recoveryCompleteWrittenToCoreState.set(true);
	}
	for (auto& t : newState.tLogs) {
		if (!t.isLocal) {
			TraceEvent("RemoteLogsWritten", dbgid).log();
			remoteLogsWrittenToCoreState = true;
			break;
		}
	}
}

Future<Void> TagPartitionedLogSystem::onError() const {
	return onError_internal(this);
}

ACTOR Future<Void> TagPartitionedLogSystem::onError_internal(TagPartitionedLogSystem const* self) {
	// Never returns normally, but throws an error if the subsystem stops working
	loop {
		std::vector<Future<Void>> failed;
		std::vector<Future<Void>> backupFailed(1, Never());
		std::vector<Future<Void>> changes;

		for (auto& it : self->tLogs) {
			for (auto& t : it->logServers) {
				if (t->get().present()) {
					failed.push_back(
					    waitFailureClient(t->get().interf().waitFailure,
					                      SERVER_KNOBS->TLOG_TIMEOUT,
					                      -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
					                      /*trace=*/true));
				} else {
					changes.push_back(t->onChange());
				}
			}
			for (auto& t : it->logRouters) {
				if (t->get().present()) {
					failed.push_back(
					    waitFailureClient(t->get().interf().waitFailure,
					                      SERVER_KNOBS->TLOG_TIMEOUT,
					                      -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
					                      /*trace=*/true));
				} else {
					changes.push_back(t->onChange());
				}
			}
			for (const auto& worker : it->backupWorkers) {
				if (worker->get().present()) {
					backupFailed.push_back(
					    waitFailureClient(worker->get().interf().waitFailure,
					                      SERVER_KNOBS->BACKUP_TIMEOUT,
					                      -SERVER_KNOBS->BACKUP_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
					                      /*trace=*/true));
				} else {
					changes.push_back(worker->onChange());
				}
			}
		}

		if (!self->recoveryCompleteWrittenToCoreState.get()) {
			for (auto& old : self->oldLogData) {
				for (auto& it : old.tLogs) {
					for (auto& t : it->logRouters) {
						if (t->get().present()) {
							failed.push_back(waitFailureClient(t->get().interf().waitFailure,
							                                   SERVER_KNOBS->TLOG_TIMEOUT,
							                                   -SERVER_KNOBS->TLOG_TIMEOUT /
							                                       SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
							                                   /*trace=*/true));
						} else {
							changes.push_back(t->onChange());
						}
					}
				}
				// Monitor changes of backup workers for old epochs.
				for (const auto& worker : old.tLogs[0]->backupWorkers) {
					if (worker->get().present()) {
						backupFailed.push_back(waitFailureClient(worker->get().interf().waitFailure,
						                                         SERVER_KNOBS->BACKUP_TIMEOUT,
						                                         -SERVER_KNOBS->BACKUP_TIMEOUT /
						                                             SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
						                                         /*trace=*/true));
					} else {
						changes.push_back(worker->onChange());
					}
				}
			}
		}

		if (self->hasRemoteServers && (!self->remoteRecovery.isReady() || self->remoteRecovery.isError())) {
			changes.push_back(self->remoteRecovery);
		}

		changes.push_back(self->recoveryCompleteWrittenToCoreState.onChange());
		changes.push_back(self->backupWorkerChanged.onTrigger());

		ASSERT(failed.size() >= 1);
		wait(quorum(changes, 1) || tagError<Void>(quorum(failed, 1), tlog_failed()) ||
		     tagError<Void>(quorum(backupFailed, 1), backup_worker_failed()));
	}
}

ACTOR Future<Void> TagPartitionedLogSystem::pushResetChecker(Reference<ConnectionResetInfo> self, NetworkAddress addr) {
	self->slowReplies = 0;
	self->fastReplies = 0;
	wait(delay(SERVER_KNOBS->PUSH_STATS_INTERVAL));
	TraceEvent("SlowPushStats")
	    .detail("PeerAddress", addr)
	    .detail("SlowReplies", self->slowReplies)
	    .detail("FastReplies", self->fastReplies);
	if (self->slowReplies >= SERVER_KNOBS->PUSH_STATS_SLOW_AMOUNT &&
	    self->slowReplies / double(self->slowReplies + self->fastReplies) >= SERVER_KNOBS->PUSH_STATS_SLOW_RATIO) {
		FlowTransport::transport().resetConnection(addr);
		self->lastReset = now();
	}
	return Void();
}

ACTOR Future<TLogCommitReply> TagPartitionedLogSystem::recordPushMetrics(Reference<ConnectionResetInfo> self,
                                                                         Reference<Histogram> dist,
                                                                         NetworkAddress addr,
                                                                         Future<TLogCommitReply> in) {
	state double startTime = now();
	TLogCommitReply t = wait(in);
	if (now() - self->lastReset > SERVER_KNOBS->PUSH_RESET_INTERVAL) {
		if (now() - startTime > SERVER_KNOBS->PUSH_MAX_LATENCY) {
			if (self->resetCheck.isReady()) {
				self->resetCheck = TagPartitionedLogSystem::pushResetChecker(self, addr);
			}
			self->slowReplies++;
		} else {
			self->fastReplies++;
		}
	}
	dist->sampleSeconds(now() - startTime);
	return t;
}

Future<Version> TagPartitionedLogSystem::push(const ILogSystem::PushVersionSet& versionSet,
                                              LogPushData& data,
                                              SpanContext const& spanContext,
                                              Optional<UID> debugID,
                                              Optional<std::unordered_map<uint16_t, Version>> tpcvMap) {
	// FIXME: Randomize request order as in LegacyLogSystem?
	Version prevVersion = versionSet.prevVersion; // this might be updated when version vector unicast is enabled
	Version seqPrevVersion = versionSet.prevVersion; // a copy of the prevVersion provided by the sequencer

	std::unordered_map<uint8_t, uint16_t> tLogCount;
	std::unordered_map<uint8_t, std::vector<uint16_t>> tLogLocIds;
	if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		uint16_t location = 0;
		uint8_t logGroupLocal = 0;
		const auto& tpcvMapRef = tpcvMap.get();
		for (const auto& it : tLogs) {
			if (!it->isLocal) {
				continue;
			}
			for (size_t loc = 0; loc < it->logServers.size(); loc++) {
				if (tpcvMapRef.contains(location)) {
					tLogCount[logGroupLocal]++;
					tLogLocIds[logGroupLocal].push_back(location);
				}
				location++;
			}
			logGroupLocal++;
		}
	}

	uint16_t location = 0;
	uint8_t logGroupLocal = 0;
	std::vector<Future<Void>> quorumResults;
	std::vector<std::pair<UID, Future<TLogCommitReply>>> allReplies;
	const Span span("TPLS:push"_loc, spanContext);
	for (auto& it : tLogs) {
		if (!it->isLocal) {
			// Remote TLogs should read from LogRouter
			continue;
		}
		if (it->logServers.size() == 0) {
			// Empty TLog set
			continue;
		}

		if (it->connectionResetTrackers.size() == 0) {
			for (int i = 0; i < it->logServers.size(); i++) {
				it->connectionResetTrackers.push_back(makeReference<ConnectionResetInfo>());
			}
		}
		if (it->tlogPushDistTrackers.empty()) {
			for (int i = 0; i < it->logServers.size(); i++) {
				it->tlogPushDistTrackers.push_back(
				    Histogram::getHistogram("ToTlog_" + it->logServers[i]->get().interf().uniqueID.toString(),
				                            it->logServers[i]->get().interf().address().toString(),
				                            Histogram::Unit::milliseconds));
			}
		}

		std::vector<Future<Void>> tLogCommitResults;
		for (size_t loc = 0; loc < it->logServers.size(); loc++) {
			Standalone<StringRef> msg = data.getMessages(location);
			data.recordEmptyMessage(location, msg);
			if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
				if (tpcvMap.get().contains(location)) {
					prevVersion = tpcvMap.get()[location];
				} else {
					location++;
					continue;
				}
			}

			const auto& interface = it->logServers[loc]->get().interf();
			const auto request = TLogCommitRequest(spanContext,
			                                       msg.arena(),
			                                       prevVersion,
			                                       versionSet.version,
			                                       versionSet.knownCommittedVersion,
			                                       versionSet.minKnownCommittedVersion,
			                                       seqPrevVersion,
			                                       msg,
			                                       tLogCount[logGroupLocal],
			                                       tLogLocIds[logGroupLocal],
			                                       debugID);
			auto tLogReply = recordPushMetrics(it->connectionResetTrackers[loc],
			                                   it->tlogPushDistTrackers[loc],
			                                   interface.address(),
			                                   interface.commit.getReply(request, TaskPriority::ProxyTLogCommitReply));

			allReplies.emplace_back(interface.id(), tLogReply);
			Future<Void> commitSuccess = success(tLogReply);
			addActor.get().send(commitSuccess);
			tLogCommitResults.push_back(commitSuccess);
			location++;
		}
		quorumResults.push_back(quorum(tLogCommitResults, tLogCommitResults.size() - it->tLogWriteAntiQuorum));
		logGroupLocal++;
	}

	return minVersionWhenReady(waitForAll(quorumResults), allReplies);
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekAll(UID dbgid,
                                                                    Version begin,
                                                                    Version end,
                                                                    Tag tag,
                                                                    bool parallelGetMore) {
	int bestSet = 0;
	std::vector<Reference<LogSet>> localSets;
	Version lastBegin = 0;
	bool foundSpecial = false;
	for (auto& log : tLogs) {
		if (log->locality == tagLocalitySpecial) {
			foundSpecial = true;
		}
		if (log->isLocal && log->logServers.size() &&
		    (log->locality == tagLocalitySpecial || log->locality == tag.locality || tag.locality == tagLocalityTxs ||
		     tag.locality == tagLocalityLogRouter || (tag == cacheTag && log->locality != tagLocalitySatellite))) {
			lastBegin = std::max(lastBegin, log->startVersion);
			localSets.push_back(log);
			if (log->locality != tagLocalitySatellite) {
				bestSet = localSets.size() - 1;
			}
		}
	}

	if (!localSets.size()) {
		lastBegin = end;
	}

	if (begin >= lastBegin && localSets.size()) {
		TraceEvent("TLogPeekAllCurrentOnly", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end)
		    .detail("BestLogs", localSets[bestSet]->logServerString());
		return makeReference<ILogSystem::SetPeekCursor>(
		    localSets, bestSet, localSets[bestSet]->bestLocationFor(tag), tag, begin, end, parallelGetMore);
	} else {
		std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		if (lastBegin < end && localSets.size()) {
			TraceEvent("TLogPeekAllAddingCurrent", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("End", end)
			    .detail("BestLogs", localSets[bestSet]->logServerString());
			cursors.push_back(makeReference<ILogSystem::SetPeekCursor>(
			    localSets, bestSet, localSets[bestSet]->bestLocationFor(tag), tag, lastBegin, end, parallelGetMore));
		}
		for (int i = 0; begin < lastBegin; i++) {
			if (i == oldLogData.size()) {
				if (tag.locality == tagLocalityTxs || tag == cacheTag) {
					break;
				}
				TraceEvent("TLogPeekAllDead", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", oldLogData.size());
				return makeReference<ILogSystem::ServerPeekCursor>(
				    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false);
			}

			int bestOldSet = 0;
			std::vector<Reference<LogSet>> localOldSets;
			Version thisBegin = begin;
			bool thisSpecial = false;
			for (auto& log : oldLogData[i].tLogs) {
				if (log->locality == tagLocalitySpecial) {
					thisSpecial = true;
				}
				if (log->isLocal && log->logServers.size() &&
				    (log->locality == tagLocalitySpecial || log->locality == tag.locality ||
				     tag.locality == tagLocalityTxs || tag.locality == tagLocalityLogRouter ||
				     (tag == cacheTag && log->locality != tagLocalitySatellite))) {
					thisBegin = std::max(thisBegin, log->startVersion);
					localOldSets.push_back(log);
					if (log->locality != tagLocalitySatellite) {
						bestOldSet = localOldSets.size() - 1;
					}
				}
			}

			if (!localOldSets.size()) {
				TraceEvent("TLogPeekAllNoLocalSets", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin);
				if (!cursors.size() && !foundSpecial) {
					continue;
				}
				return makeReference<ILogSystem::ServerPeekCursor>(
				    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false);
			}
			if (thisSpecial) {
				foundSpecial = true;
			}

			if (thisBegin < lastBegin) {
				if (thisBegin < end) {
					TraceEvent("TLogPeekAllAddingOld", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("End", end)
					    .detail("BestLogs", localOldSets[bestOldSet]->logServerString())
					    .detail("LastBegin", lastBegin)
					    .detail("ThisBegin", thisBegin);
					cursors.push_back(
					    makeReference<ILogSystem::SetPeekCursor>(localOldSets,
					                                             bestOldSet,
					                                             localOldSets[bestOldSet]->bestLocationFor(tag),
					                                             tag,
					                                             thisBegin,
					                                             std::min(lastBegin, end),
					                                             parallelGetMore));
					epochEnds.push_back(LogMessageVersion(std::min(lastBegin, end)));
				}
				lastBegin = thisBegin;
			}
		}

		return makeReference<ILogSystem::MultiCursor>(cursors, epochEnds);
	}
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekRemote(UID dbgid,
                                                                       Version begin,
                                                                       Optional<Version> end,
                                                                       Tag tag,
                                                                       bool parallelGetMore) {
	int bestSet = -1;
	Version lastBegin = recoveredAt.present() ? recoveredAt.get() + 1 : 0;
	for (int t = 0; t < tLogs.size(); t++) {
		if (tLogs[t]->isLocal) {
			lastBegin = std::max(lastBegin, tLogs[t]->startVersion);
		}

		if (tLogs[t]->logRouters.size()) {
			ASSERT(bestSet == -1);
			bestSet = t;
		}
	}
	if (bestSet == -1) {
		TraceEvent("TLogPeekRemoteNoBestSet", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end.present() ? end.get() : getPeekEnd());
		return makeReference<ILogSystem::ServerPeekCursor>(
		    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, parallelGetMore);
	}
	if (begin >= lastBegin) {
		TraceEvent("TLogPeekRemoteBestOnly", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end.present() ? end.get() : getPeekEnd())
		    .detail("BestSet", bestSet)
		    .detail("BestSetStart", lastBegin)
		    .detail("LogRouterIds", tLogs[bestSet]->logRouterString());
		return makeReference<ILogSystem::BufferedCursor>(
		    tLogs[bestSet]->logRouters, tag, begin, end.present() ? end.get() + 1 : getPeekEnd(), parallelGetMore);
	} else {
		std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;
		TraceEvent("TLogPeekRemoteAddingBest", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end.present() ? end.get() : getPeekEnd())
		    .detail("BestSet", bestSet)
		    .detail("BestSetStart", lastBegin)
		    .detail("LogRouterIds", tLogs[bestSet]->logRouterString());
		cursors.push_back(makeReference<ILogSystem::BufferedCursor>(
		    tLogs[bestSet]->logRouters, tag, lastBegin, end.present() ? end.get() + 1 : getPeekEnd(), parallelGetMore));
		int i = 0;
		while (begin < lastBegin) {
			if (i == oldLogData.size()) {
				TraceEvent("TLogPeekRemoteDead", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end.present() ? end.get() : getPeekEnd())
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", oldLogData.size());
				return makeReference<ILogSystem::ServerPeekCursor>(
				    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
				    tag,
				    begin,
				    getPeekEnd(),
				    false,
				    parallelGetMore);
			}

			int bestOldSet = -1;
			Version thisBegin = begin;
			for (int t = 0; t < oldLogData[i].tLogs.size(); t++) {
				if (oldLogData[i].tLogs[t]->isLocal) {
					thisBegin = std::max(thisBegin, oldLogData[i].tLogs[t]->startVersion);
				}

				if (oldLogData[i].tLogs[t]->logRouters.size()) {
					ASSERT(bestOldSet == -1);
					bestOldSet = t;
				}
			}
			if (bestOldSet == -1) {
				TraceEvent("TLogPeekRemoteNoOldBestSet", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end.present() ? end.get() : getPeekEnd());
				return makeReference<ILogSystem::ServerPeekCursor>(
				    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
				    tag,
				    begin,
				    getPeekEnd(),
				    false,
				    parallelGetMore);
			}

			if (thisBegin < lastBegin) {
				TraceEvent("TLogPeekRemoteAddingOldBest", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end.present() ? end.get() : getPeekEnd())
				    .detail("BestOldSet", bestOldSet)
				    .detail("LogRouterIds", oldLogData[i].tLogs[bestOldSet]->logRouterString())
				    .detail("LastBegin", lastBegin)
				    .detail("ThisBegin", thisBegin)
				    .detail("BestStartVer", oldLogData[i].tLogs[bestOldSet]->startVersion);
				cursors.push_back(makeReference<ILogSystem::BufferedCursor>(
				    oldLogData[i].tLogs[bestOldSet]->logRouters, tag, thisBegin, lastBegin, parallelGetMore));
				epochEnds.emplace_back(lastBegin);
				lastBegin = thisBegin;
			}
			i++;
		}

		return makeReference<ILogSystem::MultiCursor>(cursors, epochEnds);
	}
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peek(UID dbgid,
                                                                 Version begin,
                                                                 Optional<Version> end,
                                                                 Tag tag,
                                                                 bool parallelGetMore) {
	if (!tLogs.size()) {
		TraceEvent("TLogPeekNoLogSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
		return makeReference<ILogSystem::ServerPeekCursor>(
		    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false);
	}

	if (tag.locality == tagLocalityRemoteLog) {
		return peekRemote(dbgid, begin, end, tag, parallelGetMore);
	} else {
		return peekAll(dbgid, begin, end.present() ? end.get() + 1 : getPeekEnd(), tag, parallelGetMore);
	}
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peek(UID dbgid,
                                                                 Version begin,
                                                                 Optional<Version> end,
                                                                 std::vector<Tag> tags,
                                                                 bool parallelGetMore) {
	if (tags.empty()) {
		TraceEvent("TLogPeekNoTags", dbgid).detail("Begin", begin);
		return makeReference<ILogSystem::ServerPeekCursor>(
		    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), invalidTag, begin, getPeekEnd(), false, false);
	}

	if (tags.size() == 1) {
		return peek(dbgid, begin, end, tags[0], parallelGetMore);
	}

	std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
	cursors.reserve(tags.size());
	for (auto tag : tags) {
		cursors.push_back(peek(dbgid, begin, end, tag, parallelGetMore));
	}
	return makeReference<ILogSystem::BufferedCursor>(
	    cursors, begin, end.present() ? end.get() + 1 : getPeekEnd(), true, false);
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekLocal(UID dbgid,
                                                                      Tag tag,
                                                                      Version begin,
                                                                      Version end,
                                                                      bool useMergePeekCursors,
                                                                      int8_t peekLocality) {
	if (tag.locality >= 0 || tag.locality == tagLocalitySpecial) {
		peekLocality = tag.locality;
	}
	ASSERT(peekLocality >= 0 || tag.locality == tagLocalitySpecial);

	int bestSet = -1;
	bool foundSpecial = false;
	int logCount = 0;
	for (int t = 0; t < tLogs.size(); t++) {
		if (tLogs[t]->logServers.size() && tLogs[t]->locality != tagLocalitySatellite) {
			logCount++;
		}
		if (tLogs[t]->logServers.size() && (tLogs[t]->locality == tagLocalitySpecial ||
		                                    tLogs[t]->locality == peekLocality || peekLocality == tagLocalitySpecial)) {
			if (tLogs[t]->locality == tagLocalitySpecial) {
				foundSpecial = true;
			}
			bestSet = t;
			break;
		}
	}
	if (bestSet == -1) {
		TraceEvent("TLogPeekLocalNoBestSet", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end)
		    .detail("LogCount", logCount);
		if (useMergePeekCursors || logCount > 1) {
			throw worker_removed();
		} else {
			return makeReference<ILogSystem::ServerPeekCursor>(
			    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false);
		}
	}

	if (begin >= tLogs[bestSet]->startVersion) {
		TraceEvent("TLogPeekLocalBestOnly", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end)
		    .detail("BestSet", bestSet)
		    .detail("BestSetStart", tLogs[bestSet]->startVersion)
		    .detail("LogId", tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor(tag)]->get().id());
		if (useMergePeekCursors) {
			std::vector<int> pushLocations;
			tLogs[bestSet]->getPushLocations(VectorRef<Tag>(&tag, 1), pushLocations, 0);
			return makeReference<ILogSystem::MergedPeekCursor>(tLogs[bestSet]->logServers,
			                                                   std::move(pushLocations),
			                                                   tLogs[bestSet]->bestLocationFor(tag),
			                                                   tLogs[bestSet]->logServers.size() + 1 -
			                                                       tLogs[bestSet]->tLogReplicationFactor,
			                                                   tag,
			                                                   begin,
			                                                   end,
			                                                   true,
			                                                   tLogs[bestSet]->tLogLocalities,
			                                                   tLogs[bestSet]->tLogPolicy,
			                                                   tLogs[bestSet]->tLogReplicationFactor);
		} else {
			return makeReference<ILogSystem::ServerPeekCursor>(
			    tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor(tag)], tag, begin, end, false, false);
		}
	} else {
		std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		if (tLogs[bestSet]->startVersion < end) {
			TraceEvent("TLogPeekLocalAddingBest", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("End", end)
			    .detail("BestSet", bestSet)
			    .detail("BestSetStart", tLogs[bestSet]->startVersion)
			    .detail("LogId", tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor(tag)]->get().id());
			if (useMergePeekCursors) {
				std::vector<int> pushLocations;
				tLogs[bestSet]->getPushLocations(VectorRef<Tag>(&tag, 1), pushLocations, 0);
				cursors.push_back(makeReference<ILogSystem::MergedPeekCursor>(tLogs[bestSet]->logServers,
				                                                              std::move(pushLocations),
				                                                              tLogs[bestSet]->bestLocationFor(tag),
				                                                              tLogs[bestSet]->logServers.size() + 1 -
				                                                                  tLogs[bestSet]->tLogReplicationFactor,
				                                                              tag,
				                                                              tLogs[bestSet]->startVersion,
				                                                              end,
				                                                              true,
				                                                              tLogs[bestSet]->tLogLocalities,
				                                                              tLogs[bestSet]->tLogPolicy,
				                                                              tLogs[bestSet]->tLogReplicationFactor));
			} else {
				cursors.push_back(makeReference<ILogSystem::ServerPeekCursor>(
				    tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor(tag)],
				    tag,
				    tLogs[bestSet]->startVersion,
				    end,
				    false,
				    false));
			}
		}
		Version lastBegin = tLogs[bestSet]->startVersion;
		for (int i = 0; begin < lastBegin; i++) {
			if (i == oldLogData.size()) {
				if (tag.locality == tagLocalityTxs && cursors.size()) {
					break;
				}
				TraceEvent("TLogPeekLocalDead", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", oldLogData.size());
				throw worker_removed();
			}

			int bestOldSet = -1;
			logCount = 0;
			bool nextFoundSpecial = false;
			for (int t = 0; t < oldLogData[i].tLogs.size(); t++) {
				if (oldLogData[i].tLogs[t]->logServers.size() &&
				    oldLogData[i].tLogs[t]->locality != tagLocalitySatellite) {
					logCount++;
				}
				if (oldLogData[i].tLogs[t]->logServers.size() &&
				    (oldLogData[i].tLogs[t]->locality == tagLocalitySpecial ||
				     oldLogData[i].tLogs[t]->locality == peekLocality || peekLocality == tagLocalitySpecial)) {
					if (oldLogData[i].tLogs[t]->locality == tagLocalitySpecial) {
						nextFoundSpecial = true;
					}
					if (foundSpecial && !oldLogData[i].tLogs[t]->isLocal) {
						TraceEvent("TLogPeekLocalRemoteBeforeSpecial", dbgid)
						    .detail("Tag", tag.toString())
						    .detail("Begin", begin)
						    .detail("End", end)
						    .detail("LastBegin", lastBegin)
						    .detail("OldLogDataSize", oldLogData.size())
						    .detail("Idx", i);
						throw worker_removed();
					}
					bestOldSet = t;
					break;
				}
			}

			if (bestOldSet == -1) {
				TraceEvent("TLogPeekLocalNoBestSet", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", oldLogData.size())
				    .detail("Idx", i)
				    .detail("LogRouterTags", oldLogData[i].logRouterTags)
				    .detail("LogCount", logCount)
				    .detail("FoundSpecial", foundSpecial);
				if (oldLogData[i].logRouterTags == 0 || logCount > 1 || foundSpecial) {
					throw worker_removed();
				}
				continue;
			}

			foundSpecial = nextFoundSpecial;

			Version thisBegin = std::max(oldLogData[i].tLogs[bestOldSet]->startVersion, begin);
			if (thisBegin < lastBegin) {
				if (thisBegin < end) {
					TraceEvent("TLogPeekLocalAddingOldBest", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("End", end)
					    .detail("BestOldSet", bestOldSet)
					    .detail("LogServers", oldLogData[i].tLogs[bestOldSet]->logServerString())
					    .detail("ThisBegin", thisBegin)
					    .detail("LastBegin", lastBegin);
					// detail("LogId",
					// oldLogData[i].tLogs[bestOldSet]->logServers[tLogs[bestOldSet]->bestLocationFor( tag
					// )]->get().id());
					std::vector<int> pushLocations;
					oldLogData[i].tLogs[bestOldSet]->getPushLocations(VectorRef<Tag>(&tag, 1), pushLocations, 0);
					cursors.push_back(makeReference<ILogSystem::MergedPeekCursor>(
					    oldLogData[i].tLogs[bestOldSet]->logServers,
					    std::move(pushLocations),
					    oldLogData[i].tLogs[bestOldSet]->bestLocationFor(tag),
					    oldLogData[i].tLogs[bestOldSet]->logServers.size() + 1 -
					        oldLogData[i].tLogs[bestOldSet]->tLogReplicationFactor,
					    tag,
					    thisBegin,
					    std::min(lastBegin, end),
					    useMergePeekCursors,
					    oldLogData[i].tLogs[bestOldSet]->tLogLocalities,
					    oldLogData[i].tLogs[bestOldSet]->tLogPolicy,
					    oldLogData[i].tLogs[bestOldSet]->tLogReplicationFactor));
					epochEnds.emplace_back(std::min(lastBegin, end));
				}
				lastBegin = thisBegin;
			}
		}

		return makeReference<ILogSystem::MultiCursor>(cursors, epochEnds);
	}
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekTxs(UID dbgid,
                                                                    Version begin,
                                                                    int8_t peekLocality,
                                                                    Version localEnd,
                                                                    bool canDiscardPopped) {
	Version end = getEnd();
	if (!tLogs.size()) {
		TraceEvent("TLogPeekTxsNoLogs", dbgid).log();
		return makeReference<ILogSystem::ServerPeekCursor>(
		    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), invalidTag, begin, end, false, false);
	}
	TraceEvent("TLogPeekTxs", dbgid)
	    .detail("Begin", begin)
	    .detail("End", end)
	    .detail("LocalEnd", localEnd)
	    .detail("PeekLocality", peekLocality)
	    .detail("CanDiscardPopped", canDiscardPopped);

	int maxTxsTags = txsTags;
	for (auto& it : oldLogData) {
		maxTxsTags = std::max<int>(maxTxsTags, it.txsTags);
	}

	if (peekLocality < 0 || localEnd == invalidVersion || localEnd <= begin) {
		std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
		cursors.reserve(maxTxsTags);
		for (int i = 0; i < maxTxsTags; i++) {
			cursors.push_back(peekAll(dbgid, begin, end, Tag(tagLocalityTxs, i), true));
		}

		return makeReference<ILogSystem::BufferedCursor>(cursors, begin, end, false, canDiscardPopped);
	}

	try {
		if (localEnd >= end) {
			std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
			cursors.reserve(maxTxsTags);
			for (int i = 0; i < maxTxsTags; i++) {
				cursors.push_back(peekLocal(dbgid, Tag(tagLocalityTxs, i), begin, end, true, peekLocality));
			}

			return makeReference<ILogSystem::BufferedCursor>(cursors, begin, end, false, canDiscardPopped);
		}

		std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		cursors.resize(2);

		std::vector<Reference<ILogSystem::IPeekCursor>> localCursors;
		std::vector<Reference<ILogSystem::IPeekCursor>> allCursors;
		for (int i = 0; i < maxTxsTags; i++) {
			localCursors.push_back(peekLocal(dbgid, Tag(tagLocalityTxs, i), begin, localEnd, true, peekLocality));
			allCursors.push_back(peekAll(dbgid, localEnd, end, Tag(tagLocalityTxs, i), true));
		}

		cursors[1] = makeReference<ILogSystem::BufferedCursor>(localCursors, begin, localEnd, false, canDiscardPopped);
		cursors[0] = makeReference<ILogSystem::BufferedCursor>(allCursors, localEnd, end, false, false);
		epochEnds.emplace_back(localEnd);

		return makeReference<ILogSystem::MultiCursor>(cursors, epochEnds);
	} catch (Error& e) {
		if (e.code() == error_code_worker_removed) {
			std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
			cursors.reserve(maxTxsTags);
			for (int i = 0; i < maxTxsTags; i++) {
				cursors.push_back(peekAll(dbgid, begin, end, Tag(tagLocalityTxs, i), true));
			}

			return makeReference<ILogSystem::BufferedCursor>(cursors, begin, end, false, canDiscardPopped);
		}
		throw;
	}
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekSingle(UID dbgid,
                                                                       Version begin,
                                                                       Tag tag,
                                                                       std::vector<std::pair<Version, Tag>> history) {
	while (history.size() && begin >= history.back().first) {
		history.pop_back();
	}

	if (history.size() == 0) {
		TraceEvent("TLogPeekSingleNoHistory", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
		return peekLocal(dbgid, tag, begin, getPeekEnd(), false);
	} else {
		std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		TraceEvent("TLogPeekSingleAddingLocal", dbgid).detail("Tag", tag.toString()).detail("Begin", history[0].first);
		cursors.push_back(peekLocal(dbgid, tag, history[0].first, getPeekEnd(), false));

		for (int i = 0; i < history.size(); i++) {
			TraceEvent("TLogPeekSingleAddingOld", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("HistoryTag", history[i].second.toString())
			    .detail("Begin", i + 1 == history.size() ? begin : std::max(history[i + 1].first, begin))
			    .detail("End", history[i].first);
			cursors.push_back(peekLocal(dbgid,
			                            history[i].second,
			                            i + 1 == history.size() ? begin : std::max(history[i + 1].first, begin),
			                            history[i].first,
			                            false));
			epochEnds.emplace_back(history[i].first);
		}

		return makeReference<ILogSystem::MultiCursor>(cursors, epochEnds);
	}
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekLogRouter(UID dbgid,
                                                                          Version begin,
                                                                          Tag tag,
                                                                          bool useSatellite,
                                                                          Optional<Version> end) {
	bool found = false;
	if (!end.present()) {
		end = std::numeric_limits<Version>::max();
	} else {
		end = end.get() + 1; // The last version is exclusive to the cursor's desired range
	}

	for (const auto& log : tLogs) {
		found = log->hasLogRouter(dbgid) || log->hasBackupWorker(dbgid);
		if (found) {
			break;
		}
	}
	if (found) {
		if (stopped) {
			std::vector<Reference<LogSet>> localSets;
			int bestPrimarySet = 0;
			int bestSatelliteSet = -1;
			for (auto& log : tLogs) {
				if (log->isLocal && log->logServers.size()) {
					TraceEvent("TLogPeekLogRouterLocalSet", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("LogServers", log->logServerString());
					localSets.push_back(log);
					if (log->locality == tagLocalitySatellite) {
						bestSatelliteSet = localSets.size() - 1;
					} else {
						bestPrimarySet = localSets.size() - 1;
					}
				}
			}
			int bestSet = bestPrimarySet;
			if (useSatellite && bestSatelliteSet != -1 && tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
				bestSet = bestSatelliteSet;
			}

			TraceEvent("TLogPeekLogRouterSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
			// FIXME: do this merge on one of the logs in the other data center to avoid sending multiple copies
			// across the WAN
			return makeReference<ILogSystem::SetPeekCursor>(
			    localSets, bestSet, localSets[bestSet]->bestLocationFor(tag), tag, begin, end.get(), true);
		} else {
			int bestPrimarySet = -1;
			int bestSatelliteSet = -1;
			for (int i = 0; i < tLogs.size(); i++) {
				const auto& log = tLogs[i];
				if (log->logServers.size() && log->isLocal) {
					if (log->locality == tagLocalitySatellite) {
						bestSatelliteSet = i;
						break;
					} else {
						if (bestPrimarySet == -1)
							bestPrimarySet = i;
					}
				}
			}
			int bestSet = bestPrimarySet;
			if (useSatellite && bestSatelliteSet != -1 && tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
				bestSet = bestSatelliteSet;
			}
			const auto& log = tLogs[bestSet];
			TraceEvent("TLogPeekLogRouterBestOnly", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("LogId", log->logServers[log->bestLocationFor(tag)]->get().id());
			return makeReference<ILogSystem::ServerPeekCursor>(
			    log->logServers[log->bestLocationFor(tag)], tag, begin, end.get(), false, true);
		}
	}
	bool firstOld = true;
	for (const auto& old : oldLogData) {
		found = false;
		for (const auto& log : old.tLogs) {
			found = log->hasLogRouter(dbgid) || log->hasBackupWorker(dbgid);
			if (found) {
				break;
			}
		}
		if (found) {
			int bestPrimarySet = 0;
			int bestSatelliteSet = -1;
			std::vector<Reference<LogSet>> localSets;
			for (auto& log : old.tLogs) {
				if (log->isLocal && log->logServers.size()) {
					TraceEvent("TLogPeekLogRouterOldLocalSet", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("LogServers", log->logServerString());
					localSets.push_back(log);
					if (log->locality == tagLocalitySatellite) {
						bestSatelliteSet = localSets.size() - 1;
					} else {
						bestPrimarySet = localSets.size() - 1;
					}
				}
			}
			int bestSet = bestPrimarySet;
			if (useSatellite && bestSatelliteSet != -1 && old.tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
				bestSet = bestSatelliteSet;
			}

			TraceEvent("TLogPeekLogRouterOldSets", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("OldEpoch", old.epochEnd)
			    .detail("RecoveredAt", recoveredAt.present() ? recoveredAt.get() : -1)
			    .detail("FirstOld", firstOld);
			// FIXME: do this merge on one of the logs in the other data center to avoid sending multiple copies
			// across the WAN
			return makeReference<ILogSystem::SetPeekCursor>(localSets,
			                                                bestSet,
			                                                localSets[bestSet]->bestLocationFor(tag),
			                                                tag,
			                                                begin,
			                                                firstOld && recoveredAt.present() ? recoveredAt.get() + 1
			                                                                                  : old.epochEnd,
			                                                true);
		}
		firstOld = false;
	}
	return makeReference<ILogSystem::ServerPeekCursor>(
	    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, end.get(), false, false);
}

Version TagPartitionedLogSystem::getKnownCommittedVersion() {
	Version result = invalidVersion;
	for (auto& it : lockResults) {
		auto versions = TagPartitionedLogSystem::getDurableVersion(dbgid, it);
		if (versions.present()) {
			result = std::max(result, std::get<0>(versions.get()));
		}
	}
	return result;
}

Future<Void> TagPartitionedLogSystem::onKnownCommittedVersionChange() {
	std::vector<Future<Void>> result;
	for (auto& it : lockResults) {
		result.push_back(TagPartitionedLogSystem::getDurableVersionChanged(it));
	}
	if (!result.size()) {
		return Never();
	}
	return waitForAny(result);
}

void TagPartitionedLogSystem::popLogRouter(Version upTo,
                                           Tag tag,
                                           Version durableKnownCommittedVersion,
                                           int8_t popLocality) {
	if (!upTo)
		return;

	Version lastGenerationStartVersion = TagPartitionedLogSystem::getMaxLocalStartVersion(tLogs);
	if (upTo >= lastGenerationStartVersion) {
		for (auto& t : tLogs) {
			if (t->locality == popLocality) {
				for (auto& log : t->logRouters) {
					Version prev = outstandingPops[std::make_pair(log->get().id(), tag)].first;
					if (prev < upTo) {
						outstandingPops[std::make_pair(log->get().id(), tag)] =
						    std::make_pair(upTo, durableKnownCommittedVersion);
					}
					if (prev == 0) {
						popActors.add(popFromLog(this,
						                         log,
						                         tag,
						                         /*delayBeforePop=*/0.0,
						                         /*popLogRouter=*/true)); // Fast pop time because log routers can only
						                                                  // hold 5 seconds of data.
					}
				}
			}
		}
	}

	Version nextGenerationStartVersion = lastGenerationStartVersion;
	for (const auto& old : oldLogData) {
		Version generationStartVersion = TagPartitionedLogSystem::getMaxLocalStartVersion(old.tLogs);
		if (generationStartVersion <= upTo) {
			for (auto& t : old.tLogs) {
				if (t->locality == popLocality) {
					for (auto& log : t->logRouters) {
						auto logRouterIdTagPair = std::make_pair(log->get().id(), tag);

						// We pop the log router only if the popped version is within this generation's version range.
						// That is between the current generation's start version and the next generation's start
						// version.
						if (logRouterLastPops.find(logRouterIdTagPair) == logRouterLastPops.end() ||
						    logRouterLastPops[logRouterIdTagPair] < nextGenerationStartVersion) {
							Version prev = outstandingPops[logRouterIdTagPair].first;
							if (prev < upTo) {
								outstandingPops[logRouterIdTagPair] =
								    std::make_pair(upTo, durableKnownCommittedVersion);
							}
							if (prev == 0) {
								popActors.add(
								    popFromLog(this, log, tag, /*delayBeforePop=*/0.0, /*popLogRouter=*/true));
							}
						}
					}
				}
			}
		}

		nextGenerationStartVersion = generationStartVersion;
	}
}

void TagPartitionedLogSystem::popTxs(Version upTo, int8_t popLocality) {
	for (int i = 0; i < txsTags; i++) {
		pop(upTo, Tag(tagLocalityTxs, i), 0, popLocality);
	}
}

void TagPartitionedLogSystem::pop(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality) {
	if (upTo <= 0)
		return;
	if (tag.locality == tagLocalityRemoteLog) {
		popLogRouter(upTo, tag, durableKnownCommittedVersion, popLocality);
		return;
	}
	for (auto& t : tLogs) {
		if (t->locality == tagLocalitySpecial || t->locality == tag.locality ||
		    (tag.locality < 0 && ((popLocality == tagLocalityInvalid) == t->isLocal))) {
			for (auto& log : t->logServers) {
				Version prev = outstandingPops[std::make_pair(log->get().id(), tag)].first;
				if (prev < upTo) {
					// update pop version for popFromLog actor
					outstandingPops[std::make_pair(log->get().id(), tag)] =
					    std::make_pair(upTo, durableKnownCommittedVersion);
				}
				if (prev == 0) {
					// pop tag from log upto version defined in outstandingPops[].first
					popActors.add(popFromLog(this, log, tag, SERVER_KNOBS->POP_FROM_LOG_DELAY, /*popLogRouter=*/false));
				}
			}
		}
	}
}

ACTOR Future<Void> TagPartitionedLogSystem::popFromLog(TagPartitionedLogSystem* self,
                                                       Reference<AsyncVar<OptionalInterface<TLogInterface>>> log,
                                                       Tag tag,
                                                       double delayBeforePop,
                                                       bool popLogRouter) {
	state Version last = 0;
	loop {
		wait(delay(delayBeforePop, TaskPriority::TLogPop));

		// to: first is upto version, second is durableKnownComittedVersion
		state std::pair<Version, Version> to = self->outstandingPops[std::make_pair(log->get().id(), tag)];

		if (to.first <= last) {
			self->outstandingPops.erase(std::make_pair(log->get().id(), tag));
			return Void();
		}

		try {
			if (!log->get().present())
				return Void();
			wait(log->get().interf().popMessages.getReply(TLogPopRequest(to.first, to.second, tag),
			                                              TaskPriority::TLogPop));

			if (popLogRouter) {
				self->logRouterLastPops[std::make_pair(log->get().id(), tag)] = to.first;
			}

			last = to.first;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			TraceEvent((e.code() == error_code_broken_promise) ? SevInfo : SevError, "LogPopError", self->dbgid)
			    .error(e)
			    .detail("Log", log->get().id());
			return Void(); // Leaving outstandingPops filled in means no further pop requests to this tlog from this
			// logSystem
		}
	}
}

ACTOR Future<Version> TagPartitionedLogSystem::getPoppedFromTLog(
    Reference<AsyncVar<OptionalInterface<TLogInterface>>> log,
    Tag tag) {

	loop {
		choose {
			when(TLogPeekReply rep =
			         wait(log->get().present() ? brokenPromiseToNever(log->get().interf().peekMessages.getReply(
			                                         TLogPeekRequest(-1, tag, false, false)))
			                                   : Never())) {
				ASSERT(rep.popped.present());
				return rep.popped.get();
			}
			when(wait(log->onChange())) {}
		}
	}
}

ACTOR Future<Version> TagPartitionedLogSystem::getPoppedTxs(TagPartitionedLogSystem* self) {
	state std::vector<std::vector<Future<Version>>> poppedFutures;
	state std::vector<Future<Void>> poppedReady;
	if (self->tLogs.size()) {
		poppedFutures.push_back(std::vector<Future<Version>>());
		for (auto& it : self->tLogs) {
			for (auto& log : it->logServers) {
				poppedFutures.back().push_back(TagPartitionedLogSystem::getPoppedFromTLog(log, Tag(tagLocalityTxs, 0)));
			}
		}
		poppedReady.push_back(waitForAny(poppedFutures.back()));
	}

	for (auto& old : self->oldLogData) {
		if (old.tLogs.size()) {
			poppedFutures.push_back(std::vector<Future<Version>>());
			for (auto& it : old.tLogs) {
				for (auto& log : it->logServers) {
					poppedFutures.back().push_back(
					    TagPartitionedLogSystem::getPoppedFromTLog(log, Tag(tagLocalityTxs, 0)));
				}
			}
			poppedReady.push_back(waitForAny(poppedFutures.back()));
		}
	}

	state UID dbgid = self->dbgid;
	state Future<Void> maxGetPoppedDuration = delay(SERVER_KNOBS->TXS_POPPED_MAX_DELAY);
	wait(waitForAll(poppedReady) || maxGetPoppedDuration);

	if (maxGetPoppedDuration.isReady()) {
		TraceEvent(SevWarnAlways, "PoppedTxsNotReady", dbgid).log();
	}

	Version maxPopped = 1;
	for (auto& it : poppedFutures) {
		for (auto& v : it) {
			if (v.isReady()) {
				maxPopped = std::max(maxPopped, v.get());
			}
		}
	}
	return maxPopped;
}

Future<Version> TagPartitionedLogSystem::getTxsPoppedVersion() {
	return getPoppedTxs(this);
}

ACTOR Future<Void> TagPartitionedLogSystem::confirmEpochLive_internal(Reference<LogSet> logSet, Optional<UID> debugID) {
	state std::vector<Future<Void>> alive;
	int numPresent = 0;
	for (auto& t : logSet->logServers) {
		if (t->get().present()) {
			alive.push_back(brokenPromiseToNever(t->get().interf().confirmRunning.getReply(
			    TLogConfirmRunningRequest(debugID), TaskPriority::TLogConfirmRunningReply)));
			numPresent++;
		} else {
			alive.push_back(Never());
		}
	}

	wait(quorum(alive, std::min(logSet->tLogReplicationFactor, numPresent - logSet->tLogWriteAntiQuorum)));

	state std::vector<LocalityEntry> aliveEntries;
	state std::vector<bool> responded(alive.size(), false);
	loop {
		for (int i = 0; i < alive.size(); i++) {
			if (!responded[i] && alive[i].isReady() && !alive[i].isError()) {
				aliveEntries.push_back(logSet->logEntryArray[i]);
				responded[i] = true;
			}
		}

		if (logSet->satisfiesPolicy(aliveEntries)) {
			return Void();
		}

		// The current set of responders that we have weren't enough to form a quorum, so we must
		// wait for more responses and try again.
		std::vector<Future<Void>> changes;
		for (int i = 0; i < alive.size(); i++) {
			if (!alive[i].isReady()) {
				changes.push_back(ready(alive[i]));
			} else if (alive[i].isReady() && alive[i].isError() &&
			           alive[i].getError().code() == error_code_tlog_stopped) {
				// All commits must go to all TLogs.  If any TLog is stopped, then our epoch has ended.
				return Never();
			}
		}
		ASSERT(changes.size() != 0);
		wait(waitForAny(changes));
	}
}

Future<Void> TagPartitionedLogSystem::confirmEpochLive(Optional<UID> debugID) {
	std::vector<Future<Void>> quorumResults;
	for (auto& it : tLogs) {
		if (it->isLocal && it->logServers.size()) {
			quorumResults.push_back(confirmEpochLive_internal(it, debugID));
		}
	}

	return waitForAll(quorumResults);
}

Future<Void> TagPartitionedLogSystem::endEpoch() {
	std::vector<Future<Void>> lockResults;
	for (auto& logSet : tLogs) {
		for (auto& log : logSet->logServers) {
			lockResults.push_back(success(lockTLog(dbgid, log)));
		}
	}
	return waitForAll(lockResults);
}

Future<Reference<ILogSystem>> TagPartitionedLogSystem::newEpoch(
    RecruitFromConfigurationReply const& recr,
    Future<RecruitRemoteFromConfigurationReply> const& fRemoteWorkers,
    DatabaseConfiguration const& config,
    LogEpoch recoveryCount,
    Version recoveryTransactionVersion,
    int8_t primaryLocality,
    int8_t remoteLocality,
    std::vector<Tag> const& allTags,
    Reference<AsyncVar<bool>> const& recruitmentStalled) {
	return newEpoch(Reference<TagPartitionedLogSystem>::addRef(this),
	                recr,
	                fRemoteWorkers,
	                config,
	                recoveryCount,
	                recoveryTransactionVersion,
	                primaryLocality,
	                remoteLocality,
	                allTags,
	                recruitmentStalled);
}

LogSystemType TagPartitionedLogSystem::getLogSystemType() const {
	return logSystemType;
}

LogSystemConfig TagPartitionedLogSystem::getLogSystemConfig() const {
	LogSystemConfig logSystemConfig(epoch);
	logSystemConfig.logSystemType = logSystemType;
	logSystemConfig.expectedLogSets = expectedLogSets;
	logSystemConfig.logRouterTags = logRouterTags;
	logSystemConfig.txsTags = txsTags;
	logSystemConfig.recruitmentID = recruitmentID;
	logSystemConfig.stopped = stopped;
	logSystemConfig.recoveredAt = recoveredAt;
	logSystemConfig.pseudoLocalities = pseudoLocalities;
	logSystemConfig.oldestBackupEpoch = oldestBackupEpoch;
	for (const Reference<LogSet>& logSet : tLogs) {
		if (logSet->isLocal || remoteLogsWrittenToCoreState) {
			logSystemConfig.tLogs.emplace_back(*logSet);
		}
	}

	if (!recoveryCompleteWrittenToCoreState.get()) {
		for (const auto& oldData : oldLogData) {
			logSystemConfig.oldTLogs.emplace_back(oldData);
		}
	}
	return logSystemConfig;
}

Standalone<StringRef> TagPartitionedLogSystem::getLogsValue() const {
	std::vector<std::pair<UID, NetworkAddress>> logs;
	std::vector<std::pair<UID, NetworkAddress>> oldLogs;
	for (auto& t : tLogs) {
		if (t->isLocal || remoteLogsWrittenToCoreState) {
			for (int i = 0; i < t->logServers.size(); i++) {
				logs.emplace_back(t->logServers[i]->get().id(),
				                  t->logServers[i]->get().present() ? t->logServers[i]->get().interf().address()
				                                                    : NetworkAddress());
			}
		}
	}
	if (!recoveryCompleteWrittenToCoreState.get()) {
		for (int i = 0; i < oldLogData.size(); i++) {
			for (auto& t : oldLogData[i].tLogs) {
				for (int j = 0; j < t->logServers.size(); j++) {
					oldLogs.emplace_back(t->logServers[j]->get().id(),
					                     t->logServers[j]->get().present() ? t->logServers[j]->get().interf().address()
					                                                       : NetworkAddress());
				}
			}
		}
	}
	return logsValue(logs, oldLogs);
}

Future<Void> TagPartitionedLogSystem::onLogSystemConfigChange() {
	std::vector<Future<Void>> changes;
	changes.push_back(logSystemConfigChanged.onTrigger());
	for (auto& t : tLogs) {
		for (int i = 0; i < t->logServers.size(); i++) {
			changes.push_back(t->logServers[i]->onChange());
		}
	}
	for (int i = 0; i < oldLogData.size(); i++) {
		for (auto& t : oldLogData[i].tLogs) {
			for (int j = 0; j < t->logServers.size(); j++) {
				changes.push_back(t->logServers[j]->onChange());
			}
		}
	}

	if (hasRemoteServers && !remoteRecovery.isReady()) {
		changes.push_back(remoteRecovery);
	}

	return waitForAny(changes);
}

Version TagPartitionedLogSystem::getEnd() const {
	ASSERT(recoverAt.present());
	return recoverAt.get() + 1;
}

Version TagPartitionedLogSystem::getPeekEnd() const {
	if (recoverAt.present())
		return getEnd();
	else
		return std::numeric_limits<Version>::max();
}

void TagPartitionedLogSystem::getPushLocations(VectorRef<Tag> tags,
                                               std::vector<int>& locations,
                                               bool allLocations) const {
	int locationOffset = 0;
	for (auto& log : tLogs) {
		if (log->isLocal && log->logServers.size()) {
			log->getPushLocations(tags, locations, locationOffset, allLocations);
			locationOffset += log->logServers.size();
		}
	}
}

bool TagPartitionedLogSystem::hasRemoteLogs() const {
	return logRouterTags > 0 || pseudoLocalities.size() > 0;
}

Tag TagPartitionedLogSystem::getRandomRouterTag() const {
	return Tag(tagLocalityLogRouter, deterministicRandom()->randomInt(0, logRouterTags));
}

Tag TagPartitionedLogSystem::getRandomTxsTag() const {
	return Tag(tagLocalityTxs, deterministicRandom()->randomInt(0, txsTags));
}

TLogVersion TagPartitionedLogSystem::getTLogVersion() const {
	return tLogs[0]->tLogVersion;
}

int TagPartitionedLogSystem::getLogRouterTags() const {
	return logRouterTags;
}

Version TagPartitionedLogSystem::getBackupStartVersion() const {
	ASSERT(tLogs.size() > 0);
	return backupStartVersion;
}

std::map<LogEpoch, ILogSystem::EpochTagsVersionsInfo> TagPartitionedLogSystem::getOldEpochTagsVersionsInfo() const {
	std::map<LogEpoch, EpochTagsVersionsInfo> epochInfos;
	for (const auto& old : oldLogData) {
		epochInfos.insert(
		    { old.epoch, ILogSystem::EpochTagsVersionsInfo(old.logRouterTags, old.epochBegin, old.epochEnd) });
		TraceEvent("OldEpochTagsVersions", dbgid)
		    .detail("Epoch", old.epoch)
		    .detail("Tags", old.logRouterTags)
		    .detail("BeginVersion", old.epochBegin)
		    .detail("EndVersion", old.epochEnd);
	}
	return epochInfos;
}

inline Reference<LogSet> TagPartitionedLogSystem::getEpochLogSet(LogEpoch epoch) const {
	for (const auto& old : oldLogData) {
		if (epoch == old.epoch)
			return old.tLogs[0];
	}
	return Reference<LogSet>(nullptr);
}

void TagPartitionedLogSystem::setBackupWorkers(const std::vector<InitializeBackupReply>& replies) {
	ASSERT(tLogs.size() > 0);

	Reference<LogSet> logset = tLogs[0]; // Master recruits this epoch's worker first.
	LogEpoch logsetEpoch = this->epoch;
	oldestBackupEpoch = this->epoch;
	for (const auto& reply : replies) {
		if (removedBackupWorkers.contains(reply.interf.id())) {
			removedBackupWorkers.erase(reply.interf.id());
			continue;
		}
		auto worker = makeReference<AsyncVar<OptionalInterface<BackupInterface>>>(
		    OptionalInterface<BackupInterface>(reply.interf));
		if (reply.backupEpoch != logsetEpoch) {
			// find the logset from oldLogData
			logsetEpoch = reply.backupEpoch;
			oldestBackupEpoch = std::min(oldestBackupEpoch, logsetEpoch);
			logset = getEpochLogSet(logsetEpoch);
			ASSERT(logset.isValid());
		}
		logset->backupWorkers.push_back(worker);
		TraceEvent("AddBackupWorker", dbgid).detail("Epoch", logsetEpoch).detail("BackupWorkerID", reply.interf.id());
	}
	TraceEvent("SetOldestBackupEpoch", dbgid).detail("Epoch", oldestBackupEpoch);
	backupWorkerChanged.trigger();
}

bool TagPartitionedLogSystem::removeBackupWorker(const BackupWorkerDoneRequest& req) {
	bool removed = false;
	Reference<LogSet> logset = getEpochLogSet(req.backupEpoch);
	if (logset.isValid()) {
		for (auto it = logset->backupWorkers.begin(); it != logset->backupWorkers.end(); it++) {
			if (it->getPtr()->get().interf().id() == req.workerUID) {
				logset->backupWorkers.erase(it);
				removed = true;
				break;
			}
		}
	}

	if (removed) {
		oldestBackupEpoch = epoch;
		for (const auto& old : oldLogData) {
			if (old.epoch < oldestBackupEpoch && old.tLogs[0]->backupWorkers.size() > 0) {
				oldestBackupEpoch = old.epoch;
			}
		}
		backupWorkerChanged.trigger();
	} else {
		removedBackupWorkers.insert(req.workerUID);
	}

	TraceEvent("RemoveBackupWorker", dbgid)
	    .detail("Removed", removed)
	    .detail("BackupEpoch", req.backupEpoch)
	    .detail("WorkerID", req.workerUID)
	    .detail("OldestBackupEpoch", oldestBackupEpoch);
	return removed;
}

LogEpoch TagPartitionedLogSystem::getOldestBackupEpoch() const {
	return oldestBackupEpoch;
}

void TagPartitionedLogSystem::setOldestBackupEpoch(LogEpoch epoch) {
	oldestBackupEpoch = epoch;
	backupWorkerChanged.trigger();
}

ACTOR Future<Void> TagPartitionedLogSystem::monitorLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> logServer,
                                                       Reference<AsyncVar<bool>> failed) {
	state Future<Void> waitFailure;
	loop {
		if (logServer->get().present())
			waitFailure = waitFailureTracker(logServer->get().interf().waitFailure, failed);
		else
			failed->set(true);
		wait(logServer->onChange());
	}
}

Optional<std::tuple<Version, Version, std::vector<TLogLockResult>>> TagPartitionedLogSystem::getDurableVersion(
    UID dbgid,
    LogLockInfo lockInfo,
    std::vector<Reference<AsyncVar<bool>>> failed,
    Optional<Version> lastEnd) {

	Reference<LogSet> logSet = lockInfo.logSet;
	// To ensure consistent recovery, the number of servers NOT in the write quorum plus the number of servers NOT
	// in the read quorum have to be strictly less than the replication factor.  Otherwise there could be a replica
	// set consistent entirely of servers that are out of date due to not being in the write quorum or unavailable
	// due to not being in the read quorum. So with N = # of tlogs, W = antiquorum, R = required count, F =
	// replication factor, W + (N - R) < F, and optimally (N-W)+(N-R)=F-1.  Thus R=N+1-F+W.
	int requiredCount =
	    (int)logSet->logServers.size() + 1 - logSet->tLogReplicationFactor + logSet->tLogWriteAntiQuorum;
	ASSERT(requiredCount > 0 && requiredCount <= logSet->logServers.size());
	ASSERT(logSet->tLogReplicationFactor >= 1 && logSet->tLogReplicationFactor <= logSet->logServers.size());
	ASSERT(logSet->tLogWriteAntiQuorum >= 0 && logSet->tLogWriteAntiQuorum < logSet->logServers.size());

	std::vector<LocalityData> availableItems, badCombo;
	std::vector<TLogLockResult> results;
	std::string sServerState;
	LocalityGroup unResponsiveSet;
	for (int t = 0; t < logSet->logServers.size(); t++) {
		if (lockInfo.replies[t].isReady() && !lockInfo.replies[t].isError() && (!failed.size() || !failed[t]->get())) {
			results.push_back(lockInfo.replies[t].get());
			availableItems.push_back(logSet->tLogLocalities[t]);
			sServerState += 'a';
		} else {
			unResponsiveSet.add(logSet->tLogLocalities[t]);
			TraceEvent("GetDurableResultNoResponse").detail("TLog", logSet->logServers[t]->get().id());
			sServerState += 'f';
		}
	}

	// Check if the list of results is not larger than the anti quorum
	bool bTooManyFailures = (results.size() <= logSet->tLogWriteAntiQuorum);

	// Check if failed logs complete the policy
	// @note Do not do the replication policy validation check if version vector
	// is enabled. The implication of this change is that recoveries could take
	// longer when version vector is enabled - that's fine for now as "version
	// vector" is an experimental feature at this point.
	// @todo Extend "getRecoverVersionUnicast()" (the function that finds the
	// recovery version when version vector is enabled) to do the replication
	// policy check in an efficient manner, and enable the validation check
	// here at that point.
	bTooManyFailures =
	    bTooManyFailures ||
	    ((unResponsiveSet.size() >= logSet->tLogReplicationFactor) &&
	     (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST || (unResponsiveSet.validate(logSet->tLogPolicy))));

	// Check all combinations of the AntiQuorum within the failed
	if (!bTooManyFailures && (logSet->tLogWriteAntiQuorum) &&
	    (!validateAllCombinations(
	        badCombo, unResponsiveSet, logSet->tLogPolicy, availableItems, logSet->tLogWriteAntiQuorum, false))) {
		TraceEvent("EpochEndBadCombo", dbgid)
		    .detail("Required", requiredCount)
		    .detail("Present", results.size())
		    .detail("ServerState", sServerState);
		bTooManyFailures = true;
	}

	ASSERT(logSet->logServers.size() == lockInfo.replies.size());
	if (!bTooManyFailures) {
		std::sort(results.begin(), results.end(), [](const TLogLockResult& a, const TLogLockResult& b) -> bool {
			return a.end < b.end;
		});
		int absent = logSet->logServers.size() - results.size();
		int safe_range_begin = logSet->tLogWriteAntiQuorum;
		int new_safe_range_begin = std::min(logSet->tLogWriteAntiQuorum, (int)(results.size() - 1));
		int safe_range_end = std::max(logSet->tLogReplicationFactor - absent, 1);
		// The index (in "results" vector) of the recovery version that we will use in the check below
		// to decide whether to restart recovery not. In "main" we use the version at index
		// "(safe_range_end - 1)" - this is to minimize the chances of restarting the current recovery
		// process. With "version vector" we use the version at index "new_safe_range_begin" - this is
		// because choosing any other version may result in not copying the correct version range to the
		// log servers in the latest epoch and also will invalidate the changes that we made to the peek
		// logic in the context of version vector.
		int versionIndex =
		    (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST ? (safe_range_end - 1) : new_safe_range_begin);

		if (!lastEnd.present() ||
		    ((versionIndex >= 0) && (versionIndex < results.size()) && results[versionIndex].end < lastEnd.get())) {
			Version knownCommittedVersion = 0;
			for (int i = 0; i < results.size(); i++) {
				knownCommittedVersion = std::max(knownCommittedVersion, results[i].knownCommittedVersion);
			}

			if (knownCommittedVersion > results[new_safe_range_begin].end) {
				knownCommittedVersion = results[new_safe_range_begin].end;
			}

			TraceEvent("GetDurableResult", dbgid)
			    .detail("Required", requiredCount)
			    .detail("Present", results.size())
			    .detail("Anti", logSet->tLogWriteAntiQuorum)
			    .detail("ServerState", sServerState)
			    .detail("RecoveryVersion",
			            ((safe_range_end > 0) && (safe_range_end - 1 < results.size()))
			                ? results[safe_range_end - 1].end
			                : -1)
			    .detail("EndVersion", results[new_safe_range_begin].end)
			    .detail("SafeBegin", safe_range_begin)
			    .detail("SafeEnd", safe_range_end)
			    .detail("NewSafeBegin", new_safe_range_begin)
			    .detail("KnownCommittedVersion", knownCommittedVersion)
			    .detail("EpochEnd", lockInfo.epochEnd);

			// @note In "main" any version in the index range [safe_range_begin, safe_range_end) can be
			// picked as the recovery version. We pick the version at index "new_safe_range_begin" in order
			// to minimize the number of recovery restarts and also to minimize the amount of data we need
			// to copy during recovery. With "version vector" we pick the version "new_safe_range_begin"
			// as choosing any other version may result in not copying the correct version range to the
			// log servers in the latest epoch and also will invalidate the changes that we made to the
			// peek logic in the context of version vector.
			return std::make_tuple(knownCommittedVersion, results[new_safe_range_begin].end, results);
		}
	}
	TraceEvent("GetDurableResultWaiting", dbgid)
	    .detail("Required", requiredCount)
	    .detail("Present", results.size())
	    .detail("ServerState", sServerState);
	return Optional<std::tuple<Version, Version, std::vector<TLogLockResult>>>();
}

ACTOR Future<Void> TagPartitionedLogSystem::getDurableVersionChanged(LogLockInfo lockInfo,
                                                                     std::vector<Reference<AsyncVar<bool>>> failed) {
	// Wait for anything relevant to change
	std::vector<Future<Void>> changes;
	for (int j = 0; j < lockInfo.logSet->logServers.size(); j++) {
		if (!lockInfo.replies[j].isReady())
			changes.push_back(ready(lockInfo.replies[j]));
		else {
			changes.push_back(lockInfo.logSet->logServers[j]->onChange());
			if (failed.size()) {
				changes.push_back(failed[j]->onChange());
			}
		}
	}
	ASSERT(changes.size());
	wait(waitForAny(changes));
	return Void();
}

void getTLogLocIds(const std::vector<Reference<LogSet>>& tLogs,
                   const std::tuple<int, std::vector<TLogLockResult>>& logGroupResults,
                   std::vector<uint16_t>& tLogLocIds,
                   uint16_t& maxTLogLocId) {
	// Initialization.
	tLogLocIds.clear();
	maxTLogLocId = 0;

	// Map the interfaces of all (local) tLogs to their corresponding locations in LogSets.
	std::map<UID, uint16_t> interfLocMap;
	uint16_t location = 0;
	for (auto& it : tLogs) {
		if (!it->isLocal) {
			continue;
		}
		for (uint16_t i = 0; i < it->logServers.size(); i++) {
			if (it->logServers[i]->get().present()) {
				interfLocMap[it->logServers[i]->get().interf().id()] = location;
			}
			location++;
		}
	}

	// Set maxTLogLocId.
	maxTLogLocId = location;

	// Find the locations of tLogs in "logGroupResults".
	for (auto& tLogResult : std::get<1>(logGroupResults)) {
		ASSERT(interfLocMap.find(tLogResult.logId) != interfLocMap.end());
		tLogLocIds.push_back(interfLocMap[tLogResult.logId]);
	}
}

Version findMaxKCV(const std::tuple<int, std::vector<TLogLockResult>>& logGroupResults) {
	Version maxKCV = 0;
	for (auto& tLogResult : std::get<1>(logGroupResults)) {
		maxKCV = std::max(maxKCV, tLogResult.knownCommittedVersion);
	}
	return maxKCV;
}

void populateBitset(boost::dynamic_bitset<>& bs, const std::vector<uint16_t>& ids) {
	for (auto& id : ids) {
		ASSERT(id < bs.size());
		bs.set(id);
	}
}

// If ENABLE_VERSION_VECTOR_TLOG_UNICAST is set, one tLog's DV may advance beyond the min(DV) over all tLogs.
// This function finds the highest recoverable version for each tLog group over all log groups.
// All prior versions to the chosen RV must also be recoverable.
// TODO: unit tests to stress UNICAST
Optional<std::tuple<Version, Version>> getRecoverVersionUnicast(
    const std::vector<Reference<LogSet>>& logServers,
    const std::tuple<int, std::vector<TLogLockResult>>& logGroupResults,
    Version minDV) {
	std::vector<uint16_t> tLogLocIds;
	uint16_t maxTLogLocId; // maximum possible id, not maximum of id's of available log servers
	getTLogLocIds(logServers, logGroupResults, tLogLocIds, maxTLogLocId);
	uint16_t bsSize = maxTLogLocId + 1; // bitset size, used below

	Version maxKCV = findMaxKCV(logGroupResults);

	// Summarize the information sent by various tLogs.
	// A bitset of available tLogs
	boost::dynamic_bitset<> availableTLogs(bsSize);
	// version -> tLogs (that are avaiable) that have received the version
	std::unordered_map<Version, boost::dynamic_bitset<>> versionAvailableTLogs;
	// version -> all tLogs that the version was sent to (by the commit proxy)
	std::map<Version, boost::dynamic_bitset<>> versionAllTLogs;
	// version -> prevVersion (that was given out by the sequencer) map
	std::map<Version, Version> prevVersionMap;
	uint16_t tLogIdx = 0;
	int replicationFactor = std::get<0>(logGroupResults);
	for (auto& tLogResult : std::get<1>(logGroupResults)) {
		uint16_t tLogLocId = tLogLocIds[tLogIdx++];
		if (tLogResult.unknownCommittedVersions.empty()) {
			continue;
		}
		availableTLogs.set(tLogLocId);
		for (auto& unknownCommittedVersion : tLogResult.unknownCommittedVersions) {
			Version k = unknownCommittedVersion.version;
			if (k > maxKCV) {
				if (versionAvailableTLogs[k].empty()) {
					versionAvailableTLogs[k].resize(bsSize);
				}
				versionAvailableTLogs[k].set(tLogLocId);
				prevVersionMap[k] = unknownCommittedVersion.prev;
				if (versionAllTLogs[k].empty()) {
					versionAllTLogs[k].resize(bsSize);
				}
				populateBitset(versionAllTLogs[k], unknownCommittedVersion.tLogLocIds);
			}
		}
	}

	if (versionAllTLogs.empty()) {
		return std::make_tuple(maxKCV, maxKCV);
	}

	// Compute recovery version.
	//
	// @note we think that the unicast recovery version should always greater than or
	// equal to "min(DV)" (= "minDV"). To be conservative we use "max(KCV)" (= "maxKCV")
	// as the default (starting) recovery version and later verify that the computed
	// recovery version is greater than or equal to "minDV".
	//
	// @note we are not using "min(KCV)" as the default recovery version because "known
	// committed version" can advance even after a log server is locked when unicast is
	// enabled and so all log servers may not preserve all committed versions, from
	// "min(KCV)" till the correct recovery version, in "unknownCommittedVersions" and
	// this may cause the recovery algorithm to not find the correct recovery version.
	//
	// @todo modify code to use "minDV" as the default (starting) recovery version.
	Version RV = maxKCV; // recovery version
	// @note we currently don't use "RVs", but we may use this information later (maybe for
	// doing error checking). Commenting out the RVs related code for now.
	// std::vector<Version> RVs(maxTLogLocId + 1, maxKCV); // recovery versions of various tLogs
	Version prevVersion = maxKCV;
	for (auto const& [version, tLogs] : versionAllTLogs) {
		if (!(prevVersion == maxKCV || prevVersion == prevVersionMap[version])) {
			break;
		}
		// This version is not recoverable if there is a log server (LS) such that:
		// - the commit proxy sent this version to LS (i.e., LS is present in "versionAllTLogs[version]")
		// - LS is available (i.e., LS is present in "availableTLogs")
		// - LS didn't receive this version (i.e., LS is not present in "versionAvailableTLogs[version]")
		if (((tLogs & availableTLogs) & ~versionAvailableTLogs[version]).any()) {
			break;
		}
		// If the commit proxy sent this version to "N" log servers then at least
		// (N - replicationFactor + 1) log servers must be available.
		// @todo Also do the replication policy validation check here, and enable
		// that check in "getDurableVersion()" at that point.
		if (!(versionAvailableTLogs[version].count() >= tLogs.count() - replicationFactor + 1)) {
			break;
		}
		// Update RV.
		RV = version;
		/*
		// @note We currently don't use "RVs", but we may use this information later (maybe for doing
		// error checking). Commenting out this code for now.
		// Update recovery version vector.
		for (boost::dynamic_bitset<>::size_type id = 0; id < versionAvailableTLogs[version].size(); id++) {
		    if (versionAvailableTLogs[version][id]) {
		        RVs[id] = version;
		    }
		}
		*/
		// Update prevVersion.
		prevVersion = version;
	}
	ASSERT_WE_THINK(RV >= minDV && RV != std::numeric_limits<Version>::max());
	ASSERT_WE_THINK(RV >= maxKCV);
	return std::make_tuple(maxKCV, RV);
}

ACTOR Future<Void> TagPartitionedLogSystem::epochEnd(Reference<AsyncVar<Reference<ILogSystem>>> outLogSystem,
                                                     UID dbgid,
                                                     DBCoreState prevState,
                                                     FutureStream<TLogRejoinRequest> rejoinRequests,
                                                     LocalityData locality,
                                                     bool* forceRecovery) {
	// Stops a co-quorum of tlogs so that no further versions can be committed until the DBCoreState coordination
	// state is changed Creates a new logSystem representing the (now frozen) epoch No other important side effects.
	// The writeQuorum in the master info is from the previous configuration

	if (!prevState.tLogs.size()) {
		// This is a brand new database
		auto logSystem = makeReference<TagPartitionedLogSystem>(dbgid, locality, 0);
		logSystem->logSystemType = prevState.logSystemType;
		logSystem->recoverAt = 0;
		logSystem->knownCommittedVersion = 0;
		logSystem->stopped = true;
		outLogSystem->set(logSystem);
		wait(Future<Void>(Never()));
		throw internal_error();
	}

	if (*forceRecovery) {
		DBCoreState modifiedState = prevState;

		int8_t primaryLocality = -1;
		for (auto& coreSet : modifiedState.tLogs) {
			if (coreSet.isLocal && coreSet.locality >= 0 && coreSet.tLogLocalities[0].dcId() != locality.dcId()) {
				primaryLocality = coreSet.locality;
				break;
			}
		}

		bool foundRemote = false;
		int8_t remoteLocality = -1;
		int modifiedLogSets = 0;
		int removedLogSets = 0;
		if (primaryLocality >= 0) {
			auto copiedLogs = modifiedState.tLogs;
			for (auto& coreSet : copiedLogs) {
				if (coreSet.locality != primaryLocality && coreSet.locality >= 0) {
					foundRemote = true;
					remoteLocality = coreSet.locality;
					modifiedState.tLogs.clear();
					modifiedState.tLogs.push_back(coreSet);
					modifiedState.tLogs[0].isLocal = true;
					modifiedState.logRouterTags = 0;
					modifiedLogSets++;
					break;
				}
			}

			while (!foundRemote && modifiedState.oldTLogData.size()) {
				for (auto& coreSet : modifiedState.oldTLogData[0].tLogs) {
					if (coreSet.locality != primaryLocality && coreSet.locality >= tagLocalitySpecial) {
						foundRemote = true;
						remoteLocality = coreSet.locality;
						modifiedState.tLogs.clear();
						modifiedState.tLogs.push_back(coreSet);
						modifiedState.tLogs[0].isLocal = true;
						modifiedState.logRouterTags = 0;
						modifiedState.txsTags = modifiedState.oldTLogData[0].txsTags;
						modifiedLogSets++;
						break;
					}
				}
				modifiedState.oldTLogData.erase(modifiedState.oldTLogData.begin());
				removedLogSets++;
			}

			if (foundRemote) {
				for (int i = 0; i < modifiedState.oldTLogData.size(); i++) {
					bool found = false;
					auto copiedLogs = modifiedState.oldTLogData[i].tLogs;
					for (auto& coreSet : copiedLogs) {
						if (coreSet.locality == remoteLocality || coreSet.locality == tagLocalitySpecial) {
							found = true;
							if (!coreSet.isLocal || copiedLogs.size() > 1) {
								modifiedState.oldTLogData[i].tLogs.clear();
								modifiedState.oldTLogData[i].tLogs.push_back(coreSet);
								modifiedState.oldTLogData[i].tLogs[0].isLocal = true;
								modifiedState.oldTLogData[i].logRouterTags = 0;
								modifiedState.oldTLogData[i].epochBegin =
								    modifiedState.oldTLogData[i].tLogs[0].startVersion;
								modifiedState.oldTLogData[i].epochEnd =
								    (i == 0 ? modifiedState.tLogs[0].startVersion
								            : modifiedState.oldTLogData[i - 1].tLogs[0].startVersion);
								modifiedLogSets++;
							}
							break;
						}
					}
					if (!found) {
						modifiedState.oldTLogData.erase(modifiedState.oldTLogData.begin() + i);
						removedLogSets++;
						i--;
					}
				}
				prevState = modifiedState;
			} else {
				*forceRecovery = false;
			}
		} else {
			*forceRecovery = false;
		}
		TraceEvent(SevWarnAlways, "ForcedRecovery", dbgid)
		    .detail("PrimaryLocality", primaryLocality)
		    .detail("RemoteLocality", remoteLocality)
		    .detail("FoundRemote", foundRemote)
		    .detail("Modified", modifiedLogSets)
		    .detail("Removed", removedLogSets);
		for (int i = 0; i < prevState.tLogs.size(); i++) {
			TraceEvent("ForcedRecoveryTLogs", dbgid)
			    .detail("I", i)
			    .detail("Log", ::describe(prevState.tLogs[i].tLogs))
			    .detail("Loc", prevState.tLogs[i].locality)
			    .detail("Txs", prevState.txsTags);
		}
		for (int i = 0; i < prevState.oldTLogData.size(); i++) {
			for (int j = 0; j < prevState.oldTLogData[i].tLogs.size(); j++) {
				TraceEvent("ForcedRecoveryTLogs", dbgid)
				    .detail("I", i)
				    .detail("J", j)
				    .detail("Log", ::describe(prevState.oldTLogData[i].tLogs[j].tLogs))
				    .detail("Loc", prevState.oldTLogData[i].tLogs[j].locality)
				    .detail("Txs", prevState.oldTLogData[i].txsTags);
			}
		}
	}

	CODE_PROBE(true, "Master recovery from pre-existing database");

	// trackRejoins listens for rejoin requests from the tLogs that we are recovering from, to learn their
	// TLogInterfaces
	state std::vector<LogLockInfo> lockResults;
	state std::vector<std::pair<Reference<AsyncVar<OptionalInterface<TLogInterface>>>, Reference<IReplicationPolicy>>>
	    allLogServers;
	state std::vector<Reference<LogSet>> logServers;
	state std::vector<OldLogData> oldLogData;
	state std::vector<std::vector<Reference<AsyncVar<bool>>>> logFailed;
	state std::vector<Future<Void>> failureTrackers;

	for (const CoreTLogSet& coreSet : prevState.tLogs) {
		logServers.push_back(makeReference<LogSet>(coreSet));
		std::vector<Reference<AsyncVar<bool>>> failed;

		for (const auto& logVar : logServers.back()->logServers) {
			allLogServers.emplace_back(logVar, coreSet.tLogPolicy);
			failed.push_back(makeReference<AsyncVar<bool>>());
			failureTrackers.push_back(TagPartitionedLogSystem::monitorLog(logVar, failed.back()));
		}
		logFailed.push_back(failed);
	}

	for (const auto& oldTlogData : prevState.oldTLogData) {
		oldLogData.emplace_back(oldTlogData);

		for (const auto& logSet : oldLogData.back().tLogs) {
			for (const auto& logVar : logSet->logServers) {
				allLogServers.emplace_back(logVar, logSet->tLogPolicy);
			}
		}
	}
	state Future<Void> rejoins = TagPartitionedLogSystem::trackRejoins(dbgid, allLogServers, rejoinRequests);

	lockResults.resize(logServers.size());
	std::set<int8_t> lockedLocalities;
	bool foundSpecial = false;
	for (int i = 0; i < logServers.size(); i++) {
		if (logServers[i]->locality == tagLocalitySpecial) {
			foundSpecial = true;
		}
		lockedLocalities.insert(logServers[i]->locality);
		lockResults[i].isCurrent = true;
		lockResults[i].logSet = logServers[i];
		for (int t = 0; t < logServers[i]->logServers.size(); t++) {
			lockResults[i].replies.push_back(TagPartitionedLogSystem::lockTLog(dbgid, logServers[i]->logServers[t]));
		}
	}

	for (auto& old : oldLogData) {
		if (foundSpecial) {
			break;
		}
		for (auto& log : old.tLogs) {
			if (log->locality == tagLocalitySpecial) {
				foundSpecial = true;
				break;
			}
			if (!lockedLocalities.contains(log->locality)) {
				TraceEvent("EpochEndLockExtra").detail("Locality", log->locality);
				CODE_PROBE(true, "locking old generations for version information");
				lockedLocalities.insert(log->locality);
				LogLockInfo lockResult;
				lockResult.epochEnd = old.epochEnd;
				lockResult.logSet = log;
				for (int t = 0; t < log->logServers.size(); t++) {
					lockResult.replies.push_back(TagPartitionedLogSystem::lockTLog(dbgid, log->logServers[t]));
				}
				lockResults.push_back(lockResult);
			}
		}
	}
	if (*forceRecovery) {
		state std::vector<LogLockInfo> allLockResults;
		ASSERT(lockResults.size() == 1);
		allLockResults.push_back(lockResults[0]);
		for (auto& old : oldLogData) {
			ASSERT(old.tLogs.size() == 1);
			LogLockInfo lockResult;
			lockResult.epochEnd = old.epochEnd;
			lockResult.logSet = old.tLogs[0];
			for (int t = 0; t < old.tLogs[0]->logServers.size(); t++) {
				lockResult.replies.push_back(TagPartitionedLogSystem::lockTLog(dbgid, old.tLogs[0]->logServers[t]));
			}
			allLockResults.push_back(lockResult);
		}
		state int lockNum = 0;
		state Version maxRecoveryVersion = 0;
		state int maxRecoveryIndex = 0;
		while (lockNum < allLockResults.size()) {

			auto versions = TagPartitionedLogSystem::getDurableVersion(dbgid, allLockResults[lockNum]);
			if (versions.present()) {
				if (std::get<1>(versions.get()) > maxRecoveryVersion) {
					TraceEvent("HigherRecoveryVersion", dbgid)
					    .detail("Idx", lockNum)
					    .detail("Ver", std::get<1>(versions.get()));
					maxRecoveryVersion = std::get<1>(versions.get());
					maxRecoveryIndex = lockNum;
				}
				lockNum++;
			} else {
				wait(TagPartitionedLogSystem::getDurableVersionChanged(allLockResults[lockNum]));
			}
		}
		if (maxRecoveryIndex > 0) {
			logServers = oldLogData[maxRecoveryIndex - 1].tLogs;
			prevState.txsTags = oldLogData[maxRecoveryIndex - 1].txsTags;
			lockResults[0] = allLockResults[maxRecoveryIndex];
			lockResults[0].isCurrent = true;

			std::vector<Reference<AsyncVar<bool>>> failed;
			for (auto& log : logServers[0]->logServers) {
				failed.push_back(makeReference<AsyncVar<bool>>());
				failureTrackers.push_back(TagPartitionedLogSystem::monitorLog(log, failed.back()));
			}
			ASSERT(logFailed.size() == 1);
			logFailed[0] = failed;
			oldLogData.erase(oldLogData.begin(), oldLogData.begin() + maxRecoveryIndex);
		}
	}

	state Optional<Version> lastEnd;
	state Version knownCommittedVersion = 0;
	loop {
		Version minEnd = std::numeric_limits<Version>::max();
		Version minDV = std::numeric_limits<Version>::max();
		Version maxEnd = 0;
		state std::vector<Future<Void>> changes;
		state std::vector<std::tuple<int, std::vector<TLogLockResult>>> logGroupResults;
		for (int log = 0; log < logServers.size(); log++) {
			if (!logServers[log]->isLocal) {
				continue;
			}
			auto versions =
			    TagPartitionedLogSystem::getDurableVersion(dbgid, lockResults[log], logFailed[log], lastEnd);
			if (versions.present()) {
				logGroupResults.emplace_back(logServers[log]->tLogReplicationFactor, std::get<2>(versions.get()));
				minDV = std::min(minDV, std::get<1>(versions.get()));
				if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
					knownCommittedVersion = std::max(knownCommittedVersion, std::get<0>(versions.get()));
					maxEnd = std::max(maxEnd, std::get<1>(versions.get()));
					minEnd = std::min(minEnd, std::get<1>(versions.get()));
				} else {
					auto unicastVersions = getRecoverVersionUnicast(logServers, logGroupResults.back(), minDV);
					knownCommittedVersion = std::max(knownCommittedVersion, std::get<0>(unicastVersions.get()));
					maxEnd = std::max(maxEnd, std::get<1>(unicastVersions.get()));
					minEnd = std::min(minEnd, std::get<1>(unicastVersions.get()));
				}
			}
			changes.push_back(TagPartitionedLogSystem::getDurableVersionChanged(lockResults[log], logFailed[log]));
		}
		if (maxEnd > 0 && (!lastEnd.present() || maxEnd < lastEnd.get())) {
			CODE_PROBE(lastEnd.present(), "Restarting recovery at an earlier point");

			state Reference<TagPartitionedLogSystem> logSystem =
			    makeReference<TagPartitionedLogSystem>(dbgid, locality, prevState.recoveryCount);

			logSystem->recoverAt = minEnd;
			lastEnd = minEnd;
			logSystem->tLogs = logServers;
			logSystem->logRouterTags = prevState.logRouterTags;
			logSystem->txsTags = prevState.txsTags;
			logSystem->oldLogData = oldLogData;
			logSystem->logSystemType = prevState.logSystemType;
			logSystem->rejoins = rejoins;
			logSystem->lockResults = lockResults;
			if (knownCommittedVersion > minEnd) {
				knownCommittedVersion = minEnd;
			}
			logSystem->knownCommittedVersion = knownCommittedVersion;
			TraceEvent(SevDebug, "FinalRecoveryVersionInfo")
			    .detail("KCV", knownCommittedVersion)
			    .detail("MinEnd", minEnd);
			logSystem->remoteLogsWrittenToCoreState = true;
			logSystem->stopped = true;
			logSystem->pseudoLocalities = prevState.pseudoLocalities;
			outLogSystem->set(logSystem);
		}

		wait(waitForAny(changes));
	}
}

ACTOR Future<Void> TagPartitionedLogSystem::recruitOldLogRouters(TagPartitionedLogSystem* self,
                                                                 std::vector<WorkerInterface> workers,
                                                                 LogEpoch recoveryCount,
                                                                 int8_t locality,
                                                                 Version startVersion,
                                                                 std::vector<LocalityData> tLogLocalities,
                                                                 Reference<IReplicationPolicy> tLogPolicy,
                                                                 bool forRemote) {
	state std::vector<std::vector<Future<TLogInterface>>> logRouterInitializationReplies;
	state std::vector<Future<TLogInterface>> allReplies;
	int nextRouter = 0;
	state Version lastStart = std::numeric_limits<Version>::max();

	if (!forRemote) {
		Version maxStart = TagPartitionedLogSystem::getMaxLocalStartVersion(self->tLogs);

		lastStart = std::max(startVersion, maxStart);
		if (self->logRouterTags == 0) {
			ASSERT_WE_THINK(false);
			self->logSystemConfigChanged.trigger();
			return Void();
		}

		bool found = false;
		for (auto& tLogs : self->tLogs) {
			if (tLogs->locality == locality) {
				found = true;
			}

			tLogs->logRouters.clear();
		}

		if (!found) {
			TraceEvent("RecruitingOldLogRoutersAddingLocality")
			    .detail("Locality", locality)
			    .detail("LastStart", lastStart);
			auto newLogSet = makeReference<LogSet>();
			newLogSet->locality = locality;
			newLogSet->startVersion = lastStart;
			newLogSet->isLocal = false;
			self->tLogs.push_back(newLogSet);
		}

		for (auto& tLogs : self->tLogs) {
			// Recruit log routers for old generations of the primary locality
			if (tLogs->locality == locality) {
				logRouterInitializationReplies.emplace_back();
				for (int i = 0; i < self->logRouterTags; i++) {
					InitializeLogRouterRequest req;
					req.recoveryCount = recoveryCount;
					req.routerTag = Tag(tagLocalityLogRouter, i);
					req.startVersion = lastStart;
					req.tLogLocalities = tLogLocalities;
					req.tLogPolicy = tLogPolicy;
					req.locality = locality;
					req.recoverAt = self->recoverAt.get();
					auto reply = transformErrors(
					    throwErrorOr(workers[nextRouter].logRouter.getReplyUnlessFailedFor(
					        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
					    cluster_recovery_failed());
					logRouterInitializationReplies.back().push_back(reply);
					allReplies.push_back(reply);
					nextRouter = (nextRouter + 1) % workers.size();
				}
			}
		}
	}

	for (auto& old : self->oldLogData) {
		Version maxStart = TagPartitionedLogSystem::getMaxLocalStartVersion(old.tLogs);

		if (old.logRouterTags == 0 || maxStart >= lastStart) {
			break;
		}
		lastStart = std::max(startVersion, maxStart);
		bool found = false;
		for (auto& tLogs : old.tLogs) {
			if (tLogs->locality == locality) {
				found = true;
			}
			tLogs->logRouters.clear();
		}

		if (!found) {
			TraceEvent("RecruitingOldLogRoutersAddingLocality")
			    .detail("Locality", locality)
			    .detail("LastStart", lastStart);
			auto newLogSet = makeReference<LogSet>();
			newLogSet->locality = locality;
			newLogSet->startVersion = lastStart;
			old.tLogs.push_back(newLogSet);
		}

		for (auto& tLogs : old.tLogs) {
			// Recruit log routers for old generations of the primary locality
			if (tLogs->locality == locality) {
				logRouterInitializationReplies.emplace_back();
				for (int i = 0; i < old.logRouterTags; i++) {
					InitializeLogRouterRequest req;
					req.recoveryCount = recoveryCount;
					req.routerTag = Tag(tagLocalityLogRouter, i);
					req.startVersion = lastStart;
					req.tLogLocalities = tLogLocalities;
					req.tLogPolicy = tLogPolicy;
					req.locality = locality;
					req.recoverAt = old.recoverAt;
					auto reply = transformErrors(
					    throwErrorOr(workers[nextRouter].logRouter.getReplyUnlessFailedFor(
					        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
					    cluster_recovery_failed());
					logRouterInitializationReplies.back().push_back(reply);
					allReplies.push_back(reply);
					nextRouter = (nextRouter + 1) % workers.size();
				}
			}
		}
	}

	wait(waitForAll(allReplies));

	int nextReplies = 0;
	lastStart = std::numeric_limits<Version>::max();
	std::vector<Future<Void>> failed;

	if (!forRemote) {
		Version maxStart = TagPartitionedLogSystem::getMaxLocalStartVersion(self->tLogs);

		lastStart = std::max(startVersion, maxStart);
		for (auto& tLogs : self->tLogs) {
			if (tLogs->locality == locality) {
				for (int i = 0; i < logRouterInitializationReplies[nextReplies].size(); i++) {
					tLogs->logRouters.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
					    OptionalInterface<TLogInterface>(logRouterInitializationReplies[nextReplies][i].get())));
					failed.push_back(
					    waitFailureClient(logRouterInitializationReplies[nextReplies][i].get().waitFailure,
					                      SERVER_KNOBS->TLOG_TIMEOUT,
					                      -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
					                      /*trace=*/true));
				}
				nextReplies++;
			}
		}
	}

	for (auto& old : self->oldLogData) {
		Version maxStart = TagPartitionedLogSystem::getMaxLocalStartVersion(old.tLogs);
		if (old.logRouterTags == 0 || maxStart >= lastStart) {
			break;
		}
		lastStart = std::max(startVersion, maxStart);
		for (auto& tLogs : old.tLogs) {
			if (tLogs->locality == locality) {
				for (int i = 0; i < logRouterInitializationReplies[nextReplies].size(); i++) {
					tLogs->logRouters.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
					    OptionalInterface<TLogInterface>(logRouterInitializationReplies[nextReplies][i].get())));
					if (!forRemote) {
						failed.push_back(waitFailureClient(
						    logRouterInitializationReplies[nextReplies][i].get().waitFailure,
						    SERVER_KNOBS->TLOG_TIMEOUT,
						    -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
						    /*trace=*/true));
					}
				}
				nextReplies++;
			}
		}
	}

	if (!forRemote) {
		self->logSystemConfigChanged.trigger();
		wait(failed.size() ? tagError<Void>(quorum(failed, 1), tlog_failed()) : Future<Void>(Never()));
		throw internal_error();
	}
	return Void();
}

Version TagPartitionedLogSystem::getMaxLocalStartVersion(const std::vector<Reference<LogSet>>& tLogs) {
	Version maxStart = 0;
	for (const auto& logSet : tLogs) {
		if (logSet->isLocal) {
			maxStart = std::max(maxStart, logSet->startVersion);
		}
	}
	return maxStart;
}

std::vector<Tag> TagPartitionedLogSystem::getLocalTags(int8_t locality, const std::vector<Tag>& allTags) {
	std::vector<Tag> localTags;
	for (const auto& tag : allTags) {
		if (locality == tagLocalitySpecial || locality == tag.locality || tag.locality < 0) {
			localTags.push_back(tag);
		}
	}
	return localTags;
}

ACTOR Future<Void> TagPartitionedLogSystem::newRemoteEpoch(TagPartitionedLogSystem* self,
                                                           Reference<TagPartitionedLogSystem> oldLogSystem,
                                                           Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
                                                           DatabaseConfiguration configuration,
                                                           LogEpoch recoveryCount,
                                                           Version recoveryTransactionVersion,
                                                           int8_t remoteLocality,
                                                           std::vector<Tag> allTags,
                                                           std::vector<Version> oldGenerationRecoverAtVersions) {
	TraceEvent("RemoteLogRecruitment_WaitingForWorkers").log();
	state RecruitRemoteFromConfigurationReply remoteWorkers = wait(fRemoteWorkers);
	TraceEvent("RecruitedRemoteLogWorkers")
	    .detail("TLogs", remoteWorkers.remoteTLogs.size())
	    .detail("LogRouter", remoteWorkers.logRouters.size());

	state Reference<LogSet> logSet(new LogSet());
	logSet->tLogReplicationFactor = configuration.getRemoteTLogReplicationFactor();
	logSet->tLogVersion = configuration.tLogVersion;
	logSet->tLogPolicy = configuration.getRemoteTLogPolicy();
	logSet->isLocal = false;
	logSet->locality = remoteLocality;

	logSet->startVersion = oldLogSystem->knownCommittedVersion + 1;
	state int lockNum = 0;
	while (lockNum < oldLogSystem->lockResults.size()) {
		if (oldLogSystem->lockResults[lockNum].logSet->locality == remoteLocality) {

			loop {
				auto versions =
				    TagPartitionedLogSystem::getDurableVersion(self->dbgid, oldLogSystem->lockResults[lockNum]);
				if (versions.present()) {
					logSet->startVersion =
					    std::min(std::min(std::get<0>(versions.get()) + 1, oldLogSystem->lockResults[lockNum].epochEnd),
					             logSet->startVersion);
					break;
				}
				wait(TagPartitionedLogSystem::getDurableVersionChanged(oldLogSystem->lockResults[lockNum]));
			}
			break;
		}
		lockNum++;
	}

	std::vector<LocalityData> localities;
	localities.resize(remoteWorkers.remoteTLogs.size());
	for (int i = 0; i < remoteWorkers.remoteTLogs.size(); i++) {
		localities[i] = remoteWorkers.remoteTLogs[i].locality;
	}

	state Future<Void> oldRouterRecruitment = Void();
	if (logSet->startVersion < oldLogSystem->knownCommittedVersion + 1) {
		ASSERT(oldLogSystem->logRouterTags > 0);
		oldRouterRecruitment = TagPartitionedLogSystem::recruitOldLogRouters(self,
		                                                                     remoteWorkers.logRouters,
		                                                                     recoveryCount,
		                                                                     remoteLocality,
		                                                                     logSet->startVersion,
		                                                                     localities,
		                                                                     logSet->tLogPolicy,
		                                                                     true);
	}

	state std::vector<Future<TLogInterface>> logRouterInitializationReplies;
	const Version startVersion = oldLogSystem->logRouterTags == 0
	                                 ? oldLogSystem->recoverAt.get() + 1
	                                 : std::max(self->tLogs[0]->startVersion, logSet->startVersion);
	for (int i = 0; i < self->logRouterTags; i++) {
		InitializeLogRouterRequest req;
		req.recoveryCount = recoveryCount;
		req.routerTag = Tag(tagLocalityLogRouter, i);
		req.startVersion = startVersion;
		req.tLogLocalities = localities;
		req.tLogPolicy = logSet->tLogPolicy;
		req.locality = remoteLocality;
		TraceEvent("RemoteTLogRouterReplies", self->dbgid)
		    .detail("WorkerID", remoteWorkers.logRouters[i % remoteWorkers.logRouters.size()].id());
		logRouterInitializationReplies.push_back(transformErrors(
		    throwErrorOr(
		        remoteWorkers.logRouters[i % remoteWorkers.logRouters.size()].logRouter.getReplyUnlessFailedFor(
		            req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		    cluster_recovery_failed()));
	}

	std::vector<Tag> localTags = TagPartitionedLogSystem::getLocalTags(remoteLocality, allTags);
	LogSystemConfig oldLogSystemConfig = oldLogSystem->getLogSystemConfig();

	logSet->tLogLocalities.resize(remoteWorkers.remoteTLogs.size());
	logSet->logServers.resize(
	    remoteWorkers.remoteTLogs
	        .size()); // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
	logSet->updateLocalitySet(localities);

	state std::vector<Future<TLogInterface>> remoteTLogInitializationReplies;
	std::vector<InitializeTLogRequest> remoteTLogReqs(remoteWorkers.remoteTLogs.size());
	state std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> allRemoteTLogServers;

	if (oldLogSystem->logRouterTags == 0) {
		std::vector<int> locations;
		for (Tag tag : localTags) {
			locations.clear();
			logSet->getPushLocations(VectorRef<Tag>(&tag, 1), locations, 0);
			for (int loc : locations)
				remoteTLogReqs[loc].recoverTags.push_back(tag);
		}

		if (oldLogSystem->tLogs.size()) {
			int maxTxsTags = oldLogSystem->txsTags;
			for (auto& it : oldLogSystem->oldLogData) {
				maxTxsTags = std::max<int>(maxTxsTags, it.txsTags);
			}
			for (int i = 0; i < maxTxsTags; i++) {
				Tag tag = Tag(tagLocalityTxs, i);
				Tag pushTag = Tag(tagLocalityTxs, i % self->txsTags);
				locations.clear();
				logSet->getPushLocations(VectorRef<Tag>(&pushTag, 1), locations, 0);
				for (int loc : locations)
					remoteTLogReqs[loc].recoverTags.push_back(tag);
			}
		}
	}

	if (oldLogSystem->tLogs.size()) {
		for (int i = 0; i < self->txsTags; i++) {
			localTags.push_back(Tag(tagLocalityTxs, i));
		}
	}

	for (int i = 0; i < remoteWorkers.remoteTLogs.size(); i++) {
		InitializeTLogRequest& req = remoteTLogReqs[i];
		req.recruitmentID = self->recruitmentID;
		req.logVersion = configuration.tLogVersion;
		req.storeType = configuration.tLogDataStoreType;
		req.spillType = configuration.tLogSpillType;
		req.recoverFrom = oldLogSystemConfig;
		req.recoverAt = oldLogSystem->recoverAt.get();
		req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
		req.epoch = recoveryCount;
		req.remoteTag = Tag(tagLocalityRemoteLog, i);
		req.locality = remoteLocality;
		req.isPrimary = false;
		req.allTags = localTags;
		req.startVersion = logSet->startVersion;
		req.logRouterTags = 0;
		req.txsTags = self->txsTags;
		req.recoveryTransactionVersion = recoveryTransactionVersion;
		req.oldGenerationRecoverAtVersions = oldGenerationRecoverAtVersions;
	}

	remoteTLogInitializationReplies.reserve(remoteWorkers.remoteTLogs.size());
	for (int i = 0; i < remoteWorkers.remoteTLogs.size(); i++) {
		TraceEvent("RemoteTLogReplies", self->dbgid).detail("WorkerID", remoteWorkers.remoteTLogs[i].id());
		remoteTLogInitializationReplies.push_back(transformErrors(
		    throwErrorOr(remoteWorkers.remoteTLogs[i].tLog.getReplyUnlessFailedFor(
		        remoteTLogReqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		    cluster_recovery_failed()));
	}

	TraceEvent("RemoteLogRecruitment_InitializingRemoteLogs")
	    .detail("StartVersion", logSet->startVersion)
	    .detail("LocalStart", self->tLogs[0]->startVersion)
	    .detail("LogRouterTags", self->logRouterTags);
	wait(waitForAll(remoteTLogInitializationReplies) && waitForAll(logRouterInitializationReplies) &&
	     oldRouterRecruitment);

	for (int i = 0; i < logRouterInitializationReplies.size(); i++) {
		logSet->logRouters.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
		    OptionalInterface<TLogInterface>(logRouterInitializationReplies[i].get())));
	}

	for (int i = 0; i < remoteTLogInitializationReplies.size(); i++) {
		logSet->logServers[i] = makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
		    OptionalInterface<TLogInterface>(remoteTLogInitializationReplies[i].get()));
		logSet->tLogLocalities[i] = remoteWorkers.remoteTLogs[i].locality;
	}
	filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);

	std::vector<Future<Void>> recoveryComplete;
	recoveryComplete.reserve(logSet->logServers.size());
	for (int i = 0; i < logSet->logServers.size(); i++) {
		recoveryComplete.push_back(
		    transformErrors(throwErrorOr(logSet->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor(
		                        TLogRecoveryFinishedRequest(),
		                        SERVER_KNOBS->TLOG_TIMEOUT,
		                        SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    cluster_recovery_failed()));
		allRemoteTLogServers.push_back(logSet->logServers[i]);
	}

	self->remoteRecoveryComplete = waitForAll(recoveryComplete);
	self->remoteTrackTLogRecovery =
	    TagPartitionedLogSystem::trackTLogRecoveryActor(allRemoteTLogServers, self->remoteRecoveredVersion);
	self->tLogs.push_back(logSet);
	TraceEvent("RemoteLogRecruitment_CompletingRecovery").log();
	return Void();
}

ACTOR Future<Reference<ILogSystem>> TagPartitionedLogSystem::newEpoch(
    Reference<TagPartitionedLogSystem> oldLogSystem,
    RecruitFromConfigurationReply recr,
    Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
    DatabaseConfiguration configuration,
    LogEpoch recoveryCount,
    Version recoveryTransactionVersion,
    int8_t primaryLocality,
    int8_t remoteLocality,
    std::vector<Tag> allTags,
    Reference<AsyncVar<bool>> recruitmentStalled) {
	state double startTime = now();
	state Reference<TagPartitionedLogSystem> logSystem(
	    new TagPartitionedLogSystem(oldLogSystem->getDebugID(), oldLogSystem->locality, recoveryCount));
	logSystem->logSystemType = LogSystemType::tagPartitioned;
	logSystem->expectedLogSets = 1;
	logSystem->recoveredAt = oldLogSystem->recoverAt;
	logSystem->repopulateRegionAntiQuorum = configuration.repopulateRegionAntiQuorum;
	logSystem->recruitmentID = deterministicRandom()->randomUniqueID();
	logSystem->txsTags = configuration.tLogVersion >= TLogVersion::V4 ? recr.tLogs.size() : 0;
	oldLogSystem->recruitmentID = logSystem->recruitmentID;

	if (configuration.usableRegions > 1) {
		logSystem->logRouterTags =
		    recr.tLogs.size() *
		    std::max<int>(1, configuration.desiredLogRouterCount / std::max<int>(1, recr.tLogs.size()));
		logSystem->expectedLogSets++;
		logSystem->addPseudoLocality(tagLocalityLogRouterMapped);
		TraceEvent e("AddPseudoLocality", logSystem->getDebugID());
		e.detail("Locality1", "LogRouterMapped");
		if (configuration.backupWorkerEnabled) {
			logSystem->addPseudoLocality(tagLocalityBackup);
			e.detail("Locality2", "Backup");
		}
	} else if (configuration.backupWorkerEnabled) {
		// Single region uses log router tag for backup workers.
		logSystem->logRouterTags =
		    recr.tLogs.size() *
		    std::max<int>(1, configuration.desiredLogRouterCount / std::max<int>(1, recr.tLogs.size()));
		logSystem->addPseudoLocality(tagLocalityBackup);
		TraceEvent("AddPseudoLocality", logSystem->getDebugID()).detail("Locality", "Backup");
	}

	logSystem->tLogs.push_back(makeReference<LogSet>());
	logSystem->tLogs[0]->tLogVersion = configuration.tLogVersion;
	logSystem->tLogs[0]->tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
	logSystem->tLogs[0]->tLogReplicationFactor = configuration.tLogReplicationFactor;
	logSystem->tLogs[0]->tLogPolicy = configuration.tLogPolicy;
	logSystem->tLogs[0]->isLocal = true;
	logSystem->tLogs[0]->locality = primaryLocality;

	state RegionInfo region = configuration.getRegion(recr.dcId);

	state int maxTxsTags = oldLogSystem->txsTags;
	for (auto& it : oldLogSystem->oldLogData) {
		maxTxsTags = std::max<int>(maxTxsTags, it.txsTags);
	}

	if (region.satelliteTLogReplicationFactor > 0 && configuration.usableRegions > 1) {
		logSystem->tLogs.push_back(makeReference<LogSet>());
		if (recr.satelliteFallback) {
			logSystem->tLogs[1]->tLogWriteAntiQuorum = region.satelliteTLogWriteAntiQuorumFallback;
			logSystem->tLogs[1]->tLogReplicationFactor = region.satelliteTLogReplicationFactorFallback;
			logSystem->tLogs[1]->tLogPolicy = region.satelliteTLogPolicyFallback;
		} else {
			logSystem->tLogs[1]->tLogWriteAntiQuorum = region.satelliteTLogWriteAntiQuorum;
			logSystem->tLogs[1]->tLogReplicationFactor = region.satelliteTLogReplicationFactor;
			logSystem->tLogs[1]->tLogPolicy = region.satelliteTLogPolicy;
		}
		logSystem->tLogs[1]->isLocal = true;
		logSystem->tLogs[1]->locality = tagLocalitySatellite;
		logSystem->tLogs[1]->tLogVersion = configuration.tLogVersion;
		logSystem->tLogs[1]->startVersion = oldLogSystem->knownCommittedVersion + 1;

		logSystem->tLogs[1]->tLogLocalities.resize(recr.satelliteTLogs.size());
		for (int i = 0; i < recr.satelliteTLogs.size(); i++) {
			logSystem->tLogs[1]->tLogLocalities[i] = recr.satelliteTLogs[i].locality;
		}
		filterLocalityDataForPolicy(logSystem->tLogs[1]->tLogPolicy, &logSystem->tLogs[1]->tLogLocalities);

		logSystem->tLogs[1]->logServers.resize(
		    recr.satelliteTLogs
		        .size()); // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		logSystem->tLogs[1]->updateLocalitySet(logSystem->tLogs[1]->tLogLocalities);
		logSystem->tLogs[1]->populateSatelliteTagLocations(
		    logSystem->logRouterTags, oldLogSystem->logRouterTags, logSystem->txsTags, maxTxsTags);
		logSystem->expectedLogSets++;
	}

	if (oldLogSystem->tLogs.size()) {
		logSystem->oldLogData.emplace_back();
		logSystem->oldLogData[0].tLogs = oldLogSystem->tLogs;
		logSystem->oldLogData[0].epochBegin = oldLogSystem->tLogs[0]->startVersion;
		logSystem->oldLogData[0].epochEnd = oldLogSystem->knownCommittedVersion + 1;
		logSystem->oldLogData[0].recoverAt = oldLogSystem->recoverAt.get();
		logSystem->oldLogData[0].logRouterTags = oldLogSystem->logRouterTags;
		logSystem->oldLogData[0].txsTags = oldLogSystem->txsTags;
		logSystem->oldLogData[0].pseudoLocalities = oldLogSystem->pseudoLocalities;
		logSystem->oldLogData[0].epoch = oldLogSystem->epoch;
	}
	logSystem->oldLogData.insert(
	    logSystem->oldLogData.end(), oldLogSystem->oldLogData.begin(), oldLogSystem->oldLogData.end());

	logSystem->tLogs[0]->startVersion = oldLogSystem->knownCommittedVersion + 1;
	logSystem->backupStartVersion = oldLogSystem->knownCommittedVersion + 1;
	state int lockNum = 0;
	while (lockNum < oldLogSystem->lockResults.size()) {
		if (oldLogSystem->lockResults[lockNum].logSet->locality == primaryLocality) {
			if (oldLogSystem->lockResults[lockNum].isCurrent && oldLogSystem->lockResults[lockNum].logSet->isLocal) {
				break;
			}
			state Future<Void> stalledAfter = setAfter(recruitmentStalled, SERVER_KNOBS->MAX_RECOVERY_TIME, true);
			loop {
				auto versions =
				    TagPartitionedLogSystem::getDurableVersion(logSystem->dbgid, oldLogSystem->lockResults[lockNum]);
				if (versions.present()) {
					logSystem->tLogs[0]->startVersion =
					    std::min(std::min(std::get<0>(versions.get()) + 1, oldLogSystem->lockResults[lockNum].epochEnd),
					             logSystem->tLogs[0]->startVersion);
					break;
				}
				wait(TagPartitionedLogSystem::getDurableVersionChanged(oldLogSystem->lockResults[lockNum]));
			}
			stalledAfter.cancel();
			break;
		}
		lockNum++;
	}

	std::vector<LocalityData> localities;
	localities.resize(recr.tLogs.size());
	for (int i = 0; i < recr.tLogs.size(); i++) {
		localities[i] = recr.tLogs[i].locality;
	}

	state Future<Void> oldRouterRecruitment = Never();
	TraceEvent("NewEpochStartVersion", oldLogSystem->getDebugID())
	    .detail("StartVersion", logSystem->tLogs[0]->startVersion)
	    .detail("EpochEnd", oldLogSystem->knownCommittedVersion + 1)
	    .detail("Locality", primaryLocality)
	    .detail("OldLogRouterTags", oldLogSystem->logRouterTags);
	if (oldLogSystem->logRouterTags > 0 ||
	    logSystem->tLogs[0]->startVersion < oldLogSystem->knownCommittedVersion + 1) {
		// Use log routers to recover [knownCommittedVersion, recoveryVersion] from the old generation.
		oldRouterRecruitment = TagPartitionedLogSystem::recruitOldLogRouters(oldLogSystem.getPtr(),
		                                                                     recr.oldLogRouters,
		                                                                     recoveryCount,
		                                                                     primaryLocality,
		                                                                     logSystem->tLogs[0]->startVersion,
		                                                                     localities,
		                                                                     logSystem->tLogs[0]->tLogPolicy,
		                                                                     false);
		if (oldLogSystem->knownCommittedVersion - logSystem->tLogs[0]->startVersion >
		    SERVER_KNOBS->MAX_RECOVERY_VERSIONS) {
			// make sure we can recover in the other DC.
			for (auto& lockResult : oldLogSystem->lockResults) {
				if (lockResult.logSet->locality == remoteLocality) {
					if (TagPartitionedLogSystem::getDurableVersion(logSystem->dbgid, lockResult).present()) {
						recruitmentStalled->set(true);
					}
				}
			}
		}
	} else {
		oldLogSystem->logSystemConfigChanged.trigger();
	}

	std::vector<Tag> localTags = TagPartitionedLogSystem::getLocalTags(primaryLocality, allTags);
	state LogSystemConfig oldLogSystemConfig = oldLogSystem->getLogSystemConfig();

	state std::vector<Future<TLogInterface>> initializationReplies;
	std::vector<InitializeTLogRequest> reqs(recr.tLogs.size());

	logSystem->tLogs[0]->tLogLocalities.resize(recr.tLogs.size());
	logSystem->tLogs[0]->logServers.resize(
	    recr.tLogs.size()); // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
	logSystem->tLogs[0]->updateLocalitySet(localities);

	std::vector<int> locations;
	for (Tag tag : localTags) {
		locations.clear();
		logSystem->tLogs[0]->getPushLocations(VectorRef<Tag>(&tag, 1), locations, 0);
		for (int loc : locations)
			reqs[loc].recoverTags.push_back(tag);
	}
	for (int i = 0; i < oldLogSystem->logRouterTags; i++) {
		Tag tag = Tag(tagLocalityLogRouter, i);
		reqs[logSystem->tLogs[0]->bestLocationFor(tag)].recoverTags.push_back(tag);
	}

	if (oldLogSystem->tLogs.size()) {
		for (int i = 0; i < maxTxsTags; i++) {
			Tag tag = Tag(tagLocalityTxs, i);
			Tag pushTag = Tag(tagLocalityTxs, i % logSystem->txsTags);
			locations.clear();
			logSystem->tLogs[0]->getPushLocations(VectorRef<Tag>(&pushTag, 1), locations, 0);
			for (int loc : locations)
				reqs[loc].recoverTags.push_back(tag);
		}
		for (int i = 0; i < logSystem->txsTags; i++) {
			localTags.push_back(Tag(tagLocalityTxs, i));
		}
	}

	// Should be sorted by descending orders of versions.
	state std::vector<Version> oldGenerationRecoverAtVersions;
	for (const auto& oldLogGen : logSystem->oldLogData) {
		if (oldLogGen.recoverAt <= 0) {
			// When we have an invalid recover at value, it's possible that the previous generations' recover at is not
			// properly recorded in cstate. Therefore, we skip tracking old tlog generation recovery.
			oldGenerationRecoverAtVersions.clear();
			TraceEvent("DisableTrackingOldGenerationRecovery").log();
			break;
		}
		oldGenerationRecoverAtVersions.push_back(oldLogGen.recoverAt);
	}

	for (int i = 0; i < recr.tLogs.size(); i++) {
		InitializeTLogRequest& req = reqs[i];
		req.recruitmentID = logSystem->recruitmentID;
		req.logVersion = configuration.tLogVersion;
		req.storeType = configuration.tLogDataStoreType;
		req.spillType = configuration.tLogSpillType;
		req.recoverFrom = oldLogSystemConfig;
		req.recoverAt = oldLogSystem->recoverAt.get();
		req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
		req.epoch = recoveryCount;
		req.locality = primaryLocality;
		req.remoteTag = Tag(tagLocalityRemoteLog, i);
		req.isPrimary = true;
		req.allTags = localTags;
		req.startVersion = logSystem->tLogs[0]->startVersion;
		req.logRouterTags = logSystem->logRouterTags;
		req.txsTags = logSystem->txsTags;
		req.recoveryTransactionVersion = recoveryTransactionVersion;
		req.oldGenerationRecoverAtVersions = oldGenerationRecoverAtVersions;
	}

	initializationReplies.reserve(recr.tLogs.size());
	for (int i = 0; i < recr.tLogs.size(); i++) {
		TraceEvent("PrimaryTLogReplies", logSystem->getDebugID()).detail("WorkerID", recr.tLogs[i].id());
		initializationReplies.push_back(transformErrors(
		    throwErrorOr(recr.tLogs[i].tLog.getReplyUnlessFailedFor(
		        reqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		    cluster_recovery_failed()));
	}

	state std::vector<Future<Void>> recoveryComplete;
	state std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> allTLogServers;

	if (region.satelliteTLogReplicationFactor > 0 && configuration.usableRegions > 1) {
		state std::vector<Future<TLogInterface>> satelliteInitializationReplies;
		std::vector<InitializeTLogRequest> sreqs(recr.satelliteTLogs.size());
		std::vector<Tag> satelliteTags;

		if (logSystem->logRouterTags) {
			for (int i = 0; i < oldLogSystem->logRouterTags; i++) {
				Tag tag = Tag(tagLocalityLogRouter, i);
				// Satellite logs will index a mutation with tagLocalityLogRouter with an id greater than
				// the number of log routers as having an id mod the number of log routers.  We thus need
				// to make sure that if we're going from more log routers in the previous generation to
				// less log routers in the newer one, that we map the log router tags onto satellites that
				// are the preferred location for id%logRouterTags.
				Tag pushLocation = Tag(tagLocalityLogRouter, i % logSystem->logRouterTags);
				locations.clear();
				logSystem->tLogs[1]->getPushLocations(VectorRef<Tag>(&pushLocation, 1), locations, 0);
				for (int loc : locations)
					sreqs[loc].recoverTags.push_back(tag);
			}
		}
		if (oldLogSystem->tLogs.size()) {
			for (int i = 0; i < maxTxsTags; i++) {
				Tag tag = Tag(tagLocalityTxs, i);
				Tag pushTag = Tag(tagLocalityTxs, i % logSystem->txsTags);
				locations.clear();
				logSystem->tLogs[1]->getPushLocations(VectorRef<Tag>(&pushTag, 1), locations, 0);
				for (int loc : locations)
					sreqs[loc].recoverTags.push_back(tag);
			}
			for (int i = 0; i < logSystem->txsTags; i++) {
				satelliteTags.push_back(Tag(tagLocalityTxs, i));
			}
		}

		for (int i = 0; i < recr.satelliteTLogs.size(); i++) {
			InitializeTLogRequest& req = sreqs[i];
			req.recruitmentID = logSystem->recruitmentID;
			req.logVersion = configuration.tLogVersion;
			req.storeType = configuration.tLogDataStoreType;
			req.spillType = configuration.tLogSpillType;
			req.recoverFrom = oldLogSystemConfig;
			req.recoverAt = oldLogSystem->recoverAt.get();
			req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
			req.epoch = recoveryCount;
			req.locality = tagLocalitySatellite;
			req.remoteTag = Tag();
			req.isPrimary = true;
			req.allTags = satelliteTags;
			req.startVersion = oldLogSystem->knownCommittedVersion + 1;
			req.logRouterTags = logSystem->logRouterTags;
			req.txsTags = logSystem->txsTags;
			req.recoveryTransactionVersion = recoveryTransactionVersion;
			req.oldGenerationRecoverAtVersions = oldGenerationRecoverAtVersions;
		}

		satelliteInitializationReplies.reserve(recr.satelliteTLogs.size());
		for (int i = 0; i < recr.satelliteTLogs.size(); i++) {
			TraceEvent("PrimarySatelliteTLogReplies", logSystem->getDebugID())
			    .detail("WorkerID", recr.satelliteTLogs[i].id());
			satelliteInitializationReplies.push_back(transformErrors(
			    throwErrorOr(recr.satelliteTLogs[i].tLog.getReplyUnlessFailedFor(
			        sreqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    cluster_recovery_failed()));
		}

		wait(waitForAll(satelliteInitializationReplies) || oldRouterRecruitment);
		TraceEvent("PrimarySatelliteTLogInitializationComplete", logSystem->getDebugID()).log();

		for (int i = 0; i < satelliteInitializationReplies.size(); i++) {
			logSystem->tLogs[1]->logServers[i] = makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
			    OptionalInterface<TLogInterface>(satelliteInitializationReplies[i].get()));
		}

		for (int i = 0; i < logSystem->tLogs[1]->logServers.size(); i++) {
			recoveryComplete.push_back(transformErrors(
			    throwErrorOr(
			        logSystem->tLogs[1]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor(
			            TLogRecoveryFinishedRequest(),
			            SERVER_KNOBS->TLOG_TIMEOUT,
			            SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    cluster_recovery_failed()));
			allTLogServers.push_back(logSystem->tLogs[1]->logServers[i]);
		}
	}

	wait(waitForAll(initializationReplies) || oldRouterRecruitment);
	TraceEvent("PrimaryTLogInitializationComplete", logSystem->getDebugID()).log();

	for (int i = 0; i < initializationReplies.size(); i++) {
		logSystem->tLogs[0]->logServers[i] = makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
		    OptionalInterface<TLogInterface>(initializationReplies[i].get()));
		logSystem->tLogs[0]->tLogLocalities[i] = recr.tLogs[i].locality;
	}
	filterLocalityDataForPolicy(logSystem->tLogs[0]->tLogPolicy, &logSystem->tLogs[0]->tLogLocalities);

	// Don't force failure of recovery if it took us a long time to recover. This avoids multiple long running
	// recoveries causing tests to timeout
	if (BUGGIFY && now() - startTime < 300 && g_network->isSimulated() && g_simulator->speedUpSimulation)
		throw cluster_recovery_failed();

	for (int i = 0; i < logSystem->tLogs[0]->logServers.size(); i++) {
		recoveryComplete.push_back(transformErrors(
		    throwErrorOr(logSystem->tLogs[0]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor(
		        TLogRecoveryFinishedRequest(),
		        SERVER_KNOBS->TLOG_TIMEOUT,
		        SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		    cluster_recovery_failed()));
		allTLogServers.push_back(logSystem->tLogs[0]->logServers[i]);
	}
	logSystem->trackTLogRecovery =
	    TagPartitionedLogSystem::trackTLogRecoveryActor(allTLogServers, logSystem->recoveredVersion);
	logSystem->recoveryComplete = waitForAll(recoveryComplete);

	if (configuration.usableRegions > 1) {
		logSystem->hasRemoteServers = true;
		logSystem->remoteRecovery = TagPartitionedLogSystem::newRemoteEpoch(logSystem.getPtr(),
		                                                                    oldLogSystem,
		                                                                    fRemoteWorkers,
		                                                                    configuration,
		                                                                    recoveryCount,
		                                                                    recoveryTransactionVersion,
		                                                                    remoteLocality,
		                                                                    allTags,
		                                                                    oldGenerationRecoverAtVersions);
		if (oldLogSystem->tLogs.size() > 0 && oldLogSystem->tLogs[0]->locality == tagLocalitySpecial) {
			// The wait is required so that we know both primary logs and remote logs have copied the data between
			// the known committed version and the recovery version.
			// FIXME: we can remove this wait once we are able to have log routers which can ship data to the remote
			// logs without using log router tags.
			wait(logSystem->remoteRecovery);
		}
	} else {
		logSystem->hasRemoteServers = false;
		logSystem->remoteRecovery = logSystem->recoveryComplete;
		logSystem->remoteRecoveryComplete = logSystem->recoveryComplete;
		logSystem->remoteRecoveredVersion->set(MAX_VERSION);
	}

	return logSystem;
}

ACTOR Future<Void> TagPartitionedLogSystem::trackRejoins(
    UID dbgid,
    std::vector<std::pair<Reference<AsyncVar<OptionalInterface<TLogInterface>>>, Reference<IReplicationPolicy>>>
        logServers,
    FutureStream<struct TLogRejoinRequest> rejoinRequests) {
	state std::map<UID, ReplyPromise<TLogRejoinReply>> lastReply;
	state std::set<UID> logsWaiting;
	state double startTime = now();
	state Future<Void> warnTimeout = delay(SERVER_KNOBS->TLOG_SLOW_REJOIN_WARN_TIMEOUT_SECS);

	for (const auto& log : logServers) {
		logsWaiting.insert(log.first->get().id());
	}

	try {
		loop choose {
			when(TLogRejoinRequest req = waitNext(rejoinRequests)) {
				int pos = -1;
				for (int i = 0; i < logServers.size(); i++) {
					if (logServers[i].first->get().id() == req.myInterface.id()) {
						pos = i;
						logsWaiting.erase(logServers[i].first->get().id());
						break;
					}
				}
				if (pos != -1) {
					TraceEvent("TLogJoinedMe", dbgid)
					    .detail("TLog", req.myInterface.id())
					    .detail("Address", req.myInterface.commit.getEndpoint().getPrimaryAddress().toString());
					if (!logServers[pos].first->get().present() ||
					    req.myInterface.commit.getEndpoint() !=
					        logServers[pos].first->get().interf().commit.getEndpoint()) {
						TLogInterface interf = req.myInterface;
						filterLocalityDataForPolicyDcAndProcess(logServers[pos].second, &interf.filteredLocality);
						logServers[pos].first->setUnconditional(OptionalInterface<TLogInterface>(interf));
					}
					lastReply[req.myInterface.id()].send(TLogRejoinReply{ false });
					lastReply[req.myInterface.id()] = req.reply;
				} else {
					TraceEvent("TLogJoinedMeUnknown", dbgid)
					    .detail("TLog", req.myInterface.id())
					    .detail("Address", req.myInterface.commit.getEndpoint().getPrimaryAddress().toString());
					req.reply.send(true);
				}
			}
			when(wait(warnTimeout)) {
				for (const auto& logId : logsWaiting) {
					TraceEvent(SevWarnAlways, "TLogRejoinSlow", dbgid)
					    .detail("Elapsed", startTime - now())
					    .detail("LogId", logId);
				}
				warnTimeout = Never();
			}
		}
	} catch (...) {
		for (auto it = lastReply.begin(); it != lastReply.end(); ++it)
			it->second.send(TLogRejoinReply{ true });
		throw;
	}
}

ACTOR Future<TLogLockResult> TagPartitionedLogSystem::lockTLog(
    UID myID,
    Reference<AsyncVar<OptionalInterface<TLogInterface>>> tlog) {
	TraceEvent("TLogLockStarted", myID).detail("TLog", tlog->get().id()).detail("InfPresent", tlog->get().present());
	loop {
		choose {
			when(TLogLockResult data = wait(
			         tlog->get().present() ? brokenPromiseToNever(tlog->get().interf().lock.getReply<TLogLockResult>())
			                               : Never())) {
				TraceEvent("TLogLocked", myID).detail("TLog", tlog->get().id()).detail("End", data.end);
				return data;
			}
			when(wait(tlog->onChange())) {}
		}
	}
}

ACTOR Future<Void> TagPartitionedLogSystem::trackTLogRecoveryActor(
    std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> tlogs,
    Reference<AsyncVar<Version>> recoveredVersion) {
	if (!SERVER_KNOBS->TRACK_TLOG_RECOVERY) {
		return Never();
	}

	state std::vector<Future<TrackTLogRecoveryReply>> individualTLogRecovery;
	loop {
		individualTLogRecovery.clear();
		individualTLogRecovery.reserve(tlogs.size());
		TrackTLogRecoveryRequest req(recoveredVersion->get());
		for (int i = 0; i < tlogs.size(); ++i) {
			TraceEvent("WaitingForTLogRecovery")
			    .detail("Tlog", tlogs[i]->get().id())
			    .detail("PrevRecoveredVersion", recoveredVersion->get());
			individualTLogRecovery.push_back(transformErrors(
			    throwErrorOr(tlogs[i]->get().interf().trackRecovery.getReplyUnlessFailedFor(
			        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    cluster_recovery_failed()));
		}

		wait(waitForAll(individualTLogRecovery));

		Version currentRecoveredVersion = MAX_VERSION;
		for (int i = 0; i < individualTLogRecovery.size(); ++i) {
			currentRecoveredVersion =
			    std::min(individualTLogRecovery[i].get().oldestUnrecoveredStartVersion, currentRecoveredVersion);
		}

		TraceEvent("TLogRecoveredVersion").detail("RecoveredVersion", currentRecoveredVersion);
		recoveredVersion->set(currentRecoveredVersion);
	}
}
