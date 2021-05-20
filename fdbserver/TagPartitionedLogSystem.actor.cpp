/*
 * TagPartitionedLogSystem.actor.cpp
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

#include "fdbserver/TagPartitionedLogSystem.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Version> minVersionWhenReady(Future<Void> f, std::vector<Future<TLogCommitReply>> replies) {
    wait(f);
    Version minVersion = std::numeric_limits<Version>::max();
    for (auto& reply : replies) {
        if (reply.isReady() && !reply.isError()) {
            minVersion = std::min(minVersion, reply.get().version);
        }
    }
    return minVersion;
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
    : logRouterTags(oldLogData.logRouterTags), txsTags(oldLogData.txsTags), epochBegin(oldLogData.epochBegin),
      epochEnd(oldLogData.epochEnd), pseudoLocalities(oldLogData.pseudoLocalities), epoch(oldLogData.epoch) {
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
      epochEnd(oldData.epochEnd), pseudoLocalities(oldData.pseudoLocalities), epoch(oldData.epoch) {
    for (const Reference<LogSet>& logSet : oldData.tLogs) {
        if (logSet->logServers.size()) {
            tLogs.emplace_back(*logSet);
        }
    }
}

TagPartitionedLogSystem::TagPartitionedLogSystem(UID dbgid,
                                                 LocalityData locality,
                                                 LogEpoch e,
                                                 Optional<PromiseStream<Future<Void>>> addActor)
    : dbgid(dbgid), logSystemType(LogSystemType::empty), expectedLogSets(0), logRouterTags(0), txsTags(0),
      repopulateRegionAntiQuorum(0), epoch(e), oldestBackupEpoch(0), recoveryCompleteWrittenToCoreState(false),
      locality(locality), remoteLogsWrittenToCoreState(false), hasRemoteServers(false), stopped(false),
      addActor(addActor), popActors(false) {}
void TagPartitionedLogSystem::stopRejoins() { rejoins = Future<Void>(); }
void TagPartitionedLogSystem::addref() { ReferenceCounted<TagPartitionedLogSystem>::addref(); }
void TagPartitionedLogSystem::delref() { ReferenceCounted<TagPartitionedLogSystem>::delref(); }
std::string TagPartitionedLogSystem::describe() const {
    std::string result;
    for (int i = 0; i < tLogs.size(); i++) {
        result += format("%d: ", i);
        for (int j = 0; j < tLogs[i]->logServers.size(); j++) {
            result += tLogs[i]->logServers[j]->get().id().toString() +
                      ((j == tLogs[i]->logServers.size() - 1) ? " " : ", ");
        }
    }
    return result;
}

UID TagPartitionedLogSystem::getDebugID() const { return dbgid; }

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
            ASSERT(pseudoLocalities.count(tagLocalityLogRouterMapped) > 0);
            tag.locality = tagLocalityLogRouterMapped;
        }
        break;

    case ProcessClass::BackupClass:
        if (tag.locality == tagLocalityLogRouter) {
            ASSERT(pseudoLocalities.count(tagLocalityBackup) > 0);
            tag.locality = tagLocalityBackup;
        }
        break;

    default: // This should be an error at caller site.
        break;
    }
    return tag;
}

bool TagPartitionedLogSystem::hasPseudoLocality(int8_t locality) const { return pseudoLocalities.count(locality) > 0; }

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

Future<Void> TagPartitionedLogSystem::recoverAndEndEpoch(const Reference<AsyncVar<Reference<ILogSystem>>>& outLogSystem,
                                                         const UID& dbgid,
                                                         const DBCoreState& oldState,
                                                         const FutureStream<TLogRejoinRequest>& rejoins,
                                                         const LocalityData& locality,
                                                         bool* forceRecovery) {
    return epochEnd(outLogSystem, dbgid, oldState, rejoins, locality, forceRecovery);
}

Reference<ILogSystem> TagPartitionedLogSystem::fromLogSystemConfig(const UID& dbgid,
                                                                   const LocalityData& locality,
                                                                   const LogSystemConfig& lsConf,
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

Reference<ILogSystem> TagPartitionedLogSystem::fromOldLogSystemConfig(const UID& dbgid,
                                                                      const LocalityData& locality,
                                                                      const LogSystemConfig& lsConf) {
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

void TagPartitionedLogSystem::toCoreState(DBCoreState& newState) {
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
        (repopulateRegionAntiQuorum == 0 &&
         (!remoteRecoveryComplete.isValid() || !remoteRecoveryComplete.isReady())) ||
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

bool TagPartitionedLogSystem::remoteStorageRecovered() { return remoteRecoveryComplete.isValid() && remoteRecoveryComplete.isReady(); }

Future<Void> TagPartitionedLogSystem::onCoreStateChanged() {
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
    return waitForAny(changes);
}

void TagPartitionedLogSystem::coreStateWritten(const DBCoreState& newState) {
    if (!newState.oldTLogData.size()) {
        recoveryCompleteWrittenToCoreState.set(true);
    }
    for (auto& t : newState.tLogs) {
        if (!t.isLocal) {
            TraceEvent("RemoteLogsWritten", dbgid);
            remoteLogsWrittenToCoreState = true;
            break;
        }
    }
}

Future<Void> TagPartitionedLogSystem::onError() { return onError_internal(this); }

Future<Version> TagPartitionedLogSystem::push(Version prevVersion,
                                              Version version,
                                              Version knownCommittedVersion,
                                              Version minKnownCommittedVersion,
                                              LogPushData& data,
                                              const SpanID& spanContext,
                                              Optional<UID> debugID) {
    // FIXME: Randomize request order as in LegacyLogSystem?
    vector<Future<Void>> quorumResults;
    vector<Future<TLogCommitReply>> allReplies;
    int location = 0;
    Span span("TPLS:push"_loc, spanContext);
    for (auto& it : tLogs) {
        if (it->isLocal && it->logServers.size()) {
            if (it->connectionResetTrackers.size() == 0) {
                for (int i = 0; i < it->logServers.size(); i++) {
                    it->connectionResetTrackers.push_back(makeReference<ConnectionResetInfo>());
                }
            }
            vector<Future<Void>> tLogCommitResults;
            for (int loc = 0; loc < it->logServers.size(); loc++) {
                Standalone<StringRef> msg = data.getMessages(location);
                allReplies.push_back(recordPushMetrics(
                    it->connectionResetTrackers[loc],
                    it->logServers[loc]->get().interf().address(),
                    it->logServers[loc]->get().interf().commit.getReply(TLogCommitRequest(spanContext,
                                                                                          msg.arena(),
                                                                                          prevVersion,
                                                                                          version,
                                                                                          knownCommittedVersion,
                                                                                          minKnownCommittedVersion,
                                                                                          msg,
                                                                                          debugID),
                                                                        TaskPriority::ProxyTLogCommitReply)));
                Future<Void> commitSuccess = success(allReplies.back());
                addActor.get().send(commitSuccess);
                tLogCommitResults.push_back(commitSuccess);
                location++;
            }
            quorumResults.push_back(quorum(tLogCommitResults, tLogCommitResults.size() - it->tLogWriteAntiQuorum));
        }
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
        if (log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded) {
            foundSpecial = true;
        }
        if (log->isLocal && log->logServers.size() &&
            (log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded ||
             log->locality == tag.locality || tag == txsTag || tag.locality == tagLocalityTxs ||
             tag.locality == tagLocalityLogRouter ||
             ((tag.locality == tagLocalityUpgraded || tag == cacheTag) && log->locality != tagLocalitySatellite))) {
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
            cursors.push_back(makeReference<ILogSystem::SetPeekCursor>(localSets,
                                                                       bestSet,
                                                                       localSets[bestSet]->bestLocationFor(tag),
                                                                       tag,
                                                                       lastBegin,
                                                                       end,
                                                                       parallelGetMore));
        }
        for (int i = 0; begin < lastBegin; i++) {
            if (i == oldLogData.size()) {
                if (tag == txsTag || tag.locality == tagLocalityTxs || tag == cacheTag) {
                    break;
                }
                TraceEvent("TLogPeekAllDead", dbgid)
                    .detail("Tag", tag.toString())
                    .detail("Begin", begin)
                    .detail("End", end)
                    .detail("LastBegin", lastBegin)
                    .detail("OldLogDataSize", oldLogData.size());
                return makeReference<ILogSystem::ServerPeekCursor>(
                    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
                    tag,
                    begin,
                    getPeekEnd(),
                    false,
                    false);
            }

            int bestOldSet = 0;
            std::vector<Reference<LogSet>> localOldSets;
            Version thisBegin = begin;
            bool thisSpecial = false;
            for (auto& log : oldLogData[i].tLogs) {
                if (log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded) {
                    thisSpecial = true;
                }
                if (log->isLocal && log->logServers.size() &&
                    (log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded ||
                     log->locality == tag.locality || tag == txsTag || tag.locality == tagLocalityTxs ||
                     tag.locality == tagLocalityLogRouter ||
                     ((tag.locality == tagLocalityUpgraded || tag == cacheTag) &&
                      log->locality != tagLocalitySatellite))) {
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
                    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
                    tag,
                    begin,
                    getPeekEnd(),
                    false,
                    false);
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
        return makeReference<ILogSystem::ServerPeekCursor>(Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
                                                           tag,
                                                           begin,
                                                           getPeekEnd(),
                                                           false,
                                                           parallelGetMore);
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
        cursors.push_back(makeReference<ILogSystem::BufferedCursor>(tLogs[bestSet]->logRouters,
                                                                    tag,
                                                                    lastBegin,
                                                                    end.present() ? end.get() + 1 : getPeekEnd(),
                                                                    parallelGetMore));
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
        return peekAll(dbgid, begin, getPeekEnd(), tag, parallelGetMore);
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
    return makeReference<ILogSystem::BufferedCursor>(cursors,
                                                     begin,
                                                     end.present() ? end.get() + 1 : getPeekEnd(),
                                                     true,
                                                     tLogs[0]->locality == tagLocalityUpgraded,
                                                     false);
}

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekLocal(UID dbgid,
                                                          Tag tag,
                                                          Version begin,
                                                          Version end,
                                                          bool useMergePeekCursors,
                                                          int8_t peekLocality) {
    if (tag.locality >= 0 || tag.locality == tagLocalityUpgraded || tag.locality == tagLocalitySpecial) {
        peekLocality = tag.locality;
    }
    ASSERT(peekLocality >= 0 || peekLocality == tagLocalityUpgraded || tag.locality == tagLocalitySpecial);

    int bestSet = -1;
    bool foundSpecial = false;
    int logCount = 0;
    for (int t = 0; t < tLogs.size(); t++) {
        if (tLogs[t]->logServers.size() && tLogs[t]->locality != tagLocalitySatellite) {
            logCount++;
        }
        if (tLogs[t]->logServers.size() &&
            (tLogs[t]->locality == tagLocalitySpecial || tLogs[t]->locality == tagLocalityUpgraded ||
             tLogs[t]->locality == peekLocality || peekLocality == tagLocalityUpgraded ||
             peekLocality == tagLocalitySpecial)) {
            if (tLogs[t]->locality == tagLocalitySpecial || tLogs[t]->locality == tagLocalityUpgraded) {
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
            return makeReference<ILogSystem::MergedPeekCursor>(tLogs[bestSet]->logServers,
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
                cursors.push_back(makeReference<ILogSystem::MergedPeekCursor>(
                    tLogs[bestSet]->logServers,
                    tLogs[bestSet]->bestLocationFor(tag),
                    tLogs[bestSet]->logServers.size() + 1 - tLogs[bestSet]->tLogReplicationFactor,
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
                if ((tag == txsTag || tag.locality == tagLocalityTxs) && cursors.size()) {
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
                     oldLogData[i].tLogs[t]->locality == tagLocalityUpgraded ||
                     oldLogData[i].tLogs[t]->locality == peekLocality || peekLocality == tagLocalityUpgraded ||
                     peekLocality == tagLocalitySpecial)) {
                    if (oldLogData[i].tLogs[t]->locality == tagLocalitySpecial ||
                        oldLogData[i].tLogs[t]->locality == tagLocalityUpgraded) {
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
                    cursors.push_back(makeReference<ILogSystem::MergedPeekCursor>(
                        oldLogData[i].tLogs[bestOldSet]->logServers,
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
        TraceEvent("TLogPeekTxsNoLogs", dbgid);
        return makeReference<ILogSystem::ServerPeekCursor>(
            Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), txsTag, begin, end, false, false);
    }
    TraceEvent("TLogPeekTxs", dbgid)
        .detail("Begin", begin)
        .detail("End", end)
        .detail("LocalEnd", localEnd)
        .detail("PeekLocality", peekLocality)
        .detail("CanDiscardPopped", canDiscardPopped);

    int maxTxsTags = txsTags;
    bool needsOldTxs = tLogs[0]->tLogVersion < TLogVersion::V4;
    for (auto& it : oldLogData) {
        maxTxsTags = std::max<int>(maxTxsTags, it.txsTags);
        needsOldTxs = needsOldTxs || it.tLogs[0]->tLogVersion < TLogVersion::V4;
    }

    if (peekLocality < 0 || localEnd == invalidVersion || localEnd <= begin) {
        std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
        cursors.reserve(maxTxsTags);
        for (int i = 0; i < maxTxsTags; i++) {
            cursors.push_back(peekAll(dbgid, begin, end, Tag(tagLocalityTxs, i), true));
        }
        // SOMEDAY: remove once upgrades from 6.2 are no longer supported
        if (needsOldTxs) {
            cursors.push_back(peekAll(dbgid, begin, end, txsTag, true));
        }

        return makeReference<ILogSystem::BufferedCursor>(cursors, begin, end, false, false, canDiscardPopped);
    }

    try {
        if (localEnd >= end) {
            std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
            cursors.reserve(maxTxsTags);
            for (int i = 0; i < maxTxsTags; i++) {
                cursors.push_back(peekLocal(dbgid, Tag(tagLocalityTxs, i), begin, end, true, peekLocality));
            }
            // SOMEDAY: remove once upgrades from 6.2 are no longer supported
            if (needsOldTxs) {
                cursors.push_back(peekLocal(dbgid, txsTag, begin, end, true, peekLocality));
            }

            return makeReference<ILogSystem::BufferedCursor>(cursors, begin, end, false, false, canDiscardPopped);
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
        // SOMEDAY: remove once upgrades from 6.2 are no longer supported
        if (needsOldTxs) {
            localCursors.push_back(peekLocal(dbgid, txsTag, begin, localEnd, true, peekLocality));
            allCursors.push_back(peekAll(dbgid, localEnd, end, txsTag, true));
        }

        cursors[1] = makeReference<ILogSystem::BufferedCursor>(
            localCursors, begin, localEnd, false, false, canDiscardPopped);
        cursors[0] = makeReference<ILogSystem::BufferedCursor>(allCursors, localEnd, end, false, false, false);
        epochEnds.emplace_back(localEnd);

        return makeReference<ILogSystem::MultiCursor>(cursors, epochEnds);
    } catch (Error& e) {
        if (e.code() == error_code_worker_removed) {
            std::vector<Reference<ILogSystem::IPeekCursor>> cursors;
            cursors.reserve(maxTxsTags);
            for (int i = 0; i < maxTxsTags; i++) {
                cursors.push_back(peekAll(dbgid, begin, end, Tag(tagLocalityTxs, i), true));
            }
            // SOMEDAY: remove once upgrades from 6.2 are no longer supported
            if (needsOldTxs) {
                cursors.push_back(peekAll(dbgid, begin, end, txsTag, true));
            }

            return makeReference<ILogSystem::BufferedCursor>(cursors, begin, end, false, false, canDiscardPopped);
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

        TraceEvent("TLogPeekSingleAddingLocal", dbgid)
            .detail("Tag", tag.toString())
            .detail("Begin", history[0].first);
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

Reference<ILogSystem::IPeekCursor> TagPartitionedLogSystem::peekLogRouter(UID dbgid, Version begin, Tag tag) {
    bool found = false;
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
            if (SERVER_KNOBS->LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED && bestSatelliteSet != -1 &&
                tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
                bestSet = bestSatelliteSet;
            }

            TraceEvent("TLogPeekLogRouterSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
            // FIXME: do this merge on one of the logs in the other data center to avoid sending multiple copies
            // across the WAN
            return makeReference<ILogSystem::SetPeekCursor>(
                localSets, bestSet, localSets[bestSet]->bestLocationFor(tag), tag, begin, getPeekEnd(), true);
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
            if (SERVER_KNOBS->LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED && bestSatelliteSet != -1 &&
                tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
                bestSet = bestSatelliteSet;
            }
            const auto& log = tLogs[bestSet];
            TraceEvent("TLogPeekLogRouterBestOnly", dbgid)
                .detail("Tag", tag.toString())
                .detail("Begin", begin)
                .detail("LogId", log->logServers[log->bestLocationFor(tag)]->get().id());
            return makeReference<ILogSystem::ServerPeekCursor>(
                log->logServers[log->bestLocationFor(tag)], tag, begin, getPeekEnd(), false, true);
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
            if (SERVER_KNOBS->LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED && bestSatelliteSet != -1 &&
                old.tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
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
            return makeReference<ILogSystem::SetPeekCursor>(
                localSets,
                bestSet,
                localSets[bestSet]->bestLocationFor(tag),
                tag,
                begin,
                firstOld && recoveredAt.present() ? recoveredAt.get() + 1 : old.epochEnd,
                true);
        }
        firstOld = false;
    }
    return makeReference<ILogSystem::ServerPeekCursor>(
        Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false);
}

Version TagPartitionedLogSystem::getKnownCommittedVersion() {
    Version result = invalidVersion;
    for (auto& it : lockResults) {
        auto versions = TagPartitionedLogSystem::getDurableVersion(dbgid, it);
        if (versions.present()) {
            result = std::max(result, versions.get().first);
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

void TagPartitionedLogSystem::popLogRouter(
    Version upTo,
    Tag tag,
    Version durableKnownCommittedVersion,
    int8_t popLocality) { // FIXME: do not need to pop all generations of old logs
    if (!upTo)
        return;
    for (auto& t : tLogs) {
        if (t->locality == popLocality) {
            for (auto& log : t->logRouters) {
                Version prev = outstandingPops[std::make_pair(log->get().id(), tag)].first;
                if (prev < upTo)
                    outstandingPops[std::make_pair(log->get().id(), tag)] =
                        std::make_pair(upTo, durableKnownCommittedVersion);
                if (prev == 0) {
                    popActors.add(popFromLog(
                        this, log, tag, 0.0)); // Fast pop time because log routers can only hold 5 seconds of data.
                }
            }
        }
    }

    for (auto& old : oldLogData) {
        for (auto& t : old.tLogs) {
            if (t->locality == popLocality) {
                for (auto& log : t->logRouters) {
                    Version prev = outstandingPops[std::make_pair(log->get().id(), tag)].first;
                    if (prev < upTo)
                        outstandingPops[std::make_pair(log->get().id(), tag)] =
                            std::make_pair(upTo, durableKnownCommittedVersion);
                    if (prev == 0)
                        popActors.add(popFromLog(this, log, tag, 0.0));
                }
            }
        }
    }
}

void TagPartitionedLogSystem::popTxs(Version upTo, int8_t popLocality) {
    if (getTLogVersion() < TLogVersion::V4) {
        pop(upTo, txsTag, 0, popLocality);
    } else {
        for (int i = 0; i < txsTags; i++) {
            pop(upTo, Tag(tagLocalityTxs, i), 0, popLocality);
        }
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
            tag.locality == tagLocalityUpgraded ||
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
                    popActors.add(popFromLog(this, log, tag, 1.0)); //< FIXME: knob
                }
            }
        }
    }
}

Future<Version> TagPartitionedLogSystem::getTxsPoppedVersion() { return getPoppedTxs(this); }

Future<Void> TagPartitionedLogSystem::confirmEpochLive(Optional<UID> debugID) {
    vector<Future<Void>> quorumResults;
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
    const RecruitFromConfigurationReply& recr,
    const Future<RecruitRemoteFromConfigurationReply>& fRemoteWorkers,
    const DatabaseConfiguration& config,
    LogEpoch recoveryCount,
    int8_t primaryLocality,
    int8_t remoteLocality,
    const vector<Tag>& allTags,
    const Reference<AsyncVar<bool>>& recruitmentStalled) {
    return newEpoch(Reference<TagPartitionedLogSystem>::addRef(this),
                    recr,
                    fRemoteWorkers,
                    config,
                    recoveryCount,
                    primaryLocality,
                    remoteLocality,
                    allTags,
                    recruitmentStalled);
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
    vector<std::pair<UID, NetworkAddress>> logs;
    vector<std::pair<UID, NetworkAddress>> oldLogs;
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
                                         t->logServers[j]->get().present()
                                         ? t->logServers[j]->get().interf().address()
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

void TagPartitionedLogSystem::getPushLocations(VectorRef<Tag> tags, vector<int>& locations, bool allLocations) const {
    int locationOffset = 0;
    for (auto& log : tLogs) {
        if (log->isLocal && log->logServers.size()) {
            log->getPushLocations(tags, locations, locationOffset, allLocations);
            locationOffset += log->logServers.size();
        }
    }
}

bool TagPartitionedLogSystem::hasRemoteLogs() const { return logRouterTags > 0 || pseudoLocalities.size() > 0; }

Tag TagPartitionedLogSystem::getRandomRouterTag() const {
    return Tag(tagLocalityLogRouter, deterministicRandom()->randomInt(0, logRouterTags));
}

Tag TagPartitionedLogSystem::getRandomTxsTag() const { return Tag(tagLocalityTxs, deterministicRandom()->randomInt(0, txsTags)); }

TLogVersion TagPartitionedLogSystem::getTLogVersion() const { return tLogs[0]->tLogVersion; }

int TagPartitionedLogSystem::getLogRouterTags() const { return logRouterTags; }

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

Reference<LogSet> TagPartitionedLogSystem::getEpochLogSet(LogEpoch epoch) const {
    for (const auto& old : oldLogData) {
        if (epoch == old.epoch)
            return old.tLogs[0];
    }
    return Reference<LogSet>(nullptr);
}

void TagPartitionedLogSystem::setBackupWorkers(const vector<InitializeBackupReply>& replies) {
    ASSERT(tLogs.size() > 0);

    Reference<LogSet> logset = tLogs[0]; // Master recruits this epoch's worker first.
    LogEpoch logsetEpoch = this->epoch;
    oldestBackupEpoch = this->epoch;
    for (const auto& reply : replies) {
        if (removedBackupWorkers.count(reply.interf.id()) > 0) {
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
        TraceEvent("AddBackupWorker", dbgid)
            .detail("Epoch", logsetEpoch)
            .detail("BackupWorkerID", reply.interf.id());
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

LogEpoch TagPartitionedLogSystem::getOldestBackupEpoch() const { return oldestBackupEpoch; }

void TagPartitionedLogSystem::setOldestBackupEpoch(LogEpoch epoch) {
    oldestBackupEpoch = epoch;
    backupWorkerChanged.trigger();
}

Optional<std::pair<Version, Version>> TagPartitionedLogSystem::getDurableVersion(
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
        if (lockInfo.replies[t].isReady() && !lockInfo.replies[t].isError() &&
            (!failed.size() || !failed[t]->get())) {
            results.push_back(lockInfo.replies[t].get());
            availableItems.push_back(logSet->tLogLocalities[t]);
            sServerState += 'a';
        } else {
            unResponsiveSet.add(logSet->tLogLocalities[t]);
            sServerState += 'f';
        }
    }

    // Check if the list of results is not larger than the anti quorum
    bool bTooManyFailures = (results.size() <= logSet->tLogWriteAntiQuorum);

    // Check if failed logs complete the policy
    bTooManyFailures = bTooManyFailures || ((unResponsiveSet.size() >= logSet->tLogReplicationFactor) &&
                                            (unResponsiveSet.validate(logSet->tLogPolicy)));

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
        std::sort(results.begin(), results.end(), sort_by_end());
        int absent = logSet->logServers.size() - results.size();
        int safe_range_begin = logSet->tLogWriteAntiQuorum;
        int new_safe_range_begin = std::min(logSet->tLogWriteAntiQuorum, (int)(results.size() - 1));
        int safe_range_end = logSet->tLogReplicationFactor - absent;

        if (!lastEnd.present() || ((safe_range_end > 0) && (safe_range_end - 1 < results.size()) &&
                                   results[safe_range_end - 1].end < lastEnd.get())) {
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

            return std::make_pair(knownCommittedVersion, results[new_safe_range_begin].end);
        }
    }
    TraceEvent("GetDurableResultWaiting", dbgid)
        .detail("Required", requiredCount)
        .detail("Present", results.size())
        .detail("ServerState", sServerState);
    return Optional<std::pair<Version, Version>>();
}

Version TagPartitionedLogSystem::getMaxLocalStartVersion(const vector<Reference<LogSet>>& tLogs) {
    Version maxStart = 0;
    for (const auto& logSet : tLogs) {
        if (logSet->isLocal) {
            maxStart = std::max(maxStart, logSet->startVersion);
        }
    }
    return maxStart;
}

std::vector<Tag> TagPartitionedLogSystem::getLocalTags(int8_t locality, const vector<Tag>& allTags) {
    std::vector<Tag> localTags;
    for (const auto& tag : allTags) {
        if (locality == tagLocalitySpecial || locality == tag.locality || tag.locality < 0) {
            localTags.push_back(tag);
        }
    }
    return localTags;
}

template <class T>
vector<T> TagPartitionedLogSystem::getReadyNonError(const vector<Future<T>>& futures) {
    // Return the values of those futures which have (non-error) values ready
    std::vector<T> result;
    for (auto& f : futures)
        if (f.isReady() && !f.isError())
            result.push_back(f.get());
    return result;
}
