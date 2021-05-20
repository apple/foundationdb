/*
 * TagPartitionedLogSystem.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TAG_PARTITIONED_LOG_SYSTEM_ACTOR_G_H)
#define FDBSERVER_TAG_PARTITIONED_LOG_SYSTEM_ACTOR_G_H
#include "fdbserver/TagPartitionedLogSystem.actor.g.h"
#elif !defined(FDBSERVER_TAG_PARTITIONED_LOG_SYSTEM_ACTOR_H)
#define FDBSERVER_TAG_PARTITIONED_LOG_SYSTEM_ACTOR_H

#include "flow/ActorCollection.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/DBCoreState.h"
#include "fdbserver/WaitFailure.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/LogProtocolMessage.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TagPartitionedLogSystem info in old epoch
struct OldLogData {
    std::vector<Reference<LogSet>> tLogs;
    int32_t logRouterTags;
    int32_t txsTags; // The number of txsTags, which may change across generations.
    Version epochBegin, epochEnd; // inclusive, exclusive
    std::set<int8_t> pseudoLocalities;
    LogEpoch epoch;

    OldLogData() : epochBegin(0), epochEnd(0), logRouterTags(0), txsTags(0), epoch(0) {}

    // Constructor for T of OldTLogConf and OldTLogCoreData
    template <class T>
    explicit OldLogData(const T& conf)
        : logRouterTags(conf.logRouterTags), txsTags(conf.txsTags), epochBegin(conf.epochBegin), epochEnd(conf.epochEnd),
          pseudoLocalities(conf.pseudoLocalities), epoch(conf.epoch) {
        tLogs.resize(conf.tLogs.size());
        for (int j = 0; j < conf.tLogs.size(); j++) {
            auto logSet = makeReference<LogSet>(conf.tLogs[j]);
            tLogs[j] = logSet;
        }
    }
};

struct LogLockInfo {
    Version epochEnd;
    bool isCurrent;
    Reference<LogSet> logSet;
    std::vector<Future<TLogLockResult>> replies;

    LogLockInfo() : epochEnd(std::numeric_limits<Version>::max()), isCurrent(false) {}
};

struct TagPartitionedLogSystem : ILogSystem, ReferenceCounted<TagPartitionedLogSystem> {
	const UID dbgid;
	LogSystemType logSystemType;
	std::vector<Reference<LogSet>> tLogs; // LogSets in different locations: primary, satellite, or remote
	int expectedLogSets;
	int logRouterTags;
	int txsTags;
	UID recruitmentID;
	int repopulateRegionAntiQuorum;
	bool stopped;
	std::set<int8_t> pseudoLocalities; // Represent special localities that will be mapped to tagLocalityLogRouter
	const LogEpoch epoch;
	LogEpoch oldestBackupEpoch;

	// new members
	std::map<Tag, Version> pseudoLocalityPopVersion;
	Future<Void> rejoins;
	Future<Void> recoveryComplete;
	Future<Void> remoteRecovery;
	Future<Void> remoteRecoveryComplete;
	std::vector<LogLockInfo> lockResults;
	AsyncVar<bool> recoveryCompleteWrittenToCoreState;
	bool remoteLogsWrittenToCoreState;
	bool hasRemoteServers;
	AsyncTrigger backupWorkerChanged;
	std::set<UID> removedBackupWorkers; // Workers that are removed before setting them.

	Optional<Version> recoverAt;
	Optional<Version> recoveredAt;
	Version knownCommittedVersion;
	Version backupStartVersion = invalidVersion; // max(tLogs[0].startVersion, previous epochEnd).
	LocalityData locality;
	// For each currently running popFromLog actor, outstandingPops is
	// (logID, tag)->(max popped version, durableKnownCommittedVersion).
	// Why do we need durableKnownCommittedVersion? knownCommittedVersion gives the lower bound of what data
	// will need to be copied into the next generation to restore the replication factor.
	// Guess: It probably serves as a minimum version of what data should be on a TLog in the next generation and
	// sending a pop for anything less than durableKnownCommittedVersion for the TLog will be absurd.
	std::map<std::pair<UID, Tag>, std::pair<Version, Version>> outstandingPops;

	Optional<PromiseStream<Future<Void>>> addActor;
	ActorCollection popActors;
	std::vector<OldLogData> oldLogData; // each element has the log info. in one old epoch.
	AsyncTrigger logSystemConfigChanged;

	TagPartitionedLogSystem(UID dbgid,
	                        LocalityData locality,
	                        LogEpoch e,
	                        Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>());

	void stopRejoins() final;

	void addref() final;

	void delref() final;

	std::string describe() const final;

	UID getDebugID() const final;

	void addPseudoLocality(int8_t locality);

	Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const final;

	bool hasPseudoLocality(int8_t locality) const final;

	// Return the min version of all pseudoLocalities, i.e., logRouter and backupTag
	Version popPseudoLocalityTag(Tag tag, Version upTo) final;

	static Future<Void> recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem,
	                                       UID const& dbgid,
	                                       DBCoreState const& oldState,
	                                       FutureStream<TLogRejoinRequest> const& rejoins,
	                                       LocalityData const& locality,
	                                       bool* forceRecovery);

	static Reference<ILogSystem> fromLogSystemConfig(UID const& dbgid,
	                                                 LocalityData const& locality,
	                                                 LogSystemConfig const& lsConf,
	                                                 bool excludeRemote,
	                                                 bool useRecoveredAt,
	                                                 Optional<PromiseStream<Future<Void>>> addActor);

	static Reference<ILogSystem> fromOldLogSystemConfig(UID const& dbgid,
	                                                    LocalityData const& locality,
	                                                    LogSystemConfig const& lsConf);

	// Convert TagPartitionedLogSystem to DBCoreState and override input newState as return value
	void toCoreState(DBCoreState& newState) final;

	bool remoteStorageRecovered() final;

	Future<Void> onCoreStateChanged() final;

	void coreStateWritten(DBCoreState const& newState) final;

	Future<Void> onError() final;

	ACTOR static Future<Void> onError_internal(TagPartitionedLogSystem* self) {
		// Never returns normally, but throws an error if the subsystem stops working
		loop {
			std::vector<Future<Void>> failed;
			std::vector<Future<Void>> backupFailed(1, Never());
			std::vector<Future<Void>> changes;

			for (auto& it : self->tLogs) {
				for (auto& t : it->logServers) {
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
				for (const auto& worker : it->backupWorkers) {
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
			wait(quorum(changes, 1) || tagError<Void>(quorum(failed, 1), master_tlog_failed()) ||
			     tagError<Void>(quorum(backupFailed, 1), master_backup_worker_failed()));
		}
	}

	ACTOR static Future<Void> pushResetChecker(Reference<ConnectionResetInfo> self, NetworkAddress addr) {
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

	ACTOR static Future<TLogCommitReply> recordPushMetrics(Reference<ConnectionResetInfo> self,
	                                                       NetworkAddress addr,
	                                                       Future<TLogCommitReply> in) {
		state double startTime = now();
		TLogCommitReply t = wait(in);
		if (now() - self->lastReset > SERVER_KNOBS->PUSH_RESET_INTERVAL) {
			if (now() - startTime > SERVER_KNOBS->PUSH_MAX_LATENCY) {
				if (self->resetCheck.isReady()) {
					self->resetCheck = pushResetChecker(self, addr);
				}
				self->slowReplies++;
			} else {
				self->fastReplies++;
			}
		}
		return t;
	}

	Future<Version> push(Version prevVersion,
	                     Version version,
	                     Version knownCommittedVersion,
	                     Version minKnownCommittedVersion,
	                     LogPushData& data,
	                     SpanID const& spanContext,
	                     Optional<UID> debugID) final;

	Reference<IPeekCursor> peekAll(UID dbgid, Version begin, Version end, Tag tag, bool parallelGetMore);

	Reference<IPeekCursor> peekRemote(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore);

	Reference<IPeekCursor> peek(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore) final;

	Reference<IPeekCursor> peek(UID dbgid,
	                            Version begin,
	                            Optional<Version> end,
	                            std::vector<Tag> tags,
	                            bool parallelGetMore) final;

	Reference<IPeekCursor> peekLocal(UID dbgid,
	                                 Tag tag,
	                                 Version begin,
	                                 Version end,
	                                 bool useMergePeekCursors,
	                                 int8_t peekLocality = tagLocalityInvalid);

	Reference<IPeekCursor> peekTxs(UID dbgid,
	                               Version begin,
	                               int8_t peekLocality,
	                               Version localEnd,
	                               bool canDiscardPopped) final;

	Reference<IPeekCursor> peekSingle(UID dbgid,
	                                  Version begin,
	                                  Tag tag,
	                                  std::vector<std::pair<Version, Tag>> history) final;

	// LogRouter or BackupWorker use this function to obtain a cursor for peeking tlogs of a generation (i.e., epoch).
	// Specifically, the epoch is determined by looking up "dbgid" in tlog sets of generations.
	// The returned cursor can peek data at the "tag" from the given "begin" version to that epoch's end version or
	// the recovery version for the latest old epoch. For the current epoch, the cursor has no end version.
	Reference<IPeekCursor> peekLogRouter(UID dbgid, Version begin, Tag tag) final;

	Version getKnownCommittedVersion() final;

	Future<Void> onKnownCommittedVersionChange() final;

	void popLogRouter(Version upTo,
	                  Tag tag,
	                  Version durableKnownCommittedVersion,
	                  int8_t popLocality);

	void popTxs(Version upTo, int8_t popLocality) final;

	// pop 'tag.locality' type data up to the 'upTo' version
	void pop(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality) final;

	// pop tag from log up to the version defined in self->outstandingPops[].first
	ACTOR static Future<Void> popFromLog(TagPartitionedLogSystem* self,
	                                     Reference<AsyncVar<OptionalInterface<TLogInterface>>> log,
	                                     Tag tag,
	                                     double time) {
		state Version last = 0;
		loop {
			wait(delay(time, TaskPriority::TLogPop));

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

	// Returns the popped version for the "tag" at the given transaction "log" server.
	ACTOR static Future<Version> getPoppedFromTLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> log, Tag tag) {
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

	// Returns the maximum popped transaction state store (txs) tag by querying
	// all generations of tlogs.
	ACTOR static Future<Version> getPoppedTxs(TagPartitionedLogSystem* self) {
		state std::vector<std::vector<Future<Version>>> poppedFutures;
		state std::vector<Future<Void>> poppedReady;
		if (self->tLogs.size()) {
			poppedFutures.push_back(std::vector<Future<Version>>());
			for (auto& it : self->tLogs) {
				for (auto& log : it->logServers) {
					poppedFutures.back().push_back(getPoppedFromTLog(
					    log, self->tLogs[0]->tLogVersion < TLogVersion::V4 ? txsTag : Tag(tagLocalityTxs, 0)));
				}
			}
			poppedReady.push_back(waitForAny(poppedFutures.back()));
		}

		for (auto& old : self->oldLogData) {
			if (old.tLogs.size()) {
				poppedFutures.push_back(std::vector<Future<Version>>());
				for (auto& it : old.tLogs) {
					for (auto& log : it->logServers) {
						poppedFutures.back().push_back(getPoppedFromTLog(
						    log, old.tLogs[0]->tLogVersion < TLogVersion::V4 ? txsTag : Tag(tagLocalityTxs, 0)));
					}
				}
				poppedReady.push_back(waitForAny(poppedFutures.back()));
			}
		}

		state Future<Void> maxGetPoppedDuration = delay(SERVER_KNOBS->TXS_POPPED_MAX_DELAY);
		wait(waitForAll(poppedReady) || maxGetPoppedDuration);

		if (maxGetPoppedDuration.isReady()) {
			TraceEvent(SevWarnAlways, "PoppedTxsNotReady", self->dbgid);
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

	Future<Version> getTxsPoppedVersion() final;

	ACTOR static Future<Void> confirmEpochLive_internal(Reference<LogSet> logSet, Optional<UID> debugID) {
		state vector<Future<Void>> alive;
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

	// Returns success after confirming that pushes in the current epoch are still possible
	Future<Void> confirmEpochLive(Optional<UID> debugID) final;

	Future<Void> endEpoch() final;

	// Call only after end_epoch() has successfully completed.  Returns a new epoch immediately following this one.
	// The new epoch is only provisional until the caller updates the coordinated DBCoreState.
	Future<Reference<ILogSystem>> newEpoch(RecruitFromConfigurationReply const& recr,
	                                       Future<RecruitRemoteFromConfigurationReply> const& fRemoteWorkers,
	                                       DatabaseConfiguration const& config,
	                                       LogEpoch recoveryCount,
	                                       int8_t primaryLocality,
	                                       int8_t remoteLocality,
	                                       std::vector<Tag> const& allTags,
	                                       Reference<AsyncVar<bool>> const& recruitmentStalled) final;

	LogSystemConfig getLogSystemConfig() const final;

	Standalone<StringRef> getLogsValue() const final;

	Future<Void> onLogSystemConfigChange() final;

	Version getEnd() const final;

	Version getPeekEnd() const;

	void getPushLocations(VectorRef<Tag> tags, std::vector<int>& locations, bool allLocations) const final;

	bool hasRemoteLogs() const final;

	Tag getRandomRouterTag() const final;

	Tag getRandomTxsTag() const final;

	TLogVersion getTLogVersion() const final;

	int getLogRouterTags() const final;

	Version getBackupStartVersion() const final;

	std::map<LogEpoch, ILogSystem::EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const final;

	inline Reference<LogSet> getEpochLogSet(LogEpoch epoch) const;

	void setBackupWorkers(const std::vector<InitializeBackupReply>& replies) final;

	bool removeBackupWorker(const BackupWorkerDoneRequest& req) final;

	LogEpoch getOldestBackupEpoch() const final;

	void setOldestBackupEpoch(LogEpoch epoch) final;

	ACTOR static Future<Void> monitorLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> logServer,
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

	// Get a durable version range from a log set:
	// [max of knownCommittedVersion, min of end version] from live TLogs.
	Optional<std::pair<Version, Version>> static getDurableVersion(
	    UID dbgid,
	    LogLockInfo lockInfo,
	    std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>(),
	    Optional<Version> lastEnd = Optional<Version>());

	ACTOR static Future<Void> getDurableVersionChanged(
	    LogLockInfo lockInfo,
	    std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>()) {
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

	ACTOR static Future<Void> epochEnd(Reference<AsyncVar<Reference<ILogSystem>>> outLogSystem,
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

		TEST(true); // Master recovery from pre-existing database

		// trackRejoins listens for rejoin requests from the tLogs that we are recovering from, to learn their
		// TLogInterfaces
		state std::vector<LogLockInfo> lockResults;
		state
		    std::vector<std::pair<Reference<AsyncVar<OptionalInterface<TLogInterface>>>, Reference<IReplicationPolicy>>>
		        allLogServers;
		state std::vector<Reference<LogSet>> logServers;
		state std::vector<OldLogData> oldLogData;
		state std::vector<std::vector<Reference<AsyncVar<bool>>>> logFailed;
		state std::vector<Future<Void>> failureTrackers;

		for (const CoreTLogSet& coreSet : prevState.tLogs) {
			logServers.push_back(makeReference<LogSet>(coreSet));
			std::vector<Reference<AsyncVar<bool>>> failed;

			for (const auto& logVar : logServers.back()->logServers) {
				allLogServers.push_back(std::make_pair(logVar, coreSet.tLogPolicy));
				failed.push_back(makeReference<AsyncVar<bool>>());
				failureTrackers.push_back(monitorLog(logVar, failed.back()));
			}
			logFailed.push_back(failed);
		}

		for (const auto& oldTlogData : prevState.oldTLogData) {
			oldLogData.emplace_back(oldTlogData);

			for (const auto& logSet : oldLogData.back().tLogs) {
				for (const auto& logVar : logSet->logServers) {
					allLogServers.push_back(std::make_pair(logVar, logSet->tLogPolicy));
				}
			}
		}
		state Future<Void> rejoins = trackRejoins(dbgid, allLogServers, rejoinRequests);

		lockResults.resize(logServers.size());
		std::set<int8_t> lockedLocalities;
		bool foundSpecial = false;
		for (int i = 0; i < logServers.size(); i++) {
			if (logServers[i]->locality == tagLocalitySpecial || logServers[i]->locality == tagLocalityUpgraded) {
				foundSpecial = true;
			}
			lockedLocalities.insert(logServers[i]->locality);
			lockResults[i].isCurrent = true;
			lockResults[i].logSet = logServers[i];
			for (int t = 0; t < logServers[i]->logServers.size(); t++) {
				lockResults[i].replies.push_back(lockTLog(dbgid, logServers[i]->logServers[t]));
			}
		}

		for (auto& old : oldLogData) {
			if (foundSpecial) {
				break;
			}
			for (auto& log : old.tLogs) {
				if (log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded) {
					foundSpecial = true;
					break;
				}
				if (!lockedLocalities.count(log->locality)) {
					TraceEvent("EpochEndLockExtra").detail("Locality", log->locality);
					TEST(true); // locking old generations for version information
					lockedLocalities.insert(log->locality);
					LogLockInfo lockResult;
					lockResult.epochEnd = old.epochEnd;
					lockResult.logSet = log;
					for (int t = 0; t < log->logServers.size(); t++) {
						lockResult.replies.push_back(lockTLog(dbgid, log->logServers[t]));
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
					lockResult.replies.push_back(lockTLog(dbgid, old.tLogs[0]->logServers[t]));
				}
				allLockResults.push_back(lockResult);
			}

			state int lockNum = 0;
			state Version maxRecoveryVersion = 0;
			state int maxRecoveryIndex = 0;
			while (lockNum < allLockResults.size()) {
				auto versions = TagPartitionedLogSystem::getDurableVersion(dbgid, allLockResults[lockNum]);
				if (versions.present()) {
					if (versions.get().second > maxRecoveryVersion) {
						TraceEvent("HigherRecoveryVersion", dbgid)
						    .detail("Idx", lockNum)
						    .detail("Ver", versions.get().second);
						maxRecoveryVersion = versions.get().second;
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
					failureTrackers.push_back(monitorLog(log, failed.back()));
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
			Version maxEnd = 0;
			std::vector<Future<Void>> changes;
			for (int log = 0; log < logServers.size(); log++) {
				if (!logServers[log]->isLocal) {
					continue;
				}
				auto versions =
				    TagPartitionedLogSystem::getDurableVersion(dbgid, lockResults[log], logFailed[log], lastEnd);
				if (versions.present()) {
					knownCommittedVersion = std::max(knownCommittedVersion, versions.get().first);
					maxEnd = std::max(maxEnd, versions.get().second);
					minEnd = std::min(minEnd, versions.get().second);
				}
				changes.push_back(TagPartitionedLogSystem::getDurableVersionChanged(lockResults[log], logFailed[log]));
			}

			if (maxEnd > 0 && (!lastEnd.present() || maxEnd < lastEnd.get())) {
				TEST(lastEnd.present()); // Restarting recovery at an earlier point

				auto logSystem = makeReference<TagPartitionedLogSystem>(dbgid, locality, prevState.recoveryCount);

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
				logSystem->recoverAt = minEnd;
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

	ACTOR static Future<Void> recruitOldLogRouters(TagPartitionedLogSystem* self,
	                                               vector<WorkerInterface> workers,
	                                               LogEpoch recoveryCount,
	                                               int8_t locality,
	                                               Version startVersion,
	                                               std::vector<LocalityData> tLogLocalities,
	                                               Reference<IReplicationPolicy> tLogPolicy,
	                                               bool forRemote) {
		state vector<vector<Future<TLogInterface>>> logRouterInitializationReplies;
		state vector<Future<TLogInterface>> allReplies;
		int nextRouter = 0;
		state Version lastStart = std::numeric_limits<Version>::max();

		if (!forRemote) {
			Version maxStart = getMaxLocalStartVersion(self->tLogs);

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
						auto reply = transformErrors(
						    throwErrorOr(workers[nextRouter].logRouter.getReplyUnlessFailedFor(
						        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
						    master_recovery_failed());
						logRouterInitializationReplies.back().push_back(reply);
						allReplies.push_back(reply);
						nextRouter = (nextRouter + 1) % workers.size();
					}
				}
			}
		}

		for (auto& old : self->oldLogData) {
			Version maxStart = getMaxLocalStartVersion(old.tLogs);

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
						auto reply = transformErrors(
						    throwErrorOr(workers[nextRouter].logRouter.getReplyUnlessFailedFor(
						        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
						    master_recovery_failed());
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
		vector<Future<Void>> failed;

		if (!forRemote) {
			Version maxStart = getMaxLocalStartVersion(self->tLogs);

			lastStart = std::max(startVersion, maxStart);
			for (auto& tLogs : self->tLogs) {
				if (tLogs->locality == locality) {
					for (int i = 0; i < logRouterInitializationReplies[nextReplies].size(); i++) {
						tLogs->logRouters.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
						    OptionalInterface<TLogInterface>(logRouterInitializationReplies[nextReplies][i].get())));
						failed.push_back(waitFailureClient(
						    logRouterInitializationReplies[nextReplies][i].get().waitFailure,
						    SERVER_KNOBS->TLOG_TIMEOUT,
						    -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
						    /*trace=*/true));
					}
					nextReplies++;
				}
			}
		}

		for (auto& old : self->oldLogData) {
			Version maxStart = getMaxLocalStartVersion(old.tLogs);
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
			wait(failed.size() ? tagError<Void>(quorum(failed, 1), master_tlog_failed()) : Future<Void>(Never()));
			throw internal_error();
		}
		return Void();
	}

	static Version getMaxLocalStartVersion(const std::vector<Reference<LogSet>>& tLogs);

	static std::vector<Tag> getLocalTags(int8_t locality, const std::vector<Tag>& allTags);

	ACTOR static Future<Void> newRemoteEpoch(TagPartitionedLogSystem* self,
	                                         Reference<TagPartitionedLogSystem> oldLogSystem,
	                                         Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
	                                         DatabaseConfiguration configuration,
	                                         LogEpoch recoveryCount,
	                                         int8_t remoteLocality,
	                                         std::vector<Tag> allTags) {
		TraceEvent("RemoteLogRecruitment_WaitingForWorkers");
		state RecruitRemoteFromConfigurationReply remoteWorkers = wait(fRemoteWorkers);

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
						    std::min(std::min(versions.get().first + 1, oldLogSystem->lockResults[lockNum].epochEnd),
						             logSet->startVersion);
						break;
					}
					wait(TagPartitionedLogSystem::getDurableVersionChanged(oldLogSystem->lockResults[lockNum]));
				}
				break;
			}
			lockNum++;
		}

		vector<LocalityData> localities;
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

		state vector<Future<TLogInterface>> logRouterInitializationReplies;
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
			logRouterInitializationReplies.push_back(transformErrors(
			    throwErrorOr(
			        remoteWorkers.logRouters[i % remoteWorkers.logRouters.size()].logRouter.getReplyUnlessFailedFor(
			            req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    master_recovery_failed()));
		}

		std::vector<Tag> localTags = getLocalTags(remoteLocality, allTags);
		LogSystemConfig oldLogSystemConfig = oldLogSystem->getLogSystemConfig();

		logSet->tLogLocalities.resize(remoteWorkers.remoteTLogs.size());
		logSet->logServers.resize(
		    remoteWorkers.remoteTLogs
		        .size()); // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		logSet->updateLocalitySet(localities);

		state vector<Future<TLogInterface>> remoteTLogInitializationReplies;
		vector<InitializeTLogRequest> remoteTLogReqs(remoteWorkers.remoteTLogs.size());

		bool nonShardedTxs = self->getTLogVersion() < TLogVersion::V4;
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
				bool needsOldTxs = oldLogSystem->tLogs[0]->tLogVersion < TLogVersion::V4;
				for (auto& it : oldLogSystem->oldLogData) {
					maxTxsTags = std::max<int>(maxTxsTags, it.txsTags);
					needsOldTxs = needsOldTxs || it.tLogs[0]->tLogVersion < TLogVersion::V4;
				}
				for (int i = needsOldTxs ? -1 : 0; i < maxTxsTags; i++) {
					Tag tag = i == -1 ? txsTag : Tag(tagLocalityTxs, i);
					Tag pushTag = (i == -1 || nonShardedTxs) ? txsTag : Tag(tagLocalityTxs, i % self->txsTags);
					locations.clear();
					logSet->getPushLocations(VectorRef<Tag>(&pushTag, 1), locations, 0);
					for (int loc : locations)
						remoteTLogReqs[loc].recoverTags.push_back(tag);
				}
			}
		}

		if (oldLogSystem->tLogs.size()) {
			if (nonShardedTxs) {
				localTags.push_back(txsTag);
			} else {
				for (int i = 0; i < self->txsTags; i++) {
					localTags.push_back(Tag(tagLocalityTxs, i));
				}
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
		}

		remoteTLogInitializationReplies.reserve(remoteWorkers.remoteTLogs.size());
		for (int i = 0; i < remoteWorkers.remoteTLogs.size(); i++)
			remoteTLogInitializationReplies.push_back(transformErrors(
			    throwErrorOr(remoteWorkers.remoteTLogs[i].tLog.getReplyUnlessFailedFor(
			        remoteTLogReqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    master_recovery_failed()));

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
		for (int i = 0; i < logSet->logServers.size(); i++)
			recoveryComplete.push_back(transformErrors(
			    throwErrorOr(logSet->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor(
			        TLogRecoveryFinishedRequest(),
			        SERVER_KNOBS->TLOG_TIMEOUT,
			        SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    master_recovery_failed()));

		self->remoteRecoveryComplete = waitForAll(recoveryComplete);
		self->tLogs.push_back(logSet);
		TraceEvent("RemoteLogRecruitment_CompletingRecovery");
		return Void();
	}

	ACTOR static Future<Reference<ILogSystem>> newEpoch(Reference<TagPartitionedLogSystem> oldLogSystem,
	                                                    RecruitFromConfigurationReply recr,
	                                                    Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
	                                                    DatabaseConfiguration configuration,
	                                                    LogEpoch recoveryCount,
	                                                    int8_t primaryLocality,
	                                                    int8_t remoteLocality,
	                                                    std::vector<Tag> allTags,
	                                                    Reference<AsyncVar<bool>> recruitmentStalled) {
		state double startTime = now();
		state Reference<TagPartitionedLogSystem> logSystem(
		    new TagPartitionedLogSystem(oldLogSystem->getDebugID(), oldLogSystem->locality, recoveryCount));
		// TODO: configure it to teamPartitioned for V7
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
		state bool needsOldTxs = oldLogSystem->tLogs.size() && oldLogSystem->getTLogVersion() < TLogVersion::V4;
		for (auto& it : oldLogSystem->oldLogData) {
			maxTxsTags = std::max<int>(maxTxsTags, it.txsTags);
			needsOldTxs = needsOldTxs || it.tLogs[0]->tLogVersion < TLogVersion::V4;
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
				if (oldLogSystem->lockResults[lockNum].isCurrent &&
				    oldLogSystem->lockResults[lockNum].logSet->isLocal) {
					break;
				}
				state Future<Void> stalledAfter = setAfter(recruitmentStalled, SERVER_KNOBS->MAX_RECOVERY_TIME, true);
				loop {
					auto versions = TagPartitionedLogSystem::getDurableVersion(logSystem->dbgid,
					                                                           oldLogSystem->lockResults[lockNum]);
					if (versions.present()) {
						logSystem->tLogs[0]->startVersion =
						    std::min(std::min(versions.get().first + 1, oldLogSystem->lockResults[lockNum].epochEnd),
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

		vector<LocalityData> localities;
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

		std::vector<Tag> localTags = getLocalTags(primaryLocality, allTags);
		state LogSystemConfig oldLogSystemConfig = oldLogSystem->getLogSystemConfig();

		state vector<Future<TLogInterface>> initializationReplies;
		vector<InitializeTLogRequest> reqs(recr.tLogs.size());

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
		bool nonShardedTxs = logSystem->getTLogVersion() < TLogVersion::V4;
		if (oldLogSystem->tLogs.size()) {
			for (int i = needsOldTxs ? -1 : 0; i < maxTxsTags; i++) {
				Tag tag = i == -1 ? txsTag : Tag(tagLocalityTxs, i);
				Tag pushTag = (i == -1 || nonShardedTxs) ? txsTag : Tag(tagLocalityTxs, i % logSystem->txsTags);
				locations.clear();
				logSystem->tLogs[0]->getPushLocations(VectorRef<Tag>(&pushTag, 1), locations, 0);
				for (int loc : locations)
					reqs[loc].recoverTags.push_back(tag);
			}
			if (nonShardedTxs) {
				localTags.push_back(txsTag);
			} else {
				for (int i = 0; i < logSystem->txsTags; i++) {
					localTags.push_back(Tag(tagLocalityTxs, i));
				}
			}
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
		}

		initializationReplies.reserve(recr.tLogs.size());
		for (int i = 0; i < recr.tLogs.size(); i++)
			initializationReplies.push_back(transformErrors(
			    throwErrorOr(recr.tLogs[i].tLog.getReplyUnlessFailedFor(
			        reqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    master_recovery_failed()));

		state std::vector<Future<Void>> recoveryComplete;

		if (region.satelliteTLogReplicationFactor > 0 && configuration.usableRegions > 1) {
			state vector<Future<TLogInterface>> satelliteInitializationReplies;
			vector<InitializeTLogRequest> sreqs(recr.satelliteTLogs.size());
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
				for (int i = needsOldTxs ? -1 : 0; i < maxTxsTags; i++) {
					Tag tag = i == -1 ? txsTag : Tag(tagLocalityTxs, i);
					Tag pushTag = (i == -1 || nonShardedTxs) ? txsTag : Tag(tagLocalityTxs, i % logSystem->txsTags);
					locations.clear();
					logSystem->tLogs[1]->getPushLocations(VectorRef<Tag>(&pushTag, 1), locations, 0);
					for (int loc : locations)
						sreqs[loc].recoverTags.push_back(tag);
				}
				if (nonShardedTxs) {
					satelliteTags.push_back(txsTag);
				} else {
					for (int i = 0; i < logSystem->txsTags; i++) {
						satelliteTags.push_back(Tag(tagLocalityTxs, i));
					}
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
			}

			satelliteInitializationReplies.reserve(recr.satelliteTLogs.size());
			for (int i = 0; i < recr.satelliteTLogs.size(); i++)
				satelliteInitializationReplies.push_back(transformErrors(
				    throwErrorOr(recr.satelliteTLogs[i].tLog.getReplyUnlessFailedFor(
				        sreqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
				    master_recovery_failed()));

			wait(waitForAll(satelliteInitializationReplies) || oldRouterRecruitment);

			for (int i = 0; i < satelliteInitializationReplies.size(); i++) {
				logSystem->tLogs[1]->logServers[i] = makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
				    OptionalInterface<TLogInterface>(satelliteInitializationReplies[i].get()));
			}

			for (int i = 0; i < logSystem->tLogs[1]->logServers.size(); i++)
				recoveryComplete.push_back(transformErrors(
				    throwErrorOr(
				        logSystem->tLogs[1]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor(
				            TLogRecoveryFinishedRequest(),
				            SERVER_KNOBS->TLOG_TIMEOUT,
				            SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
				    master_recovery_failed()));
		}

		wait(waitForAll(initializationReplies) || oldRouterRecruitment);

		for (int i = 0; i < initializationReplies.size(); i++) {
			logSystem->tLogs[0]->logServers[i] = makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
			    OptionalInterface<TLogInterface>(initializationReplies[i].get()));
			logSystem->tLogs[0]->tLogLocalities[i] = recr.tLogs[i].locality;
		}
		filterLocalityDataForPolicy(logSystem->tLogs[0]->tLogPolicy, &logSystem->tLogs[0]->tLogLocalities);

		// Don't force failure of recovery if it took us a long time to recover. This avoids multiple long running
		// recoveries causing tests to timeout
		if (BUGGIFY && now() - startTime < 300 && g_network->isSimulated() && g_simulator.speedUpSimulation)
			throw master_recovery_failed();

		for (int i = 0; i < logSystem->tLogs[0]->logServers.size(); i++)
			recoveryComplete.push_back(transformErrors(
			    throwErrorOr(
			        logSystem->tLogs[0]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor(
			            TLogRecoveryFinishedRequest(),
			            SERVER_KNOBS->TLOG_TIMEOUT,
			            SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    master_recovery_failed()));
		logSystem->recoveryComplete = waitForAll(recoveryComplete);

		if (configuration.usableRegions > 1) {
			logSystem->hasRemoteServers = true;
			logSystem->remoteRecovery = TagPartitionedLogSystem::newRemoteEpoch(logSystem.getPtr(),
			                                                                    oldLogSystem,
			                                                                    fRemoteWorkers,
			                                                                    configuration,
			                                                                    recoveryCount,
			                                                                    remoteLocality,
			                                                                    allTags);
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
		}

		return logSystem;
	}

	ACTOR static Future<Void> trackRejoins(
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

	ACTOR static Future<TLogLockResult> lockTLog(UID myID, Reference<AsyncVar<OptionalInterface<TLogInterface>>> tlog) {
		TraceEvent("TLogLockStarted", myID).detail("TLog", tlog->get().id());
		loop {
			choose {
				when(TLogLockResult data =
				         wait(tlog->get().present()
				                  ? brokenPromiseToNever(tlog->get().interf().lock.getReply<TLogLockResult>())
				                  : Never())) {
					TraceEvent("TLogLocked", myID).detail("TLog", tlog->get().id()).detail("End", data.end);
					return data;
				}
				when(wait(tlog->onChange())) {}
			}
		}
	}

	// FIXME: disabled during merge, update and use in epochEnd()
	/*
	static void lockMinimalTLogSet(const UID& dbgid, const DBCoreState& prevState,
	                               const std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>>& logServers,
	                               const std::vector<Reference<AsyncVar<bool>>>& logFailed,
	                               vector<Future<TLogLockResult>>* tLogReply ) {
	    // Invariant: tLogReply[i] must correspond to the tlog stored as logServers[i].
	    ASSERT(tLogReply->size() == prevState.tLogLocalities.size());
	    ASSERT(logFailed.size() == tLogReply->size());

	    // For any given index, only one of the following will be true.
	    auto locking_completed = [&logFailed, tLogReply](int index) {
	        const auto& entry = tLogReply->at(index);
	        return !logFailed[index]->get() && entry.isValid() && entry.isReady() && !entry.isError();
	    };
	    auto locking_failed = [&logFailed, tLogReply](int index) {
	        const auto& entry = tLogReply->at(index);
	        return logFailed[index]->get() || (entry.isValid() && entry.isReady() && entry.isError());
	    };
	    auto locking_pending = [&logFailed, tLogReply](int index) {
	        const auto& entry = tLogReply->at(index);
	        return !logFailed[index]->get() && (entry.isValid() && !entry.isReady());
	    };
	    auto locking_skipped = [&logFailed, tLogReply](int index) {
	        const auto& entry = tLogReply->at(index);
	        return !logFailed[index]->get() && !entry.isValid();
	    };

	    auto can_obtain_quorum = [&prevState](std::function<bool(int)> filter) {
	        LocalityGroup filter_true;
	        std::vector<LocalityData> filter_false, unused;
	        for (int i = 0; i < prevState.tLogLocalities.size() ; i++) {
	            if (filter(i)) {
	                filter_true.add(prevState.tLogLocalities[i]);
	            } else {
	                filter_false.push_back(prevState.tLogLocalities[i]);
	            }
	        }
	        bool valid = filter_true.validate(prevState.tLogPolicy);
	        if (!valid && prevState.tLogWriteAntiQuorum > 0 ) {
	            valid = !validateAllCombinations(unused, filter_true, prevState.tLogPolicy, filter_false,
	prevState.tLogWriteAntiQuorum, false);
	        }
	        return valid;
	    };

	    // Step 1: Verify that if all the failed TLogs come back, they can't form a quorum.
	    if (can_obtain_quorum(locking_failed)) {
	        TraceEvent(SevInfo, "MasterRecoveryTLogLockingImpossible", dbgid);
	        return;
	    }

	    // Step 2: It's possible for us to succeed, but we need to lock additional logs.
	    //
	    // First, we need an accurate picture of what TLogs we're capable of locking. We can't tell the
	    // difference between a temporarily failed TLog and a permanently failed TLog. Thus, we assume
	    // all failures are permanent, and manually re-issue lock requests if they rejoin.
	    for (int i = 0; i < logFailed.size(); i++) {
	        const auto& r = tLogReply->at(i);
	        TEST(locking_failed(i) && (r.isValid() && !r.isReady()));  // A TLog failed with a pending request.
	        // The reboot_a_tlog BUGGIFY below should cause the above case to be hit.
	        if (locking_failed(i)) {
	            tLogReply->at(i) = Future<TLogLockResult>();
	        }
	    }

	    // We're trying to paritition the set of old tlogs into two sets, L and R, such that:
	    // (1). R does not validate the policy
	    // (2). |R| is as large as possible
	    // (3). L contains all the already-locked TLogs
	    // and then we only issue lock requests to TLogs in L. This is safe, as R does not have quorum,
	    // so no commits may occur.  It does not matter if L forms a quorum or not.
	    //
	    // We form these sets by starting with L as all machines and R as the empty set, and moving a
	    // random machine from L to R until (1) or (2) no longer holds as true. Code-wise, L is
	    // [0..end-can_omit), and R is [end-can_omit..end), and we move a random machine via randomizing
	    // the order of the tlogs. Choosing a random machine was verified to generate a good-enough
	    // result to be interesting intests sufficiently frequently that we don't need to try to
	    // calculate the exact optimal solution.
	    std::vector<std::pair<LocalityData, int>> tlogs;
	    for (int i = 0; i < prevState.tLogLocalities.size(); i++) {
	        tlogs.emplace_back(prevState.tLogLocalities[i], i);
	    }
	    deterministicRandom()->randomShuffle(tlogs);
	    // Rearrange the array such that things that the left is logs closer to being locked, and
	    // the right is logs that can't be locked.  This makes us prefer locking already-locked TLogs,
	    // which is how we respect the decisions made in the previous execution.
	    auto idx_to_order = [&locking_completed, &locking_failed, &locking_pending, &locking_skipped](int index) {
	        bool complete = locking_completed(index);
	        bool pending = locking_pending(index);
	        bool skipped = locking_skipped(index);
	        bool failed = locking_failed(index);

	        ASSERT( complete + pending + skipped + failed == 1 );

	        if (complete) return 0;
	        if (pending) return 1;
	        if (skipped) return 2;
	        if (failed) return 3;

	        ASSERT(false);  // Programmer error.
	        return -1;
	    };
	    std::sort(tlogs.begin(), tlogs.end(),
	        // TODO: Change long type to `auto` once toolchain supports C++17.
	        [&idx_to_order](const std::pair<LocalityData, int>& lhs, const std::pair<LocalityData, int>& rhs) {
	            return idx_to_order(lhs.second) < idx_to_order(rhs.second);
	        });

	    // Indexes that aren't in the vector are the ones we're considering omitting. Remove indexes until
	    // the removed set forms a quorum.
	    int can_omit = 0;
	    std::vector<int> to_lock_indexes;
	    for (auto it = tlogs.cbegin() ; it != tlogs.cend() - 1 ; it++ ) {
	        to_lock_indexes.push_back(it->second);
	    }
	    auto filter = [&to_lock_indexes](int index) {
	        return std::find(to_lock_indexes.cbegin(), to_lock_indexes.cend(), index) == to_lock_indexes.cend();
	    };
	    while(true) {
	        if (can_obtain_quorum(filter)) {
	            break;
	        } else {
	            can_omit++;
	            ASSERT(can_omit < tlogs.size());
	            to_lock_indexes.pop_back();
	        }
	    }

	    if (prevState.tLogReplicationFactor - prevState.tLogWriteAntiQuorum == 1) {
	        ASSERT(can_omit == 0);
	    }
	    // Our previous check of making sure there aren't too many failed logs should have prevented this.
	    ASSERT(!locking_failed(tlogs[tlogs.size()-can_omit-1].second));

	    // If we've managed to leave more tlogs unlocked than (RF-AQ), it means we've hit the case
	    // where the policy engine has allowed us to have multiple logs in the same failure domain
	    // with independant sets of data. This case will validated that no code is relying on the old
	    // quorum=(RF-AQ) logic, and now goes through the policy engine instead.
	    TEST(can_omit >= prevState.tLogReplicationFactor - prevState.tLogWriteAntiQuorum);  // Locking a subset of the
	TLogs while ending an epoch. const bool reboot_a_tlog = g_network->now() - g_simulator.lastConnectionFailure >
	g_simulator.connectionFailuresDisableDuration && BUGGIFY && deterministicRandom()->random01() < 0.25;
	    TraceEvent(SevInfo, "MasterRecoveryTLogLocking", dbgid)
	        detail("Locks", tlogs.size() - can_omit)
	        detail("Skipped", can_omit)
	        detail("Replication", prevState.tLogReplicationFactor)
	        detail("Antiquorum", prevState.tLogWriteAntiQuorum)
	        detail("RebootBuggify", reboot_a_tlog);
	    for (int i = 0; i < tlogs.size() - can_omit; i++) {
	        const int index = tlogs[i].second;
	        Future<TLogLockResult>& entry = tLogReply->at(index);
	        if (!entry.isValid()) {
	            entry = lockTLog( dbgid, logServers[index] );
	        }
	    }
	    if (reboot_a_tlog) {
	        g_simulator.lastConnectionFailure = g_network->now();
	        for (int i = 0; i < tlogs.size() - can_omit; i++) {
	            const int index = tlogs[i].second;
	            if (logServers[index]->get().present()) {
	                g_simulator.rebootProcess(
	                    g_simulator.getProcessByAddress(
	                        logServers[index]->get().interf().address()),
	                    ISimulator::RebootProcess);
	                break;
	            }
	        }
	    }
	    // Intentionally leave `tlogs.size() - can_omit` .. `tlogs.size()` as !isValid() Futures.
	}*/

	template <class T>
	static vector<T> getReadyNonError(vector<Future<T>> const& futures);

	struct sort_by_end {
		bool operator()(TLogLockResult const& a, TLogLockResult const& b) const { return a.end < b.end; }
	};
};

#endif // FDBSERVER_TAG_PARTITIONED_LOG_SYSTEM_ACTOR_H