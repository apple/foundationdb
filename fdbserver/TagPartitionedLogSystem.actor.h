/*
 * TagPartitionedLogSystem.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TAGPARTITIONEDLOGSYSTEM_ACTOR_G_H)
#define FDBSERVER_TAGPARTITIONEDLOGSYSTEM_ACTOR_G_H
#include "fdbserver/TagPartitionedLogSystem.actor.g.h"
#elif !defined(FDBSERVER_TAGPARTITIONEDLOGSYSTEM_ACTOR_H)
#define FDBSERVER_TAGPARTITIONEDLOGSYSTEM_ACTOR_H

#pragma once

#include "fdbclient/SystemData.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/DBCoreState.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "flow/ActorCollection.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// TagPartitionedLogSystem info in old epoch
struct OldLogData {
	std::vector<Reference<LogSet>> tLogs;
	int32_t logRouterTags;
	int32_t txsTags; // The number of txsTags, which may change across generations.
	Version epochBegin, epochEnd;
	std::set<int8_t> pseudoLocalities;
	LogEpoch epoch;

	OldLogData() : logRouterTags(0), txsTags(0), epochBegin(0), epochEnd(0), epoch(0) {}

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

struct TagPartitionedLogSystem final : ILogSystem, ReferenceCounted<TagPartitionedLogSystem> {
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
	std::map<UID, Version> rvLogs; // recovery versions per tlog
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
	                        Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>())
	  : dbgid(dbgid), logSystemType(LogSystemType::empty), expectedLogSets(0), logRouterTags(0), txsTags(0),
	    repopulateRegionAntiQuorum(0), stopped(false), epoch(e), oldestBackupEpoch(0),
	    recoveryCompleteWrittenToCoreState(false), remoteLogsWrittenToCoreState(false), hasRemoteServers(false),
	    locality(locality), addActor(addActor), popActors(false) {}

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

	ACTOR static Future<Void> onError_internal(TagPartitionedLogSystem* self);

	ACTOR static Future<Void> pushResetChecker(Reference<ConnectionResetInfo> self, NetworkAddress addr);

	ACTOR static Future<TLogCommitReply> recordPushMetrics(Reference<ConnectionResetInfo> self,
	                                                       Reference<Histogram> dist,
	                                                       NetworkAddress addr,
	                                                       Future<TLogCommitReply> in);

	Future<Version> push(Version prevVersion,
	                     Version version,
	                     Version knownCommittedVersion,
	                     Version minKnownCommittedVersion,
	                     LogPushData& data,
	                     SpanID const& spanContext,
	                     Optional<UID> debugID,
	                     Optional<std::unordered_map<uint16_t, Version>> tpcvMap) final;

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

	void popLogRouter(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality);

	void popTxs(Version upTo, int8_t popLocality) final;

	// pop 'tag.locality' type data up to the 'upTo' version
	void pop(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality) final;

	// pop tag from log up to the version defined in self->outstandingPops[].first
	ACTOR static Future<Void> popFromLog(TagPartitionedLogSystem* self,
	                                     Reference<AsyncVar<OptionalInterface<TLogInterface>>> log,
	                                     Tag tag,
	                                     double time);

	ACTOR static Future<Version> getPoppedFromTLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> log, Tag tag);

	ACTOR static Future<Version> getPoppedTxs(TagPartitionedLogSystem* self);

	Future<Version> getTxsPoppedVersion() final;

	ACTOR static Future<Void> confirmEpochLive_internal(Reference<LogSet> logSet, Optional<UID> debugID);

	// Returns success after confirming that pushes in the current epoch are still possible
	Future<Void> confirmEpochLive(Optional<UID> debugID) final;

	Future<Void> endEpoch() final;

	// Call only after end_epoch() has successfully completed.  Returns a new epoch immediately following this one.
	// The new epoch is only provisional until the caller updates the coordinated DBCoreState.
	Future<Reference<ILogSystem>> newEpoch(RecruitFromConfigurationReply const& recr,
	                                       Future<RecruitRemoteFromConfigurationReply> const& fRemoteWorkers,
	                                       UID clusterId,
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
	                                     Reference<AsyncVar<bool>> failed);

	// returns the log group's knownComittedVersion, DV, and a vector of TLogLockResults for each tLog in the group.
	Optional<std::tuple<Version, Version, std::vector<TLogLockResult>>> static getDurableVersion(
	    UID dbgid,
	    LogLockInfo lockInfo,
	    std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>(),
	    Optional<Version> lastEnd = Optional<Version>());

	ACTOR static Future<Void> getDurableVersionChanged(
	    LogLockInfo lockInfo,
	    std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>());

	ACTOR static Future<Void> epochEnd(Reference<AsyncVar<Reference<ILogSystem>>> outLogSystem,
	                                   UID dbgid,
	                                   DBCoreState prevState,
	                                   FutureStream<TLogRejoinRequest> rejoinRequests,
	                                   LocalityData locality,
	                                   bool* forceRecovery);

	ACTOR static Future<Void> recruitOldLogRouters(TagPartitionedLogSystem* self,
	                                               std::vector<WorkerInterface> workers,
	                                               LogEpoch recoveryCount,
	                                               int8_t locality,
	                                               Version startVersion,
	                                               std::vector<LocalityData> tLogLocalities,
	                                               Reference<IReplicationPolicy> tLogPolicy,
	                                               bool forRemote);

	static Version getMaxLocalStartVersion(const std::vector<Reference<LogSet>>& tLogs);

	static std::vector<Tag> getLocalTags(int8_t locality, const std::vector<Tag>& allTags);

	ACTOR static Future<Void> newRemoteEpoch(TagPartitionedLogSystem* self,
	                                         Reference<TagPartitionedLogSystem> oldLogSystem,
	                                         Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
	                                         UID clusterId,
	                                         DatabaseConfiguration configuration,
	                                         LogEpoch recoveryCount,
	                                         int8_t remoteLocality,
	                                         std::vector<Tag> allTags);

	ACTOR static Future<Reference<ILogSystem>> newEpoch(Reference<TagPartitionedLogSystem> oldLogSystem,
	                                                    RecruitFromConfigurationReply recr,
	                                                    Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
	                                                    UID clusterId,
	                                                    DatabaseConfiguration configuration,
	                                                    LogEpoch recoveryCount,
	                                                    int8_t primaryLocality,
	                                                    int8_t remoteLocality,
	                                                    std::vector<Tag> allTags,
	                                                    Reference<AsyncVar<bool>> recruitmentStalled);

	ACTOR static Future<Void> trackRejoins(
	    UID dbgid,
	    std::vector<std::pair<Reference<AsyncVar<OptionalInterface<TLogInterface>>>, Reference<IReplicationPolicy>>>
	        logServers,
	    FutureStream<struct TLogRejoinRequest> rejoinRequests);

	ACTOR static Future<TLogLockResult> lockTLog(UID myID, Reference<AsyncVar<OptionalInterface<TLogInterface>>> tlog);

	template <class T>
	static std::vector<T> getReadyNonError(std::vector<Future<T>> const& futures);
};

template <class T>
std::vector<T> TagPartitionedLogSystem::getReadyNonError(std::vector<Future<T>> const& futures) {
	// Return the values of those futures which have (non-error) values ready
	std::vector<T> result;
	for (auto& f : futures)
		if (f.isReady() && !f.isError())
			result.push_back(f.get());
	return result;
}

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_TAGPARTITIONEDLOGSYSTEM_ACTOR_H
