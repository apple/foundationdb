/*
 * LogSystem.h
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

#ifndef FDBSERVER_LOGSYSTEM_LOGSYSTEM_H
#define FDBSERVER_LOGSYSTEM_LOGSYSTEM_H

#pragma once

#include <algorithm>
#include <cstdint>
#include <set>
#include <unordered_set>
#include <vector>

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/core/BackupProgressTypes.h"
#include "fdbserver/core/DBCoreState.h"
#include "fdbserver/core/LogSystemConfig.h"
#include "fdbserver/core/MutationTracking.h"
#include "fdbserver/core/OTELSpanContextMessage.h"
#include "fdbserver/core/SpanContextMessage.h"
#include "fdbserver/core/TLogInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/ActorCollection.h"
#include "flow/Histogram.h"
#include "flow/IndexedSet.h"
#include "flow/Knobs.h"

struct DBCoreState;
struct LogPushData;
struct LocalityData;
struct LogSystem;
class LogSet;

struct ConnectionResetInfo : public ReferenceCounted<ConnectionResetInfo> {
	double lastReset;
	Future<Void> resetCheck;
	int slowReplies;
	int fastReplies;

	ConnectionResetInfo() : lastReset(now()), resetCheck(Void()), slowReplies(0), fastReplies(0) {}
};

struct IPeekCursor {
	virtual Reference<IPeekCursor> cloneNoMore() = 0;

	virtual void setProtocolVersion(ProtocolVersion version) = 0;

	virtual bool hasMessage() const = 0;
	virtual VectorRef<Tag> getTags() const = 0;
	virtual Arena& arena() = 0;
	virtual ArenaReader* reader() = 0;
	virtual StringRef getMessage() = 0;
	virtual StringRef getMessageWithTags() = 0;
	virtual void nextMessage() = 0;
	virtual void advanceTo(LogMessageVersion n) = 0;
	virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) = 0;
	virtual Future<Void> onFailed() const = 0;
	virtual bool isActive() const = 0;
	virtual bool isExhausted() const = 0;
	virtual const LogMessageVersion& version() const = 0;
	virtual Version popped() const = 0;
	virtual Version getMaxKnownVersion() const { return 0; }
	virtual Version getMinKnownCommittedVersion() const = 0;
	virtual Optional<UID> getPrimaryPeekLocation() const = 0;
	virtual Optional<UID> getCurrentPeekLocation() const = 0;
	virtual void addref() = 0;
	virtual void delref() = 0;
};

struct LogPushVersionSet {
	Version prevVersion;
	Version version;
	Version knownCommittedVersion;
	Version minKnownCommittedVersion;
};

bool logSystemHasRemoteLogs(LogSystem const& logSystem);
void logSystemGetPushLocations(
    LogSystem const& logSystem,
    VectorRef<Tag> tags,
    std::vector<int>& locations,
    bool allLocations = false,
    Optional<std::vector<Reference<LocalitySet>>> fromLocations = Optional<std::vector<Reference<LocalitySet>>>());
std::vector<Reference<LocalitySet>> logSystemGetPushLocationsForTags(LogSystem const& logSystem,
                                                                     std::vector<int>& fromLocations);
Tag logSystemGetRandomRouterTag(LogSystem const& logSystem);
int logSystemGetLogRouterTags(LogSystem const& logSystem);
Tag logSystemGetRandomTxsTag(LogSystem const& logSystem);
TLogVersion logSystemGetTLogVersion(LogSystem const& logSystem);

struct LengthPrefixedStringRef {
	uint32_t* length;

	StringRef toStringRef() const {
		ASSERT(length);
		return StringRef((uint8_t*)(length + 1), *length);
	}
	int expectedSize() const {
		ASSERT(length);
		return *length;
	}
	uint32_t* getLengthPtr() const { return length; }

	LengthPrefixedStringRef() : length(nullptr) {}
	LengthPrefixedStringRef(uint32_t* length) : length(length) {}
};

struct LogPushData : NonCopyable {
	explicit LogPushData(Reference<LogSystem> logSystem, int tlogCount);

	void addTxsTag();

	void addTag(Tag tag) { next_message_tags.push_back(tag); }

	template <class T>
	void addTags(T tags) {
		next_message_tags.insert(next_message_tags.end(), tags.begin(), tags.end());
	}

	void addTransactionInfo(SpanContext const& context);

	void saveTags(std::set<Tag>& filteredTags) const {
		for (const auto& tag : written_tags) {
			filteredTags.insert(tag);
		}
	}

	void addWrittenTags(const std::set<Tag>& tags) { written_tags.insert(tags.begin(), tags.end()); }

	void getLocations(const std::set<Tag>& tags, std::set<uint16_t>& writtenTLogs) {
		std::vector<Tag> vtags(tags.begin(), tags.end());
		std::vector<int> locations;
		logSystemGetPushLocations(*logSystem, VectorRef<Tag>((Tag*)vtags.data(), vtags.size()), locations);
		writtenTLogs.insert(locations.begin(), locations.end());
	}

	void saveLocations(std::set<uint16_t>& writtenTLogs) {
		writtenTLogs.insert(msg_locations.begin(), msg_locations.end());
	}

	void setLogsChanged() { logsChanged = true; }
	bool haveLogsChanged() const { return logsChanged; }

	void writeMessage(StringRef rawMessageWithoutLength, bool usePreviousLocations);

	void setPushLocationsForTags(std::vector<int> fromLocationsVec) {
		fromLocations = logSystemGetPushLocationsForTags(*logSystem, fromLocationsVec);
	}

	template <class T>
	void writeTypedMessage(T const& item, bool metadataMessage = false, bool allLocations = false);

	Standalone<StringRef> getMessages(int loc) const { return messagesWriter[loc].toValue(); }

	std::vector<Standalone<StringRef>> getAllMessages() const;

	void recordEmptyMessage(int loc, const Standalone<StringRef>& value);

	float getEmptyMessageRatio() const;

	uint32_t getMutationCount() const { return subsequence - 1; }

	void setMutations(uint32_t totalMutations, VectorRef<StringRef> mutations);

	Optional<Tag> savedRandomRouterTag;
	void storeRandomRouterTag() { savedRandomRouterTag = logSystemGetRandomRouterTag(*logSystem); }
	int getLogRouterTags() { return logSystemGetLogRouterTags(*logSystem); }

private:
	Reference<LogSystem> logSystem;
	std::vector<Tag> next_message_tags;
	std::vector<Tag> prev_tags;
	std::set<Tag> written_tags;
	std::vector<BinaryWriter> messagesWriter;
	std::vector<bool> messagesWritten;
	std::vector<int> msg_locations;
	Optional<std::vector<Reference<LocalitySet>>> fromLocations;
	std::unordered_set<int> writtenLocations;
	uint32_t subsequence;
	SpanContext spanContext;
	bool logsChanged = false;

	bool writeTransactionInfo(int location, uint32_t subseq);

	Tag chooseRouterTag() {
		return savedRandomRouterTag.present() ? savedRandomRouterTag.get() : logSystemGetRandomRouterTag(*logSystem);
	}
};

// LogSystem info in old epoch
struct OldLogData {
	std::vector<Reference<LogSet>> tLogs;
	int32_t logRouterTags;
	int32_t txsTags; // The number of txsTags, which may change across generations.
	Version epochBegin, epochEnd;
	Version recoverAt;
	std::set<int8_t> pseudoLocalities;
	LogEpoch epoch;

	OldLogData() : logRouterTags(0), txsTags(0), epochBegin(0), epochEnd(0), recoverAt(0), epoch(0) {}

	// Constructor for T of OldTLogConf and OldTLogCoreData
	template <class T>
	explicit OldLogData(const T& conf);
};

struct IdToInterf : ReferenceCounted<IdToInterf> {
	Optional<Version> recoverAt = Optional<Version>();
	std::map<UID, TLogInterface> lockInterf;
};

struct LogLockInfo {
	Version epochEnd;
	bool isCurrent;
	Reference<LogSet> logSet;
	std::vector<Future<TLogLockResult>> replies;

	LogLockInfo() : epochEnd(std::numeric_limits<Version>::max()), isCurrent(false) {}
};

struct DurableVersionInfo {
	Version knownCommittedVersion; // maximum of the known committed versions of available tLogs
	Version minimumDurableVersion; // mimimum of the durable versions of available tLogs
	std::vector<TLogLockResult> lockResults; // replies from various tLogs
	bool policyResult; // unavailable tLogs meet the replication policy or not
	std::vector<uint16_t> knownLockedTLogIds; // tLogs that are known to have been locked

	DurableVersionInfo(Version kcv,
	                   Version dv,
	                   std::vector<TLogLockResult>& replies,
	                   bool meetsPolicy,
	                   std::vector<uint16_t>& lockedTLogIds)
	  : knownCommittedVersion(kcv), minimumDurableVersion(dv), lockResults(std::move(replies)),
	    policyResult(meetsPolicy), knownLockedTLogIds(std::move(lockedTLogIds)) {}
};

struct LogSystem : ReferenceCounted<LogSystem> {
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
	Future<Void> trackTLogRecovery;
	Future<Void> remoteTrackTLogRecovery;
	Reference<AsyncVar<Version>> recoveredVersion;
	Reference<AsyncVar<Version>> remoteRecoveredVersion;
	std::vector<LogLockInfo> lockResults;
	AsyncVar<bool> recoveryCompleteWrittenToCoreState;
	bool remoteLogsWrittenToCoreState;
	bool hasRemoteServers;
	AsyncTrigger backupWorkerChanged;
	std::set<UID> removedBackupWorkers; // Workers that are removed before setting them.
	std::map<uint8_t, std::vector<uint16_t>> knownLockedTLogIds;

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

	// Stores each <log router, tag> pair's last popped version. This is used to determine whether we need to pop an old
	// generation log router.
	std::map<std::pair<UID, Tag>, Version> logRouterLastPops;

	Optional<PromiseStream<Future<Void>>> addActor;
	ActorCollection popActors;
	std::vector<OldLogData> oldLogData; // each element has the log info. in one old epoch.
	AsyncTrigger logSystemConfigChanged;

	LogSystem(UID dbgid,
	          LocalityData locality,
	          LogEpoch e,
	          Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>())
	  : dbgid(dbgid), logSystemType(LogSystemType::empty), expectedLogSets(0), logRouterTags(0), txsTags(0),
	    repopulateRegionAntiQuorum(0), stopped(false), epoch(e), oldestBackupEpoch(0),
	    recoveredVersion(makeReference<AsyncVar<Version>>(invalidVersion)),
	    remoteRecoveredVersion(makeReference<AsyncVar<Version>>(invalidVersion)),
	    recoveryCompleteWrittenToCoreState(false), remoteLogsWrittenToCoreState(false), hasRemoteServers(false),
	    locality(locality), addActor(addActor), popActors(false) {}

	void stopRejoins();

	void addref();

	void delref();

	std::string describe() const;

	UID getDebugID() const;

	LogSystemType getLogSystemType() const;

	void addPseudoLocality(int8_t locality);

	Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const;

	bool hasPseudoLocality(int8_t locality) const;

	// Return the min version of all pseudoLocalities, i.e., logRouter and backupTag
	Version popPseudoLocalityTag(Tag tag, Version upTo);

	static Future<Void> recoverAndEndEpoch(Reference<AsyncVar<Reference<LogSystem>>> const& outLogSystem,
	                                       UID const& dbgid,
	                                       DBCoreState const& oldState,
	                                       FutureStream<TLogRejoinRequest> const& rejoins,
	                                       LocalityData const& locality,
	                                       bool* forceRecovery);

	static Reference<LogSystem> fromLogSystemConfig(UID const& dbgid,
	                                                LocalityData const& locality,
	                                                LogSystemConfig const& lsConf,
	                                                bool excludeRemote,
	                                                bool useRecoveredAt,
	                                                Optional<PromiseStream<Future<Void>>> addActor);

	static Reference<LogSystem> fromOldLogSystemConfig(UID const& dbgid,
	                                                   LocalityData const& locality,
	                                                   LogSystemConfig const& lsConf);

	// Convert LogSystem to DBCoreState and override input newState as return value
	void toCoreState(DBCoreState& newState) const;

	bool remoteStorageRecovered() const;

	// Checks older TLog generations and remove no longer needed generations from the log system.
	void purgeOldRecoveredGenerationsCoreState(DBCoreState&);
	void purgeOldRecoveredGenerationsInMemory(const DBCoreState&);

	Future<Void> onCoreStateChanged() const;

	void coreStateWritten(DBCoreState const& newState);

	Future<Void> onError() const;

	static Future<Void> onError_internal(LogSystem const* self);

	static Future<Void> pushResetChecker(Reference<ConnectionResetInfo> self, NetworkAddress addr);

	static Future<TLogCommitReply> recordPushMetrics(Reference<ConnectionResetInfo> self,
	                                                 Reference<Histogram> dist,
	                                                 NetworkAddress addr,
	                                                 Future<TLogCommitReply> in);

	Future<Version> push(const LogPushVersionSet& versionSet,
	                     LogPushData& data,
	                     SpanContext const& spanContext,
	                     Optional<UID> debugID,
	                     Optional<std::unordered_map<uint16_t, Version>> tpcvMap);

	// Version vector/Unicast specific: reset best server if it is not known to have been locked/stopped.
	void resetBestServerIfNotLocked(int bestSet,
	                                int& bestServer,
	                                Optional<Version> end,
	                                const Optional<std::map<uint8_t, std::vector<uint16_t>>>& knownLockedTLogIds);

	Reference<IPeekCursor> peekAll(UID dbgid, Version begin, Version end, Tag tag, bool parallelGetMore);

	Reference<IPeekCursor> peekRemote(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore);

	Reference<IPeekCursor> peek(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore);

	Reference<IPeekCursor> peek(UID dbgid,
	                            Version begin,
	                            Optional<Version> end,
	                            std::vector<Tag> tags,
	                            bool parallelGetMore);

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
	                               bool canDiscardPopped);

	Reference<IPeekCursor> peekSingle(
	    UID dbgid,
	    Version begin,
	    Tag tag,
	    std::vector<std::pair<Version, Tag>> history = std::vector<std::pair<Version, Tag>>());

	// LogRouter or BackupWorker use this function to obtain a cursor for peeking tlogs of a generation (i.e., epoch).
	// Specifically, the epoch is determined by looking up "dbgid" in tlog sets of generations.
	// The returned cursor can peek data at the "tag" from the given "begin" version to that epoch's end version or
	// the recovery version for the latest old epoch. For the current epoch, the cursor has no end version.
	// For the old epoch, the cursor is provided an end version.
	Reference<IPeekCursor> peekLogRouter(UID dbgid,
	                                     Version begin,
	                                     Tag tag,
	                                     bool useSatellite,
	                                     Optional<Version> end = Optional<Version>(),
	                                     const Optional<std::map<uint8_t, std::vector<uint16_t>>>& knownStoppedTLogIds =
	                                         Optional<std::map<uint8_t, std::vector<uint16_t>>>());

	Version getKnownCommittedVersion();

	Future<Void> onKnownCommittedVersionChange();

	void popLogRouter(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality);

	void popTxs(Version upTo, int8_t popLocality = tagLocalityInvalid);

	// pop 'tag.locality' type data up to the 'upTo' version
	void pop(Version upTo, Tag tag, Version durableKnownCommittedVersion = 0, int8_t popLocality = tagLocalityInvalid);

	// pop tag from log up to the version defined in self->outstandingPops[].first
	static Future<Void> popFromLog(LogSystem* self,
	                               Reference<AsyncVar<OptionalInterface<TLogInterface>>> log,
	                               Tag tag,
	                               double delayBeforePop,
	                               bool popLogRouter);

	static Future<Version> getPoppedFromTLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> log, Tag tag);

	static Future<Version> getPoppedTxs(LogSystem* self);

	Future<Version> getTxsPoppedVersion();

	static Future<Void> confirmEpochLive_internal(Reference<LogSet> logSet, Optional<UID> debugID);

	// Returns success after confirming that pushes in the current epoch are still possible
	Future<Void> confirmEpochLive(Optional<UID> debugID = Optional<UID>());

	Future<Void> endEpoch();

	// Call only after end_epoch() has successfully completed.  Returns a new epoch immediately following this one.
	// The new epoch is only provisional until the caller updates the coordinated DBCoreState.
	Future<Reference<LogSystem>> newEpoch(RecruitFromConfigurationReply const& recr,
	                                      Future<RecruitRemoteFromConfigurationReply> const& fRemoteWorkers,
	                                      DatabaseConfiguration const& config,
	                                      LogEpoch recoveryCount,
	                                      Version recoveryTransactionVersion,
	                                      int8_t primaryLocality,
	                                      int8_t remoteLocality,
	                                      std::vector<Tag> const& allTags,
	                                      Reference<AsyncVar<bool>> const& recruitmentStalled);

	LogSystemConfig getLogSystemConfig() const;

	Standalone<StringRef> getLogsValue() const;

	Future<Void> onLogSystemConfigChange();

	void updateLogRouter(int logSetIndex, int tagId, TLogInterface const& newLogRouter);

	Version getEnd() const;

	Version getPeekEnd() const;

	void getPushLocations(VectorRef<Tag> tags,
	                      std::vector<int>& locations,
	                      bool allLocations,
	                      Optional<std::vector<Reference<LocalitySet>>> fromLocations) const;

	std::vector<Reference<LocalitySet>> getPushLocationsForTags(std::vector<int>& fromLocations) const;

	bool hasRemoteLogs() const;

	Tag getRandomRouterTag() const;

	Tag getRandomTxsTag() const;

	TLogVersion getTLogVersion() const;

	int getLogRouterTags() const;

	Version getBackupStartVersion() const;

	std::map<LogEpoch, EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const;

	inline Reference<LogSet> getEpochLogSet(LogEpoch epoch) const;

	void setBackupWorkers(const std::vector<InitializeBackupReply>& replies);

	bool removeBackupWorker(const BackupWorkerDoneRequest& req);

	LogEpoch getOldestBackupEpoch() const;

	void setOldestBackupEpoch(LogEpoch epoch);
	static Future<Void> monitorLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> logServer,
	                               Reference<AsyncVar<bool>> failed);

	// returns the log group's knownComittedVersion, DV, a vector of TLogLockResults for
	// each tLog in the group, and the result of applying the replication policy check over
	// unavailable tLogs from the group
	Optional<DurableVersionInfo> static getDurableVersion(
	    UID dbgid,
	    LogLockInfo lockInfo,
	    std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>(),
	    Optional<Version> lastEnd = Optional<Version>());

	static Future<Void> getDurableVersionChanged(
	    LogLockInfo lockInfo,
	    std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>());

	static Future<Void> epochEnd(Reference<AsyncVar<Reference<LogSystem>>> outLogSystem,
	                             UID dbgid,
	                             DBCoreState prevState,
	                             FutureStream<TLogRejoinRequest> rejoinRequests,
	                             LocalityData locality,
	                             bool* forceRecovery);

	static Future<Void> recruitOldLogRouters(LogSystem* self,
	                                         std::vector<WorkerInterface> workers,
	                                         LogEpoch recoveryCount,
	                                         int8_t locality,
	                                         Version startVersion,
	                                         std::vector<LocalityData> tLogLocalities,
	                                         Reference<IReplicationPolicy> tLogPolicy,
	                                         bool forRemote);

	static Version getMaxLocalStartVersion(const std::vector<Reference<LogSet>>& tLogs);

	static std::vector<Tag> getLocalTags(int8_t locality, const std::vector<Tag>& allTags);

	static Future<Void> newRemoteEpoch(LogSystem* self,
	                                   Reference<LogSystem> oldLogSystem,
	                                   Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
	                                   DatabaseConfiguration configuration,
	                                   LogEpoch recoveryCount,
	                                   Version recoveryTransactionVersion,
	                                   int8_t remoteLocality,
	                                   std::vector<Tag> allTags,
	                                   std::vector<Version> oldGenerationRecoverAtVersions);

	static Future<Reference<LogSystem>> newEpoch(Reference<LogSystem> oldLogSystem,
	                                             RecruitFromConfigurationReply recr,
	                                             Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers,
	                                             DatabaseConfiguration configuration,
	                                             LogEpoch recoveryCount,
	                                             Version recoveryTransactionVersion,
	                                             int8_t primaryLocality,
	                                             int8_t remoteLocality,
	                                             std::vector<Tag> allTags,
	                                             Reference<AsyncVar<bool>> recruitmentStalled);

	static Future<Void> trackRejoins(
	    UID dbgid,
	    std::vector<std::pair<Reference<AsyncVar<OptionalInterface<TLogInterface>>>, Reference<IReplicationPolicy>>>
	        logServers,
	    FutureStream<struct TLogRejoinRequest> rejoinRequests);

	// Keeps track of the recovered generations in all the TLogs in `tlogs` list and updates the latest
	// `recoveredVersion`.
	static Future<Void> trackTLogRecoveryActor(std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> tlogs,
	                                           Reference<AsyncVar<Version>> recoveredVersion);

	static Future<TLogLockResult> lockTLog(UID myID, Reference<AsyncVar<OptionalInterface<TLogInterface>>> tlog);
	template <class T>
	static std::vector<T> getReadyNonError(std::vector<Future<T>> const& futures);
};

// Recovery version calculation for version vector unicast
Optional<std::tuple<Version, Version>> getRecoverVersionUnicast(
    const std::vector<Reference<LogSet>>& logServers,
    const std::tuple<int, std::vector<TLogLockResult>, bool>& logGroupResults,
    Version minDV);

template <class T>
std::vector<T> LogSystem::getReadyNonError(std::vector<Future<T>> const& futures) {
	// Return the values of those futures which have (non-error) values ready
	std::vector<T> result;
	for (auto& f : futures)
		if (f.isReady() && !f.isError())
			result.push_back(f.get());
	return result;
}

#include "LogSystemTypes.h"

template <class T>
OldLogData::OldLogData(const T& conf)
  : logRouterTags(conf.logRouterTags), txsTags(conf.txsTags), epochBegin(conf.epochBegin), epochEnd(conf.epochEnd),
    recoverAt(conf.recoverAt), pseudoLocalities(conf.pseudoLocalities), epoch(conf.epoch) {
	tLogs.resize(conf.tLogs.size());
	for (int j = 0; j < conf.tLogs.size(); j++) {
		auto logSet = makeReference<LogSet>(conf.tLogs[j]);
		tLogs[j] = logSet;
	}
}

template <class T>
void LogPushData::writeTypedMessage(T const& item, bool metadataMessage, bool allLocations) {
	prev_tags.clear();
	if (logSystemHasRemoteLogs(*logSystem)) {
		prev_tags.push_back(chooseRouterTag());
	}
	for (auto& tag : next_message_tags) {
		prev_tags.push_back(tag);
	}
	msg_locations.clear();
	logSystemGetPushLocations(*logSystem,
	                          VectorRef<Tag>((Tag*)prev_tags.data(), prev_tags.size()),
	                          msg_locations,
	                          allLocations,
	                          fromLocations);

	BinaryWriter bw(AssumeVersion(g_network->protocolVersion()));

	if (!metadataMessage) {
		uint32_t subseq = this->subsequence++;
		bool updatedLocation = false;
		for (int loc : msg_locations) {
			updatedLocation = writeTransactionInfo(loc, subseq) || updatedLocation;
		}
		if (!updatedLocation) {
			this->subsequence--;
			CODE_PROBE(true, "No new SpanContextMessage written to transaction logs");
			ASSERT(this->subsequence > 0);
		}
	} else {
		ASSERT(writtenLocations.size() == 0);
	}

	uint32_t subseq = this->subsequence++;
	bool first = true;
	int firstOffset = -1, firstLength = -1;
	for (int loc : msg_locations) {
		BinaryWriter& wr = messagesWriter[loc];

		if (first) {
			firstOffset = wr.getLength();
			wr << uint32_t(0) << subseq << uint16_t(prev_tags.size());
			for (auto& tag : prev_tags)
				wr << tag;
			wr << item;
			firstLength = wr.getLength() - firstOffset;
			*(uint32_t*)((uint8_t*)wr.getData() + firstOffset) = firstLength - sizeof(uint32_t);
			DEBUG_TAGS_AND_MESSAGE(
			    "ProxyPushLocations", invalidVersion, StringRef(((uint8_t*)wr.getData() + firstOffset), firstLength))
			    .detail("PushLocations", msg_locations);
			first = false;
		} else {
			BinaryWriter& from = messagesWriter[msg_locations[0]];
			wr.serializeBytes((uint8_t*)from.getData() + firstOffset, firstLength);
		}
	}
	written_tags.insert(next_message_tags.begin(), next_message_tags.end());
	next_message_tags.clear();
}

#endif // FDBSERVER_LOGSYSTEM_LOGSYSTEM_H
