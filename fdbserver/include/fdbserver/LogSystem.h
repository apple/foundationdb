/*
 * LogSystem.h
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

#ifndef FDBSERVER_LOGSYSTEM_H
#define FDBSERVER_LOGSYSTEM_H

#include <cstdint>
#include <set>
#include <vector>

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/OTELSpanContextMessage.h"
#include "fdbserver/SpanContextMessage.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/Histogram.h"
#include "flow/IndexedSet.h"
#include "flow/Knobs.h"

struct DBCoreState;
struct TLogSet;
struct CoreTLogSet;
struct LogPushData;
struct LocalityData;

struct ConnectionResetInfo : public ReferenceCounted<ConnectionResetInfo> {
	double lastReset;
	Future<Void> resetCheck;
	int slowReplies;
	int fastReplies;

	ConnectionResetInfo() : lastReset(now()), resetCheck(Void()), slowReplies(0), fastReplies(0) {}
};

// The set of tLog servers, logRouters and backupWorkers for a log tag
class LogSet : NonCopyable, public ReferenceCounted<LogSet> {
public:
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers;
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logRouters;
	std::vector<Reference<AsyncVar<OptionalInterface<BackupInterface>>>> backupWorkers;
	std::vector<Reference<ConnectionResetInfo>> connectionResetTrackers;
	std::vector<Reference<Histogram>> tlogPushDistTrackers;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	std::vector<LocalityData> tLogLocalities; // Stores the localities of the log servers
	TLogVersion tLogVersion;
	Reference<IReplicationPolicy> tLogPolicy;
	Reference<LocalitySet> logServerSet;
	std::vector<int> logIndexArray;
	std::vector<LocalityEntry> logEntryArray;
	bool isLocal; // true if the LogSet is in primary DC or primary DC's satellite
	int8_t locality;
	Version startVersion;
	std::vector<Future<TLogLockResult>> replies;
	std::vector<std::vector<int>> satelliteTagLocations;

	LogSet()
	  : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), isLocal(true), locality(tagLocalityInvalid),
	    startVersion(invalidVersion) {}
	LogSet(const TLogSet& tlogSet);
	LogSet(const CoreTLogSet& coreSet);

	std::string logRouterString();

	bool hasLogRouter(UID id) const;

	bool hasBackupWorker(UID id) const;

	std::string logServerString();

	void populateSatelliteTagLocations(int logRouterTags, int oldLogRouterTags, int txsTags, int oldTxsTags);

	void checkSatelliteTagLocations();

	int bestLocationFor(Tag tag);

	void updateLocalitySet(std::vector<LocalityData> const& localities);

	bool satisfiesPolicy(const std::vector<LocalityEntry>& locations);

	void getPushLocations(VectorRef<Tag> tags,
	                      std::vector<int>& locations,
	                      int locationOffset,
	                      bool allLocations = false);

private:
	std::vector<LocalityEntry> alsoServers, resultEntries;
	std::vector<int> newLocations;
};

struct ILogSystem {
	// Represents a particular (possibly provisional) epoch of the log subsystem

	struct IPeekCursor {
		// clones the peek cursor, however you cannot call getMore() on the cloned cursor.
		virtual Reference<IPeekCursor> cloneNoMore() = 0;

		virtual void setProtocolVersion(ProtocolVersion version) = 0;

		// if hasMessage() returns true, getMessage(), getMessageWithTags(), or reader() can be called.
		// does not modify the cursor
		virtual bool hasMessage() const = 0;

		// pre: only callable if hasMessage() returns true
		// return the tags associated with the message for the current sequence
		virtual VectorRef<Tag> getTags() const = 0;

		// pre: only callable if hasMessage() returns true
		// returns the arena containing the contents of getMessage(), getMessageWithTags(), and reader()
		virtual Arena& arena() = 0;

		// pre: only callable if hasMessage() returns true
		// returns an arena reader for the next message
		// caller cannot call getMessage(), getMessageWithTags(), and reader()
		// the caller must advance the reader before calling nextMessage()
		virtual ArenaReader* reader() = 0;

		// pre: only callable if hasMessage() returns true
		// caller cannot call getMessage(), getMessageWithTags(), and reader()
		// return the contents of the message for the current sequence
		virtual StringRef getMessage() = 0;

		// pre: only callable if hasMessage() returns true
		// caller cannot call getMessage(), getMessageWithTags(), and reader()
		// return the contents of the message for the current sequence
		virtual StringRef getMessageWithTags() = 0;

		// pre: only callable after getMessage(), getMessageWithTags(), or reader()
		// post: hasMessage() and version() have been updated
		// hasMessage() will never return false "in the middle" of a version (that is, if it does return false,
		// version().subsequence will be zero)  < FIXME: Can we lose this property?
		virtual void nextMessage() = 0;

		// advances the cursor to the supplied LogMessageVersion, and updates hasMessage
		virtual void advanceTo(LogMessageVersion n) = 0;

		// returns immediately if hasMessage() returns true.
		// returns when either the result of hasMessage() or version() has changed, or a cursor has internally been
		// exhausted.
		virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) = 0;

		// returns when the failure monitor detects that the servers associated with the cursor are failed
		virtual Future<Void> onFailed() const = 0;

		// returns false if:
		// (1) the failure monitor detects that the servers associated with the cursor is failed
		// (2) the interface is not present
		// (3) the cursor cannot return any more results
		virtual bool isActive() const = 0;

		// returns true if the cursor cannot return any more results
		virtual bool isExhausted() const = 0;

		// Returns the smallest possible message version which the current message (if any) or a subsequent message
		// might have (If hasMessage(), this is therefore the message version of the current message)
		virtual const LogMessageVersion& version() const = 0;

		// So far, the cursor has returned all messages which both satisfy the criteria passed to peek() to create the
		// cursor AND have (popped(),0) <= message version number <= version() Other messages might have been skipped
		virtual Version popped() const = 0;

		// Returns the maximum version known to have been pushed (not necessarily durably) into the log system (0 is
		// always a possible result!)
		virtual Version getMaxKnownVersion() const { return 0; }

		virtual Version getMinKnownCommittedVersion() const = 0;

		virtual Optional<UID> getPrimaryPeekLocation() const = 0;

		virtual Optional<UID> getCurrentPeekLocation() const = 0;

		virtual void addref() = 0;

		virtual void delref() = 0;
	};

	struct ServerPeekCursor final : IPeekCursor, ReferenceCounted<ServerPeekCursor> {
		Reference<AsyncVar<OptionalInterface<TLogInterface>>> interf;
		const Tag tag;

		TLogPeekReply results;
		ArenaReader rd;
		LogMessageVersion messageVersion,
		    end; // the version of current message; the intended end version of current cursor
		Version poppedVersion;
		TagsAndMessage messageAndTags;
		bool hasMsg;
		Future<Void> more;
		UID randomID;
		bool returnIfBlocked;

		bool onlySpilled;
		bool parallelGetMore;
		bool usePeekStream;
		int sequence;
		Deque<Future<TLogPeekReply>> futureResults;
		Future<Void> interfaceChanged;
		Optional<ReplyPromiseStream<TLogPeekStreamReply>> peekReplyStream;

		double lastReset;
		Future<Void> resetCheck;
		int slowReplies;
		int fastReplies;
		int unknownReplies;

		ServerPeekCursor(Reference<AsyncVar<OptionalInterface<TLogInterface>>> const& interf,
		                 Tag tag,
		                 Version begin,
		                 Version end,
		                 bool returnIfBlocked,
		                 bool parallelGetMore);
		ServerPeekCursor(TLogPeekReply const& results,
		                 LogMessageVersion const& messageVersion,
		                 LogMessageVersion const& end,
		                 TagsAndMessage const& message,
		                 bool hasMsg,
		                 Version poppedVersion,
		                 Tag tag);

		Reference<IPeekCursor> cloneNoMore() override;
		void setProtocolVersion(ProtocolVersion version) override;
		Arena& arena() override;
		ArenaReader* reader() override;
		bool hasMessage() const override;
		void nextMessage() override;
		StringRef getMessage() override;
		StringRef getMessageWithTags() override;
		VectorRef<Tag> getTags() const override;
		void advanceTo(LogMessageVersion n) override;
		Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) override;
		Future<Void> onFailed() const override;
		bool isActive() const override;
		bool isExhausted() const override;
		const LogMessageVersion& version() const override;
		Version popped() const override;
		Version getMinKnownCommittedVersion() const override;
		Optional<UID> getPrimaryPeekLocation() const override;
		Optional<UID> getCurrentPeekLocation() const override;

		void addref() override { ReferenceCounted<ServerPeekCursor>::addref(); }

		void delref() override { ReferenceCounted<ServerPeekCursor>::delref(); }

		Version getMaxKnownVersion() const override { return results.maxKnownVersion; }
	};

	struct MergedPeekCursor final : IPeekCursor, ReferenceCounted<MergedPeekCursor> {
		Reference<LogSet> logSet;
		std::vector<Reference<IPeekCursor>> serverCursors;
		std::vector<LocalityEntry> locations;
		std::vector<std::pair<LogMessageVersion, int>> sortedVersions;
		Tag tag;
		int bestServer, currentCursor, readQuorum;
		Optional<LogMessageVersion> nextVersion;
		LogMessageVersion messageVersion;
		bool hasNextMessage;
		UID randomID;
		int tLogReplicationFactor;
		Future<Void> more;

		MergedPeekCursor(std::vector<Reference<ILogSystem::IPeekCursor>> const& serverCursors, Version begin);
		MergedPeekCursor(std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers,
		                 int bestServer,
		                 int readQuorum,
		                 Tag tag,
		                 Version begin,
		                 Version end,
		                 bool parallelGetMore,
		                 std::vector<LocalityData> const& tLogLocalities,
		                 Reference<IReplicationPolicy> const tLogPolicy,
		                 int tLogReplicationFactor);
		MergedPeekCursor(std::vector<Reference<IPeekCursor>> const& serverCursors,
		                 LogMessageVersion const& messageVersion,
		                 int bestServer,
		                 int readQuorum,
		                 Optional<LogMessageVersion> nextVersion,
		                 Reference<LogSet> logSet,
		                 int tLogReplicationFactor);

		Reference<IPeekCursor> cloneNoMore() override;
		void setProtocolVersion(ProtocolVersion version) override;
		Arena& arena() override;
		ArenaReader* reader() override;
		void calcHasMessage();
		void updateMessage(bool usePolicy);
		bool hasMessage() const override;
		void nextMessage() override;
		StringRef getMessage() override;
		StringRef getMessageWithTags() override;
		VectorRef<Tag> getTags() const override;
		void advanceTo(LogMessageVersion n) override;
		Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) override;
		Future<Void> onFailed() const override;
		bool isActive() const override;
		bool isExhausted() const override;
		const LogMessageVersion& version() const override;
		Version popped() const override;
		Version getMinKnownCommittedVersion() const override;
		Optional<UID> getPrimaryPeekLocation() const override;
		Optional<UID> getCurrentPeekLocation() const override;

		void addref() override { ReferenceCounted<MergedPeekCursor>::addref(); }

		void delref() override { ReferenceCounted<MergedPeekCursor>::delref(); }
	};

	struct SetPeekCursor final : IPeekCursor, ReferenceCounted<SetPeekCursor> {
		std::vector<Reference<LogSet>> logSets;
		std::vector<std::vector<Reference<IPeekCursor>>> serverCursors;
		Tag tag;
		int bestSet, bestServer, currentSet, currentCursor;
		std::vector<LocalityEntry> locations;
		std::vector<std::pair<LogMessageVersion, int>> sortedVersions;
		Optional<LogMessageVersion> nextVersion;
		LogMessageVersion messageVersion;
		bool hasNextMessage;
		bool useBestSet;
		UID randomID;
		Future<Void> more;

		SetPeekCursor(std::vector<Reference<LogSet>> const& logSets,
		              int bestSet,
		              int bestServer,
		              Tag tag,
		              Version begin,
		              Version end,
		              bool parallelGetMore);
		SetPeekCursor(std::vector<Reference<LogSet>> const& logSets,
		              std::vector<std::vector<Reference<IPeekCursor>>> const& serverCursors,
		              LogMessageVersion const& messageVersion,
		              int bestSet,
		              int bestServer,
		              Optional<LogMessageVersion> nextVersion,
		              bool useBestSet);

		Reference<IPeekCursor> cloneNoMore() override;
		void setProtocolVersion(ProtocolVersion version) override;
		Arena& arena() override;
		ArenaReader* reader() override;
		void calcHasMessage();
		void updateMessage(int logIdx, bool usePolicy);
		bool hasMessage() const override;
		void nextMessage() override;
		StringRef getMessage() override;
		StringRef getMessageWithTags() override;
		VectorRef<Tag> getTags() const override;
		void advanceTo(LogMessageVersion n) override;
		Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) override;
		Future<Void> onFailed() const override;
		bool isActive() const override;
		bool isExhausted() const override;
		const LogMessageVersion& version() const override;
		Version popped() const override;
		Version getMinKnownCommittedVersion() const override;
		Optional<UID> getPrimaryPeekLocation() const override;
		Optional<UID> getCurrentPeekLocation() const override;

		void addref() override { ReferenceCounted<SetPeekCursor>::addref(); }

		void delref() override { ReferenceCounted<SetPeekCursor>::delref(); }
	};

	struct MultiCursor final : IPeekCursor, ReferenceCounted<MultiCursor> {
		std::vector<Reference<IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;
		Version poppedVersion;

		MultiCursor(std::vector<Reference<IPeekCursor>> cursors, std::vector<LogMessageVersion> epochEnds);

		Reference<IPeekCursor> cloneNoMore() override;
		void setProtocolVersion(ProtocolVersion version) override;
		Arena& arena() override;
		ArenaReader* reader() override;
		bool hasMessage() const override;
		void nextMessage() override;
		StringRef getMessage() override;
		StringRef getMessageWithTags() override;
		VectorRef<Tag> getTags() const override;
		void advanceTo(LogMessageVersion n) override;
		Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) override;
		Future<Void> onFailed() const override;
		bool isActive() const override;
		bool isExhausted() const override;
		const LogMessageVersion& version() const override;
		Version popped() const override;
		Version getMinKnownCommittedVersion() const override;
		Optional<UID> getPrimaryPeekLocation() const override;
		Optional<UID> getCurrentPeekLocation() const override;

		void addref() override { ReferenceCounted<MultiCursor>::addref(); }

		void delref() override { ReferenceCounted<MultiCursor>::delref(); }
	};

	struct BufferedCursor final : IPeekCursor, ReferenceCounted<BufferedCursor> {
		struct BufferedMessage {
			Arena arena;
			StringRef message;
			VectorRef<Tag> tags;
			LogMessageVersion version;

			BufferedMessage() {}
			explicit BufferedMessage(Version version) : version(version) {}
			BufferedMessage(Arena arena,
			                StringRef message,
			                const VectorRef<Tag>& tags,
			                const LogMessageVersion& version)
			  : arena(arena), message(message), tags(tags), version(version) {}

			bool operator<(BufferedMessage const& r) const { return version < r.version; }

			bool operator==(BufferedMessage const& r) const { return version == r.version; }
		};

		std::vector<Reference<IPeekCursor>> cursors;
		std::vector<Deque<BufferedMessage>> cursorMessages;
		std::vector<BufferedMessage> messages;
		int messageIndex;
		LogMessageVersion messageVersion;
		Version end;
		bool hasNextMessage;
		bool withTags;
		bool knownUnique;
		Version minKnownCommittedVersion;
		Version poppedVersion;
		Version initialPoppedVersion;
		bool canDiscardPopped;
		Future<Void> more;
		int targetQueueSize;
		UID randomID;

		BufferedCursor(std::vector<Reference<IPeekCursor>> cursors,
		               Version begin,
		               Version end,
		               bool withTags,
		               bool canDiscardPopped);
		BufferedCursor(std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers,
		               Tag tag,
		               Version begin,
		               Version end,
		               bool parallelGetMore);

		Reference<IPeekCursor> cloneNoMore() override;
		void setProtocolVersion(ProtocolVersion version) override;
		Arena& arena() override;
		ArenaReader* reader() override;
		bool hasMessage() const override;
		void nextMessage() override;
		StringRef getMessage() override;
		StringRef getMessageWithTags() override;
		VectorRef<Tag> getTags() const override;
		void advanceTo(LogMessageVersion n) override;
		Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) override;
		Future<Void> onFailed() const override;
		bool isActive() const override;
		bool isExhausted() const override;
		const LogMessageVersion& version() const override;
		Version popped() const override;
		Version getMinKnownCommittedVersion() const override;
		Optional<UID> getPrimaryPeekLocation() const override;
		Optional<UID> getCurrentPeekLocation() const override;

		void addref() override { ReferenceCounted<BufferedCursor>::addref(); }

		void delref() override { ReferenceCounted<BufferedCursor>::delref(); }
	};

	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual std::string describe() const = 0;
	virtual UID getDebugID() const = 0;

	virtual void toCoreState(DBCoreState&) const = 0;

	virtual bool remoteStorageRecovered() const = 0;

	virtual void purgeOldRecoveredGenerations() = 0;

	virtual Future<Void> onCoreStateChanged() const = 0;
	// Returns if and when the output of toCoreState() would change (for example, when older logs can be discarded from
	// the state)

	virtual void coreStateWritten(DBCoreState const& newState) = 0;
	// Called when a core state has been written to the coordinators

	virtual Future<Void> onError() const = 0;
	// Never returns normally, but throws an error if the subsystem stops working

	struct PushVersionSet {
		Version prevVersion;
		Version version;
		Version knownCommittedVersion;
		Version minKnownCommittedVersion;
	};

	virtual Future<Version> push(const PushVersionSet& verisonSet,
	                             LogPushData& data,
	                             SpanContext const& spanContext,
	                             Optional<UID> debugID = Optional<UID>(),
	                             Optional<std::unordered_map<uint16_t, Version>> tpcvMap =
	                                 Optional<std::unordered_map<uint16_t, Version>>()) = 0;
	// Waits for the version number of the bundle (in this epoch) to be prevVersion (i.e. for all pushes ordered
	// earlier) Puts the given messages into the bundle, each with the given tags, and with message versions (version,
	// 0) - (version, N) Changes the version number of the bundle to be version (unblocking the next push) Returns when
	// the preceding changes are durable.  (Later we will need multiple return signals for different durability levels)
	// If the current epoch has ended, push will not return, and the pushed messages will not be visible in any
	// subsequent epoch (but may become visible in this epoch)

	virtual Reference<IPeekCursor> peek(UID dbgid,
	                                    Version begin,
	                                    Optional<Version> end,
	                                    Tag tag,
	                                    bool parallelGetMore = false) = 0;
	// Returns (via cursor interface) a stream of messages with the given tag and message versions >= (begin, 0),
	// ordered by message version If pop was previously or concurrently called with upTo > begin, the cursor may not
	// return all such messages.  In that case cursor->popped() will be greater than begin to reflect that.

	virtual Reference<IPeekCursor> peek(UID dbgid,
	                                    Version begin,
	                                    Optional<Version> end,
	                                    std::vector<Tag> tags,
	                                    bool parallelGetMore = false) = 0;
	// Same contract as peek(), but for a set of tags

	virtual Reference<IPeekCursor> peekSingle(
	    UID dbgid,
	    Version begin,
	    Tag tag,
	    std::vector<std::pair<Version, Tag>> history = std::vector<std::pair<Version, Tag>>()) = 0;
	// Same contract as peek(), but blocks until the preferred log server(s) for the given tag are available (and is
	// correspondingly less expensive)

	virtual Reference<IPeekCursor> peekLogRouter(UID dbgid,
	                                             Version begin,
	                                             Tag tag,
	                                             bool useSatellite,
	                                             Optional<Version> end = Optional<Version>()) = 0;
	// Same contract as peek(), but can only peek from the logs elected in the same generation.
	// If the preferred log server is down, a different log from the same generation will merge results locally before
	// sending them to the log router.

	virtual Reference<IPeekCursor> peekTxs(UID dbgid,
	                                       Version begin,
	                                       int8_t peekLocality,
	                                       Version localEnd,
	                                       bool canDiscardPopped) = 0;
	// Same contract as peek(), but only for peeking the txsLocality. It allows specifying a preferred peek locality.

	virtual Future<Version> getTxsPoppedVersion() = 0;

	virtual Version getKnownCommittedVersion() = 0;

	virtual Future<Void> onKnownCommittedVersionChange() = 0;

	virtual void popTxs(Version upTo, int8_t popLocality = tagLocalityInvalid) = 0;

	virtual void pop(Version upTo,
	                 Tag tag,
	                 Version knownCommittedVersion = 0,
	                 int8_t popLocality = tagLocalityInvalid) = 0;
	// Permits, but does not require, the log subsystem to strip `tag` from any or all messages with message versions <
	// (upTo,0) The popping of any given message may be arbitrarily delayed.

	virtual Future<Void> confirmEpochLive(Optional<UID> debugID = Optional<UID>()) = 0;
	// Returns success after confirming that pushes in the current epoch are still possible

	virtual Future<Void> endEpoch() = 0;
	// Ends the current epoch without starting a new one

	static Reference<ILogSystem> fromServerDBInfo(
	    UID const& dbgid,
	    ServerDBInfo const& db,
	    bool useRecoveredAt = false,
	    Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>());
	static Reference<ILogSystem> fromLogSystemConfig(
	    UID const& dbgid,
	    LocalityData const&,
	    LogSystemConfig const&,
	    bool excludeRemote = false,
	    bool useRecoveredAt = false,
	    Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>());
	// Constructs a new ILogSystem implementation from the given ServerDBInfo/LogSystemConfig.  Might return a null
	// reference if there isn't a fully recovered log system available. The caller can peek() the returned log system
	// and can push() if it has version numbers reserved for it and prevVersions

	static Reference<ILogSystem> fromOldLogSystemConfig(UID const& dbgid, LocalityData const&, LogSystemConfig const&);
	// Constructs a new ILogSystem implementation from the old log data within a ServerDBInfo/LogSystemConfig.  Might
	// return a null reference if there isn't a fully recovered log system available.

	static Future<Void> recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem,
	                                       UID const& dbgid,
	                                       DBCoreState const& oldState,
	                                       FutureStream<TLogRejoinRequest> const& rejoins,
	                                       LocalityData const& locality,
	                                       bool* forceRecovery);
	// Constructs a new ILogSystem implementation based on the given oldState and rejoining log servers
	// Ensures that any calls to push or confirmEpochLive in the current epoch but strictly later than change_epoch will
	// not return Whenever changes in the set of available log servers require restarting recovery with a different end
	// sequence, outLogSystem will be changed to a new ILogSystem

	virtual Version getEnd() const = 0;
	// Call only on an ILogSystem obtained from recoverAndEndEpoch()
	// Returns the first unreadable version number of the recovered epoch (i.e. message version numbers < (get_end(), 0)
	// will be readable)

	// Returns the start version of current epoch for backup workers.
	virtual Version getBackupStartVersion() const = 0;

	struct EpochTagsVersionsInfo {
		int32_t logRouterTags; // Number of log router tags.
		Version epochBegin, epochEnd;

		explicit EpochTagsVersionsInfo(int32_t n, Version begin, Version end)
		  : logRouterTags(n), epochBegin(begin), epochEnd(end) {}
	};

	// Returns EpochTagVersionsInfo for old epochs that this log system is aware of, excluding the current epoch.
	virtual std::map<LogEpoch, EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const = 0;

	virtual Future<Reference<ILogSystem>> newEpoch(
	    RecruitFromConfigurationReply const& recr,
	    Future<struct RecruitRemoteFromConfigurationReply> const& fRemoteWorkers,
	    DatabaseConfiguration const& config,
	    LogEpoch recoveryCount,
	    Version recoveryTransactionVersion,
	    int8_t primaryLocality,
	    int8_t remoteLocality,
	    std::vector<Tag> const& allTags,
	    Reference<AsyncVar<bool>> const& recruitmentStalled) = 0;
	// Call only on an ILogSystem obtained from recoverAndEndEpoch()
	// Returns an ILogSystem representing a new epoch immediately following this one.  The new epoch is only provisional
	// until the caller updates the coordinated DBCoreState

	// Returns the physical configuration of this LogSystem, that could be used to construct an equivalent LogSystem
	// using fromLogSystemConfig()
	virtual LogSystemConfig getLogSystemConfig() const = 0;

	// Returns the type of LogSystem, this should be faster than using RTTI
	virtual LogSystemType getLogSystemType() const = 0;

	virtual Standalone<StringRef> getLogsValue() const = 0;

	// Returns when the log system configuration has changed due to a tlog rejoin.
	virtual Future<Void> onLogSystemConfigChange() = 0;

	virtual void getPushLocations(VectorRef<Tag> tags,
	                              std::vector<int>& locations,
	                              bool allLocations = false) const = 0;

	void getPushLocations(std::vector<Tag> const& tags, std::vector<int>& locations, bool allLocations = false) {
		getPushLocations(VectorRef<Tag>((Tag*)&tags.front(), tags.size()), locations, allLocations);
	}

	virtual bool hasRemoteLogs() const = 0;

	virtual Tag getRandomRouterTag() const = 0;
	virtual int getLogRouterTags() const = 0; // Returns the number of router tags.

	virtual Tag getRandomTxsTag() const = 0;

	// Returns the TLogVersion of the current generation of TLogs.
	// (This only exists because getLogSystemConfig is a significantly more expensive call.)
	virtual TLogVersion getTLogVersion() const = 0;

	virtual void stopRejoins() = 0;

	// XXX: Should Tag related functions stay inside TagPartitionedLogSystem??
	// Returns the pseudo tag to be popped for the given process class. If the
	// process class doesn't use pseudo tag, return the same tag.
	virtual Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const = 0;

	virtual bool hasPseudoLocality(int8_t locality) const = 0;

	// Returns the actual version to be popped from the log router tag for the given pseudo tag.
	// For instance, a pseudo tag (-8, 2) means the actual popping tag is (-2, 2). Assuming there
	// are multiple pseudo tags, the returned version is the min(all pseudo tags' "upTo" versions).
	virtual Version popPseudoLocalityTag(Tag tag, Version upTo) = 0;

	virtual void setBackupWorkers(const std::vector<InitializeBackupReply>& replies) = 0;

	// Removes a finished backup worker from log system and returns true. Returns false
	// if the worker is not found.
	virtual bool removeBackupWorker(const BackupWorkerDoneRequest& req) = 0;

	virtual LogEpoch getOldestBackupEpoch() const = 0;
	virtual void setOldestBackupEpoch(LogEpoch epoch) = 0;
};

struct LengthPrefixedStringRef {
	// Represents a pointer to a string which is prefixed by a 4-byte length
	// A LengthPrefixedStringRef is only pointer-sized (8 bytes vs 12 bytes for StringRef), but the corresponding string
	// is 4 bytes bigger, and substring operations aren't efficient as they are with StringRef.  It's a good choice when
	// there might be lots of references to the same exact string.

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

// Structure to store serialized mutations sent from the proxy to the
// transaction logs. The serialization repeats with the following format:
//
// +----------------------+ +----------------------+ +----------+ +----------------+         +----------------------+
// |     Message size     | |      Subsequence     | | # of tags| |      Tag       | . . . . |       Mutation       |
// +----------------------+ +----------------------+ +----------+ +----------------+         +----------------------+
// <------- 32 bits ------> <------- 32 bits ------> <- 16 bits-> <---- 24 bits --->         <---- variable bits --->
//
// `Mutation` can be a serialized MutationRef or a special metadata message
// such as LogProtocolMessage or SpanContextMessage. The type of `Mutation` is
// uniquely identified by its first byte -- a value from MutationRef::Type.
//
struct LogPushData : NonCopyable {
	// Log subsequences have to start at 1 (the MergedPeekCursor relies on this to make sure we never have !hasMessage()
	// in the middle of data for a version

	explicit LogPushData(Reference<ILogSystem> logSystem, int tlogCount);

	void addTxsTag();

	// addTag() adds a tag for the *next* message to be added
	void addTag(Tag tag) { next_message_tags.push_back(tag); }

	template <class T>
	void addTags(T tags) {
		next_message_tags.insert(next_message_tags.end(), tags.begin(), tags.end());
	}

	// Add transaction info to be written before the first mutation in the transaction.
	void addTransactionInfo(SpanContext const& context);

	// copy written_tags, after filtering, into given set
	void saveTags(std::set<Tag>& filteredTags) const {
		for (const auto& tag : written_tags) {
			filteredTags.insert(tag);
		}
	}

	void addWrittenTags(const std::set<Tag>& tags) { written_tags.insert(tags.begin(), tags.end()); }

	void getLocations(const std::set<Tag>& tags, std::set<uint16_t>& writtenTLogs) {
		std::vector<Tag> vtags(tags.begin(), tags.end());
		std::vector<int> msg_locations;
		logSystem->getPushLocations(vtags, msg_locations, false /*allLocations*/);
		writtenTLogs.insert(msg_locations.begin(), msg_locations.end());
	}

	// store tlogs as represented by index
	void saveLocations(std::set<uint16_t>& writtenTLogs) {
		writtenTLogs.insert(msg_locations.begin(), msg_locations.end());
	}

	void setShardChanged() { shardChanged = true; }
	bool isShardChanged() const { return shardChanged; }

	void writeMessage(StringRef rawMessageWithoutLength, bool usePreviousLocations);

	template <class T>
	void writeTypedMessage(T const& item, bool metadataMessage = false, bool allLocations = false);

	Standalone<StringRef> getMessages(int loc) const { return messagesWriter[loc].toValue(); }

	// Returns all locations' messages, including empty ones.
	std::vector<Standalone<StringRef>> getAllMessages() const;

	// Records if a tlog (specified by "loc") will receive an empty version batch message.
	// "value" is the message returned by getMessages() call.
	void recordEmptyMessage(int loc, const Standalone<StringRef>& value);

	// Returns the ratio of empty messages in this version batch.
	// MUST be called after getMessages() and recordEmptyMessage().
	float getEmptyMessageRatio() const;

	// Returns the total number of mutations. Subsequence is initialized to 1, so subtract 1 to get count.
	uint32_t getMutationCount() const { return subsequence - 1; }

	// Sets mutations for all internal writers. "mutations" is the output from
	// getAllMessages() and is used before writing any other mutations.
	void setMutations(uint32_t totalMutations, VectorRef<StringRef> mutations);

	Optional<Tag> savedRandomRouterTag;
	void storeRandomRouterTag() { savedRandomRouterTag = logSystem->getRandomRouterTag(); }
	int getLogRouterTags() { return logSystem->getLogRouterTags(); }

private:
	Reference<ILogSystem> logSystem;
	std::vector<Tag> next_message_tags;
	std::vector<Tag> prev_tags;
	std::set<Tag> written_tags;
	std::vector<BinaryWriter> messagesWriter;
	std::vector<bool> messagesWritten; // if messagesWriter has written anything
	std::vector<int> msg_locations;
	// Stores message locations that have had span information written to them
	// for the current transaction. Adding transaction info will reset this
	// field.
	std::unordered_set<int> writtenLocations;
	uint32_t subsequence;
	SpanContext spanContext;
	bool shardChanged = false; // if keyServers has any changes, i.e., shard boundary modifications.

	// Writes transaction info to the message stream at the given location if
	// it has not already been written (for the current transaction). Returns
	// true on a successful write, and false if the location has already been
	// written.
	bool writeTransactionInfo(int location, uint32_t subseq);

	Tag chooseRouterTag() {
		return savedRandomRouterTag.present() ? savedRandomRouterTag.get() : logSystem->getRandomRouterTag();
	}
};

template <class T>
void LogPushData::writeTypedMessage(T const& item, bool metadataMessage, bool allLocations) {
	prev_tags.clear();
	if (logSystem->hasRemoteLogs()) {
		prev_tags.push_back(chooseRouterTag());
	}
	for (auto& tag : next_message_tags) {
		prev_tags.push_back(tag);
	}
	msg_locations.clear();
	logSystem->getPushLocations(prev_tags, msg_locations, allLocations);

	BinaryWriter bw(AssumeVersion(g_network->protocolVersion()));

	// Metadata messages (currently LogProtocolMessage is the only metadata
	// message) should be written before span information. If this isn't a
	// metadata message, make sure all locations have had transaction info
	// written to them.  Mutations may have different sets of tags, so it
	// is necessary to check all tag locations each time a mutation is
	// written.
	if (!metadataMessage) {
		uint32_t subseq = this->subsequence++;
		bool updatedLocation = false;
		for (int loc : msg_locations) {
			updatedLocation = writeTransactionInfo(loc, subseq) || updatedLocation;
		}
		// If this message doesn't write to any new locations, the
		// subsequence wasn't actually used and can be decremented.
		if (!updatedLocation) {
			this->subsequence--;
			CODE_PROBE(true, "No new SpanContextMessage written to transaction logs");
			ASSERT(this->subsequence > 0);
		}
	} else {
		// When writing a metadata message, make sure transaction state has
		// been reset. If you are running into this assertion, make sure
		// you are calling addTransactionInfo before each transaction.
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

#endif // FDBSERVER_LOGSYSTEM_H
