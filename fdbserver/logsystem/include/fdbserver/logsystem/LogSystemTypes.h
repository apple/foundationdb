/*
 * LogSystemTypes.h
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

#ifndef FDBSERVER_LOGSYSTEM_LOGSYSTEMTYPES_H
#define FDBSERVER_LOGSYSTEM_LOGSYSTEMTYPES_H
#pragma once

#include "fdbserver/core/LogSystemConfig.h"
#include "fdbserver/core/DBCoreState.h"

class LogSet : NonCopyable, public ReferenceCounted<LogSet> {
public:
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers;
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logRouters;
	std::vector<Reference<AsyncVar<OptionalInterface<BackupInterface>>>> backupWorkers;
	std::vector<Reference<ConnectionResetInfo>> connectionResetTrackers;
	std::vector<Reference<Histogram>> tlogPushDistTrackers;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	std::vector<LocalityData> tLogLocalities;
	TLogVersion tLogVersion;
	Reference<IReplicationPolicy> tLogPolicy;
	Reference<LocalitySet> logServerSet;
	std::vector<int> logIndexArray;
	std::vector<LocalityEntry> logEntryArray;
	bool isLocal;
	int8_t locality;
	Version startVersion;
	std::vector<Future<TLogLockResult>> replies;
	std::vector<std::vector<int>> satelliteTagLocations;

	LogSet()
	  : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), isLocal(true), locality(tagLocalityInvalid),
	    startVersion(invalidVersion) {}
	explicit LogSet(const TLogSet& tlogSet);
	explicit LogSet(const CoreTLogSet& coreSet);

	std::string logRouterString();
	bool hasLogRouter(UID id) const;
	bool hasBackupWorker(UID id) const;
	std::string logServerString();
	void populateSatelliteTagLocations(int logRouterTags, int oldLogRouterTags, int txsTags, int oldTxsTags);
	void checkSatelliteTagLocations();
	int bestLocationFor(Tag tag);
	void updateLocalitySet(std::vector<LocalityData> const& localities);
	bool satisfiesPolicy(const std::vector<LocalityEntry>& locations);
	void getPushLocations(
	    VectorRef<Tag> tags,
	    std::vector<int>& locations,
	    int locationOffset,
	    bool allLocations = false,
	    const Optional<Reference<LocalitySet>>& restrictedLogSet = Optional<Reference<LocalitySet>>());

private:
	std::vector<LocalityEntry> alsoServers, resultEntries;
	std::vector<int> newLocations;
};

class ServerPeekCursor final : public IPeekCursor, public ReferenceCounted<ServerPeekCursor> {
public:
	Reference<AsyncVar<OptionalInterface<TLogInterface>>> interf;
	const Tag tag;
	TLogPeekReply results;
	ArenaReader rd;
	LogMessageVersion messageVersion, end;
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
	bool returnEmptyIfStopped;

	ServerPeekCursor(Reference<AsyncVar<OptionalInterface<TLogInterface>>> const& interf,
	                 Tag tag,
	                 Version begin,
	                 Version end,
	                 bool returnIfBlocked,
	                 bool parallelGetMore,
	                 bool returnEmtpyIfStopped = false);
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

class MergedPeekCursor final : public IPeekCursor, public ReferenceCounted<MergedPeekCursor> {
public:
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

	MergedPeekCursor(std::vector<Reference<IPeekCursor>> const& serverCursors, Version begin);
	MergedPeekCursor(std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers,
	                 int bestServer,
	                 int readQuorum,
	                 Tag tag,
	                 Version begin,
	                 Version end,
	                 bool parallelGetMore,
	                 std::vector<LocalityData> const& tLogLocalities,
	                 Reference<IReplicationPolicy> const tLogPolicy,
	                 int tLogReplicationFactor,
	                 const Optional<std::vector<uint16_t>>& knownLockedTLogIds = Optional<std::vector<uint16_t>>());
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

class SetPeekCursor final : public IPeekCursor, public ReferenceCounted<SetPeekCursor> {
public:
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
	Optional<Version> end;

	SetPeekCursor(std::vector<Reference<LogSet>> const& logSets,
	              int bestSet,
	              int bestServer,
	              Tag tag,
	              Version begin,
	              Version end,
	              bool parallelGetMore,
	              const Optional<std::vector<uint16_t>>& knownLockedTLogIds = Optional<std::vector<uint16_t>>());
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

class MultiCursor final : public IPeekCursor, public ReferenceCounted<MultiCursor> {
public:
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

class BufferedCursor final : public IPeekCursor, public ReferenceCounted<BufferedCursor> {
public:
	struct BufferedMessage {
		Arena arena;
		StringRef message;
		VectorRef<Tag> tags;
		LogMessageVersion version;

		BufferedMessage() {}
		explicit BufferedMessage(Version version) : version(version) {}
		BufferedMessage(Arena arena, StringRef message, const VectorRef<Tag>& tags, const LogMessageVersion& version)
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

#endif
