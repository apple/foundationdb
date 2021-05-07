/*
 * MockLogSystem.h
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

#ifndef FDBSERVER_MOCK_LOG_SYSTEM_H
#define FDBSERVER_MOCK_LOG_SYSTEM_H

#include "fdbserver/LogSystem.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// struct TagPartitionedLogSystem : ILogSystem, ReferenceCounted<TagPartitionedLogSystem> {
//}
struct MockLogSystem : ILogSystem, ReferenceCounted<MockLogSystem> {
	Reference<ILogSystem::IPeekCursor> cursor;

	MockLogSystem();

	void addref();
	void delref();
	std::string describe() const;
	UID getDebugID() const;
	void toCoreState(DBCoreState& coreState);
	bool remoteStorageRecovered();
	Future<Void> onCoreStateChanged();
	void coreStateWritten(const DBCoreState& newState);
	Future<Void> onError();
	Future<Version> push(Version prevVersion,
	                     Version version,
	                     Version knownCommittedVersion,
	                     Version minKnownCommittedVersion,
	                     struct LogPushData& data,
	                     const SpanID& spanContext,
	                     Optional<UID> debugID);
	Reference<IPeekCursor> peek(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore);
	Reference<IPeekCursor> peek(UID dbgid,
	                            Version begin,
	                            Optional<Version> end,
	                            std::vector<Tag> tags,
	                            bool parallelGetMore);
	Reference<IPeekCursor> peekSingle(UID dbgid, Version begin, Tag tag, std::vector<std::pair<Version, Tag>> history);
	Reference<IPeekCursor> peekLogRouter(UID dbgid, Version begin, Tag tag);
	Reference<IPeekCursor> peekTxs(UID dbgid,
	                               Version begin,
	                               int8_t peekLocality,
	                               Version localEnd,
	                               bool canDiscardPopped);
	Future<Version> getTxsPoppedVersion();
	Version getKnownCommittedVersion();
	Future<Void> onKnownCommittedVersionChange();
	void popTxs(Version upTo, int8_t popLocality);
	void pop(Version upTo, Tag tag, Version knownCommittedVersion, int8_t popLocality);
	Future<Void> confirmEpochLive(Optional<UID> debugID);
	Future<Void> endEpoch();
	Version getEnd() const;
	Version getBackupStartVersion() const;
	std::map<LogEpoch, EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const;
	Future<Reference<ILogSystem>> newEpoch(const RecruitFromConfigurationReply& recr,
	                                       const Future<struct RecruitRemoteFromConfigurationReply>& fRemoteWorkers,
	                                       const DatabaseConfiguration& config,
	                                       LogEpoch recoveryCount,
	                                       int8_t primaryLocality,
	                                       int8_t remoteLocality,
	                                       const vector<Tag>& allTags,
	                                       const Reference<AsyncVar<bool>>& recruitmentStalled);
	LogSystemConfig getLogSystemConfig() const;
	Standalone<StringRef> getLogsValue() const;
	Future<Void> onLogSystemConfigChange();
	void getPushLocations(VectorRef<Tag> tags, vector<int>& locations, bool allLocations) const;
	bool hasRemoteLogs() const;
	Tag getRandomRouterTag() const;
	int getLogRouterTags() const;
	Tag getRandomTxsTag() const;
	TLogVersion getTLogVersion() const;
	void stopRejoins();
	Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const;
	bool hasPseudoLocality(int8_t locality) const;
	Version popPseudoLocalityTag(Tag tag, Version upTo);
	void setBackupWorkers(const std::vector<InitializeBackupReply>& replies);
	bool removeBackupWorker(const BackupWorkerDoneRequest& req);
	LogEpoch getOldestBackupEpoch() const;
	void setOldestBackupEpoch(LogEpoch epoch);
};

#endif // FDBSERVER_MOCK_LOG_SYSTEM_H
