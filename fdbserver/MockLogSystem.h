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

struct MockLogSystem : ILogSystem, ReferenceCounted<MockLogSystem> {
	Reference<ILogSystem::IPeekCursor> cursor;

	MockLogSystem();

	MockLogSystem(const MockLogSystem&);
	MockLogSystem& operator=(const MockLogSystem&);

	MockLogSystem(const MockLogSystem&&) = delete;
	MockLogSystem& operator=(const MockLogSystem&&) = delete;

	void addref() final;
	void delref() final;
	std::string describe() const final;
	UID getDebugID() const final;
	void toCoreState(DBCoreState& coreState) final;
	bool remoteStorageRecovered() final;
	Future<Void> onCoreStateChanged() final;
	void coreStateWritten(const DBCoreState& newState) final;
	Future<Void> onError() final;
	Future<Version> push(std::vector<Version> prevVersions,
	                     Version version,
	                     Version knownCommittedVersion,
	                     Version minKnownCommittedVersion,
	                     struct LogPushData& data,
	                     const SpanID& spanContext,
	                     Optional<UID> debugID,
	                     std::vector<ptxn::TLogGroupID> tLogGroups) final;
	Reference<IPeekCursor> peek(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore) final;
	Reference<IPeekCursor> peek(UID dbgid,
	                            Version begin,
	                            Optional<Version> end,
	                            std::vector<Tag> tags,
	                            bool parallelGetMore) final;
	Reference<IPeekCursor> peekSingle(UID dbgid,
	                                  Version begin,
	                                  Tag tag,
	                                  Optional<ptxn::StorageTeamID> storageTeam,
	                                  std::vector<std::pair<Version, Tag>> history) final;
	Reference<IPeekCursor> peekLogRouter(UID dbgid, Version begin, Tag tag) final;
	Reference<IPeekCursor> peekTxs(UID dbgid,
	                               Version begin,
	                               int8_t peekLocality,
	                               Version localEnd,
	                               bool canDiscardPopped) final;
	Future<Version> getTxsPoppedVersion() final;
	Version getKnownCommittedVersion() final;
	Future<Void> onKnownCommittedVersionChange() final;
	void popTxs(Version upTo, int8_t popLocality) final;
	void pop(Version upTo, Tag tag, Version knownCommittedVersion, int8_t popLocality) final;
	Future<Void> confirmEpochLive(Optional<UID> debugID) final;
	Future<Void> endEpoch() final;
	Version getEnd() const final;
	Version getBackupStartVersion() const final;
	std::map<LogEpoch, EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const final;
	Future<Reference<ILogSystem>> newEpoch(const RecruitFromConfigurationReply& recr,
	                                       const Future<struct RecruitRemoteFromConfigurationReply>& fRemoteWorkers,
	                                       const DatabaseConfiguration& config,
	                                       LogEpoch recoveryCount,
	                                       int8_t primaryLocality,
	                                       int8_t remoteLocality,
	                                       const vector<Tag>& allTags,
	                                       const Reference<AsyncVar<bool>>& recruitmentStalled,
	                                       Reference<TLogGroupCollection> tLogGroupCollection) final;
	LogSystemConfig getLogSystemConfig() const final;
	Standalone<StringRef> getLogsValue() const final;
	Future<Void> onLogSystemConfigChange() final;
	void getPushLocations(VectorRef<Tag> tags, vector<int>& locations, bool allLocations) const final;
	bool hasRemoteLogs() const final;
	Tag getRandomRouterTag() const final;
	int getLogRouterTags() const final;
	Tag getRandomTxsTag() const final;
	TLogVersion getTLogVersion() const final;
	void stopRejoins() final;
	Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const final;
	bool hasPseudoLocality(int8_t locality) const final;
	Version popPseudoLocalityTag(Tag tag, Version upTo) final;
	void setBackupWorkers(const std::vector<InitializeBackupReply>& replies) final;
	bool removeBackupWorker(const BackupWorkerDoneRequest& req) final;
	LogEpoch getOldestBackupEpoch() const final;
	void setOldestBackupEpoch(LogEpoch epoch) final;
};

#endif // FDBSERVER_MOCK_LOG_SYSTEM_H
