/*
 * FakeLogSystem.h
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

#ifndef FDBSERVER_PTXN_TEST_MOCK_LOG_SYSTEM_H
#define FDBSERVER_PTXN_TEST_MOCK_LOG_SYSTEM_H

#pragma once

#include <unordered_map>

#include "fdbserver/LogSystem.h"
#include "fdbserver/ptxn/test/FakePeekCursor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct InitializeBackupReply;
struct RecruitFromConfigurationReply;
struct RecruitRemoteFromConfigurationReply;

namespace ptxn::test {

class FakeLogSystem : public ::ILogSystem, public ReferenceCounted<FakeLogSystem> {

protected:
	// The debug ID for the object that created the log system
	const UID creatorDebugID;

public:
	FakeLogSystem(const UID& creatorDebugID_ = UID());

	FakeLogSystem(const FakeLogSystem&&) = delete;
	FakeLogSystem& operator=(const FakeLogSystem&&) = delete;

	virtual void addref() override;
	virtual void delref() override;
	virtual std::string describe() const override;
	virtual UID getDebugID() const override;
	virtual void toCoreState(DBCoreState& coreState) override;
	virtual bool remoteStorageRecovered() override;
	virtual Future<Void> onCoreStateChanged() override;
	virtual void coreStateWritten(const DBCoreState& newState) override;
	virtual Future<Void> onError() override;
	virtual Future<Version> push(Version prevVersion,
	                             Version version,
	                             Version knownCommittedVersion,
	                             Version minKnownCommittedVersion,
	                             struct LogPushData& data,
	                             const SpanID& spanContext,
	                             Optional<UID> debugID,
	                             Optional<ptxn::TLogGroupID> tLogGroup) override;
	virtual Reference<IPeekCursor> peek(UID dbgid,
	                                    Version begin,
	                                    Optional<Version> end,
	                                    Tag tag,
	                                    bool parallelGetMore) override;
	virtual Reference<IPeekCursor> peek(UID dbgid,
	                                    Version begin,
	                                    Optional<Version> end,
	                                    std::vector<Tag> tags,
	                                    bool parallelGetMore) override;
	virtual Reference<IPeekCursor> peekSingle(UID dbgid,
	                                          Version begin,
	                                          Tag tag,
	                                          Optional<ptxn::StorageTeamID> storageTeam,
	                                          std::vector<std::pair<Version, Tag>> history) override;
	virtual Reference<IPeekCursor> peekLogRouter(UID dbgid, Version begin, Tag tag) override;
	virtual Reference<IPeekCursor> peekTxs(UID dbgid,
	                                       Version begin,
	                                       int8_t peekLocality,
	                                       Version localEnd,
	                                       bool canDiscardPopped) override;
	virtual Future<Version> getTxsPoppedVersion() override;
	virtual Version getKnownCommittedVersion() override;
	virtual Future<Void> onKnownCommittedVersionChange() override;
	virtual void popTxs(Version upTo, int8_t popLocality) override;
	virtual void pop(Version upTo, Tag tag, Version knownCommittedVersion, int8_t popLocality) override;
	virtual Future<Void> confirmEpochLive(Optional<UID> debugID) override;
	virtual Future<Void> endEpoch() override;
	virtual Version getEnd() const override;
	virtual Version getBackupStartVersion() const override;
	virtual std::map<LogEpoch, EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const override;
	virtual Future<Reference<ILogSystem>> newEpoch(const RecruitFromConfigurationReply& recr,
	                                               const Future<::RecruitRemoteFromConfigurationReply>& fRemoteWorkers,
	                                               const DatabaseConfiguration& config,
	                                               LogEpoch recoveryCount,
	                                               int8_t primaryLocality,
	                                               int8_t remoteLocality,
	                                               const vector<Tag>& allTags,
	                                               const Reference<AsyncVar<bool>>& recruitmentStalled,
	                                               TLogGroupCollectionRef tLogGroupCollection) override;
	virtual LogSystemConfig getLogSystemConfig() const override;
	virtual Standalone<StringRef> getLogsValue() const override;
	virtual Future<Void> onLogSystemConfigChange() override;
	virtual void getPushLocations(VectorRef<Tag> tags, vector<int>& locations, bool allLocations) const override;
	virtual bool hasRemoteLogs() const override;
	virtual Tag getRandomRouterTag() const override;
	virtual int getLogRouterTags() const override;
	virtual Tag getRandomTxsTag() const override;
	virtual TLogVersion getTLogVersion() const override;
	virtual void stopRejoins() override;
	virtual Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const override;
	virtual bool hasPseudoLocality(int8_t locality) const override;
	virtual Version popPseudoLocalityTag(Tag tag, Version upTo) override;
	virtual void setBackupWorkers(const std::vector<::InitializeBackupReply>& replies) override;
	virtual bool removeBackupWorker(const BackupWorkerDoneRequest& req) override;
	virtual LogEpoch getOldestBackupEpoch() const override;
	virtual void setOldestBackupEpoch(LogEpoch epoch) override;
};

// FakeLogSystem that returns a FakePeekCursor binding to an id, this allows the cursor can be manipulated
// outside the scope the LogSystem is created.
class FakeLogSystem_CustomPeekCursor : public FakeLogSystem {
private:
	static std::unordered_map<UID, Reference<::ILogSystem::IPeekCursor>> cursorIDMapping;

public:
	FakeLogSystem_CustomPeekCursor(const UID& creatorDebugID_ = UID());

	virtual Reference<::ILogSystem::IPeekCursor> peekSingle(UID dbgid,
	                                                        Version begin,
	                                                        Tag tag,
	                                                        Optional<ptxn::StorageTeamID> storageTeam,
	                                                        std::vector<std::pair<Version, Tag>> history) override;

	// Get the cursor by its ID
	// Any LogSystems that are created by the
	static Reference<::ILogSystem::IPeekCursor>& getCursorByID(const UID& id);
};

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_MOCK_LOG_SYSTEM_H
