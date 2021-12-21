/*
 * FakeLogSystem.cpp
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

#include "fdbserver/ptxn/test/FakeLogSystem.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace ptxn::test {

const static bool LOG_METHOD_CALLS = false;

static void logMethodName(std::string methodName) {
	if (LOG_METHOD_CALLS) {
		std::cout << "FakeLogSystem::" << methodName << std::endl;
	}
}

#ifndef __INTEL_COMPILER
#pragma region FakeLogSystem
#endif

FakeLogSystem::FakeLogSystem(const UID& creatorDebugID_) : creatorDebugID(creatorDebugID_) {
	logMethodName(__func__);
}

void FakeLogSystem::addref() {
	logMethodName(__func__);
	ReferenceCounted<FakeLogSystem>::addref();
}

void FakeLogSystem::delref() {
	logMethodName(__func__);
	ReferenceCounted<FakeLogSystem>::delref();
}

std::string FakeLogSystem::describe() const {
	logMethodName(__func__);
	return std::string();
}

UID FakeLogSystem::getDebugID() const {
	logMethodName(__func__);
	return UID();
}

void FakeLogSystem::toCoreState(DBCoreState& coreState) {
	logMethodName(__func__);
}

bool FakeLogSystem::remoteStorageRecovered() {
	logMethodName(__func__);
	return false;
}

Future<Void> FakeLogSystem::onCoreStateChanged() {
	logMethodName(__func__);
	return Future<Void>();
}

void FakeLogSystem::coreStateWritten(const DBCoreState& newState) {
	logMethodName(__func__);
}

Future<Void> FakeLogSystem::onError() {
	logMethodName(__func__);
	return Future<Void>();
}

Future<Version> FakeLogSystem::push(Version prevVersion,
                                    Version version,
                                    Version knownCommittedVersion,
                                    Version minKnownCommittedVersion,
                                    struct LogPushData& data,
                                    const SpanID& spanContext,
                                    Optional<UID> debugID,
                                    Optional<ptxn::TLogGroupID> tLogGroup,
                                    const std::set<ptxn::StorageTeamID>& addedTeams,
                                    const std::set<ptxn::StorageTeamID>& removedTeams) {

	logMethodName(__func__);
	return Future<Version>();
}

Reference<ILogSystem::IPeekCursor> FakeLogSystem::peek(UID dbgid,
                                                       Version begin,
                                                       Optional<Version> end,
                                                       Tag tag,
                                                       bool parallelGetMore) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

Reference<ILogSystem::IPeekCursor> FakeLogSystem::peek(UID dbgid,
                                                       Version begin,
                                                       Optional<Version> end,
                                                       std::vector<Tag> tags,
                                                       bool parallelGetMore) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

// Return mocked cursor.
Reference<ILogSystem::IPeekCursor> FakeLogSystem::peekSingle(UID dbgid,
                                                             Version begin,
                                                             Tag tag,
                                                             Optional<ptxn::StorageTeamID> storageTeam,
                                                             std::vector<std::pair<Version, Tag>> history) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

Reference<ILogSystem::IPeekCursor> FakeLogSystem::peekLogRouter(UID dbgid, Version begin, Tag tag) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

Reference<ILogSystem::IPeekCursor> FakeLogSystem::peekTxs(UID dbgid,
                                                          Version begin,
                                                          int8_t peekLocality,
                                                          Version localEnd,
                                                          bool canDiscardPopped) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

Future<Version> FakeLogSystem::getTxsPoppedVersion() {
	logMethodName(__func__);
	return Future<Version>();
}

Version FakeLogSystem::getKnownCommittedVersion() {
	logMethodName(__func__);
	return 0;
}

Future<Void> FakeLogSystem::onKnownCommittedVersionChange() {
	logMethodName(__func__);
	return Future<Void>();
}

void FakeLogSystem::popTxs(Version upTo, int8_t popLocality) {
	logMethodName(__func__);
}

void FakeLogSystem::pop(Version upTo, Tag tag, Version knownCommittedVersion, int8_t popLocality) {
	logMethodName(__func__);
}

Future<Void> FakeLogSystem::confirmEpochLive(Optional<UID> debugID) {
	logMethodName(__func__);
	return Future<Void>();
}

Future<Void> FakeLogSystem::endEpoch() {
	logMethodName(__func__);
	return Future<Void>();
}

Version FakeLogSystem::getEnd() const {
	logMethodName(__func__);
	return 0;
}

Version FakeLogSystem::getBackupStartVersion() const {
	logMethodName(__func__);
	return 0;
}

std::map<LogEpoch, ILogSystem::EpochTagsVersionsInfo> FakeLogSystem::getOldEpochTagsVersionsInfo() const {
	logMethodName(__func__);
	return std::map<LogEpoch, EpochTagsVersionsInfo>();
}

Future<Reference<ILogSystem>> FakeLogSystem::newEpoch(
    const RecruitFromConfigurationReply& recr,
    const Future<struct RecruitRemoteFromConfigurationReply>& fRemoteWorkers,
    const DatabaseConfiguration& config,
    LogEpoch recoveryCount,
    int8_t primaryLocality,
    int8_t remoteLocality,
    const std::vector<Tag>& allTags,
    const Reference<AsyncVar<bool>>& recruitmentStalled,
    TLogGroupCollectionRef tLogGroupCollection) {
	logMethodName(__func__);
	return Future<Reference<ILogSystem>>();
}

LogSystemConfig FakeLogSystem::getLogSystemConfig() const {
	logMethodName(__func__);
	return LogSystemConfig();
}

Standalone<StringRef> FakeLogSystem::getLogsValue() const {
	logMethodName(__func__);
	return Standalone<StringRef>();
}

Future<Void> FakeLogSystem::onLogSystemConfigChange() {
	logMethodName(__func__);
	return Future<Void>();
}

void FakeLogSystem::getPushLocations(VectorRef<Tag> tags, std::vector<int>& locations, bool allLocations) const {
	logMethodName(__func__);
}

bool FakeLogSystem::hasRemoteLogs() const {
	logMethodName(__func__);
	return false;
}

Tag FakeLogSystem::getRandomRouterTag() const {
	logMethodName(__func__);
	return Tag();
}

int FakeLogSystem::getLogRouterTags() const {
	logMethodName(__func__);
	return 0;
}

Tag FakeLogSystem::getRandomTxsTag() const {
	logMethodName(__func__);
	return Tag();
}

TLogVersion FakeLogSystem::getTLogVersion() const {
	logMethodName(__func__);
	return TLogVersion();
}

void FakeLogSystem::stopRejoins() {
	logMethodName(__func__);
}

Tag FakeLogSystem::getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const {
	logMethodName(__func__);
	return Tag();
}

bool FakeLogSystem::hasPseudoLocality(int8_t locality) const {
	logMethodName(__func__);
	return false;
}

Version FakeLogSystem::popPseudoLocalityTag(Tag tag, Version upTo) {
	logMethodName(__func__);
	return 0;
}

void FakeLogSystem::setBackupWorkers(const std::vector<InitializeBackupReply>& replies) {
	logMethodName(__func__);
}

bool FakeLogSystem::removeBackupWorker(const BackupWorkerDoneRequest& req) {
	logMethodName(__func__);
	return false;
}

LogEpoch FakeLogSystem::getOldestBackupEpoch() const {
	logMethodName(__func__);
	return 0;
}

void FakeLogSystem::setOldestBackupEpoch(LogEpoch epoch) {
	logMethodName(__func__);
}

#ifndef __INTEL_COMPILER
#pragma endregion FakeLogSystem
#endif

#ifndef __INTEL_COMPILER
#pragma region FakeLogSystem_CustomPeekCursor
#endif

std::unordered_map<UID, Reference<::ILogSystem::IPeekCursor>> FakeLogSystem_CustomPeekCursor::cursorIDMapping;

FakeLogSystem_CustomPeekCursor::FakeLogSystem_CustomPeekCursor(const UID& creatorDebugID_)
  : FakeLogSystem(creatorDebugID_) {
	if (cursorIDMapping.find(creatorDebugID) == cursorIDMapping.end()) {
		cursorIDMapping[creatorDebugID] = Reference<::ILogSystem::IPeekCursor>();
	}
}

Reference<::ILogSystem::IPeekCursor> FakeLogSystem_CustomPeekCursor::peekSingle(
    UID _1,
    Version _2,
    Tag _3,
    Optional<ptxn::StorageTeamID> _4,
    std::vector<std::pair<Version, Tag>> _5) {
	return cursorIDMapping[creatorDebugID];
}

Reference<::ILogSystem::IPeekCursor>& FakeLogSystem_CustomPeekCursor::getCursorByID(const UID& id) {
	if (cursorIDMapping.find(id) == cursorIDMapping.end()) {
		cursorIDMapping[id] = Reference<::ILogSystem::IPeekCursor>();
	}

	return cursorIDMapping[id];
}

#ifndef __INTEL_COMPILER
#pragma endregion FakeLogSystem_CustomPeekCursor
#endif

} // namespace ptxn::test
