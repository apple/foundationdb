/*
 * MockLogSystem.cpp
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

#include "fdbserver/MockLogSystem.h"

#include "flow/actorcompiler.h" // This must be the last #include.

const static bool LOG_METHOD_CALLS = false;

static void logMethodName(std::string methodName) {
	if (LOG_METHOD_CALLS) {
		std::cout << "MockLogSystem::" << methodName << std::endl;
	}
}

MockLogSystem::MockLogSystem() {
	logMethodName(__func__);
}

MockLogSystem::MockLogSystem(const MockLogSystem& that) : cursor(that.cursor) {
	logMethodName(__func__);
}

MockLogSystem& MockLogSystem::operator=(const MockLogSystem& that) {
	logMethodName(__func__);
	cursor = that.cursor;
	return *this;
}

void MockLogSystem::addref() {
	logMethodName(__func__);
	ReferenceCounted<MockLogSystem>::addref();
}

void MockLogSystem::delref() {
	logMethodName(__func__);
	ReferenceCounted<MockLogSystem>::delref();
}

std::string MockLogSystem::describe() const {
	logMethodName(__func__);
	return std::string();
}

UID MockLogSystem::getDebugID() const {
	logMethodName(__func__);
	return UID();
}

void MockLogSystem::toCoreState(DBCoreState& coreState) {
	logMethodName(__func__);
}

bool MockLogSystem::remoteStorageRecovered() {
	logMethodName(__func__);
	return false;
}

Future<Void> MockLogSystem::onCoreStateChanged() {
	logMethodName(__func__);
	return Future<Void>();
}

void MockLogSystem::coreStateWritten(const DBCoreState& newState) {
	logMethodName(__func__);
}

Future<Void> MockLogSystem::onError() {
	logMethodName(__func__);
	return Future<Void>();
}

Future<Version> MockLogSystem::push(std::vector<Version> prevVersions,
                                    Version version,
                                    Version knownCommittedVersion,
                                    Version minKnownCommittedVersion,
                                    struct LogPushData& data,
                                    const SpanID& spanContext,
                                    Optional<UID> debugID,
                                    std::vector<ptxn::TLogGroupID> tLogGroups) {
	logMethodName(__func__);
	return Future<Version>();
}

Reference<ILogSystem::IPeekCursor> MockLogSystem::peek(UID dbgid,
                                                       Version begin,
                                                       Optional<Version> end,
                                                       Tag tag,
                                                       bool parallelGetMore) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

Reference<ILogSystem::IPeekCursor> MockLogSystem::peek(UID dbgid,
                                                       Version begin,
                                                       Optional<Version> end,
                                                       std::vector<Tag> tags,
                                                       bool parallelGetMore) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

// Return mocked cursor.
Reference<ILogSystem::IPeekCursor> MockLogSystem::peekSingle(UID dbgid,
                                                             Version begin,
                                                             Tag tag,
                                                             Optional<ptxn::StorageTeamID> storageTeam,
                                                             std::vector<std::pair<Version, Tag>> history) {
	logMethodName(__func__);
	return cursor;
}

Reference<ILogSystem::IPeekCursor> MockLogSystem::peekLogRouter(UID dbgid, Version begin, Tag tag) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

Reference<ILogSystem::IPeekCursor> MockLogSystem::peekTxs(UID dbgid,
                                                          Version begin,
                                                          int8_t peekLocality,
                                                          Version localEnd,
                                                          bool canDiscardPopped) {
	logMethodName(__func__);
	return Reference<IPeekCursor>();
}

Future<Version> MockLogSystem::getTxsPoppedVersion() {
	logMethodName(__func__);
	return Future<Version>();
}

Version MockLogSystem::getKnownCommittedVersion() {
	logMethodName(__func__);
	return 0;
}

Future<Void> MockLogSystem::onKnownCommittedVersionChange() {
	logMethodName(__func__);
	return Future<Void>();
}

void MockLogSystem::popTxs(Version upTo, int8_t popLocality) {
	logMethodName(__func__);
}

void MockLogSystem::pop(Version upTo, Tag tag, Version knownCommittedVersion, int8_t popLocality) {
	logMethodName(__func__);
}

Future<Void> MockLogSystem::confirmEpochLive(Optional<UID> debugID) {
	logMethodName(__func__);
	return Future<Void>();
}

Future<Void> MockLogSystem::endEpoch() {
	logMethodName(__func__);
	return Future<Void>();
}

Version MockLogSystem::getEnd() const {
	logMethodName(__func__);
	return 0;
}

Version MockLogSystem::getBackupStartVersion() const {
	logMethodName(__func__);
	return 0;
}

std::map<LogEpoch, ILogSystem::EpochTagsVersionsInfo> MockLogSystem::getOldEpochTagsVersionsInfo() const {
	logMethodName(__func__);
	return std::map<LogEpoch, EpochTagsVersionsInfo>();
}

Future<Reference<ILogSystem>> MockLogSystem::newEpoch(
    const RecruitFromConfigurationReply& recr,
    const Future<struct RecruitRemoteFromConfigurationReply>& fRemoteWorkers,
    const DatabaseConfiguration& config,
    LogEpoch recoveryCount,
    int8_t primaryLocality,
    int8_t remoteLocality,
    const vector<Tag>& allTags,
    const Reference<AsyncVar<bool>>& recruitmentStalled,
    Reference<TLogGroupCollection> tLogGroupCollection) {
	logMethodName(__func__);
	return Future<Reference<ILogSystem>>();
}

LogSystemConfig MockLogSystem::getLogSystemConfig() const {
	logMethodName(__func__);
	return LogSystemConfig();
}

Standalone<StringRef> MockLogSystem::getLogsValue() const {
	logMethodName(__func__);
	return Standalone<StringRef>();
}

Future<Void> MockLogSystem::onLogSystemConfigChange() {
	logMethodName(__func__);
	return Future<Void>();
}

void MockLogSystem::getPushLocations(VectorRef<Tag> tags, vector<int>& locations, bool allLocations) const {
	logMethodName(__func__);
}

bool MockLogSystem::hasRemoteLogs() const {
	logMethodName(__func__);
	return false;
}

Tag MockLogSystem::getRandomRouterTag() const {
	logMethodName(__func__);
	return Tag();
}

int MockLogSystem::getLogRouterTags() const {
	logMethodName(__func__);
	return 0;
}

Tag MockLogSystem::getRandomTxsTag() const {
	logMethodName(__func__);
	return Tag();
}

TLogVersion MockLogSystem::getTLogVersion() const {
	logMethodName(__func__);
	return TLogVersion();
}

void MockLogSystem::stopRejoins() {
	logMethodName(__func__);
}

Tag MockLogSystem::getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const {
	logMethodName(__func__);
	return Tag();
}

bool MockLogSystem::hasPseudoLocality(int8_t locality) const {
	logMethodName(__func__);
	return false;
}

Version MockLogSystem::popPseudoLocalityTag(Tag tag, Version upTo) {
	logMethodName(__func__);
	return 0;
}

void MockLogSystem::setBackupWorkers(const std::vector<InitializeBackupReply>& replies) {
	logMethodName(__func__);
}

bool MockLogSystem::removeBackupWorker(const BackupWorkerDoneRequest& req) {
	logMethodName(__func__);
	return false;
}

LogEpoch MockLogSystem::getOldestBackupEpoch() const {
	logMethodName(__func__);
	return 0;
}

void MockLogSystem::setOldestBackupEpoch(LogEpoch epoch) {
	logMethodName(__func__);
}
