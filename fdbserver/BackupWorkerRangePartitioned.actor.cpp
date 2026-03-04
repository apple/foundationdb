/*
 * BackupWorkerRangePartitioned.actor.cpp
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

#include "fdbserver/WaitFailure.h"
#include "fdbserver/LogSystem.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/Tracing.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define SevDebugMemory SevVerbose

struct RangePartitionedVersionedMessage {
	LogMessageVersion version;
	StringRef message;
	VectorRef<Tag> tags;
	Arena arena;

	RangePartitionedVersionedMessage(LogMessageVersion v, StringRef m, const VectorRef<Tag>& t, const Arena& a)
	  : version(v), message(m), tags(t), arena(a) {}

	Version getVersion() const { return version.version; }
	size_t getEstimatedSize() const { return message.size() + TagsAndMessage::getHeaderSize(6); }
};

struct BackupRangePartitionedData {
	const UID myId;
	const Tag tag; // tag for this backup worker
	const int totalTags; // Total backup worker tags
	const Version startVersion; // This worker's start version
	const Optional<Version> endVersion; // old epoch's end version (inclusive), or empty for current epoch
	const LogEpoch recruitedEpoch; // current epoch whose tLogs are receiving mutations
	const LogEpoch backupEpoch; // the epoch workers should pull mutations
	Version minKnownCommittedVersion;
	Version savedVersion; // Largest version saved to blob storage
	NotifiedVersion pulledVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	AsyncVar<bool> paused; // Track if "backupPausedKey" is set.
	Reference<FlowLock> lock;
	AsyncTrigger doneTrigger;
	std::vector<RangePartitionedVersionedMessage> messages;
	// Key range to partition ID map, used to determine which partition a mutation belongs to based on its key.
	KeyRangeMap<uint64_t> keyRangeToPartition;

	explicit BackupRangePartitionedData(UID id,
	                                    Reference<AsyncVar<ServerDBInfo> const> db,
	                                    const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), totalTags(req.totalTags), startVersion(req.startVersion),
	    endVersion(req.endVersion), recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch),
	    minKnownCommittedVersion(invalidVersion), savedVersion(req.startVersion - 1), pulledVersion(0), paused(false),
	    lock(new FlowLock(SERVER_KNOBS->BACKUP_WORKER_LOCK_BYTES)) {

		// TODO akanksha: Initialize keyRangeToPartition map. Each backworker will receive a partition map
		// which is BackupTag ->  list<Partition> mapping and Partition contains partition Id and key range. We can
		// construct the keyRangeToPartition map from that.
	}

	bool pullFinished() const { return endVersion.present() && pulledVersion.get() > endVersion.get(); }

	void eraseMessagesAfterEndVersion() {
		ASSERT(endVersion.present());
		const Version ver = endVersion.get();
		while (!messages.empty()) {
			if (messages.back().getVersion() > ver) {
				size_t bytes = messages.back().getEstimatedSize();
				TraceEvent(SevDebugMemory, "BWRangePartitionedMemory", myId).detail("Release", bytes);
				lock->release(bytes);
				messages.pop_back();
			} else {
				break;
			}
		}
	}
};

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                                LogEpoch recoveryCount,
                                BackupRangePartitionedData* self) {
	loop {
		bool isDisplaced =
		    db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED;
		if (isDisplaced) {
			TraceEvent("BWRangePartitionedDisplaced", self->myId)
			    .detail("RecoveryCount", recoveryCount)
			    .detail("RecoveryState", (int)db->get().recoveryState);
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> backupWorkerRangePartitioned(BackupInterface interf,
                                                InitializeBackupRequest req,
                                                Reference<AsyncVar<ServerDBInfo> const> db) {
	state BackupRangePartitionedData self(interf.id(), db, req);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection(addActor.getFuture());
	state Future<Void> dbInfoChange = Void();
	state Future<Void> done;

	TraceEvent("BWRangePartitionedStart", self.myId)
	    .detail("Tag", req.routerTag.toString())
	    .detail("TotalTags", req.totalTags)
	    .detail("StartVersion", req.startVersion)
	    .detail("EndVersion", req.endVersion.present() ? req.endVersion.get() : -1)
	    .detail("LogEpoch", req.recruitedEpoch)
	    .detail("BackupEpoch", req.backupEpoch);

	try {
		addActor.send(checkRemoved(db, req.recruitedEpoch, &self));
		addActor.send(waitFailureServer(interf.waitFailure.getFuture()));

		loop choose {
			when(wait(dbInfoChange)) {
				dbInfoChange = db->onChange();
				Reference<ILogSystem> ls = ILogSystem::fromServerDBInfo(self.myId, db->get(), true);
			}
			when(wait(done)) {
				TraceEvent("BWRangePartitionedDone", self.myId).detail("BackupEpoch", self.backupEpoch);
				// Notify master so that this worker can be removed from log system, then this
				// worker (for an old epoch's unfinished work) can safely exit.
				wait(brokenPromiseToNever(db->get().clusterInterface.notifyBackupWorkerDone.getReply(
				    BackupWorkerDoneRequest(self.myId, self.backupEpoch))));
				break;
			}
			when(wait(error)) {}
		}
	} catch (Error& e) {
		state Error err = e;
		if (e.code() == error_code_worker_removed) {
			try {
				wait(done);
			} catch (Error& e) {
				TraceEvent("BWRangePartitionedShutdownError", self.myId).errorUnsuppressed(e);
			}
		}
		TraceEvent("BWRangePartitionedTerminated", self.myId).errorUnsuppressed(err);
		if (err.code() != error_code_actor_cancelled && err.code() != error_code_worker_removed) {
			throw err;
		}
	}
	return Void();
}

// Pulls data from TLog servers.
ACTOR Future<Void> pullAsyncData(BackupRangePartitionedData* self) {
	state Future<Void> logSystemChange = Void();
	state Reference<ILogSystem::IPeekCursor> cursor;

	state Version tagAt = std::max({ self->pulledVersion.get(), self->startVersion, self->savedVersion });

	TraceEvent("BWRangePartitionedPull", self->myId)
	    .detail("Tag", self->tag)
	    .detail("Version", tagAt)
	    .detail("StartVersion", self->startVersion)
	    .detail("SavedVersion", self->savedVersion);

	loop {
		while (self->paused.get()) {
			wait(self->paused.onChange());
		}

		loop choose {
			when(wait(cursor ? cursor->getMore(TaskPriority::TLogCommit) : Never())) {
				DisabledTraceEvent("BWRangePartitionedGotMore", self->myId)
				    .detail("Tag", self->tag)
				    .detail("CursorVersion", cursor->version().version);
				break;
			}
			when(wait(logSystemChange)) {
				if (self->logSystem.get()) {
					// TODO akanksha: Use peekSingle as of now instead of peekLogRouter and later confirm if it works as
					// expected.
					cursor = self->logSystem.get()->peekSingle(self->myId, tagAt, self->tag);
				} else {
					cursor = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			}
		}

		if (cursor->popped() > 0) {
			TraceEvent(SevError, "BWRangePartitionedDataPopped", self->myId)
			    .detail("Popped", cursor->popped())
			    .detail("Expected", tagAt);
			throw worker_removed();
		}

		self->minKnownCommittedVersion =
		    std::max(self->minKnownCommittedVersion, cursor->getMinKnownCommittedVersion());
		state int64_t peekedBytes = 0;

		// Hold messages until we know how many we can take, self->messages always
		// contains messages that we have reserved memory for. Therefore, lock->release()
		// will always encounter message with reserved memory.
		state std::vector<RangePartitionedVersionedMessage> tmpMessages;

		// Messages may be prefetched in peek here, but uncommitted messages should not be uploaded in uploadData().
		while (cursor->hasMessage()) {
			auto msg = RangePartitionedVersionedMessage(
			    cursor->version(), cursor->getMessage(), cursor->getTags(), cursor->arena());
			tmpMessages.emplace_back(std::move(msg));
			peekedBytes += tmpMessages.back().getEstimatedSize();
			cursor->nextMessage();
		}

		if (peekedBytes > 0) {
			TraceEvent(SevDebugMemory, "BWRangePartitionedMemory", self->myId)
			    .detail("Take", peekedBytes)
			    .detail("Current", self->lock->activePermits());
			wait(self->lock->take(TaskPriority::DefaultYield, peekedBytes));
			self->messages.insert(self->messages.end(),
			                      std::make_move_iterator(tmpMessages.begin()),
			                      std::make_move_iterator(tmpMessages.end()));
		}

		tagAt = cursor->version().version;
		self->pulledVersion.set(tagAt);
		TraceEvent("BWRangePartitionedGot", self->myId).suppressFor(1.0).detail("LatestPulledVersion", tagAt);

		// For older epochs, we may have an end version to stop at.
		if (self->pullFinished()) {
			self->eraseMessagesAfterEndVersion();
			self->doneTrigger.trigger();
			TraceEvent("BWRangePartitionedFinishPull", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("VersionGot", tagAt)
			    .detail("EndVersion", self->endVersion.get())
			    .detail("LogEpoch", self->recruitedEpoch)
			    .detail("BackupEpoch", self->backupEpoch);
			return Void();
		}
		wait(yield());
	}
}

// ============================================================================
// Unit Tests
// ============================================================================

#include "flow/UnitTest.h"

// --- Minimal test log message type ---

struct TestLogMessage {
	LogMessageVersion version;
	Standalone<StringRef> payload;
	Standalone<VectorRef<Tag>> tags;
};

// --- Minimal IPeekCursor that returns a fixed list of messages ---

struct TestPeekCursor : ILogSystem::IPeekCursor, ReferenceCounted<TestPeekCursor> {
	std::vector<TestLogMessage> msgs;
	int idx = 0;
	LogMessageVersion invalidVer;
	ArenaReader readerObj;
	ProtocolVersion protoVersion;

	explicit TestPeekCursor(std::vector<TestLogMessage> m)
	  : msgs(std::move(m)), invalidVer(invalidVersion),
	    readerObj(Arena(), StringRef(), AssumeVersion(g_network->protocolVersion())),
	    protoVersion(g_network->protocolVersion()) {}

	void addref() override { ReferenceCounted<TestPeekCursor>::addref(); }
	void delref() override { ReferenceCounted<TestPeekCursor>::delref(); }

	Future<Void> getMore(TaskPriority = TaskPriority::DefaultYield) override { return Void(); }

	bool hasMessage() const override { return idx < (int)msgs.size(); }

	void nextMessage() override {
		if (idx < (int)msgs.size())
			++idx;
	}

	const LogMessageVersion& version() const override {
		if (!hasMessage())
			return invalidVer;
		return msgs[idx].version;
	}

	StringRef getMessage() override {
		ASSERT(hasMessage());
		return msgs[idx].payload;
	}

	VectorRef<Tag> getTags() const override {
		ASSERT(hasMessage());
		return msgs[idx].tags;
	}

	Arena& arena() override {
		ASSERT(hasMessage());
		return msgs[idx].payload.arena();
	}

	Version popped() const override { return 0; }

	Version getMinKnownCommittedVersion() const override { return 0; }

	Future<Void> onFailed() const override { return Never(); }

	bool isActive() const override { return true; }

	Reference<IPeekCursor> cloneNoMore() override {
		auto c = makeReference<TestPeekCursor>(msgs);
		c->idx = idx;
		return c.castTo<IPeekCursor>();
	}

	void setProtocolVersion(ProtocolVersion v) override { protoVersion = v; }

	ArenaReader* reader() override { return &readerObj; }

	Optional<UID> getPrimaryPeekLocation() const override { return Optional<UID>(); }
	Optional<UID> getCurrentPeekLocation() const override { return Optional<UID>(); }

	bool isExhausted() const override { return false; }
	void advanceTo(LogMessageVersion) override {}
	StringRef getMessageWithTags() override { return getMessage(); }
};

// --- Minimal ILogSystem that hands out the test cursor ---

struct TestLogSystem : ILogSystem, ReferenceCounted<TestLogSystem> {
	std::vector<TestLogMessage> msgs;

	explicit TestLogSystem(std::vector<TestLogMessage> m) : msgs(std::move(m)) {}

	virtual ~TestLogSystem() {}

	void addref() override { ReferenceCounted<TestLogSystem>::addref(); }
	void delref() override { ReferenceCounted<TestLogSystem>::delref(); }

	Reference<IPeekCursor> peekSingle(UID, Version, Tag, std::vector<std::pair<Version, Tag>>) override {
		return makeReference<TestPeekCursor>(msgs).castTo<IPeekCursor>();
	}

	void pop(Version, Tag, Version = 0, int8_t = tagLocalityInvalid) override {}

	// Unused virtuals: stub as no-ops.
	LogSystemType getType() const { return LogSystemType::empty; }
	void stopRejoins() override {}
	Future<Void> onCoreStateChanged() const override { return Never(); }
	void coreStateWritten(DBCoreState const&) override {}
	Future<Void> onError() const override { return Never(); }
	Future<Version> push(const PushVersionSet&,
	                     LogPushData&,
	                     SpanContext const&,
	                     Optional<UID>,
	                     Optional<std::unordered_map<uint16_t, Version>>) override {
		return Version(0);
	}
	Reference<IPeekCursor> peek(UID, Version, Optional<Version>, Tag, bool) override {
		return Reference<IPeekCursor>();
	}
	Reference<IPeekCursor> peek(UID, Version, Optional<Version>, std::vector<Tag>, bool) override {
		return Reference<IPeekCursor>();
	}
	Reference<IPeekCursor> peekLogRouter(UID,
	                                     Version,
	                                     Tag,
	                                     bool,
	                                     Optional<Version>,
	                                     const Optional<std::map<uint8_t, std::vector<uint16_t>>>&) override {
		return Reference<IPeekCursor>();
	}
	Reference<IPeekCursor> peekTxs(UID, Version, int8_t, Version, bool) override { return Reference<IPeekCursor>(); }
	Version getKnownCommittedVersion() override { return 0; }
	Future<Void> onKnownCommittedVersionChange() override { return Never(); }
	void popTxs(Version, int8_t) override {}
	Future<Void> confirmEpochLive(Optional<UID>) override { return Void(); }
	Future<Void> endEpoch() override { return Void(); }
	LogEpoch getOldestBackupEpoch() const override { return 0; }
	void setBackupWorkers(const std::vector<InitializeBackupReply>&) override {}
	bool removeBackupWorker(const BackupWorkerDoneRequest&) override { return false; }
	LogSystemConfig getLogSystemConfig() const override { return LogSystemConfig(); }
	Standalone<StringRef> getLogsValue() const override { return Standalone<StringRef>(); }
	Future<Version> getTxsPoppedVersion() override { return Version(0); }
	Version getEnd() const override { return 0; }
	Version getBackupStartVersion() const override { return 0; }
	std::string describe() const override { return "TestLogSystem"; }
	UID getDebugID() const override { return UID(); }
	void toCoreState(DBCoreState&) const override {}
	bool remoteStorageRecovered() const override { return true; }
	void purgeOldRecoveredGenerationsCoreState(DBCoreState&) override {}
	void purgeOldRecoveredGenerationsInMemory(const DBCoreState&) override {}
	std::map<LogEpoch, EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const override { return {}; }
	LogSystemType getLogSystemType() const override { return LogSystemType::empty; }
	Future<Void> onLogSystemConfigChange() override { return Never(); }
	void updateLogRouter(int, int, TLogInterface const&) override {}
	std::vector<Reference<LocalitySet>> getPushLocationsForTags(std::vector<int>&) const override { return {}; }
	bool hasRemoteLogs() const override { return false; }
	Tag getRandomRouterTag() const override { return Tag(); }
	int getLogRouterTags() const override { return 0; }
	Tag getRandomTxsTag() const override { return Tag(); }
	TLogVersion getTLogVersion() const override { return TLogVersion::DEFAULT; }
	Tag getPseudoPopTag(Tag, ProcessClass::ClassType) const override { return Tag(); }
	bool hasPseudoLocality(int8_t) const override { return false; }
	Version popPseudoLocalityTag(Tag, Version) override { return 0; }
	void setOldestBackupEpoch(LogEpoch) override {}

	Future<Reference<ILogSystem>> newEpoch(RecruitFromConfigurationReply const&,
	                                       Future<RecruitRemoteFromConfigurationReply> const&,
	                                       DatabaseConfiguration const&,
	                                       LogEpoch,
	                                       Version,
	                                       int8_t,
	                                       int8_t,
	                                       std::vector<Tag> const&,
	                                       Reference<AsyncVar<bool>> const&) override {
		return Future<Reference<ILogSystem>>();
	}

	void getPushLocations(VectorRef<Tag>,
	                      std::vector<int>&,
	                      bool = false,
	                      Optional<std::vector<Reference<LocalitySet>>> =
	                          Optional<std::vector<Reference<LocalitySet>>>()) const override {}
};

// --- Unit test: pullPartitionMapFromTLog returns message version ---

TEST_CASE("/backup/rangePartitioned/pullPartitionMap/basic") {
	wait(delay(0.0));

	state UID workerId = deterministicRandom()->randomUniqueID();
	state InitializeBackupRequest req;
	req.routerTag = Tag(1, 0);
	req.totalTags = 1;
	req.startVersion = 100;
	req.endVersion = Optional<Version>();
	req.recruitedEpoch = 1;
	req.backupEpoch = 1;

	state ServerDBInfo dbi;
	state Reference<AsyncVar<ServerDBInfo> const> dbVar = makeReference<AsyncVar<ServerDBInfo> const>(dbi);

	state BackupRangePartitionedData* self = new BackupRangePartitionedData(workerId, dbVar, req);

	// Prepare a single dummy log message at version 150.
	state Version v = 150;
	// Build a small partition map: this worker's tag has two ranges.
	std::map<Tag, std::vector<PartitionInfo>> pm;
	pm[req.routerTag].push_back(PartitionInfo(0, KeyRangeRef(""_sr, "m"_sr)));
	pm[req.routerTag].push_back(PartitionInfo(1, KeyRangeRef("m"_sr, "\xff\xff"_sr)));

	PartitionMapMessage pmMsg;
	pmMsg.partitionMap = pm;

	// Serialize PartitionMapMessage into a payload using the current protocol version.
	Standalone<StringRef> payload;
	{
		BinaryWriter wr(AssumeVersion(g_network->protocolVersion()));
		wr << pmMsg;
		payload = wr.toValue();
	}

	Standalone<VectorRef<Tag>> tags;
	tags.push_back(tags.arena(), req.routerTag);

	std::vector<TestLogMessage> msgs;
	TestLogMessage m;
	m.version = LogMessageVersion(v);
	m.payload = payload;
	m.tags = tags;
	msgs.push_back(m);

	self->logSystem.set(Reference<ILogSystem>(new TestLogSystem(std::move(msgs))));

	state std::map<Tag, std::vector<PartitionInfo>> outMap;
	state Version got = wait(pullPartitionMapFromTLog(self, &outMap));

	ASSERT_EQ(got, v);
	ASSERT_EQ(outMap.size(), 1);
	ASSERT(outMap.find(req.routerTag) != outMap.end());
	ASSERT_EQ(outMap[req.routerTag].size(), 2);

	// Add a real backup container to test partition map upload with actual file writes
	state std::string testURL = "file://simfdb/backups/test_" + deterministicRandom()->randomUniqueID().toString();
	state Reference<IBackupContainer> realContainer = IBackupContainer::openContainer(testURL, {}, {});
	wait(realContainer->create());

	state UID backupId = deterministicRandom()->randomUniqueID();

	BackupRangePartitionedData::PerBackupInfo backupInfo;
	backupInfo.container = Optional<Reference<IBackupContainer>>(realContainer);
	backupInfo.stopped = false;
	self->backups[backupId] = backupInfo;

	wait(uploadPartitionMap(self, got, outMap));

	printf("!!!!! Partition map upload completed successfully to %s\n", testURL.c_str());
	return Void();
}
