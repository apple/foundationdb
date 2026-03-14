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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tracing.h"
#include "fdbserver/BackupPartitionMap.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/WaitFailure.h"
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
	Version logFolderBaseVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	AsyncVar<bool> paused; // Track if "backupPausedKey" is set.
	Reference<FlowLock> lock;
	AsyncTrigger doneTrigger;
	Database cx;
	std::vector<RangePartitionedVersionedMessage> messages;
	// Key range to partition ID map, used to determine which partition a mutation belongs to based on its key.
	KeyRangeMap<int> keyRangeToPartitionId;

	struct PerBackupInfo {
		PerBackupInfo() = default;
		PerBackupInfo(BackupRangePartitionedData* data, UID uid, Version v) : self(data), startVersion(v) {
			// Open the container and get the key ranges.
			BackupConfig config(uid);
			container = config.backupContainer().get(data->cx.getReference());
			ranges = config.backupRanges().get(data->cx.getReference());
			TraceEvent("BWRangePartitionedAddBackup", data->myId).detail("BackupID", uid).detail("Version", v);
		}

		BackupRangePartitionedData* self = nullptr;
		Future<Optional<std::vector<KeyRange>>> ranges; // Key ranges of this backup
		Future<Optional<Reference<IBackupContainer>>> container;
		// Backup request's commit version. Mutations are logged at some version after this.
		Version startVersion = invalidVersion;
		bool stopped = false;
	};

	// TODO akanksha: Add backups in this map when backup worker receives backup request.
	std::unordered_map<UID, PerBackupInfo> backups; // Backup UID to infos

	explicit BackupRangePartitionedData(UID id,
	                                    Reference<AsyncVar<ServerDBInfo> const> db,
	                                    const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), totalTags(req.totalTags), startVersion(req.startVersion),
	    endVersion(req.endVersion), recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch),
	    minKnownCommittedVersion(invalidVersion), savedVersion(req.startVersion - 1), pulledVersion(0),
	    logFolderBaseVersion(invalidVersion), paused(false),
	    lock(new FlowLock(SERVER_KNOBS->BACKUP_WORKER_LOCK_BYTES)) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True);
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

	void pop() {
		if (!logSystem.get()) {
			return;
		}
		logSystem.get()->pop(savedVersion, tag);
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

ACTOR Future<Version> pullPartitionMapFromTLog(BackupRangePartitionedData* self, PartitionMap* outPartitionMap) {
	state Reference<ILogSystem::IPeekCursor> cursor;
	state Version partitionMapVersion = invalidVersion;
	state Future<Void> logSystemChange = Void();

	loop {
		loop choose {
			when(wait(cursor ? cursor->getMore() : Never())) {
				break;
			}
			when(wait(logSystemChange)) {
				if (self->logSystem.get()) {
					cursor = self->logSystem.get()->peekSingle(self->myId, self->startVersion, self->tag);
				} else {
					cursor = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			}
		}
		wait(cursor->getMore());
		if (!cursor->hasMessage()) {
			continue;
		}
		for (; cursor->hasMessage(); cursor->nextMessage()) {
			state Version msgVersion = cursor->version().version;
			state StringRef message = cursor->getMessage();
			state VectorRef<Tag> tags = cursor->getTags();
			state Arena arena = cursor->arena();
			ArenaReader reader(arena, message, AssumeVersion(g_network->protocolVersion()));
			if (reader.protocolVersion().hasSpanContext() && SpanContextMessage::isNextIn(reader)) {
				cursor->nextMessage();
				continue;
			}
			if (reader.protocolVersion().hasOTELSpanContext() && OTELSpanContextMessage::isNextIn(reader)) {
				cursor->nextMessage();
				continue;
			}
			// TODO akanksha: Uncomment once PartitionMapMessage is implemented.
			// bool isPartitionMap = PartitionMapMessage::isNextIn(reader);
			bool isPartitionMap = true;
			if (!isPartitionMap) {
				TraceEvent(SevError, "BWRangeParitionedPartitionMapNotReceived", self->myId)
				    .detail("Version", msgVersion)
				    .detail("Tag", self->tag.toString())
				    .detail("MessageSize", message.size());
				throw worker_removed();
			}

			// TODO akanksha: 1. std::unordered_map is not supported by ArenaReader right now, need to implement custom
			// deserialization logic for it. Or we can switch to std::map which is supported by ArenaReader.
			// 2. Uncomment and update the code once the deserialization logic is implemented.
			/*
			PartitionMapMessage pmMsg;
			reader >> pmMsg;
			*outPartitionMap  = pmMsg.partitionMap;
			*/
			partitionMapVersion = msgVersion;
			return partitionMapVersion;
		}
	}
}

ACTOR Future<Void> uploadPartitionList(BackupRangePartitionedData* self, PartitionMap partitionMap) {
	state std::vector<Future<Void>> fileFutures;
	state std::unordered_map<UID, BackupRangePartitionedData::PerBackupInfo>::iterator it = self->backups.begin();

	state std::string jsonContent = serializePartitionListJSON(partitionMap);

	for (; it != self->backups.end();) {
		if (it->second.stopped || !it->second.container.get().present()) {
			TraceEvent("BWRangePartitionedRemoveContainer", self->myId).detail("BackupId", it->first);
			it = self->backups.erase(it);
			continue;
		}
		state Reference<IBackupContainer> container = it->second.container.get().get();
		fileFutures.push_back(container->writePartitionListFile(self->logFolderBaseVersion, jsonContent));
		it++;
	}
	if (fileFutures.empty()) {
		TraceEvent("BWRangePartitionedNoContainers", self->myId);
		return Void();
	}

	wait(waitForAll(fileFutures));
	return Void();
}

// TODO akanksha -> Need to figure out if
// 1. For new requests -> PartitionMap in TLOG will be same for all containers
// 2. For older epochs with different containers is PartitionMap specific to container or same for all.
// Right now assumption is that PartitionMap will be passed by TLOG with the first message after start version for both
// older epochs and newer epochs.
ACTOR Future<Void> waitAndProcessPartitionMap(BackupRangePartitionedData* self) {
	TraceEvent("BWRangeParitionedWaitingForPartitionMap", self->myId)
	    .detail("Tag", self->tag.toString())
	    .detail("StartVersion", self->startVersion);
	state PartitionMap partitionMap;

	state Version partitionMapVersion = wait(pullPartitionMapFromTLog(self, &partitionMap));
	self->logFolderBaseVersion = partitionMapVersion + 1;

	ASSERT(partitionMap.find(self->tag) != partitionMap.end());
	TraceEvent("BWRangeParitionedPulledPartitionMap", self->myId)
	    .detail("Version", partitionMapVersion)
	    .detail("NumTags", partitionMap.size())
	    .detail("Tag", self->tag.toString())
	    .detail("NumPartitions", partitionMap[self->tag].size());

	self->keyRangeToPartitionId.clear();
	ASSERT_GT(partitionMap[self->tag].size(), 0);
	for (auto& partition : partitionMap[self->tag]) {
		self->keyRangeToPartitionId.insert(partition.ranges, partition.partitionId);
	}

	state Key doneKey = backupRangePartitionedMapUploadedKeyFor(partitionMapVersion);
	state ReadYourWritesTransaction tr(self->cx);

	// TODO akanksha: Check what will be the tag id once tags are implemented for backup workers and update the
	// condition accordingly.
	// Also add background actor to clean up the done key at regular
	if (self->tag.id == 0) {
		wait(uploadPartitionList(self, partitionMap));
		TraceEvent("BWRangePartitionedPartitionMapUploaded", self->myId)
		    .detail("Version", partitionMapVersion)
		    .detail("NumBackups", self->backups.size());
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.set(doneKey, "1"_sr);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} else {
		// All other backup workers waits for done key to be set by the worker with tag id 0, then start pulling
		// mutations. This is to make sure partition map is uploaded before any worker starts pulling mutations.
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE); // More efficient for reads
				Optional<Value> v = wait(tr.get(doneKey));
				if (v.present()) {
					break;
				}
				state Future<Void> watchFuture = tr.watch(doneKey);
				wait(tr.commit());
				wait(watchFuture);
				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
	self->pulledVersion.set(partitionMapVersion);
	self->savedVersion = partitionMapVersion;
	self->pop();
	return Void();
}

// Pulls mutations from TLog servers.
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

struct PartitionMapMessage {
	PartitionMap partitionMap;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, partitionMap);
	}

	// For now there is no explicit type tag; we assume the first
	// non-span, non-OTEL message at this position is the partition map.
	static bool isNextIn(ArenaReader& r) {
		// If later you add a type byte, decode and check it here.
		return true;
	}
};

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
	PartitionMap pm;
	pm[req.routerTag].push_back(Partition(0, KeyRangeRef(""_sr, "m"_sr)));
	pm[req.routerTag].push_back(Partition(1, KeyRangeRef("m"_sr, "\xff\xff"_sr)));

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

	state PartitionMap outMap;
	state Version partitionMapVersion = wait(pullPartitionMapFromTLog(self, &outMap));

	ASSERT_EQ(partitionMapVersion, v);
	ASSERT_EQ(outMap.size(), 1);
	ASSERT(outMap.find(req.routerTag) != outMap.end());
	ASSERT_EQ(outMap[req.routerTag].size(), 2);

	self->logFolderBaseVersion = partitionMapVersion + 1;

	// Add a real backup container to test partition map upload with actual file writes
	state std::string testURL = "file://simfdb/backups/test_" + deterministicRandom()->randomUniqueID().toString();
	state Reference<IBackupContainer> realContainer = IBackupContainer::openContainer(testURL, {}, {});
	wait(realContainer->create());

	state UID backupId = deterministicRandom()->randomUniqueID();

	BackupRangePartitionedData::PerBackupInfo backupInfo;
	backupInfo.container = Optional<Reference<IBackupContainer>>(realContainer);
	backupInfo.stopped = false;
	self->backups[backupId] = backupInfo;

	wait(uploadPartitionList(self, outMap));

	printf("!!!!! Partition map upload completed successfully to %s\n", testURL.c_str());
	return Void();
}

TEST_CASE("/backup/rangePartitioned/saveMutationsToFile/basic") {
	wait(delay(0.0));

	// --- Setup worker and DB info ---
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

	// Pretend we are ready to write at version 150.
	self->logFolderBaseVersion = 200;
	self->savedVersion = 149;
	self->pulledVersion.set(150);

	// --- Create a real backup container and fake ranges future ---
	state std::string testURL =
	    "file://simfdb/backups/saveMutations_" + deterministicRandom()->randomUniqueID().toString();
	state Reference<IBackupContainer> container = IBackupContainer::openContainer(testURL, {}, {});
	wait(container->create());

	state UID backupId = deterministicRandom()->randomUniqueID();
	BackupRangePartitionedData::PerBackupInfo info;
	info.self = self;
	info.startVersion = 100;
	info.nextFileBeginVersion = invalidVersion;
	info.stopped = false;

	// Set container and ranges as already-resolved futures.
	info.container = Future<Optional<Reference<IBackupContainer>>>(Optional<Reference<IBackupContainer>>(container));
	info.ranges = Future<Optional<std::vector<KeyRange>>>(
	    Optional<std::vector<KeyRange>>(std::vector<KeyRange>{ KeyRangeRef(""_sr, "\xff"_sr) }));

	self->backups[backupId] = info;

	// --- Partition map: single partition covering all keys, id = 0 ---
	self->keyRangeToPartitionId.insert(KeyRangeRef(""_sr, "\xff"_sr), 7);
	self->partitionToKeyRange[7] = KeyRangeRef(""_sr, "\xff"_sr);

	// Compute keyRangeToBackupAssignment from backups + partition map.
	wait(computeKeyRangeToBackupAssignment(self));

	// At this point, keyRangeToBackupAssignment should map all keys
	// to (backupId, partitionId=7) and isAllInfoReady() should be true.
	ASSERT(self->isAllInfoReady());

	// --- Prepare a single mutation message at version 150 ---
	state Arena arena;
	MutationRef m(MutationRef::Type::SetValue, "k"_sr, "v"_sr);
	BinaryWriter wr(AssumeVersion(g_network->protocolVersion()));
	wr << m;
	Standalone<StringRef> payload = wr.toValue();

	LogMessageVersion logVer(150);
	VectorRef<Tag> tags;
	tags.push_back(arena, req.routerTag);

	RangePartitionedVersionedMessage msg(logVer, payload, tags, arena);
	self->messages.push_back(msg);

	state Version lastVersionInFile = 150;
	state int numMsg = 1;

	// --- Call saveMutationsToFile ---
	wait(saveMutationsToFile(self, lastVersionInFile, numMsg));

	printf("!!!!! saveMutationsToFile completed for %s\n", testURL.c_str());

	return Void();
}
