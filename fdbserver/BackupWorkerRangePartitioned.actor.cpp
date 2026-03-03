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

	// Returns true if the message is a mutation that could be backed up (normal keys, system key backup ranges, or the
	// metadata version key).
	bool isCandidateBackupMessage(MutationRef* m) {
		// TODO akanksha: Implement this function to filter out messages that are not mutations or not relevant to
		// backup. Need to figure out the what those message can be.
		return true;
	}
};

struct RangePartitionedLogFileInfo {
	UID backupUid;
	int32_t partitionId;
	KeyRange fileKeyRange;
	Version beginVersion;
	Version endVersion; // exclusive
	Reference<IBackupFile> file;
	int64_t blockEnd = 0;
};

struct PartitionInfo {
	int32_t partitionId;
	KeyRange ranges;

	PartitionInfo() : partitionId(-1) {}
	PartitionInfo(int32_t id, KeyRange r) : partitionId(id), ranges(r) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, partitionId, ranges);
	}
};

struct BackupRangePartitionedData {
	const UID myId;
	const Tag tag; // tag for this backup worker
	const int totalTags; // Total backup worker tags
	const Version startVersion; // This worker's start version
	const Optional<Version> endVersion; // old epoch's end version (inclusive), or empty for current epoch
	const LogEpoch recruitedEpoch; // current epoch whose tLogs are receiving mutations
	const LogEpoch backupEpoch; // the epoch workers should pull mutations
	// TODO akanksha: Check if minKnownCommittedVersion is needed after or remove it.
	Version minKnownCommittedVersion;
	Version savedVersion; // Largest version saved to blob storage
	NotifiedVersion pulledVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	AsyncVar<bool> paused; // Track if "backupPausedKey" is set.
	Reference<FlowLock> lock;
	AsyncTrigger doneTrigger;
	Database cx;
	std::vector<RangePartitionedVersionedMessage> messages;
	// Key range to partition ID map, used to determine which partition a mutation belongs to based on its key.
	KeyRangeMap<int> keyRangeToPartition;
	std::unordered_map<int, KeyRange>
	    partitionToKeyRange; // Partition ID to key range map for easy lookup of partition's key range.

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
		// The next log's begin version.
		Version nextFileBeginVersion = invalidVersion;
		bool stopped = false;

		bool isReady() const { return stopped || (container.isReady() && ranges.isReady()); }

		Future<Void> waitReady() {
			if (stopped)
				return Void();
			return _waitReady(this);
		}

		ACTOR static Future<Void> _waitReady(PerBackupInfo* info) {
			wait(success(info->container) && success(info->ranges));
			return Void();
		}
	};

	// TODO akanksha: Add backups in this map when backup worker receives backup request.
	std::unordered_map<UID, PerBackupInfo> backups; // Backup UID to infos

	explicit BackupRangePartitionedData(UID id,
	                                    Reference<AsyncVar<ServerDBInfo> const> db,
	                                    const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), totalTags(req.totalTags), startVersion(req.startVersion),
	    endVersion(req.endVersion), recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch),
	    minKnownCommittedVersion(invalidVersion), savedVersion(req.startVersion - 1), pulledVersion(0), paused(false),
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

	// Inserts a backup's single range into rangeMap.
	template <class T>
	void insertRange(KeyRangeMap<std::set<T>>& keyRangeMap, KeyRangeRef range, T value) {
		for (auto& logRange : keyRangeMap.modify(range)) {
			logRange->value().insert(value);
		}
		for (auto& logRange : keyRangeMap.modify(singleKeyRange(metadataVersionKey))) {
			logRange->value().insert(value);
		}
		TraceEvent("BackupWorkerInsertRange", myId)
		    .detail("Value", value)
		    .detail("Begin", range.begin)
		    .detail("End", range.end);
	}

	// Finds the intersection between a vector of ranges and a target range.
	// Returns the union of all intersecting portions as a single range.
	// Returns empty Optional if there are no intersections.
	Optional<KeyRange> getKeyRangeIntersection(const std::vector<KeyRange>& ranges, const KeyRange& target) {
		Optional<KeyRange> result;

		for (const auto& range : ranges) {
			KeyRange intersection = range & target;
			if (intersection.empty()) {
				continue;
			}

			if (!result.present()) {
				result = intersection;
			} else {
				KeyRef newBegin = std::min(result.get().begin, intersection.begin);
				KeyRef newEnd = std::max(result.get().end, intersection.end);
				result = KeyRange(KeyRangeRef(newBegin, newEnd));
			}
		}
		return result;
	}

	void pop() {
		if (!logSystem.get()) {
			return;
		}
		logSystem.get()->pop(savedVersion, tag);
	}

	ACTOR static Future<Void> _waitAllInfoReady(BackupRangePartitionedData* self) {
		std::vector<Future<Void>> all;
		for (auto it = self->backups.begin(); it != self->backups.end();) {
			if (it->second.stopped) {
				TraceEvent("BWRangeParitionedRemoveStoppedContainer", self->myId).detail("BackupId", it->first);
				it = self->backups.erase(it);
				continue;
			}
			all.push_back(it->second.waitReady());
			it++;
		}
		wait(waitForAll(all));
		return Void();
	}

	Future<Void> waitAllInfoReady() { return _waitAllInfoReady(this); }

	bool isAllInfoReady() const {
		for (const auto& [uid, info] : backups) {
			if (!info.isReady())
				return false;
		}
		return true;
	}
};

std::string serializePartitionMapJSON(const std::unordered_map<Tag, std::vector<PartitionInfo>>& partitionMap) {
	JsonBuilderObject root;
	JsonBuilderArray partitionsArray;
	for (const auto& [tag, partitions] : partitionMap) {
		for (const auto& partition : partitions) {
			JsonBuilderObject partitionObj;
			partitionObj["tagId"] = tag.id;
			partitionObj["partitionId"] = partition.partitionId;
			partitionObj["beginKey"] = partition.ranges.begin.printable();
			partitionObj["endKey"] = partition.ranges.end.printable();
			partitionsArray.push_back(partitionObj);
		}
	}
	root["partitions"] = partitionsArray;
	root["totalTags"] = partitionMap.size();
	root["totalPartitions"] = partitionsArray.size();
	return root.getJson();
}

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

ACTOR Future<Version> pullPartitionMapFromTLog(BackupRangePartitionedData* self,
                                               std::unordered_map<Tag, std::vector<PartitionInfo>>* outPartitionMap) {
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

ACTOR Future<Void> uploadPartitionMap(BackupRangePartitionedData* self,
                                      Version partitionMapVersion,
                                      std::unordered_map<Tag, std::vector<PartitionInfo>> partitionMap) {
	state std::vector<Future<Void>> fileFutures;
	state std::unordered_map<UID, BackupRangePartitionedData::PerBackupInfo>::iterator it = self->backups.begin();

	state std::string jsonContent = serializePartitionMapJSON(partitionMap);

	for (; it != self->backups.end();) {
		if (it->second.stopped || !it->second.container.get().present()) {
			TraceEvent("BWRangePartitionedRemoveContainer", self->myId).detail("BackupId", it->first);
			it = self->backups.erase(it);
			continue;
		}
		state Reference<IBackupContainer> container = it->second.container.get().get();
		fileFutures.push_back(container->writePartitionMapFile(partitionMapVersion + 1, jsonContent));
		it++;
	}
	if (fileFutures.empty()) {
		TraceEvent("BWRangePartitionedNoContainers", self->myId).detail("Version", partitionMapVersion);
		return Void();
	}

	wait(waitForAll(fileFutures));
	TraceEvent("BWRangePartitionedPartitionMapUploaded", self->myId)
	    .detail("Version", partitionMapVersion)
	    .detail("NumBackups", self->backups.size());
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
	state std::unordered_map<Tag, std::vector<PartitionInfo>> partitionMap;

	state Version partitionMapVersion = wait(pullPartitionMapFromTLog(self, &partitionMap));

	ASSERT(partitionMap.find(self->tag) != partitionMap.end());
	TraceEvent("BWRangeParitionedPulledPartitionMap", self->myId)
	    .detail("Version", partitionMapVersion)
	    .detail("NumTags", partitionMap.size())
	    .detail("Tag", self->tag.toString())
	    .detail("NumPartitions", partitionMap[self->tag].size());

	self->keyRangeToPartition.clear();
	ASSERT_GT(partitionMap[self->tag].size(), 0);
	for (auto& partition : partitionMap[self->tag]) {
		self->keyRangeToPartition.insert(partition.ranges, partition.partitionId);
	}

	state Key doneKey = backupRangePartitionedMapUploadedKeyFor(partitionMapVersion);
	state ReadYourWritesTransaction tr(self->cx);

	// TODO akanksha: Check what will be the tag id once tags are implemented for backup workers and update the
	// condition accordingly.
	if (self->tag.id == 0) {
		wait(uploadPartitionMap(self, partitionMapVersion, partitionMap));
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

	if (self->tag.id == 0) {
		// Wait to give other workers time to read the key
		wait(delay(5.0));
		loop {
			try {
				tr.reset();
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.clear(doneKey);
				wait(tr.commit());
				TraceEvent("BWRangePartitionedMapUploadedKeyCleared", self->myId)
				    .detail("Version", partitionMapVersion);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

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

ACTOR Future<Void> writeFileHeader(Reference<IBackupFile> logFile, int32_t partitionId, KeyRange range) {
	wait(logFile->append((uint8_t*)&RANGE_PARTITIONED_MLOG_VERSION, sizeof(RANGE_PARTITIONED_MLOG_VERSION)));

	BinaryWriter wr(Unversioned());
	wr << partitionId << range.begin << range.end;
	Standalone<StringRef> header = wr.toValue();
	wait(logFile->append(header.begin(), header.size()));
	return Void();
}

ACTOR Future<Void> addMutation(Reference<IBackupFile> logFile,
                               RangePartitionedVersionedMessage message,
                               StringRef mutation,
                               int64_t* blockEnd,
                               int blockSize) {
	// Format: version, subversion, messageSize, message
	state int bytes = sizeof(Version) + sizeof(uint32_t) + sizeof(int) + mutation.size();

	// Convert to big Endianness for version.version, version.sub, and msgSize
	// The decoder assumes 0xFF is the end, so little endian can easily be
	// mistaken as the end. In contrast, big endian for version almost guarantee
	// the first byte is not 0xFF (should always be 0x00).
	BinaryWriter wr(Unversioned());
	wr << bigEndian64(message.version.version) << bigEndian32(message.version.sub) << bigEndian32(mutation.size());
	state Standalone<StringRef> mutationHeader = wr.toValue();

	// Start a new block if needed
	if (logFile->size() + bytes > *blockEnd) {
		const int bytesLeft = *blockEnd - logFile->size();
		if (bytesLeft > 0) {
			state Value paddingFFs = fileBackup::makePadding(bytesLeft);
			wait(logFile->append(paddingFFs.begin(), bytesLeft));
		}

		*blockEnd += blockSize;
		// Block header.
		wait(logFile->append((uint8_t*)&RANGE_PARTITIONED_MLOG_VERSION, sizeof(RANGE_PARTITIONED_MLOG_VERSION)));
	}

	wait(logFile->append((void*)mutationHeader.begin(), mutationHeader.size()));
	wait(logFile->append(mutation.begin(), mutation.size()));

	return Void();
}

ACTOR Future<Void> saveMutationsToFile(BackupRangePartitionedData* self, Version lastVersionInFile, int numMsg) {
	// Make sure all backups are ready, otherwise mutations will be lost.
	while (!self->isAllInfoReady()) {
		wait(self->waitAllInfoReady());
	}

	state std::vector<RangePartitionedLogFileInfo> activeFiles;
	state KeyRangeMap<std::set<int>> keyRangetoFileIdxMap;
	state int blockSize = SERVER_KNOBS->BACKUP_FILE_BLOCK_BYTES;
	state std::vector<Future<Reference<IBackupFile>>> fileFutures;

	for (auto it = self->backups.begin(); it != self->backups.end();) {
		if (it->second.stopped || !it->second.container.get().present()) {
			TraceEvent("BWRangeParititonedNoContainer", self->myId).detail("BackupId", it->first);
			it = self->backups.erase(it);
			continue;
		}

		state Version fileEndVersion = lastVersionInFile + 1;
		if (it->second.nextFileBeginVersion == invalidVersion) {
			it->second.nextFileBeginVersion = self->savedVersion + 1;
		}

		state Version beginVersion = it->second.nextFileBeginVersion;

		for (auto range : self->keyRangeToPartition.ranges()) {
			state int32_t partitionId = range.value();
			state KeyRange partitionRange = range.range();

			// Calculate intersection between backup's ranges and partition range
			state Optional<KeyRange> intersection =
			    self->getKeyRangeIntersection(it->second.ranges.get().get(), partitionRange);

			if (!intersection.present()) {
				continue; // No overlap, skip this partition for this backup
			}

			// Register file in data structures
			RangePartitionedLogFileInfo lf;
			lf.backupUid = it->first;
			lf.partitionId = partitionId;
			lf.fileKeyRange = intersection.get();
			lf.beginVersion = beginVersion;
			lf.endVersion = fileEndVersion;
			lf.blockEnd = 0;

			activeFiles.push_back(lf);
			int fileIndex = activeFiles.size() - 1;

			fileFutures.push_back(it->second.container.get().get()->writeRangePartitionedLogFile(
			    beginVersion, fileEndVersion, partitionId, blockSize));

			self->insertRange(keyRangetoFileIdxMap, intersection.get(), fileIndex);
		}
	}

	if (fileFutures.empty()) {
		return Void();
	}

	wait(waitForAll(fileFutures));

	state std::vector<Future<Void>> headerWrites;
	state int i;
	for (i = 0; i < activeFiles.size(); i++) {
		activeFiles[i].file = fileFutures[i].get();

		headerWrites.push_back(
		    writeFileHeader(activeFiles[i].file, activeFiles[i].partitionId, activeFiles[i].fileKeyRange));
	}

	// Wait for all headers to complete
	wait(waitForAll(headerWrites));

	keyRangetoFileIdxMap.coalesce(allKeys);
	if (activeFiles.empty()) {
		return Void();
	}

	// Process mutations
	state int idx;
	for (idx = 0; idx < numMsg; idx++) {
		auto& message = self->messages[idx];
		MutationRef m;

		if (!message.isCandidateBackupMessage(&m)) {
			continue;
		}

		DEBUG_MUTATION("BWRangeParitionedAddMutation", message.version.version, m, self->myId)
		    .detail("KCV", self->minKnownCommittedVersion)
		    .detail("SavedVersion", self->savedVersion);

		std::vector<Future<Void>> adds;
		if (m.type != MutationRef::Type::ClearRange) {
			for (int fileIdx : keyRangetoFileIdxMap[m.param1]) {
				auto& lf = activeFiles[fileIdx];
				// Different backups may have different start version so need this check before writing.
				if (message.getVersion() >= lf.beginVersion) {
					adds.push_back(addMutation(lf.file, message, message.message, &lf.blockEnd, blockSize));
				}
			}
		} else {
			KeyRangeRef mutationRange(m.param1, m.param2);
			std::unordered_set<int> writtenFiles;
			for (auto range : keyRangetoFileIdxMap.intersectingRanges(mutationRange)) {
				for (int fileIdx : range.value()) {
					// For ClearRange, we only need to write the full mutation once for each file.
					if (writtenFiles.find(fileIdx) != writtenFiles.end()) {
						continue;
					}
					auto& lf = activeFiles[fileIdx];
					if (message.getVersion() >= lf.beginVersion) {
						adds.push_back(addMutation(lf.file, message, message.message, &lf.blockEnd, blockSize));
					}
					writtenFiles.insert(fileIdx);
				}
			}
		}
		if (!adds.empty()) {
			wait(waitForAll(adds));
		}
	}

	// Finish files
	std::vector<Future<Void>> finished;
	for (auto& lf : activeFiles) {
		finished.push_back(lf.file->finish());
	}
	wait(waitForAll(finished));

	for (auto& lf : activeFiles) {
		self->backups[lf.backupUid].nextFileBeginVersion = lastVersionInFile + 1;
	}
	return Void();
}