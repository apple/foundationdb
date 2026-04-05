/*
 * BackupWorkerRangePartitioned.cpp
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

#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tracing.h"
#include "BackupPartitionMap.h"
#include "BackupRangePartitionedProgress.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/logsystem/LogSystem.h"
#include "fdbserver/core/WaitFailure.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "flow/CoroUtils.h"

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
	Reference<IBackupFile> file;
	int64_t blockEnd = 0;
};

struct BackupRangePartitionedData {
	const UID myId;
	const Tag tag; // tag for this backup worker
	const int totalTags; // Total backup worker tags
	const Version startVersion; // This worker's start version
	const Optional<Version> endVersion; // old epoch's end version (inclusive), or empty for current epoch
	const LogEpoch recruitedEpoch; // current epoch whose tLogs are receiving mutations
	const LogEpoch backupEpoch; // the epoch workers should pull mutations
	// TODO akanksha: Update oldestBackupEpoch wherever needed.
	LogEpoch oldestBackupEpoch = 0; // oldest epoch that still has data on tLogs for backup to pull
	// Minimumum known committed version in StorageServers.
	Version minKnownCommittedVersion;
	Version savedVersion; // Largest version saved to blob storage
	NotifiedVersion pulledVersion;
	Version logFolderBaseVersion;
	AsyncVar<Reference<LogSystem>> logSystem;
	AsyncVar<bool> paused; // Track if "backupRangePartitionedPausedKey" is set.
	Reference<FlowLock> lock;
	AsyncTrigger doneTrigger;
	AsyncTrigger changedTrigger;
	Database cx;
	std::vector<RangePartitionedVersionedMessage> messages;
	// Key range to partition ID map, used to determine which partition a mutation belongs to based on its key.
	KeyRangeMap<int> keyRangeToPartitionId;
	// Partition ID to key range map for easy lookup of partition's key range.
	std::unordered_map<int, KeyRange> partitionToKeyRange;
	// KeyRange to backup UID and partition id map needed to create log files for the right backup and partition.
	KeyRangeMap<std::vector<std::pair<UID, int32_t>>> keyRangeToBackupAssignment;
	// TODO akanksha: Once end to end implementation complete, check if stopped need to be added or not.

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

		static Future<Void> _waitReady(PerBackupInfo* info) {
			co_await (success(info->container) && success(info->ranges));
		}
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

	Version maxPopVersion() const { return endVersion.present() ? endVersion.get() : minKnownCommittedVersion; }

	bool allMessageSaved() const { return (endVersion.present() && savedVersion >= endVersion.get()); }

	// Erases messages and updates lock with memory released.
	void eraseMessages(int num) {
		ASSERT(num <= messages.size());
		if (num == 0)
			return;

		// Accumulate erased message sizes
		int64_t bytes = 0;
		for (int i = 0; i < num; i++) {
			bytes += messages[i].getEstimatedSize();
		}
		TraceEvent(SevDebugMemory, "BWRangePartitionedMemory", myId)
		    .detail("Release", bytes)
		    .detail("Total", lock->activePermits());
		lock->release(bytes);
		messages.erase(messages.begin(), messages.begin() + num);
	}

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

	static Future<Void> _waitAllInfoReady(BackupRangePartitionedData* self) {
		std::vector<Future<Void>> all;
		for (auto it = self->backups.begin(); it != self->backups.end();) {
			all.push_back(it->second.waitReady());
			it++;
		}
		co_await waitForAll(all);
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

static Future<Void> computeKeyRangeToBackupAssignment(BackupRangePartitionedData* self) {
	self->keyRangeToBackupAssignment = KeyRangeMap<std::vector<std::pair<UID, int32_t>>>();

	while (!self->isAllInfoReady()) {
		co_await self->waitAllInfoReady();
	}

	for (auto& [uid, info] : self->backups) {
		const auto& backupRanges = info.ranges.get().get();

		for (auto iter : self->keyRangeToPartitionId.ranges()) {
			int32_t partitionId = iter.value();
			KeyRange partitionRange = iter.range();

			Optional<KeyRange> intersection = self->getKeyRangeIntersection(backupRanges, partitionRange);
			if (!intersection.present())
				continue;

			std::pair<UID, int32_t> bk{ uid, partitionId };
			for (auto& range : self->keyRangeToBackupAssignment.modify(intersection.get())) {
				range->value().push_back(bk);
			}
		}
	}
	self->keyRangeToBackupAssignment.coalesce(allKeys);
}

static Future<Void> onBackupChanges(BackupRangePartitionedData* self,
                                    std::vector<std::pair<UID, Version>> uidVersions) {
	std::unordered_set<UID> activeUids;
	for (const auto& [uid, version] : uidVersions) {
		activeUids.insert(uid);
	}

	bool modified = false;
	bool hasNewBackup = false;
	Version newBackupsMinVersion = std::numeric_limits<Version>::max();

	// Add any new backups
	for (const auto& [uid, version] : uidVersions) {
		if (self->backups.find(uid) == self->backups.end()) {
			self->backups.emplace(uid, BackupRangePartitionedData::PerBackupInfo(self, uid, version));
			modified = true;
			newBackupsMinVersion = std::min(newBackupsMinVersion, version);
			hasNewBackup = true;
		}
	}

	// Remove backups that are no longer active.
	for (auto it = self->backups.begin(); it != self->backups.end(); it++) {
		if (activeUids.find(it->first) == activeUids.end()) {
			it->second.stopped = true;
			it = self->backups.erase(it);
			modified = true;
		}
	}

	if (hasNewBackup && self->backupEpoch < self->recruitedEpoch && self->savedVersion + 1 == self->startVersion) {
		// Advance savedVersion to minimize version ranges in case backupEpoch's progress is not saved. Master may set a
		// very low startVersion that is already popped. Advance the version is safe because these versions are not
		// popped -- if they are popped, their progress should be already recorded and Master would use a higher version
		// than minVersion.
		self->savedVersion = std::max(newBackupsMinVersion, self->savedVersion);
	}
	if (modified) {
		self->changedTrigger.trigger();
		co_await computeKeyRangeToBackupAssignment(self);
	}
}

Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                          LogEpoch recoveryCount,
                          BackupRangePartitionedData* self) {
	while (true) {
		bool isDisplaced =
		    db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED;
		if (isDisplaced) {
			TraceEvent("BWRangePartitionedDisplaced", self->myId)
			    .detail("RecoveryCount", recoveryCount)
			    .detail("RecoveryState", (int)db->get().recoveryState);
			throw worker_removed();
		}
		co_await db->onChange();
	}
}

Future<Version> pullPartitionMapFromTLog(BackupRangePartitionedData* self, PartitionMap* outPartitionMap) {
	Reference<IPeekCursor> cursor;
	Version partitionMapVersion = invalidVersion;
	Future<Void> logSystemChange = Void();

	while (true) {
		while (true) {
			auto res = co_await race(cursor ? cursor->getMore() : Never(), logSystemChange);
			if (res.index() == 0) {
				break;
			} else if (res.index() == 1) {
				if (self->logSystem.get()) {
					cursor = self->logSystem.get()->peekSingle(self->myId, self->startVersion, self->tag);
				} else {
					cursor = Reference<IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			} else {
				UNREACHABLE();
			}
		}
		co_await cursor->getMore();
		if (!cursor->hasMessage()) {
			continue;
		}
		for (; cursor->hasMessage(); cursor->nextMessage()) {
			Version msgVersion = cursor->version().version;
			StringRef message = cursor->getMessage();
			Arena arena = cursor->arena();
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
			co_return partitionMapVersion;
		}
	}
}

Future<Void> uploadPartitionList(BackupRangePartitionedData* self, PartitionMap partitionMap) {
	std::vector<Future<Void>> fileFutures;
	auto it = self->backups.begin();

	std::string jsonContent = serializePartitionListJSON(partitionMap);

	for (; it != self->backups.end();) {
		if (!it->second.container.get().present()) {
			TraceEvent("BWRangePartitionedRemoveContainer", self->myId).detail("BackupId", it->first);
			it = self->backups.erase(it);
			continue;
		}
		Reference<IBackupContainer> container = it->second.container.get().get();
		fileFutures.push_back(container->writePartitionListFile(self->logFolderBaseVersion, jsonContent));
		it++;
	}
	if (fileFutures.empty()) {
		TraceEvent("BWRangePartitionedNoContainers", self->myId);
		co_return;
	}

	co_await waitForAll(fileFutures);
}

// TODO akanksha -> Need to figure out if
// 1. For new requests -> PartitionMap in TLOG will be same for all containers
// 2. For older epochs with different containers is PartitionMap specific to container or same for all.
// Right now assumption is that PartitionMap will be passed by TLOG with the first message after start version for both
// older epochs and newer epochs.
Future<Void> waitAndProcessPartitionMap(BackupRangePartitionedData* self) {
	TraceEvent("BWRangeParitionedWaitingForPartitionMap", self->myId)
	    .detail("Tag", self->tag.toString())
	    .detail("StartVersion", self->startVersion);
	PartitionMap partitionMap;

	Version partitionMapVersion = co_await pullPartitionMapFromTLog(self, &partitionMap);
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

	Key doneKey = backupRangePartitionedMapUploadedKeyFor(partitionMapVersion);
	ReadYourWritesTransaction tr(self->cx);

	// TODO akanksha: Check what will be the tag id once tags are implemented for backup workers and update the
	// condition accordingly.
	// Also add background actor to clean up the done key at regular
	if (self->tag.id == 0) {
		co_await uploadPartitionList(self, partitionMap);
		TraceEvent("BWRangePartitionedPartitionMapUploaded", self->myId)
		    .detail("Version", partitionMapVersion)
		    .detail("NumBackups", self->backups.size());
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.set(doneKey, "1"_sr);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	} else {
		// All other backup workers waits for done key to be set by the worker with tag id 0, then start pulling
		// mutations. This is to make sure partition map is uploaded before any worker starts pulling mutations.
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE); // More efficient for reads
				Optional<Value> v = co_await tr.get(doneKey);
				if (v.present()) {
					break;
				}
				Future<Void> watchFuture = tr.watch(doneKey);
				co_await tr.commit();
				co_await watchFuture;
				tr.reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}
	self->pulledVersion.set(partitionMapVersion);
	self->savedVersion = partitionMapVersion;
	self->pop();
	co_await computeKeyRangeToBackupAssignment(self);
}

// Pulls mutations from TLog servers.
Future<Void> pullAsyncData(BackupRangePartitionedData* self) {
	Future<Void> logSystemChange = Void();
	Reference<IPeekCursor> cursor;

	Version tagAt = std::max({ self->pulledVersion.get(), self->startVersion, self->savedVersion });

	TraceEvent("BWRangePartitionedPull", self->myId)
	    .detail("Tag", self->tag)
	    .detail("Version", tagAt)
	    .detail("StartVersion", self->startVersion)
	    .detail("SavedVersion", self->savedVersion);

	while (true) {
		while (self->paused.get()) {
			co_await self->paused.onChange();
		}

		while (true) {
			auto res = co_await race(cursor ? cursor->getMore(TaskPriority::TLogCommit) : Never(), logSystemChange);
			if (res.index() == 0) {
				DisabledTraceEvent("BWRangePartitionedGotMore", self->myId)
				    .detail("Tag", self->tag)
				    .detail("CursorVersion", cursor->version().version);
				break;
			} else if (res.index() == 1) {
				if (self->logSystem.get()) {
					// TODO akanksha: Use peekSingle as of now instead of peekLogRouter and later confirm if it works as
					// expected.
					cursor = self->logSystem.get()->peekSingle(self->myId, tagAt, self->tag);
				} else {
					cursor = Reference<IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			} else {
				UNREACHABLE();
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
		int64_t peekedBytes = 0;

		// Hold messages until we know how many we can take, self->messages always
		// contains messages that we have reserved memory for. Therefore, lock->release()
		// will always encounter message with reserved memory.
		std::vector<RangePartitionedVersionedMessage> tmpMessages;

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
			co_await self->lock->take(TaskPriority::DefaultYield, peekedBytes);
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
			co_return;
		}
		co_await yield();
	}
}

Future<Void> writeFileHeader(Reference<IBackupFile> logFile, int32_t partitionId, KeyRange range) {
	co_await logFile->append((uint8_t*)&RANGE_PARTITIONED_MLOG_VERSION, sizeof(RANGE_PARTITIONED_MLOG_VERSION));

	BinaryWriter wr(Unversioned());
	wr << partitionId << range.begin << range.end;
	Standalone<StringRef> header = wr.toValue();
	co_await logFile->append(header.begin(), header.size());
}

Future<Void> addMutation(Reference<IBackupFile> logFile,
                         RangePartitionedVersionedMessage message,
                         StringRef mutation,
                         int64_t* blockEnd,
                         int blockSize) {
	// Format: version, subversion, messageSize, message
	int bytes = sizeof(Version) + sizeof(uint32_t) + sizeof(int) + mutation.size();

	// Convert to big Endianness for version.version, version.sub, and msgSize
	// The decoder assumes 0xFF is the end, so little endian can easily be
	// mistaken as the end. In contrast, big endian for version almost guarantee
	// the first byte is not 0xFF (should always be 0x00).
	BinaryWriter wr(Unversioned());
	wr << bigEndian64(message.version.version) << bigEndian32(message.version.sub) << bigEndian32(mutation.size());
	Standalone<StringRef> mutationHeader = wr.toValue();

	// Start a new block if needed
	if (logFile->size() + bytes > *blockEnd) {
		const int bytesLeft = *blockEnd - logFile->size();
		if (bytesLeft > 0) {
			Value paddingFFs = fileBackup::makePadding(bytesLeft);
			co_await logFile->append(paddingFFs.begin(), bytesLeft);
		}

		*blockEnd += blockSize;
		// Block header.
		co_await logFile->append((uint8_t*)&RANGE_PARTITIONED_MLOG_VERSION, sizeof(RANGE_PARTITIONED_MLOG_VERSION));
	}
	co_await logFile->append((void*)mutationHeader.begin(), mutationHeader.size());
	co_await logFile->append(mutation.begin(), mutation.size());
}

static Future<Void> updateLogBytesWritten(BackupRangePartitionedData* self, std::map<UID, int64_t> bytesPerBackup) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			for (const auto& [uid, bytes] : bytesPerBackup) {
				BackupConfig config(uid);
				config.logBytesWritten().atomicOp(tr, bytes, MutationRef::AddValue);
			}
			co_await tr->commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

Future<Void> saveMutationsToFile(BackupRangePartitionedData* self, Version lastVersionInFile, int numMsg) {
	// Make sure all backups are ready, otherwise mutations will be lost.
	while (!self->isAllInfoReady()) {
		co_await self->waitAllInfoReady();
	}

	std::vector<RangePartitionedLogFileInfo> activeFiles;
	// Map of (backupUid, partitionId) -> index into activeFiles.
	std::map<std::pair<UID, int32_t>, int> fileIndexByBackupPartition;
	int blockSize = SERVER_KNOBS->BACKUP_FILE_BLOCK_BYTES;
	std::vector<Future<Reference<IBackupFile>>> fileFutures;

	for (auto entry = self->keyRangeToBackupAssignment.ranges().begin();
	     entry != self->keyRangeToBackupAssignment.ranges().end();
	     ++entry) {
		for (const auto& bkPartition : entry->value()) {
			UID backupUid = bkPartition.first;
			int32_t partitionId = bkPartition.second;

			auto it = self->backups.find(backupUid);
			if (it == self->backups.end() || !it->second.container.get().present()) {
				TraceEvent("BWRangePartitionedRemoveContainerInFileCreation", self->myId).detail("BackupId", backupUid);
				continue;
			}

			std::pair<UID, int32_t> bpKey(backupUid, partitionId);
			if (fileIndexByBackupPartition.contains(bpKey)) {
				continue;
			}

			Version fileEndVersion = lastVersionInFile + 1;
			if (it->second.nextFileBeginVersion == invalidVersion) {
				it->second.nextFileBeginVersion = self->savedVersion + 1;
			}
			Version beginVersion = it->second.nextFileBeginVersion;

			RangePartitionedLogFileInfo lf;
			lf.backupUid = it->first;
			lf.partitionId = partitionId;
			lf.fileKeyRange = entry->range();
			lf.beginVersion = beginVersion;
			lf.blockEnd = 0;

			activeFiles.push_back(lf);
			fileIndexByBackupPartition[bpKey] = activeFiles.size() - 1;
			fileFutures.push_back(it->second.container.get().get()->writeRangePartitionedLogFile(
			    beginVersion, fileEndVersion, self->logFolderBaseVersion, partitionId, blockSize));
		}
	}

	if (fileFutures.empty()) {
		co_return;
	}
	co_await waitForAll(fileFutures);

	std::vector<Future<Void>> headerWrites;
	int i;
	for (i = 0; i < activeFiles.size(); i++) {
		activeFiles[i].file = fileFutures[i].get();
		headerWrites.push_back(
		    writeFileHeader(activeFiles[i].file, activeFiles[i].partitionId, activeFiles[i].fileKeyRange));
	}
	co_await waitForAll(headerWrites);

	if (activeFiles.empty()) {
		co_return;
	}

	// Process mutations
	int idx;
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
			for (const auto& entry : self->keyRangeToBackupAssignment[m.param1]) {
				auto it = fileIndexByBackupPartition.find(entry);
				ASSERT(it != fileIndexByBackupPartition.end());

				int fileIdx = it->second;
				auto& lf = activeFiles[fileIdx];
				// Different backups may have different start version so need this check before writing.
				if (message.getVersion() >= lf.beginVersion) {
					adds.push_back(addMutation(lf.file, message, message.message, &lf.blockEnd, blockSize));
				}
			}
		} else {
			KeyRangeRef mutationRange(m.param1, m.param2);
			std::unordered_set<int> writtenFiles;
			for (auto range : self->keyRangeToBackupAssignment.intersectingRanges(mutationRange)) {
				for (const auto& entry : range.value()) {
					auto it = fileIndexByBackupPartition.find(entry);
					ASSERT(it != fileIndexByBackupPartition.end());

					int fileIdx = it->second;
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
			co_await waitForAll(adds);
		}
	}

	// Finish files
	// TODO akanksha: Add FileLevel checksum.
	std::vector<Future<Void>> finished;
	for (auto& lf : activeFiles) {
		finished.push_back(lf.file->finish());
	}
	co_await waitForAll(finished);

	std::map<UID, int64_t> bytesPerBackup;
	for (auto& lf : activeFiles) {
		self->backups[lf.backupUid].nextFileBeginVersion = lastVersionInFile + 1;
		bytesPerBackup[lf.backupUid] += lf.file->size();
	}

	co_await updateLogBytesWritten(self, std::move(bytesPerBackup));
}

// It closes the race between getMinBackupVersion's snapshot at master-recruit time and the actual state of
// backupStartedKey when the old epoch backup worker comes up — specifically the case where backup configuration changed
// during that window so the backup worker is no longer needed.
static Future<bool> shouldBackupWorkerExitEarly(BackupRangePartitionedData* self) {
	while (true) {
		ReadYourWritesTransaction tr(self->cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = co_await tr.get(backupRangePartitionedStartedKey);
				std::vector<std::pair<UID, Version>> uidVersions;
				if (value.present()) {
					bool shouldExit = self->endVersion.present();
					uidVersions = decodeBackupRangePartitionedStartedValue(value.get());
					TraceEvent e("BWRangePartitionedGotStartKey", self->myId);
					int i = 1;
					for (auto [uid, version] : uidVersions) {
						e.detail(format("BackupID%d", i), uid).detail(format("Version%d", i), version);
						i++;
						if (shouldExit && version < self->endVersion.get()) {
							shouldExit = false;
						}
					}
					co_await onBackupChanges(self, uidVersions);
					co_return shouldExit;
				}

				TraceEvent("BWRangePartitionedEmptyStartKey", self->myId);
				Future<Void> watchFuture = tr.watch(backupRangePartitionedStartedKey);
				co_await tr.commit();
				co_await watchFuture;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}
}

static Future<Void> monitorBackupStartedKeyChanges(BackupRangePartitionedData* self) {
	while (true) {
		ReadYourWritesTransaction tr(self->cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = co_await tr.get(backupRangePartitionedStartedKey);
				std::vector<std::pair<UID, Version>> uidVersions;
				if (value.present()) {
					uidVersions = decodeBackupRangePartitionedStartedValue(value.get());
					TraceEvent e("BWRangePartitionedGotStartKey", self->myId);
					int i = 1;
					for (auto [uid, version] : uidVersions) {
						e.detail(format("BackupID%d", i), uid).detail(format("Version%d", i), version);
						i++;
					}
				}

				onBackupChanges(self, uidVersions);
				Future<Void> watchFuture = tr.watch(backupRangePartitionedStartedKey);
				co_await tr.commit();
				co_await watchFuture;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}
}

// This function is used to set backup worker's saved version latestBackupWorkerSavedVersion in BackupConfig.
Future<Void> setBackupKeys(BackupRangePartitionedData* self, std::map<UID, Version> savedLogVersions) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			std::vector<Future<Optional<Version>>> prevBackupWorkerSavedVersions;
			std::vector<BackupConfig> versionConfigs;
			std::vector<Future<Optional<bool>>> allWorkersReady;
			for (const auto& [uid, version] : savedLogVersions) {
				BackupConfig config(uid);
				versionConfigs.emplace_back(config);
				prevBackupWorkerSavedVersions.push_back(config.latestBackupWorkerSavedVersion().get(tr));
				allWorkersReady.push_back(config.allWorkerStarted().get(tr));
			}
			co_await (waitForAll(prevBackupWorkerSavedVersions) && waitForAll(allWorkersReady));

			for (int i = 0; i < prevBackupWorkerSavedVersions.size(); i++) {
				if (!allWorkersReady[i].get().present() || !allWorkersReady[i].get().get()) {
					continue;
				}

				const Version current = savedLogVersions[versionConfigs[i].getUid()];
				if (prevBackupWorkerSavedVersions[i].get().present()) {
					const Version prev = prevBackupWorkerSavedVersions[i].get().get();
					if (prev > current) {
						TraceEvent(SevWarn, "BWRangePartitionedVersionInverse", self->myId)
						    .detail("Prev", prev)
						    .detail("Current", current);
					}
				}
				if (self->backupEpoch == self->oldestBackupEpoch &&
				    (!prevBackupWorkerSavedVersions[i].get().present() ||
				     prevBackupWorkerSavedVersions[i].get().get() < current)) {
					TraceEvent("BWRangePartitionedSetVersion", self->myId)
					    .detail("BackupID", versionConfigs[i].getUid())
					    .detail("Version", current);
					versionConfigs[i].latestBackupWorkerSavedVersion().set(tr, current);
				}
			}
			co_await tr->commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

static Future<Void> monitorWorkerPause(BackupRangePartitionedData* self) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));
	Future<Void> watch;

	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> value = co_await tr->get(backupRangePartitionedPausedKey);
			bool paused = value.present() && value.get() == "1"_sr;
			if (self->paused.get() != paused) {
				TraceEvent(paused ? "BWRangePartitionedPaused" : "BWRangePartitionedResumed", self->myId).log();
				self->paused.set(paused);
			}

			watch = tr->watch(backupRangePartitionedPausedKey);
			co_await tr->commit();
			co_await watch;
			tr->reset();
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

Future<Void> monitorBackupRangePartitionedProgress(BackupRangePartitionedData* self) {
	Future<Void> interval;

	while (true) {
		interval = delay(SERVER_KNOBS->WORKER_LOGGING_INTERVAL / 2.0);
		while (self->backups.empty() || !self->logSystem.get()) {
			co_await (self->changedTrigger.onTrigger() || self->logSystem.onChange());
		}

		// Check all workers have started by checking their progress is larger than the backup's start version.
		Reference<BackupRangePartitionedProgress> progress(new BackupRangePartitionedProgress(self->myId));
		co_await getBackupRangePartitionedProgress(self->cx, self->myId, progress, SevDebug);

		std::map<Tag, Version> tagVersions = progress->getEpochStatus(self->recruitedEpoch);
		if (tagVersions.size() != self->totalTags) {
			co_await interval;
			continue;
		}

		std::map<UID, Version> savedLogVersions;
		// update progress so far if previous epochs are done.
		if (self->recruitedEpoch == self->oldestBackupEpoch) {
			Version v = std::numeric_limits<Version>::max();
			// Find the version we can gurantee is fully backed up for all backup workers.
			for (const auto& [tag, version] : tagVersions) {
				v = std::min(v, version);
			}

			for (auto& [uid, info] : self->backups) {
				savedLogVersions.emplace(uid, v);
				TraceEvent("BWRangePartitionedSavedBackupVersion", self->myId)
				    .detail("BackupID", uid)
				    .detail("Version", v);
			}
		}

		Future<Void> setKeys = savedLogVersions.empty() ? Void() : setBackupKeys(self, savedLogVersions);
		co_await (interval && setKeys);
	}
}

Future<Void> saveProgress(BackupRangePartitionedData* self, Version backupVersion) {
	Transaction tr(self->cx);
	Key key = backupRangePartitionedProgressKey(self->myId);

	while (true) {
		Error err;
		try {
			// It's critical to save progress immediately so that after a master
			// recovery, the new master can know the progress so far.
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			// CHECK: Don't save progress if backup workers are disabled
			Optional<Value> backupWorkerEnabled = co_await tr.get(backupWorkerEnabledKey);
			if (!backupWorkerEnabled.present() || backupWorkerEnabled.get() == "0"_sr) {
				TraceEvent("BWRangePartitionedProgressSkipped", self->myId).detail("Reason", "BackupWorkersDisabled");
				co_return;
			}

			WorkerBackupStatus status(self->backupEpoch, backupVersion, self->tag, self->totalTags);
			tr.set(key, backupRangePartitionedProgressValue(status));
			tr.addReadConflictRange(singleKeyRange(key));
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Uploads self->messages to storage and updates savedVersion.
Future<Void> uploadData(BackupRangePartitionedData* self) {
	// Version up to which messages will be popped from tlog.
	Version popVersion = invalidVersion;

	while (true) {
		// Too large uploadDelay will delay popping tLog data for too long.
		Future<Void> uploadDelay = delay(SERVER_KNOBS->BACKUP_UPLOAD_DELAY);

		int numMsg = 0;
		// Last Version that we popped from tlog.
		Version lastPopVersion = popVersion;
		// index of last version's end position in self->messages. i.e. the first index of next version???
		int lastVersionIndex = 0;
		// Version just before current popVersion - to find version boundaries.
		Version lastVersion = invalidVersion;

		for (auto& message : self->messages) {
			// message may be prefetched in peek; uncommitted message should not be uploaded.
			const Version msgVersion = message.getVersion();
			if (msgVersion > self->maxPopVersion()) {
				break;
			}
			if (msgVersion > popVersion) {
				lastVersionIndex = numMsg;
				lastVersion = popVersion;
				popVersion = msgVersion;
			}
			numMsg++;
		}
		if (self->pullFinished()) {
			popVersion = self->endVersion.get();
		} else {
			// make sure file is saved on version boundary
			popVersion = lastVersion;
			numMsg = lastVersionIndex;

			// If we aren't able to process any messages and the lock is blocking us from
			// queuing more, then we are stuck. This could suggest the lock capacity is too small.
			ASSERT(numMsg > 0 || self->lock->waiters() == 0);
		}

		// TODO akanksha: Removed redundant check popVersion > lastPopVersion. Remove todo after testing completes.
		if (numMsg > 0 || self->pullFinished()) {
			TraceEvent("BWRangePartitionedSave", self->myId)
			    .detail("Version", popVersion)
			    .detail("LastPopVersion", lastPopVersion)
			    .detail("SavedVersion", self->savedVersion)
			    .detail("NumMsg", numMsg)
			    .detail("MsgQ", self->messages.size());
			// save an empty file for old epochs so that log file versions are continuous
			co_await saveMutationsToFile(self, popVersion, numMsg);
			self->eraseMessages(numMsg);
		}

		if (popVersion > self->savedVersion) {
			co_await saveProgress(self, popVersion);
			TraceEvent("BWRangePartitionedSavedProgress", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("Version", popVersion)
			    .detail("MsgQ", self->messages.size());
			self->savedVersion = popVersion;
			self->pop();
		}

		if (self->allMessageSaved()) {
			co_return;
		}

		if (!self->pullFinished()) {
			co_await (uploadDelay || self->doneTrigger.onTrigger());
		}
	}
}

Future<Void> backupWorkerRangePartitioned(BackupInterface interf,
                                          InitializeBackupRequest req,
                                          Reference<AsyncVar<ServerDBInfo> const> db) {
	BackupRangePartitionedData self(interf.id(), db, req);
	PromiseStream<Future<Void>> addActor;
	Future<Void> error = actorCollection(addActor.getFuture());
	Future<Void> dbInfoChange = Void();
	Future<Void> pull;
	Future<Void> done;
	Error err;

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

		if (req.recruitedEpoch == req.backupEpoch && req.routerTag.id == 0) {
			addActor.send(monitorBackupRangePartitionedProgress(&self));
		}

		addActor.send(monitorWorkerPause(&self));

		// First need to call waitAndProcessPartitionMap before starting to pull data, because we need to know the
		// partition assignment.
		co_await waitAndProcessPartitionMap(&self);

		// If the worker is on an old epoch and all backups starts a version >= the endVersion
		bool exitEarly = co_await shouldBackupWorkerExitEarly(&self);
		TraceEvent("BWRangePartitionedExitEarly", self.myId).detail("ExitEarly", exitEarly);
		if (!exitEarly) {
			addActor.send(monitorBackupStartedKeyChanges(&self));
		}

		pull = exitEarly ? Void() : pullAsyncData(&self);
		addActor.send(pull);
		done = exitEarly ? Void() : uploadData(&self);

		while (true) {
			auto res = co_await race(dbInfoChange, done, error);
			if (res.index() == 0) {
				dbInfoChange = db->onChange();
				Reference<LogSystem> ls = makeLogSystemFromServerDBInfo(self.myId, db->get(), true);

				if (ls.isValid()) {
					self.logSystem.set(ls);
					self.oldestBackupEpoch = std::max(self.oldestBackupEpoch, ls->getOldestBackupEpoch());
					TraceEvent("BWRangePartitionedLogSystemUpdate", self.myId)
					    .detail("Tag", self.tag.toString())
					    .detail("TagLocality", self.tag.locality)
					    .detail("OldestEpoch", self.oldestBackupEpoch);
				} else {
					TraceEvent("BWRangePartitionedNoLogSystem", self.myId);
				}
			} else if (res.index() == 1) {
				TraceEvent("BWRangePartitionedDone", self.myId).detail("BackupEpoch", self.backupEpoch);
				// Notify master so that this worker can be removed from log system, then this
				// worker (for an old epoch's unfinished work) can safely exit.
				co_await brokenPromiseToNever(db->get().clusterInterface.notifyBackupWorkerDone.getReply(
				    BackupWorkerDoneRequest(self.myId, self.backupEpoch)));
				break;
			} else if (res.index() != 2) {
				UNREACHABLE();
			}
		}
		co_return;
	} catch (Error& e) {
		err = e;
	}

	if (err.code() == error_code_worker_removed) {
		pull = Void(); // cancels pulling
		try {
			co_await done;
		} catch (Error& shutdownErr) {
			TraceEvent("BWRangePartitionedShutdownError", self.myId).errorUnsuppressed(shutdownErr);
		}
	}
	TraceEvent("BWRangePartitionedTerminated", self.myId).errorUnsuppressed(err);
	if (err.code() != error_code_actor_cancelled && err.code() != error_code_worker_removed) {
		throw err;
	}
}
