/*
 * BackupWorker.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/BackupProgress.actor.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

struct VersionedMessage {
	LogMessageVersion version;
	StringRef message;
	VectorRef<Tag> tags;
	Arena arena; // Keep a reference to the memory containing the message

	VersionedMessage(LogMessageVersion v, StringRef m, const VectorRef<Tag>& t, const Arena& a)
	  : version(v), message(m), tags(t), arena(a) {}
	const Version getVersion() const { return version.version; }
	const uint32_t getSubVersion() const { return version.sub; }

	// Returns true if the message is a mutation that should be backuped, i.e.,
	// either key is not in system key space or is not a metadataVersionKey.
	bool isBackupMessage(MutationRef* m) const {
		for (Tag tag : tags) {
			if (tag.locality == tagLocalitySpecial || tag.locality == tagLocalityTxs) {
				return false; // skip Txs mutations
			}
		}

		ArenaReader reader(arena, message, AssumeVersion(currentProtocolVersion));

		// Return false for LogProtocolMessage.
		if (LogProtocolMessage::isNextIn(reader)) return false;

		reader >> *m;
		return normalKeys.contains(m->param1) || m->param1 == metadataVersionKey;
	}
};

struct BackupData {
	const UID myId;
	const Tag tag; // LogRouter tag for this worker, i.e., (-2, i)
	const int totalTags; // Total log router tags
	const Version startVersion;
	const Optional<Version> endVersion; // old epoch's end version (inclusive), or empty for current epoch
	const LogEpoch recruitedEpoch;
	const LogEpoch backupEpoch;
	Version minKnownCommittedVersion;
	Version savedVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	Database cx;
	std::vector<VersionedMessage> messages;
	AsyncVar<bool> pullFinished;
	NotifiedVersion pulledVersion;

	struct PerBackupInfo {
		PerBackupInfo() = default;
		PerBackupInfo(BackupData* data, Version v) : self(data), startVersion(v) {}

		bool isReady() const {
			return stopped || (container.isReady() && ranges.isReady());
		}

		bool isRunning() const {
			return container.isReady() && ranges.isReady() && !stopped;
		}

		Future<Void> waitReady() {
			if (stopped) return Void();
			return _waitReady(this);
		}

		ACTOR static Future<Void> _waitReady(PerBackupInfo* info) {
			wait(success(info->container) && success(info->ranges));
			return Void();
		}

		BackupData* self = nullptr;
		Version startVersion = invalidVersion;
		Version lastSavedVersion = invalidVersion;
		Future<Optional<Reference<IBackupContainer>>> container;
		Future<Optional<std::vector<KeyRange>>> ranges; // Key ranges of this backup
		bool allWorkerStarted = false; // Only worker with Tag(-2,0) uses & sets this field
		bool stopped = false; // Is the backup stopped?
	};

	std::map<UID, PerBackupInfo> backups; // Backup UID to infos
	AsyncTrigger changedTrigger;

	CounterCollection cc;
	Future<Void> logger;

	explicit BackupData(UID id, Reference<AsyncVar<ServerDBInfo>> db, const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), totalTags(req.totalTags), startVersion(req.startVersion),
	    endVersion(req.endVersion), recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch),
	    minKnownCommittedVersion(invalidVersion), savedVersion(req.startVersion), cc("BackupWorker", myId.toString()),
	    pulledVersion(0) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true);
		pullFinished.set(false);

		specialCounter(cc, "SavedVersion", [this]() { return this->savedVersion; });
		specialCounter(cc, "MinKnownCommittedVersion", [this]() { return this->minKnownCommittedVersion; });
		specialCounter(cc, "MsgQ", [this]() { return this->messages.size(); });
		logger = traceCounters("BackupWorkerMetrics", myId, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc,
		                       "BackupWorkerMetrics");
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

	// Inserts a backup's ranges into rangeMap.
	template <class T>
	void insertRanges(KeyRangeMap<std::set<T>>& keyRangeMap, const Optional<std::vector<KeyRange>>& ranges, T value) {
		if (!ranges.present() || ranges.get().empty()) {
			// insert full ranges of normal keys
			return insertRange(keyRangeMap, normalKeys, value);
		}
		for (const auto& range : ranges.get()) {
			insertRange(keyRangeMap, range, value);
		}
	}

	void pop() {
		const LogEpoch oldest = logSystem.get()->getOldestBackupEpoch();
		if (backupEpoch > oldest) {
			// Defer pop if old epoch hasn't finished popping yet.
			TraceEvent("BackupWorkerPopDeferred", myId)
			    .suppressFor(1.0)
			    .detail("BackupEpoch", backupEpoch)
			    .detail("OldestEpoch", oldest)
			    .detail("Version", savedVersion);
			return;
		}
		const Tag popTag = logSystem.get()->getPseudoPopTag(tag, ProcessClass::BackupClass);
		logSystem.get()->pop(savedVersion, popTag);
	}

	void eraseMessagesAfterEndVersion() {
		ASSERT(endVersion.present());
		const Version ver = endVersion.get();
		while (!messages.empty()) {
			if (messages.back().getVersion() > ver) {
				messages.pop_back();
			} else {
				return;
			}
		}
	}

	// Give a list of current active backups, compare with current list and decide
	// to start new backups and stop ones not in the active state.
	void onBackupChanges(const std::vector<std::pair<UID, Version>>& uidVersions) {
		std::set<UID> stopList;
		for (auto it : backups) {
			stopList.insert(it.first);
		}

		bool modified = false;
		for (const auto uidVersion : uidVersions) {
			const UID uid = uidVersion.first;

			auto it = backups.find(uid);
			if (it == backups.end()) {
				modified = true;
				auto inserted = backups.emplace(uid, BackupData::PerBackupInfo(this, uidVersion.second));

				// Open the container and get key ranges
				BackupConfig config(uid);
				inserted.first->second.container = config.backupContainer().get(cx);
				inserted.first->second.ranges = config.backupRanges().get(cx);
			} else {
				stopList.erase(uid);
			}
		}

		for (UID uid : stopList) {
			auto it = backups.find(uid);
			ASSERT(it != backups.end());
			it->second.stopped = true;
			modified = true;
		}
		if (modified) changedTrigger.trigger();
	}

	ACTOR static Future<Void> _waitAllInfoReady(BackupData* self) {
		std::vector<Future<Void>> all;
		for (auto it = self->backups.begin(); it != self->backups.end(); ) {
			if (it->second.stopped) {
				TraceEvent("BackupWorkerRemoveStoppedContainer", self->myId).detail("BackupId", it->first);
				it = self->backups.erase(it);
				continue;
			}
			all.push_back(it->second.waitReady());
			it++;
		}
		wait(waitForAll(all));
		return Void();
	}

	Future<Void> waitAllInfoReady() {
		return _waitAllInfoReady(this);
	}

	bool isAllInfoReady() const {
		for (const auto& [uid, info] : backups) {
			if (!info.isReady()) return false;
		}
		return true;
	}
};

// Monitors "backupStartedKey". If "started" is true, wait until the key is set;
// otherwise, wait until the key is cleared. If "watch" is false, do not perform
// the wait for key set/clear events. Returns if key present.
ACTOR Future<bool> monitorBackupStartedKeyChanges(BackupData* self, bool started, bool watch) {
	loop {
		state ReadYourWritesTransaction tr(self->cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = wait(tr.get(backupStartedKey));
				std::vector<std::pair<UID, Version>> uidVersions;
				if (value.present()) {
					uidVersions = decodeBackupStartedValue(value.get());
					TraceEvent e("BackupWorkerGotStartKey", self->myId);
					int i = 1;
					for (auto uidVersion : uidVersions) {
						e.detail(format("BackupID%d", i), uidVersion.first)
						    .detail(format("Version%d", i), uidVersion.second);
						i++;
					}
					self->onBackupChanges(uidVersions);
					if (started || !watch) return true;
				} else {
					TraceEvent("BackupWorkerEmptyStartKey", self->myId);
					self->onBackupChanges(uidVersions);

					if (!started || !watch) {
						return false;
					}
				}

				state Future<Void> watchFuture = tr.watch(backupStartedKey);
				wait(tr.commit());
				wait(watchFuture);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

// Monitor all backup worker in the recruited epoch has been started. If so,
// set the "allWorkerStarted" key of the BackupConfig to true, which in turn
// unblocks StartFullBackupTaskFunc::_execute. Note only worker with Tag (-2,0)
// runs this actor so that the key is set by one process.
ACTOR Future<Void> monitorAllWorkerStarted(BackupData* self) {
	loop {
		wait(delay(SERVER_KNOBS->WORKER_LOGGING_INTERVAL / 2.0) || self->changedTrigger.onTrigger());
		if (self->backups.empty()) {
			continue;
		}

		// check all workers have started by checking their progress is larger
		// than the backup's start version.
		state Reference<BackupProgress> progress(new BackupProgress(self->myId, {}));
		wait(getBackupProgress(self->cx, self->myId, progress));
		std::map<Tag, Version> tagVersions = progress->getEpochStatus(self->recruitedEpoch);

		state std::vector<UID> ready;
		if (tagVersions.size() == self->logSystem.get()->getLogRouterTags()) {
			// Check every version is larger than backup's startVersion
			for (auto& uidInfo : self->backups) {
				if (uidInfo.second.allWorkerStarted) continue;
				bool saved = true;
				for (const std::pair<Tag, Version> tv : tagVersions) {
					if (tv.second < uidInfo.second.startVersion) {
						saved = false;
						break;
					}
				}
				if (saved) {
					ready.push_back(uidInfo.first);
					uidInfo.second.allWorkerStarted = true;
				}
			}
			if (ready.empty()) continue;

			// Set "allWorkerStarted" key for ready backups
			loop {
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					state std::vector<Future<Optional<Value>>> readyValues;
					state std::vector<BackupConfig> configs;
					for (UID uid : ready) {
						configs.emplace_back(uid);
						readyValues.push_back(tr->get(configs.back().allWorkerStarted().key));
					}
					wait(waitForAll(readyValues));
					for (int i = 0; i < readyValues.size(); i++) {
						if (!readyValues[i].get().present()) {
							configs[i].allWorkerStarted().set(tr, true);
							TraceEvent("BackupWorkerSetReady", self->myId).detail("BackupID", ready[i].toString());
						}
					}
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}
	}
}

ACTOR Future<Void> saveProgress(BackupData* self, Version backupVersion) {
	state Transaction tr(self->cx);
	state Key key = backupProgressKeyFor(self->myId);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			WorkerBackupStatus status(self->backupEpoch, backupVersion, self->tag);
			tr.set(key, backupProgressValue(status));
			tr.addReadConflictRange(singleKeyRange(key));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Return a block of contiguous padding bytes, growing if needed.
static Value makePadding(int size) {
	static Value pad;
	if (pad.size() < size) {
		pad = makeString(size);
		memset(mutateString(pad), '\xff', pad.size());
	}

	return pad.substr(0, size);
}

// Write a mutation to a log file. Note the mutation can be different from
// message.message for clear mutations.
ACTOR Future<Void> addMutation(Reference<IBackupFile> logFile, VersionedMessage message, StringRef mutation,
                               int64_t* blockEnd, int blockSize) {
	state int bytes = sizeof(Version) + sizeof(uint32_t) + sizeof(int) + mutation.size();

	// Convert to big Endianness for version.version, version.sub, and msgSize
	// The decoder assumes 0xFF is the end, so little endian can easily be
	// mistaken as the end. In contrast, big endian for version almost guarantee
	// the first byte is not 0xFF (should always be 0x00).
	BinaryWriter wr(Unversioned());
	wr << bigEndian64(message.version.version) << bigEndian32(message.version.sub)
	   << bigEndian32(mutation.size());
	state Standalone<StringRef> header = wr.toValue();

	// Start a new block if needed
	if (logFile->size() + bytes > *blockEnd) {
		// Write padding if needed
		const int bytesLeft = *blockEnd - logFile->size();
		if (bytesLeft > 0) {
			state Value paddingFFs = makePadding(bytesLeft);
			wait(logFile->append(paddingFFs.begin(), bytesLeft));
		}

		*blockEnd += blockSize;
		// write block Header
		wait(logFile->append((uint8_t*)&PARTITIONED_MLOG_VERSION, sizeof(PARTITIONED_MLOG_VERSION)));
	}

	wait(logFile->append((void*)header.begin(), header.size()));
	wait(logFile->append(mutation.begin(), mutation.size()));
	return Void();
}

// Saves messages in the range of [0, numMsg) to a file and then remove these
// messages. The file format is a sequence of (Version, sub#, msgSize, message).
ACTOR Future<Void> saveMutationsToFile(BackupData* self, Version popVersion, int numMsg) {
	state int blockSize = SERVER_KNOBS->BACKUP_FILE_BLOCK_BYTES;
	state std::vector<Future<Reference<IBackupFile>>> logFileFutures;
	state std::vector<Reference<IBackupFile>> logFiles;
	state std::vector<int64_t> blockEnds;
	state std::set<UID> activeUids; // active Backups' UIDs
	state KeyRangeMap<std::set<int>> keyRangeMap; // range to index in logFileFutures, logFiles, & blockEnds
	state std::vector<Standalone<StringRef>> mutations;
	state int idx;

	// Make sure all backups are ready, otherwise mutations will be lost.
	while (!self->isAllInfoReady()) {
		wait(self->waitAllInfoReady());
	}

	for (auto it = self->backups.begin(); it != self->backups.end(); it++) {
		ASSERT(it->second.container.get().present());
		const int index = logFileFutures.size();
		activeUids.insert(it->first);
		self->insertRanges(keyRangeMap, it->second.ranges.get(), index);
		if (it->second.lastSavedVersion == invalidVersion) {
			it->second.lastSavedVersion = self->savedVersion;
		}
		logFileFutures.push_back(it->second.container.get().get()->writeTaggedLogFile(
		    it->second.lastSavedVersion, popVersion + 1, blockSize, self->tag.id, self->totalTags));
	}
	ASSERT(!activeUids.empty());

	keyRangeMap.coalesce(allKeys);
	wait(waitForAll(logFileFutures));

	std::transform(logFileFutures.begin(), logFileFutures.end(), std::back_inserter(logFiles),
	               [](const Future<Reference<IBackupFile>>& f) { return f.get(); });

	for (const auto& file : logFiles) {
		TraceEvent("OpenMutationFile", self->myId)
		    .detail("TagId", self->tag.id)
		    .detail("File", file->getFileName());
	}

	blockEnds = std::vector<int64_t>(logFiles.size(), 0);
	for (idx = 0; idx < numMsg; idx++) {
		const auto& message = self->messages[idx];
		MutationRef m;
		if (!message.isBackupMessage(&m)) continue;

		std::vector<Future<Void>> adds;
		if (m.type != MutationRef::Type::ClearRange) {
			for (int index : keyRangeMap[m.param1]) {
				adds.push_back(addMutation(logFiles[index], message, message.message, &blockEnds[index], blockSize));
			}
		} else {
			KeyRangeRef mutationRange(m.param1, m.param2);
			KeyRangeRef intersectionRange;

			// Find intersection ranges and create mutations for sub-ranges
			for (auto range : keyRangeMap.intersectingRanges(mutationRange)) {
				const auto& subrange = range.range();
				intersectionRange = mutationRange & subrange;
				MutationRef subm(MutationRef::Type::ClearRange, intersectionRange.begin, intersectionRange.end);
				BinaryWriter wr(AssumeVersion(currentProtocolVersion));
				wr << subm;
				mutations.push_back(wr.toValue());
				for (int index : range.value()) {
					adds.push_back(
					    addMutation(logFiles[index], message, mutations.back(), &blockEnds[index], blockSize));
				}
			}
		}
		wait(waitForAll(adds));
		mutations.clear();
	}

	std::vector<Future<Void>> finished;
	std::transform(logFiles.begin(), logFiles.end(), std::back_inserter(finished),
	               [](const Reference<IBackupFile>& f) { return f->finish(); });

	wait(waitForAll(finished));

	for (const auto& file : logFiles) {
		TraceEvent("CloseMutationFile", self->myId)
		    .detail("FileSize", file->size())
		    .detail("TagId", self->tag.id)
		    .detail("File", file->getFileName());
	}
	for (const UID uid : activeUids) {
		self->backups[uid].lastSavedVersion = popVersion + 1;
	}

	return Void();
}

// Uploads self->messages to cloud storage and updates savedVersion.
ACTOR Future<Void> uploadData(BackupData* self) {
	state Version popVersion = invalidVersion;

	loop {
		if (self->endVersion.present() && self->savedVersion >= self->endVersion.get()) {
			self->messages.clear();
			return Void();
		}

		// FIXME: knobify the delay of 10s. This delay is sensitive, as it is the
		// lag TLog might have. Changing to 20s may fail consistency check.
		state Future<Void> uploadDelay = delay(10);

		const Version maxPopVersion =
		    self->endVersion.present() ? self->endVersion.get() : self->minKnownCommittedVersion;
		state int numMsg = 0;
		if (self->messages.empty()) {
			// Even though messages is empty, we still want to advance popVersion.
			popVersion = std::max(popVersion, maxPopVersion);
		} else {
			for (const auto& message : self->messages) {
				if (message.getVersion() > maxPopVersion) break;
				popVersion = std::max(popVersion, message.getVersion());
				numMsg++;
			}
		}
		if (self->pullFinished.get() && self->messages.empty()) {
			// Advance popVersion to the endVersion to avoid gap between last
			// message version and the endVersion.
			popVersion = self->endVersion.get();
		}
		if (numMsg > 0 || self->endVersion.present()) {
			// save an empty file for old epochs so that log file versions are continuous
			TraceEvent("BackupWorkerSave", self->myId).detail("PopVersion", popVersion).detail("MsgQ", self->messages.size());
			wait(saveMutationsToFile(self, popVersion, numMsg));
			self->messages.erase(self->messages.begin(), self->messages.begin() + numMsg);
		}

		if (popVersion > self->savedVersion) {
			wait(saveProgress(self, popVersion));
			TraceEvent("BackupWorkerSavedProgress", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("Version", popVersion)
			    .detail("MsgQ", self->messages.size());
			self->savedVersion = std::max(popVersion, self->savedVersion);
			self->pop();
		}

		if (!self->pullFinished.get()) {
			wait(uploadDelay || self->pullFinished.onChange());
		}
	}
}

// Pulls data from TLog servers using LogRouter tag.
ACTOR Future<Void> pullAsyncData(BackupData* self) {
	state Future<Void> logSystemChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = std::max(self->startVersion, self->savedVersion);

	TraceEvent("BackupWorkerPull", self->myId);
	loop {
		loop choose {
			when (wait(r ? r->getMore(TaskPriority::TLogCommit) : Never())) {
				break;
			}
			when (wait(logSystemChange)) {
				if (self->logSystem.get()) {
					r = self->logSystem.get()->peekLogRouter(self->myId, tagAt, self->tag);
				} else {
					r = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			}
		}
		self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, r->getMinKnownCommittedVersion());

		// Note we aggressively peek (uncommitted) messages, but only committed
		// messages/mutations will be flushed to disk/blob in uploadData().
		while (r->hasMessage()) {
			self->messages.emplace_back(r->version(), r->getMessage(), r->getTags(), r->arena());
			r->nextMessage();
		}

		tagAt = r->version().version;
		self->pulledVersion = tagAt;
		TraceEvent("BackupWorkerGot", self->myId).suppressFor(1.0).detail("V", tagAt);
		if (self->endVersion.present() && tagAt > self->endVersion.get()) {
			self->eraseMessagesAfterEndVersion();
			TraceEvent("BackupWorkerFinishPull", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("VersionGot", tagAt)
			    .detail("EndVersion", self->endVersion.get())
			    .detail("MsgQ", self->messages.size());
			self->pullFinished.set(true);
			return Void();
		}
		wait(yield());
	}
}

ACTOR Future<Void> monitorBackupKeyOrPullData(BackupData* self) {
	state Future<Void> pullFinished = Void();
	state Future<bool> started;
	state Future<GetReadVersionReply> replyFuture = Never();

	loop {
		started = monitorBackupStartedKeyChanges(self, true, true);
		loop choose {
			when(bool present = wait(started)) {
				replyFuture = Never();
				break;
			}
			when(wait(self->cx->onMasterProxiesChanged() ||
			          delay(SERVER_KNOBS->BACKUP_NOOP_POP_DELAY, self->cx->taskID))) {
				GetReadVersionRequest request(1, GetReadVersionRequest::PRIORITY_DEFAULT |
				                                     GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION);
				replyFuture = loadBalance(self->cx->getMasterProxies(false),
				                          &MasterProxyInterface::getConsistentReadVersion, request, self->cx->taskID);
			}
			when(GetReadVersionReply reply = wait(replyFuture)) {
				replyFuture = Never();
				self->savedVersion = std::max(reply.version, self->savedVersion);
				self->minKnownCommittedVersion = std::max(reply.version, self->minKnownCommittedVersion);
				TraceEvent("BackupWorkerNoopPop", self->myId).detail("SavedVersion", self->savedVersion);
				self->pop(); // Pop while the worker is in this NOOP state.
			}
		}

		Future<bool> stopped = monitorBackupStartedKeyChanges(self, false, true);
		pullFinished = pullAsyncData(self);
		wait(success(stopped) || pullFinished);
		if (pullFinished.isReady()) return Void(); // backup is done for some old epoch.

		// Even though the snapshot is done, mutation logs may not be written
		// out yet. We need to make usre mutations up to this point is written.
		state Version currentVersion;
		loop {
			GetReadVersionRequest request(1, GetReadVersionRequest::PRIORITY_DEFAULT |
			                                     GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION);
			choose {
				when(wait(self->cx->onMasterProxiesChanged())) {}
				when(GetReadVersionReply reply = wait(loadBalance(self->cx->getMasterProxies(false),
				                                                  &MasterProxyInterface::getConsistentReadVersion,
				                                                  request, self->cx->taskID))) {
					currentVersion = reply.version;
					break;
				}
			}
		}
		wait(self->pulledVersion.whenAtLeast(currentVersion));
		pullFinished = Future<Void>(); // cancels pullAsyncData()
		TraceEvent("BackupWorkerPaused", self->myId);
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, LogEpoch recoveryCount,
                                BackupData* self) {
	loop {
		bool isDisplaced =
		    db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED;
		if (isDisplaced) {
			TraceEvent("BackupWorkerDisplaced", self->myId)
			    .detail("RecoveryCount", recoveryCount)
			    .detail("SavedVersion", self->savedVersion)
			    .detail("BackupWorkers", describe(db->get().logSystemConfig.tLogs))
			    .detail("DBRecoveryCount", db->get().recoveryCount)
			    .detail("RecoveryState", (int)db->get().recoveryState);
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> backupWorker(BackupInterface interf, InitializeBackupRequest req,
                                Reference<AsyncVar<ServerDBInfo>> db) {
	state BackupData self(interf.id(), db, req);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection(addActor.getFuture());
	state Future<Void> dbInfoChange = Void();

	TraceEvent("BackupWorkerStart", self.myId)
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
			addActor.send(monitorAllWorkerStarted(&self));
		}

		// Check if backup key is present to avoid race between this check and
		// noop pop as well as upload data: pop or skip upload before knowing
		// there are backup keys.
		bool present = wait(monitorBackupStartedKeyChanges(&self, true, false));
		TraceEvent("BackupWorkerWaitKey", self.myId).detail("Present", present);

		addActor.send(monitorBackupKeyOrPullData(&self));
		state Future<Void> done = uploadData(&self);

		loop choose {
			when(wait(dbInfoChange)) {
				dbInfoChange = db->onChange();
				Reference<ILogSystem> ls = ILogSystem::fromServerDBInfo(self.myId, db->get(), true);
				bool hasPseudoLocality = ls.isValid() && ls->hasPseudoLocality(tagLocalityBackup);
				LogEpoch oldestBackupEpoch = 0;
				if (hasPseudoLocality) {
					self.logSystem.set(ls);
					self.pop();
					oldestBackupEpoch = ls->getOldestBackupEpoch();
				}
				TraceEvent("BackupWorkerLogSystem", self.myId)
				    .detail("HasBackupLocality", hasPseudoLocality)
				    .detail("OldestBackupEpoch", oldestBackupEpoch)
				    .detail("Tag", self.tag.toString());
			}
			when(wait(done)) {
				TraceEvent("BackupWorkerDone", self.myId).detail("BackupEpoch", self.backupEpoch);
				// Notify master so that this worker can be removed from log system, then this
				// worker (for an old epoch's unfinished work) can safely exit.
				wait(brokenPromiseToNever(db->get().master.notifyBackupWorkerDone.getReply(
				    BackupWorkerDoneRequest(self.myId, self.backupEpoch))));
				break;
			}
			when(wait(error)) {}
		}
	} catch (Error& e) {
		TraceEvent("BackupWorkerTerminated", self.myId).error(e, true);
		if (e.code() != error_code_actor_cancelled && e.code() != error_code_worker_removed) {
			throw;
		}
	}
	return Void();
}

