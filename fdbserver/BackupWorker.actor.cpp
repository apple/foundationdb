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
};

struct BackupData {
	const UID myId;
	const Tag tag; // LogRouter tag for this worker, i.e., (-2, i)
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

	struct PerBackupInfo {
		PerBackupInfo() = default;
		PerBackupInfo(BackupData* data, Version v) : self(data), startVersion(v) {}

		ACTOR static Future<Void> _waitReady(PerBackupInfo* info, UID backupUid) {
			wait(success(info->container) && success(info->ranges));
			info->ready = true;
			const auto& ranges = info->ranges.get();
			info->self->insertRanges(ranges, backupUid);
			TraceEvent("BackupWorkerInsertRanges", info->self->myId)
			    .detail("BackupID", backupUid)
			    .detail("URL", info->container.get()->getURL())
			    .detail("Ranges", ranges.present() ? describe(ranges.get()) : "[empty]");
			return Void();
		}

		Future<Void> waitReady(UID backupUid) { return _waitReady(this, backupUid); }

		bool isRunning() { return ready && !stopped; }

		BackupData* self = nullptr;
		Version startVersion = invalidVersion;
		Future<Reference<IBackupContainer>> container;
		Future<Optional<std::vector<KeyRange>>> ranges; // Key ranges of this backup
		bool allWorkerStarted = false; // Only worker with Tag(-2,0) uses & sets this field
		bool stopped = false; // Is the backup stopped?
		bool ready = false; // Change to true when container and ranges are ready
	};

	KeyRangeMap<std::set<UID>> rangeMap; // Save key ranges to a set of backup UIDs
	std::map<UID, PerBackupInfo> backups; // Backup UID to infos
	PromiseStream<std::vector<std::pair<UID, Version>>> backupUidVersions; // active backup (UID, StartVersion) pairs
	AsyncTrigger changedTrigger;

	CounterCollection cc;
	Future<Void> logger;

	explicit BackupData(UID id, Reference<AsyncVar<ServerDBInfo>> db, const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), startVersion(req.startVersion), endVersion(req.endVersion),
	    recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch), minKnownCommittedVersion(invalidVersion),
	    savedVersion(invalidVersion), cc("BackupWorker", myId.toString()) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true);
		pullFinished.set(false);

		specialCounter(cc, "SavedVersion", [this]() { return this->savedVersion; });
		specialCounter(cc, "MinKnownCommittedVersion", [this]() { return this->minKnownCommittedVersion; });
		specialCounter(cc, "MsgQ", [this]() { return this->messages.size(); });
		logger = traceCounters("BackupWorkerMetrics", myId, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc,
		                       "BackupWorkerMetrics");
	}

	// Inserts a backup's single range into rangeMap.
	void insertRange(KeyRangeRef range, UID uid) {
		for (auto& logRange : rangeMap.modify(range)) {
			logRange->value().insert(uid);
		}
		for (auto& logRange : rangeMap.modify(singleKeyRange(metadataVersionKey))) {
			logRange->value().insert(uid);
		}
		TraceEvent("BackupWorkerInsertRange", myId)
		    .detail("BackupID", uid)
		    .detail("Begin", range.begin)
		    .detail("End", range.end);
	}

	// Inserts a backup's ranges into rangeMap.
	void insertRanges(const Optional<std::vector<KeyRange>>& ranges, UID uid) {
		if (!ranges.present() || ranges.get().empty()) {
			// insert full ranges of normal keys
			return insertRange(normalKeys, uid);
		}
		for (const auto& range : ranges.get()) {
			insertRange(range, uid);
		}
	}

	// Clears a backup's ranges in the rangeMap.
	void clearRanges(UID uid) {
		for (auto& logRange : rangeMap.ranges()) {
			logRange->value().erase(uid);
		}
		rangeMap.coalesce(allKeys);
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
				inserted.first->second.container = config.backupContainer().getOrThrow(cx);
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
};

// Monitors "backupStartedKey". If "started" is true, wait until the key is set;
// otherwise, wait until the key is cleared.
ACTOR Future<Void> monitorBackupStartedKeyChanges(BackupData* self, bool started) {
	loop {
		state ReadYourWritesTransaction tr(self->cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = wait(tr.get(backupStartedKey));
				if (value.present()) {
					self->backupUidVersions.send(decodeBackupStartedValue(value.get()));
					if (started) return Void();
				} else {
					self->backupUidVersions.send({});

					if (!started) {
						TraceEvent("BackupWorkerNoStartKey", self->myId);
						return Void();
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

// Returns true if the message is a mutation that should be backuped, i.e.,
// either key is not in system key space or is not a metadataVersionKey.
bool isBackupMessage(const VersionedMessage& msg, MutationRef* m) {
	for (Tag tag : msg.tags) {
		if (tag.locality == tagLocalitySpecial || tag.locality == tagLocalityTxs) {
			return false; // skip Txs mutations
		}
	}

	BinaryReader reader(msg.message.begin(), msg.message.size(), AssumeVersion(currentProtocolVersion));

	// Return false for LogProtocolMessage.
	if (LogProtocolMessage::isNextIn(reader)) return false;

	reader >> *m;

	// check for metadataVersionKey and special metadata mutations
	if (!normalKeys.contains(m->param1) && m->param1 != metadataVersionKey) {
		return false;
	}

	return true;
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

ACTOR Future<Void> addMutation(BackupData* self, UID backupUid, Reference<IBackupFile> logFile, int idx, MutationRef m,
                               int64_t* blockEnd, int blockSize, bool useM) {
	if (!self->backups[backupUid].isRunning()) return Void();

	state Standalone<StringRef> keep;
	state StringRef message;

	if (useM) {
		BinaryWriter wr(AssumeVersion(currentProtocolVersion));
		wr << m;
		keep = wr.toValue();
		message = keep;
	} else {
		message = self->messages[idx].message;
	}
	state int bytes = sizeof(Version) + sizeof(uint32_t) + sizeof(int) + message.size();

	// Convert to big Endianness for version.version, version.sub, and msgSize
	// The decoder assumes 0xFF is the end, so little endian can easily be
	// mistaken as the end. In contrast, big endian for version almost guarantee
	// the first byte is not 0xFF (should always be 0x00).
	BinaryWriter wr(Unversioned());
	wr << bigEndian64(self->messages[idx].version.version) << bigEndian32(self->messages[idx].version.sub)
	   << bigEndian32(message.size());
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
		// TODO: add block header
	}

	wait(logFile->append((void*)header.begin(), header.size()));
	wait(logFile->append(message.begin(), message.size()));
	return Void();
}

// Saves messages in the range of [0, numMsg) to a file and then remove these
// messages. The file format is a sequence of (Version, sub#, msgSize, message).
// Note only ready backups are saved.
ACTOR Future<Void> saveMutationsToFile(BackupData* self, Version popVersion, int numMsg) {
	state int blockSize = SERVER_KNOBS->BACKUP_FILE_BLOCK_BYTES;
	state std::vector<Future<Reference<IBackupFile>>> logFileFutures;
	state std::map<UID, int> uidMap; // Backup UID to index in logFileFutures & logFiles

	for (auto& uidInfo : self->backups) {
		if (!uidInfo.second.isRunning()) {
			// TODO: remove this uidInfo from self->backups & update self->rangeMap
			continue;
		}
		uidMap.emplace(uidInfo.first, logFileFutures.size());
		logFileFutures.push_back(uidInfo.second.container.get()->writeTaggedLogFile(
		    self->messages[0].getVersion(), popVersion, blockSize, self->tag.id));
	}
	wait(waitForAll(logFileFutures));

	state std::vector<Reference<IBackupFile>> logFiles;
	std::transform(logFileFutures.begin(), logFileFutures.end(), std::back_inserter(logFiles),
	               [](const Future<Reference<IBackupFile>>& f) { return f.get(); });

	for (const auto& file : logFiles) {
		TraceEvent("OpenMutationFile", self->myId)
		    .detail("StartVersion", self->messages[0].getVersion())
		    .detail("EndVersion", popVersion)
		    .detail("BlockSize", blockSize)
		    .detail("TagId", self->tag.id)
		    .detail("File", file->getFileName());
	}

	state int idx = 0;
	state int64_t blockEnd = 0;
	for (; idx < numMsg; idx++) {
		state MutationRef m;
		if (!isBackupMessage(self->messages[idx], &m)) continue;

		std::vector<Future<Void>> adds;
		if (m.type != MutationRef::Type::ClearRange) {
			for (UID uid : self->rangeMap[m.param1]) {
				auto it = uidMap.find(uid);
				if (it == uidMap.end()) continue;
				adds.push_back(addMutation(self, uid, logFiles[it->second], idx, m, &blockEnd, blockSize, false));
			}
		} else {
			KeyRangeRef mutationRange(m.param1, m.param2);
			KeyRangeRef intersectionRange;

			// Find intersection ranges and create mutations for sub-ranges
			for (auto range : self->rangeMap.intersectingRanges(mutationRange)) {
				const auto& subrange = range.range();
				intersectionRange = mutationRange & subrange;
				MutationRef subm(MutationRef::Type::ClearRange, intersectionRange.begin, intersectionRange.end);
				for (UID uid : range.value()) {
					auto it = uidMap.find(uid);
					if (it == uidMap.end()) continue;
					adds.push_back(addMutation(self, uid, logFiles[it->second], idx, subm, &blockEnd, blockSize, true));
				}
			}
		}
		wait(waitForAll(adds));
	}

	self->messages.erase(self->messages.begin(), self->messages.begin() + numMsg);

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
		if (self->messages.empty()) {
			// Even though messages is empty, we still want to advance popVersion.
			popVersion = std::max(popVersion, maxPopVersion);
		} else {
			int numMsg = 0;
			for (const auto& message : self->messages) {
				if (message.getVersion() > maxPopVersion) break;
				popVersion = std::max(popVersion, message.getVersion());
				numMsg++;
			}
			if (numMsg > 0) {
				wait(saveMutationsToFile(self, popVersion, numMsg));
			}
		}
		if (self->pullFinished.get() && self->messages.empty()) {
			// Advance popVersion to the endVersion to avoid gap between last
			// message version and the endVersion.
			popVersion = self->endVersion.get();
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
			when(wait(self->changedTrigger.onTrigger())) {
				// Check all backups and wait for container and key ranges
				std::vector<Future<Void>> all;
				for (auto& uidInfo : self->backups) {
					if (uidInfo.second.ready || uidInfo.second.stopped) continue;
					all.push_back(uidInfo.second.waitReady(uidInfo.first));
				}
				wait(waitForAll(all));
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
	state Future<Void> started, pullFinished;

	loop {
		started = monitorBackupStartedKeyChanges(self, true);
		loop {
			GetReadVersionRequest request(1, GetReadVersionRequest::PRIORITY_DEFAULT |
			                                     GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION);

			choose {
				when(wait(started)) { break; }
				when(wait(self->cx->onMasterProxiesChanged())) {}
				when(GetReadVersionReply reply = wait(loadBalance(self->cx->getMasterProxies(false),
				                                                  &MasterProxyInterface::getConsistentReadVersion,
				                                                  request, self->cx->taskID))) {
					self->savedVersion = std::max(reply.version, self->savedVersion);
					self->minKnownCommittedVersion = std::max(reply.version, self->minKnownCommittedVersion);
					TraceEvent("BackupWorkerNoopPop", self->myId).detail("SavedVersion", self->savedVersion);
					self->pop(); // Pop while the worker is in this NOOP state.
					wait(delay(SERVER_KNOBS->BACKUP_NOOP_POP_DELAY, self->cx->taskID));
				}
			}
		}

		Future<Void> stopped = monitorBackupStartedKeyChanges(self, false);
		pullFinished = pullAsyncData(self);
		wait(stopped || pullFinished);
		if (pullFinished.isReady()) return Void(); // backup is done for some old epoch.
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
	    .detail("StartVersion", req.startVersion)
	    .detail("EndVersion", req.endVersion.present() ? req.endVersion.get() : -1)
	    .detail("LogEpoch", req.recruitedEpoch)
	    .detail("BackupEpoch", req.backupEpoch);
	try {
		addActor.send(monitorBackupKeyOrPullData(&self));
		addActor.send(checkRemoved(db, req.recruitedEpoch, &self));
		addActor.send(waitFailureServer(interf.waitFailure.getFuture()));
		if (req.recruitedEpoch == req.backupEpoch && req.routerTag.id == 0) {
			addActor.send(monitorAllWorkerStarted(&self));
		}

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
			when(std::vector<std::pair<UID, Version>> uidVersions = waitNext(self.backupUidVersions.getFuture())) {
				TraceEvent e("BackupWorkerGotStartKey", self.myId);
				int i = 1;
				for (auto uidVersion : uidVersions) {
					e.detail(format("BackupID%d", i), uidVersion.first.toString())
					    .detail(format("Version%d", i), uidVersion.second);
					i++;
				}
				self.onBackupChanges(uidVersions);
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