/*
 * BackupWorker.cpp
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
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/BackupInterface.h"
#include "fdbserver/core/BackupProgress.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/LogProtocolMessage.h"
#include "fdbserver/core/LogSystem.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/WaitFailure.h"
#include "fdbserver/backupworker/BackupWorker.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "flow/Error.h"

#include "flow/IRandom.h"
#include "fdbclient/Tracing.h"
#include "flow/CoroUtils.h"

#define SevDebugMemory SevVerbose

struct VersionedMessage {
	LogMessageVersion version;
	StringRef message;
	VectorRef<Tag> tags;
	Arena arena; // Keep a reference to the memory containing the message
	Arena decryptArena; // Arena used for decrypt buffer.

	VersionedMessage(LogMessageVersion v, StringRef m, const VectorRef<Tag>& t, const Arena& a)
	  : version(v), message(m), tags(t), arena(a) {}
	Version getVersion() const { return version.version; }
	uint32_t getSubVersion() const { return version.sub; }
	// Returns the estimated size of the message in bytes, assuming 6 tags.
	size_t getEstimatedSize() const { return message.size() + TagsAndMessage::getHeaderSize(6); }

	// Returns true if the message is a mutation
	bool isThisMessageMutation(MutationRef* m) {
		for (Tag tag : tags) {
			if (tag.locality == tagLocalitySpecial || tag.locality == tagLocalityTxs) {
				return false; // skip Txs mutations
			}
		}

		ArenaReader reader(arena, message, AssumeVersion(g_network->protocolVersion()));

		// Return false for LogProtocolMessage and SpanContextMessage metadata messages.
		if (LogProtocolMessage::isNextIn(reader))
			return false;
		if (reader.protocolVersion().hasSpanContext() && SpanContextMessage::isNextIn(reader))
			return false;
		if (reader.protocolVersion().hasOTELSpanContext() && OTELSpanContextMessage::isNextIn(reader)) {
			CODE_PROBE(true, "Returning false for OTELSpanContextMessage");
			return false;
		}

		reader >> *m;
		return true;
	}

	// Returns true if the message is a mutation that could be backed up (normal keys, system key backup ranges, or the
	// metadata version key)
	bool isCandidateBackupMessage(MutationRef* m) {
		if (!isThisMessageMutation(m))
			return false;

		// Return true if the mutation intersects any legal backup ranges
		if (normalKeys.contains(m->param1) || m->param1 == metadataVersionKey) {
			return true;
		} else if (m->type != MutationRef::Type::ClearRange) {
			return systemBackupMutationMask().rangeContaining(m->param1).value();
		} else {
			for (auto& r : systemBackupMutationMask().intersectingRanges(KeyRangeRef(m->param1, m->param2))) {
				if (r->value()) {
					return true;
				}
			}

			return false;
		}
	}
};

struct BackupData {
	const UID myId;
	const Tag tag; // LogRouter tag for this worker, i.e., (-2, i)
	const int totalTags; // Total log router tags
	const Version startVersion; // This worker's start version
	const Optional<Version> endVersion; // old epoch's end version (inclusive), or empty for current epoch
	const LogEpoch recruitedEpoch; // current epoch whose tLogs are receiving mutations
	const LogEpoch backupEpoch; // the epoch workers should pull mutations
	LogEpoch oldestBackupEpoch = 0; // oldest epoch that still has data on tLogs for backup to pull
	Version minKnownCommittedVersion;
	Version savedVersion; // Largest version saved to blob storage
	Reference<AsyncVar<ServerDBInfo> const> db;
	AsyncVar<Reference<ILogSystem>> logSystem;
	Database cx;
	std::vector<VersionedMessage> messages;
	NotifiedVersion pulledVersion;
	bool stopped = false;
	AsyncVar<bool> paused; // Track if "backupPausedKey" is set.
	Reference<FlowLock> lock;

	struct PerBackupInfo {
		PerBackupInfo() = default;
		PerBackupInfo(BackupData* data, UID uid, Version v) : self(data), startVersion(v) {
			// Open the container and get key ranges
			BackupConfig config(uid);
			container = config.backupContainer().get(data->cx.getReference());
			ranges = config.backupRanges().get(data->cx.getReference());
			if (self->backupEpoch == self->recruitedEpoch) {
				// Only current epoch's worker update the number of backup workers.
				updateWorker = _updateStartedWorkers(this, data, uid);
			}
			TraceEvent("BackupWorkerAddJob", data->myId).detail("BackupID", uid).detail("Version", v);
		}

		void stop() {
			stopped = true;
			updateWorker = Void(); // cancel actors
		}

		void cancelUpdater() { updateWorker = Void(); }

		bool isReady() const { return stopped || (container.isReady() && ranges.isReady()); }

		Future<Void> waitReady() {
			if (stopped)
				return Void();
			return _waitReady(this);
		}

		static Future<Void> _waitReady(PerBackupInfo* info) {
			co_await (success(info->container) && success(info->ranges));
		}

		// Update the number of backup workers in the BackupConfig. Each worker
		// writes (epoch, tag.id) into the key. Worker 0 monitors the key and once
		// all workers have updated the key, this backup is considered as started
		// (i.e., the "submitBackup" call is successful). Worker 0 then sets
		// the "allWorkerStarted" flag, which in turn unblocks
		// StartFullBackupTaskFunc::_execute.
		static Future<Void> _updateStartedWorkers(PerBackupInfo* info, BackupData* self, UID uid) {
			BackupConfig config(uid);
			Future<Void> watchFuture;
			bool updated = false;
			const bool firstWorker = info->self->tag.id == 0;
			bool allUpdated = false;
			Optional<std::vector<std::pair<int64_t, int64_t>>> workers;
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

			while (true) {
				Error err;
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					workers = co_await config.startedBackupWorkers().get(tr);
					if (!updated) {
						if (workers.present()) {
							workers.get().emplace_back(self->recruitedEpoch, (int64_t)self->tag.id);
						} else {
							std::vector<std::pair<int64_t, int64_t>> v(1, { self->recruitedEpoch, self->tag.id });
							workers = Optional<std::vector<std::pair<int64_t, int64_t>>>(v);
						}
					}

					if (firstWorker) {
						if (!workers.present()) {
							TraceEvent("BackupWorkerDetectAbortedJob", self->myId).detail("BackupID", uid);
							co_return;
						}
						ASSERT(workers.present() && workers.get().size() > 0);
						auto& v = workers.get();
						v.erase(std::remove_if(v.begin(),
						                       v.end(),
						                       [epoch = self->recruitedEpoch](const std::pair<int64_t, int64_t>& p) {
							                       return p.first != epoch;
						                       }),
						        v.end());
						std::set<int64_t> tags;
						for (const auto& p : v) {
							tags.insert(p.second);
						}
						if (self->totalTags == tags.size()) {
							config.allWorkerStarted().set(tr, true);
							allUpdated = true;
						} else {
							// monitor all workers' updates
							watchFuture = tr->watch(config.startedBackupWorkers().key);
						}
						ASSERT(workers.present() && workers.get().size() > 0);
						if (!updated) {
							config.startedBackupWorkers().set(tr, workers.get());
						}
						for (const auto& p : workers.get()) {
							TraceEvent("BackupWorkerDebugTag", self->myId)
							    .detail("Epoch", p.first)
							    .detail("TagID", p.second);
						}
						co_await tr->commit();

						updated = true; // Only set to true after commit.
						if (allUpdated) {
							break;
						}
						co_await watchFuture;
						tr->reset();
						continue;
					} else {
						ASSERT(workers.present() && workers.get().size() > 0);
						config.startedBackupWorkers().set(tr, workers.get());
						co_await tr->commit();
						break;
					}
				} catch (Error& e) {
					err = e;
					allUpdated = false;
				}
				co_await tr->onError(err);
			}
			TraceEvent("BackupWorkerSetReady", self->myId).detail("BackupID", uid).detail("TagId", self->tag.id);
		}

		BackupData* self = nullptr;

		// Backup request's commit version. Mutations are logged at some version after this.
		Version startVersion = invalidVersion;
		// The last mutation log's saved version (not inclusive), i.e., next log's begin version.
		Version lastSavedVersion = invalidVersion;

		Future<Optional<Reference<IBackupContainer>>> container;
		Future<Optional<std::vector<KeyRange>>> ranges; // Key ranges of this backup
		Future<Void> updateWorker;
		bool stopped = false; // Is the backup stopped?
	};

	std::map<UID, PerBackupInfo> backups; // Backup UID to infos
	AsyncTrigger changedTrigger;
	AsyncTrigger doneTrigger;

	CounterCollection cc;
	Future<Void> logger;

	explicit BackupData(UID id, Reference<AsyncVar<ServerDBInfo> const> db, const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), totalTags(req.totalTags), startVersion(req.startVersion),
	    endVersion(req.endVersion), recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch),
	    minKnownCommittedVersion(invalidVersion), savedVersion(req.startVersion - 1), db(db), pulledVersion(0),
	    paused(false), lock(new FlowLock(SERVER_KNOBS->BACKUP_WORKER_LOCK_BYTES)), cc("BackupWorker", myId.toString()) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True);

		specialCounter(cc, "SavedVersion", [this]() { return this->savedVersion; });
		specialCounter(cc, "MinKnownCommittedVersion", [this]() { return this->minKnownCommittedVersion; });
		specialCounter(cc, "MsgQ", [this]() { return this->messages.size(); });
		specialCounter(cc, "BufferedBytes", [this]() { return this->lock->activePermits(); });
		specialCounter(cc, "AvailableBytes", [this]() { return this->lock->available(); });
		logger =
		    cc.traceCounters("BackupWorkerMetrics", myId, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, "BackupWorkerMetrics");
	}

	bool pullFinished() const { return endVersion.present() && pulledVersion.get() > endVersion.get(); }

	bool allMessageSaved() const { return (endVersion.present() && savedVersion >= endVersion.get()) || stopped; }

	Version maxPopVersion() const { return endVersion.present() ? endVersion.get() : minKnownCommittedVersion; }

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
		if (backupEpoch > oldestBackupEpoch || stopped) {
			// Defer pop if old epoch hasn't finished popping yet.
			// If stopped because of displacement, do NOT pop as the progress may
			// not be saved in a timely fashion. As a result, next epoch may still
			// need to read mutations in the version range. Let the next epoch's
			// worker do the pop instead.
			TraceEvent("BackupWorkerPopDeferred", myId)
			    .suppressFor(1.0)
			    .detail("BackupEpoch", backupEpoch)
			    .detail("OldestEpoch", oldestBackupEpoch)
			    .detail("Version", savedVersion);
			return;
		}
		ASSERT_WE_THINK(backupEpoch == oldestBackupEpoch);
		const Tag popTag = logSystem.get()->getPseudoPopTag(tag, ProcessClass::BackupClass);
		DisabledTraceEvent("BackupWorkerPop", myId).detail("Tag", popTag).detail("SavedVersion", savedVersion);
		logSystem.get()->pop(savedVersion, popTag);
	}

	void stop() {
		stopped = true;
		for (auto& [uid, info] : backups) {
			// Cancel the actor. Because container is valid, CANNOT set the
			// "stop" flag that will block writing mutation files in
			// saveMutationsToFile().
			info.cancelUpdater();
		}
		doneTrigger.trigger();
	}

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
		TraceEvent(SevDebugMemory, "BackupWorkerMemory", myId)
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
				TraceEvent(SevDebugMemory, "BackupWorkerMemory", myId).detail("Release", bytes);
				lock->release(bytes);
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
		bool minVersionChanged = false;
		Version minVersion = std::numeric_limits<Version>::max();
		for (const auto& [uid, version] : uidVersions) {
			auto it = backups.find(uid);
			if (it == backups.end()) {
				modified = true;
				backups.emplace(uid, BackupData::PerBackupInfo(this, uid, version));
				minVersion = std::min(minVersion, version);
				minVersionChanged = true;
			} else {
				stopList.erase(uid);
			}
		}

		for (UID uid : stopList) {
			auto it = backups.find(uid);
			ASSERT(it != backups.end());
			it->second.stop();
			modified = true;
		}
		if (minVersionChanged && backupEpoch < recruitedEpoch && savedVersion + 1 == startVersion) {
			// Advance savedVersion to minimize version ranges in case backupEpoch's
			// progress is not saved. Master may set a very low startVersion that
			// is already popped. Advance the version is safe because these
			// versions are not popped -- if they are popped, their progress should
			// be already recorded and Master would use a higher version than minVersion.
			savedVersion = std::max(minVersion, savedVersion);
		}
		if (modified)
			changedTrigger.trigger();
	}

	static Future<Void> _waitAllInfoReady(BackupData* self) {
		std::vector<Future<Void>> all;
		for (auto it = self->backups.begin(); it != self->backups.end();) {
			if (it->second.stopped) {
				TraceEvent("BackupWorkerRemoveStoppedContainer", self->myId).detail("BackupId", it->first);
				it = self->backups.erase(it);
				continue;
			}

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

// If the worker is on an old epoch and all backups starts a version >= the endVersion
// it will exit early.
static Future<bool> shouldBackupWorkerExitEarly(BackupData* self) {
	while (true) {
		ReadYourWritesTransaction tr(self->cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = co_await tr.get(backupStartedKey);
				std::vector<std::pair<UID, Version>> uidVersions;
				if (value.present()) {
					bool shouldExit = self->endVersion.present();
					uidVersions = decodeBackupStartedValue(value.get());
					TraceEvent e("BackupWorkerGotStartKey", self->myId);
					int i = 1;
					for (auto [uid, version] : uidVersions) {
						e.detail(format("BackupID%d", i), uid).detail(format("Version%d", i), version);
						i++;
						if (shouldExit && version < self->endVersion.get()) {
							shouldExit = false;
						}
					}
					self->onBackupChanges(uidVersions);
					co_return shouldExit;
				}

				TraceEvent("BackupWorkerEmptyStartKey", self->myId);
				Future<Void> watchFuture = tr.watch(backupStartedKey);
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

// Monitors "backupStartedKey".
static Future<Void> monitorBackupStartedKeyChanges(BackupData* self) {
	while (true) {
		ReadYourWritesTransaction tr(self->cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = co_await tr.get(backupStartedKey);
				std::vector<std::pair<UID, Version>> uidVersions;
				if (value.present()) {
					uidVersions = decodeBackupStartedValue(value.get());
					TraceEvent e("BackupWorkerGotStartKey", self->myId);
					int i = 1;
					for (auto [uid, version] : uidVersions) {
						e.detail(format("BackupID%d", i), uid).detail(format("Version%d", i), version);
						i++;
					}
				}

				self->onBackupChanges(uidVersions);
				Future<Void> watchFuture = tr.watch(backupStartedKey);
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

// Set "latestBackupWorkerSavedVersion" key for backups
Future<Void> setBackupKeys(BackupData* self, std::map<UID, Version> savedLogVersions) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			std::vector<Future<Optional<Version>>> prevVersions;
			std::vector<BackupConfig> versionConfigs;
			std::vector<Future<Optional<bool>>> allWorkersReady;
			for (const auto& [uid, version] : savedLogVersions) {
				versionConfigs.emplace_back(uid);
				prevVersions.push_back(versionConfigs.back().latestBackupWorkerSavedVersion().get(tr));
				allWorkersReady.push_back(versionConfigs.back().allWorkerStarted().get(tr));
			}

			co_await (waitForAll(prevVersions) && waitForAll(allWorkersReady));

			for (int i = 0; i < prevVersions.size(); i++) {
				if (!allWorkersReady[i].get().present() || !allWorkersReady[i].get().get()) {
					continue;
				}

				const Version current = savedLogVersions[versionConfigs[i].getUid()];
				if (prevVersions[i].get().present()) {
					const Version prev = prevVersions[i].get().get();
					if (prev > current) {
						TraceEvent(SevWarn, "BackupWorkerVersionInverse", self->myId)
						    .detail("Prev", prev)
						    .detail("Current", current);
					}
				}
				if (self->backupEpoch == self->oldestBackupEpoch &&
				    (!prevVersions[i].get().present() || prevVersions[i].get().get() < current)) {
					TraceEvent("BackupWorkerSetVersion", self->myId)
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

// Note only worker with Tag (-2,0) runs this actor so that the latest saved
// version key is set by one process, which is stored in each BackupConfig in
// the system space. The client can know if a backup is restorable by checking
// log saved version > snapshot version.
Future<Void> monitorBackupProgress(BackupData* self) {
	Future<Void> interval;

	while (true) {
		interval = delay(SERVER_KNOBS->WORKER_LOGGING_INTERVAL / 2.0);
		while (self->backups.empty() || !self->logSystem.get()) {
			co_await (self->changedTrigger.onTrigger() || self->logSystem.onChange());
		}

		// check all workers have started by checking their progress is larger
		// than the backup's start version.
		Reference<BackupProgress> progress(new BackupProgress(self->myId, {}));
		co_await getBackupProgress(self->cx, self->myId, progress, SevDebug);
		std::map<Tag, Version> tagVersions = progress->getEpochStatus(self->recruitedEpoch);
		std::map<UID, Version> savedLogVersions;
		if (tagVersions.size() != self->totalTags) {
			co_await interval;
			continue;
		}

		// Check every version is larger than backup's startVersion
		for (auto& [uid, info] : self->backups) {
			if (self->recruitedEpoch == self->oldestBackupEpoch) {
				// update update progress so far if previous epochs are done
				Version v = std::numeric_limits<Version>::max();
				for (const auto& [tag, version] : tagVersions) {
					v = std::min(v, version);
				}
				savedLogVersions.emplace(uid, v);
				TraceEvent("BackupWorkerSavedBackupVersion", self->myId).detail("BackupID", uid).detail("Version", v);
			}
		}
		Future<Void> setKeys = savedLogVersions.empty() ? Void() : setBackupKeys(self, savedLogVersions);

		co_await (interval && setKeys);
	}
}

Future<Void> saveProgress(BackupData* self, Version backupVersion) {
	Transaction tr(self->cx);
	Key key = backupProgressKeyFor(self->myId);

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
				TraceEvent("BackupWorkerProgressSkipped", self->myId).detail("Reason", "BackupWorkersDisabled");
				co_return;
			}

			WorkerBackupStatus status(self->backupEpoch, backupVersion, self->tag, self->totalTags);
			tr.set(key, backupProgressValue(status));
			tr.addReadConflictRange(singleKeyRange(key));
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Write a mutation to a log file. Note the mutation can be different from
// message.message for clear mutations.
Future<Void> addMutation(Reference<IBackupFile> logFile,
                         VersionedMessage message,
                         StringRef mutation,
                         int64_t* blockEnd,
                         int blockSize) {
	// format: version, subversion, messageSize, message
	int bytes = sizeof(Version) + sizeof(uint32_t) + sizeof(int) + mutation.size();

	// Convert to big Endianness for version.version, version.sub, and msgSize
	// The decoder assumes 0xFF is the end, so little endian can easily be
	// mistaken as the end. In contrast, big endian for version almost guarantee
	// the first byte is not 0xFF (should always be 0x00).
	BinaryWriter wr(Unversioned());
	wr << bigEndian64(message.version.version) << bigEndian32(message.version.sub) << bigEndian32(mutation.size());
	Standalone<StringRef> header = wr.toValue();

	// Start a new block if needed
	if (logFile->size() + bytes > *blockEnd) {
		// Write padding if needed
		const int bytesLeft = *blockEnd - logFile->size();
		if (bytesLeft > 0) {
			Value paddingFFs = fileBackup::makePadding(bytesLeft);
			co_await logFile->append(paddingFFs.begin(), bytesLeft);
		}

		*blockEnd += blockSize;
		// write block Header
		co_await logFile->append((uint8_t*)&PARTITIONED_MLOG_VERSION, sizeof(PARTITIONED_MLOG_VERSION));
	}

	co_await logFile->append((void*)header.begin(), header.size());
	co_await logFile->append(mutation.begin(), mutation.size());
}

static Future<Void> updateLogBytesWritten(BackupData* self,
                                          std::vector<UID> backupUids,
                                          std::vector<Reference<IBackupFile>> logFiles) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

	ASSERT(backupUids.size() == logFiles.size());
	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			for (int i = 0; i < backupUids.size(); i++) {
				BackupConfig config(backupUids[i]);
				config.logBytesWritten().atomicOp(tr, logFiles[i]->size(), MutationRef::AddValue);
			}
			co_await tr->commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

// Saves messages in the range of [0, numMsg) to a file and then remove these
// messages. The file content format is a sequence of (Version, sub#, msgSize, message).
// Note only ready backups are saved.
Future<Void> saveMutationsToFile(BackupData* self, Version popVersion, int numMsg) {
	int blockSize = SERVER_KNOBS->BACKUP_FILE_BLOCK_BYTES;
	std::vector<Future<Reference<IBackupFile>>> logFileFutures;
	std::vector<Reference<IBackupFile>> logFiles;
	std::vector<int64_t> blockEnds;
	std::vector<UID> activeUids; // active Backups' UIDs
	std::vector<Version> beginVersions; // logFiles' begin versions
	KeyRangeMap<std::set<int>> keyRangeMap; // range to index in logFileFutures, logFiles, & blockEnds
	std::vector<Standalone<StringRef>> mutations;
	int idx{ 0 };

	// Make sure all backups are ready, otherwise mutations will be lost.
	while (!self->isAllInfoReady()) {
		co_await self->waitAllInfoReady();
	}

	for (auto it = self->backups.begin(); it != self->backups.end();) {
		if (it->second.stopped || !it->second.container.get().present()) {
			TraceEvent("BackupWorkerNoContainer", self->myId).detail("BackupId", it->first);
			it = self->backups.erase(it);
			continue;
		}
		const int index = logFileFutures.size();
		activeUids.push_back(it->first);
		self->insertRanges(keyRangeMap, it->second.ranges.get(), index);

		if (it->second.lastSavedVersion == invalidVersion) {
			if (it->second.startVersion > self->startVersion && !self->messages.empty()) {
				// True-up first mutation log's begin version
				it->second.lastSavedVersion = self->messages[0].getVersion();
			} else {
				it->second.lastSavedVersion = std::max({ self->savedVersion, self->startVersion });
			}
			TraceEvent("BackupWorkerTrueUp", self->myId).detail("LastSavedVersion", it->second.lastSavedVersion);
		}
		// The true-up version can be larger than first message version, so keep
		// the begin versions for later muation filtering.
		beginVersions.push_back(it->second.lastSavedVersion);

		logFileFutures.push_back(it->second.container.get().get()->writeTaggedLogFile(
		    it->second.lastSavedVersion, popVersion + 1, blockSize, self->tag.id, self->totalTags));
		it++;
	}

	keyRangeMap.coalesce(allKeys);
	co_await waitForAll(logFileFutures);

	std::transform(logFileFutures.begin(),
	               logFileFutures.end(),
	               std::back_inserter(logFiles),
	               [](const Future<Reference<IBackupFile>>& f) { return f.get(); });

	ASSERT(activeUids.size() == logFiles.size() && beginVersions.size() == logFiles.size());
	for (int i = 0; i < logFiles.size(); i++) {
		TraceEvent("OpenMutationFile", self->myId)
		    .detail("BackupID", activeUids[i])
		    .detail("TagId", self->tag.id)
		    .detail("File", logFiles[i]->getFileName());
	}

	blockEnds = std::vector<int64_t>(logFiles.size(), 0);
	for (idx = 0; idx < numMsg; idx++) {
		auto& message = self->messages[idx];
		MutationRef m;
		if (!message.isCandidateBackupMessage(&m)) {
			continue;
		}

		DEBUG_MUTATION("addMutation", message.version.version, m, self->myId)
		    .detail("KCV", self->minKnownCommittedVersion)
		    .detail("SavedVersion", self->savedVersion);

		std::vector<Future<Void>> adds;
		if (m.type != MutationRef::Type::ClearRange) {
			for (int index : keyRangeMap[m.param1]) {
				if (message.getVersion() >= beginVersions[index]) {
					adds.push_back(
					    addMutation(logFiles[index], message, message.message, &blockEnds[index], blockSize));
				}
			}
		} else {
			KeyRangeRef mutationRange(m.param1, m.param2);
			KeyRangeRef intersectionRange;

			// Find intersection ranges and create mutations for sub-ranges
			for (auto range : keyRangeMap.intersectingRanges(mutationRange)) {
				const auto& subrange = range.range();
				intersectionRange = mutationRange & subrange;
				MutationRef subm(MutationRef::Type::ClearRange, intersectionRange.begin, intersectionRange.end);
				BinaryWriter wr(AssumeVersion(g_network->protocolVersion()));
				wr << subm;
				mutations.push_back(wr.toValue());
				for (int index : range.value()) {
					if (message.getVersion() >= beginVersions[index]) {
						adds.push_back(
						    addMutation(logFiles[index], message, mutations.back(), &blockEnds[index], blockSize));
					}
				}
			}
		}
		co_await waitForAll(adds);
		mutations.clear();
	}

	std::vector<Future<Void>> finished;
	std::transform(logFiles.begin(), logFiles.end(), std::back_inserter(finished), [](const Reference<IBackupFile>& f) {
		return f->finish();
	});

	co_await waitForAll(finished);

	for (const auto& file : logFiles) {
		TraceEvent("CloseMutationFile", self->myId)
		    .detail("FileSize", file->size())
		    .detail("TagId", self->tag.id)
		    .detail("File", file->getFileName());
	}
	for (const UID& uid : activeUids) {
		self->backups[uid].lastSavedVersion = popVersion + 1;
	}

	co_await updateLogBytesWritten(self, activeUids, logFiles);
}

// Uploads self->messages to cloud storage and updates savedVersion.
Future<Void> uploadData(BackupData* self) {
	Version popVersion = invalidVersion;

	while (true) {
		// Too large uploadDelay will delay popping tLog data for too long.
		Future<Void> uploadDelay = delay(SERVER_KNOBS->BACKUP_UPLOAD_DELAY);

		int numMsg = 0;
		Version lastPopVersion = popVersion;
		// index of last version's end position in self->messages
		int lastVersionIndex = 0;
		Version lastVersion = invalidVersion;

		for (auto& message : self->messages) {
			// message may be prefetched in peek; uncommitted message should not be uploaded.
			const Version version = message.getVersion();
			if (version > self->maxPopVersion()) {
				break;
			}
			if (version > popVersion) {
				lastVersionIndex = numMsg;
				lastVersion = popVersion;
				popVersion = version;
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
		if ((numMsg > 0 || popVersion > lastPopVersion) || self->pullFinished()) {
			TraceEvent("BackupWorkerSave", self->myId)
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
			TraceEvent("BackupWorkerSavedProgress", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("Version", popVersion)
			    .detail("MsgQ", self->messages.size());
			self->savedVersion = std::max(popVersion, self->savedVersion);
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

// Pulls data from TLog servers using LogRouter tag.
Future<Void> pullAsyncData(BackupData* self) {
	Future<Void> logSystemChange = Void();
	Reference<ILogSystem::IPeekCursor> r;

	Version tagAt = std::max({ self->pulledVersion.get(), self->startVersion, self->savedVersion });

	TraceEvent("BackupWorkerPull", self->myId)
	    .detail("Tag", self->tag)
	    .detail("Version", tagAt)
	    .detail("StartVersion", self->startVersion)
	    .detail("SavedVersion", self->savedVersion);
	while (true) {
		while (self->paused.get()) {
			co_await self->paused.onChange();
		}

		while (true) {
			Future<Void> getMoreFuture = r ? r->getMore(TaskPriority::TLogCommit) : Never();
			auto const res = co_await race(getMoreFuture, logSystemChange);
			if (res.index() == 0) {
				// getMoreFuture finished
				DisabledTraceEvent("BackupWorkerGotMore", self->myId)
				    .detail("Tag", self->tag)
				    .detail("CursorVersion", r->version().version);
				break;
			} else {
				// logSystemChange detected
				if (self->logSystem.get()) {
					r = self->logSystem.get()->peekLogRouter(
					    self->myId, tagAt, self->tag, SERVER_KNOBS->LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED);
				} else {
					r = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			}
		}
		// When TLog sets popped version, it means mutations between popped() and tagAt are unavailable
		// on the TLog. So, we should stop pulling data from the TLog.
		if (r->popped() > 0) {
			TraceEvent(SevError, "BackupWorkerPullMissingMutations", self->myId)
			    .detail("Tag", self->tag)
			    .detail("BackupEpoch", self->backupEpoch)
			    .detail("Popped", r->popped())
			    .detail("ExpectedPeekVersion", tagAt)
			    .detail("RecruitedEpoch", self->recruitedEpoch);
			ASSERT(true);
		}
		self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, r->getMinKnownCommittedVersion());

		// Note we aggressively peek (uncommitted) messages, but only committed
		// messages/mutations will be flushed to disk/blob in uploadData().
		int64_t peekedBytes = 0;
		// Hold messages until we know how many we can take, self->messages always
		// contains messages that we have reserved memory for. Therefore, lock->release()
		// will always encounter message with reserved memory.
		std::vector<VersionedMessage> tmpMessages;
		while (r->hasMessage()) {
			tmpMessages.emplace_back(r->version(), r->getMessage(), r->getTags(), r->arena());
			peekedBytes += tmpMessages.back().getEstimatedSize();
			r->nextMessage();
		}
		if (peekedBytes > 0) {
			TraceEvent(SevDebugMemory, "BackupWorkerMemory", self->myId)
			    .detail("Take", peekedBytes)
			    .detail("Current", self->lock->activePermits());
			co_await self->lock->take(TaskPriority::DefaultYield, peekedBytes);
			self->messages.insert(self->messages.end(),
			                      std::make_move_iterator(tmpMessages.begin()),
			                      std::make_move_iterator(tmpMessages.end()));
		}

		tagAt = r->version().version;
		self->pulledVersion.set(tagAt);
		TraceEvent("BackupWorkerGot", self->myId).suppressFor(1.0).detail("V", tagAt);
		if (self->pullFinished()) {
			self->eraseMessagesAfterEndVersion();
			self->doneTrigger.trigger();
			TraceEvent("BackupWorkerFinishPull", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("VersionGot", tagAt)
			    .detail("EndVersion", self->endVersion.get())
			    .detail("MsgQ", self->messages.size());
			co_return;
		}
		co_await yield();
	}
}

Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db, LogEpoch recoveryCount, BackupData* self) {
	while (true) {
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
		co_await db->onChange();
	}
}

static Future<Void> monitorWorkerPause(BackupData* self) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));
	Future<Void> watch;

	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> value = co_await tr->get(backupPausedKey);
			bool paused = value.present() && value.get() == "1"_sr;
			if (self->paused.get() != paused) {
				TraceEvent(paused ? "BackupWorkerPaused" : "BackupWorkerResumed", self->myId).log();
				self->paused.set(paused);
			}

			watch = tr->watch(backupPausedKey);
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

Future<Void> backupWorker(BackupInterface interf,
                          InitializeBackupRequest req,
                          Reference<AsyncVar<ServerDBInfo> const> db) {
	BackupData self(interf.id(), db, req);
	PromiseStream<Future<Void>> addActor;
	Future<Void> error = actorCollection(addActor.getFuture());
	Future<Void> dbInfoChange = Void();
	Future<Void> pull;
	Future<Void> done;
	Error err;

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
			addActor.send(monitorBackupProgress(&self));
		}
		addActor.send(monitorWorkerPause(&self));

		// If the worker is on an old epoch and all backups starts a version >= the endVersion
		bool exitEarly = co_await shouldBackupWorkerExitEarly(&self);
		TraceEvent("BackupWorkerExitEarly", self.myId).detail("ExitEarly", exitEarly);
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
				Reference<ILogSystem> ls = makeLogSystemFromServerDBInfo(self.myId, db->get(), true);
				bool hasPseudoLocality = ls.isValid() && ls->hasPseudoLocality(tagLocalityBackup);
				if (hasPseudoLocality) {
					self.logSystem.set(ls);
					self.oldestBackupEpoch = std::max(self.oldestBackupEpoch, ls->getOldestBackupEpoch());
				}
				TraceEvent("BackupWorkerLogSystem", self.myId)
				    .detail("HasBackupLocality", hasPseudoLocality)
				    .detail("OldestBackupEpoch", self.oldestBackupEpoch)
				    .detail("Tag", self.tag.toString());
			} else if (res.index() == 1) {
				TraceEvent("BackupWorkerDone", self.myId).detail("BackupEpoch", self.backupEpoch);
				// Notify master so that this worker can be removed from log system, then this
				// worker (for an old epoch's unfinished work) can safely exit.
				co_await brokenPromiseToNever(db->get().clusterInterface.notifyBackupWorkerDone.getReply(
				    BackupWorkerDoneRequest(self.myId, self.backupEpoch)));
				break;
			}
		}
		co_return;
	} catch (Error& e) {
		err = e;
	}

	if (err.code() == error_code_worker_removed) {
		pull = Void(); // cancels pulling
		self.stop();
		try {
			co_await done;
		} catch (Error& shutdownErr) {
			TraceEvent("BackupWorkerShutdownError", self.myId).errorUnsuppressed(shutdownErr);
		}
	}
	TraceEvent("BackupWorkerTerminated", self.myId).errorUnsuppressed(err);
	if (err.code() != error_code_actor_cancelled && err.code() != error_code_worker_removed) {
		throw err;
	}
}
