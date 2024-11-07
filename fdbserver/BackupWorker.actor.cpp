/*
 * BackupWorker.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/BlobCipher.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/BackupProgress.actor.h"
#include "fdbclient/GetEncryptCipherKeys.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"

#include "flow/IRandom.h"
#include "fdbclient/Tracing.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define SevDebugMemory SevVerbose

struct VersionedMessage {
	LogMessageVersion version;
	StringRef message;
	VectorRef<Tag> tags;
	Arena arena; // Keep a reference to the memory containing the message
	Arena decryptArena; // Arena used for decrypt buffer.
	size_t bytes; // arena's size when inserted, which can grow afterwards

	VersionedMessage(LogMessageVersion v, StringRef m, const VectorRef<Tag>& t, const Arena& a, size_t n)
	  : version(v), message(m), tags(t), arena(a), bytes(n) {}
	Version getVersion() const { return version.version; }
	uint32_t getSubVersion() const { return version.sub; }

	// Returns true if the message is a mutation that could be backed up (normal keys, system key backup ranges, or the
	// metadata version key)
	bool isCandidateBackupMessage(MutationRef* m,
	                              const std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>& cipherKeys) {
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
		if (m->isEncrypted()) {
			// In case the mutation is encrypted, get the decrypted mutation and also update message to point to
			// the decrypted mutation.
			// We use dedicated arena for decrypt buffer, as the other arena is used to count towards backup lock bytes.
			*m = m->decrypt(cipherKeys, decryptArena, BlobCipherMetrics::BACKUP, &message);
		}

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

	void collectCipherDetailIfEncrypted(std::unordered_set<BlobCipherDetails>& cipherDetails) {
		ASSERT(!message.empty());
		if (*message.begin() == MutationRef::Encrypted) {
			ArenaReader reader(arena, message, AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
			MutationRef m;
			reader >> m;
			m.updateEncryptCipherDetails(cipherDetails);
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
	Version popVersion; // Largest version popped in NOOP mode, can be larger than savedVersion.
	Reference<AsyncVar<ServerDBInfo> const> db;
	AsyncVar<Reference<ILogSystem>> logSystem;
	Database cx;
	std::vector<VersionedMessage> messages;
	NotifiedVersion pulledVersion;
	bool pulling = false;
	bool stopped = false;
	bool exitEarly = false; // If the worker is on an old epoch and all backups starts a version >= the endVersion
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

		ACTOR static Future<Void> _waitReady(PerBackupInfo* info) {
			wait(success(info->container) && success(info->ranges));
			return Void();
		}

		// Update the number of backup workers in the BackupConfig. Each worker
		// writes (epoch, tag.id) into the key. Worker 0 monitors the key and once
		// all workers have updated the key, this backup is considered as started
		// (i.e., the "submitBackup" call is successful). Worker 0 then sets
		// the "allWorkerStarted" flag, which in turn unblocks
		// StartFullBackupTaskFunc::_execute.
		ACTOR static Future<Void> _updateStartedWorkers(PerBackupInfo* info, BackupData* self, UID uid) {
			state BackupConfig config(uid);
			state Future<Void> watchFuture;
			state bool updated = false;
			state bool firstWorker = info->self->tag.id == 0;
			state bool allUpdated = false;
			state Optional<std::vector<std::pair<int64_t, int64_t>>> workers;
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					Optional<std::vector<std::pair<int64_t, int64_t>>> tmp =
					    wait(config.startedBackupWorkers().get(tr));
					workers = tmp;
					if (!updated) {
						// add this worker's info into "workers" vector
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
							return Void();
						}
						ASSERT(workers.present() && workers.get().size() > 0);
						std::vector<std::pair<int64_t, int64_t>>& v = workers.get();
						v.erase(std::remove_if(v.begin(),
						                       v.end(),
						                       [epoch = self->recruitedEpoch](const std::pair<int64_t, int64_t>& p) {
							                       return p.first != epoch;
						                       }),
						        v.end());
						std::set<int64_t> tags;
						for (auto p : v) {
							tags.insert(p.second);
						}
						if (self->totalTags == tags.size()) {
							// first set it to 0 to indicate all worker has been started, then get the commit version of this txn and set the version
							config.allWorkerStarted().set(tr, 0);
							allUpdated = true;
						} else {
							// monitor all workers' updates
							watchFuture = tr->watch(config.startedBackupWorkers().key);
						}
						ASSERT(workers.present() && workers.get().size() > 0);
						if (!updated) {
							config.startedBackupWorkers().set(tr, workers.get());
						}
						for (auto p : workers.get()) {
							TraceEvent("BackupWorkerDebugTag", self->myId)
							    .detail("Epoch", p.first)
							    .detail("TagID", p.second);
						}
						wait(tr->commit());

						updated = true; // Only set to true after commit.
						if (allUpdated) {
							Version commitVersion = tr->getCommittedVersion();
							tr->reset();
							config.allWorkerStarted().set(tr, commitVersion);
							wait(tr->commit());
							break;
						}
						wait(watchFuture);
						tr->reset();
					} else {
						// update startedBackupWorkers's value to "workers"
						ASSERT(workers.present() && workers.get().size() > 0);
						config.startedBackupWorkers().set(tr, workers.get());
						wait(tr->commit());
						break;
					}
				} catch (Error& e) {
					wait(tr->onError(e));
					allUpdated = false;
				}
			}
			TraceEvent("BackupWorkerSetReady", self->myId).detail("BackupID", uid).detail("TagId", self->tag.id);
			return Void();
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
	    minKnownCommittedVersion(invalidVersion), savedVersion(req.startVersion - 1), popVersion(req.startVersion - 1),
	    db(db), pulledVersion(0), paused(false), lock(new FlowLock(SERVER_KNOBS->BACKUP_LOCK_BYTES)),
	    cc("BackupWorker", myId.toString()) {
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

	bool allMessageSaved() const {
		return (endVersion.present() && savedVersion >= endVersion.get()) || stopped || exitEarly;
	}

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
		logSystem.get()->pop(std::max(popVersion, savedVersion), popTag);
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

		if (messages.size() == num) {
			messages.clear();
			TraceEvent(SevDebugMemory, "BackupWorkerMemory", myId).detail("ReleaseAll", lock->activePermits());
			lock->release(lock->activePermits());
			return;
		}

		// keep track of each arena and accumulate their sizes
		int64_t bytes = messages[0].bytes;
		for (int i = 1; i < num; i++) {
			const Arena& a = messages[i].arena;
			const Arena& b = messages[i - 1].arena;
			if (!a.sameArena(b)) {
				bytes += messages[i].bytes;
				TraceEvent(SevDebugMemory, "BackupWorkerMemory", myId).detail("Release", messages[i].bytes);
			}
		}
		lock->release(bytes);
		messages.erase(messages.begin(), messages.begin() + num);
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
			TraceEvent("Hfu5ChangeSavedVersion")
				.detail("BcakupEpoch", backupEpoch)
				.detail("RecruitedEpoch", recruitedEpoch)
				.detail("SavedVersion", savedVersion)
				.detail("StartVersion", startVersion)
				.log();
			savedVersion = std::max(minVersion, savedVersion);
		}
		if (modified)
			changedTrigger.trigger();
	}

	ACTOR static Future<Void> _waitAllInfoReady(BackupData* self) {
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

	ACTOR static Future<Version> _getMinKnownCommittedVersion(BackupData* self) {
		state Span span("BA:GetMinCommittedVersion"_loc);
		loop {
			try {
				GetReadVersionRequest request(span.context,
				                              0,
				                              TransactionPriority::DEFAULT,
				                              invalidVersion,
				                              GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION);
				choose {
					when(wait(self->cx->onProxiesChanged())) {}
					when(GetReadVersionReply reply =
					         wait(basicLoadBalance(self->cx->getGrvProxies(UseProvisionalProxies::False),
					                               &GrvProxyInterface::getConsistentReadVersion,
					                               request,
					                               self->cx->taskID))) {
						self->cx->ssVersionVectorCache.applyDelta(reply.ssVersionVectorDelta);
						return reply.version;
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_batch_transaction_throttled ||
				    e.code() == error_code_grv_proxy_memory_limit_exceeded) {
					// GRV Proxy returns an error
					wait(delayJittered(CLIENT_KNOBS->GRV_ERROR_RETRY_DELAY));
				} else {
					throw;
				}
			}
		}
	}

	Future<Version> getMinKnownCommittedVersion() { return _getMinKnownCommittedVersion(this); }
};

// Monitors "backupStartedKey". If "present" is true, wait until the key is set;
// otherwise, wait until the key is cleared. If "watch" is false, do not perform
// the wait for key set/clear events. Returns if key present.
ACTOR Future<bool> monitorBackupStartedKeyChanges(BackupData* self, bool present, bool watch) {
	loop {
		state ReadYourWritesTransaction tr(self->cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> value = wait(tr.get(backupStartedKey));
				state std::vector<std::pair<UID, Version>> uidVersions;
				state bool shouldExit = self->endVersion.present();
				if (value.present()) {
					uidVersions = decodeBackupStartedValue(value.get());
					state TraceEvent e("BackupWorkerGotStartKey", self->myId);
					state int i = 1;
					for (auto [uid, version] : uidVersions) {
						e.detail(format("BackupID%d", i), uid).detail(format("Version%d", i), version);
						i++;
						if (shouldExit && version < self->endVersion.get()) {
							shouldExit = false;
						}
						BackupConfig config(uid);
						// Optional<Version> taskStarted = wait(config.allWorkerStarted().get(tr)); this does not compile
						// transform the Value to Version, what's the best way?
						Optional<Value> taskStarted = wait(tr.get(config.allWorkerStarted().key));
						if (taskStarted.present()) {
							Version v = Tuple::unpack(taskStarted.get()).getInt(0);
							TraceEvent("Hfu5TaskPresent").detail("V", v).detail("Saved", self->savedVersion).log();
							self->savedVersion = std::max(self->savedVersion, v);
						}
					}
					self->exitEarly = shouldExit;
					self->onBackupChanges(uidVersions);
					// hfu5: it returns when backupStarted key is set
					if (present || !watch)
						return true;
				} else {
					TraceEvent("BackupWorkerEmptyStartKey", self->myId).log();
					self->onBackupChanges(uidVersions);

					self->exitEarly = shouldExit;
					if (!present || !watch) {
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

// Set "latestBackupWorkerSavedVersion" key for backups
ACTOR Future<Void> setBackupKeys(BackupData* self, std::map<UID, Version> savedLogVersions) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			state std::vector<Future<Optional<Version>>> prevVersions;
			state std::vector<BackupConfig> versionConfigs;
			state std::vector<Future<Optional<Version>>> allWorkersReady;
			for (const auto& [uid, version] : savedLogVersions) {
				versionConfigs.emplace_back(uid);
				prevVersions.push_back(versionConfigs.back().latestBackupWorkerSavedVersion().get(tr));
				allWorkersReady.push_back(versionConfigs.back().allWorkerStarted().get(tr));
			}

			wait(waitForAll(prevVersions) && waitForAll(allWorkersReady));

			for (int i = 0; i < prevVersions.size(); i++) {
				if (!allWorkersReady[i].get().present() || allWorkersReady[i].get().get() <= 0) {
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
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Note only worker with Tag (-2,0) runs this actor so that the latest saved
// version key is set by one process, which is stored in each BackupConfig in
// the system space. The client can know if a backup is restorable by checking
// log saved version > snapshot version.
ACTOR Future<Void> monitorBackupProgress(BackupData* self) {
	state Future<Void> interval;

	loop {
		interval = delay(SERVER_KNOBS->WORKER_LOGGING_INTERVAL / 2.0);
		while (self->backups.empty() || !self->logSystem.get()) {
			wait(self->changedTrigger.onTrigger() || self->logSystem.onChange());
		}

		// check all workers have started by checking their progress is larger
		// than the backup's start version.
		state Reference<BackupProgress> progress(new BackupProgress(self->myId, {}));
		wait(getBackupProgress(self->cx, self->myId, progress, /*logging=*/false));
		state std::map<Tag, Version> tagVersions = progress->getEpochStatus(self->recruitedEpoch);
		state std::map<UID, Version> savedLogVersions;
		if (tagVersions.size() != self->totalTags) {
			wait(interval);
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

		wait(interval && setKeys);
	}
}

ACTOR Future<Void> saveProgress(BackupData* self, Version backupVersion) {
	state Transaction tr(self->cx);
	state Key key = backupProgressKeyFor(self->myId);

	loop {
		try {
			// It's critical to save progress immediately so that after a master
			// recovery, the new master can know the progress so far.
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			WorkerBackupStatus status(self->backupEpoch, backupVersion, self->tag, self->totalTags);
			tr.set(key, backupProgressValue(status));
			tr.addReadConflictRange(singleKeyRange(key));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Write a mutation to a log file. Note the mutation can be different from
// message.message for clear mutations.
ACTOR Future<Void> addMutation(Reference<IBackupFile> logFile,
                               VersionedMessage message,
                               StringRef mutation,
                               int64_t* blockEnd,
                               int blockSize) {
	state int bytes = sizeof(Version) + sizeof(uint32_t) + sizeof(int) + mutation.size();

	// Convert to big Endianness for version.version, version.sub, and msgSize
	// The decoder assumes 0xFF is the end, so little endian can easily be
	// mistaken as the end. In contrast, big endian for version almost guarantee
	// the first byte is not 0xFF (should always be 0x00).
	BinaryWriter wr(Unversioned());
	wr << bigEndian64(message.version.version) << bigEndian32(message.version.sub) << bigEndian32(mutation.size());
	state Standalone<StringRef> header = wr.toValue();

	// Start a new block if needed
	if (logFile->size() + bytes > *blockEnd) {
		// Write padding if needed
		const int bytesLeft = *blockEnd - logFile->size();
		if (bytesLeft > 0) {
			state Value paddingFFs = fileBackup::makePadding(bytesLeft);
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

ACTOR static Future<Void> updateLogBytesWritten(BackupData* self,
                                                std::vector<UID> backupUids,
                                                std::vector<Reference<IBackupFile>> logFiles) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));

	ASSERT(backupUids.size() == logFiles.size());
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			for (int i = 0; i < backupUids.size(); i++) {
				BackupConfig config(backupUids[i]);
				config.logBytesWritten().atomicOp(tr, logFiles[i]->size(), MutationRef::AddValue);
			}
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Saves messages in the range of [0, numMsg) to a file and then remove these
// messages. The file content format is a sequence of (Version, sub#, msgSize, message).
// Note only ready backups are saved.
ACTOR Future<Void> saveMutationsToFile(BackupData* self,
                                       Version popVersion,
                                       int numMsg,
                                       std::unordered_set<BlobCipherDetails> cipherDetails) {
	state int blockSize = SERVER_KNOBS->BACKUP_FILE_BLOCK_BYTES;
	state std::vector<Future<Reference<IBackupFile>>> logFileFutures;
	state std::vector<Reference<IBackupFile>> logFiles;
	state std::vector<int64_t> blockEnds;
	state std::vector<UID> activeUids; // active Backups' UIDs
	state std::vector<Version> beginVersions; // logFiles' begin versions
	state KeyRangeMap<std::set<int>> keyRangeMap; // range to index in logFileFutures, logFiles, & blockEnds
	state std::vector<Standalone<StringRef>> mutations;
	state std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys;
	state int idx;

	// Make sure all backups are ready, otherwise mutations will be lost.
	while (!self->isAllInfoReady()) {
		wait(self->waitAllInfoReady());
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
				it->second.lastSavedVersion = std::max({ self->popVersion, self->savedVersion, self->startVersion });
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
	wait(waitForAll(logFileFutures));

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

	// Fetch cipher keys if any of the messages are encrypted.
	if (!cipherDetails.empty()) {
		std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> getCipherKeysResult =
		    wait(GetEncryptCipherKeys<ServerDBInfo>::getEncryptCipherKeys(
		        self->db, cipherDetails, BlobCipherMetrics::BLOB_GRANULE));
		cipherKeys = getCipherKeysResult;
	}

	blockEnds = std::vector<int64_t>(logFiles.size(), 0);
	for (idx = 0; idx < numMsg; idx++) {
		auto& message = self->messages[idx];
		MutationRef m;
		if (!message.isCandidateBackupMessage(&m, cipherKeys))
			continue;

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
		wait(waitForAll(adds));
		mutations.clear();
	}

	std::vector<Future<Void>> finished;
	std::transform(logFiles.begin(), logFiles.end(), std::back_inserter(finished), [](const Reference<IBackupFile>& f) {
		return f->finish();
	});

	wait(waitForAll(finished));

	for (const auto& file : logFiles) {
		TraceEvent("CloseMutationFile", self->myId)
		    .detail("FileSize", file->size())
		    .detail("TagId", self->tag.id)
		    .detail("File", file->getFileName());
	}
	for (const UID& uid : activeUids) {
		self->backups[uid].lastSavedVersion = popVersion + 1;
	}

	wait(updateLogBytesWritten(self, activeUids, logFiles));
	return Void();
}

// Uploads self->messages to cloud storage and updates savedVersion.
ACTOR Future<Void> uploadData(BackupData* self) {
	state Version popVersion = invalidVersion;

	loop {
		// Too large uploadDelay will delay popping tLog data for too long.
		state Future<Void> uploadDelay = delay(SERVER_KNOBS->BACKUP_UPLOAD_DELAY);

		state int numMsg = 0;
		state std::unordered_set<BlobCipherDetails> cipherDetails;
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
			message.collectCipherDetailIfEncrypted(cipherDetails);
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
		if (((numMsg > 0 || popVersion > lastPopVersion) && self->pulling) || self->pullFinished()) {
			TraceEvent("BackupWorkerSave", self->myId)
			    .detail("Version", popVersion)
			    .detail("LastPopVersion", lastPopVersion)
			    .detail("Pulling", self->pulling)
			    .detail("SavedVersion", self->savedVersion)
			    .detail("NumMsg", numMsg)
			    .detail("MsgQ", self->messages.size());
			// save an empty file for old epochs so that log file versions are continuous
			wait(saveMutationsToFile(self, popVersion, numMsg, cipherDetails));
			self->eraseMessages(numMsg);
		}

		// If transition into NOOP mode, should clear messages
		if (!self->pulling && self->backupEpoch == self->recruitedEpoch) {
			self->eraseMessages(self->messages.size());
		}

		if (popVersion > self->savedVersion && popVersion > self->popVersion) {
			// hfu5: saveProgress is called here, but not in NOOP mode. 
			// Thus there might be race condition when 1 backup runs in NOOP mode popping, ther other backup start and not seeing
			// certain versions being popped and try to get it from TLog.
			wait(saveProgress(self, popVersion));
			TraceEvent("BackupWorkerSavedProgress", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("Version", popVersion)
			    .detail("MsgQ", self->messages.size());
			self->savedVersion = std::max(popVersion, self->savedVersion);
			self->pop();
		}

		if (self->allMessageSaved()) {
			return Void();
		}

		if (!self->pullFinished()) {
			wait(uploadDelay || self->doneTrigger.onTrigger());
		}
	}
}

// Pulls data from TLog servers using LogRouter tag.
ACTOR Future<Void> pullAsyncData(BackupData* self) {
	TraceEvent("BackupWorkerPull", self->myId)
	    .detail("SavedVersion", self->savedVersion)
	    .detail("PopVersion", self->popVersion)
	    .log();
	state Future<Void> logSystemChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = std::max(self->pulledVersion.get(), std::max(self->startVersion, self->savedVersion));
	state Arena prev;

	loop {
		while (self->paused.get()) {
			wait(self->paused.onChange());
		}

		loop choose {
			when(wait(r ? r->getMore(TaskPriority::TLogCommit) : Never())) {
				break;
			}
			when(wait(logSystemChange)) {
				if (self->logSystem.get()) {
					r = self->logSystem.get()->peekLogRouter(
					    self->myId, tagAt, self->tag, SERVER_KNOBS->LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED);
				} else {
					r = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			}
		}

		// It's data loss issue if popped() > 0. It means mutations between popped() and tagAt are not available
		if (r->popped() > 0) {
			TraceEvent(SevWarn, "BackupWorkerPullMissingMutations", self->myId)
			    .detail("Tag", self->tag)
			    .detail("Start", self->startVersion)
			    .detail("Saved", self->savedVersion)
			    .detail("BackupEpoch", self->backupEpoch)
			    .detail("Popped", r->popped())
			    .detail("ExpectedPeekVersion", tagAt);
			throw worker_removed();
		}
		self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, r->getMinKnownCommittedVersion());

		// Note we aggressively peek (uncommitted) messages, but only committed
		// messages/mutations will be flushed to disk/blob in uploadData().
		while (r->hasMessage()) {
			state size_t takeBytes = 0;
			if (!prev.sameArena(r->arena())) {
				TraceEvent(SevDebugMemory, "BackupWorkerMemory", self->myId)
				    .detail("Take", r->arena().getSize())
				    .detail("Current", self->lock->activePermits());

				takeBytes = r->arena().getSize(); // more bytes can be allocated after the wait.
				wait(self->lock->take(TaskPriority::DefaultYield, takeBytes));
				prev = r->arena();
			}
			self->messages.emplace_back(r->version(), r->getMessage(), r->getTags(), r->arena(), takeBytes);
			r->nextMessage();
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
			return Void();
		}
		wait(yield());
	}
}

ACTOR Future<Void> monitorBackupKeyOrPullData(BackupData* self, bool keyPresent) {
	state Future<Void> pullFinished = Void();
	TraceEvent("MonitorBackupActor", self->myId)
	    .detail("SavedVersion", self->savedVersion)
	    .detail("PopVersion", self->popVersion)
	    .detail("KeyPresent", keyPresent)
	    .log();
	loop {
		state Future<bool> present = monitorBackupStartedKeyChanges(self, !keyPresent, /*watch=*/true);
		// NOOP mode is quitted once each backup worker found out the `backupStartedKey` is set
		// so it is always earlier than the allWorkerStarted
		if (keyPresent) {
			TraceEvent("Hfu5StartPull", self->myId)
			    .detail("SavedVersion", self->savedVersion)
			    .detail("PopVersion", self->popVersion)
			    .log();
			pullFinished = pullAsyncData(self);
			self->pulling = true;
			wait(success(present) || pullFinished);
			if (pullFinished.isReady()) {
				self->pulling = false;
				return Void(); // backup is done for some old epoch.
			}

			// Even though the snapshot is done, mutation logs may not be written
			// out yet. We need to make sure mutations up to this point is written.
			Version currentVersion = wait(self->getMinKnownCommittedVersion());
			wait(self->pulledVersion.whenAtLeast(currentVersion));
			pullFinished = Future<Void>(); // cancels pullAsyncData()
			self->pulling = false;
			TraceEvent("BackupWorkerPaused", self->myId).detail("Reason", "NoBackup");
		} else {
			// Backup key is not present, enter this NOOP POP mode.
			state Future<Version> committedVersion = self->getMinKnownCommittedVersion();

			loop choose {
				when(wait(success(present))) {
					TraceEvent("Hfu5Present", self->myId)
					    .detail("SavedVersion", self->savedVersion)
					    .detail("PopVersion", self->popVersion)
					    .log();
					break;
				}
				when(wait(success(committedVersion) || delay(SERVER_KNOBS->BACKUP_NOOP_POP_DELAY, self->cx->taskID))) {
					if (committedVersion.isReady()) {
						self->popVersion =
						    std::max(self->popVersion, std::max(committedVersion.get(), self->savedVersion));
						self->minKnownCommittedVersion =
						    std::max(committedVersion.get(), self->minKnownCommittedVersion);
						// wait(saveProgress(self, self->popVersion));
						self->savedVersion = std::max(self->popVersion, self->savedVersion);
						TraceEvent("BackupWorkerNoopPop", self->myId)
						    .detail("SavedVersion", self->savedVersion)
						    .detail("PopVersion", self->popVersion)
						    .log();
						self->pop(); // Pop while the worker is in this NOOP state.
						committedVersion = Never();
					} else {
						committedVersion = self->getMinKnownCommittedVersion();
					}
				}
			}
		}
		ASSERT(!keyPresent == present.get());
		keyPresent = !keyPresent;
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db, LogEpoch recoveryCount, BackupData* self) {
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

ACTOR static Future<Void> monitorWorkerPause(BackupData* self) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->cx));
	state Future<Void> watch;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> value = wait(tr->get(backupPausedKey));
			bool paused = value.present() && value.get() == "1"_sr;
			if (self->paused.get() != paused) {
				TraceEvent(paused ? "BackupWorkerPaused" : "BackupWorkerResumed", self->myId).log();
				self->paused.set(paused);
			}

			watch = tr->watch(backupPausedKey);
			wait(tr->commit());
			wait(watch);
			tr->reset();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> backupWorker(BackupInterface interf,
                                InitializeBackupRequest req,
                                Reference<AsyncVar<ServerDBInfo> const> db) {
	state BackupData self(interf.id(), db, req);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection(addActor.getFuture());
	state Future<Void> dbInfoChange = Void();
	state Future<Void> pull;
	state Future<Void> done;

	TraceEvent("BackupWorkerStart", self.myId)
	    .detail("Tag", req.routerTag.toString())
	    .detail("TotalTags", req.totalTags)
	    .detail("StartVersion", req.startVersion)
	    .detail("EndVersion", req.endVersion.present() ? req.endVersion.get() : -1)
	    .detail("LogEpoch", req.recruitedEpoch)
	    .detail("Saved", self.savedVersion)
	    .detail("BackupEpoch", req.backupEpoch);
	try {
		// hfu5: savedVersion is updated but not to the latest updated by noop mode.
		// it should be updated in the next lines below
		addActor.send(checkRemoved(db, req.recruitedEpoch, &self));
		addActor.send(waitFailureServer(interf.waitFailure.getFuture()));
		if (req.recruitedEpoch == req.backupEpoch && req.routerTag.id == 0) {
			addActor.send(monitorBackupProgress(&self));
		}
		addActor.send(monitorWorkerPause(&self));

		TraceEvent("BackupWorkerWaitKeyBeforeMonitor", self.myId)
		    .detail("Saved", self.savedVersion)
		    .detail("ExitEarly", self.exitEarly);
		// Check if backup key is present to avoid race between this check and
		// noop pop as well as upload data: pop or skip upload before knowing
		// there are backup keys. Set the "exitEarly" flag if needed.
		bool present = wait(monitorBackupStartedKeyChanges(&self, true, false));
		TraceEvent("BackupWorkerWaitKey", self.myId)
		    .detail("Present", present)
		    .detail("Saved", self.savedVersion)
		    .detail("ExitEarly", self.exitEarly);

		pull = self.exitEarly ? Void() : monitorBackupKeyOrPullData(&self, present);
		addActor.send(pull);
		done = self.exitEarly ? Void() : uploadData(&self);

		loop choose {
			when(wait(dbInfoChange)) {
				dbInfoChange = db->onChange();
				Reference<ILogSystem> ls = ILogSystem::fromServerDBInfo(self.myId, db->get(), true);
				bool hasPseudoLocality = ls.isValid() && ls->hasPseudoLocality(tagLocalityBackup);
				if (hasPseudoLocality) {
					self.logSystem.set(ls);
					self.oldestBackupEpoch = std::max(self.oldestBackupEpoch, ls->getOldestBackupEpoch());
				}
				TraceEvent("BackupWorkerLogSystem", self.myId)
				    .detail("HasBackupLocality", hasPseudoLocality)
				    .detail("OldestBackupEpoch", self.oldestBackupEpoch)
				    .detail("Tag", self.tag.toString());
			}
			when(wait(done)) {
				TraceEvent("BackupWorkerDone", self.myId).detail("BackupEpoch", self.backupEpoch);
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
			pull = Void(); // cancels pulling
			self.stop();
			try {
				wait(done);
			} catch (Error& e) {
				TraceEvent("BackupWorkerShutdownError", self.myId).errorUnsuppressed(e);
			}
		}
		TraceEvent("BackupWorkerTerminated", self.myId).errorUnsuppressed(err);
		if (err.code() != error_code_actor_cancelled && err.code() != error_code_worker_removed) {
			throw err;
		}
	}
	return Void();
}
