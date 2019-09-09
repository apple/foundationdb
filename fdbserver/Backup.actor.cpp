/*
 * Backup.actor.cpp
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

#include "fdbclient/BackupContainer.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BackupInterface.h"
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

	VersionedMessage(LogMessageVersion v, StringRef m) : version(v), message(m) {}
	const Version getVersion() const { return version.version; }
	const uint32_t getSubVersion() const { return version.sub; }
};

struct BackupData {
	const UID myId;
	const Tag tag; // LogRouter tag for this worker, i.e., (-2, i)
	const Version startVersion;
	const Version endVersion; // old epoch's end version (inclusive), or numeric limit for current epoch
	const LogEpoch recruitedEpoch;
	const LogEpoch backupEpoch;
	Version minKnownCommittedVersion;
	Version savedVersion, lastSeenVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	Database cx;
	std::vector<VersionedMessage> messages;
	Reference<IBackupContainer> container;

	CounterCollection cc;
	Future<Void> logger;

	explicit BackupData(UID id, Reference<AsyncVar<ServerDBInfo>> db, const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), startVersion(req.startVersion),
	    endVersion(req.endVersion.present() ? req.endVersion.get() : std::numeric_limits<Version>::max()),
	    recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch), minKnownCommittedVersion(invalidVersion),
	    savedVersion(invalidVersion), lastSeenVersion(invalidVersion), cc("BackupWorker", id.toString()) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true);

		specialCounter(cc, "SavedVersion", [this]() { return this->savedVersion; });
		specialCounter(cc, "MinKnownCommittedVersion", [this]() { return this->minKnownCommittedVersion; });
		logger = traceCounters("BackupWorkerMetrics", myId, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc,
		                       "BackupWorkerMetrics");
		if (g_network->isSimulated()) {
			container = IBackupContainer::openContainer("file://simfdb/mutation_backups/");
		} else {
			// TODO: use blobstore URL passed from somewhere.
			ASSERT(false);
			container = IBackupContainer::openContainer("blobstore://");
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
};

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

// Debug function: to be removed.
void parseMessage(StringRef message) {
	// MutationRef m = BinaryReader::fromStringRef<MutationRef>(message, AssumeVersion(currentProtocolVersion));
	// std::cout << m.toString() << std::endl;
	std::cout << message.printable() << " : " << message.toHexString() << std::endl;
	BinaryReader reader(message.begin(), message.size(), Unversioned());
	if (LogProtocolMessage::isNextIn(reader)) {
		LogProtocolMessage lpm;
		reader >> lpm;
	} else {
		MutationRef m;
		reader >> m;
		std::cout << m.toString() << std::endl;
	}
}

// Uploads self->messages to cloud storage and updates poppedVersion.
ACTOR Future<Void> uploadData(BackupData* self) {
	state Version popVersion = invalidVersion;

	loop {
		if (self->savedVersion >= self->endVersion) {
			return Void();
		}

		// FIXME: knobify the delay of 10s. This delay is sensitive, as it is the
		// lag TLog might have. Changing to 20s may fail consistency check.
		state Future<Void> uploadDelay = delay(10);

		state std::string logFilename;
		if (self->messages.empty()) {
			// Even though messages is empty, we still want to advance popVersion.
			popVersion = std::max(popVersion, self->lastSeenVersion);
		} else {
			state int numMsg = 0;
			for (auto it = self->messages.begin(); it < self->messages.end(); it++) {
				if (it->getVersion() > std::min(self->endVersion, self->minKnownCommittedVersion)) break;
				popVersion = std::max(popVersion, it->getVersion());
				numMsg++;
			}
			if (numMsg > 0) {
				// Upload messages
				const int blockSize = 1 << 20;
				state Reference<IBackupFile> logFile =
				    wait(self->container->writeLogFile(self->messages[0].getVersion(), popVersion, blockSize));
				state int idx = 0;
				for (; idx < numMsg; idx++) {
					// TODO: Endianness for version.version & version.sub
					wait(logFile->append((void*)&self->messages[idx].version.version, sizeof(Version)));
					wait(logFile->append((void*)&self->messages[idx].version.sub, sizeof(int32_t)));
					// parseMessage(self->messages[idx].message);
					wait(logFile->appendStringRefWithLen(self->messages[idx].message));
				}

				self->messages.erase(self->messages.begin(), self->messages.begin() + numMsg);
				wait(logFile->finish());
				logFilename = logFile->getFileName(); // TODO: save this somewhere with tag info.
			}
		}

		state Future<Void> savedProgress = Never();
		if (popVersion > self->savedVersion) {
			savedProgress = saveProgress(self, popVersion);
		}

		loop choose {
			when(wait(uploadDelay)) {
				// Note the progress may not be saved yet. Cancel it?
				break;
			}
			when(wait(savedProgress)) {
				TraceEvent("BackupWorkerSavedProgress", self->myId).detail("Version", popVersion);
				self->savedVersion = std::max(popVersion, self->savedVersion);
				self->pop();
				savedProgress = Never();  // Still wait until uploadDelay expires
			}
		}
	}
}

// Pulls data from TLog servers using LogRouter tag.
ACTOR Future<Void> pullAsyncData(BackupData* self) {
	state Future<Void> logSystemChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = self->startVersion;
	state Version lastVersion = 0;

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
			self->messages.emplace_back(r->version(), r->getMessage());
			r->nextMessage();
		}

		tagAt = std::max(r->version().version, lastVersion);
		self->lastSeenVersion = std::max(tagAt, self->lastSeenVersion);
		TraceEvent("BackupWorkerGot", self->myId).suppressFor(1.0).detail("V", tagAt);
		if (tagAt > self->endVersion) {
			TraceEvent("BackupWorkerFinishPull", self->myId)
			    .detail("VersionGot", tagAt)
			    .detail("EndVersion", self->endVersion);
			return Void();
		}
		wait(yield());
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

	TraceEvent("BackupWorkerStart", interf.id())
	    .detail("Tag", req.routerTag.toString())
	    .detail("StartVersion", req.startVersion)
	    .detail("EndVersion", req.endVersion.present() ? req.endVersion.get() : -1)
	    .detail("LogEpoch", req.recruitedEpoch)
	    .detail("BackupEpoch", req.backupEpoch);
	try {
		addActor.send(pullAsyncData(&self));
		addActor.send(checkRemoved(db, req.recruitedEpoch, &self));
		addActor.send(waitFailureServer(interf.waitFailure.getFuture()));

		state Future<Void> done = uploadData(&self);

		loop choose {
			when(wait(dbInfoChange)) {
				dbInfoChange = db->onChange();
				Reference<ILogSystem> ls = ILogSystem::fromServerDBInfo(self.myId, db->get(), true);
				bool hasPseudoLocality = ls.isValid() && ls->hasPseudoLocality(tagLocalityBackup);
				if (hasPseudoLocality) {
					self.logSystem.set(ls);
					self.pop();
				}
				TraceEvent("BackupWorkerLogSystem", interf.id())
				    .detail("HasBackupLocality", hasPseudoLocality)
				    .detail("Tag", self.tag.toString());
			}
			when(wait(done)) {
				TraceEvent("BackupWorkerDone", interf.id()).detail("BackupEpoch", self.backupEpoch);
				// Notify master so that this worker can be removed from log system, then this
				// worker (for an old epoch's unfinished work) can safely exit.
				wait(brokenPromiseToNever(db->get().master.notifyBackupWorkerDone.getReply(
				    BackupWorkerDoneRequest(self.myId, self.endVersion))));
				break;
			}
			when(wait(error)) {}
		}
	} catch (Error& e) {
		TraceEvent("BackupWorkerTerminated", interf.id()).error(e, true);
		if (e.code() != error_code_actor_cancelled && e.code() != error_code_worker_removed) {
			throw;
		}
	}
	return Void();
}