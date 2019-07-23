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

#include "fdbclient/Notified.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct BackupData {
	const UID myId;
	const Tag tag; // LogRouter tag for this worker, i.e., (-2, i)
	const Version startVersion;
	Version endVersion;  // mutable in a new epoch, i.e., end version for this epoch is known.
	const LogEpoch epoch;
	Version minKnownCommittedVersion;
	Version poppedVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	Database cx;
	std::vector<TagsAndMessage> messages;
	std::vector<Version> versions;  // one for each of the "messages"
	NotifiedVersion version;
	AsyncTrigger backupDone;

	CounterCollection cc;
	Future<Void> logger;

	explicit BackupData(UID id, Reference<AsyncVar<ServerDBInfo>> db, const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), startVersion(req.startVersion),
	    endVersion(req.endVersion.present() ? req.endVersion.get() : std::numeric_limits<Version>::max()),
	    epoch(req.epoch), minKnownCommittedVersion(invalidVersion), poppedVersion(invalidVersion),
	    version(req.startVersion - 1), cc("BackupWorker", id.toString()) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true);

		specialCounter(cc, "PoppedVersion", [this]() { return this->poppedVersion; });
		specialCounter(cc, "MinKnownCommittedVersion", [this]() { return this->minKnownCommittedVersion; });
		logger = traceCounters("BackupWorkerMetrics", myId, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc,
		                       "BackupWorkerMetrics");
	}
};

ACTOR Future<Void> saveProgress(BackupData* self, Version backupVersion) {
	state Transaction tr(self->cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			tr.set(backupProgressKeyFor(self->myId), backupProgressValue(self->epoch, backupVersion));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Uploads self->messages to cloud storage.
ACTOR Future<Void> uploadData(BackupData* self) {
	state Version popVersion = invalidVersion;

	loop {
		ASSERT(self->messages.size() == self->versions.size());
		while (!self->messages.empty()) {
			popVersion = std::max(popVersion, self->versions[0]);
			// TODO: consume the messages
			self->messages.erase(self->messages.begin());
			self->versions.erase(self->versions.begin());
		}
		// TODO: upload messages
		Future<Void> savedProgress = Void();
		if (self->logSystem.get() && popVersion > self->poppedVersion) {
			savedProgress = saveProgress(self, popVersion);
		}
		wait(delay(30) && savedProgress); // TODO: knobify the delay of 30s
		if (self->logSystem.get() && popVersion > self->poppedVersion) {
			const Tag popTag = self->logSystem.get()->getPseudoPopTag(self->tag, ProcessClass::BackupClass);
			self->logSystem.get()->pop(popVersion, popTag);
			self->poppedVersion = popVersion;
			TraceEvent("BackupWorkerPop", self->myId)
			    .detail("V", popVersion)
			    .detail("Tag", self->tag.toString())
			    .detail("PopTag", popTag.toString());
		}
		if (self->poppedVersion >= self->endVersion) {
			self->backupDone.trigger();
		}
	}
}

// Pulls data from TLog servers using LogRouter tag.
ACTOR Future<Void> pullAsyncData(BackupData* self) {
	state Future<Void> logSystemChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = 0;
	state Version tagPopped = 0;
	state Version lastVersion = 0;

	loop {
		loop choose {
			when (wait(r ? r->getMore(TaskPriority::TLogCommit) : Never())) {
				break;
			}
			when (wait(logSystemChange)) {
				if (r) tagPopped = std::max(tagPopped, r->popped());
				if (self->logSystem.get()) {
					r = self->logSystem.get()->peekLogRouter(self->myId, tagAt, self->tag);
				} else {
					r = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			}
		}
		self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, r->getMinKnownCommittedVersion());

		// TODO: avoid peeking uncommitted messages
		// Should we wait until knownCommittedVersion == startVersion - 1 ? In this way, we know previous
		// epoch has finished and then starting for this epoch.
		while (r->hasMessage()) {
			lastVersion = r->version().version;
			self->messages.emplace_back(r->getMessage(), std::vector<Tag>());
			self->versions.push_back(lastVersion);
			r->nextMessage();
		}

		tagAt = std::max(r->version().version, lastVersion);
		TraceEvent("BackupWorkerGot", self->myId).detail("V", tagAt);
		if (tagAt > self->endVersion) return Void();
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, LogEpoch recoveryCount,
                                BackupData* self) {
	state UID lastMasterID(0,0);
	loop {
		bool isDisplaced =
		    (db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED);
		// (db->get().recoveryCount == recoveryCount && db->get().recoveryState == RecoveryState::FULLY_RECOVERED));
		isDisplaced = isDisplaced && !db->get().logSystemConfig.hasBackupWorker(self->myId);
		if (isDisplaced) {
			TraceEvent("BackupWorkerDisplaced", self->myId)
			    .detail("RecoveryCount", recoveryCount)
			    .detail("PoppedVersion", self->poppedVersion)
			    .detail("DBRecoveryCount", db->get().recoveryCount)
			    .detail("RecoveryState", (int)db->get().recoveryState);
			throw worker_removed();
		}
		if (db->get().master.id() != lastMasterID) {
			lastMasterID = db->get().master.id();
			const Version endVersion = db->get().logSystemConfig.getEpochEndVersion(recoveryCount);
			if (endVersion != invalidVersion) {
				TraceEvent("BackupWorkerSet", self->myId)
					.detail("Before", self->endVersion)
					.detail("Now", endVersion - 1);
				self->endVersion = endVersion - 1;
			}
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
	    .detail("LogEpoch", req.epoch);
	try {
		addActor.send(pullAsyncData(&self));
		addActor.send(uploadData(&self));
		addActor.send(waitFailureServer(interf.waitFailure.getFuture()));

		loop choose {
			when(wait(dbInfoChange)) {
				dbInfoChange = db->onChange();
				self.logSystem.set(ILogSystem::fromServerDBInfo(self.myId, db->get(), true));
			}
			when(HaltBackupRequest req = waitNext(interf.haltBackup.getFuture())) {
				req.reply.send(Void());
				TraceEvent("BackupWorkerHalted", interf.id()).detail("ReqID", req.requesterID);
				break;
			}
			when(wait(checkRemoved(db, req.epoch, &self))) {
				TraceEvent("BackupWorkerRemoved", interf.id());
				break;
			}
			when(wait(self.backupDone.onTrigger())) {
				TraceEvent("BackupWorkerDone", interf.id());
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