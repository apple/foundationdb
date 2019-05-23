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
	const Tag tag;
	const Version startVersion;
	Version minKnownCommittedVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	Database cx;
	std::vector<TagsAndMessage> messages;
	std::vector<Version> versions;  // one for each of the "messages"
	NotifiedVersion version;

	explicit BackupData(UID id, Reference<AsyncVar<ServerDBInfo>> db, const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), startVersion(req.startVersion), minKnownCommittedVersion(invalidVersion),
	    version(invalidVersion) {
		cx = openDBOnServer(db, TaskDefaultEndpoint, true, true);
	}
};

ACTOR Future<Void> setProgress(BackupData* self, int64_t recoveryCount, Version backupVersion) {
	state Transaction tr(self->cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			tr.set(backupProgressKeyFor(self->myId), backupProgressValue(recoveryCount, backupVersion));
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
	state Version lastPopVersion = 0;

	loop {
		ASSERT(self->messages.size() == self->versions.size());
		// TODO: upload messages
		while (!self->messages.empty()) {
			popVersion = std::max(popVersion, self->versions[0]);
			self->messages.erase(self->messages.begin());
			self->versions.erase(self->versions.begin());
		}
		Future<Void> saveProgress = Void();
		if (self->logSystem.get() && popVersion > lastPopVersion) {
			const Tag popTag = self->logSystem.get()->getPseudoPopTag(self->tag, ProcessClass::BackupClass);
			self->logSystem.get()->pop(popVersion, popTag);
			lastPopVersion = popVersion;
			TraceEvent("BackupWorkerPop", self->myId).detail("V", popVersion).detail("Tag", self->tag.toString());
			saveProgress = setProgress(self, 0, popVersion);
		}
		wait(delay(30) && saveProgress);
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
			when (wait(r ? r->getMore(TaskTLogCommit) : Never())) {
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
		while (r->hasMessage()) {
			lastVersion = r->version().version;
			self->messages.emplace_back(r->getMessage(), std::vector<Tag>());
			self->versions.push_back(lastVersion);
			r->nextMessage();
		}

		tagAt = std::max(r->version().version, lastVersion);
		TraceEvent("BackupWorkerGot", self->myId).detail("V", tagAt);
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, uint64_t recoveryCount,
                                BackupInterface myInterface) {
	loop {
		bool isDisplaced =
		    ((db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED) ||
		     (db->get().recoveryCount == recoveryCount && db->get().recoveryState == RecoveryState::FULLY_RECOVERED));
		if (isDisplaced) {
			for (auto& log : db->get().logSystemConfig.tLogs) {
				if (std::count(log.backupWorkers.begin(), log.backupWorkers.end(), myInterface.id())) {
					isDisplaced = false;
					break;
				}
			}
		}
		if (isDisplaced) {
			for (auto& old : db->get().logSystemConfig.oldTLogs) {
				for (auto& log : old.tLogs) {
					if (std::count(log.backupWorkers.begin(), log.backupWorkers.end(), myInterface.id())) {
						isDisplaced = false;
						break;
					}
				}
			}
		}
		if (isDisplaced) {
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

	TraceEvent("BackupWorkerStart", interf.id());
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
			when(wait(checkRemoved(db, req.recoveryCount, interf))) {
				TraceEvent("BackupWorkerRemoved", interf.id());
				break;
			}
			when(wait(error)) {}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed) {
			TraceEvent("BackupWorkerTerminated", interf.id()).error(e, true);
		} else {
			throw;
		}
	}
	return Void();
}