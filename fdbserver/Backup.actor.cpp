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

#include "fdbserver/BackupInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct BackupData {
  UID myId;

  explicit BackupData(UID id, const InitializeBackupRequest& req) : myId(id) {}
};

// Pulls data from TLog servers using LogRouter tag.
ACTOR Future<Void> pullAsyncData(BackupData* self) {
  loop {
    wait(Never());
  }
}

ACTOR Future<Void> backupCore(
	BackupInterface interf,
	InitializeBackupRequest req,
	Reference<AsyncVar<ServerDBInfo>> db)
{
  state BackupData self(interf.id(), req);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection( addActor.getFuture() );

	addActor.send(pullAsyncData(&self));

  loop choose {
    when (wait(db->onChange())) {}
    when (wait(error)) {}
  }
}

ACTOR Future<Void> backupWorker(
  BackupInterface interf, InitializeBackupRequest req, Reference<AsyncVar<ServerDBInfo>> db)
{
	try {
		TraceEvent("BackupWorkerStart", interf.id());
		state Future<Void> core = backupCore(interf, req, db);
		loop choose{
			when(wait(core)) { return Void(); }
		}
	}
	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed)
		{
			TraceEvent("LogRouterTerminated", interf.id()).error(e, true);
			return Void();
		}
		throw;
	}
}