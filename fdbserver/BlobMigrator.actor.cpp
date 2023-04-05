/*
 * BlobMigrator.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/ActorCollection.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WaitFailure.h"

#include "flow/actorcompiler.h" // has to be last include

// BlobMigrator manages data migration from blob storage to storage server. It implements a minimal set of
// StorageServerInterface APIs which are needed for DataDistributor to start data migration.
class BlobMigrator : public NonCopyable, public ReferenceCounted<BlobMigrator> {
public:
	BlobMigrator(Reference<AsyncVar<ServerDBInfo> const> dbInfo, BlobMigratorInterface interf)
	  : blobMigratorInterf(interf), actors(false) {
		if (!blobConn.isValid() && SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			blobConn = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
		}
		db = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	}
	~BlobMigrator() {}

	ACTOR static Future<Void> start(Reference<BlobMigrator> self) {
		self->actors.add(waitFailureServer(self->blobMigratorInterf.waitFailure.getFuture()));
		loop {
			choose {
				when(HaltBlobMigratorRequest req = waitNext(self->blobMigratorInterf.haltBlobMigrator.getFuture())) {
					req.reply.send(Void());
					TraceEvent("BlobMigratorHalted", self->blobMigratorInterf.id()).detail("ReqID", req.requesterID);
					break;
				}
				when(wait(self->actors.getResult())) {}
			}
		}
		return Void();
	}

private:
	Database db;
	Reference<BlobConnectionProvider> blobConn;
	BlobMigratorInterface blobMigratorInterf;
	ActorCollection actors;
};

// Main entry point
ACTOR Future<Void> blobMigrator(BlobMigratorInterface ssi, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	fmt::print("Start blob migrator {} \n", ssi.id().toString());
	try {
		Reference<BlobMigrator> self = makeReference<BlobMigrator>(dbInfo, ssi);
		wait(BlobMigrator::start(self));
	} catch (Error& e) {
		fmt::print("unexpected blob migrator error {}\n", e.what());
	}
	return Void();
}
