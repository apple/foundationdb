/*
 * StorageCache.actor.cpp
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

#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct StorageCacheData {
	UID dbgid;
	Reference<AsyncVar<Reference<ILogSystem>>> logSystem;
	
	explicit StorageCacheData(UID dbgid) : dbgid(dbgid), logSystem(new AsyncVar<Reference<ILogSystem>>()) {}
};

ACTOR Future<Void> pullAsyncData( StorageCacheData *self ) {
	state Future<Void> dbInfoChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = 0;

	loop {
		loop {
			choose {
				when(wait( r ? r->getMore(TaskPriority::TLogCommit) : Never() ) ) {
					break;
				}
				when( wait( dbInfoChange ) ) {
					if( self->logSystem->get() )
						r = self->logSystem->get()->peek( self->dbgid, tagAt, cacheTag );
					else
						r = Reference<ILogSystem::IPeekCursor>();
					dbInfoChange = self->logSystem->onChange();
				}
			}
		}
		//FIXME: if the popped version is greater than our last version, we need to clear the cache

		//FIXME: ensure this can only read data from the current version
		r->setProtocolVersion(currentProtocolVersion);

		for (; r->hasMessage(); r->nextMessage()) {
			ArenaReader& reader = *r->reader();

			MutationRef msg;
			reader >> msg;

			//fprintf(stderr, "%lld : %s\n", r->version().version, msg.toString().c_str());

			tagAt = r->version().version + 1;
		}

		tagAt = std::max( tagAt, r->version().version);
	}
}

ACTOR Future<Void> storageCache(StorageServerInterface ssi, uint16_t id, Reference<AsyncVar<ServerDBInfo>> db) {
	state StorageCacheData self(ssi.id());
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection( addActor.getFuture() );
	state Future<Void> dbInfoChange = Void();

	addActor.send(pullAsyncData(&self));

	try {
		loop choose {
			when( wait( dbInfoChange ) ) {
				dbInfoChange = db->onChange();
				self.logSystem->set(ILogSystem::fromServerDBInfo( ssi.id(), db->get(), true ));
			}
			when( GetValueRequest req = waitNext(ssi.getValue.getFuture()) ) {
				ASSERT(false);
			}
			when( WatchValueRequest req = waitNext(ssi.watchValue.getFuture()) ) {
				ASSERT(false);
			}
			when (GetKeyRequest req = waitNext(ssi.getKey.getFuture())) {
				ASSERT(false);
			}
			when (GetKeyValuesRequest req = waitNext(ssi.getKeyValues.getFuture()) ) {
				ASSERT(false);
			}
			when (GetShardStateRequest req = waitNext(ssi.getShardState.getFuture()) ) {
				ASSERT(false);
			}
			when (StorageQueuingMetricsRequest req = waitNext(ssi.getQueuingMetrics.getFuture())) {
				ASSERT(false);
			}
			when( ReplyPromise<Version> reply = waitNext(ssi.getVersion.getFuture()) ) {
				ASSERT(false);
			}
			when( ReplyPromise<KeyValueStoreType> reply = waitNext(ssi.getKeyValueStoreType.getFuture()) ) {
				ASSERT(false);
			}
			when (wait(error)) {}
		}
	}
	catch (Error& err) {
		TraceEvent("StorageCacheTerminated", ssi.id()).error(err, true);
	}
	return Void();
}
