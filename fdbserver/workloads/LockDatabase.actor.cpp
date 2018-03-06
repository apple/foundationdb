/*
 * LockDatabase.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "workloads.h"
#include "fdbclient/ManagementAPI.h"

struct LockDatabaseWorkload : TestWorkload {
	double lockAfter, unlockAfter;
	bool ok;

	LockDatabaseWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), ok(true)
	{
		lockAfter = getOption( options, LiteralStringRef("lockAfter"), 0.0 );
		unlockAfter = getOption( options, LiteralStringRef("unlockAfter"), 10.0 );
		ASSERT(unlockAfter > lockAfter);
	}

	virtual std::string description() { return "LockDatabase"; }

	virtual Future<Void> setup( Database const& cx ) {
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		if( clientId == 0 )
			return lockWorker( cx, this );
		return Void();
	}

	virtual Future<bool> check( Database const& cx ) {
		return ok;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	ACTOR static Future<Standalone<RangeResultRef>> lockAndSave( Database cx, LockDatabaseWorkload* self, UID lockID ) {
		state Transaction tr(cx);
		loop {
			try {
				Void _ = wait( lockDatabase(&tr, lockID) );
				state Standalone<RangeResultRef> data = wait( tr.getRange(normalKeys, 50000) );
				ASSERT(!data.more);
				Void _ = wait( tr.commit() );
				return data;
			} catch( Error &e ) {
				Void _ = wait( tr.onError(e) );
			}
		}
	}

	ACTOR static Future<Void> unlockAndCheck( Database cx, LockDatabaseWorkload* self, UID lockID, Standalone<RangeResultRef> data ) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> val = wait( tr.get(databaseLockedKey) );
				if(!val.present())
					return Void();
				
				Void _ = wait( unlockDatabase(&tr, lockID) );
				state Standalone<RangeResultRef> data2 = wait( tr.getRange(normalKeys, 50000) );
				if(data.size() != data2.size()) {
					TraceEvent(SevError, "DataChangedWhileLocked").detail("beforeSize", data.size()).detail("afterSize", data2.size());
					self->ok = false;
				} else if(data != data2) {
					TraceEvent(SevError, "DataChangedWhileLocked").detail("size", data.size());
					for(int i = 0; i < data.size(); i++) {
						if( data[i] != data2[i] ) {
							TraceEvent(SevError, "DataChangedWhileLocked").detail("i", i).detail("before", printable(data[i])).detail("after", printable(data2[i]));
						}
					}
					self->ok = false;
				}
				Void _ = wait( tr.commit() );
				return Void();
			} catch( Error &e ) {
				Void _ = wait( tr.onError(e) );
			}
		}
	}

	ACTOR static Future<Void> checkLocked( Database cx, LockDatabaseWorkload* self ) {
		state Transaction tr(cx);
		loop {
			try {
				Version v = wait( tr.getReadVersion() );
				TraceEvent(SevError, "GotVersionWhileLocked").detail("Version", v);
				self->ok = false;
				return Void();
			} catch( Error &e ) {
				Void _ = wait( tr.onError(e) );
			}
		}
	}

	ACTOR static Future<Void> lockWorker( Database cx, LockDatabaseWorkload* self ) {
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "lockDb");
		state UID lockID = g_random->randomUniqueID();
		Void _ = wait(delay(self->lockAfter));
		state Standalone<RangeResultRef> data = wait(lockAndSave(cx, self, lockID));
		state Future<Void> checker = checkLocked(cx, self);
		Void _ = wait(delay(self->unlockAfter - self->lockAfter));
		checker.cancel();
		Void _ = wait(unlockAndCheck(cx, self, lockID, data));

		return Void();
	}
};

WorkloadFactory<LockDatabaseWorkload> LockDatabaseWorkloadFactory("LockDatabase");
