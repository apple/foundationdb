/*
 * AtomicSwitchover.actor.cpp
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
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "workloads.h"
#include "BulkSetup.actor.h"

//A workload which test the correctness of backup and restore process
struct AtomicSwitchoverWorkload : TestWorkload {
	double switch1delay, switch2delay, stopDelay;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	Database extraDB;

	AtomicSwitchoverWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {

		switch1delay = getOption(options, LiteralStringRef("switch1delay"), 50.0);
		switch2delay = getOption(options, LiteralStringRef("switch2delay"), 50.0);
		stopDelay = getOption(options, LiteralStringRef("stopDelay"), 50.0);

		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
		extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();
	}

	virtual std::string description() {
		return "AtomicSwitchover";
	}

	virtual Future<Void> setup(Database const& cx) {
		if (clientId != 0)
			return Void();
		return _setup(cx, this);
	}

	ACTOR static Future<Void> _setup(Database cx, AtomicSwitchoverWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "atomicSwitchover");
		try {
			TraceEvent("AS_Submit1");
			Void _ = wait( backupAgent.submitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), self->backupRanges, false, StringRef(), StringRef(), true) );
			TraceEvent("AS_Submit2");
		} catch( Error &e ) {
			if( e.code() != error_code_backup_duplicate )
				throw;
		}
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

	ACTOR static Future<Void> diffRanges(Standalone<VectorRef<KeyRangeRef>> ranges, StringRef backupPrefix, Database src, Database dest) {
		state int rangeIndex;
		for (rangeIndex = 0; rangeIndex < ranges.size(); ++rangeIndex) {
			state KeyRangeRef range = ranges[rangeIndex];
			state Key begin = range.begin;
			loop {
				state Transaction tr(src);
				state Transaction tr2(dest);
				try {
					loop {
						state Future<Standalone<RangeResultRef>> srcFuture = tr.getRange(KeyRangeRef(begin, range.end), 1000);
						state Future<Standalone<RangeResultRef>> bkpFuture = tr2.getRange(KeyRangeRef(begin, range.end).withPrefix(backupPrefix), 1000);
						Void _ = wait(success(srcFuture) && success(bkpFuture));

						auto src = srcFuture.get().begin();
						auto bkp = bkpFuture.get().begin();

						while (src != srcFuture.get().end() && bkp != bkpFuture.get().end()) {
							KeyRef bkpKey = bkp->key.substr(backupPrefix.size());
							if (src->key != bkpKey && src->value != bkp->value) {
								TraceEvent(SevError, "MismatchKeyAndValue").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value)).detail("bkpKey", printable(bkpKey)).detail("bkpVal", printable(bkp->value));
							}
							else if (src->key != bkpKey) {
								TraceEvent(SevError, "MismatchKey").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value)).detail("bkpKey", printable(bkpKey)).detail("bkpVal", printable(bkp->value));
							}
							else if (src->value != bkp->value) {
								TraceEvent(SevError, "MismatchValue").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value)).detail("bkpKey", printable(bkpKey)).detail("bkpVal", printable(bkp->value));
							}
							begin = std::min(src->key, bkpKey);
							if (src->key == bkpKey) {
								++src;
								++bkp;
							}
							else if (src->key < bkpKey) {
								++src;
							}
							else {
								++bkp;
							}
						}
						while (src != srcFuture.get().end() && !bkpFuture.get().more) {
							TraceEvent(SevError, "MissingBkpKey").detail("srcKey", printable(src->key)).detail("srcVal", printable(src->value));
							begin = src->key;
							++src;
						}
						while (bkp != bkpFuture.get().end() && !srcFuture.get().more) {
							TraceEvent(SevError, "MissingSrcKey").detail("bkpKey", printable(bkp->key.substr(backupPrefix.size()))).detail("bkpVal", printable(bkp->value));
							begin = bkp->key;
							++bkp;
						}

						if (!srcFuture.get().more && !bkpFuture.get().more) {
							break;
						}

						begin = keyAfter(begin);
					}

					break;
				}
				catch (Error &e) {
					Void _ = wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, AtomicSwitchoverWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state DatabaseBackupAgent restoreAgent(self->extraDB);
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "atomicSwitchover");

		TraceEvent("AS_Wait1");
		int _ = wait( backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), false) );
		TraceEvent("AS_Ready1");
		Void _ = wait( delay(g_random->random01()*self->switch1delay) );
		TraceEvent("AS_Switch1");
		Void _ = wait( backupAgent.atomicSwitchover(self->extraDB, BackupAgentBase::getDefaultTag(), self->backupRanges, StringRef(), StringRef()) );
		TraceEvent("AS_Wait2");
		int _ = wait( restoreAgent.waitBackup(cx, BackupAgentBase::getDefaultTag(), false) );
		TraceEvent("AS_Ready2");
		Void _ = wait( delay(g_random->random01()*self->switch2delay) );
		TraceEvent("AS_Switch2");
		Void _ = wait( restoreAgent.atomicSwitchover(cx, BackupAgentBase::getDefaultTag(), self->backupRanges, StringRef(), StringRef()) );
		TraceEvent("AS_Wait3");
		int _ = wait( backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), false) );
		TraceEvent("AS_Ready3");
		Void _ = wait( delay(g_random->random01()*self->stopDelay) );
		TraceEvent("AS_Abort");
		Void _ = wait( backupAgent.abortBackup(self->extraDB, BackupAgentBase::getDefaultTag()) );
		TraceEvent("AS_Done");

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator.backupAgents == ISimulator::BackupToDB) {
			g_simulator.backupAgents = ISimulator::NoBackupAgents;
		}

		return Void();
	}
};

WorkloadFactory<AtomicSwitchoverWorkload> AtomicSwitchoverWorkloadFactory("AtomicSwitchover");
