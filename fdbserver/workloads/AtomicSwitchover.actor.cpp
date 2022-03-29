/*
 * AtomicSwitchover.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A workload which test the correctness of backup and restore process
struct AtomicSwitchoverWorkload : TestWorkload {
	double switch1delay, switch2delay, stopDelay;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	Database extraDB;

	AtomicSwitchoverWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {

		switch1delay = getOption(options, LiteralStringRef("switch1delay"), 50.0);
		switch2delay = getOption(options, LiteralStringRef("switch2delay"), 50.0);
		stopDelay = getOption(options, LiteralStringRef("stopDelay"), 50.0);

		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);

		auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
		extraDB = Database::createDatabase(extraFile, -1);
	}

	std::string description() const override { return "AtomicSwitchover"; }

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _setup(cx, this);
	}

	ACTOR static Future<Void> _setup(Database cx, AtomicSwitchoverWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		try {
			TraceEvent("AS_Submit1").log();
			wait(backupAgent.submitBackup(self->extraDB,
			                              BackupAgentBase::getDefaultTag(),
			                              self->backupRanges,
			                              StopWhenDone::False,
			                              StringRef(),
			                              StringRef(),
			                              LockDB::True));
			TraceEvent("AS_Submit2").log();
		} catch (Error& e) {
			if (e.code() != error_code_backup_duplicate)
				throw;
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> diffRanges(Standalone<VectorRef<KeyRangeRef>> ranges,
	                                     StringRef backupPrefix,
	                                     Database src,
	                                     Database dest) {
		state int rangeIndex;
		for (rangeIndex = 0; rangeIndex < ranges.size(); ++rangeIndex) {
			state KeyRangeRef range = ranges[rangeIndex];
			state Key begin = range.begin;
			loop {
				state Transaction tr(src);
				state Transaction tr2(dest);
				try {
					loop {
						state Future<RangeResult> srcFuture = tr.getRange(KeyRangeRef(begin, range.end), 1000);
						state Future<RangeResult> bkpFuture =
						    tr2.getRange(KeyRangeRef(begin, range.end).withPrefix(backupPrefix), 1000);
						wait(success(srcFuture) && success(bkpFuture));

						auto src = srcFuture.get().begin();
						auto bkp = bkpFuture.get().begin();

						while (src != srcFuture.get().end() && bkp != bkpFuture.get().end()) {
							KeyRef bkpKey = bkp->key.substr(backupPrefix.size());
							if (src->key != bkpKey && src->value != bkp->value) {
								TraceEvent(SevError, "MismatchKeyAndValue")
								    .detail("SrcKey", printable(src->key))
								    .detail("SrcVal", printable(src->value))
								    .detail("BkpKey", printable(bkpKey))
								    .detail("BkpVal", printable(bkp->value));
							} else if (src->key != bkpKey) {
								TraceEvent(SevError, "MismatchKey")
								    .detail("SrcKey", printable(src->key))
								    .detail("SrcVal", printable(src->value))
								    .detail("BkpKey", printable(bkpKey))
								    .detail("BkpVal", printable(bkp->value));
							} else if (src->value != bkp->value) {
								TraceEvent(SevError, "MismatchValue")
								    .detail("SrcKey", printable(src->key))
								    .detail("SrcVal", printable(src->value))
								    .detail("BkpKey", printable(bkpKey))
								    .detail("BkpVal", printable(bkp->value));
							}
							begin = std::min(src->key, bkpKey);
							if (src->key == bkpKey) {
								++src;
								++bkp;
							} else if (src->key < bkpKey) {
								++src;
							} else {
								++bkp;
							}
						}
						while (src != srcFuture.get().end() && !bkpFuture.get().more) {
							TraceEvent(SevError, "MissingBkpKey")
							    .detail("SrcKey", printable(src->key))
							    .detail("SrcVal", printable(src->value));
							begin = src->key;
							++src;
						}
						while (bkp != bkpFuture.get().end() && !srcFuture.get().more) {
							TraceEvent(SevError, "MissingSrcKey")
							    .detail("BkpKey", printable(bkp->key.substr(backupPrefix.size())))
							    .detail("BkpVal", printable(bkp->value));
							begin = bkp->key;
							++bkp;
						}

						if (!srcFuture.get().more && !bkpFuture.get().more) {
							break;
						}

						begin = keyAfter(begin);
					}

					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, AtomicSwitchoverWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state DatabaseBackupAgent restoreTool(self->extraDB);

		TraceEvent("AS_Wait1").log();
		wait(success(backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), StopWhenDone::False)));
		TraceEvent("AS_Ready1").log();
		wait(delay(deterministicRandom()->random01() * self->switch1delay));
		TraceEvent("AS_Switch1").log();
		wait(backupAgent.atomicSwitchover(
		    self->extraDB, BackupAgentBase::getDefaultTag(), self->backupRanges, StringRef(), StringRef()));
		TraceEvent("AS_Wait2").log();
		wait(success(restoreTool.waitBackup(cx, BackupAgentBase::getDefaultTag(), StopWhenDone::False)));
		TraceEvent("AS_Ready2").log();
		wait(delay(deterministicRandom()->random01() * self->switch2delay));
		TraceEvent("AS_Switch2").log();
		wait(restoreTool.atomicSwitchover(
		    cx, BackupAgentBase::getDefaultTag(), self->backupRanges, StringRef(), StringRef()));
		TraceEvent("AS_Wait3").log();
		wait(success(backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), StopWhenDone::False)));
		TraceEvent("AS_Ready3").log();
		wait(delay(deterministicRandom()->random01() * self->stopDelay));
		TraceEvent("AS_Abort").log();
		wait(backupAgent.abortBackup(self->extraDB, BackupAgentBase::getDefaultTag()));
		TraceEvent("AS_Done").log();

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator.drAgents == ISimulator::BackupAgentType::BackupToDB) {
			g_simulator.drAgents = ISimulator::BackupAgentType::NoBackupAgents;
		}

		return Void();
	}
};

WorkloadFactory<AtomicSwitchoverWorkload> AtomicSwitchoverWorkloadFactory("AtomicSwitchover");
