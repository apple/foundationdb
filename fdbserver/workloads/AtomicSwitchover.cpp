/*
 * AtomicSwitchover.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/BackupAgent.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"

// A workload which test the correctness of backup and restore process
struct AtomicSwitchoverWorkload : TestWorkload {
	static constexpr auto NAME = "AtomicSwitchover";
	double switch1delay, switch2delay, stopDelay;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	Database extraDB;

	explicit AtomicSwitchoverWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {

		switch1delay = getOption(options, "switch1delay"_sr, 50.0);
		switch2delay = getOption(options, "switch2delay"_sr, 50.0);
		stopDelay = getOption(options, "stopDelay"_sr, 50.0);

		addDefaultBackupRanges(backupRanges);

		ASSERT(fdbSimulationPolicyState().extraDatabases.size() == 1);
		extraDB = Database::createSimulatedExtraDatabase(fdbSimulationPolicyState().extraDatabases[0]);
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _setup(cx);
	}

	Future<Void> _setup(Database cx) {
		DatabaseBackupAgent backupAgent(cx);
		try {
			TraceEvent("AS_Submit1").log();
			co_await backupAgent.submitBackup(extraDB,
			                                  BackupAgentBase::getDefaultTag(),
			                                  backupRanges,
			                                  StopWhenDone::False,
			                                  StringRef(),
			                                  StringRef(),
			                                  LockDB::True);
			TraceEvent("AS_Submit2").log();
		} catch (Error& e) {
			if (e.code() != error_code_backup_duplicate)
				throw;
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Future<Void> diffRanges(Standalone<VectorRef<KeyRangeRef>> ranges,
	                               StringRef backupPrefix,
	                               Database src,
	                               Database dest) {
		for (int rangeIndex = 0; rangeIndex < ranges.size(); ++rangeIndex) {
			KeyRangeRef range = ranges[rangeIndex];
			Key begin = range.begin;
			while (true) {
				Transaction tr(src);
				Transaction tr2(dest);
				Error err;
				try {
					while (true) {
						Future<RangeResult> srcFuture = tr.getRange(KeyRangeRef(begin, range.end), 1000);
						Future<RangeResult> bkpFuture =
						    tr2.getRange(KeyRangeRef(begin, range.end).withPrefix(backupPrefix), 1000);
						co_await (success(srcFuture) && success(bkpFuture));

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
					err = e;
				}
				co_await tr.onError(err);
			}
		}
	}

	Future<Void> _start(Database cx) {
		DatabaseBackupAgent backupAgent(cx);
		DatabaseBackupAgent restoreTool(extraDB);

		TraceEvent("AS_Wait1").log();
		co_await backupAgent.waitBackup(extraDB, BackupAgentBase::getDefaultTag(), StopWhenDone::False);
		TraceEvent("AS_Ready1").log();
		co_await delay(deterministicRandom()->random01() * switch1delay);
		TraceEvent("AS_Switch1").log();
		co_await backupAgent.atomicSwitchover(
		    extraDB, BackupAgentBase::getDefaultTag(), backupRanges, StringRef(), StringRef());
		TraceEvent("AS_Wait2").log();
		co_await restoreTool.waitBackup(cx, BackupAgentBase::getDefaultTag(), StopWhenDone::False);
		TraceEvent("AS_Ready2").log();
		co_await delay(deterministicRandom()->random01() * switch2delay);
		TraceEvent("AS_Switch2").log();
		co_await restoreTool.atomicSwitchover(
		    cx, BackupAgentBase::getDefaultTag(), backupRanges, StringRef(), StringRef());
		TraceEvent("AS_Wait3").log();
		co_await backupAgent.waitBackup(extraDB, BackupAgentBase::getDefaultTag(), StopWhenDone::False);
		TraceEvent("AS_Ready3").log();
		co_await delay(deterministicRandom()->random01() * stopDelay);
		TraceEvent("AS_Abort").log();
		co_await backupAgent.abortBackup(extraDB, BackupAgentBase::getDefaultTag());
		TraceEvent("AS_Done").log();

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (fdbSimulationPolicyState().drAgents == FDBBackupAgentType::BackupToDB) {
			fdbSimulationPolicyState().drAgents = FDBBackupAgentType::NoBackupAgents;
		}
	}
};

WorkloadFactory<AtomicSwitchoverWorkload> AtomicSwitchoverWorkloadFactory;
