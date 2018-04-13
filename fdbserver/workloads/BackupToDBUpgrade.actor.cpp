/*
 * BackupToDBUpgrade.actor.cpp
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

//A workload which test the correctness of upgrading DR from 5.1 to 5.2
struct BackupToDBUpgradeWorkload : TestWorkload {
	double backupAfter, stopDifferentialAfter;
	Key backupTag, backupPrefix, extraPrefix;
	int  backupRangesCount, backupRangeLengthMax;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	Database extraDB;

	BackupToDBUpgradeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupAfter = getOption(options, LiteralStringRef("backupAfter"), g_random->random01() * 10.0);
		backupPrefix = getOption(options, LiteralStringRef("backupPrefix"), StringRef());
		backupRangeLengthMax = getOption(options, LiteralStringRef("backupRangeLengthMax"), 1);
		stopDifferentialAfter = getOption(options, LiteralStringRef("stopDifferentialAfter"), 60.0);
		backupTag = getOption(options, LiteralStringRef("backupTag"), BackupAgentBase::getDefaultTag());
		backupRangesCount = getOption(options, LiteralStringRef("backupRangesCount"), 5);
		extraPrefix = backupPrefix.withPrefix(LiteralStringRef("\xfe\xff\xfe"));
		backupPrefix = backupPrefix.withPrefix(LiteralStringRef("\xfe\xff\xff"));

		ASSERT(backupPrefix != StringRef());

		KeyRef beginRange;
		KeyRef endRange;

		if(backupRangesCount <= 0) {
			backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef(normalKeys.begin, std::min(backupPrefix, extraPrefix)));
		} else {
			// Add backup ranges
			for (int rangeLoop = 0; rangeLoop < backupRangesCount; rangeLoop++)
			{
				// Get a random range of a random sizes
				beginRange = KeyRef(backupRanges.arena(), g_random->randomAlphaNumeric(g_random->randomInt(1, backupRangeLengthMax + 1)));
				endRange = KeyRef(backupRanges.arena(), g_random->randomAlphaNumeric(g_random->randomInt(1, backupRangeLengthMax + 1)));

				// Add the range to the array
				backupRanges.push_back_deep(backupRanges.arena(), (beginRange < endRange) ? KeyRangeRef(beginRange, endRange) : KeyRangeRef(endRange, beginRange));

				// Track the added range
				TraceEvent("DRU_backup_range").detail("rangeBegin", (beginRange < endRange) ? printable(beginRange) : printable(endRange))
					.detail("rangeEnd", (beginRange < endRange) ? printable(endRange) : printable(beginRange));
			}
		}

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
		extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();

		TraceEvent("DRU_start");
	}

	virtual std::string description() {
		return "BackupToDBUpgrade";
	}

	virtual Future<Void> setup(Database const& cx) {
		if (clientId != 0)
			return Void();
		return _setup(cx, this);
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

	ACTOR static Future<Void> doBackup(BackupToDBUpgradeWorkload* self, DatabaseBackupAgent* backupAgent, Database cx, Key tag, Standalone<VectorRef<KeyRangeRef>> backupRanges) {
		try {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->extraDB));
			loop{
				try {
					for (auto r : self->backupRanges) {
						if (!r.empty()) {
							auto targetRange = r.withPrefix(self->backupPrefix);
							printf("Clearing %s in destination\n", printable(targetRange).c_str());
							tr->addReadConflictRange(targetRange);
							tr->clear(targetRange);
						}
					}
					Void _ = wait(backupAgent->submitBackup(tr, tag, backupRanges, false, self->backupPrefix, StringRef()));
					Void _ = wait(tr->commit());
					break;
				} catch (Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			TraceEvent("DRU_doBackup in differential mode").detail("tag", printable(tag));
		} catch (Error &e) {
			TraceEvent("DRU_doBackup submitBackup Exception").detail("tag", printable(tag)).error(e);
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) {
				throw e;
			}
		}

		int _ = wait( backupAgent->waitBackup(self->extraDB, tag, false) );

		return Void();
	}

	ACTOR static Future<Void> _setup(Database cx, BackupToDBUpgradeWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "BackupToDBUpgradeSetup");

		try{
			Void _ = wait(delay(self->backupAfter));

			TraceEvent("DRU_doBackup").detail("tag", printable(self->backupTag));
			state Future<Void> b = doBackup(self, &backupAgent, self->extraDB, self->backupTag, self->backupRanges);

			TraceEvent("DRU_doBackupWait").detail("backupTag", printable(self->backupTag));
			Void _ = wait(b);
			TraceEvent("DRU_doBackupWaitEnd").detail("backupTag", printable(self->backupTag));
		}
		catch (Error& e) {
			TraceEvent(SevError, "BackupToDBUpgradeSetup").error(e);
			throw;
		}

		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, BackupToDBUpgradeWorkload* self) {
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "BackupToDBUpgradeStart");
		try {
			// Wait for saveAndKill to kill before differential ends
			state Future<Void> stopDifferential = delay(self->stopDifferentialAfter);
			Void _ = wait(stopDifferential);

			// Test should be saved and killed before wait ends
			ASSERT(false);
		} catch (Error& e) {
			TraceEvent(SevError, "BackupToDBUpgradeStart").error(e);
			throw;
		}

		return Void();
	}
};

WorkloadFactory<BackupToDBUpgradeWorkload> BackupToDBUpgradeWorkloadFactory("BackupToDBUpgrade");

