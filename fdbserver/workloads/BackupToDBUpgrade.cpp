/*
 * BackupToDBUpgrade.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"
#include "fdbclient/ManagementAPI.h"
#include "flow/ApiVersion.h"

// TODO: explain the purpose of this workload and how it different from the
// 20+ (literally) other backup/restore workloads.

struct BackupToDBUpgradeWorkload : TestWorkload {
	static constexpr auto NAME = "BackupToDBUpgrade";
	double backupAfter, stopDifferentialAfter;
	Key backupTag, restoreTag, backupPrefix, extraPrefix;
	int backupRangesCount, backupRangeLengthMax;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	Database extraDB;

	// This workload is not compatible with RandomRangeLock workload because they will race in locked range
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomRangeLock" });
	}

	BackupToDBUpgradeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupAfter = getOption(options, "backupAfter"_sr, deterministicRandom()->random01() * 10.0);
		backupPrefix = getOption(options, "backupPrefix"_sr, StringRef());
		backupRangeLengthMax = getOption(options, "backupRangeLengthMax"_sr, 1);
		stopDifferentialAfter = getOption(options, "stopDifferentialAfter"_sr, 60.0);
		backupTag = getOption(options, "backupTag"_sr, BackupAgentBase::getDefaultTag());
		restoreTag = getOption(options, "restoreTag"_sr, "restore"_sr);
		backupRangesCount = getOption(options, "backupRangesCount"_sr, 5);
		extraPrefix = backupPrefix.withPrefix("\xfe\xff\xfe"_sr);
		backupPrefix = backupPrefix.withPrefix("\xfe\xff\xff"_sr);

		ASSERT(!backupPrefix.empty());

		KeyRef beginRange;
		KeyRef endRange;

		if (backupRangesCount <= 0) {
			backupRanges.push_back_deep(backupRanges.arena(),
			                            KeyRangeRef(normalKeys.begin, std::min(backupPrefix, extraPrefix)));
		} else {
			// Add backup ranges
			for (int rangeLoop = 0; rangeLoop < backupRangesCount; rangeLoop++) {
				// Get a random range of a random sizes
				beginRange = KeyRef(backupRanges.arena(),
				                    deterministicRandom()->randomAlphaNumeric(
				                        deterministicRandom()->randomInt(1, backupRangeLengthMax + 1)));
				endRange = KeyRef(backupRanges.arena(),
				                  deterministicRandom()->randomAlphaNumeric(
				                      deterministicRandom()->randomInt(1, backupRangeLengthMax + 1)));

				// Add the range to the array
				backupRanges.push_back_deep(backupRanges.arena(),
				                            (beginRange < endRange) ? KeyRangeRef(beginRange, endRange)
				                                                    : KeyRangeRef(endRange, beginRange));

				// Track the added range
				TraceEvent("DRU_BackupRange")
				    .detail("RangeBegin", (beginRange < endRange) ? printable(beginRange) : printable(endRange))
				    .detail("RangeEnd", (beginRange < endRange) ? printable(endRange) : printable(beginRange));
			}
		}

		ASSERT(g_simulator->extraDatabases.size() == 1);
		extraDB = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0]);

		TraceEvent("DRU_Start").log();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _setup(cx);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> doBackup(DatabaseBackupAgent* backupAgent,
	                      Database cx,
	                      Key tag,
	                      Standalone<VectorRef<KeyRangeRef>> backupRanges) {
		try {
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(extraDB));
			while (true) {
				Error err;
				try {
					for (auto r : backupRanges) {
						if (!r.empty()) {
							auto targetRange = r.withPrefix(backupPrefix);
							printf("Clearing %s in destination\n", printable(targetRange).c_str());
							tr->addReadConflictRange(targetRange);
							tr->clear(targetRange);
						}
					}
					co_await backupAgent->submitBackup(
					    tr, tag, backupRanges, StopWhenDone::False, backupPrefix, StringRef());
					co_await tr->commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}

			TraceEvent("DRU_DoBackupInDifferentialMode").detail("Tag", printable(tag));
		} catch (Error& e) {
			TraceEvent("DRU_DoBackupSubmitBackupError").error(e).detail("Tag", printable(tag));
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate) {
				throw e;
			}
		}

		co_await backupAgent->waitBackup(extraDB, tag, StopWhenDone::False);
	}

	static Future<Void> checkData(Database cx, UID logUid, UID destUid, Key tag, DatabaseBackupAgent* backupAgent) {
		Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
		Key backupLogValuesKey = uidPrefixKey(backupLogKeys.begin, destUid);
		Key backupLatestVersionsPath = uidPrefixKey(backupLatestVersionsPrefix, destUid);
		Key backupLatestVersionsKey = uidPrefixKey(backupLatestVersionsPath, logUid);
		int displaySystemKeys = 0;

		ASSERT(destUid.isValid());

		// Ensure that there is no left over key within the backup subspace
		while (true) {
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			TraceEvent("DRU_CheckLeftoverkeys").detail("BackupTag", printable(tag));

			Error err;
			try {
				// Check the left over tasks
				// We have to wait for the list to empty since an abort and get status
				// can leave extra tasks in the queue
				TraceEvent("DRU_CheckLeftovertasks").detail("BackupTag", printable(tag));
				int64_t taskCount = co_await backupAgent->getTaskCount(tr);
				int waitCycles = 0;

				if ((taskCount) && false) {
					TraceEvent("DRU_EndingNonzeroTaskCount")
					    .detail("BackupTag", printable(tag))
					    .detail("TaskCount", taskCount)
					    .detail("WaitCycles", waitCycles);
					printf("EndingNonZeroTasks: %ld\n", (long)taskCount);
					co_await TaskBucket::debugPrintRange(cx, "\xff"_sr, StringRef());
				}

				while (taskCount > 0) {
					waitCycles++;

					TraceEvent("DRU_NonzeroTaskWait")
					    .detail("BackupTag", printable(tag))
					    .detail("TaskCount", taskCount)
					    .detail("WaitCycles", waitCycles);
					printf("%.6f Wait #%4d for %lld tasks to end\n", now(), waitCycles, (long long)taskCount);

					co_await delay(20.0);
					tr = makeReference<ReadYourWritesTransaction>(cx);
					taskCount = co_await backupAgent->getTaskCount(tr);
				}

				RangeResult agentValues =
				    co_await tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100);

				// Error if the system keyspace for the backup tag is not empty
				if (!agentValues.empty()) {
					displaySystemKeys++;
					printf("BackupCorrectnessLeftoverMutationKeys: (%d) %s\n",
					       agentValues.size(),
					       printable(backupAgentKey).c_str());
					TraceEvent(SevError, "BackupCorrectnessLeftoverMutationKeys")
					    .detail("BackupTag", printable(tag))
					    .detail("LeftoverKeys", agentValues.size())
					    .detail("KeySpace", printable(backupAgentKey));
					for (auto& s : agentValues) {
						TraceEvent("DRU_LeftoverKey")
						    .detail("Key", printable(StringRef(s.key.toString())))
						    .detail("Value", printable(StringRef(s.value.toString())));
						printf("   Key: %-50s  Value: %s\n",
						       printable(StringRef(s.key.toString())).c_str(),
						       printable(StringRef(s.value.toString())).c_str());
					}
				} else {
					printf("No left over backup agent configuration keys\n");
				}

				Optional<Value> latestVersion = co_await tr->get(backupLatestVersionsKey);
				if (latestVersion.present()) {
					TraceEvent(SevError, "BackupCorrectnessLeftoverVersionKey")
					    .detail("BackupTag", printable(tag))
					    .detail("Key", backupLatestVersionsKey.printable())
					    .detail("Value", BinaryReader::fromStringRef<Version>(latestVersion.get(), Unversioned()));
				} else {
					printf("No left over backup version key\n");
				}

				RangeResult versions = co_await tr->getRange(
				    KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1);
				if (versions.empty()) {
					RangeResult logValues = co_await tr->getRange(
					    KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100);

					// Error if the log/mutation keyspace for the backup tag is not empty
					if (!logValues.empty()) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftoverLogKeys: (%d) %s\n",
						       logValues.size(),
						       printable(backupLogValuesKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftoverLogKeys")
						    .detail("BackupTag", printable(tag))
						    .detail("LeftoverKeys", logValues.size())
						    .detail("KeySpace", printable(backupLogValuesKey))
						    .detail("Version", decodeBKMutationLogKey(logValues[0].key).first);
						for (auto& s : logValues) {
							TraceEvent("DRU_LeftoverKey")
							    .detail("Key", printable(StringRef(s.key.toString())))
							    .detail("Value", printable(StringRef(s.value.toString())));
							printf("   Key: %-50s  Value: %s\n",
							       printable(StringRef(s.key.toString())).c_str(),
							       printable(StringRef(s.value.toString())).c_str());
						}
					} else {
						printf("No left over backup log keys\n");
					}
				}

				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("DRU_CheckError").error(err);
			co_await tr->onError(err);
		}

		if (displaySystemKeys) {
			co_await TaskBucket::debugPrintRange(cx, "\xff"_sr, StringRef());
		}
	}

	Future<Void> _setup(Database cx) {
		DatabaseBackupAgent backupAgent(cx);

		if (BUGGIFY) {
			for (auto r : getSystemBackupRanges()) {
				backupRanges.push_back_deep(backupRanges.arena(), r);
			}
		}

		try {
			co_await delay(backupAfter);

			TraceEvent("DRU_DoBackup").detail("Tag", printable(backupTag));
			Future<Void> b = doBackup(&backupAgent, extraDB, backupTag, backupRanges);

			TraceEvent("DRU_DoBackupWait").detail("BackupTag", printable(backupTag));
			co_await b;
			TraceEvent("DRU_DoBackupWaitEnd").detail("BackupTag", printable(backupTag));
		} catch (Error& e) {
			TraceEvent(SevError, "BackupToDBUpgradeSetupError").error(e);
			throw;
		}
	}

	static Future<Void> diffRanges(Standalone<VectorRef<KeyRangeRef>> ranges,
	                               StringRef backupPrefix,
	                               Database src,
	                               Database dest) {
		for (int rangeIndex = 0; rangeIndex < ranges.size(); ++rangeIndex) {
			KeyRangeRef range = ranges[rangeIndex];
			Key begin = range.begin;
			if (range.empty()) {
				continue;
			}
			while (true) {
				Transaction tr(src);
				Transaction tr2(dest);
				Error err;
				try {
					while (true) {
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);
						tr.setOption(FDBTransactionOptions::RAW_ACCESS);
						tr2.setOption(FDBTransactionOptions::LOCK_AWARE);
						tr2.setOption(FDBTransactionOptions::RAW_ACCESS);
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
		Standalone<VectorRef<KeyRangeRef>> prevBackupRanges;
		UID logUid;
		Version commitVersion{ 0 };

		Future<Void> stopDifferential = delay(stopDifferentialAfter);
		Future<Void> waitUpgrade = backupAgent.waitUpgradeToLatestDrVersion(extraDB, backupTag);
		co_await (success(stopDifferential) && success(waitUpgrade));
		TraceEvent("DRU_WaitDifferentialEnd").detail("Tag", printable(backupTag));

		try {
			// Get restore ranges before aborting
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(extraDB));
			while (true) {
				Error err;
				try {
					// Get backup ranges
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					UID _logUid = co_await backupAgent.getLogUid(tr, backupTag);
					logUid = _logUid;

					Optional<Key> backupKeysPacked =
					    co_await tr->get(backupAgent.config.get(BinaryWriter::toValue(logUid, Unversioned()))
					                         .pack(BackupAgentBase::keyConfigBackupRanges));
					ASSERT(backupKeysPacked.present());

					BinaryReader br(backupKeysPacked.get(), IncludeVersion());
					prevBackupRanges = Standalone<VectorRef<KeyRangeRef>>();
					br >> prevBackupRanges;
					co_await lockDatabase(tr, logUid);
					tr->addWriteConflictRange(singleKeyRange(StringRef()));
					co_await tr->commit();
					commitVersion = tr->getCommittedVersion();
					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("DRU_GetRestoreRangeError").error(err);
				co_await tr->onError(err);
			}

			TraceEvent("DRU_Locked").detail("LockedVersion", commitVersion);

			// Wait for the destination to apply mutations up to the lock commit before switching over.
			ReadYourWritesTransaction versionCheckTr(extraDB);
			while (true) {
				Error err;
				try {
					versionCheckTr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					versionCheckTr.setOption(FDBTransactionOptions::LOCK_AWARE);
					Optional<Value> v = co_await versionCheckTr.get(
					    BinaryWriter::toValue(logUid, Unversioned()).withPrefix(applyMutationsBeginRange.begin));
					TraceEvent("DRU_Applied")
					    .detail("AppliedVersion",
					            v.present() ? BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) : -1);
					if (v.present() && BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) >= commitVersion)
						break;

					Future<Void> versionWatch = versionCheckTr.watch(
					    BinaryWriter::toValue(logUid, Unversioned()).withPrefix(applyMutationsBeginRange.begin));
					co_await versionCheckTr.commit();
					co_await versionWatch;
					versionCheckTr.reset();
				} catch (Error& e) {
					err = e;
				}
				if (!err.isValid()) {
					continue;
				}
				TraceEvent("DRU_GetAppliedVersionError").error(err);
				co_await versionCheckTr.onError(err);
			}

			TraceEvent("DRU_DiffRanges").log();
			co_await diffRanges(prevBackupRanges, backupPrefix, cx, extraDB);

			// abort backup
			TraceEvent("DRU_AbortBackup").detail("Tag", printable(backupTag));
			co_await backupAgent.abortBackup(extraDB, backupTag);
			co_await unlockDatabase(extraDB, logUid);

			// restore database
			TraceEvent("DRU_PrepareRestore").detail("RestoreTag", printable(restoreTag));
			Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(cx));
			while (true) {
				Error err;
				try {
					tr2->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr2->setOption(FDBTransactionOptions::LOCK_AWARE);
					for (auto r : prevBackupRanges) {
						if (!r.empty()) {
							std::cout << "r: " << r.begin.printable() << " - " << r.end.printable() << std::endl;
							tr2->addReadConflictRange(r);
							tr2->clear(r);
						}
					}
					co_await tr2->commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("DRU_RestoreSetupError").errorUnsuppressed(err);
				co_await tr2->onError(err);
			}

			Standalone<VectorRef<KeyRangeRef>> restoreRanges;
			for (auto r : prevBackupRanges) {
				restoreRanges.push_back_deep(
				    restoreRanges.arena(),
				    KeyRangeRef(r.begin.withPrefix(backupPrefix), r.end.withPrefix(backupPrefix)));
			}

			// start restoring db
			try {
				TraceEvent("DRU_RestoreDb").detail("RestoreTag", printable(restoreTag));
				co_await restoreTool.submitBackup(
				    cx, restoreTag, restoreRanges, StopWhenDone::True, StringRef(), backupPrefix);
			} catch (Error& e) {
				TraceEvent("DRU_RestoreSubmitBackupError").error(e).detail("Tag", printable(restoreTag));
				if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
					throw;
			}

			co_await restoreTool.waitBackup(cx, restoreTag);
			co_await restoreTool.unlockBackup(cx, restoreTag);
			co_await checkData(extraDB, logUid, logUid, backupTag, &backupAgent);

			UID restoreUid = co_await restoreTool.getLogUid(cx, restoreTag);
			co_await checkData(cx, restoreUid, restoreUid, restoreTag, &restoreTool);

			TraceEvent("DRU_Complete").detail("BackupTag", printable(backupTag));

			if (fdbSimulationPolicyState().drAgents == FDBBackupAgentType::BackupToDB) {
				fdbSimulationPolicyState().drAgents = FDBBackupAgentType::NoBackupAgents;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestoreCorrectnessError").error(e);
			throw;
		}
	}
};

WorkloadFactory<BackupToDBUpgradeWorkload> BackupToDBUpgradeWorkloadFactory;
