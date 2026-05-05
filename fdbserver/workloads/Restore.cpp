/*
 * Restore.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"
#include "flow/IRandom.h"

// TODO: explain the purpose of this workload and how it different from the
// 20+ (literally) other backup/restore workloads.

struct RestoreWorkload : TestWorkload {
	static constexpr auto NAME = "Restore";
	Key backupTag, backupTag1, backupTag2;
	bool performRestore, agentRequest;
	Standalone<VectorRef<KeyRangeRef>> backupRanges, restoreRanges;
	static int backupAgentRequests;
	LockDB locked{ false };
	bool allowPauses;
	bool shareLogRange;
	bool shouldSkipRestoreRanges;
	UID randomID;

	RestoreWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		locked.set(sharedRandomNumber % 2);
		performRestore = getOption(options, "performRestore"_sr, true);
		backupTag1 = getOption(options, "backupTag1"_sr, BackupAgentBase::getDefaultTag());
		backupTag2 = getOption(options, "backupTag2"_sr, BackupAgentBase::getDefaultTag());
		backupTag = deterministicRandom()->coinflip() ? backupTag1 : backupTag2;
		agentRequest = getOption(options, "simBackupAgents"_sr, true);
		allowPauses = getOption(options, "allowPauses"_sr, true);
		shareLogRange = getOption(options, "shareLogRange"_sr, false);

		std::vector<std::string> restorePrefixesToInclude =
		    getOption(options, "restorePrefixesToInclude"_sr, std::vector<std::string>());

		shouldSkipRestoreRanges = deterministicRandom()->random01() < 0.3 ? true : false;
		randomID = nondeterministicRandom()->randomUniqueID();
		TraceEvent("RW_ClientId").detail("Id", wcx.clientId);
		TraceEvent("RW_PerformRestore", randomID).detail("Value", performRestore);

		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		restoreRanges = backupRanges; // may be modified later

		for (auto& range : restoreRanges) {
			TraceEvent("RW_RestoreRange", randomID)
			    .detail("RangeBegin", printable(range.begin))
			    .detail("RangeEnd", printable(range.end));
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();

		TraceEvent(SevInfo, "RW_Param")
		    .detail("Locked", locked)
		    .detail("PerformRestore", performRestore)
		    .detail("BackupTag", printable(backupTag).c_str())
		    .detail("AgentRequest", agentRequest);

		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		while (true) {
			co_await backupAgent->changePause(cx, true);
			TraceEvent("RW_AgentPaused").log();
			co_await delay(30 * deterministicRandom()->random01());
			co_await backupAgent->changePause(cx, false);
			TraceEvent("RW_AgentResumed").log();
			co_await delay(120 * deterministicRandom()->random01());
		}
	}

	// Resume the backup agent if it is paused
	static Future<Void> resumeAgent(Database cx, FileBackupAgent* backupAgent) {
		TraceEvent("RW_AgentResuming").log();
		co_await backupAgent->changePause(cx, false);
		TraceEvent("RW_AgentResumed").log();
	}

	static Future<Void> statusLoop(Database cx, std::string tag) {
		FileBackupAgent agent;
		while (true) {
			bool active = co_await agent.checkActive(cx);
			TraceEvent("RW_AgentActivityCheck").detail("IsActive", active);
			std::string status = co_await agent.getStatus(cx, ShowErrors::True, tag);
			puts(status.c_str());
			std::string statusJSON = co_await agent.getStatusJSON(cx, tag);
			puts(statusJSON.c_str());
			co_await delay(10.0);
		}
	}

	Future<Void> _start(Database cx) {
		FileBackupAgent backupAgent;
		Future<Void> cp;
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);
		TraceEvent("RW_Arguments")
		    .detail("BackupTag", printable(backupTag))
		    .detail("PerformRestore", performRestore)
		    .detail("AllowPauses", allowPauses);

		if (allowPauses && BUGGIFY) {
			cp = changePaused(cx, &backupAgent);
		} else {
			cp = resumeAgent(cx, &backupAgent);
		}
		Future<Void> status = statusLoop(cx, backupTag.toString());

		// Increment the backup agent requests
		if (agentRequest) {
			RestoreWorkload::backupAgentRequests++;
		}

		try {
			KeyBackedTag keyBackedTag = makeBackupTag(backupTag.toString());
			UidAndAbortedFlagT uidFlag = co_await keyBackedTag.getOrThrow(cx.getReference());
			UID logUid = uidFlag.first;
			Key destUidValue = co_await BackupConfig(logUid).destUidValue().getD(cx.getReference());
			Reference<IBackupContainer> lastBackupContainer =
			    co_await BackupConfig(logUid).backupContainer().getD(cx.getReference());

			if (lastBackupContainer && performRestore) {
				auto container = IBackupContainer::openContainer(lastBackupContainer->getURL(),
				                                                 lastBackupContainer->getProxy(),
				                                                 lastBackupContainer->getEncryptionKeyFileName(),
				                                                 lastBackupContainer->getEncryptionBlockSize());
				BackupDescription desc = co_await container->describeBackup();
				TraceEvent("RW_Restore", randomID)
				    .setMaxEventLength(12000)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("BackupTag", printable(backupTag))
				    .setMaxFieldLength(10000)
				    .detail("Description", desc.toString());
				Version targetVersion = -1;
				if (desc.maxRestorableVersion.present()) {
					if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.minRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.1) {
						targetVersion = desc.maxRestorableVersion.get();
					} else if (deterministicRandom()->random01() < 0.5) {
						targetVersion = (desc.minRestorableVersion.get() != desc.maxRestorableVersion.get())
						                    ? deterministicRandom()->randomInt64(desc.minRestorableVersion.get(),
						                                                         desc.maxRestorableVersion.get())
						                    : desc.maxRestorableVersion.get();
					}
				}
				co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					for (auto& kvrange : backupRanges) {
						// version needs to be decided before this transaction otherwise
						// this clear mutation might be backup as well
						tr->clear(kvrange);
					}
					return Void();
				});

				TraceEvent("RW_Restore", randomID)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("BackupTag", printable(backupTag))
				    .detail("TargetVersion", targetVersion);
				int restoreIndex = 0;
				// make sure system keys are not present in the restoreRanges as they will get restored first separately
				// from the rest
				Standalone<VectorRef<KeyRangeRef>> modifiedRestoreRanges;
				for (int i = 0; i < restoreRanges.size(); ++i) {
					if (!restoreRanges[i].intersects(getSystemBackupRanges())) {
						modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), restoreRanges[i]);
					} else {
						KeyRangeRef normalKeyRange = restoreRanges[i] & normalKeys;
						if (!normalKeyRange.empty()) {
							modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), normalKeyRange);
						}
					}
				}
				restoreRanges = modifiedRestoreRanges;

				Standalone<StringRef> restoreTag(backupTag.toString() + "_" + std::to_string(restoreIndex));
				printf("BackupCorrectness, backupAgent.restore is called for restoreIndex:%d tag:%s\n",
				       restoreIndex,
				       restoreTag.toString().c_str());
				TraceEvent("RW_RestoreRanges", randomID)
				    .detail("RestoreIndex", restoreIndex)
				    .detail("RestoreTag", printable(restoreTag))
				    .detail("RestoreRanges", restoreRanges.size());
				Future<Version> restore;
				restore = backupAgent.restore(cx,
				                              cx,
				                              restoreTag,
				                              KeyRef(lastBackupContainer->getURL()),
				                              lastBackupContainer->getProxy(),
				                              restoreRanges,
				                              WaitForComplete::True,
				                              targetVersion,
				                              Verbose::True,
				                              Key(),
				                              Key(),
				                              locked,
				                              UnlockDB::True,
				                              OnlyApplyMutationLogs::False,
				                              InconsistentSnapshotOnly::False,
				                              ::invalidVersion,
				                              lastBackupContainer->getEncryptionKeyFileName());

				co_await restore;
				ASSERT(!restore.isError());
			}
			Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
			Key backupLogValuesKey = destUidValue.withPrefix(backupLogKeys.begin);
			Key backupLatestVersionsPath = destUidValue.withPrefix(backupLatestVersionsPrefix);
			Key backupLatestVersionsKey = uidPrefixKey(backupLatestVersionsPath, logUid);
			int displaySystemKeys = 0;

			// Ensure that there is no left over key within the backup subspace
			while (true) {
				Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

				TraceEvent("RW_CheckLeftoverKeys", randomID).detail("BackupTag", printable(backupTag));

				Error err;
				try {
					// Check the left over tasks
					// We have to wait for the list to empty since an abort and get status
					// can leave extra tasks in the queue
					TraceEvent("RW_CheckLeftoverTasks", randomID).detail("BackupTag", printable(backupTag));
					int64_t taskCount = co_await backupAgent.getTaskCount(tr);
					int waitCycles = 0;

					while (taskCount > 0) {
						waitCycles++;

						TraceEvent("RW_NonzeroTaskWait", randomID)
						    .detail("BackupTag", printable(backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("%.6f %-10s Wait #%4d for %lld tasks to end\n",
						       now(),
						       randomID.toString().c_str(),
						       waitCycles,
						       (long long)taskCount);

						co_await delay(5.0);

						tr = makeReference<ReadYourWritesTransaction>(cx);
						taskCount = co_await backupAgent.getTaskCount(tr);
					}

					RangeResult agentValues =
					    co_await tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100);

					// Error if the system keyspace for the backup tag is not empty
					if (!agentValues.empty()) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftOverMutationKeys: (%d) %s\n",
						       agentValues.size(),
						       printable(backupAgentKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftOverMutationKeys", randomID)
						    .detail("BackupTag", printable(backupTag))
						    .detail("LeftOverKeys", agentValues.size())
						    .detail("KeySpace", printable(backupAgentKey));
						for (auto& s : agentValues) {
							TraceEvent("RW_LeftOverKey", randomID)
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
						TraceEvent(SevError, "BackupCorrectnessLeftOverVersionKey", randomID)
						    .detail("BackupTag", printable(backupTag))
						    .detail("BackupLatestVersionsKey", backupLatestVersionsKey.printable())
						    .detail("DestUidValue", destUidValue.printable());
					} else {
						printf("No left over backup version key\n");
					}

					RangeResult versions = co_await tr->getRange(
					    KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1);
					if (!shareLogRange || versions.empty()) {
						RangeResult logValues = co_await tr->getRange(
						    KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100);

						// Error if the log/mutation keyspace for the backup tag  is not empty
						if (!logValues.empty()) {
							displaySystemKeys++;
							printf("BackupCorrectnessLeftOverLogKeys: (%d) %s\n",
							       logValues.size(),
							       printable(backupLogValuesKey).c_str());
							TraceEvent(SevError, "BackupCorrectnessLeftOverLogKeys", randomID)
							    .detail("BackupTag", printable(backupTag))
							    .detail("LeftOverKeys", logValues.size())
							    .detail("KeySpace", printable(backupLogValuesKey));
						} else {
							printf("No left over backup log keys\n");
						}
					}

					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("RW_CheckException", randomID).error(err);
				co_await tr->onError(err);
			}

			if (displaySystemKeys) {
				co_await TaskBucket::debugPrintRange(cx, normalKeys.end, StringRef());
			}

			TraceEvent("RW_Complete", randomID).detail("BackupTag", printable(backupTag));

			// Decrement the backup agent requets
			if (agentRequest) {
				RestoreWorkload::backupAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((fdbSimulationPolicyState().backupAgents == FDBBackupAgentType::BackupToFile) &&
			    (!RestoreWorkload::backupAgentRequests)) {
				fdbSimulationPolicyState().backupAgents = FDBBackupAgentType::NoBackupAgents;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestorePartitionedCorrectness").error(e).GetLastError();
			throw;
		}
	}
};

int RestoreWorkload::backupAgentRequests = 0;

WorkloadFactory<RestoreWorkload> RestoreWorkloadFactory;
