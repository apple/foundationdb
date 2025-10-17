/*
 * BackupCorrectness.actor.cpp
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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A workload which test the correctness of backup and restore process
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

		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> changePaused(Database cx, FileBackupAgent* backupAgent) {
		loop {
			wait(backupAgent->changePause(cx, true));
			TraceEvent("RW_AgentPaused").log();
			wait(delay(30 * deterministicRandom()->random01()));
			wait(backupAgent->changePause(cx, false));
			TraceEvent("RW_AgentResumed").log();
			wait(delay(120 * deterministicRandom()->random01()));
		}
	}

	// Resume the backup agent if it is paused
	ACTOR static Future<Void> resumeAgent(Database cx, FileBackupAgent* backupAgent) {
		TraceEvent("RW_AgentResuming").log();
		wait(backupAgent->changePause(cx, false));
		TraceEvent("RW_AgentResumed").log();
		return Void();
	}

	ACTOR static Future<Void> statusLoop(Database cx, std::string tag) {
		state FileBackupAgent agent;
		loop {
			bool active = wait(agent.checkActive(cx));
			TraceEvent("RW_AgentActivityCheck").detail("IsActive", active);
			std::string status = wait(agent.getStatus(cx, ShowErrors::True, tag));
			puts(status.c_str());
			std::string statusJSON = wait(agent.getStatusJSON(cx, tag));
			puts(statusJSON.c_str());
			wait(delay(10.0));
		}
	}

	ACTOR static Future<Void> _start(Database cx, RestoreWorkload* self) {
		state FileBackupAgent backupAgent;
		state Future<Void> cp;
		state DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		TraceEvent("RW_Arguments")
		    .detail("BackupTag", printable(self->backupTag))
		    .detail("PerformRestore", self->performRestore)
		    .detail("AllowPauses", self->allowPauses);

		if (self->allowPauses && BUGGIFY) {
			cp = changePaused(cx, &backupAgent);
		} else {
			cp = resumeAgent(cx, &backupAgent);
		}
		state Future<Void> status = statusLoop(cx, self->backupTag.toString());

		// Increment the backup agent requests
		if (self->agentRequest) {
			RestoreWorkload::backupAgentRequests++;
		}

		try {
			state KeyBackedTag keyBackedTag = makeBackupTag(self->backupTag.toString());
			UidAndAbortedFlagT uidFlag = wait(keyBackedTag.getOrThrow(cx.getReference()));
			state UID logUid = uidFlag.first;
			state Key destUidValue = wait(BackupConfig(logUid).destUidValue().getD(cx.getReference()));
			state Reference<IBackupContainer> lastBackupContainer =
			    wait(BackupConfig(logUid).backupContainer().getD(cx.getReference()));

			if (lastBackupContainer && self->performRestore) {
				auto container = IBackupContainer::openContainer(lastBackupContainer->getURL(),
				                                                 lastBackupContainer->getProxy(),
				                                                 lastBackupContainer->getEncryptionKeyFileName());
				BackupDescription desc = wait(container->describeBackup());
				TraceEvent("RW_Restore", self->randomID)
				    .setMaxEventLength(12000)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("BackupTag", printable(self->backupTag))
				    .setMaxFieldLength(10000)
				    .detail("Description", desc.toString());
				state Version targetVersion = -1;
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
				wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					for (auto& kvrange : self->backupRanges) {
						// version needs to be decided before this transaction otherwise
						// this clear mutation might be backup as well
						tr->clear(kvrange);
					}
					return Void();
				}));

				TraceEvent("RW_Restore", self->randomID)
				    .detail("LastBackupContainer", lastBackupContainer->getURL())
				    .detail("BackupTag", printable(self->backupTag))
				    .detail("TargetVersion", targetVersion);
				state int restoreIndex = 0;
				// make sure system keys are not present in the restoreRanges as they will get restored first separately
				// from the rest
				Standalone<VectorRef<KeyRangeRef>> modifiedRestoreRanges;
				for (int i = 0; i < self->restoreRanges.size(); ++i) {
					if (config.tenantMode != TenantMode::REQUIRED ||
					    !self->restoreRanges[i].intersects(getSystemBackupRanges())) {
						modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), self->restoreRanges[i]);
					} else {
						KeyRangeRef normalKeyRange = self->restoreRanges[i] & normalKeys;
						if (!normalKeyRange.empty()) {
							modifiedRestoreRanges.push_back_deep(modifiedRestoreRanges.arena(), normalKeyRange);
						}
					}
				}
				self->restoreRanges = modifiedRestoreRanges;

				Standalone<StringRef> restoreTag(self->backupTag.toString() + "_" + std::to_string(restoreIndex));
				printf("BackupCorrectness, backupAgent.restore is called for restoreIndex:%d tag:%s\n",
				       restoreIndex,
				       restoreTag.toString().c_str());
				TraceEvent("RW_RestoreRanges", self->randomID)
				    .detail("RestoreIndex", restoreIndex)
				    .detail("RestoreTag", printable(restoreTag))
				    .detail("RestoreRanges", self->restoreRanges.size());
				state Future<Version> restore;
				restore = backupAgent.restore(cx,
				                              cx,
				                              restoreTag,
				                              KeyRef(lastBackupContainer->getURL()),
				                              lastBackupContainer->getProxy(),
				                              self->restoreRanges,
				                              WaitForComplete::True,
				                              targetVersion,
				                              Verbose::True,
				                              Key(),
				                              Key(),
				                              self->locked,
				                              UnlockDB::True,
				                              OnlyApplyMutationLogs::False,
				                              InconsistentSnapshotOnly::False,
				                              ::invalidVersion,
				                              lastBackupContainer->getEncryptionKeyFileName());

				wait(success(restore));
				ASSERT(!restore.isError());
			}
			state Key backupAgentKey = uidPrefixKey(logRangesRange.begin, logUid);
			state Key backupLogValuesKey = destUidValue.withPrefix(backupLogKeys.begin);
			state Key backupLatestVersionsPath = destUidValue.withPrefix(backupLatestVersionsPrefix);
			state Key backupLatestVersionsKey = uidPrefixKey(backupLatestVersionsPath, logUid);
			state int displaySystemKeys = 0;

			// Ensure that there is no left over key within the backup subspace
			loop {
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

				TraceEvent("RW_CheckLeftoverKeys", self->randomID).detail("BackupTag", printable(self->backupTag));

				try {
					// Check the left over tasks
					// We have to wait for the list to empty since an abort and get status
					// can leave extra tasks in the queue
					TraceEvent("RW_CheckLeftoverTasks", self->randomID).detail("BackupTag", printable(self->backupTag));
					state int64_t taskCount = wait(backupAgent.getTaskCount(tr));
					state int waitCycles = 0;

					while (taskCount > 0) {
						waitCycles++;

						TraceEvent("RW_NonzeroTaskWait", self->randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("TaskCount", taskCount)
						    .detail("WaitCycles", waitCycles);
						printf("%.6f %-10s Wait #%4d for %lld tasks to end\n",
						       now(),
						       self->randomID.toString().c_str(),
						       waitCycles,
						       (long long)taskCount);

						wait(delay(5.0));

						tr = makeReference<ReadYourWritesTransaction>(cx);
						wait(store(taskCount, backupAgent.getTaskCount(tr)));
					}

					RangeResult agentValues =
					    wait(tr->getRange(KeyRange(KeyRangeRef(backupAgentKey, strinc(backupAgentKey))), 100));

					// Error if the system keyspace for the backup tag is not empty
					if (agentValues.size() > 0) {
						displaySystemKeys++;
						printf("BackupCorrectnessLeftOverMutationKeys: (%d) %s\n",
						       agentValues.size(),
						       printable(backupAgentKey).c_str());
						TraceEvent(SevError, "BackupCorrectnessLeftOverMutationKeys", self->randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("LeftOverKeys", agentValues.size())
						    .detail("KeySpace", printable(backupAgentKey));
						for (auto& s : agentValues) {
							TraceEvent("RW_LeftOverKey", self->randomID)
							    .detail("Key", printable(StringRef(s.key.toString())))
							    .detail("Value", printable(StringRef(s.value.toString())));
							printf("   Key: %-50s  Value: %s\n",
							       printable(StringRef(s.key.toString())).c_str(),
							       printable(StringRef(s.value.toString())).c_str());
						}
					} else {
						printf("No left over backup agent configuration keys\n");
					}

					Optional<Value> latestVersion = wait(tr->get(backupLatestVersionsKey));
					if (latestVersion.present()) {
						TraceEvent(SevError, "BackupCorrectnessLeftOverVersionKey", self->randomID)
						    .detail("BackupTag", printable(self->backupTag))
						    .detail("BackupLatestVersionsKey", backupLatestVersionsKey.printable())
						    .detail("DestUidValue", destUidValue.printable());
					} else {
						printf("No left over backup version key\n");
					}

					RangeResult versions = wait(tr->getRange(
					    KeyRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath))), 1));
					if (!self->shareLogRange || !versions.size()) {
						RangeResult logValues = wait(
						    tr->getRange(KeyRange(KeyRangeRef(backupLogValuesKey, strinc(backupLogValuesKey))), 100));

						// Error if the log/mutation keyspace for the backup tag  is not empty
						if (logValues.size() > 0) {
							displaySystemKeys++;
							printf("BackupCorrectnessLeftOverLogKeys: (%d) %s\n",
							       logValues.size(),
							       printable(backupLogValuesKey).c_str());
							TraceEvent(SevError, "BackupCorrectnessLeftOverLogKeys", self->randomID)
							    .detail("BackupTag", printable(self->backupTag))
							    .detail("LeftOverKeys", logValues.size())
							    .detail("KeySpace", printable(backupLogValuesKey));
						} else {
							printf("No left over backup log keys\n");
						}
					}

					break;
				} catch (Error& e) {
					TraceEvent("RW_CheckException", self->randomID).error(e);
					wait(tr->onError(e));
				}
			}

			if (displaySystemKeys) {
				wait(TaskBucket::debugPrintRange(cx, normalKeys.end, StringRef()));
			}

			TraceEvent("RW_Complete", self->randomID).detail("BackupTag", printable(self->backupTag));

			// Decrement the backup agent requets
			if (self->agentRequest) {
				RestoreWorkload::backupAgentRequests--;
			}

			// SOMEDAY: Remove after backup agents can exist quiescently
			if ((g_simulator->backupAgents == ISimulator::BackupAgentType::BackupToFile) &&
			    (!RestoreWorkload::backupAgentRequests)) {
				g_simulator->backupAgents = ISimulator::BackupAgentType::NoBackupAgents;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "BackupAndRestorePartitionedCorrectness").error(e).GetLastError();
			throw;
		}
		return Void();
	}
};

int RestoreWorkload::backupAgentRequests = 0;

WorkloadFactory<RestoreWorkload> RestoreWorkloadFactory;
