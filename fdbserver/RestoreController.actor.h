/*
 * RestoreController.actor.h
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

// This file declear RestoreController interface and actors

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORE_CONTROLLER_G_H)
#define FDBSERVER_RESTORE_CONTROLLER_G_H
#include "fdbserver/RestoreController.actor.g.h"
#elif !defined(FDBSERVER_RESTORE_CONTROLLER_H)
#define FDBSERVER_RESTORE_CONTROLLER_H

#include <sstream>
#include "flow/Platform.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreWorker.actor.h"

#include "flow/actorcompiler.h" // has to be last include

struct VersionBatch {
	Version beginVersion; // Inclusive
	Version endVersion; // exclusive
	std::set<RestoreFileFR> logFiles;
	std::set<RestoreFileFR> rangeFiles;
	double size; // size of data in range and log files
	int batchIndex; // Never reset

	VersionBatch() : beginVersion(0), endVersion(0), size(0){};

	bool operator<(const VersionBatch& rhs) const {
		return std::tie(batchIndex, beginVersion, endVersion, logFiles, rangeFiles, size) <
		       std::tie(rhs.batchIndex, rhs.beginVersion, rhs.endVersion, rhs.logFiles, rhs.rangeFiles, rhs.size);
	}

	bool isEmpty() { return logFiles.empty() && rangeFiles.empty(); }
	void reset() {
		beginVersion = 0;
		endVersion = 0;
		logFiles.clear();
		rangeFiles.clear();
		size = 0;
	}

	// RestoreAsset and VersionBatch both use endVersion as exclusive in version range
	bool isInVersionRange(Version version) const { return version >= beginVersion && version < endVersion; }
};

struct ControllerBatchData : public ReferenceCounted<ControllerBatchData> {
	// rangeToApplier is in controller and loader node. Loader uses this to determine which applier a mutation should be
	// sent.
	//   KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Key, UID> rangeToApplier;
	Optional<Future<Void>> applyToDB;

	IndexedSet<Key, int64_t> samples; // sample of range and log files
	double samplesSize; // sum of the metric of all samples
	std::set<UID> sampleMsgs; // deduplicate sample messages

	ControllerBatchData() = default;
	~ControllerBatchData() = default;

	// Return true if pass the sanity check
	bool sanityCheckApplierKeyRange() {
		bool ret = true;
		// An applier should only appear once in rangeToApplier
		std::map<UID, Key> applierToRange;
		for (auto& applier : rangeToApplier) {
			if (applierToRange.find(applier.second) == applierToRange.end()) {
				applierToRange[applier.second] = applier.first;
			} else {
				TraceEvent(SevError, "FastRestoreController")
				    .detail("SanityCheckApplierKeyRange", applierToRange.size())
				    .detail("ApplierID", applier.second)
				    .detail("Key1", applierToRange[applier.second])
				    .detail("Key2", applier.first);
				ret = false;
			}
		}
		return ret;
	}

	void logApplierKeyRange(int batchIndex) {
		TraceEvent("FastRestoreLogApplierKeyRange")
		    .detail("BatchIndex", batchIndex)
		    .detail("ApplierKeyRangeNum", rangeToApplier.size());
		for (auto& applier : rangeToApplier) {
			TraceEvent("FastRestoreLogApplierKeyRange")
			    .detail("BatchIndex", batchIndex)
			    .detail("KeyRangeLowerBound", applier.first)
			    .detail("Applier", applier.second);
		}
	}
};

enum class RestoreAssetStatus { Loading, Loaded };

enum class RestoreSendStatus { SendingLogs, SendedLogs, SendingRanges, SendedRanges };

enum class RestoreApplyStatus { Applying, Applied };

// Track restore progress of each RestoreAsset (RA) and
// Use status to sanity check restore property, e.g., each RA should be processed exactly once.
struct ControllerBatchStatus : public ReferenceCounted<ControllerBatchStatus> {
	std::map<RestoreAsset, RestoreAssetStatus> raStatus;
	std::map<UID, RestoreSendStatus> loadStatus;
	std::map<UID, RestoreApplyStatus> applyStatus;

	void addref() { return ReferenceCounted<ControllerBatchStatus>::addref(); }
	void delref() { return ReferenceCounted<ControllerBatchStatus>::delref(); }

	ControllerBatchStatus() = default;
	~ControllerBatchStatus() = default;
};

struct RestoreControllerData : RestoreRoleData, public ReferenceCounted<RestoreControllerData> {
	std::map<Version, VersionBatch> versionBatches; // key is the beginVersion of the version batch

	Reference<IBackupContainer> bc; // Backup container is used to read backup files
	Key bcUrl; // The url used to get the bc

	std::map<int, Reference<ControllerBatchData>> batch;
	std::map<int, Reference<ControllerBatchStatus>> batchStatus;

	AsyncVar<int> runningVersionBatches; // Currently running version batches

	std::map<UID, double> rolesHeartBeatTime; // Key: role id; Value: most recent time controller receives heart beat

	// addActor: add to actorCollection so that when an actor has error, the ActorCollection can catch the error.
	// addActor is used to create the actorCollection when the RestoreController is created
	PromiseStream<Future<Void>> addActor;

	void addref() { return ReferenceCounted<RestoreControllerData>::addref(); }
	void delref() { return ReferenceCounted<RestoreControllerData>::delref(); }

	RestoreControllerData(UID interfId) {
		role = RestoreRole::Controller;
		nodeID = interfId;
		runningVersionBatches.set(0);
	}

	~RestoreControllerData() override = default;

	int getVersionBatchState(int batchIndex) final { return RoleVersionBatchState::INVALID; }
	void setVersionBatchState(int batchIndex, int vbState) final {}

	void initVersionBatch(int batchIndex) override {
		TraceEvent("FastRestoreControllerInitVersionBatch", id()).detail("VersionBatchIndex", batchIndex);
	}

	// Reset controller data at the beginning of each restore request
	void resetPerRestoreRequest() override {
		TraceEvent("FastRestoreControllerReset").detail("OldVersionBatches", versionBatches.size());
		versionBatches.clear();
		batch.clear();
		batchStatus.clear();
		finishedBatch = NotifiedVersion(0);
		versionBatchId = NotifiedVersion(0);
		ASSERT(runningVersionBatches.get() == 0);
	}

	std::string describeNode() override {
		std::stringstream ss;
		ss << "Controller";
		return ss.str();
	}

	void dumpVersionBatches(const std::map<Version, VersionBatch>& versionBatches) {
		int i = 1;
		double rangeFiles = 0;
		double rangeSize = 0;
		double logFiles = 0;
		double logSize = 0;
		for (auto& vb : versionBatches) {
			TraceEvent("FastRestoreVersionBatches")
			    .detail("BatchIndex", vb.second.batchIndex)
			    .detail("ExpectedBatchIndex", i)
			    .detail("BeginVersion", vb.second.beginVersion)
			    .detail("EndVersion", vb.second.endVersion)
			    .detail("Size", vb.second.size);
			for (auto& f : vb.second.rangeFiles) {
				bool invalidVersion = (f.beginVersion != f.endVersion) || (f.beginVersion >= vb.second.endVersion ||
				                                                           f.beginVersion < vb.second.beginVersion);
				TraceEvent(invalidVersion ? SevError : SevInfo, "FastRestoreVersionBatches")
				    .detail("BatchIndex", i)
				    .detail("RangeFile", f.toString());
				rangeSize += f.fileSize;
				rangeFiles++;
			}
			for (auto& f : vb.second.logFiles) {
				bool outOfRange = (f.beginVersion >= vb.second.endVersion || f.endVersion <= vb.second.beginVersion);
				TraceEvent(outOfRange ? SevError : SevInfo, "FastRestoreVersionBatches")
				    .detail("BatchIndex", i)
				    .detail("LogFile", f.toString());
				logSize += f.fileSize;
				logFiles++;
			}
			++i;
		}

		TraceEvent("FastRestoreVersionBatchesSummary")
		    .detail("VersionBatches", versionBatches.size())
		    .detail("LogFiles", logFiles)
		    .detail("RangeFiles", rangeFiles)
		    .detail("LogBytes", logSize)
		    .detail("RangeBytes", rangeSize);
	}

	// Input: Get the size of data in backup files in version range [prevVersion, nextVersion)
	// Return: param1: the size of data at nextVersion, param2: the minimum range file index whose version >
	// nextVersion, param3: log files with data in [prevVersion, nextVersion)
	std::tuple<double, int, std::vector<RestoreFileFR>> getVersionSize(Version prevVersion,
	                                                                   Version nextVersion,
	                                                                   const std::vector<RestoreFileFR>& rangeFiles,
	                                                                   int rangeIdx,
	                                                                   const std::vector<RestoreFileFR>& logFiles) {
		double size = 0;
		TraceEvent(SevDebug, "FastRestoreGetVersionSize")
		    .detail("PreviousVersion", prevVersion)
		    .detail("NextVersion", nextVersion)
		    .detail("RangeFiles", rangeFiles.size())
		    .detail("RangeIndex", rangeIdx)
		    .detail("LogFiles", logFiles.size());
		ASSERT(prevVersion <= nextVersion);
		while (rangeIdx < rangeFiles.size()) {
			TraceEvent(SevDebug, "FastRestoreGetVersionSize").detail("RangeFile", rangeFiles[rangeIdx].toString());
			if (rangeFiles[rangeIdx].version < nextVersion) {
				ASSERT(rangeFiles[rangeIdx].version >= prevVersion);
				size += rangeFiles[rangeIdx].fileSize;
			} else {
				break;
			}
			++rangeIdx;
		}
		std::vector<RestoreFileFR> retLogs;
		// Scan all logFiles every time to avoid assumption on log files' version ranges.
		// For example, we do not assume each version range only exists in one log file
		for (const auto& file : logFiles) {
			Version begin = std::max(prevVersion, file.beginVersion);
			Version end = std::min(nextVersion, file.endVersion);
			if (begin < end) { // logIdx file overlap in [prevVersion, nextVersion)
				double ratio = (end - begin) * 1.0 / (file.endVersion - file.beginVersion);
				size += file.fileSize * ratio;
				retLogs.push_back(file);
			}
		}
		return std::make_tuple(size, rangeIdx, retLogs);
	}

	// Split backup files into version batches, each of which has similar data size
	// Input: sorted range files, sorted log files;
	// Output: a set of version batches whose size is less than SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES
	//         and each mutation in backup files is included in the version batches exactly once.
	// Assumption 1: input files has no empty files;
	// Assumption 2: range files at one version <= FASTRESTORE_VERSIONBATCH_MAX_BYTES.
	// Note: We do not allow a versionBatch size larger than the FASTRESTORE_VERSIONBATCH_MAX_BYTES because the range
	// file size at a version depends on the number of backupAgents and its upper bound is hard to get.
	void buildVersionBatches(const std::vector<RestoreFileFR>& rangeFiles,
	                         const std::vector<RestoreFileFR>& logFiles,
	                         std::map<Version, VersionBatch>* versionBatches,
	                         Version targetVersion) {
		bool rewriteNextVersion = false;
		int rangeIdx = 0;
		int logIdx = 0; // Ensure each log file is included in version batch
		Version prevEndVersion = 0;
		Version nextVersion = 0; // Used to calculate the batch's endVersion
		VersionBatch vb;
		Version maxVBVersion = 0;
		bool lastLogFile = false;
		vb.beginVersion = 0; // Version batch range [beginVersion, endVersion)
		vb.batchIndex = 1;

		while (rangeIdx < rangeFiles.size() || logIdx < logFiles.size()) {
			if (!rewriteNextVersion) {
				if (rangeIdx < rangeFiles.size() && logIdx < logFiles.size()) {
					// nextVersion as endVersion is exclusive in the version range
					nextVersion = std::max(rangeFiles[rangeIdx].version + 1, nextVersion);
				} else if (rangeIdx < rangeFiles.size()) { // i.e., logIdx >= logFiles.size()
					nextVersion = rangeFiles[rangeIdx].version + 1;
				} else if (logIdx < logFiles.size()) {
					while (logIdx < logFiles.size() && logFiles[logIdx].endVersion <= nextVersion) {
						logIdx++;
					}
					if (logIdx < logFiles.size()) {
						nextVersion = logFiles[logIdx].endVersion;
					} else {
						TraceEvent(SevFRDebugInfo, "FastRestoreBuildVersionBatch")
						    .detail("FinishAllLogFiles", logIdx)
						    .detail("CurBatchIndex", vb.batchIndex)
						    .detail("CurBatchSize", vb.size);
						if (prevEndVersion < nextVersion) {
							// Ensure the last log file is included in version batch
							lastLogFile = true;
						} else {
							break; // Finished all log files
						}
					}
				} else {
					// TODO: Check why this may happen?!
					TraceEvent(SevError, "FastRestoreBuildVersionBatch")
					    .detail("RangeIndex", rangeIdx)
					    .detail("RangeFiles", rangeFiles.size())
					    .detail("LogIndex", logIdx)
					    .detail("LogFiles", logFiles.size());
				}
			} else {
				rewriteNextVersion = false;
			}

			double nextVersionSize;
			int nextRangeIdx;
			std::vector<RestoreFileFR> curLogFiles;
			std::tie(nextVersionSize, nextRangeIdx, curLogFiles) =
			    getVersionSize(prevEndVersion, nextVersion, rangeFiles, rangeIdx, logFiles);

			TraceEvent(SevFRDebugInfo, "FastRestoreBuildVersionBatch")
			    .detail("BatchIndex", vb.batchIndex)
			    .detail("VersionBatchBeginVersion", vb.beginVersion)
			    .detail("PreviousEndVersion", prevEndVersion)
			    .detail("NextVersion", nextVersion)
			    .detail("TargetVersion", targetVersion)
			    .detail("RangeIndex", rangeIdx)
			    .detail("RangeFiles", rangeFiles.size())
			    .detail("LogIndex", logIdx)
			    .detail("LogFiles", logFiles.size())
			    .detail("VersionBatchSizeThreshold", SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES)
			    .detail("CurrentBatchSize", vb.size)
			    .detail("NextVersionIntervalSize", nextVersionSize)
			    .detail("NextRangeIndex", nextRangeIdx)
			    .detail("UsedLogFiles", curLogFiles.size())
			    .detail("VersionBatchCurRangeFiles", vb.rangeFiles.size())
			    .detail("VersionBatchCurLogFiles", vb.logFiles.size())
			    .detail("LastLogFile", lastLogFile);

			ASSERT(prevEndVersion < nextVersion); // Ensure progress
			if (vb.size + nextVersionSize <= SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES ||
			    (vb.size < 1 && prevEndVersion + 1 == nextVersion) || lastLogFile) {
				// In case the batch size at a single version > FASTRESTORE_VERSIONBATCH_MAX_BYTES,
				// the version batch should include the single version to avoid false positive in simulation.
				if (vb.size + nextVersionSize > SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES) {
					TraceEvent(g_network->isSimulated() ? SevWarnAlways : SevError, "FastRestoreBuildVersionBatch")
					    .detail("NextVersion", nextVersion)
					    .detail("PreviousEndVersion", prevEndVersion)
					    .detail("NextVersionIntervalSize", nextVersionSize)
					    .detail("VersionBatchSizeThreshold", SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES)
					    .detail("SuggestedMinimumVersionBatchSizeThreshold", nextVersionSize * 2);
				}
				// nextVersion should be included in this batch
				vb.size += nextVersionSize;
				while (rangeIdx < nextRangeIdx && rangeIdx < rangeFiles.size()) {
					ASSERT(rangeFiles[rangeIdx].fileSize > 0);
					vb.rangeFiles.insert(rangeFiles[rangeIdx]);
					++rangeIdx;
				}

				for (auto& log : curLogFiles) {
					ASSERT(log.beginVersion < nextVersion);
					ASSERT(log.endVersion > prevEndVersion);
					ASSERT(log.fileSize > 0);
					vb.logFiles.insert(log);
				}

				vb.endVersion = std::min(nextVersion, targetVersion + 1);
				maxVBVersion = std::max(maxVBVersion, vb.endVersion);
				prevEndVersion = vb.endVersion;
			} else {
				if (vb.size < 1) {
					// [vb.endVersion, nextVersion) > SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES. We should split
					// the version range
					if (prevEndVersion >= nextVersion) {
						// If range files at one version > FASTRESTORE_VERSIONBATCH_MAX_BYTES, DBA should increase
						// FASTRESTORE_VERSIONBATCH_MAX_BYTES to some value larger than nextVersion
						TraceEvent(SevError, "FastRestoreBuildVersionBatch")
						    .detail("NextVersion", nextVersion)
						    .detail("PreviousEndVersion", prevEndVersion)
						    .detail("NextVersionIntervalSize", nextVersionSize)
						    .detail("VersionBatchSizeThreshold", SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES)
						    .detail("SuggestedMinimumVersionBatchSizeThreshold", nextVersionSize * 2);
						// Exit restore early if it won't succeed
						flushAndExit(FDB_EXIT_ERROR);
					}
					ASSERT(prevEndVersion < nextVersion); // Ensure progress
					nextVersion = (prevEndVersion + nextVersion) / 2;
					rewriteNextVersion = true;
					TraceEvent(SevFRDebugInfo, "FastRestoreBuildVersionBatch")
					    .detail("NextVersionIntervalSize", nextVersionSize); // Duplicate Trace
					continue;
				}
				// Finalize the current version batch
				versionBatches->emplace(vb.beginVersion, vb); // copy vb to versionBatch
				TraceEvent(SevFRDebugInfo, "FastRestoreBuildVersionBatch")
				    .detail("FinishBatchIndex", vb.batchIndex)
				    .detail("VersionBatchBeginVersion", vb.beginVersion)
				    .detail("VersionBatchEndVersion", vb.endVersion)
				    .detail("VersionBatchLogFiles", vb.logFiles.size())
				    .detail("VersionBatchRangeFiles", vb.rangeFiles.size())
				    .detail("VersionBatchSize", vb.size)
				    .detail("RangeIndex", rangeIdx)
				    .detail("LogIndex", logIdx)
				    .detail("NewVersionBatchBeginVersion", prevEndVersion)
				    .detail("RewriteNextVersion", rewriteNextVersion);

				// start finding the next version batch
				vb.reset();
				vb.size = 0;
				vb.beginVersion = prevEndVersion;
				vb.batchIndex++;
			}
		}
		// The last wip version batch has some files
		if (vb.size > 0) {
			vb.endVersion = std::min(nextVersion, targetVersion + 1);
			maxVBVersion = std::max(maxVBVersion, vb.endVersion);
			versionBatches->emplace(vb.beginVersion, vb);
		}
		// Invariant: The last vb endverion should be no smaller than targetVersion
		if (maxVBVersion < targetVersion) {
			// Q: Is the restorable version always less than the maximum version from all backup filenames?
			// A: This is true for the raw backup files returned by backup container before we remove the empty files.
			TraceEvent(SevWarnAlways, "FastRestoreBuildVersionBatch")
			    .detail("TargetVersion", targetVersion)
			    .detail("MaxVersionBatchVersion", maxVBVersion);
		}
	}

	void initBackupContainer(Key url, Optional<std::string> proxy) {
		if (bcUrl == url && bc.isValid()) {
			return;
		}
		TraceEvent("FastRestoreControllerInitBackupContainer")
		    .detail("URL", url)
		    .detail("Proxy", proxy.present() ? proxy.get() : "");
		bcUrl = url;
		bc = IBackupContainer::openContainer(url.toString(), proxy, {});
	}
};

ACTOR Future<Void> startRestoreController(Reference<RestoreWorkerData> controllerWorker, Database cx);

#include "flow/unactorcompiler.h"
#endif
