/*
 * RestoreMaster.h
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

// This file declear RestoreMaster interface and actors

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORE_MASTER_G_H)
#define FDBSERVER_RESTORE_MASTER_G_H
#include "fdbserver/RestoreMaster.actor.g.h"
#elif !defined(FDBSERVER_RESTORE_MASTER_H)
#define FDBSERVER_RESTORE_MASTER_H

#include <sstream>
#include "flow/Stats.h"
#include "flow/Platform.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
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

struct MasterBatchData : public ReferenceCounted<MasterBatchData> {
	// rangeToApplier is in master and loader node. Loader uses this to determine which applier a mutation should be sent.
	//   KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Key, UID> rangeToApplier;
	IndexedSet<Key, int64_t> samples; // sample of range and log files
	double samplesSize; // sum of the metric of all samples
	Optional<Future<Void>> applyToDB;

	MasterBatchData() = default;
	~MasterBatchData() = default;

	// Return true if pass the sanity check
	bool sanityCheckApplierKeyRange() {
		bool ret = true;
		// An applier should only appear once in rangeToApplier
		std::map<UID, Key> applierToRange;
		for (auto& applier : rangeToApplier) {
			if (applierToRange.find(applier.second) == applierToRange.end()) {
				applierToRange[applier.second] = applier.first;
			} else {
				TraceEvent(SevError, "FastRestore")
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
struct MasterBatchStatus : public ReferenceCounted<MasterBatchStatus> {
	std::map<RestoreAsset, RestoreAssetStatus> raStatus;
	std::map<UID, RestoreSendStatus> loadStatus;
	std::map<UID, RestoreApplyStatus> applyStatus;

	void addref() { return ReferenceCounted<MasterBatchStatus>::addref(); }
	void delref() { return ReferenceCounted<MasterBatchStatus>::delref(); }

	MasterBatchStatus() = default;
	~MasterBatchStatus() = default;
};

struct RestoreMasterData : RestoreRoleData, public ReferenceCounted<RestoreMasterData> {
	std::map<Version, VersionBatch> versionBatches; // key is the beginVersion of the version batch

	Reference<IBackupContainer> bc; // Backup container is used to read backup files
	Key bcUrl; // The url used to get the bc

	std::map<int, Reference<MasterBatchData>> batch;
	std::map<int, Reference<MasterBatchStatus>> batchStatus;

	AsyncVar<int> runningVersionBatches; // Currently running version batches

	std::map<UID, double> rolesHeartBeatTime; // Key: role id; Value: most recent time master receives heart beat

	void addref() { return ReferenceCounted<RestoreMasterData>::addref(); }
	void delref() { return ReferenceCounted<RestoreMasterData>::delref(); }

	RestoreMasterData() {
		role = RestoreRole::Master;
		nodeID = UID();
		runningVersionBatches.set(0);
	}

	~RestoreMasterData() = default;

	int getVersionBatchState(int batchIndex) final { return RoleVersionBatchState::INVALID; }
	void setVersionBatchState(int batchIndex, int vbState) final {}

	void initVersionBatch(int batchIndex) {
		TraceEvent("FastRestoreMasterInitVersionBatch", id()).detail("VersionBatchIndex", batchIndex);
	}

	// Reset master data at the beginning of each restore request
	void resetPerRestoreRequest() {
		TraceEvent("FastRestoreMasterReset").detail("OldVersionBatches", versionBatches.size());
		versionBatches.clear();
		batch.clear();
		batchStatus.clear();
		finishedBatch = NotifiedVersion();
		ASSERT(runningVersionBatches.get() == 0);
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "Master";
		return ss.str();
	}

	void dumpVersionBatches(const std::map<Version, VersionBatch>& versionBatches) {
		int i = 1;
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
			}
			for (auto& f : vb.second.logFiles) {
				bool outOfRange = (f.beginVersion >= vb.second.endVersion || f.endVersion <= vb.second.beginVersion);
				TraceEvent(outOfRange ? SevError : SevInfo, "FastRestoreVersionBatches")
				    .detail("BatchIndex", i)
				    .detail("LogFile", f.toString());
			}
			++i;
		}
	}

	// Input: Get the size of data in backup files in version range [prevVersion, nextVersion)
	// Return: param1: the size of data at nextVersion, param2: the minimum range file index whose version >
	// nextVersion, param3: log files with data in [prevVersion, nextVersion)
	std::tuple<double, int, std::vector<RestoreFileFR>> getVersionSize(Version prevVersion, Version nextVersion,
	                                                                   const std::vector<RestoreFileFR>& rangeFiles,
	                                                                   int rangeIdx,
	                                                                   const std::vector<RestoreFileFR>& logFiles) {
		double size = 0;
		TraceEvent("FastRestoreGetVersionSize")
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
		int logIdx = 0;
		std::vector<RestoreFileFR> retLogs;
		// Scan all logFiles every time to avoid assumption on log files' version ranges.
		// For example, we do not assume each version range only exists in one log file
		while (logIdx < logFiles.size()) {
			Version begin = std::max(prevVersion, logFiles[logIdx].beginVersion);
			Version end = std::min(nextVersion, logFiles[logIdx].endVersion);
			if (begin < end) { // logIdx file overlap in [prevVersion, nextVersion)
				double ratio = (end - begin) * 1.0 / (logFiles[logIdx].endVersion - logFiles[logIdx].beginVersion);
				size += logFiles[logIdx].fileSize * ratio;
				retLogs.push_back(logFiles[logIdx]);
			}
			++logIdx;
		}
		return std::make_tuple(size, rangeIdx, retLogs);
	}

	// Split backup files into version batches, each of which has similar data size
	// Input: sorted range files, sorted log files;
	// Output: a set of version batches whose size is less than SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES
	//    	   and each mutation in backup files is included in the version batches exactly once.
	// Assumption 1: input files has no empty files;
	// Assumption 2: range files at one version <= FASTRESTORE_VERSIONBATCH_MAX_BYTES.
	// Note: We do not allow a versionBatch size larger than the FASTRESTORE_VERSIONBATCH_MAX_BYTES because the range
	// file size at a version depends on the number of backupAgents and its upper bound is hard to get.
	void buildVersionBatches(const std::vector<RestoreFileFR>& rangeFiles, const std::vector<RestoreFileFR>& logFiles,
	                         std::map<Version, VersionBatch>* versionBatches) {
		bool rewriteNextVersion = false;
		int rangeIdx = 0;
		int logIdx = 0; // Ensure each log file is included in version batch
		Version prevEndVersion = 0;
		Version nextVersion = 0; // Used to calculate the batch's endVersion
		VersionBatch vb;
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
						break; // Finished all log files
					}
				} else {
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

			TraceEvent("FastRestoreBuildVersionBatch")
			    .detail("VersionBatchBeginVersion", vb.beginVersion)
			    .detail("PreviousEndVersion", prevEndVersion)
			    .detail("NextVersion", nextVersion)
			    .detail("RangeIndex", rangeIdx)
			    .detail("RangeFiles", rangeFiles.size())
			    .detail("LogIndex", logIdx)
			    .detail("LogFiles", logFiles.size())
			    .detail("VersionBatchSizeThreshold", SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES)
			    .detail("CurrentBatchSize", vb.size)
			    .detail("NextVersionIntervalSize", nextVersionSize)
			    .detail("NextRangeIndex", nextRangeIdx)
			    .detail("UsedLogFiles", curLogFiles.size());

			ASSERT(prevEndVersion < nextVersion); // Ensure progress
			if (vb.size + nextVersionSize <= SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES ||
			    (vb.size < 1 && prevEndVersion + 1 == nextVersion)) {
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

				vb.endVersion = nextVersion;
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
					TraceEvent("FastRestoreBuildVersionBatch")
					    .detail("NextVersionIntervalSize", nextVersionSize); // Duplicate Trace
					continue;
				}
				// Finalize the current version batch
				versionBatches->emplace(vb.beginVersion, vb); // copy vb to versionBatch
				// start finding the next version batch
				vb.reset();
				vb.size = 0;
				vb.beginVersion = prevEndVersion;
				vb.batchIndex++;
			}
		}
		// The last wip version batch has some files
		if (vb.size > 0) {
			vb.endVersion = nextVersion;
			versionBatches->emplace(vb.beginVersion, vb);
		}
	}

	void initBackupContainer(Key url) {
		if (bcUrl == url && bc.isValid()) {
			return;
		}
		printf("initBackupContainer, url:%s\n", url.toString().c_str());
		bcUrl = url;
		bc = IBackupContainer::openContainer(url.toString());
	}
};

ACTOR Future<Void> startRestoreMaster(Reference<RestoreWorkerData> masterWorker, Database cx);

#include "flow/unactorcompiler.h"
#endif
