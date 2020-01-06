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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreWorker.actor.h"

#include "flow/actorcompiler.h" // has to be last include

extern int restoreStatusIndex;

struct VersionBatch {
	Version beginVersion; // Inclusive
	Version endVersion; // exclusive
	std::vector<RestoreFileFR> logFiles;
	std::vector<RestoreFileFR> rangeFiles;
	double size; // size of data in range and log files

	bool isEmpty() { return logFiles.empty() && rangeFiles.empty(); }
	void reset() {
		beginVersion = 0;
		endVersion = 0;
		logFiles.clear();
		rangeFiles.clear();
	}

	// RestoreAsset and VersionBatch both use endVersion as exclusive in version range
	bool isInVersionRange(Version version) const {
		return version >= beginVersion && version < endVersion;
	}
};

struct RestoreMasterData : RestoreRoleData, public ReferenceCounted<RestoreMasterData> {
	// rangeToApplier is in master and loader node. Loader uses this to determine which applier a mutation should be sent.
	//   KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Key, UID> rangeToApplier;
	std::map<Version, VersionBatch> versionBatches; // key is the beginVersion of the version batch

	int batchIndex;

	Reference<IBackupContainer> bc; // Backup container is used to read backup files
	Key bcUrl; // The url used to get the bc

	IndexedSet<Key, int64_t> samples; // sample of range and log files
	double samplesSize; // sum of the metric of all samples

	void addref() { return ReferenceCounted<RestoreMasterData>::addref(); }
	void delref() { return ReferenceCounted<RestoreMasterData>::delref(); }

	RestoreMasterData() {
		role = RestoreRole::Master;
		nodeID = UID();
		batchIndex = 1; // starts with 1 because batchId (NotifiedVersion) in loaders and appliers start with 0
	}

	~RestoreMasterData() = default;

	void resetPerVersionBatch() {
		TraceEvent("FastRestore")
		    .detail("RestoreMaster", "ResetPerVersionBatch")
		    .detail("VersionBatchIndex", batchIndex);
		samplesSize = 0;
		samples.clear();
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "Master versionBatch:" << batchIndex;
		return ss.str();
	}

	void dumpVersionBatches(const std::map<Version, VersionBatch>& versionBatches) {
		int i = 0;
		for (auto& vb : versionBatches) {
			TraceEvent("FastRestoreVersionBatches")
				.detail("BatchIndex", i)
			    .detail("BeginVersion", vb.second.beginVersion)
			    .detail("EndVersion", vb.second.endVersion)
			    .detail("Size", vb.second.size);
			for (auto& f : vb.second.rangeFiles) {
				bool invalidVersion = (f.beginVersion != f.endVersion) || (f.beginVersion >= vb.second.endVersion || f.beginVersion < vb.second.beginVersion);
				TraceEvent(invalidVersion ? SevError : SevInfo, "FastRestoreVersionBatches").detail("BatchIndex", i).detail("RangeFile", f.toString());
			}
			for (auto& f : vb.second.logFiles) {
				bool outOfRange = (f.beginVersion >= vb.second.endVersion || f.endVersion <= vb.second.beginVersion);
				TraceEvent(outOfRange ? SevError : SevInfo, "FastRestoreVersionBatches").detail("BatchIndex", i).detail("LogFile", f.toString());
			}
			++i;
		}
	}

	// Input: Get the size of data at nextVersion in rangeFiles from rangeIdx and logFiles from logIdx
	// Return: param1: the size of data at nextVersion, param2: the minimum range file index whose version >
	// nextVersion, param3: log files with data in [prevVersion, nextVersion]
	std::tuple<double, int, std::vector<RestoreFileFR>> getVersionSize(Version prevVersion, Version nextVersion,
	                                                                   const std::vector<RestoreFileFR>& rangeFiles,
	                                                                   int rangeIdx,
	                                                                   const std::vector<RestoreFileFR>& logFiles) {
		double size = 0;
		TraceEvent("FastRestoreGetVersionSize")
		    .detail("PrevVersion", prevVersion)
		    .detail("NextVersion", nextVersion)
		    .detail("RangeFiles", rangeFiles.size())
		    .detail("RangeIndex", rangeIdx)
		    .detail("LogFiles", logFiles.size());
		ASSERT(prevVersion <= nextVersion);
		while (rangeIdx < rangeFiles.size()) {
			TraceEvent(SevDebug, "FastRestoreGetVersionSize").detail("RangeFile", rangeFiles[rangeIdx].toString());
			if (rangeFiles[rangeIdx].version <= nextVersion) {
				ASSERT(rangeFiles[rangeIdx].version >= prevVersion);
				size += rangeFiles[rangeIdx].fileSize;
			} else {
				//TraceEvent("FastRestoreGetVersionSize").detail("RangeIdx", rangeIdx).detail("FileVersion", rangeFiles[rangeIdx].version).detail("NextVersion", nextVersion);
				// ASSERT(rangeFiles[rangeIdx].version > nextVersion);
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
			Version end = std::min(nextVersion + 1, logFiles[logIdx].endVersion);
			if (begin < end) { // logIdx file overlap in [prevVersion, nextVersion]
				double ratio = (end - begin) / (logFiles[logIdx].endVersion - logFiles[logIdx].beginVersion);
				size += logFiles[logIdx].fileSize * ratio;
				retLogs.push_back(logFiles[logIdx]);
			}
			++logIdx;
		}
		return std::make_tuple(size, rangeIdx, retLogs);
	}

	// Split backup files into version batches, each of which has similar data size
	// Input: sorted range files, sorted log files;
	// Output: a set of version batches whose size is less than opConfig.batchSizeThreshold
	//    and each mutation data in backup files is included in the version batches exactly once
	// Assume: input files has no empty files
	void buildVersionBatches(const std::vector<RestoreFileFR>& rangeFiles, const std::vector<RestoreFileFR>& logFiles,
	                           std::map<Version, VersionBatch>* versionBatches) {
		// Version batch range [beginVersion, endVersion)
		Version beginVersion = 0;
		Version endVersion = 0;
		Version prevEndVersion = 0;
		double batchSize = 0;
		int rangeIdx = 0;
		int logIdx = 0;
		Version nextVersion = 0;
		VersionBatch vb;
		vb.beginVersion = beginVersion;
		bool rewriteNextVersion = false;
		while (rangeIdx < rangeFiles.size() || logIdx < logFiles.size()) {
			if (!rewriteNextVersion) {
				if (rangeIdx < rangeFiles.size() && logIdx < logFiles.size()) {
					nextVersion = std::max(rangeFiles[rangeIdx].version, nextVersion);
				} else if (rangeIdx < rangeFiles.size()) { // i.e., logIdx >= logFiles.size()
					nextVersion = rangeFiles[rangeIdx].version;
				} else if (logIdx < logFiles.size()) {
					while (logIdx < logFiles.size() && logFiles[logIdx].endVersion <= nextVersion) {
						logIdx++;
					}
					if (logIdx < logFiles.size()) {
						nextVersion = logFiles[logIdx].endVersion;
					} else {
						break; // Finished all files
					}
				} else {
					TraceEvent(SevError, "FastRestoreBuildVersionBatch")
					    .detail("RangeIdx", rangeIdx)
					    .detail("RangeFiles", rangeFiles.size())
					    .detail("LogIdx", logIdx)
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
				.detail("PrevEndVersion", prevEndVersion)
			    .detail("NextVersion", nextVersion)
			    .detail("RangeIndex", rangeIdx)
			    .detail("RangeFiles", rangeFiles.size())
			    .detail("LogIndex", logIdx)
			    .detail("LogFiles", logFiles.size())
			    .detail("BatchSizeThreshold", opConfig.batchSizeThreshold)
			    .detail("BatchSize", batchSize)
			    .detail("NextVersionSize", nextVersionSize)
			    .detail("NextRangeIndex", nextRangeIdx)
				.detail("UsedLogFiles", curLogFiles.size());

			if (batchSize + nextVersionSize <= opConfig.batchSizeThreshold) {
				// nextVersion should be included in this batch
				bool advanced = false;
				batchSize += nextVersionSize;
				while (rangeIdx < nextRangeIdx) {
					vb.rangeFiles.push_back(rangeFiles[rangeIdx]);
					++rangeIdx;
					advanced = true;
				}

				for (auto& log : curLogFiles) {
					ASSERT(log.beginVersion <= nextVersion);
					ASSERT(log.endVersion > prevEndVersion);
					vb.logFiles.push_back(log);
					advanced = true;
				}

				ASSERT(advanced == true || nextVersionSize > 0); // Ensure progress
				vb.endVersion = nextVersion + 1;
				prevEndVersion = vb.endVersion;
			} else {
				if (batchSize < 1) {
					// [vb.endVersion, nextVersion) > opConfig.batchSizeThreshold. We should split the version range
					if (prevEndVersion >= nextVersion) {
						// If range files at one version > batchSizeThreshold, DBA should increase the
						// batchSizeThreshold
						TraceEvent(SevError, "FastRestoreBuildVersionBatch")
						    .detail("NextVersion", nextVersion)
						    .detail("PrevEndVersion", prevEndVersion)
						    .detail("NextVersionSize", nextVersionSize)
						    .detail("BatchSizeThreshold", opConfig.batchSizeThreshold);
					}
					ASSERT(prevEndVersion < nextVersion); // Ensure progress
					nextVersion = (prevEndVersion + nextVersion) / 2;
					rewriteNextVersion = true;
					TraceEvent("FastRestoreBuildVersionBatch")
					    .detail("NextVersionSize", nextVersionSize); // Duplicate Trace
					continue;
				}
				// Finalize the current version batch
				vb.endVersion = nextVersion;
				vb.size = batchSize;
				versionBatches->emplace(vb.beginVersion, vb); // copy vb to versionBatch
				vb.reset();
				batchSize = 0;
				vb.beginVersion = nextVersion;
				prevEndVersion = nextVersion; //ensure prevEndVersion is in the batch's version range
			}
		}
		// The last wip version batch has some files
		if (batchSize > 0) {
			vb.endVersion = nextVersion + 1;
			vb.size = batchSize;
			versionBatches->emplace(vb.beginVersion, vb);
		}
	}

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

	void logApplierKeyRange() {
		TraceEvent("FastRestore").detail("ApplierKeyRangeNum", rangeToApplier.size());
		for (auto& applier : rangeToApplier) {
			TraceEvent("FastRestore").detail("KeyRangeLowerBound", applier.first).detail("Applier", applier.second);
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