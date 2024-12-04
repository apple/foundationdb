/*
 * BulkDump.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_BULKDUMPING_H
#define FDBCLIENT_BULKDUMPING_H
#include "flow/Trace.h"
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

// Define the configuration of bytes sampling
// Use for setting manifest file
struct ByteSampleSetting {
	constexpr static FileIdentifier file_identifier = 1384500;

	ByteSampleSetting() = default;

	ByteSampleSetting(int version, const std::string& method, int factor, int overhead, double minimalProbability)
	  : version(version), method(method), factor(factor), overhead(overhead), minimalProbability(minimalProbability) {
		ASSERT(isValid());
	}

	bool isValid() const {
		if (method.size() == 0) {
			return false;
		}
		return true;
	}

	std::string toString() const {
		return "[ByteSampleVersion]: " + std::to_string(version) + ", [ByteSampleMethod]: " + method +
		       ", [ByteSampleFactor]: " + std::to_string(factor) +
		       ", [ByteSampleOverhead]: " + std::to_string(overhead) +
		       ", [ByteSampleMinimalProbability]: " + std::to_string(minimalProbability);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, method, factor, overhead, minimalProbability);
	}

	int version = 0;
	std::string method = "";
	int factor = 0;
	int overhead = 0;
	double minimalProbability = 0.0;
};

// Definition of bulkdump files metadata
struct BulkDumpFileSet {
	constexpr static FileIdentifier file_identifier = 1384501;

	BulkDumpFileSet() = default;

	BulkDumpFileSet(const std::string& rootPath,
	                const std::string& relativePath,
	                const std::string& manifestFileName,
	                const std::string& dataFileName,
	                const std::string& byteSampleFileName)
	  : rootPath(rootPath), relativePath(relativePath), manifestFileName(manifestFileName), dataFileName(dataFileName),
	    byteSampleFileName(byteSampleFileName) {
		if (!isValid()) {
			TraceEvent(SevError, "BulkDumpFileSetInvalid").detail("Content", toString());
			ASSERT(false);
		}
	}

	bool isValid() const {
		if (rootPath.empty()) {
			ASSERT(false);
			return false;
		}
		if (relativePath.empty()) {
			ASSERT(false);
			return false;
		}
		if (manifestFileName.empty()) {
			ASSERT(false);
			return false;
		}
		if (dataFileName.empty() && !byteSampleFileName.empty()) {
			ASSERT(false);
			return false;
		}
		return true;
	}

	std::string toString() const {
		return "[RootPath]: " + rootPath + ", [RelativePath]: " + relativePath +
		       ", [ManifestFileName]: " + manifestFileName + ", [DataFileName]: " + dataFileName +
		       ", [ByteSampleFileName]: " + byteSampleFileName;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rootPath, relativePath, manifestFileName, dataFileName, byteSampleFileName);
	}

	std::string rootPath = "";
	std::string relativePath = "";
	std::string manifestFileName = "";
	std::string dataFileName = "";
	std::string byteSampleFileName = "";
};

struct BulkDumpFileFullPathSet {
	BulkDumpFileFullPathSet(const BulkDumpFileSet& fileSet) {
		folder = joinPath(fileSet.rootPath, fileSet.relativePath);
		dataFilePath = joinPath(folder, fileSet.dataFileName);
		byteSampleFilePath = joinPath(folder, fileSet.byteSampleFileName);
		manifestFilePath = joinPath(folder, fileSet.manifestFileName);
	}
	std::string folder = "";
	std::string dataFilePath = "";
	std::string byteSampleFilePath = "";
	std::string manifestFilePath = "";
};

// Define the metadata of bulkdump manifest file
// The file is uploaded along with the data files
struct BulkDumpManifest {
	constexpr static FileIdentifier file_identifier = 1384502;

	BulkDumpManifest() = default;

	BulkDumpManifest(const BulkDumpFileSet& fileSet,
	                 const Key& beginKey,
	                 const Key& endKey,
	                 const Version& version,
	                 const std::string& checksum,
	                 int64_t bytes,
	                 const ByteSampleSetting& byteSampleSetting)
	  : fileSet(fileSet), beginKey(beginKey), endKey(endKey), version(version), checksum(checksum), bytes(bytes),
	    byteSampleSetting(byteSampleSetting) {
		ASSERT(isValid());
	}

	bool isValid() const {
		if (beginKey >= endKey) {
			return false;
		}
		if (!fileSet.isValid()) {
			return false;
		}
		if (!byteSampleSetting.isValid()) {
			return false;
		}
		return true;
	}

	std::string getBeginKeyString() const { return beginKey.toFullHexStringPlain(); }

	std::string getEndKeyString() const { return endKey.toFullHexStringPlain(); }

	// Generating human readable string to stored in the manifest file
	std::string toString() const {
		return fileSet.toString() + ", [BeginKey]: " + getBeginKeyString() + ", [EndKey]: " + getEndKeyString() +
		       ", [Version]: " + std::to_string(version) + ", [Checksum]: " + checksum +
		       ", [Bytes]: " + std::to_string(bytes) + ", " + byteSampleSetting.toString();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, fileSet, beginKey, endKey, version, checksum, bytes, byteSampleSetting);
	}

	BulkDumpFileSet fileSet;
	Key beginKey;
	Key endKey;
	Version version;
	std::string checksum;
	int64_t bytes;
	ByteSampleSetting byteSampleSetting;
};

enum class BulkDumpPhase : uint8_t {
	Invalid = 0,
	Submitted = 1,
	Complete = 2,
};

enum class BulkDumpFileType : uint8_t {
	Invalid = 0,
	SST = 1,
};

enum class BulkDumpTransportMethod : uint8_t {
	Invalid = 0,
	CP = 1,
};

enum class BulkDumpExportMethod : uint8_t {
	Invalid = 0,
	File = 1,
};

// Definition of bulkdump metadata
struct BulkDumpState {
	constexpr static FileIdentifier file_identifier = 1384498;

	BulkDumpState() = default;

	// The only public interface to create a valid task
	// This constructor is call when users submitting a task, e.g. by newBulkDumpTaskLocalSST()
	BulkDumpState(KeyRange range,
	              BulkDumpFileType fileType,
	              BulkDumpTransportMethod transportMethod,
	              BulkDumpExportMethod exportMethod,
	              std::string remoteRoot)
	  : jobId(deterministicRandom()->randomUniqueID()), range(range), fileType(fileType),
	    transportMethod(transportMethod), exportMethod(exportMethod), remoteRoot(remoteRoot),
	    phase(BulkDumpPhase::Submitted) {
		ASSERT(isValid());
	}

	bool operator==(const BulkDumpState& rhs) const {
		return jobId == rhs.jobId && taskId == rhs.taskId && range == rhs.range && remoteRoot == rhs.remoteRoot;
	}

	std::string toString() const {
		std::string res = "BulkDumpState: [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		                  ", [FileType]: " + std::to_string(static_cast<uint8_t>(fileType)) +
		                  ", [TransportMethod]: " + std::to_string(static_cast<uint8_t>(transportMethod)) +
		                  ", [ExportMethod]: " + std::to_string(static_cast<uint8_t>(exportMethod)) +
		                  ", [Phase]: " + std::to_string(static_cast<uint8_t>(phase)) +
		                  ", [RemoteRoot]: " + remoteRoot + ", [JobId]: " + jobId.toString();
		if (taskId.present()) {
			res = res + ", [TaskId]: " + taskId.get().toString();
		}
		if (version.present()) {
			res = res + ", [Version]: " + std::to_string(version.get());
		}
		if (bulkDumpManifest.present()) {
			res = res + ", [BulkDumpManifest]: " + bulkDumpManifest.get().toString();
		}
		return res;
	}

	KeyRange getRange() const { return range; }

	UID getJobId() const { return jobId; }

	Optional<UID> getTaskId() const { return taskId; }

	std::string getRemoteRoot() const { return remoteRoot; }

	BulkDumpPhase getPhase() const { return phase; }

	BulkDumpTransportMethod getTransportMethod() const { return transportMethod; }

	bool isValid() const {
		if (!jobId.isValid()) {
			return false;
		}
		if (taskId.present() && !taskId.get().isValid()) {
			return false;
		}
		if (range.empty()) {
			return false;
		}
		if (transportMethod == BulkDumpTransportMethod::Invalid) {
			return false;
		} else if (transportMethod != BulkDumpTransportMethod::CP) {
			throw not_implemented();
		}
		if (exportMethod == BulkDumpExportMethod::Invalid) {
			return false;
		} else if (exportMethod != BulkDumpExportMethod::File) {
			throw not_implemented();
		}
		if (remoteRoot.empty()) {
			return false;
		}
		return true;
	}

	// The user job spawns a series of ranges tasks based on shard boundary to cover the user task range.
	// Those spawned tasks are executed by SSes.
	// Return metadata of the task.
	BulkDumpState getRangeTaskState(const KeyRange& taskRange) {
		ASSERT(range.contains(taskRange));
		BulkDumpState res = *this; // the task inherits configuration from the job
		UID newTaskId;
		// Guarantee to have a brand new taskId for the new spawned task
		int retryCount = 0;
		while (true) {
			newTaskId = deterministicRandom()->randomUniqueID();
			if (!res.taskId.present() || res.taskId.get() != newTaskId) {
				break;
			}
			retryCount++;
			if (retryCount > 50) {
				TraceEvent(SevError, "GetRangeTaskStateRetryTooManyTimes").detail("TaskRange", taskRange);
				throw bulkdump_task_failed();
			}
		}
		res.taskId = newTaskId;
		res.range = taskRange;
		return res;
	}

	// Generate a metadata with Complete state.
	BulkDumpState getRangeCompleteState(const KeyRange& completeRange, const BulkDumpManifest& bulkDumpManifest) {
		ASSERT(range.contains(completeRange));
		ASSERT(bulkDumpManifest.isValid());
		ASSERT(taskId.present() && taskId.get().isValid());
		BulkDumpState res = *this;
		res.phase = BulkDumpPhase::Complete;
		res.bulkDumpManifest = bulkDumpManifest;
		res.range = completeRange;
		return res;
	}

	Optional<BulkDumpManifest> getManifest() const { return bulkDumpManifest; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           jobId,
		           range,
		           fileType,
		           transportMethod,
		           exportMethod,
		           remoteRoot,
		           phase,
		           taskId,
		           version,
		           bulkDumpManifest);
	}

private:
	UID jobId; // The unique identifier of a job. Set by user. Any task spawned by the job shares the same jobId and
	           // configuration.

	// File dump config:
	KeyRange range; // Dump the key-value within this range "[begin, end)" from data file
	BulkDumpFileType fileType = BulkDumpFileType::Invalid;
	BulkDumpTransportMethod transportMethod = BulkDumpTransportMethod::Invalid;
	BulkDumpExportMethod exportMethod = BulkDumpExportMethod::Invalid;
	std::string remoteRoot; // remoteRoot is the root string to where the data is set to be uploaded

	// Task dynamics:
	BulkDumpPhase phase = BulkDumpPhase::Invalid;
	Optional<UID> taskId; // The unique identifier of a task. Any SS can do a task. If a task is failed, this remaining
	                      // part of the task can be picked up by any SS with a changed taskId.
	Optional<Version> version;
	Optional<BulkDumpManifest> bulkDumpManifest; // Resulting remote bulkDumpManifest after the dumping task completes
};

// User API to create bulkDump task metadata
// The dumped data is within the input range
// The data is dumped to the input remoteRoot
// The remoteRoot can be either a local root or a remote blobstore root string
BulkDumpState newBulkDumpTaskLocalSST(const KeyRange& range, const std::string& remoteRoot);

#endif
