/*
 * BulkLoading.h
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

#ifndef FDBCLIENT_BULKLOADING_H
#define FDBCLIENT_BULKLOADING_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class BulkLoadType : uint8_t {
	Invalid = 0,
	SST = 1,
};

enum class BulkLoadTransportMethod : uint8_t {
	Invalid = 0,
	CP = 1, // Local file copy. Used when the data file is in the local file system for any storage server. Used for
	        // simulation test and local cluster test.
	BLOBSTORE = 2,
};

// Define the configuration of bytes sampling
// Use for setting manifest file
struct BulkLoadByteSampleSetting {
	constexpr static FileIdentifier file_identifier = 1384500;

	BulkLoadByteSampleSetting() = default;

	BulkLoadByteSampleSetting(int version,
	                          const std::string& method,
	                          int factor,
	                          int overhead,
	                          double minimalProbability)
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

// Definition of bulkload/dump files metadata
struct BulkLoadFileSet {
	constexpr static FileIdentifier file_identifier = 1384501;

	BulkLoadFileSet() = default;

	BulkLoadFileSet(const std::string& rootPath,
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
	BulkDumpFileFullPathSet(const BulkLoadFileSet& fileSet) {
		folder = joinPath(fileSet.rootPath, fileSet.relativePath);
		dataFilePath = joinPath(folder, fileSet.dataFileName);
		byteSampleFilePath = joinPath(folder, fileSet.byteSampleFileName);
		manifestFilePath = joinPath(folder, fileSet.manifestFileName);
	}
	std::string folder = "";
	std::string dataFilePath = "";
	std::string byteSampleFilePath = "";
	std::string manifestFilePath = "";

	std::string toString() const {
		return "[Folder]: " + folder + ", [ManifestFilePath]: " + manifestFilePath +
		       ", [DataFilePath]: " + dataFilePath + ", [ByteSampleFilePath]: " + byteSampleFilePath;
	}
};

// Define the metadata of bulkdump manifest file
// The file is uploaded along with the data files
struct BulkLoadManifest {
	constexpr static FileIdentifier file_identifier = 1384502;

	BulkLoadManifest() = default;

	BulkLoadManifest(const BulkLoadFileSet& fileSet,
	                 const Key& beginKey,
	                 const Key& endKey,
	                 const Version& version,
	                 const std::string& checksum,
	                 int64_t bytes,
	                 const BulkLoadByteSampleSetting& byteSampleSetting)
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

	BulkLoadFileSet fileSet;
	Key beginKey;
	Key endKey;
	Version version;
	std::string checksum;
	int64_t bytes;
	BulkLoadByteSampleSetting byteSampleSetting;
};

enum class BulkLoadPhase : uint8_t {
	Invalid = 0, // Used to distinguish if a BulkLoadTaskState is a valid task
	Submitted = 1, // Set by users
	Triggered = 2, // Update when DD trigger a data move for the task
	Running = 3, // Update atomically with updating KeyServer dest servers in startMoveKey
	Complete = 4, // Update atomically with updating KeyServer src servers in finishMoveKey
	Acknowledged = 5, // Updated by users; DD automatically clear metadata with this phase
};

struct BulkLoadTaskState {
	constexpr static FileIdentifier file_identifier = 1384499;

	BulkLoadTaskState() = default;

	// for acknowledging a completed task, where only taskId and range are used
	BulkLoadTaskState(UID taskId, KeyRange range) : taskId(taskId), range(range), phase(BulkLoadPhase::Invalid) {}

	// for submitting a task
	BulkLoadTaskState(KeyRange range,
	                  BulkLoadType loadType,
	                  BulkLoadTransportMethod transportMethod,
	                  std::string folder,
	                  std::unordered_set<std::string> dataFiles,
	                  Optional<std::string> bytesSampleFile)
	  : taskId(deterministicRandom()->randomUniqueID()), range(range), loadType(loadType),
	    transportMethod(transportMethod), folder(folder), dataFiles(dataFiles), bytesSampleFile(bytesSampleFile),
	    phase(BulkLoadPhase::Submitted) {
		ASSERT(isValid());
	}

	bool operator==(const BulkLoadTaskState& rhs) const {
		return taskId == rhs.taskId && range == rhs.range && dataFiles == rhs.dataFiles;
	}

	std::string toString() const {
		std::string res =
		    "BulkLoadTaskState: [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		    ", [Type]: " + std::to_string(static_cast<uint8_t>(loadType)) +
		    ", [TransportMethod]: " + std::to_string(static_cast<uint8_t>(transportMethod)) +
		    ", [Phase]: " + std::to_string(static_cast<uint8_t>(phase)) + ", [Folder]: " + folder +
		    ", [DataFiles]: " + describe(dataFiles) + ", [SubmitTime]: " + std::to_string(submitTime) +
		    ", [TriggerTime]: " + std::to_string(triggerTime) + ", [StartTime]: " + std::to_string(startTime) +
		    ", [CompleteTime]: " + std::to_string(completeTime) + ", [RestartCount]: " + std::to_string(restartCount);
		if (bytesSampleFile.present()) {
			res = res + ", [ByteSampleFile]: " + bytesSampleFile.get();
		}
		if (dataMoveId.present()) {
			res = res + ", [DataMoveId]: " + dataMoveId.get().toString();
		}
		res = res + ", [TaskId]: " + taskId.toString();
		return res;
	}

	KeyRange getRange() const { return range; }

	UID getTaskId() const { return taskId; }

	std::string getFolder() const { return folder; }

	BulkLoadTransportMethod getTransportMethod() const { return transportMethod; }

	std::unordered_set<std::string> getDataFiles() const { return dataFiles; }

	Optional<std::string> getBytesSampleFile() const { return bytesSampleFile; }

	bool onAnyPhase(const std::vector<BulkLoadPhase>& inputPhases) const {
		for (const auto& inputPhase : inputPhases) {
			if (inputPhase == phase) {
				return true;
			}
		}
		return false;
	}

	void setDataMoveId(UID id) {
		if (dataMoveId.present() && dataMoveId.get() != id) {
			TraceEvent(SevWarn, "DDBulkLoadEngineTaskUpdateDataMoveId")
			    .detail("NewId", id)
			    .detail("BulkLoadTask", this->toString());
		}
		dataMoveId = id;
	}

	inline Optional<UID> getDataMoveId() const { return dataMoveId; }

	inline void clearDataMoveId() { dataMoveId.reset(); }

	bool isValid() const {
		if (!taskId.isValid()) {
			return false;
		}
		if (range.empty()) {
			return false;
		}
		if (transportMethod == BulkLoadTransportMethod::Invalid) {
			return false;
		} else if (transportMethod != BulkLoadTransportMethod::CP) {
			ASSERT(false);
		}
		if (dataFiles.empty()) {
			return false;
		}
		for (const auto& filePath : dataFiles) {
			if (filePath.substr(0, folder.size()) != folder) {
				return false;
			}
		}
		if (bytesSampleFile.present()) {
			if (bytesSampleFile.get().substr(0, folder.size()) != folder) {
				return false;
			}
		}
		// TODO(BulkLoad): do some validation between methods and files

		return true;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           range,
		           loadType,
		           transportMethod,
		           phase,
		           folder,
		           dataFiles,
		           bytesSampleFile,
		           dataMoveId,
		           taskId,
		           submitTime,
		           triggerTime,
		           startTime,
		           completeTime,
		           restartCount);
	}

	// Updated by DD
	BulkLoadPhase phase = BulkLoadPhase::Invalid;
	double submitTime = 0;
	double triggerTime = 0;
	double startTime = 0;
	double completeTime = 0;
	int restartCount = -1;

private:
	// Set by user
	UID taskId; // Unique ID of the task
	KeyRange range; // Load the key-value within this range "[begin, end)" from data file
	// File inject config
	BulkLoadType loadType = BulkLoadType::Invalid;
	BulkLoadTransportMethod transportMethod = BulkLoadTransportMethod::Invalid;
	// Folder includes all files to be injected
	std::string folder;
	// Files to inject
	std::unordered_set<std::string> dataFiles;
	Optional<std::string> bytesSampleFile;
	// bytesSampleFile is Optional. If bytesSampleFile is not provided, storage server will go through all keys and
	// conduct byte sampling, which will slow down the bulk loading rate.
	// TODO(BulkLoad): add file checksum

	// Set by DD
	Optional<UID> dataMoveId;
};

BulkLoadTaskState newBulkLoadTaskLocalSST(KeyRange range,
                                          std::string folder,
                                          std::string dataFile,
                                          std::string bytesSampleFile);

#endif
