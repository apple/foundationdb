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
#include "flow/Trace.h"
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class BulkLoadType : uint8_t {
	Invalid = 0,
	SST = 1,
};

enum class BulkLoadTransportMethod : uint8_t {
	Invalid = 0,
	CP = 1, // Upload/download to local file system. Used by simulation test and local cluster test.
	BLOBSTORE = 2, // Upload/download to remote blob store. Used by real clusters.
};

// Here are important metadata: (1) BulkLoadTaskState; (2) BulkDumpState; (3) BulkLoadManifest. BulkLoadTaskState is
// only used for bulkload core engine which persists the metadata for each unit bulkload range (aka. task).
// BulkDumpState is used for bulk dumping. BulkLoadManifest is the metadata for persisting core information of the
// load/dumped range. TODO(BulkLoad): In the next PR, there will be a BulkLoadState metadata which is used for the
// bulkload job. (aka loading a folder).

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

	bool operator==(const BulkLoadByteSampleSetting& rhs) const {
		return version == rhs.version && method == rhs.method && factor == rhs.factor && overhead == rhs.overhead &&
		       minimalProbability == rhs.minimalProbability;
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
// Each bulkload/dump task has exactly one range.
// The range of the data is included in the Folder = RootPath + RelativePath.
// The folder includes 1 manifest file which has all necessary metadata to load the range.
// If the range is not empty, the folder includes 1 data file.
// Otherwise, the dataFileName is empty.
// If the data is sufficiently large, the folder includes 1 byteSample file.
// Otherwise, the byteSampleFileName is empty.
struct BulkLoadFileSet {
public:
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

	BulkLoadFileSet(const std::string& rootPath) : rootPath(rootPath) {
		if (rootPath.empty()) {
			TraceEvent(SevError, "BulkLoadFileSetProvideInvalidPath")
			    .suppressFor(10.0)
			    .detail("Reason", "InitBulkLoadFileSet")
			    .detail("FileSet", toString());
			throw bulkload_fileset_invalid_filepath();
		}
	}

	bool hasManifestFile() const { return !manifestFileName.empty(); }

	bool hasDataFile() const { return !dataFileName.empty(); }

	bool hasByteSampleFile() const { return !byteSampleFileName.empty(); }

	bool isValid() const {
		if (rootPath.empty()) {
			ASSERT(false);
			return false;
		}
		if (!hasManifestFile()) {
			ASSERT(false);
			return false;
		}
		if (!hasDataFile() && hasByteSampleFile()) {
			// If bytes sample file exists, the data file must exist.
			ASSERT(false);
			return false;
		}
		return true;
	}

	std::string getRootPath() const {
		if (rootPath.empty()) {
			TraceEvent(SevError, "BulkLoadSetByteSampleFileNameError")
			    .suppressFor(10.0)
			    .detail("Reason", "GetRootPath")
			    .detail("FileSet", toString());
		}
		return rootPath;
	}

	std::string getRelativePath() const { return relativePath; }

	std::string getManifestFileName() const { return manifestFileName; }

	std::string getDataFileName() const { return dataFileName; }

	std::string getByteSampleFileName() const { return byteSampleFileName; }

	void setByteSampleFileName(const std::string& inputFileName) {
		if (hasByteSampleFile() || inputFileName.empty()) {
			TraceEvent(SevError, "BulkLoadSetByteSampleFileNameError")
			    .suppressFor(10.0)
			    .detail("InputFileName", inputFileName)
			    .detail("FileSet", toString());
		}
		byteSampleFileName = inputFileName;
	}

	void removeDataFile() { dataFileName = ""; }

	void removeByteSampleFile() { byteSampleFileName = ""; }

	std::string getFolder() const {
		if (rootPath.empty()) {
			TraceEvent(SevError, "BulkLoadFileSetProvideInvalidPath")
			    .suppressFor(10.0)
			    .detail("Reason", "GetFolder")
			    .detail("FileSet", toString());
			throw bulkload_fileset_invalid_filepath();
		} else if (relativePath.empty()) {
			return rootPath;
		} else {
			return joinPath(rootPath, relativePath);
		}
	}

	std::string getManifestFileFullPath() const {
		if (!hasManifestFile()) {
			TraceEvent(SevError, "BulkLoadFileSetProvideInvalidPath")
			    .suppressFor(10.0)
			    .detail("Reason", "GetManifestFileFullPath")
			    .detail("FileSet", toString());
			throw bulkload_fileset_invalid_filepath();
		} else {
			return joinPath(getFolder(), manifestFileName);
		}
	}

	std::string getDataFileFullPath() const {
		if (!hasDataFile()) {
			TraceEvent(SevError, "BulkLoadFileSetProvideInvalidPath")
			    .suppressFor(10.0)
			    .detail("Reason", "GetDataFileFullPath")
			    .detail("FileSet", toString());
			throw bulkload_fileset_invalid_filepath();
		} else {
			return joinPath(getFolder(), dataFileName);
		}
	}

	std::string getBytesSampleFileFullPath() const {
		if (!hasByteSampleFile()) {
			TraceEvent(SevError, "BulkLoadFileSetProvideInvalidPath")
			    .suppressFor(10.0)
			    .detail("Reason", "GetBytesSampleFileFullPath")
			    .detail("FileSet", toString());
			throw bulkload_fileset_invalid_filepath();
		} else {
			return joinPath(getFolder(), byteSampleFileName);
		}
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

private:
	std::string rootPath = "";
	std::string relativePath = "";
	std::string manifestFileName = "";
	std::string dataFileName = "";
	std::string byteSampleFileName = "";
};

// Define the metadata of bulkload manifest file.
// The manifest file stores the ground true of metadata of dumped data file, such as range and version.
// The manifest file is uploaded along with the data file.
struct BulkLoadManifest {
	constexpr static FileIdentifier file_identifier = 1384502;

	BulkLoadManifest() = default;

	// Used when dumping to manifest file and persist to metadata or loading data.
	// So, we need to make sure the content is valid.
	BulkLoadManifest(const BulkLoadFileSet& fileSet,
	                 const Key& beginKey,
	                 const Key& endKey,
	                 const Version& version,
	                 const std::string& checksum,
	                 int64_t bytes,
	                 const BulkLoadByteSampleSetting& byteSampleSetting,
	                 BulkLoadType loadType,
	                 BulkLoadTransportMethod transportMethod)
	  : fileSet(fileSet), beginKey(beginKey), endKey(endKey), version(version), checksum(checksum), bytes(bytes),
	    byteSampleSetting(byteSampleSetting), loadType(loadType), transportMethod(transportMethod) {
		ASSERT(isValid());
	}

	// Used when initialize a bulk dump job. Information are partially filled at this time.
	BulkLoadManifest(BulkLoadType loadType, BulkLoadTransportMethod transportMethod, const std::string& rootPath)
	  : loadType(loadType), transportMethod(transportMethod) {
		fileSet = BulkLoadFileSet(rootPath);
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
		if (transportMethod == BulkLoadTransportMethod::Invalid) {
			return false;
		} else if (transportMethod != BulkLoadTransportMethod::CP &&
		           transportMethod != BulkLoadTransportMethod::BLOBSTORE) {
			ASSERT(false);
		}
		if (loadType == BulkLoadType::Invalid) {
			return false;
		} else if (loadType != BulkLoadType::SST) {
			ASSERT(false);
		}
		return true;
	}

	std::string getRootPath() const { return fileSet.getRootPath(); }

	KeyRange getRange() const { return Standalone(KeyRangeRef(beginKey, endKey)); }

	BulkLoadTransportMethod getTransportMethod() const { return transportMethod; }

	BulkLoadType getLoadType() const { return loadType; }

	std::string getBeginKeyString() const { return beginKey.toFullHexStringPlain(); }

	std::string getEndKeyString() const { return endKey.toFullHexStringPlain(); }

	std::string getDataFileFullPath() const { return fileSet.getDataFileFullPath(); }

	std::string getBytesSampleFileFullPath() const { return fileSet.getBytesSampleFileFullPath(); }

	BulkLoadByteSampleSetting getByteSampleSetting() const { return byteSampleSetting; }

	std::string getFolder() const { return fileSet.getFolder(); }

	int64_t getTotalBytes() const { return bytes; }

	BulkLoadFileSet getFileSet() const { return fileSet; }

	void setRange(const KeyRange& range) {
		ASSERT(!range.empty() && beginKey.empty() && endKey.empty());
		beginKey = range.begin;
		endKey = range.end;
		return;
	}

	// Generating human readable string to stored in the manifest file
	std::string toString() const {
		return fileSet.toString() + ", [BeginKey]: " + getBeginKeyString() + ", [EndKey]: " + getEndKeyString() +
		       ", [Version]: " + std::to_string(version) + ", [Checksum]: " + checksum +
		       ", [Bytes]: " + std::to_string(bytes) + ", " + byteSampleSetting.toString() +
		       ", [loadType]: " + std::to_string(static_cast<uint8_t>(loadType)) +
		       ", [TransportMethod]: " + std::to_string(static_cast<uint8_t>(transportMethod));
	}

	// Generate human readable string as an entry in the job manifest file.
	// A job is partitioned into tasks by ranges.
	// The job manifest includes all manifest files for all ranges within the job.
	std::string generateEntryInJobManifest() const {
		return getBeginKeyString() + ", " + getEndKeyString() + ", " + std::to_string(version) + ", " +
		       std::to_string(bytes) + ", " + joinPath(fileSet.getRelativePath(), fileSet.getManifestFileName());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, fileSet, beginKey, endKey, version, checksum, bytes, byteSampleSetting, loadType, transportMethod);
	}

	BulkLoadFileSet fileSet;
	Key beginKey;
	Key endKey;
	Version version;
	std::string checksum;
	int64_t bytes;
	BulkLoadByteSampleSetting byteSampleSetting;
	BulkLoadType loadType = BulkLoadType::Invalid;
	BulkLoadTransportMethod transportMethod = BulkLoadTransportMethod::Invalid;
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

	// For submitting a task by a job
	BulkLoadTaskState(const UID& jobId, const BulkLoadManifest& manifest)
	  : jobId(jobId), taskId(deterministicRandom()->randomUniqueID()), phase(BulkLoadPhase::Submitted),
	    manifest(manifest) {}

	bool operator==(const BulkLoadTaskState& rhs) const {
		return jobId == rhs.jobId && taskId == rhs.taskId && getRange() == rhs.getRange() &&
		       getDataFileFullPath() == rhs.getDataFileFullPath();
	}

	std::string toString() const {
		std::string res = "BulkLoadTaskState: [JobId]: " + jobId.toString() + ", [TaskId]: " + taskId.toString() +
		                  ", [Manifest]: " + manifest.toString();
		if (dataMoveId.present()) {
			res = res + ", [DataMoveId]: " + dataMoveId.get().toString();
		}
		res = res + ", [TaskId]: " + taskId.toString();
		return res;
	}

	KeyRange getRange() const { return manifest.getRange(); }

	UID getTaskId() const { return taskId; }

	std::string getRootPath() const { return manifest.getRootPath(); }

	BulkLoadTransportMethod getTransportMethod() const { return manifest.getTransportMethod(); }

	std::string getDataFileFullPath() const { return manifest.getDataFileFullPath(); }

	std::string getBytesSampleFileFullPath() const { return manifest.getBytesSampleFileFullPath(); }

	std::string getFolder() const { return manifest.getFolder(); }

	int64_t getTotalBytes() const { return manifest.getTotalBytes(); }

	BulkLoadFileSet getFileSet() const { return manifest.getFileSet(); }

	BulkLoadByteSampleSetting getByteSampleSetting() const { return manifest.getByteSampleSetting(); }

	BulkLoadType getLoadType() const { return manifest.getLoadType(); }

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

	bool isValid(bool checkManifest = true) const {
		if (!jobId.isValid()) {
			return false;
		}
		if (!taskId.isValid()) {
			return false;
		}
		if (checkManifest && !manifest.isValid()) {
			return false;
		}
		return true;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           jobId,
		           taskId,
		           dataMoveId,
		           manifest,
		           phase,
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
	UID jobId; // Unique ID of the job
	UID taskId; // Unique ID of the task
	// Set by DD
	Optional<UID> dataMoveId;
	// Set by DD or users
	BulkLoadManifest manifest;
};

// Define job manifest file name.
std::string generateBulkLoadJobManifestFileName();

// Used when a SS load a range but the range does not have a valid byte
// sampling file. In this case, the SS generates a byte sampling file with a name tight to the data filename. This
// generated file temporarily stored locally and the file is used to inject the byte sampling metadata to SS. After the
// injection, the file is removed.
std::string generateBulkLoadBytesSampleFileNameFromDataFileName(const std::string& dataFileName);

// Return a manifest filename as a place holder when testing bulkload feature, where we issue bulkload tasks without
// providing a manifest file. So, this manifest file is never read in this scenario.
std::string generateEmptyManifestFileName();

// Define human readable BulkLoad job manifest content in the following format:
// Head: Manifest count: <count>, Root: <root>
// Rows: BeginKey, EndKey, Version, Bytes, ManifestPath
std::string generateBulkLoadJobManifestFileContent(const std::map<Key, BulkLoadManifest>& manifests);

// For submitting a task manually (for testing)
BulkLoadTaskState createNewBulkLoadTask(const UID& jobId,
                                        const KeyRange& range,
                                        const BulkLoadFileSet& fileSet,
                                        const BulkLoadByteSampleSetting& byteSampleSetting,
                                        const Version& snapshotVersion,
                                        const std::string& checksum,
                                        const int64_t& bytes,
                                        const BulkLoadType& type,
                                        const BulkLoadTransportMethod& transportMethod);

#endif
