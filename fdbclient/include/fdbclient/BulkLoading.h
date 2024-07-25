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

enum class BulkLoadPhase : uint8_t {
	Invalid = 0, // Used to distinguish if a BulkLoadState is a valid task
	Submitted = 1, // Set by users
	Triggered = 2, // Update when DD trigger a data move for the task
	Running = 3, // Update atomically with updating KeyServer dest servers in startMoveKey
	Complete = 4, // Update atomically with updating KeyServer src servers in finishMoveKey
	Acknowledged = 5, // Updated by users; DD automatically clear metadata with this phase
};

enum class BulkLoadType : uint8_t {
	Invalid = 0,
	SST = 1,
};

enum class BulkLoadTransportMethod : uint8_t {
	Invalid = 0,
	CP = 1, // Local file copy. Used when the data file is in the local file system for any storage server. Used for
	        // simulation test and local cluster test.
};

enum class BulkLoadInjectMethod : uint8_t {
	Invalid = 0,
	File = 1,
};

struct BulkLoadState {
	constexpr static FileIdentifier file_identifier = 1384499;

	BulkLoadState() = default;

	// for acknowledging a completed task, where only taskId and range are used
	BulkLoadState(UID taskId, KeyRange range) : taskId(taskId), range(range), phase(BulkLoadPhase::Invalid) {}

	// for submitting a task
	BulkLoadState(KeyRange range,
	              BulkLoadType loadType,
	              BulkLoadTransportMethod transportMethod,
	              BulkLoadInjectMethod injectMethod,
	              std::string folder,
	              std::unordered_set<std::string> dataFiles,
	              Optional<std::string> bytesSampleFile)
	  : taskId(deterministicRandom()->randomUniqueID()), range(range), loadType(loadType),
	    transportMethod(transportMethod), injectMethod(injectMethod), folder(folder), dataFiles(dataFiles),
	    bytesSampleFile(bytesSampleFile), phase(BulkLoadPhase::Submitted) {
		ASSERT(isValid());
	}

	bool operator==(const BulkLoadState& rhs) const {
		return taskId == rhs.taskId && range == rhs.range && dataFiles == rhs.dataFiles;
	}

	std::string toString() const {
		std::string res =
		    "BulkLoadState: [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		    ", [Type]: " + std::to_string(static_cast<uint8_t>(loadType)) +
		    ", [TransportMethod]: " + std::to_string(static_cast<uint8_t>(transportMethod)) +
		    ", [InjectMethod]: " + std::to_string(static_cast<uint8_t>(injectMethod)) +
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
			TraceEvent(SevWarn, "DDBulkLoadTaskUpdateDataMoveId")
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
			throw not_implemented();
		}
		if (injectMethod == BulkLoadInjectMethod::Invalid) {
			return false;
		} else if (injectMethod != BulkLoadInjectMethod::File) {
			throw not_implemented();
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
		           injectMethod,
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
	BulkLoadInjectMethod injectMethod = BulkLoadInjectMethod::Invalid;
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

BulkLoadState newBulkLoadTaskLocalSST(KeyRange range,
                                      std::string folder,
                                      std::string dataFile,
                                      std::string bytesSampleFile);

#endif
