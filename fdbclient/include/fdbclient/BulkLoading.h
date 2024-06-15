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
	Invalid = 0, // Set by users
	Triggered = 1, // Update when DD trigger a data move for the task
	Running = 2, // Update atomically with updating KeyServer dest servers in startMoveKey
	Complete = 3, // Update atomically with updating KeyServer src servers in finishMoveKey
};

enum class BulkLoadType : uint8_t {
	Invalid = 0,
	SST = 1,
};

enum class BulkLoadTransportMethod : uint8_t {
	Invalid = 0,
	CP = 1,
};

enum class BulkLoadInjectMethod : uint8_t {
	Invalid = 0,
	File = 1,
};

struct BulkLoadState {
	constexpr static FileIdentifier file_identifier = 1384499;

	BulkLoadState() = default;

	BulkLoadState(BulkLoadType loadType, std::string folder)
	  : loadType(loadType), folder(folder), phase(BulkLoadPhase::Invalid), taskId(UID()) {}

	BulkLoadState(KeyRange range, BulkLoadType loadType, std::string folder)
	  : range(range), loadType(loadType), folder(folder), phase(BulkLoadPhase::Invalid), taskId(UID()) {}

	bool isValid() const { return loadType != BulkLoadType::Invalid; }

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
		if (commitVersion.present()) {
			res = res + ", [CommitVersion]: " + std::to_string(commitVersion.get());
		}
		res = res + ", [TaskId]: " + taskId.toString();
		return res;
	}

	bool setTaskId(UID id) {
		if (taskId.isValid() && taskId != id) {
			return false;
		}
		taskId = id;
		return true;
	}

	void setDataMoveId(UID id) {
		if (dataMoveId.present() && dataMoveId.get() != id) {
			TraceEvent(SevWarn, "DDBulkLoadTaskUpdateDataMoveId")
			    .detail("NewId", id)
			    .detail("BulkLoadTask", this->toString());
		}
		dataMoveId = id;
	}

	bool setTransportMethod(BulkLoadTransportMethod method) {
		// TODO(Zhe): do some validation between method and path
		if (method == BulkLoadTransportMethod::Invalid) {
			return false;
		} else if (method == BulkLoadTransportMethod::CP) {
			transportMethod = method;
			return true;
		} else {
			throw not_implemented();
		}
	}

	bool setInjectMethod(BulkLoadInjectMethod method) {
		// TODO(Zhe): do some validation between method and type
		if (method == BulkLoadInjectMethod::Invalid) {
			return false;
		} else if (method == BulkLoadInjectMethod::File) {
			injectMethod = method;
			return true;
		} else {
			throw not_implemented();
		}
	}

	bool addDataFile(std::string filePath) {
		if (filePath.substr(0, folder.size()) != folder) {
			return false;
		}
		dataFiles.insert(filePath);
		return true;
	}

	bool setByteSampleFile(std::string filePath) {
		if (filePath.substr(0, folder.size()) != folder) {
			return false;
		}
		bytesSampleFile = filePath;
		return true;
	}

	bool isValid() { return !dataFiles.empty(); }

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

	// Set by users
	KeyRange range;
	BulkLoadType loadType;
	BulkLoadTransportMethod transportMethod;
	BulkLoadInjectMethod injectMethod;
	BulkLoadPhase phase;
	std::string folder; // Used by SS to inject files
	std::unordered_set<std::string> dataFiles; // Used by SS to inject files
	Optional<std::string> bytesSampleFile; // Used by SS to inject files

	// Set by DD
	Optional<UID> dataMoveId;
	UID taskId;
	double submitTime = 0;
	double triggerTime = 0;
	double startTime = 0;
	double completeTime = 0;
	int restartCount = -1;
	// TODO(Zhe): add file checksum

	// Do not serialize
	Promise<Void> completeAck; // Used in DDQueue to propagate task complete signal out
	Optional<Version> commitVersion; // Used in DDTracker to decide the latest bulk load task
};

BulkLoadState newBulkLoadTaskLocalSST(KeyRange range,
                                      std::string folder,
                                      std::string dataFile,
                                      std::string bytesSampleFile);

struct TriggerBulkLoadRequest {
	constexpr static FileIdentifier file_identifier = 1384500;

	TriggerBulkLoadRequest() = default;
	TriggerBulkLoadRequest(BulkLoadState bulkLoadTask) : bulkLoadTask(bulkLoadTask) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, bulkLoadTask, reply);
	}

	BulkLoadState bulkLoadTask;
	ReplyPromise<Void> reply;
};

#endif
