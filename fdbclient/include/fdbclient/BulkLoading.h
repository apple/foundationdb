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
	SQLite = 1,
	RocksDB = 2,
	ShardedRocksDB = 3,
};

enum class BulkLoadAckType : uint8_t {
	Failed = 0,
	Succeed = 1,
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
		std::string res = "BulkLoadState: [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		                  ", [Type]: " + std::to_string(static_cast<uint8_t>(loadType)) +
		                  ", [Phase]: " + std::to_string(static_cast<uint8_t>(phase)) + ", [Folder]: " + folder +
		                  ", [FilePath]: " + describe(filePaths);
		if (bytesSampleFile.present()) {
			res = res + ", [ByteSampleFile]: " + bytesSampleFile.get();
		}
		if (dataMoveId.present()) {
			res = res + ", [DataMoveId]: " + dataMoveId.get().toString();
		}
		res = res + ", [TaskId]: " + taskId.toString();
		return res;
	}

	void setTaskId(UID id) { taskId = id; }

	void setDataMoveId(UID id) {
		ASSERT(!dataMoveId.present() || dataMoveId.get() == id);
		dataMoveId = id;
	}

	bool addDataFile(std::string filePath) {
		if (filePath.substr(0, folder.size()) != folder) {
			return false;
		}
		filePaths.insert(filePath);
		return true;
	}

	bool addByteSampleFile(std::string filePath) {
		if (filePath.substr(0, folder.size()) != folder) {
			return false;
		}
		bytesSampleFile = filePath;
		return true;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, loadType, phase, folder, filePaths, bytesSampleFile, dataMoveId, taskId);
	}

	KeyRange range;
	BulkLoadType loadType;
	BulkLoadPhase phase;
	std::string folder;
	std::unordered_set<std::string> filePaths;
	Optional<std::string> bytesSampleFile;
	Optional<UID> dataMoveId;
	UID taskId;
	Promise<BulkLoadAckType> launchAck; // Used in DDQueue to propagate task launch signal out. Do not serialize
};

#endif
