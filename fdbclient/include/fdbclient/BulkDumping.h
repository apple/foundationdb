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
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class BulkDumpPhase : uint8_t {
	Invalid = 0,
	Submitted = 1,
	Running = 2,
	Complete = 3,
	Failed = 4,
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

struct BulkDumpState {
	constexpr static FileIdentifier file_identifier = 1384498;

	BulkDumpState() = default;

	// for submitting a task
	BulkDumpState(KeyRange range,
	              BulkDumpFileType fileType,
	              BulkDumpTransportMethod transportMethod,
	              BulkDumpExportMethod exportMethod,
	              std::string folder)
	  : taskId(deterministicRandom()->randomUniqueID()), range(range), fileType(fileType),
	    transportMethod(transportMethod), exportMethod(exportMethod), folder(folder), phase(BulkDumpPhase::Submitted) {
		ASSERT(isValid());
	}

	bool operator==(const BulkDumpState& rhs) const {
		return taskId == rhs.taskId && range == rhs.range && folder == rhs.folder;
	}

	std::string toString() const {
		std::string res = "BulkDumpState: [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		                  ", [FileType]: " + std::to_string(static_cast<uint8_t>(fileType)) +
		                  ", [TransportMethod]: " + std::to_string(static_cast<uint8_t>(transportMethod)) +
		                  ", [ExportMethod]: " + std::to_string(static_cast<uint8_t>(exportMethod)) +
		                  ", [Phase]: " + std::to_string(static_cast<uint8_t>(phase)) + ", [Folder]: " + folder +
		                  ", [TaskId]: " + taskId.toString();
		if (subTaskId.present()) {
			res = res + ", [SubTaskId]: " + subTaskId.get().toString();
		}
		if (subFolder.present()) {
			res = res + ", [SubFolder]: " + subFolder.get();
		}
		if (version.present()) {
			res = res + ", [Version]: " + std::to_string(version.get());
		}
		return res;
	}

	KeyRange getRange() const { return range; }

	UID getTaskId() const { return taskId; }

	std::string getFolder() const { return folder; }

	BulkDumpTransportMethod getTransportMethod() const { return transportMethod; }

	bool isValid() const {
		if (!taskId.isValid()) {
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
		if (folder.empty()) {
			return false;
		}
		return true;
	}

	BulkDumpState spawn(const KeyRange& subRange) {
		ASSERT(range.contains(subRange));
		UID randomSubtaskId = deterministicRandom()->randomUniqueID();
		BulkDumpState res(taskId,
		                  subRange,
		                  fileType,
		                  transportMethod,
		                  exportMethod,
		                  folder,
		                  randomSubtaskId,
		                  randomSubtaskId.toString());
		// the child task keeps the parent task id
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, phase, taskId, range, fileType, transportMethod, exportMethod, folder, subTaskId, subFolder, version);
	}

	BulkDumpPhase phase = BulkDumpPhase::Invalid;

private:
	// for spawning a task
	BulkDumpState(UID taskId,
	              KeyRange range,
	              BulkDumpFileType fileType,
	              BulkDumpTransportMethod transportMethod,
	              BulkDumpExportMethod exportMethod,
	              std::string folder,
	              UID subTaskId,
	              std::string subFolder)
	  : taskId(taskId), range(range), fileType(fileType), transportMethod(transportMethod), exportMethod(exportMethod),
	    folder(folder), phase(BulkDumpPhase::Submitted), subTaskId(subTaskId), subFolder(subFolder) {
		ASSERT(isValid());
	}

	UID taskId; // Unique ID of the task
	KeyRange range; // Load the key-value within this range "[begin, end)" from data file
	// File dump config
	BulkDumpFileType fileType = BulkDumpFileType::Invalid;
	BulkDumpTransportMethod transportMethod = BulkDumpTransportMethod::Invalid;
	BulkDumpExportMethod exportMethod = BulkDumpExportMethod::Invalid;
	// Folder includes all files to be exported
	std::string folder;

	// The data of a subrange must be at the same version --- snapshot
	Optional<UID> subTaskId;
	Optional<std::string> subFolder;
	Optional<Version> version;

	void setTaskId(const UID& inputTaskId) { taskId = inputTaskId; }
};

BulkDumpState newBulkDumpTaskLocalSST(const KeyRange& range, const std::string& folder);

#endif
