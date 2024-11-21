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

struct BulkDumpState {
	constexpr static FileIdentifier file_identifier = 1384498;

	BulkDumpState() = default;

	// The only public interface to create a valid task
	// This constructor is call when users submitting a task, e.g. by newBulkDumpTaskLocalSST()
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
		if (version.present()) {
			res = res + ", [Version]: " + std::to_string(version.get());
		}
		if (parentTaskId.present()) {
			res = res + ", [ParentTaskId]: " + parentTaskId.get().toString();
		}
		if (parentTaskFolder.present()) {
			res = res + ", [ParentTaskFolder]: " + parentTaskFolder.get();
		}
		return res;
	}

	KeyRange getRange() const { return range; }

	UID getTaskId() const { return taskId; }

	std::string getFolder() const { return folder; }

	BulkDumpPhase getPhase() const { return phase; }

	BulkDumpTransportMethod getTransportMethod() const { return transportMethod; }

	Optional<std::string> getParentFolder() const { return parentTaskFolder; }

	Optional<UID> getParentId() const { return parentTaskId; }

	bool isValid() const {
		if (parentTaskId.present() && !parentTaskId.get().isValid()) {
			return false;
		}
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

	// A parent task is the task on a large range set by user. The user task will spawn a series of small ranges tasks
	// based on shard boundary to cover the user task range. Those spawned tasks are executed by SSes.
	BulkDumpState spawn(const KeyRange& childTaskRange) {
		ASSERT(range.contains(childTaskRange));
		UID childTaskId = deterministicRandom()->randomUniqueID();
		std::string childTaskFolder = childTaskId.toString();
		// Spawn a new state as a child and set this as the parent of the child
		BulkDumpState res(childTaskId,
		                  childTaskRange,
		                  fileType,
		                  transportMethod,
		                  exportMethod,
		                  childTaskFolder,
		                  parentTaskId.present() ? parentTaskId.get() : taskId,
		                  parentTaskFolder.present() ? parentTaskFolder.get() : folder);
		return res;
	}

	// Generate a metadata with Complete state.
	BulkDumpState getRangeCompleteState(const KeyRange& completeRange) {
		ASSERT(range.contains(completeRange));
		ASSERT(parentTaskId.present());
		ASSERT(parentTaskFolder.present());
		BulkDumpState res(taskId,
		                  completeRange,
		                  fileType,
		                  transportMethod,
		                  exportMethod,
		                  folder,
		                  parentTaskId.get(),
		                  parentTaskFolder.get());
		res.phase = BulkDumpPhase::Complete;
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           phase,
		           taskId,
		           range,
		           fileType,
		           transportMethod,
		           exportMethod,
		           folder,
		           version,
		           parentTaskId,
		           parentTaskFolder);
	}

private:
	// for spawning a task
	BulkDumpState(UID taskId,
	              KeyRange range,
	              BulkDumpFileType fileType,
	              BulkDumpTransportMethod transportMethod,
	              BulkDumpExportMethod exportMethod,
	              std::string folder,
	              UID inputParentId,
	              std::string inputParentFolder)
	  : taskId(taskId), range(range), fileType(fileType), transportMethod(transportMethod), exportMethod(exportMethod),
	    folder(folder), phase(BulkDumpPhase::Submitted) {
		ASSERT(isValid());
		if (parentTaskId.present()) {
			ASSERT(parentTaskId.get() == inputParentId);
		} else {
			parentTaskId = inputParentId;
		}
		if (parentTaskFolder.present()) {
			ASSERT(parentTaskFolder.get() == inputParentFolder);
		} else {
			parentTaskFolder = inputParentFolder;
		}
	}

	// The taskId is the unique identifier of a task. 
	// Any SS can do a task. If a task is failed, this remaining part of the task can be picked up by any SS with a changed taskId
	UID taskId;

	// File dump config
	KeyRange range; // Dump the key-value within this range "[begin, end)" from data file
	BulkDumpFileType fileType = BulkDumpFileType::Invalid;
	BulkDumpTransportMethod transportMethod = BulkDumpTransportMethod::Invalid;
	BulkDumpExportMethod exportMethod = BulkDumpExportMethod::Invalid;
	std::string folder; // Folder includes all files to be exported

	Optional<Version> version;
	BulkDumpPhase phase = BulkDumpPhase::Invalid;

	Optional<UID> parentTaskId;
	Optional<std::string> parentTaskFolder;
};

// User API to create bulkDump task metadata
// The dumped data is within the input range
// The data is dumped to the input folder
// The folder can be either a local file path or a remote blobstore file path
BulkDumpState newBulkDumpTaskLocalSST(const KeyRange& range, const std::string& folder);

#endif
