/*
 * BulkDumping.h
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

#include "fdbclient/BulkLoading.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class BulkDumpPhase : uint8_t {
	Invalid = 0,
	Submitted = 1,
	Complete = 2,
};

// Definition of bulkdump metadata
struct BulkDumpState {
	constexpr static FileIdentifier file_identifier = 1384498;

	BulkDumpState() = default;

	// The only public interface to create a valid job
	// This constructor is call when users submitting a job, e.g. by createNewBulkDumpJob()
	BulkDumpState(KeyRange jobRange,
	              BulkLoadType loadType,
	              BulkLoadTransportMethod transportMethod,
	              std::string remoteRoot)
	  : jobId(deterministicRandom()->randomUniqueID()), jobRange(jobRange), phase(BulkDumpPhase::Submitted) {
		manifest = BulkLoadManifest(loadType, transportMethod, remoteRoot);
	}

	bool operator==(const BulkDumpState& rhs) const {
		return jobId == rhs.jobId && jobRange == rhs.jobRange && taskId == rhs.taskId &&
		       getRemoteRoot() == rhs.getRemoteRoot();
	}

	std::string toString() const {
		std::string res = "BulkDumpState: [JobId]: " + jobId.toString() + ", [JobRange]: " + jobRange.toString() +
		                  ", [Manifest]: " + manifest.toString() +
		                  ", [Phase]: " + std::to_string(static_cast<uint8_t>(phase));
		if (taskId.present()) {
			res = res + ", [TaskId]: " + taskId.get().toString();
		}
		return res;
	}

	KeyRange getJobRange() const { return jobRange; }

	UID getJobId() const { return jobId; }

	Optional<UID> getTaskId() const { return taskId; }

	KeyRange getRange() const { return manifest.getRange(); }

	std::string getRemoteRoot() const { return manifest.getRootPath(); }

	BulkDumpPhase getPhase() const { return phase; }

	BulkLoadTransportMethod getTransportMethod() const { return manifest.getTransportMethod(); }

	BulkLoadType getType() const { return manifest.getLoadType(); }

	bool isValid() const {
		if (!jobId.isValid()) {
			return false;
		}
		if (jobRange.empty()) {
			return false;
		}
		if (taskId.present() && !taskId.get().isValid()) {
			return false;
		}
		if (!manifest.isValid()) {
			return false;
		}
		if (!jobRange.contains(getRange())) {
			return false;
		}
		return true;
	}

	// The user job spawns a series of ranges tasks based on shard boundary to cover the user task range.
	// Those spawned tasks are sent to SSes and executed by the SSes.
	// The output task does not have full content. The output task is only used for delivery the dumping task config to
	// SSes. The output task is not persisted to the system metadata.
	// The system metadata used to persist is generated by generateBulkDumpMetadataToPersist().
	BulkDumpState generateRangeTask(const KeyRange& taskRange) {
		ASSERT(jobRange.contains(taskRange));
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
				TraceEvent(SevError, "GenerateRangeTaskRetryTooManyTimes").detail("TaskRange", taskRange);
				throw bulkdump_task_failed();
			}
		}
		res.taskId = newTaskId;
		res.manifest.setRange(taskRange);
		return res;
	}

	// Generate a metadata with Complete state.
	// We must validate the correctness of metadata before persisting it.
	BulkDumpState generateBulkDumpMetadataToPersist(const BulkLoadManifest& manifest) {
		BulkDumpState res = *this;
		res.phase = BulkDumpPhase::Complete;
		res.manifest = manifest;
		ASSERT(res.isValid());
		return res;
	}

	Optional<BulkLoadManifest> getManifest() const { return manifest; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, jobId, jobRange, phase, taskId, manifest);
	}

private:
	// Set by users
	// The unique identifier of a job. There is at most one job globally at a time. Any task spawned by the job shares
	// the same jobId.
	UID jobId;
	// The range to dump of a job. Any task spawned by the job shares the same jobRange. Any task spawned by the job
	// must have a range within the jobRange.
	KeyRange jobRange;

	// Set by system
	// Set to "submit" when user submit a job. // Set to "complete" by SS when the SS completes a task.
	BulkDumpPhase phase = BulkDumpPhase::Invalid;
	// The unique identifier of a task. Any SS can do a task. If a task is failed, this remaining part of the task can
	// be picked up by any SS with a changed taskId.
	Optional<UID> taskId;
	// The manifest metadata persist to system key space and manifest file when a dump task completes.
	BulkLoadManifest manifest; // TODO(Zhe): make this optional
};

// User API to create bulkDump task metadata
// The dumped data is within the input range
// The data is dumped to the input remoteRoot
// The remoteRoot is a local root string or a remote blobstore root string
BulkDumpState createNewBulkDumpJob(const KeyRange& range,
                                   const std::string& remoteRoot,
                                   const BulkLoadType& type,
                                   const BulkLoadTransportMethod& transportMethod);

#endif
