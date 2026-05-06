/*
 * BackupRangePartitionedProgress.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include <map>
#include <tuple>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/logsystem/LogSystem.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"

class BackupRangePartitionedProgress : NonCopyable, ReferenceCounted<BackupRangePartitionedProgress> {
public:
	BackupRangePartitionedProgress(UID id) : dbgid(id) {}
	~BackupRangePartitionedProgress() {}

	// Adds a backup status. If the tag already has an entry, then the max of
	// savedVersion is used.
	void addBackupStatus(const WorkerBackupStatus& status);

	// Returns progress for an epoch.
	std::map<Tag, Version> getEpochStatus(LogEpoch epoch) const {
		const auto it = progress.find(epoch);
		if (it == progress.end())
			return {};
		return it->second;
	}

	void addref() { ReferenceCounted<BackupRangePartitionedProgress>::addref(); }

	void delref() { ReferenceCounted<BackupRangePartitionedProgress>::delref(); }

	std::map<std::tuple<LogEpoch, Version, int>, std::map<Tag, Version>> getUnfinishedBackup();

private:
	// Used for logging and debugging purpose to identify which backup progress it is.
	const UID dbgid;

	// Backup progress saved in the system keyspace. Note there can be multiple
	// progress status for a tag in an epoch due to later epoch trying to fill
	// the gap. "progress" MUST be iterated in ascending order.
	std::map<LogEpoch, std::map<Tag, Version>> progress;
};

Future<Void> getBackupRangePartitionedProgress(Database cx,
                                               UID dbgid,
                                               Reference<BackupRangePartitionedProgress> bStatus,
                                               Severity severity);
