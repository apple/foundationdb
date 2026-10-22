/*
 * BackupSteps.h
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

#ifndef FDBSERVER_BACKUP_STEPS_H
#define FDBSERVER_BACKUP_STEPS_H

#include <algorithm>
#include <string>
#include <vector>

#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbserver/tester/workloads.h"

// Steps shared by the workloads that act on an existing backup, taking explicit parameters.
namespace BackupSteps {

// Discontinues the backup `tag`, tolerating one that no longer needs it.
inline Future<Void> discontinueBackup(Database cx, FileBackupAgent* agent, Key tag) {
	try {
		TraceEvent("DiscontinuingBackup").log();
		co_await agent->discontinueBackup(cx, tag);
	} catch (Error& e) {
		TraceEvent("ErrorDiscontinuingBackup").error(e);
		if (e.code() != error_code_backup_unneeded) {
			throw;
		}
	}
}

// The restorable snapshot with the greatest endVersion, which is the one a restore to the latest restorable version
// uses.
inline const KeyspaceSnapshotFile* latestRestorableSnapshot(BackupDescription const& desc) {
	const KeyspaceSnapshotFile* snapshot = nullptr;
	for (const auto& s : desc.snapshots) {
		if (s.restorable.orDefault(false) && (!snapshot || s.endVersion > snapshot->endVersion)) {
			snapshot = &s;
		}
	}
	return snapshot;
}

inline std::string describeRange(KeyRangeRef const& r) {
	return printable(r.begin) + ":" + printable(r.end);
}

inline std::string describeRanges(std::vector<KeyRange> const& ranges) {
	std::string result;
	for (auto const& r : ranges) {
		result += (result.empty() ? "" : ";") + describeRange(r);
	}
	return result;
}

// Returns `requested` if set, else `backupRanges`. Every requested entry must lie within one backup range:
// submitBackup coalesces them, so a range spanning two is not covered. Otherwise traces `notCoveredEvent` and throws
// restore_error.
inline Standalone<VectorRef<KeyRangeRef>> selectBackupRanges(
    std::vector<KeyRange> const& backupRanges,
    Optional<Standalone<VectorRef<KeyRangeRef>>> const& requested,
    const char* notCoveredEvent) {
	Standalone<VectorRef<KeyRangeRef>> ranges;
	if (requested.present()) {
		for (auto const& r : requested.get()) {
			bool covered =
			    std::any_of(backupRanges.begin(), backupRanges.end(), [&](KeyRange const& b) { return b.contains(r); });
			if (!covered) {
				TraceEvent(SevWarnAlways, notCoveredEvent)
				    .detail("RestoreRange", describeRange(r))
				    .detail("BackupRanges", describeRanges(backupRanges));
				throw restore_error();
			}
			ranges.push_back_deep(ranges.arena(), r);
		}
	} else {
		for (auto const& r : backupRanges) {
			ranges.push_back_deep(ranges.arena(), r);
		}
	}
	return ranges;
}

// The ranges the backup `backupUid` covers.
inline Future<std::vector<KeyRange>> readBackupRanges(Database cx, UID backupUid) {
	return BackupConfig(backupUid).backupRanges().getOrThrow(cx.getReference());
}

} // namespace BackupSteps

#endif
