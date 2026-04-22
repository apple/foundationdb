/*
 * BackupRangePartitionedProgress.cpp
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

#include "fdbserver/backupworker/BackupRangePartitionedProgress.h"

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"

void BackupRangePartitionedProgress::addBackupStatus(const WorkerBackupStatus& status) {
	auto& it = progress[status.epoch];
	auto lb = it.lower_bound(status.tag);
	if (lb != it.end() && status.tag == lb->first) {
		if (lb->second < status.version) {
			lb->second = status.version;
		}
	} else {
		it.insert(lb, { status.tag, status.version });
	}
}

Future<Void> getBackupRangePartitionedProgress(Database cx,
                                               UID dbgid,
                                               Reference<BackupRangePartitionedProgress> bStatus,
                                               Severity severity) {
	Transaction tr(cx);

	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			RangeResult results = co_await tr.getRange(backupRangePartitionedProgressKeys, CLIENT_KNOBS->TOO_MANY);
			ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

			for (auto& it : results) {
				const UID workerID = decodeBackupRangePartitionedProgressKey(it.key);
				const WorkerBackupStatus status = decodeBackupRangePartitionedProgressValue(it.value);
				bStatus->addBackupStatus(status);

				TraceEvent(severity, "GotBackupRangePartitionedProgress", dbgid)
				    .detail("BackupWorker", workerID)
				    .detail("Epoch", status.epoch)
				    .detail("Version", status.version)
				    .detail("Tag", status.tag.toString())
				    .detail("TotalTags", status.totalTags);
			}
			co_return;
		} catch (Error& e) {
			err = e;
		}

		co_await tr.onError(err);
	}
}
