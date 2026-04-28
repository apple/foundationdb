/*
 * FileBackupAgent.cpp
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbrpc/simulator.h"
#include "flow/EncryptUtils.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "fmt/format.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BulkDumping.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/JSONDoc.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.h"
#include "PartitionedLogIterator.h"
#include "RestoreInterface.h"
#include "fdbclient/Status.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TaskBucket.h"
#include "flow/network.h"
#include "flow/Trace.h"
#include "flow/Util.h"

#include <cinttypes>
#include <cstdint>
#include <ctime>
#include <climits>
#include "flow/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include "flow/xxhash.h"

#include <memory>
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>
#include <unordered_map>
#include <utility>

// Counters to verify BulkDump/BulkLoad were actually used (for test assertions)
std::atomic<int> g_bulkDumpTaskCompleteCount(0);
std::atomic<int> g_bulkLoadRestoreTaskCompleteCount(0);

// Helper function to monitor BulkDump job completion
// Returns true if job completed successfully, false if timed out
Future<bool> monitorBulkDumpJobCompletion(Database cx, UID jobId, double timeoutDuration, double pollInterval) {
	double timeoutStart = now();
	Transaction tr(cx);

	while (true) {
		Error err;
		try {
			Optional<BulkDumpState> currentJob = co_await getSubmittedBulkDumpJob(&tr);
			bool stillRunning = currentJob.present() && currentJob.get().getJobId() == jobId;

			if (!stillRunning) {
				co_return true; // Job completed successfully
			}

			if (now() - timeoutStart > timeoutDuration) {
				co_return false; // Timed out
			}

			co_await delay(pollInterval);
			tr.reset();
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Verify that a complete BulkDump dataset exists for BulkLoad restore
// Returns true if dataset is complete, false if incomplete
// JobId corresponds to a single BulkDump job which represents one snapshot at a specific version
Future<bool> verifyBulkDumpDatasetCompleteness(Reference<IBackupContainer> bc, std::string bulkDumpJobId) {
	try {
		if (bulkDumpJobId.empty()) {
			TraceEvent(SevWarn, "BulkLoadVerifyDatasetEmptyJobId");
			co_return false;
		}

		// Check if job-specific directory exists: bulkdump_data/<job-uuid>/
		// BulkDump stores data under data/<container>/bulkdump_data/ via getBackupDataPath(),
		// which is consistent with where BackupContainer stores other files (logs, ranges, etc.)
		std::string jobDirectoryPath = "bulkdump_data/" + bulkDumpJobId + "/";

		Error err;
		try {
			// Try to list files in the job directory to verify it exists and has content
			Reference<BackupContainerFileSystem> bcfs = bc.castTo<BackupContainerFileSystem>();
			if (bcfs) {
				// Standard listFiles works because BulkDump now writes under data/<container>/
				BackupContainerFileSystem::FilesAndSizesT files = co_await bcfs->listFiles(jobDirectoryPath);
				if (files.empty()) {
					TraceEvent(SevWarn, "BulkLoadVerifyDatasetJobDirectoryEmpty")
					    .detail("JobDirectoryPath", jobDirectoryPath)
					    .detail("BulkDumpJobId", bulkDumpJobId);
					co_return false;
				}

				TraceEvent("BulkLoadVerifyDatasetJobDirectoryFound")
				    .detail("JobDirectoryPath", jobDirectoryPath)
				    .detail("BulkDumpJobId", bulkDumpJobId)
				    .detail("FileCount", files.size());

				// Basic verification: job directory exists and contains files
				// More detailed verification (parsing shard manifests) is delegated to BulkLoad system
				co_return true;
			} else {
				// Cannot verify - backup container doesn't support file listing
				// Use SevWarn (not SevError) to avoid abort in simulation
				TraceEvent(SevWarn, "BulkLoadVerifyDatasetCannotCast")
				    .detail("BulkDumpJobId", bulkDumpJobId)
				    .detail("Note", "BackupContainer does not support file listing required for verification");
				co_return false;
			}
		} catch (Error& e) {
			if (e.code() == error_code_file_not_found) {
				TraceEvent(SevWarn, "BulkLoadVerifyDatasetJobDirectoryNotFound")
				    .detail("JobDirectoryPath", jobDirectoryPath)
				    .detail("BulkDumpJobId", bulkDumpJobId);
				co_return false;
			}
			throw;
		}

	} catch (Error& e) {
		TraceEvent(SevWarn, "BulkLoadVerifyDatasetError").error(e).detail("BulkDumpJobId", bulkDumpJobId);
		co_return false;
	}
}

// Note: BulkLoad configuration validation (shard_encode_location_metadata, enable_read_lock_on_range)
// is performed by the BulkLoad system on the server side. Client-side validation is not possible
// because SERVER_KNOBS are not accessible from fdbclient.

Optional<std::string> fileBackupAgentProxy = Optional<std::string>();

#define SevFRTestInfo SevVerbose
// #define SevFRTestInfo SevInfo

static std::string boolToYesOrNo(bool val) {
	return val ? std::string("Yes") : std::string("No");
}

static std::string versionToString(Optional<Version> version) {
	if (version.present())
		return std::to_string(version.get());
	else
		return "N/A";
}

static std::string timeStampToString(Optional<int64_t> epochs) {
	if (!epochs.present())
		return "N/A";
	return BackupAgentBase::formatTime(epochs.get());
}

static Future<Optional<int64_t>> getTimestampFromVersion(Optional<Version> ver,
                                                         Reference<ReadYourWritesTransaction> tr) {
	if (!ver.present())
		return Optional<int64_t>();

	return timeKeeperEpochsFromVersion(ver.get(), tr);
}

// Time format :
// <= 59 seconds
// <= 59.99 minutes
// <= 23.99 hours
// N.NN days
std::string secondsToTimeFormat(int64_t seconds) {
	if (seconds >= 86400)
		return format("%.2f day(s)", seconds / 86400.0);
	else if (seconds >= 3600)
		return format("%.2f hour(s)", seconds / 3600.0);
	else if (seconds >= 60)
		return format("%.2f minute(s)", seconds / 60.0);
	else
		return format("%lld second(s)", seconds);
}

const Key FileBackupAgent::keyLastRestorable = "last_restorable"_sr;

// For convenience
using ERestoreState = FileBackupAgent::ERestoreState;

StringRef FileBackupAgent::restoreStateText(ERestoreState id) {
	switch (id) {
	case ERestoreState::UNINITIALIZED:
		return "uninitialized"_sr;
	case ERestoreState::QUEUED:
		return "queued"_sr;
	case ERestoreState::STARTING:
		return "starting"_sr;
	case ERestoreState::RUNNING:
		return "running"_sr;
	case ERestoreState::COMPLETED:
		return "completed"_sr;
	case ERestoreState::ABORTED:
		return "aborted"_sr;
	default:
		return "Unknown"_sr;
	}
}

Key FileBackupAgent::getPauseKey() {
	FileBackupAgent backupAgent;
	return backupAgent.taskBucket->getPauseKey();
}

Future<std::vector<KeyBackedTag>> TagUidMap::getAll_impl(TagUidMap* tagsMap,
                                                         Reference<ReadYourWritesTransaction> tr,
                                                         Snapshot snapshot) {
	Key prefix = tagsMap->prefix; // Copying it here as tagsMap lifetime is not tied to this actor
	TagMap::RangeResultType tagPairs = co_await tagsMap->getRange(tr, std::string(), {}, 1e6, snapshot);
	std::vector<KeyBackedTag> results;
	for (auto& p : tagPairs.results)
		results.push_back(KeyBackedTag(p.first, prefix));
	co_return results;
}

KeyBackedTag::KeyBackedTag(std::string tagName, StringRef tagMapPrefix)
  : KeyBackedProperty<UidAndAbortedFlagT>(TagUidMap(tagMapPrefix).getProperty(tagName)), tagName(tagName),
    tagMapPrefix(tagMapPrefix) {}

// Lists all backups and find if any partitioned backup is running.
Future<bool> anyPartitionedBackupRunning(Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	std::vector<KeyBackedTag> tags = co_await getAllBackupTags(tr);

	std::vector<Future<Optional<UidAndAbortedFlagT>>> futures;
	for (const auto& tag : tags) {
		futures.push_back(tag.get(tr));
	}

	co_await waitForAll(futures);
	int i = 0;
	for (i = 0; i < futures.size(); i++) {
		if (futures[i].get().present()) {
			Optional<bool> partitionedLog;
			EBackupState eState;
			BackupConfig config(futures[i].get().get().first);

			co_await (store(eState, config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN)) &&
			          store(partitionedLog, config.partitionedLogEnabled().get(tr)));
			if (FileBackupAgent::isRunnable(eState) && partitionedLog.present() && partitionedLog.get()) {
				co_return true;
			}
		}
	}
	co_return false;
}

class RestoreConfig : public KeyBackedTaskConfig {
public:
	RestoreConfig(UID uid = UID()) : KeyBackedTaskConfig(fileRestorePrefixRange.begin, uid) {}
	RestoreConfig(Reference<Task> task) : KeyBackedTaskConfig(fileRestorePrefixRange.begin, task) {}

	KeyBackedProperty<ERestoreState> stateEnum() { return configSpace.pack(__FUNCTION__sr); }
	Future<StringRef> stateText(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr),
		           [](ERestoreState s) -> StringRef { return FileBackupAgent::restoreStateText(s); });
	}
	KeyBackedProperty<Key> addPrefix() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Key> removePrefix() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<bool> onlyApplyMutationLogs() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<bool> inconsistentSnapshotOnly() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<bool> unlockDBAfterRestore() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<bool> transformPartitionedLog() { return configSpace.pack(__FUNCTION__sr); }
	// BulkLoad integration properties
	KeyBackedProperty<bool> useRangeFileRestore() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<std::string> bulkDumpJobId() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Key> bulkLoadCompleteFuture() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<bool> bulkLoadComplete() { return configSpace.pack(__FUNCTION__sr); }
	// Original BulkLoad mode before restore enabled it - used to restore state after completion/crash
	KeyBackedProperty<int> originalBulkLoadMode() { return configSpace.pack(__FUNCTION__sr); }
	// XXX: Remove restoreRange() once it is safe to remove. It has been changed to restoreRanges
	KeyBackedProperty<KeyRange> restoreRange() { return configSpace.pack(__FUNCTION__sr); }
	// XXX: Changed to restoreRangeSet. It can be removed.
	KeyBackedProperty<std::vector<KeyRange>> restoreRanges() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedSet<KeyRange> restoreRangeSet() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Key> batchFuture() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Version> beginVersion() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Version> restoreVersion() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Version> firstConsistentVersion() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<Reference<IBackupContainer>> sourceContainer() { return configSpace.pack(__FUNCTION__sr); }
	// Get the source container as a bare URL, without creating a container instance
	KeyBackedProperty<Value> sourceContainerURL() { return configSpace.pack("sourceContainer"_sr); }

	// Total bytes written by all log and range restore tasks.
	KeyBackedBinaryValue<int64_t> bytesWritten() { return configSpace.pack(__FUNCTION__sr); }
	// File blocks that have had tasks created for them by the Dispatch task
	KeyBackedBinaryValue<int64_t> filesBlocksDispatched() { return configSpace.pack(__FUNCTION__sr); }
	// File blocks whose tasks have finished
	KeyBackedBinaryValue<int64_t> fileBlocksFinished() { return configSpace.pack(__FUNCTION__sr); }
	// Total number of files in the fileMap
	KeyBackedBinaryValue<int64_t> fileCount() { return configSpace.pack(__FUNCTION__sr); }
	// Total number of file blocks in the fileMap
	KeyBackedBinaryValue<int64_t> fileBlockCount() { return configSpace.pack(__FUNCTION__sr); }
	// BulkLoad sub-phase task counts for detailed progress tracking
	KeyBackedBinaryValue<int64_t> bulkLoadSubmittedTasks() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedBinaryValue<int64_t> bulkLoadTriggeredTasks() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedBinaryValue<int64_t> bulkLoadRunningTasks() { return configSpace.pack(__FUNCTION__sr); }
	KeyBackedBinaryValue<int64_t> bulkLoadTotalTasks() { return configSpace.pack(__FUNCTION__sr); }

	Future<std::vector<KeyRange>> getRestoreRangesOrDefault(Reference<ReadYourWritesTransaction> tr) {
		return getRestoreRangesOrDefault_impl(this, tr);
	}

	static Future<std::vector<KeyRange>> getRestoreRangesOrDefault_impl(RestoreConfig* self,
	                                                                    Reference<ReadYourWritesTransaction> tr) {
		std::vector<KeyRange> ranges;
		int batchSize = BUGGIFY ? 1 : CLIENT_KNOBS->RESTORE_RANGES_READ_BATCH;
		Optional<KeyRange> begin;
		Arena arena;
		while (true) {
			KeyBackedSet<KeyRange>::RangeResultType rangeResult =
			    co_await self->restoreRangeSet().getRange(tr, begin, {}, batchSize);
			ranges.insert(ranges.end(), rangeResult.results.begin(), rangeResult.results.end());
			if (!rangeResult.more) {
				break;
			}
			ASSERT(!rangeResult.results.empty());
			begin = KeyRangeRef(KeyRef(arena, ranges.back().begin), keyAfter(ranges.back().end, arena));
		}

		// fall back to original fields if the new field is empty
		if (ranges.empty()) {
			std::vector<KeyRange> _ranges = co_await self->restoreRanges().getD(tr);
			ranges = _ranges;
			if (ranges.empty()) {
				KeyRange range = co_await self->restoreRange().getD(tr);
				ranges.push_back(range);
			}
		}
		co_return ranges;
	}

	// Describes a file to load blocks from during restore.  Ordered by version and then fileName to enable
	// incrementally advancing through the map, saving the version and path of the next starting point.
	struct RestoreFile {
		Version version; // this is beginVersion, not endVersion
		std::string fileName;
		bool isRange{ false }; // false for log file
		int64_t blockSize{ 0 };
		int64_t fileSize{ 0 };
		Version endVersion{ ::invalidVersion }; // not meaningful for range files
		int64_t tagId = -1; // only meaningful to log files, Log router tag. Non-negative for new backup format.
		int64_t totalTags = -1; // only meaningful to log files, Total number of log router tags.

		Tuple pack() const {
			return Tuple::makeTuple(
			    version, fileName, (int64_t)isRange, fileSize, blockSize, endVersion, tagId, totalTags);
		}
		static RestoreFile unpack(Tuple const& t) {
			RestoreFile r;
			int i = 0;
			r.version = t.getInt(i++);
			r.fileName = t.getString(i++).toString();
			r.isRange = t.getInt(i++) != 0;
			r.fileSize = t.getInt(i++);
			r.blockSize = t.getInt(i++);
			r.endVersion = t.getInt(i++);
			r.tagId = t.getInt(i++);
			r.totalTags = t.getInt(i++);
			return r;
		}
	};

	using FileSetT = KeyBackedSet<RestoreFile>;
	FileSetT fileSet() { return configSpace.pack(__FUNCTION__sr); }

	FileSetT logFileSet() { return configSpace.pack(__FUNCTION__sr); }
	FileSetT rangeFileSet() { return configSpace.pack(__FUNCTION__sr); }

	Future<bool> isRunnable(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr), [](ERestoreState s) -> bool {
			return s != ERestoreState::ABORTED && s != ERestoreState::COMPLETED && s != ERestoreState::UNINITIALIZED;
		});
	}

	Future<Void> logError(Database cx, Error e, std::string const& details, void* taskInstance = nullptr) {
		if (!uid.isValid()) {
			TraceEvent(SevError, "FileRestoreErrorNoUID").error(e).detail("Description", details);
			return Void();
		}
		TraceEvent t(SevWarn, "FileRestoreError");
		t.error(e)
		    .detail("RestoreUID", uid)
		    .detail("Description", details)
		    .detail("TaskInstance", (uint64_t)taskInstance);
		// key_not_found could happen
		if (e.code() == error_code_key_not_found)
			t.backtrace();

		return updateErrorInfo(cx, e, details);
	}

	Key mutationLogPrefix() { return uidPrefixKey(applyLogKeys.begin, uid); }

	Key applyMutationsMapPrefix() { return uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid); }

	static Future<int64_t> getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid) {
		Future<Optional<Value>> beginVal = tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid), Snapshot::True);
		Future<Optional<Value>> endVal = tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid), Snapshot::True);
		co_await (success(beginVal) && success(endVal));

		if (!beginVal.get().present() || !endVal.get().present())
			co_return 0;

		Version beginVersion = BinaryReader::fromStringRef<Version>(beginVal.get().get(), Unversioned());
		Version endVersion = BinaryReader::fromStringRef<Version>(endVal.get().get(), Unversioned());
		co_return endVersion - beginVersion;
	}

	Future<int64_t> getApplyVersionLag(Reference<ReadYourWritesTransaction> tr) {
		return getApplyVersionLag_impl(tr, uid);
	}

	void initApplyMutations(Reference<ReadYourWritesTransaction> tr,
	                        Key addPrefix,
	                        Key removePrefix,
	                        OnlyApplyMutationLogs onlyApplyMutationLogs) {
		// Set these because they have to match the applyMutations values.
		this->addPrefix().set(tr, addPrefix);
		this->removePrefix().set(tr, removePrefix);

		clearApplyMutationsKeys(tr);

		// Initialize add/remove prefix, range version map count and set the map's start key to InvalidVersion
		tr->set(uidPrefixKey(applyMutationsAddPrefixRange.begin, uid), addPrefix);
		tr->set(uidPrefixKey(applyMutationsRemovePrefixRange.begin, uid), removePrefix);

		int64_t startCount = 0;
		tr->set(uidPrefixKey(applyMutationsKeyVersionCountRange.begin, uid), StringRef((uint8_t*)&startCount, 8));
		Key mapStart = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
		tr->set(mapStart, BinaryWriter::toValue<Version>(invalidVersion, Unversioned()));
	}

	void clearApplyMutationsKeys(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

		// Clear add/remove prefix keys
		tr->clear(uidPrefixKey(applyMutationsAddPrefixRange.begin, uid));
		tr->clear(uidPrefixKey(applyMutationsRemovePrefixRange.begin, uid));

		// Clear range version map and count key
		tr->clear(uidPrefixKey(applyMutationsKeyVersionCountRange.begin, uid));
		Key mapStart = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
		tr->clear(KeyRangeRef(mapStart, strinc(mapStart)));

		// Clear any loaded mutations that have not yet been applied
		Key mutationPrefix = mutationLogPrefix();
		tr->clear(KeyRangeRef(mutationPrefix, strinc(mutationPrefix)));

		// Clear end and begin versions (intentionally in this order)
		tr->clear(uidPrefixKey(applyMutationsEndRange.begin, uid));
		tr->clear(uidPrefixKey(applyMutationsBeginRange.begin, uid));
	}

	void setApplyBeginVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
		tr->set(uidPrefixKey(applyMutationsBeginRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
	}

	Future<Version> getApplyBeginVersion(Reference<ReadYourWritesTransaction> tr) {
		return map(tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid)),
		           [=](Optional<Value> const& value) -> Version {
			           return value.present() ? BinaryReader::fromStringRef<Version>(value.get(), Unversioned()) : 0;
		           });
	}

	void setApplyEndVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
		tr->set(uidPrefixKey(applyMutationsEndRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
	}

	Future<Version> getApplyEndVersion(Reference<ReadYourWritesTransaction> tr) {
		return map(tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid)),
		           [=](Optional<Value> const& value) -> Version {
			           return value.present() ? BinaryReader::fromStringRef<Version>(value.get(), Unversioned()) : 0;
		           });
	}

	static Future<Version> getCurrentVersion_impl(RestoreConfig* self, Reference<ReadYourWritesTransaction> tr) {
		ERestoreState status = co_await self->stateEnum().getD(tr);
		Version version = -1;
		if (status == ERestoreState::RUNNING) {
			version = co_await self->getApplyBeginVersion(tr);
		} else if (status == ERestoreState::COMPLETED) {
			version = co_await self->restoreVersion().getD(tr);
		}
		co_return version;
	}

	Future<Version> getCurrentVersion(Reference<ReadYourWritesTransaction> tr) {
		return getCurrentVersion_impl(this, tr);
	}

	static Future<std::string> getProgress_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getProgress(Reference<ReadYourWritesTransaction> tr) { return getProgress_impl(*this, tr); }

	static Future<std::string> getFullStatus_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getFullStatus(Reference<ReadYourWritesTransaction> tr) { return getFullStatus_impl(*this, tr); }
};

using RestoreFile = RestoreConfig::RestoreFile;

// Helper to count bulkload task progress for a job
// Returns: <completedTasks, submittedTasks, triggeredTasks, runningTasks, totalTasks, completedBytes>
// Sub-phases help track progress during the long "running" period:
//   - Submitted: task created, waiting to be picked up by DD
//   - Triggered: assigned to storage server, waiting to start
//   - Running: storage server actively downloading/ingesting SST files
Future<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t, int64_t>> getBulkLoadTaskProgress(Database cx,
                                                                                                 UID jobId) {
	Transaction tr(cx);
	Key readBegin = normalKeys.begin;
	Key readEnd = normalKeys.end;
	int64_t completedTasks = 0;
	int64_t submittedTasks = 0;
	int64_t triggeredTasks = 0;
	int64_t runningTasks = 0;
	int64_t totalTasks = 0;
	int64_t completedBytes = 0;

	while (readBegin < readEnd) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult rangeResult = co_await krmGetRanges(&tr, bulkLoadTaskPrefix, KeyRangeRef(readBegin, readEnd));
			if (rangeResult.empty()) {
				break;
			}
			for (int i = 0; i < static_cast<int>(rangeResult.size()) - 1; ++i) {
				if (rangeResult[i].value.empty()) {
					continue;
				}
				BulkLoadTaskState task = decodeBulkLoadTaskState(rangeResult[i].value);
				if (task.getJobId() != jobId) {
					// Different job, stop counting
					co_return std::make_tuple(
					    completedTasks, submittedTasks, triggeredTasks, runningTasks, totalTasks, completedBytes);
				}
				int manifestCount = task.getManifests().size();
				totalTasks += manifestCount;
				if (task.phase == BulkLoadPhase::Complete) {
					completedTasks += manifestCount;
					// Sum bytes from manifest data sizes
					for (const auto& manifest : task.getManifests()) {
						completedBytes += manifest.getTotalBytes();
					}
				} else if (task.phase == BulkLoadPhase::Submitted) {
					submittedTasks += manifestCount;
				} else if (task.phase == BulkLoadPhase::Triggered) {
					triggeredTasks += manifestCount;
				} else if (task.phase == BulkLoadPhase::Running) {
					runningTasks += manifestCount;
				}
			}
			readBegin = rangeResult.back().key;
		} catch (Error& e) {
			err = e;
		}
		if (err.isValid() && err.code() != error_code_success) {
			TraceEvent(SevWarn, "BulkLoadTaskProgressRetry").error(err).detail("JobId", jobId);
			co_await tr.onError(err);
		}
	}
	co_return std::make_tuple(completedTasks, submittedTasks, triggeredTasks, runningTasks, totalTasks, completedBytes);
}

// Monitor BulkLoad job completion and update restore progress counters
// restoreUid is used to update the RestoreConfig progress
Future<bool> monitorBulkLoadJobCompletionWithProgress(Database cx,
                                                      UID jobId,
                                                      UID restoreUid,
                                                      int64_t totalBlocks,
                                                      double timeoutDuration,
                                                      double pollInterval,
                                                      bool lockAware) {
	double timeoutStart = now();
	RestoreConfig restore(restoreUid);

	while (true) {
		Optional<BulkLoadJobState> currentJob = co_await getRunningBulkLoadJob(cx, lockAware);
		bool stillRunning = currentJob.present() && currentJob.get().getJobId() == jobId;

		if (!stillRunning) {
			co_return true;
		}

		// Update progress based on completed bulkload tasks
		try {
			auto [completed, submitted, triggered, running, total, bytes] = co_await getBulkLoadTaskProgress(cx, jobId);
			if (total > 0) {
				// For bulkload restores, fileBlockCount is 0, so use task count as "blocks"
				// This provides meaningful progress tracking for the restore status display
				// Include all in-progress tasks in dispatched count to show scheduling progress
				int64_t inProgress = submitted + triggered + running;
				int64_t effectiveTotalBlocks = totalBlocks > 0 ? totalBlocks : total;
				int64_t blocksFinished = totalBlocks > 0 ? (totalBlocks * completed) / total : completed;
				int64_t blocksDispatched =
				    totalBlocks > 0 ? (totalBlocks * (completed + inProgress)) / total : (completed + inProgress);

				Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				if (lockAware) {
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				}
				restore.fileBlocksFinished().set(tr, blocksFinished);
				restore.filesBlocksDispatched().set(tr, blocksDispatched);
				restore.fileBlockCount().set(tr, effectiveTotalBlocks);
				restore.bytesWritten().set(tr, bytes);
				// Store sub-phase counts for detailed progress display
				restore.bulkLoadSubmittedTasks().set(tr, submitted);
				restore.bulkLoadTriggeredTasks().set(tr, triggered);
				restore.bulkLoadRunningTasks().set(tr, running);
				restore.bulkLoadTotalTasks().set(tr, total);
				co_await tr->commit();

				TraceEvent("BulkLoadRestoreProgress")
				    .detail("RestoreUID", restoreUid)
				    .detail("JobId", jobId)
				    .detail("CompletedTasks", completed)
				    .detail("SubmittedTasks", submitted)
				    .detail("TriggeredTasks", triggered)
				    .detail("RunningTasks", running)
				    .detail("TotalTasks", total)
				    .detail("BlocksFinished", blocksFinished)
				    .detail("BlocksDispatched", blocksDispatched)
				    .detail("EffectiveTotalBlocks", effectiveTotalBlocks)
				    .detail("BytesWritten", bytes);
			}
		} catch (Error& e) {
			// Log but don't fail - progress updates are best-effort
			TraceEvent(SevWarn, "BulkLoadRestoreProgressError").error(e).detail("JobId", jobId);
		}

		if (now() - timeoutStart > timeoutDuration) {
			co_return false; // Timed out
		}

		co_await delay(pollInterval);
	}
}

Future<std::string> RestoreConfig::getProgress_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	Future<int64_t> fileCount = restore.fileCount().getD(tr);
	Future<int64_t> fileBlockCount = restore.fileBlockCount().getD(tr);
	Future<int64_t> fileBlocksDispatched = restore.filesBlocksDispatched().getD(tr);
	Future<int64_t> fileBlocksFinished = restore.fileBlocksFinished().getD(tr);
	Future<int64_t> bytesWritten = restore.bytesWritten().getD(tr);
	Future<StringRef> status = restore.stateText(tr);
	Future<Version> currentVersion = restore.getCurrentVersion(tr);
	Future<Version> lag = restore.getApplyVersionLag(tr);
	Future<Version> firstConsistentVersion = restore.firstConsistentVersion().getD(tr);
	Future<std::string> tag = restore.tag().getD(tr);
	Future<std::pair<std::string, Version>> lastError = restore.lastError().getD(tr);
	Future<int64_t> submittedTasks = restore.bulkLoadSubmittedTasks().getD(tr);
	Future<int64_t> triggeredTasks = restore.bulkLoadTriggeredTasks().getD(tr);
	Future<int64_t> runningTasks = restore.bulkLoadRunningTasks().getD(tr);
	Future<int64_t> totalTasks = restore.bulkLoadTotalTasks().getD(tr);
	Future<Optional<bool>> useRangeFileRestore = restore.useRangeFileRestore().get(tr);

	UID uid = restore.getUid();
	co_await (success(fileCount) && success(fileBlockCount) && success(fileBlocksDispatched) &&
	          success(fileBlocksFinished) && success(bytesWritten) && success(status) && success(currentVersion) &&
	          success(lag) && success(firstConsistentVersion) && success(tag) && success(lastError) &&
	          success(submittedTasks) && success(triggeredTasks) && success(runningTasks) && success(totalTasks) &&
	          success(useRangeFileRestore));

	bool useRangeFile = useRangeFileRestore.get().present() && useRangeFileRestore.get().get();

	std::string errstr = "None";
	if (lastError.get().second != 0)
		errstr = format("'%s' %" PRId64 "s ago.\n",
		                lastError.get().first.c_str(),
		                (tr->getReadVersion().get() - lastError.get().second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

	TraceEvent("FileRestoreProgress")
	    .detail("RestoreUID", uid)
	    .detail("Tag", tag.get())
	    .detail("State", status.get().toString())
	    .detail("FileCount", fileCount.get())
	    .detail("FileBlocksFinished", fileBlocksFinished.get())
	    .detail("FileBlocksTotal", fileBlockCount.get())
	    .detail("FileBlocksInProgress", fileBlocksDispatched.get() - fileBlocksFinished.get())
	    .detail("SubmittedTasks", submittedTasks.get())
	    .detail("TriggeredTasks", triggeredTasks.get())
	    .detail("RunningTasks", runningTasks.get())
	    .detail("TotalTasks", totalTasks.get())
	    .detail("BytesWritten", bytesWritten.get())
	    .detail("CurrentVersion", currentVersion.get())
	    .detail("FirstConsistentVersion", firstConsistentVersion.get())
	    .detail("ApplyLag", lag.get());

	std::string progressStr;
	if (useRangeFile) {
		progressStr = format("Tag: %s  UID: %s  State: %s\n",
		                     tag.get().c_str(),
		                     uid.toString().c_str(),
		                     status.get().toString().c_str());
		progressStr += format(" Blocks: %lld/%lld complete\n", fileBlocksFinished.get(), fileBlockCount.get());
		progressStr += format(" Files: %lld\n", fileCount.get());
		progressStr += format(" Bytes written: %s\n", formatBytesHumanReadable(bytesWritten.get()).c_str());
		progressStr += format(" Apply version lag: %s\n", versionToString(lag.get()).c_str());
	} else {
		progressStr = format("Tag: %s  UID: %s  State: %s\n",
		                     tag.get().c_str(),
		                     uid.toString().c_str(),
		                     status.get().toString().c_str());
		progressStr += format(" Tasks submitted: %lld  triggered: %lld  running: %lld\n",
		                      submittedTasks.get(),
		                      triggeredTasks.get(),
		                      runningTasks.get());
		progressStr += format(" Tasks complete: %lld / %lld total\n", triggeredTasks.get(), totalTasks.get());
		progressStr += format(" Bytes written: %s\n", formatBytesHumanReadable(bytesWritten.get()).c_str());
		double throughput =
		    triggeredTasks.get() > 0 && totalTasks.get() > 0 ? (double)bytesWritten.get() / totalTasks.get() : 0;
		if (throughput > 0) {
			progressStr += format(" Avg bytes/task: %s\n", formatBytesHumanReadable((int64_t)throughput).c_str());
		}
	}

	co_return progressStr;
}

Future<std::string> RestoreConfig::getFullStatus_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	Future<std::vector<KeyRange>> ranges = restore.getRestoreRangesOrDefault(tr);
	Future<Key> addPrefix = restore.addPrefix().getD(tr);
	Future<Key> removePrefix = restore.removePrefix().getD(tr);
	Future<Key> url = restore.sourceContainerURL().getD(tr);
	Future<Version> restoreVersion = restore.restoreVersion().getD(tr);
	Future<std::string> progress = restore.getProgress(tr);
	Future<ERestoreState> restoreState = restore.stateEnum().getD(tr);
	Future<Optional<bool>> useRangeFileRestore = restore.useRangeFileRestore().get(tr);
	Future<Optional<bool>> bulkLoadComplete = restore.bulkLoadComplete().get(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	co_await (success(ranges) && success(addPrefix) && success(removePrefix) && success(url) &&
	          success(restoreVersion) && success(progress) && success(restoreState) && success(useRangeFileRestore) &&
	          success(bulkLoadComplete));

	std::string returnStr;
	returnStr = format("%s  URL: %s", progress.get().c_str(), url.get().toString().c_str());
	for (auto& range : ranges.get()) {
		returnStr += format("  Range: '%s'-'%s'", printable(range.begin).c_str(), printable(range.end).c_str());
	}
	returnStr += format("  AddPrefix: '%s'  RemovePrefix: '%s'  Version: %lld",
	                    printable(addPrefix.get()).c_str(),
	                    printable(removePrefix.get()).c_str(),
	                    restoreVersion.get());

	// Add enhanced status fields for BulkLoad integration
	bool usingBulkLoad = useRangeFileRestore.get().present() && !useRangeFileRestore.get().get();
	std::string snapshotMethod = usingBulkLoad ? "bulkload" : "rangefile";
	returnStr += format("  Snapshot Method: %s", snapshotMethod.c_str());

	// Add phase status information
	ERestoreState currentState = restoreState.get();
	bool bulkLoadDone = bulkLoadComplete.get().present() && bulkLoadComplete.get().get();

	if (currentState == ERestoreState::RUNNING) {
		if (usingBulkLoad) {
			std::string snapshotPhase = bulkLoadDone ? "complete" : "in_progress";
			returnStr += format("  Snapshot Phase: %s", snapshotPhase.c_str());
			std::string mutationPhase = bulkLoadDone ? "in_progress" : "not_started";
			returnStr += format("  Mutation Log Phase: %s", mutationPhase.c_str());
		} else {
			returnStr += "  Snapshot Phase: in_progress  Mutation Log Phase: in_progress";
		}
	} else if (currentState == ERestoreState::COMPLETED) {
		returnStr += "  Snapshot Phase: complete  Mutation Log Phase: complete";
	}

	co_return returnStr;
}

// two buffers are alternatively serving data and reading data from file
// thus when one buffer is serving data through peek()
// the other buffer is reading data from file to provide pipelining.
class TwoBuffers : public ReferenceCounted<TwoBuffers>, NonCopyable {
public:
	class IteratorBuffer : public ReferenceCounted<IteratorBuffer> {
	public:
		std::shared_ptr<char[]> data;
		// has_value means there is data, otherwise it means there is no data being fetched or ready
		// is_valid means data is being fetched, is_ready means data is ready
		std::optional<Future<Void>> fetchingData;
		size_t size;
		int index;
		int capacity;
		IteratorBuffer(int _capacity) {
			capacity = _capacity;
			data = std::shared_ptr<char[]>(new char[capacity]());
			fetchingData.reset();
			size = 0;
		}
		bool is_valid() { return fetchingData.has_value(); }
		void reset() {
			size = 0;
			index = 0;
			fetchingData.reset();
		}
	};
	TwoBuffers(int capacity, Reference<IBackupContainer> _bc, std::vector<RestoreConfig::RestoreFile>& _files, int tag);
	// ready need to be called first before calling peek
	// because a shared_ptr cannot be wrapped by a Future
	// this method ensures the current buffer has available data
	Future<Void> ready();
	static Future<Void> ready(Reference<TwoBuffers> self);
	// fill buffer[index] with the next block of file
	// it has side effects to change currentFileIndex and currentFilePosition
	static Future<Void> readNextBlock(Reference<TwoBuffers> self, int index);
	// peek can only be called after ready is called
	// it returns the pointer to the active buffer
	std::shared_ptr<char[]> peek();

	int getFileIndex();
	void setFileIndex(int);

	bool hasNext();

	void reset();

	// discard the current buffer and swap to the next one
	void discardAndSwap();

	// try to fill the buffer[index]
	// but no-op if the buffer have valid data or it is actively being filled
	void fillBufferIfAbsent(int index);

	size_t getBufferSize();

private:
	Reference<IteratorBuffer> buffers[2]; // Two buffers for alternating
	size_t bufferCapacity; // Size of each buffer in bytes
	Reference<IBackupContainer> bc;
	std::vector<RestoreConfig::RestoreFile> files;
	int tag;

	int cur; // Index of the current active buffer (0 or 1)
	size_t currentFileIndex; // Index of the current file being read
	size_t currentFilePosition; // Current read position in the current file
};

TwoBuffers::TwoBuffers(int capacity,
                       Reference<IBackupContainer> _bc,
                       std::vector<RestoreConfig::RestoreFile>& _files,
                       int _tag)
  : currentFileIndex(0), currentFilePosition(0), cur(0), bufferCapacity(capacity), files(_files), bc(_bc), tag(_tag) {
	buffers[0] = makeReference<IteratorBuffer>(capacity);
	buffers[1] = makeReference<IteratorBuffer>(capacity);
}

bool TwoBuffers::hasNext() {
	// if it is being load (valid but not ready, what would be the size?)
	while (currentFileIndex < files.size() && currentFilePosition >= files[currentFileIndex].fileSize) {
		currentFileIndex++;
		currentFilePosition = 0;
	}

	if (buffers[0]->is_valid() || buffers[1]->is_valid()) {
		return true;
	}

	return currentFileIndex != files.size();
}

Future<Void> TwoBuffers::ready() {
	return ready(Reference<TwoBuffers>::addRef(this));
}

Future<Void> TwoBuffers::ready(Reference<TwoBuffers> self) {
	// if cur is not ready, then wait
	if (!self->hasNext()) {
		co_return;
	}
	// try to fill the current buffer, and wait before it is filled
	self->fillBufferIfAbsent(self->cur);
	co_await self->buffers[self->cur]->fetchingData.value();
	// try to fill the next buffer, do not wait for the filling
	if (self->hasNext()) {
		self->fillBufferIfAbsent(1 - self->cur);
	}
}

std::shared_ptr<char[]> TwoBuffers::peek() {
	return buffers[cur]->data;
}

int TwoBuffers::getFileIndex() {
	return buffers[cur]->index;
}

void TwoBuffers::setFileIndex(int newIndex) {
	if (newIndex < 0 || newIndex >= files.size()) {
		TraceEvent(SevError, "TwoBuffersFileIndexOutOfBound")
		    .detail("FilesSize", files.size())
		    .detail("NewIndex", newIndex)
		    .log();
	}
	currentFileIndex = newIndex;
}

void TwoBuffers::discardAndSwap() {
	// invalidate cur and change cur to next
	buffers[cur]->fetchingData.reset();
	cur = 1 - cur;
}

void TwoBuffers::reset() {
	// invalidate cur and change cur to next
	buffers[0]->reset();
	buffers[1]->reset();
	cur = 0;
	currentFileIndex = 0;
	currentFilePosition = 0;
}

size_t TwoBuffers::getBufferSize() {
	return buffers[cur]->size;
}

// only one readNextBlock can be run at a single time, otherwie the same block might be loaded twice
Future<Void> TwoBuffers::readNextBlock(Reference<TwoBuffers> self, int index) {
	if (self->currentFileIndex >= self->files.size()) {
		TraceEvent(SevError, "ReadNextBlockOutOfBound")
		    .detail("FileIndex", self->currentFileIndex)
		    .detail("Tag", self->tag)
		    .detail("Position", self->currentFilePosition)
		    .detail("FileSize", self->files[self->currentFileIndex].fileSize)
		    .detail("FilesCount", self->files.size())
		    .log();
		co_return;
	}
	Reference<IAsyncFile> asyncFile = co_await self->bc->readFile(self->files[self->currentFileIndex].fileName);
	size_t fileSize = self->files[self->currentFileIndex].fileSize;
	size_t remaining = fileSize - self->currentFilePosition;
	size_t bytesToRead = std::min(self->bufferCapacity, remaining);
	int bytesRead = co_await asyncFile->read(
	    static_cast<void*>(self->buffers[index]->data.get()), bytesToRead, self->currentFilePosition);
	if (bytesRead != bytesToRead)
		throw restore_bad_read();
	self->buffers[index]->index = self->currentFileIndex;
	self->buffers[index]->size = bytesRead; // Set to actual bytes read
	self->currentFilePosition += bytesRead;
}

void TwoBuffers::fillBufferIfAbsent(int index) {
	if (buffers[index]->is_valid()) {
		// if this buffer is valid, then do not overwrite it
		return;
	}
	if (currentFileIndex == files.size()) {
		// quit if no more contents
		return;
	}
	auto self = Reference<TwoBuffers>::addRef(this);
	self->buffers[index]->fetchingData = readNextBlock(self, index);
	return;
}

bool endOfBlock(char* start, int offset) {
	const unsigned char paddingChar = '\xff';
	return (unsigned char)*(start + offset) == paddingChar;
}

class PartitionedLogIteratorSimple : public PartitionedLogIterator {
public:
	const int BATCH_READ_BLOCK_COUNT = 1;
	const int BLOCK_SIZE = CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE;
	const int mutationHeaderBytes = sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t);
	Reference<IBackupContainer> bc;
	size_t bufferCapacity;
	int tag;
	std::vector<RestoreConfig::RestoreFile> files;
	size_t bufferOffset; // Current read offset
	int bufferSize;
	int fileOffset;
	int fileIndex;
	std::shared_ptr<char[]> buffer;
	std::vector<Version> endVersions;

	PartitionedLogIteratorSimple(Reference<IBackupContainer> _bc,
	                             int _tag,
	                             std::vector<RestoreConfig::RestoreFile> _files,
	                             std::vector<Version> _endVersions);

	bool hasNext() override;
	Future<Void> loadNextBlock();
	static Future<Void> loadNextBlock(Reference<PartitionedLogIteratorSimple> self);
	void removeBlockHeader();

	Standalone<VectorRef<VersionedMutation>> consumeData(Version firstVersion);

	// find the next version without advanding the iterator
	Future<Version> peekNextVersion() override;
	static Future<Version> peekNextVersion(Reference<PartitionedLogIteratorSimple> iterator);

	// get all the mutations of next version and advance the iterator
	// this might issue multiple consumeData() if the data of a version cross buffer boundary
	Future<Standalone<VectorRef<VersionedMutation>>> getNext() override;
	static Future<Standalone<VectorRef<VersionedMutation>>> getNext(Reference<PartitionedLogIteratorSimple> iterator);
};

PartitionedLogIteratorSimple::PartitionedLogIteratorSimple(Reference<IBackupContainer> _bc,
                                                           int _tag,
                                                           std::vector<RestoreConfig::RestoreFile> _files,
                                                           std::vector<Version> _endVersions)
  : bc(_bc), tag(_tag), endVersions(_endVersions), files(std::move(_files)), bufferOffset(0) {
	bufferCapacity = BATCH_READ_BLOCK_COUNT * BLOCK_SIZE;
	buffer = std::shared_ptr<char[]>(new char[bufferCapacity]());
	fileOffset = 0;
	fileIndex = 0;
	bufferSize = 0;
}

// it will set fileOffset and fileIndex
bool PartitionedLogIteratorSimple::hasNext() {
	if (bufferOffset < bufferSize) {
		return true;
	}
	while (fileIndex < files.size() && fileOffset >= files[fileIndex].fileSize) {
		TraceEvent("ReachEndOfLogFiles")
		    .detail("BufferOffset", bufferOffset)
		    .detail("BufferSize", bufferSize)
		    .detail("FileOffset", fileOffset)
		    .detail("FileSize", files[fileIndex].fileSize)
		    .detail("FileName", files[fileIndex].fileName)
		    .detail("Tag", tag)
		    .detail("Index", fileIndex)
		    .log();
		fileOffset = 0;
		fileIndex++;
	}
	return fileIndex < files.size() && fileOffset < files[fileIndex].fileSize;
}

void PartitionedLogIteratorSimple::removeBlockHeader() {
	if (bufferOffset % BLOCK_SIZE == 0) {
		bufferOffset += sizeof(uint32_t);
	}
}

Standalone<VectorRef<VersionedMutation>> PartitionedLogIteratorSimple::consumeData(Version firstVersion) {
	Standalone<VectorRef<VersionedMutation>> mutations = Standalone<VectorRef<VersionedMutation>>();
	char* start = buffer.get();
	bool foundNewVersion = false;
	while (bufferOffset < bufferSize) {
		while (bufferOffset < bufferSize && !endOfBlock(start, bufferOffset)) {
			// for each block
			removeBlockHeader();

			// encoding format:
			// wr << bigEndian64(message.version.version) << bigEndian32(message.version.sub) <<
			// bigEndian32(mutation.size());
			Version version;
			std::memcpy(&version, start + bufferOffset, sizeof(Version));
			version = bigEndian64(version);
			if (version != firstVersion) {
				foundNewVersion = true;
				break; // Different version, stop here
			}

			int32_t subsequence;
			std::memcpy(&subsequence, start + bufferOffset + sizeof(Version), sizeof(int32_t));
			subsequence = bigEndian32(subsequence);

			int32_t mutationSize;
			std::memcpy(&mutationSize, start + bufferOffset + sizeof(Version) + sizeof(int32_t), sizeof(int32_t));
			mutationSize = bigEndian32(mutationSize);

			// assumption: the entire mutation is within the buffer
			size_t mutationTotalSize = mutationHeaderBytes + mutationSize;
			ASSERT(bufferOffset + mutationTotalSize <= bufferSize);

			// transform from stringref to mutationref here
			Standalone<StringRef> mutationData = makeString(mutationSize);
			std::memcpy(mutateString(mutationData), start + bufferOffset + mutationHeaderBytes, mutationSize);
			ArenaReader reader(mutationData.arena(), mutationData, AssumeVersion(g_network->protocolVersion()));
			MutationRef mutation;
			reader >> mutation;

			VersionedMutation vm;
			vm.version = version;
			vm.subsequence = subsequence;
			vm.mutation = mutation;
			mutations.push_back_deep(mutations.arena(), vm);
			// Move the bufferOffset to include this mutation
			bufferOffset += mutationTotalSize;
		}

		if (bufferOffset < bufferSize && endOfBlock(start, bufferOffset)) {
			// there are paddings
			int remain = BLOCK_SIZE - (bufferOffset % BLOCK_SIZE);
			bufferOffset += remain;
		}
		if (foundNewVersion) {
			break;
		}
	}

	return mutations;
}

Future<Void> PartitionedLogIteratorSimple::loadNextBlock() {
	return loadNextBlock(Reference<PartitionedLogIteratorSimple>::addRef(this));
}

Future<Void> PartitionedLogIteratorSimple::loadNextBlock(Reference<PartitionedLogIteratorSimple> self) {
	if (self->bufferOffset < self->bufferSize) {
		// do nothing
		co_return;
	}
	if (!self->hasNext()) {
		co_return;
	}
	Reference<IAsyncFile> asyncFile = co_await self->bc->readFile(self->files[self->fileIndex].fileName);
	size_t fileSize = self->files[self->fileIndex].fileSize;
	size_t remaining = fileSize - self->fileOffset;
	size_t bytesToRead = std::min(self->bufferCapacity, remaining);
	int bytesRead = co_await asyncFile->read(static_cast<void*>((self->buffer.get())), bytesToRead, self->fileOffset);
	if (bytesRead != bytesToRead)
		throw restore_bad_read();
	self->bufferSize = bytesRead; // Set to actual bytes read
	self->bufferOffset = 0; // Reset bufferOffset for the new data
	self->fileOffset += bytesRead;
}

Future<Version> PartitionedLogIteratorSimple::peekNextVersion() {
	return peekNextVersion(Reference<PartitionedLogIteratorSimple>::addRef(this));
}

Future<Version> PartitionedLogIteratorSimple::peekNextVersion(Reference<PartitionedLogIteratorSimple> self) {
	// Read the first mutation's version
	if (!self->hasNext()) {
		co_return Version(0);
	}
	co_await self->loadNextBlock();
	self->removeBlockHeader();
	Version version{ 0 };
	std::memcpy(&version, self->buffer.get() + self->bufferOffset, sizeof(Version));
	version = bigEndian64(version);

	while (self->fileIndex < self->endVersions.size() - 1 && version >= self->endVersions[self->fileIndex]) {
		TraceEvent("SimpleIteratorFindOverlapAndSkip")
		    .detail("Version", version)
		    .detail("FileIndex", self->fileIndex)
		    .log();
		self->bufferOffset = 0;
		self->bufferSize = 0;
		self->fileOffset = 0;
		self->fileIndex += 1;
		co_await self->loadNextBlock();
		self->removeBlockHeader();
		std::memcpy(&version, self->buffer.get() + self->bufferOffset, sizeof(Version));
		version = bigEndian64(version);
	}
	co_return version;
}

Future<Standalone<VectorRef<VersionedMutation>>> PartitionedLogIteratorSimple::getNext(
    Reference<PartitionedLogIteratorSimple> self) {
	Standalone<VectorRef<VersionedMutation>> mutations;
	if (!self->hasNext()) {
		TraceEvent(SevWarn, "SimpleIteratorExhausted")
		    .detail("BufferOffset", self->bufferOffset)
		    .detail("BufferSize", self->bufferSize)
		    .detail("Tag", self->tag)
		    .log();
		co_return mutations;
	}
	Version firstVersion = co_await self->peekNextVersion();
	Standalone<VectorRef<VersionedMutation>> firstBatch = self->consumeData(firstVersion);
	mutations = firstBatch;
	// If the current buffer is fully consumed, then we need to check the next buffer in case
	// the version is sliced across this buffer boundary

	while (self->bufferOffset >= self->bufferSize) {
		// data for one version cannot exceed single buffer size
		// if hitting the end of a batch, check the next batch in case version is
		if (self->hasNext()) {
			// now this is run for each block, but it is not necessary if it is the last block of a file
			// cannot check hasMoreData here because other buffer might have the last piece
			co_await self->loadNextBlock();
			Standalone<VectorRef<VersionedMutation>> batch = self->consumeData(firstVersion);
			for (const VersionedMutation& vm : batch) {
				mutations.push_back_deep(mutations.arena(), vm);
			}
		} else {
			break;
		}
	}
	co_return mutations;
}

Future<Standalone<VectorRef<VersionedMutation>>> PartitionedLogIteratorSimple::getNext() {
	return getNext(Reference<PartitionedLogIteratorSimple>::addRef(this));
}

class PartitionedLogIteratorTwoBuffers : public PartitionedLogIterator {
private:
	Reference<TwoBuffers> twobuffer;

	// consume single version data upto the end of the current batch
	// stop if seeing a different version from the parameter.
	// it has side effects to update bufferOffset after reading the data
	Future<Standalone<VectorRef<VersionedMutation>>> consumeData(Version firstVersion);
	static Future<Standalone<VectorRef<VersionedMutation>>> consumeData(
	    Reference<PartitionedLogIteratorTwoBuffers> self,
	    Version v);

	// each block has a format of {<header>[mutations]<padding>}, need to skip the header to read mutations
	// this method check if bufferOffset is at the boundary and advance it if necessary
	void removeBlockHeader();

public:
	// read up to a fixed number of block count
	// noted that each version has to be contained within 2 blocks
	const int BATCH_READ_BLOCK_COUNT = 1;
	const int BLOCK_SIZE = CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE;
	const int mutationHeaderBytes = sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t);
	Reference<IBackupContainer> bc;
	int tag;
	std::vector<RestoreConfig::RestoreFile> files;
	std::vector<Version> endVersions;
	bool hasMoreData; // Flag indicating if more data is available
	size_t bufferOffset; // Current read offset
	// empty means no data, future is valid but not ready means being fetched
	// future is ready means it currently holds data

	PartitionedLogIteratorTwoBuffers(Reference<IBackupContainer> _bc,
	                                 int _tag,
	                                 std::vector<RestoreConfig::RestoreFile> _files,
	                                 std::vector<Version> _endVersions);

	// whether there are more contents for this tag in all files specified
	bool hasNext() override;

	// find the next version without advanding the iterator
	Future<Version> peekNextVersion() override;
	static Future<Version> peekNextVersion(Reference<PartitionedLogIteratorTwoBuffers> iterator);

	// get all the mutations of next version and advance the iterator
	// this might issue multiple consumeData() if the data of a version cross buffer boundary
	Future<Standalone<VectorRef<VersionedMutation>>> getNext() override;
	static Future<Standalone<VectorRef<VersionedMutation>>> getNext(
	    Reference<PartitionedLogIteratorTwoBuffers> iterator);
};

Future<Standalone<VectorRef<VersionedMutation>>> PartitionedLogIteratorTwoBuffers::consumeData(Version firstVersion) {
	return consumeData(Reference<PartitionedLogIteratorTwoBuffers>::addRef(this), firstVersion);
}

Future<Standalone<VectorRef<VersionedMutation>>> PartitionedLogIteratorTwoBuffers::consumeData(
    Reference<PartitionedLogIteratorTwoBuffers> self,
    Version firstVersion) {
	Standalone<VectorRef<VersionedMutation>> mutations = Standalone<VectorRef<VersionedMutation>>();
	co_await self->twobuffer->ready();
	std::shared_ptr<char[]> start = self->twobuffer->peek();
	int size = self->twobuffer->getBufferSize();
	bool foundNewVersion = false;
	while (self->bufferOffset < size) {
		while (self->bufferOffset < size && !endOfBlock(start.get(), self->bufferOffset)) {
			// for each block
			self->removeBlockHeader();

			// encoding is:
			// wr << bigEndian64(message.version.version) << bigEndian32(message.version.sub) <<
			// bigEndian32(mutation.size());
			Version version;
			std::memcpy(&version, start.get() + self->bufferOffset, sizeof(Version));
			version = bigEndian64(version);
			if (version != firstVersion) {
				foundNewVersion = true;
				break; // Different version, stop here
			}

			int32_t subsequence;
			std::memcpy(&subsequence, start.get() + self->bufferOffset + sizeof(Version), sizeof(int32_t));
			subsequence = bigEndian32(subsequence);

			int32_t mutationSize;
			std::memcpy(
			    &mutationSize, start.get() + self->bufferOffset + sizeof(Version) + sizeof(int32_t), sizeof(int32_t));
			mutationSize = bigEndian32(mutationSize);

			// assumption: the entire mutation is within the buffer
			size_t mutationTotalSize = self->mutationHeaderBytes + mutationSize;
			ASSERT(self->bufferOffset + mutationTotalSize <= size);

			Standalone<StringRef> mutationData = makeString(mutationSize);
			std::memcpy(
			    mutateString(mutationData), start.get() + self->bufferOffset + self->mutationHeaderBytes, mutationSize);
			// transform from stringref to mutationref here
			ArenaReader reader(mutationData.arena(), mutationData, AssumeVersion(g_network->protocolVersion()));
			MutationRef mutation;
			reader >> mutation;

			VersionedMutation vm;
			vm.version = version;
			vm.subsequence = subsequence;
			vm.mutation = mutation;
			mutations.push_back_deep(mutations.arena(), vm);
			// Move the bufferOffset to include this mutation
			self->bufferOffset += mutationTotalSize;
		}

		if (self->bufferOffset < size && endOfBlock(start.get(), self->bufferOffset)) {
			// there are paddings, skip them
			int remain = self->BLOCK_SIZE - (self->bufferOffset % self->BLOCK_SIZE);
			self->bufferOffset += remain;
		}
		if (foundNewVersion) {
			break;
		}
	}
	co_return mutations;
}

void PartitionedLogIteratorTwoBuffers::removeBlockHeader() {
	if (bufferOffset % BLOCK_SIZE == 0) {
		bufferOffset += sizeof(uint32_t);
	}
}

PartitionedLogIteratorTwoBuffers::PartitionedLogIteratorTwoBuffers(Reference<IBackupContainer> _bc,
                                                                   int _tag,
                                                                   std::vector<RestoreConfig::RestoreFile> _files,
                                                                   std::vector<Version> _endVersions)
  : bc(_bc), tag(_tag), files(std::move(_files)), endVersions(_endVersions), bufferOffset(0) {
	int bufferCapacity = BATCH_READ_BLOCK_COUNT * BLOCK_SIZE;
	twobuffer = makeReference<TwoBuffers>(bufferCapacity, _bc, files, tag);
}

bool PartitionedLogIteratorTwoBuffers::hasNext() {
	return twobuffer->hasNext();
}

Future<Version> PartitionedLogIteratorTwoBuffers::peekNextVersion() {
	return peekNextVersion(Reference<PartitionedLogIteratorTwoBuffers>::addRef(this));
}
Future<Version> PartitionedLogIteratorTwoBuffers::peekNextVersion(Reference<PartitionedLogIteratorTwoBuffers> self) {
	// Read the first mutation's version
	std::shared_ptr<char[]> start;
	Version version{ 0 };
	int fileIndex{ 0 };
	if (!self->hasNext()) {
		co_return Version(0);
	}
	co_await self->twobuffer->ready();
	start = self->twobuffer->peek();
	self->removeBlockHeader();
	std::memcpy(&version, start.get() + self->bufferOffset, sizeof(Version));
	version = bigEndian64(version);
	fileIndex = self->twobuffer->getFileIndex();
	while (fileIndex < self->endVersions.size() - 1 && version >= self->endVersions[fileIndex]) {
		TraceEvent("RestoreLogFilesFoundOverlapAndSkip")
		    .detail("Version", version)
		    .detail("FileIndex", fileIndex)
		    .log();
		// need to read from next file in the case of overlap range versions between log files
		self->twobuffer->reset();
		self->bufferOffset = 0;
		self->twobuffer->setFileIndex(fileIndex + 1);
		co_await self->twobuffer->ready();
		start = self->twobuffer->peek();
		self->removeBlockHeader();
		std::memcpy(&version, start.get() + self->bufferOffset, sizeof(Version));
		version = bigEndian64(version);
		fileIndex = self->twobuffer->getFileIndex();
	}
	co_return version;
}

Future<Standalone<VectorRef<VersionedMutation>>> PartitionedLogIteratorTwoBuffers::getNext(
    Reference<PartitionedLogIteratorTwoBuffers> self) {
	Standalone<VectorRef<VersionedMutation>> mutations;
	if (!self->hasNext()) {
		TraceEvent(SevWarn, "IteratorExhausted").log();
		co_return mutations;
	}
	Version firstVersion = co_await self->peekNextVersion();

	Standalone<VectorRef<VersionedMutation>> firstBatch = co_await self->consumeData(firstVersion);
	mutations = firstBatch;
	// If the current buffer is fully consumed, then we need to check the next buffer in case
	// the version is sliced across this buffer boundary
	while (self->bufferOffset >= self->twobuffer->getBufferSize()) {
		self->twobuffer->discardAndSwap();
		self->bufferOffset = 0;
		// data for one version cannot exceed single buffer size
		// if hitting the end of a batch, check the next batch in case version is
		if (self->twobuffer->hasNext()) {
			// now this is run for each block, but it is not necessary if it is the last block of a file
			// cannot check hasMoreData here because other buffer might have the last piece
			Version nextVersion = co_await self->peekNextVersion();
			if (nextVersion != firstVersion) {
				break;
			}
			Standalone<VectorRef<VersionedMutation>> batch = co_await self->consumeData(firstVersion);
			for (const VersionedMutation& vm : batch) {
				mutations.push_back_deep(mutations.arena(), vm);
			}
		} else {
			break;
		}
	}
	co_return mutations;
}

Future<Standalone<VectorRef<VersionedMutation>>> PartitionedLogIteratorTwoBuffers::getNext() {
	return getNext(Reference<PartitionedLogIteratorTwoBuffers>::addRef(this));
}

FileBackupAgent::FileBackupAgent()
  : subspace(Subspace(fileBackupPrefixRange.begin))
    // The other subspaces have logUID -> value
    ,
    config(subspace.get(BackupAgentBase::keyConfig)), lastRestorable(subspace.get(FileBackupAgent::keyLastRestorable)),
    taskBucket(new TaskBucket(subspace.get(BackupAgentBase::keyTasks),
                              AccessSystemKeys::True,
                              PriorityBatch::False,
                              LockAware::True)),
    futureBucket(new FutureBucket(subspace.get(BackupAgentBase::keyFutures), AccessSystemKeys::True, LockAware::True)) {
}

namespace fileBackup {

// Return a block of contiguous padding bytes, growing if needed.
Value makePadding(int size) {
	static Value pad;
	if (pad.size() < size) {
		pad = makeString(size);
		memset(mutateString(pad), '\xff', pad.size());
	}

	return pad.substr(0, size);
}

struct IRangeFileWriter {
public:
	virtual Future<Void> padEnd(bool final) = 0;

	virtual Future<Void> writeKV(Key k, Value v) = 0;

	virtual Future<Void> writeKey(Key k) = 0;

	virtual Future<Void> finish() = 0;

	virtual ~IRangeFileWriter() = default;
};

// File Format handlers.
// Both Range and Log formats are designed to be readable starting at any BACKUP_RANGEFILE_BLOCK_SIZE boundary
// so they can be read in parallel.
//
// Writer instances must be kept alive while any member actors are in progress.
//
// RangeFileWriter must be used as follows:
//   1 - writeKey(key) the queried key range begin
//   2 - writeKV(k, v) each kv pair to restore
//   3 - writeKey(key) the queried key range end
//	 4 - finish()
//
// RangeFileWriter will insert the required padding, header, and extra
// end/begin keys around the 1MB boundaries as needed.
//
// Example:
//   The range a-z is queries and returns c-j which covers 3 blocks.
//   The client code writes keys in this sequence:
//             a c d e f g h i j z
//
//   H = header   P = padding   a...z = keys  v = value | = block boundary
//
//   Encoded file:  H a cv dv ev P | H e ev fv gv hv P | H h hv iv jv z
//   Decoded in blocks yields:
//           Block 1: range [a, e) with kv pairs cv, dv
//           Block 2: range [e, h) with kv pairs ev, fv, gv
//           Block 3: range [h, z) with kv pairs hv, iv, jv
//
//   NOTE: All blocks except for the final block will have one last
//   value which will not be used.  This isn't actually a waste since
//   if the next KV pair wouldn't fit within the block after the value
//   then the space after the final key to the next 1MB boundary would
//   just be padding anyway.
struct RangeFileWriter : public IRangeFileWriter {
	RangeFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0)
	  : file(file), blockSize(blockSize), blockEnd(0), fileVersion(BACKUP_AGENT_SNAPSHOT_FILE_VERSION) {}

	// Handles the first block and internal blocks.  Ends current block if needed.
	// The final flag is used in simulation to pad the file's final block to a whole block size
	static Future<Void> newBlock(RangeFileWriter* self, int bytesNeeded, bool final = false) {
		// Write padding to finish current block if needed
		int bytesLeft = self->blockEnd - self->file->size();
		if (bytesLeft > 0) {
			Value paddingFFs = makePadding(bytesLeft);
			co_await self->file->append(paddingFFs.begin(), bytesLeft);
		}

		if (final) {
			ASSERT(g_network->isSimulated());
			co_return;
		}

		// Set new blockEnd
		self->blockEnd += self->blockSize;

		// write Header
		co_await self->file->append((uint8_t*)&self->fileVersion, sizeof(self->fileVersion));

		// If this is NOT the first block then write duplicate stuff needed from last block
		if (self->blockEnd > self->blockSize) {
			co_await self->file->appendStringRefWithLen(self->lastKey);
			co_await self->file->appendStringRefWithLen(self->lastKey);
			co_await self->file->appendStringRefWithLen(self->lastValue);
		}

		// There must now be room in the current block for bytesNeeded or the block size is too small
		if (self->file->size() + bytesNeeded > self->blockEnd)
			throw backup_bad_block_size();

		co_return;
	}

	// Used in simulation only to create backup file sizes which are an integer multiple of the block size
	Future<Void> padEnd(bool final) override {
		ASSERT(g_network->isSimulated());
		if (file->size() > 0) {
			return newBlock(this, 0, final);
		}
		return Void();
	}

	// Ends the current block if necessary based on bytesNeeded.
	Future<Void> newBlockIfNeeded(int bytesNeeded) {
		if (file->size() + bytesNeeded > blockEnd)
			return newBlock(this, bytesNeeded);
		return Void();
	}

	// Start a new block if needed, then write the key and value
	static Future<Void> writeKV_impl(RangeFileWriter* self, Key k, Value v) {
		int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
		co_await self->newBlockIfNeeded(toWrite);
		co_await self->file->appendStringRefWithLen(k);
		co_await self->file->appendStringRefWithLen(v);
		self->lastKey = k;
		self->lastValue = v;
		co_return;
	}

	Future<Void> writeKV(Key k, Value v) override { return writeKV_impl(this, k, v); }

	// Write begin key or end key.
	static Future<Void> writeKey_impl(RangeFileWriter* self, Key k) {
		int toWrite = sizeof(uint32_t) + k.size();
		co_await self->newBlockIfNeeded(toWrite);
		co_await self->file->appendStringRefWithLen(k);
		co_return;
	}

	Future<Void> writeKey(Key k) override { return writeKey_impl(this, k); }

	Future<Void> finish() override { return Void(); }

	Reference<IBackupFile> file;
	int blockSize;

private:
	int64_t blockEnd;
	uint32_t fileVersion;
	Key lastKey;
	Key lastValue;
};

void decodeKVPairs(StringRefReader* reader, Standalone<VectorRef<KeyValueRef>>* results) {
	// Read begin key, if this fails then block was invalid.
	uint32_t kLen = reader->consumeNetworkUInt32();
	const uint8_t* k = reader->consume(kLen);
	results->push_back(results->arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
	KeyRef prevKey = KeyRef(k, kLen);
	// Read kv pairs and end key
	while (1) {
		// Read a key.
		kLen = reader->consumeNetworkUInt32();
		k = reader->consume(kLen);

		// If eof reached or first value len byte is 0xFF then a valid block end was reached.
		if (reader->eof() || *reader->rptr == 0xFF) {
			results->push_back(results->arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
			break;
		}

		// Read a value, which must exist or the block is invalid
		uint32_t vLen = reader->consumeNetworkUInt32();
		const uint8_t* v = reader->consume(vLen);

		results->push_back(results->arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));

		// If eof reached or first byte of next key len is 0xFF then a valid block end was reached.
		if (reader->eof() || *reader->rptr == 0xFF)
			break;
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader->remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();
}

static Reference<IBackupContainer> getBackupContainerWithProxy(Reference<IBackupContainer> _bc) {
	Reference<IBackupContainer> bc = IBackupContainer::openContainer(_bc->getURL(), fileBackupAgentProxy, {});
	return bc;
}

Standalone<VectorRef<KeyValueRef>> decodeRangeFileBlock(const Standalone<StringRef>& buf) {
	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());

	// Read header, currently only decoding BACKUP_AGENT_SNAPSHOT_FILE_VERSION
	if (reader.consume<int32_t>() != BACKUP_AGENT_SNAPSHOT_FILE_VERSION)
		throw restore_unsupported_file_version();

	// Read begin key, if this fails then block was invalid.
	uint32_t beginKeyLen = reader.consumeNetworkUInt32();
	const uint8_t* beginKey = reader.consume(beginKeyLen);
	results.push_back(results.arena(), KeyValueRef(KeyRef(beginKey, beginKeyLen), ValueRef()));

	// Read kv pairs and end key
	while (1) {
		// If eof reached or first value len byte is 0xFF then a valid block end was reached.
		if (reader.eof() || *reader.rptr == 0xFF) {
			break;
		}

		// Read a key, which must exist or the block is invalid
		uint32_t kLen = reader.consumeNetworkUInt32();
		const uint8_t* k = reader.consume(kLen);

		// If eof reached or first value len byte is 0xFF then a valid block end was reached.
		if (reader.eof() || *reader.rptr == 0xFF) {
			// The last block in the file, will have Read End key.
			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
			break;
		}

		// Read a value, which must exist or the block is invalid
		uint32_t vLen = reader.consumeNetworkUInt32();
		const uint8_t* v = reader.consume(vLen);
		results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader.remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();

	return results;
}

Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file,
                                                                int64_t offset,
                                                                int len,
                                                                Database cx) {
	Standalone<StringRef> buf = makeString(len);
	int rLen = co_await uncancellable(holdWhile(buf, file->read(mutateString(buf), len, offset)));
	if (rLen != len)
		throw restore_bad_read();

	simulateBlobFailure();

	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());
	Arena arena;
	try {
		int32_t file_version = reader.consume<int32_t>();
		if (file_version != BACKUP_AGENT_SNAPSHOT_FILE_VERSION) {
			throw restore_unsupported_file_version();
		}
		decodeKVPairs(&reader, &results);
		co_return results;
	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreDecodeRangeFileBlockFailed")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len)
		    .detail("ErrorRelativeOffset", reader.rptr - buf.begin())
		    .detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
		throw;
	}
}

// Very simple format compared to KeyRange files.
// Header, [Key, Value]... Key len
struct LogFileWriter {
	LogFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0)
	  : file(file), blockSize(blockSize), blockEnd(0) {}

	// Start a new block if needed, then write the key and value
	static Future<Void> writeKV_impl(LogFileWriter* self, Key k, Value v) {
		// If key and value do not fit in this block, end it and start a new one
		int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
		if (self->file->size() + toWrite > self->blockEnd) {
			// Write padding if needed
			int bytesLeft = self->blockEnd - self->file->size();
			if (bytesLeft > 0) {
				Value paddingFFs = makePadding(bytesLeft);
				co_await self->file->append(paddingFFs.begin(), bytesLeft);
			}

			// Set new blockEnd
			self->blockEnd += self->blockSize;

			// write the block header
			co_await self->file->append((uint8_t*)&BACKUP_AGENT_MLOG_VERSION, sizeof(BACKUP_AGENT_MLOG_VERSION));
		}

		co_await self->file->appendStringRefWithLen(k);
		co_await self->file->appendStringRefWithLen(v);

		// At this point we should be in whatever the current block is or the block size is too small
		if (self->file->size() > self->blockEnd)
			throw backup_bad_block_size();

		co_return;
	}

	Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

	Reference<IBackupFile> file;
	int blockSize;

private:
	int64_t blockEnd;
};

// input: a string of [param1, param2], [param1, param2] ..., [param1, param2]
// output: a vector of [param1, param2] after removing the length info
Standalone<VectorRef<KeyValueRef>> decodeMutationLogFileBlock(const Standalone<StringRef>& buf) {
	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());

	// Read header, currently only decoding version BACKUP_AGENT_MLOG_VERSION
	if (reader.consume<int32_t>() != BACKUP_AGENT_MLOG_VERSION)
		throw restore_unsupported_file_version();

	// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
	while (1) {
		// If eof reached or first key len bytes is 0xFF then end of block was reached.
		if (reader.eof() || *reader.rptr == 0xFF)
			break;

		// Read key and value.  If anything throws then there is a problem.
		uint32_t kLen = reader.consumeNetworkUInt32();
		const uint8_t* k = reader.consume(kLen);
		uint32_t vLen = reader.consumeNetworkUInt32();
		const uint8_t* v = reader.consume(vLen);

		results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader.remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();

	return results;
}

Future<Standalone<VectorRef<KeyValueRef>>> decodeMutationLogFileBlock(Reference<IAsyncFile> file,
                                                                      int64_t offset,
                                                                      int len) {
	Standalone<StringRef> buf = makeString(len);
	int rLen = co_await file->read(mutateString(buf), len, offset);
	if (rLen != len)
		throw restore_bad_read();

	try {
		co_return decodeMutationLogFileBlock(buf);
	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreCorruptLogFileBlock")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len);
		throw;
	}
}

Future<Void> checkTaskVersion(Database cx, Reference<Task> task, StringRef name, uint32_t version) {
	uint32_t taskVersion = task->getVersion();
	if (taskVersion > version) {
		Error err = task_invalid_version();

		TraceEvent(SevWarn, "BA_BackupRangeTaskFuncExecute")
		    .detail("TaskVersion", taskVersion)
		    .detail("Name", name)
		    .detail("Version", version);
		if (KeyBackedTaskConfig::TaskParams.uid().exists(task)) {
			std::string msg = format("%s task version `%lu' is greater than supported version `%lu'",
			                         task->params[Task::reservedTaskParamKeyType].toString().c_str(),
			                         (unsigned long)taskVersion,
			                         (unsigned long)version);
			co_await BackupConfig(task).logError(cx, err, msg);
		}

		throw err;
	}
}

static Future<Void> abortFiveZeroBackup(FileBackupAgent* backupAgent,
                                        Reference<ReadYourWritesTransaction> tr,
                                        std::string tagName) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	Subspace tagNames = backupAgent->subspace.get(BackupAgentBase::keyTagName);
	Optional<Value> uidStr = co_await tr->get(tagNames.pack(Key(tagName)));
	if (!uidStr.present()) {
		TraceEvent(SevWarn, "FileBackupAbortIncompatibleBackup_TagNotFound").detail("TagName", tagName.c_str());
		co_return;
	}
	UID uid = BinaryReader::fromStringRef<UID>(uidStr.get(), Unversioned());

	Subspace statusSpace = backupAgent->subspace.get(BackupAgentBase::keyStates).get(uid.toString());
	Subspace globalConfig = backupAgent->subspace.get(BackupAgentBase::keyConfig).get(uid.toString());
	Subspace newConfigSpace = uidPrefixKey("uid->config/"_sr.withPrefix(fileBackupPrefixRange.begin), uid);

	Optional<Value> statusStr = co_await tr->get(statusSpace.pack(FileBackupAgent::keyStateStatus));
	EBackupState status =
	    !statusStr.present() ? EBackupState::STATE_NEVERRAN : BackupAgentBase::getState(statusStr.get().toString());

	TraceEvent(SevInfo, "FileBackupAbortIncompatibleBackup")
	    .detail("TagName", tagName.c_str())
	    .detail("Status", BackupAgentBase::getStateText(status));

	// Clear the folder id to prevent future tasks from executing at all
	tr->clear(singleKeyRange(StringRef(globalConfig.pack(FileBackupAgent::keyFolderId))));

	// Clear the mutations logging config and data
	Key configPath = uidPrefixKey(logRangesRange.begin, uid);
	Key logsPath = uidPrefixKey(backupLogKeys.begin, uid);
	tr->clear(KeyRangeRef(configPath, strinc(configPath)));
	tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

	// Clear the new-style config space
	tr->clear(newConfigSpace.range());

	Key statusKey = StringRef(statusSpace.pack(FileBackupAgent::keyStateStatus));

	// Set old style state key to Aborted if it was Runnable
	if (backupAgent->isRunnable(status))
		tr->set(statusKey, StringRef(FileBackupAgent::getStateText(EBackupState::STATE_ABORTED)));
}

struct AbortFiveZeroBackupTask : TaskFuncBase {
	static StringRef name;
	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		FileBackupAgent backupAgent;
		std::string tagName = task->params[BackupAgentBase::keyConfigBackupTag].toString();

		TraceEvent(SevInfo, "FileBackupCancelOldTask")
		    .detail("Task", task->params[Task::reservedTaskParamKeyType])
		    .detail("TagName", tagName);
		co_await abortFiveZeroBackup(&backupAgent, tr, tagName);

		co_await taskBucket->finish(tr, task);
		co_return;
	}

	StringRef getName() const override {
		TraceEvent(SevError, "FileBackupError")
		    .detail("Cause", "AbortFiveZeroBackupTaskFunc::name() should never be called");
		ASSERT(false);
		return StringRef();
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Future<Void>(Void());
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef AbortFiveZeroBackupTask::name = "abort_legacy_backup"_sr;
REGISTER_TASKFUNC(AbortFiveZeroBackupTask);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_diff_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_log_range);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_range);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_restorable);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_finish_full_backup);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_finished_full_backup);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_start_full_backup);

static Future<Void> abortFiveOneBackup(FileBackupAgent* backupAgent,
                                       Reference<ReadYourWritesTransaction> tr,
                                       std::string tagName) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	KeyBackedTag tag = makeBackupTag(tagName);
	UidAndAbortedFlagT current = co_await tag.getOrThrow(tr, Snapshot::False, backup_unneeded());

	BackupConfig config(current.first);
	EBackupState status = co_await config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);

	if (!backupAgent->isRunnable(status)) {
		throw backup_unneeded();
	}

	TraceEvent(SevInfo, "FBA_AbortFileOneBackup")
	    .detail("TagName", tagName.c_str())
	    .detail("Status", BackupAgentBase::getStateText(status));

	// Cancel backup task through tag
	co_await tag.cancel(tr);

	Key configPath = uidPrefixKey(logRangesRange.begin, config.getUid());
	Key logsPath = uidPrefixKey(backupLogKeys.begin, config.getUid());

	tr->clear(KeyRangeRef(configPath, strinc(configPath)));
	tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

	config.stateEnum().set(tr, EBackupState::STATE_ABORTED);
}

struct AbortFiveOneBackupTask : TaskFuncBase {
	static StringRef name;
	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		FileBackupAgent backupAgent;
		BackupConfig config(task);
		std::string tagName = co_await config.tag().getOrThrow(tr);

		TraceEvent(SevInfo, "FileBackupCancelFiveOneTask")
		    .detail("Task", task->params[Task::reservedTaskParamKeyType])
		    .detail("TagName", tagName);
		co_await abortFiveOneBackup(&backupAgent, tr, tagName);

		co_await taskBucket->finish(tr, task);
	}

	StringRef getName() const override {
		TraceEvent(SevError, "FileBackupError")
		    .detail("Cause", "AbortFiveOneBackupTaskFunc::name() should never be called");
		ASSERT(false);
		return StringRef();
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Future<Void>(Void());
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef AbortFiveOneBackupTask::name = "abort_legacy_backup_5.2"_sr;
REGISTER_TASKFUNC(AbortFiveOneBackupTask);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_range);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_dispatch_ranges);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_erase_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_dispatch_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_finished);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_snapshot_manifest);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_start);

std::function<void(Reference<Task>)> NOP_SETUP_TASK_FN = [](Reference<Task> task) { /* NOP */ };
static Future<Key> addBackupTask(StringRef name,
                                 uint32_t version,
                                 Reference<ReadYourWritesTransaction> tr,
                                 Reference<TaskBucket> taskBucket,
                                 TaskCompletionKey completionKey,
                                 BackupConfig config,
                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
                                 std::function<void(Reference<Task>)> setupTaskFn = NOP_SETUP_TASK_FN,
                                 int priority = 0,
                                 SetValidation setValidation = SetValidation::True) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	Key doneKey = co_await completionKey.get(tr, taskBucket);
	Reference<Task> task(new Task(name, version, doneKey, priority));

	// Bind backup config to new task
	// allow this new task to find the config(keyspace) of the parent task
	co_await config.toTask(tr, task, setValidation);

	// Set task specific params
	setupTaskFn(task);

	if (!waitFor) {
		co_return taskBucket->addTask(tr, task);
	}
	co_await waitFor->onSetAddTask(tr, taskBucket, task);

	co_return "OnSetAddTask"_sr;
}

// Clears the backup ID from "backupStartedKey" to pause backup workers.
static Future<Void> clearBackupStartID(Reference<ReadYourWritesTransaction> tr, UID backupUid) {
	// If backup worker is not enabled, exit early.
	Optional<Value> started = co_await tr->get(backupStartedKey);
	std::vector<std::pair<UID, Version>> ids;
	if (started.present()) {
		ids = decodeBackupStartedValue(started.get());
	}
	auto it =
	    std::find_if(ids.begin(), ids.end(), [=](const std::pair<UID, Version>& p) { return p.first == backupUid; });
	if (it != ids.end()) {
		ids.erase(it);
	}

	if (ids.empty()) {
		TraceEvent("ClearBackup").detail("BackupID", backupUid);
		tr->clear(backupStartedKey);
	} else {
		tr->set(backupStartedKey, encodeBackupStartedValue(ids));
	}
}

// Backup and Restore taskFunc definitions will inherit from one of the following classes which
// servers to catch and log to the appropriate config any error that execute/finish didn't catch and log.
struct RestoreTaskFuncBase : TaskFuncBase {
	Future<Void> handleError(Database cx, Reference<Task> task, Error const& error) final {
		return RestoreConfig(task).logError(
		    cx,
		    error,
		    format("'%s' on '%s'", error.what(), task->params[Task::reservedTaskParamKeyType].printable().c_str()));
	}
	virtual std::string toString(Reference<Task> task) const { return ""; }
};

struct BackupTaskFuncBase : TaskFuncBase {
	Future<Void> handleError(Database cx, Reference<Task> task, Error const& error) final {
		return BackupConfig(task).logError(
		    cx,
		    error,
		    format("'%s' on '%s'", error.what(), task->params[Task::reservedTaskParamKeyType].printable().c_str()));
	}
	virtual std::string toString(Reference<Task> task) const { return ""; }
};

static Future<Standalone<VectorRef<KeyRef>>> getBlockOfShards(Reference<ReadYourWritesTransaction> tr,
                                                              Key beginKey,
                                                              Key endKey,
                                                              int limit) {

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Standalone<VectorRef<KeyRef>> results;
	RangeResult values = co_await tr->getRange(
	    KeyRangeRef(keyAfter(beginKey.withPrefix(keyServersPrefix)), endKey.withPrefix(keyServersPrefix)), limit);

	for (auto& s : values) {
		KeyRef k = s.key.removePrefix(keyServersPrefix);
		results.push_back_deep(results.arena(), k);
	}

	co_return results;
}

struct BackupRangeTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Key> beginKey() { return __FUNCTION__sr; }
		static TaskParam<Key> endKey() { return __FUNCTION__sr; }
		static TaskParam<bool> addBackupRangeTasks() { return __FUNCTION__sr; }
	} Params;

	std::string toString(Reference<Task> task) const override {
		return format("beginKey '%s' endKey '%s' addTasks %d",
		              Params.beginKey().get(task).printable().c_str(),
		              Params.endKey().get(task).printable().c_str(),
		              Params.addBackupRangeTasks().get(task));
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	// Finish (which flushes/syncs) the file, and then in a single transaction, make some range backup progress
	// durable. This means:
	//  - increment the backup config's range bytes written
	//  - update the range file map
	//  - update the task begin key
	//  - save/extend the task with the new params
	// Returns whether or not the caller should continue executing the task.
	static Future<bool> finishRangeFile(Reference<IBackupFile> file,
	                                    Database cx,
	                                    Reference<Task> task,
	                                    Reference<TaskBucket> taskBucket,
	                                    KeyRange range,
	                                    Version version) {
		co_await file->finish();

		TraceEvent("BackupRangeFileFinished")
		    .detail("BackupUID", BackupConfig(task).getUid())
		    .detail("FileName", file->getFileName())
		    .detail("FileSize", file->size())
		    .detail("RangeBegin", range.begin.printable())
		    .detail("RangeEnd", range.end.printable())
		    .detail("RangeEmpty", range.empty())
		    .detail("Version", version);

		// Ignore empty ranges.
		if (range.empty())
			co_return false;

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		BackupConfig backup(task);
		bool usedFile = false;

		// Avoid unnecessary conflict by prevent taskbucket's automatic timeout extension
		// because the following transaction loop extends and updates the task.
		co_await task->extendMutex.take();
		FlowLock::Releaser releaser(task->extendMutex, 1);

		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// Update the start key of the task so if this transaction completes but the task then fails
				// when it is restarted it will continue where this execution left off.
				Params.beginKey().set(task, range.end);

				// Save and extend the task with the new begin parameter
				Version newTimeout = co_await taskBucket->extendTimeout(tr, task, UpdateParams::True);

				// Update the range bytes written in the backup config
				backup.rangeBytesWritten().atomicOp(tr, file->size(), MutationRef::AddValue);
				backup.snapshotRangeFileCount().atomicOp(tr, 1, MutationRef::AddValue);

				// See if there is already a file for this key which has an earlier begin, update the map if not.
				Optional<BackupConfig::RangeSlice> s = co_await backup.snapshotRangeFileMap().get(tr, range.end);
				if (!s.present() || s.get().begin >= range.begin) {
					backup.snapshotRangeFileMap().set(
					    tr, range.end, { range.begin, version, file->getFileName(), file->size() });
					usedFile = true;
				}

				co_await tr->commit();
				task->timeoutVersion = newTimeout;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		co_return usedFile;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           int priority,
	                           Key begin,
	                           Key end,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
	                           Version scheduledVersion = invalidVersion) {
		Key key = co_await addBackupTask(
		    BackupRangeTaskFunc::name,
		    BackupRangeTaskFunc::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.beginKey().set(task, begin);
			    Params.endKey().set(task, end);
			    Params.addBackupRangeTasks().set(task, false);
			    if (scheduledVersion != invalidVersion)
				    ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
		    },
		    priority);
		co_return key;
	}

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

		co_await checkTaskVersion(cx, task, BackupRangeTaskFunc::name, BackupRangeTaskFunc::version);

		Key beginKey = Params.beginKey().get(task);
		Key endKey = Params.endKey().get(task);

		TraceEvent("FileBackupRangeStart")
		    .suppressFor(60)
		    .detail("BackupUID", BackupConfig(task).getUid())
		    .detail("BeginKey", Params.beginKey().get(task).printable())
		    .detail("EndKey", Params.endKey().get(task).printable())
		    .detail("TaskKey", task->key.printable());

		// When a key range task saves the last chunk of progress and then the executor dies, when the task
		// continues its beginKey and endKey will be equal but there is no work to be done.
		if (beginKey == endKey)
			co_return;

		// Find out if there is a shard boundary in(beginKey, endKey)
		Standalone<VectorRef<KeyRef>> keys = co_await runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return getBlockOfShards(tr, beginKey, endKey, 1); });
		if (!keys.empty()) {
			Params.addBackupRangeTasks().set(task, true);
			co_return;
		}

		// Read everything from beginKey to endKey, write it to an output file, run the output file processor, and
		// then set on_done. If we are still writing after X seconds, end the output file and insert a new
		// backup_range task for the remainder.
		Reference<IBackupFile> outFile;
		Version outVersion = invalidVersion;
		Key lastKey;

		// retrieve kvData
		PromiseStream<RangeResultWithVersion> results;

		Future<Void> rc = readCommitted(cx,
		                                results,
		                                lock,
		                                KeyRangeRef(beginKey, endKey),
		                                Terminator::True,
		                                AccessSystemKeys::True,
		                                LockAware::True,
		                                ReadLowPriority(CLIENT_KNOBS->BACKUP_READS_USE_LOW_PRIORITY));
		std::unique_ptr<IRangeFileWriter> rangeFile;
		BackupConfig backup(task);
		Arena arena;

		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);

		// Don't need to check keepRunning(task) here because we will do that while finishing each output file, but
		// if bc is false then clearly the backup is no longer in progress
		Reference<IBackupContainer> _bc = co_await backup.backupContainer().getD(cx.getReference());
		if (!_bc) {
			co_return;
		}
		Reference<IBackupContainer> bc = getBackupContainerWithProxy(_bc);
		bool done = false;
		int64_t nrKeys = 0;

		while (true) {
			RangeResultWithVersion values;
			try {
				RangeResultWithVersion _values = co_await results.getFuture();
				values = _values;
				lock->release(values.first.expectedSize());
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream)
					done = true;
				else
					throw;
			}

			// If we've seen a new read version OR hit the end of the stream, then if we were writing a file finish
			// it.
			if (values.second != outVersion || done) {
				if (outFile) {
					CODE_PROBE(outVersion != invalidVersion, "Backup range task wrote multiple versions");
					Key nextKey = done ? endKey : keyAfter(lastKey);
					co_await rangeFile->writeKey(nextKey);

					if (BUGGIFY) {
						co_await rangeFile->padEnd(true);
					}

					co_await rangeFile->finish();

					bool usedFile = co_await finishRangeFile(
					    outFile, cx, task, taskBucket, KeyRangeRef(beginKey, nextKey), outVersion);
					TraceEvent("FileBackupWroteRangeFile")
					    .suppressFor(60)
					    .detail("BackupUID", backup.getUid())
					    .detail("Size", outFile->size())
					    .detail("Keys", nrKeys)
					    .detail("ReadVersion", outVersion)
					    .detail("BeginKey", beginKey.printable())
					    .detail("EndKey", nextKey.printable())
					    .detail("AddedFileToMap", usedFile);

					nrKeys = 0;
					beginKey = nextKey;
				}

				if (done)
					co_return;

				// Start writing a new file after verifying this task should keep running as of a new read version
				// (which must be >= outVersion)
				outVersion = values.second;
				// block size must be at least large enough for 3 max size keys and 2 max size values + overhead so
				// 250k conservatively.
				int blockSize =
				    BUGGIFY ? deterministicRandom()->randomInt(250e3, 4e6) : CLIENT_KNOBS->BACKUP_RANGEFILE_BLOCK_SIZE;
				Version snapshotBeginVersion{ 0 };
				int64_t snapshotRangeFileCount{ 0 };

				Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				while (true) {
					Error err;
					try {
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);

						co_await (taskBucket->keepRunning(tr, task) &&
						          storeOrThrow(snapshotBeginVersion, backup.snapshotBeginVersion().get(tr)) &&
						          store(snapshotRangeFileCount, backup.snapshotRangeFileCount().getD(tr)));

						break;
					} catch (Error& e) {
						err = e;
					}
					co_await tr->onError(err);
				}

				Reference<IBackupFile> f =
				    co_await bc->writeRangeFile(snapshotBeginVersion, snapshotRangeFileCount, outVersion, blockSize);
				outFile = f;

				// Initialize range file writer and write begin key
				rangeFile = std::make_unique<RangeFileWriter>(outFile, blockSize);
				co_await rangeFile->writeKey(beginKey);
			}

			// write kvData to file, update lastKey and key count
			if (!values.first.empty()) {
				for (size_t i = 0; i < values.first.size(); ++i) {
					co_await rangeFile->writeKV(values.first[i].key, values.first[i].value);
				}
				lastKey = values.first.back().key;
				nrKeys += values.first.size();
			}
		}
	}

	static Future<Void> startBackupRangeInternal(Reference<ReadYourWritesTransaction> tr,
	                                             Reference<TaskBucket> taskBucket,
	                                             Reference<FutureBucket> futureBucket,
	                                             Reference<Task> task,
	                                             Reference<TaskFuture> onDone) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		Key nextKey = Params.beginKey().get(task);
		Key endKey = Params.endKey().get(task);

		Standalone<VectorRef<KeyRef>> keys =
		    co_await getBlockOfShards(tr, nextKey, endKey, CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT);

		std::vector<Future<Key>> addTaskVector;
		for (const auto& splitKey : keys) {
			if (nextKey != splitKey) {
				addTaskVector.push_back(addTask(
				    tr, taskBucket, task, task->getPriority(), nextKey, splitKey, TaskCompletionKey::joinWith(onDone)));
				TraceEvent("FileBackupRangeSplit")
				    .suppressFor(60)
				    .detail("BackupUID", BackupConfig(task).getUid())
				    .detail("BeginKey", Params.beginKey().get(task).printable())
				    .detail("EndKey", Params.endKey().get(task).printable())
				    .detail("SliceBeginKey", nextKey.printable())
				    .detail("SliceEndKey", splitKey.printable());
			}
			nextKey = splitKey;
		}

		co_await waitForAll(addTaskVector);

		if (nextKey != endKey) {
			// Add task to cover nextKey to the end, using the priority of the current task
			co_await addTask(tr,
			                 taskBucket,
			                 task,
			                 task->getPriority(),
			                 nextKey,
			                 endKey,
			                 TaskCompletionKey::joinWith(onDone),
			                 Reference<TaskFuture>(),
			                 task->getPriority());
		}

		co_return;
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		if (Params.addBackupRangeTasks().get(task)) {
			co_await startBackupRangeInternal(tr, taskBucket, futureBucket, task, taskFuture);
		} else {
			co_await taskFuture->set(tr, taskBucket);
		}

		co_await taskBucket->finish(tr, task);

		TraceEvent("FileBackupRangeFinish")
		    .suppressFor(60)
		    .detail("BackupUID", BackupConfig(task).getUid())
		    .detail("BeginKey", Params.beginKey().get(task).printable())
		    .detail("EndKey", Params.endKey().get(task).printable())
		    .detail("TaskKey", task->key.printable());

		co_return;
	}
};
StringRef BackupRangeTaskFunc::name = "file_backup_write_range_5.2"_sr;
REGISTER_TASKFUNC(BackupRangeTaskFunc);

struct BackupSnapshotDispatchTask : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		// Set by Execute, used by Finish
		static TaskParam<int64_t> shardsBehind() { return __FUNCTION__sr; }
		// Set by Execute, used by Finish
		static TaskParam<bool> snapshotFinished() { return __FUNCTION__sr; }
		// Set by Execute, used by Finish
		static TaskParam<Version> nextDispatchVersion() { return __FUNCTION__sr; }
	} Params;

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           int priority,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
	                           Version scheduledVersion = invalidVersion) {
		Key key = co_await addBackupTask(
		    name,
		    version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    if (scheduledVersion != invalidVersion)
				    ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
		    },
		    priority);
		co_return key;
	}

	enum DispatchState { SKIP = 0, DONE = 1, NOT_DONE_MIN = 2 };

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));
		co_await checkTaskVersion(cx, task, name, version);

		double startTime = timer();
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

		// The shard map will use 3 values classes.  Exactly SKIP, exactly DONE, then any number >= NOT_DONE_MIN
		// which will mean not done. This is to enable an efficient coalesce() call to squash adjacent ranges which
		// are not yet finished to enable efficiently finding random database shards which are not done.
		int notDoneSequence = NOT_DONE_MIN;
		KeyRangeMap<int> shardMap(notDoneSequence++);
		Key beginKey = allKeys.begin;

		// Read all shard boundaries and add them to the map
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Future<Standalone<VectorRef<KeyRef>>> shardBoundaries =
				    getBlockOfShards(tr, beginKey, allKeys.end, CLIENT_KNOBS->TOO_MANY);
				co_await (success(shardBoundaries) && taskBucket->keepRunning(tr, task));

				if (shardBoundaries.get().empty())
					break;

				for (auto& boundary : shardBoundaries.get()) {
					shardMap.rawInsert(boundary, notDoneSequence++);
				}

				beginKey = keyAfter(shardBoundaries.get().back());
				tr->reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// Read required stuff from backup config
		BackupConfig config(task);
		Version recentReadVersion{ 0 };
		Version snapshotBeginVersion{ 0 };
		Version snapshotTargetEndVersion{ 0 };
		int64_t snapshotIntervalSeconds{ 0 };
		Optional<Version> latestSnapshotEndVersion;
		std::vector<KeyRange> backupRanges;
		Optional<Key> snapshotBatchFutureKey;
		Reference<TaskFuture> snapshotBatchFuture;
		Optional<int64_t> snapshotBatchSize;

		tr->reset();
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				co_await (store(snapshotBeginVersion, config.snapshotBeginVersion().getOrThrow(tr)) &&
				          store(snapshotTargetEndVersion, config.snapshotTargetEndVersion().getOrThrow(tr)) &&
				          store(backupRanges, config.backupRanges().getOrThrow(tr)) &&
				          store(snapshotIntervalSeconds, config.snapshotIntervalSeconds().getOrThrow(tr))
				          // The next two parameters are optional
				          && store(snapshotBatchFutureKey, config.snapshotBatchFuture().get(tr)) &&
				          store(snapshotBatchSize, config.snapshotBatchSize().get(tr)) &&
				          store(latestSnapshotEndVersion, config.latestSnapshotEndVersion().get(tr)) &&
				          store(recentReadVersion, tr->getReadVersion()) && taskBucket->keepRunning(tr, task));

				// If the snapshot batch future key does not exist, this is the first execution of this dispatch
				// task so
				//    - create and set the snapshot batch future key
				//    - initialize the batch size to 0
				//    - initialize the target snapshot end version if it is not yet set
				//    - commit
				if (!snapshotBatchFutureKey.present()) {
					snapshotBatchFuture = futureBucket->future(tr);
					config.snapshotBatchFuture().set(tr, snapshotBatchFuture->pack());
					snapshotBatchSize = 0;
					config.snapshotBatchSize().set(tr, snapshotBatchSize.get());

					// The dispatch of this batch can take multiple separate executions if the executor fails
					// so store a completion key for the dispatch finish() to set when dispatching the batch is
					// done.
					TaskCompletionKey dispatchCompletionKey = TaskCompletionKey::joinWith(snapshotBatchFuture);
					// this is a bad hack - but flow doesn't work well with lambda functions and capturing
					// state variables...
					auto cfg = &config;
					auto tx = &tr;
					co_await map(dispatchCompletionKey.get(tr, taskBucket), [cfg, tx](Key const& k) {
						cfg->snapshotBatchDispatchDoneKey().set(*tx, k);
						return Void();
					});
					co_await tr->commit();
				} else {
					ASSERT(snapshotBatchSize.present());
					// Batch future key exists in the config so create future from it
					snapshotBatchFuture = makeReference<TaskFuture>(futureBucket, snapshotBatchFutureKey.get());
				}

				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// Read all dispatched ranges
		std::vector<std::pair<Key, bool>> dispatchBoundaries;
		tr->reset();
		beginKey = allKeys.begin;
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Future<BackupConfig::RangeDispatchMapT::RangeResultType> bounds =
				    config.snapshotRangeDispatchMap().getRange(
				        tr, beginKey, keyAfter(allKeys.end), CLIENT_KNOBS->TOO_MANY);
				co_await (success(bounds) && taskBucket->keepRunning(tr, task) &&
				          store(recentReadVersion, tr->getReadVersion()));

				if (!bounds.get().results.empty()) {
					dispatchBoundaries.reserve(dispatchBoundaries.size() + bounds.get().results.size());
					dispatchBoundaries.insert(
					    dispatchBoundaries.end(), bounds.get().results.begin(), bounds.get().results.end());
				}

				if (!bounds.get().more) {
					break;
				}

				beginKey = keyAfter(bounds.get().results.back().first);
				tr->reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// The next few sections involve combining the results above.  Yields are used after operations
		// that could have operated on many thousands of things and in loops which could have many
		// thousands of iterations.
		// Declare some common iterators which must be state vars and will be used multiple times.
		int i{ 0 };
		RangeMap<Key, int, KeyRangeRef>::iterator iShard;
		RangeMap<Key, int, KeyRangeRef>::iterator iShardEnd;

		// Set anything inside a dispatched range to DONE.
		// Also ensure that the boundary value are true, false, [true, false]...
		if (!dispatchBoundaries.empty()) {
			bool lastValue = false;
			Key lastKey;
			for (i = 0; i < dispatchBoundaries.size(); ++i) {
				const std::pair<Key, bool>& boundary = dispatchBoundaries[i];

				// Values must alternate
				ASSERT(boundary.second == !lastValue);

				// If this was the end of a dispatched range
				if (!boundary.second) {
					// Ensure that the dispatched boundaries exist AND set all shard ranges in the dispatched range
					// to DONE.
					RangeMap<Key, int, KeyRangeRef>::Ranges shardRanges =
					    shardMap.modify(KeyRangeRef(lastKey, boundary.first));
					iShard = shardRanges.begin();
					iShardEnd = shardRanges.end();
					for (; iShard != iShardEnd; ++iShard) {
						iShard->value() = DONE;
						co_await yield();
					}
				}
				lastValue = dispatchBoundaries[i].second;
				lastKey = dispatchBoundaries[i].first;

				co_await yield();
			}
			ASSERT(lastValue == false);
		}

		// Set anything outside the backup ranges to SKIP.  We can use insert() here instead of modify()
		// because it's OK to delete shard boundaries in the skipped ranges.
		if (!backupRanges.empty()) {
			shardMap.insert(KeyRangeRef(allKeys.begin, backupRanges.front().begin), SKIP);
			co_await yield();

			for (i = 0; i < backupRanges.size() - 1; ++i) {
				shardMap.insert(KeyRangeRef(backupRanges[i].end, backupRanges[i + 1].begin), SKIP);
				co_await yield();
			}

			shardMap.insert(KeyRangeRef(backupRanges.back().end, allKeys.end), SKIP);
			co_await yield();
		}

		int countShardsDone = 0;
		int countShardsNotDone = 0;

		// Scan through the shard map, counting the DONE and NOT_DONE shards.
		RangeMap<Key, int, KeyRangeRef>::Ranges shardRanges = shardMap.ranges();
		iShard = shardRanges.begin();
		iShardEnd = shardRanges.end();
		for (; iShard != iShardEnd; ++iShard) {
			if (iShard->value() == DONE) {
				++countShardsDone;
			} else if (iShard->value() >= NOT_DONE_MIN)
				++countShardsNotDone;

			co_await yield();
		}

		// Coalesce the shard map to make random selection below more efficient.
		shardMap.coalesce(allKeys);
		co_await yield();

		// In this context "all" refers to all of the shards relevant for this particular backup
		int countAllShards = countShardsDone + countShardsNotDone;

		// Log backup ranges and shard counts for debugging mode=BOTH issues
		TraceEvent("FileBackupSnapshotDispatchShardCount")
		    .detail("BackupUID", config.getUid())
		    .detail("BackupRangesCount", backupRanges.size())
		    .detail("FirstRangeBegin", backupRanges.empty() ? ""_sr : backupRanges.front().begin.printable())
		    .detail("FirstRangeEnd", backupRanges.empty() ? ""_sr : backupRanges.front().end.printable())
		    .detail("CountAllShards", countAllShards)
		    .detail("CountShardsDone", countShardsDone)
		    .detail("CountShardsNotDone", countShardsNotDone)
		    .detail("LatestSnapshotEndVersion", latestSnapshotEndVersion.orDefault(-1));

		// NOTE: Don't finish here even if countShardsNotDone == 0. We need to dispatch tasks first.
		// The completion check after dispatch (with dispatchedInThisIteration guard) prevents
		// finishing in the same iteration we dispatch the last tasks.
		if (countShardsNotDone == 0) {
			TraceEvent("FileBackupSnapshotDispatchAllDoneBeforeDispatch")
			    .detail("BackupUID", config.getUid())
			    .detail("Note", "Will check again after dispatch loop");
		}

		// Decide when the next snapshot dispatch should run.
		Version nextDispatchVersion{ 0 };

		// In simulation, use snapshot interval / 5 to ensure multiple dispatches run
		// Otherwise, use the knob for the number of seconds between snapshot dispatch tasks.
		if (g_network->isSimulated())
			nextDispatchVersion =
			    recentReadVersion + CLIENT_KNOBS->CORE_VERSIONSPERSECOND * (snapshotIntervalSeconds / 5.0);
		else
			nextDispatchVersion = recentReadVersion + CLIENT_KNOBS->CORE_VERSIONSPERSECOND *
			                                              CLIENT_KNOBS->BACKUP_SNAPSHOT_DISPATCH_INTERVAL_SEC;

		// If nextDispatchVersion is greater than snapshotTargetEndVersion (which could be in the past) then just
		// use the greater of recentReadVersion or snapshotTargetEndVersion.  Any range tasks created in this
		// dispatch will be scheduled at a random time between recentReadVersion and nextDispatchVersion, so
		// nextDispatchVersion shouldn't be less than recentReadVersion.
		if (nextDispatchVersion > snapshotTargetEndVersion)
			nextDispatchVersion = std::max(recentReadVersion, snapshotTargetEndVersion);

		Params.nextDispatchVersion().set(task, nextDispatchVersion);

		// Calculate number of shards that should be done before the next interval end
		// timeElapsed is between 0 and 1 and represents what portion of the shards we should have completed by now
		double timeElapsed;
		Version snapshotScheduledVersionInterval = snapshotTargetEndVersion - snapshotBeginVersion;
		if (snapshotTargetEndVersion > snapshotBeginVersion)
			timeElapsed = std::min(
			    1.0, (double)(nextDispatchVersion - snapshotBeginVersion) / (snapshotScheduledVersionInterval));
		else
			timeElapsed = 1.0;

		int countExpectedShardsDone = countAllShards * timeElapsed;
		int countShardsToDispatch = std::max<int>(0, countExpectedShardsDone - countShardsDone);

		// Calculate the number of shards that would have been dispatched by a normal (on-schedule)
		// BackupSnapshotDispatchTask given the dispatch window and the start and expected-end versions of the
		// current snapshot.
		int64_t dispatchWindow = nextDispatchVersion - recentReadVersion;

		// If the scheduled snapshot interval is 0 (such as for initial, as-fast-as-possible snapshot) then all
		// shards are considered late
		int countShardsExpectedPerNormalWindow;
		if (snapshotScheduledVersionInterval == 0) {
			countShardsExpectedPerNormalWindow = 0;
		} else {
			// A dispatchWindow of 0 means the target end version is <= now which also results in all shards being
			// considered late
			countShardsExpectedPerNormalWindow =
			    (double(dispatchWindow) / snapshotScheduledVersionInterval) * countAllShards;
		}

		// The number of shards 'behind' the snapshot is the count of how may additional shards beyond normal are
		// being dispatched, if any.
		int countShardsBehind =
		    std::max<int64_t>(0, countShardsToDispatch + snapshotBatchSize.get() - countShardsExpectedPerNormalWindow);
		Params.shardsBehind().set(task, countShardsBehind);

		TraceEvent("FileBackupSnapshotDispatchStats")
		    .detail("BackupUID", config.getUid())
		    .detail("AllShards", countAllShards)
		    .detail("ShardsDone", countShardsDone)
		    .detail("ShardsNotDone", countShardsNotDone)
		    .detail("ExpectedShardsDone", countExpectedShardsDone)
		    .detail("ShardsToDispatch", countShardsToDispatch)
		    .detail("ShardsBehind", countShardsBehind)
		    .detail("SnapshotBeginVersion", snapshotBeginVersion)
		    .detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
		    .detail("NextDispatchVersion", nextDispatchVersion)
		    .detail("CurrentVersion", recentReadVersion)
		    .detail("TimeElapsed", timeElapsed)
		    .detail("SnapshotIntervalSeconds", snapshotIntervalSeconds);

		// Track whether we dispatched any tasks in this iteration
		bool dispatchedInThisIteration = false;

		// Dispatch random shards to catch up to the expected progress
		while (countShardsToDispatch > 0) {
			// First select ranges to add
			std::vector<KeyRange> rangesToAdd;

			// Limit number of tasks added per transaction
			int taskBatchSize = BUGGIFY ? deterministicRandom()->randomInt(1, countShardsToDispatch + 1)
			                            : CLIENT_KNOBS->BACKUP_DISPATCH_ADDTASK_SIZE;
			int added = 0;

			while (countShardsToDispatch > 0 && added < taskBatchSize && shardMap.size() > 0) {
				// Get a random range.
				auto it = shardMap.randomRange();
				// Find a NOT_DONE range and add it to rangesToAdd
				while (1) {
					if (it->value() >= NOT_DONE_MIN) {
						rangesToAdd.push_back(it->range());
						it->value() = DONE;
						shardMap.coalesce(Key(it->begin()));
						++added;
						++countShardsDone;
						--countShardsToDispatch;
						--countShardsNotDone;
						break;
					}
					if (it->end() == shardMap.mapEnd)
						break;
					++it;
				}
			}

			int64_t oldBatchSize = snapshotBatchSize.get();
			int64_t newBatchSize = oldBatchSize + rangesToAdd.size();

			// Now add the selected ranges in a single transaction.
			tr->reset();
			while (true) {
				Error err;
				try {
					TraceEvent("FileBackupSnapshotDispatchAddingTasks")
					    .suppressFor(2)
					    .detail("TasksToAdd", rangesToAdd.size())
					    .detail("NewBatchSize", newBatchSize);

					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					// For each range, make sure it isn't set in the dispatched range map.
					std::vector<Future<Optional<bool>>> beginReads;
					std::vector<Future<Optional<bool>>> endReads;

					for (auto& range : rangesToAdd) {
						beginReads.push_back(config.snapshotRangeDispatchMap().get(tr, range.begin));
						endReads.push_back(config.snapshotRangeDispatchMap().get(tr, range.end));
					}

					co_await (store(snapshotBatchSize.get(), config.snapshotBatchSize().getOrThrow(tr)) &&
					          waitForAll(beginReads) && waitForAll(endReads) && taskBucket->keepRunning(tr, task));

					// Snapshot batch size should be either oldBatchSize or newBatchSize. If new, this transaction
					// is already done.
					if (snapshotBatchSize.get() == newBatchSize) {
						break;
					} else {
						ASSERT(snapshotBatchSize.get() == oldBatchSize);
						config.snapshotBatchSize().set(tr, newBatchSize);
						snapshotBatchSize = newBatchSize;
						config.snapshotDispatchLastShardsBehind().set(tr, Params.shardsBehind().get(task));
						config.snapshotDispatchLastVersion().set(tr, tr->getReadVersion().get());
					}

					std::vector<Future<Void>> addTaskFutures;

					for (i = 0; i < beginReads.size(); ++i) {
						KeyRange& range = rangesToAdd[i];

						// This loop might have made changes to begin or end boundaries in a prior
						// iteration.  If so, the updated values exist in the RYW cache so re-read both entries.
						Optional<bool> beginValue = config.snapshotRangeDispatchMap().get(tr, range.begin).get();
						Optional<bool> endValue = config.snapshotRangeDispatchMap().get(tr, range.end).get();

						ASSERT(!beginValue.present() || !endValue.present() || beginValue != endValue);

						// If begin is present, it must be a range end so value must be false
						// If end is present, it must be a range begin so value must be true
						if ((!beginValue.present() || !beginValue.get()) && (!endValue.present() || endValue.get())) {
							if (beginValue.present()) {
								config.snapshotRangeDispatchMap().erase(tr, range.begin);
							} else {
								config.snapshotRangeDispatchMap().set(tr, range.begin, true);
							}
							if (endValue.present()) {
								config.snapshotRangeDispatchMap().erase(tr, range.end);
							} else {
								config.snapshotRangeDispatchMap().set(tr, range.end, false);
							}

							Version scheduledVersion = invalidVersion;
							// If the next dispatch version is in the future, choose a random version at which to
							// start the new task.
							if (nextDispatchVersion > recentReadVersion)
								scheduledVersion = recentReadVersion + deterministicRandom()->random01() *
								                                           (nextDispatchVersion - recentReadVersion);

							// Range tasks during the initial snapshot should run at a higher priority
							int priority = latestSnapshotEndVersion.present() ? 0 : 1;
							addTaskFutures.push_back(
							    success(BackupRangeTaskFunc::addTask(tr,
							                                         taskBucket,
							                                         task,
							                                         priority,
							                                         range.begin,
							                                         range.end,
							                                         TaskCompletionKey::joinWith(snapshotBatchFuture),
							                                         Reference<TaskFuture>(),
							                                         scheduledVersion)));

							TraceEvent("FileBackupSnapshotRangeDispatched")
							    .suppressFor(2)
							    .detail("BackupUID", config.getUid())
							    .detail("CurrentVersion", recentReadVersion)
							    .detail("ScheduledVersion", scheduledVersion)
							    .detail("BeginKey", range.begin.printable())
							    .detail("EndKey", range.end.printable());
						} else {
							// This shouldn't happen because if the transaction was already done or if another
							// execution of this task is making progress it should have been detected above.
							ASSERT(false);
						}
					}

					co_await waitForAll(addTaskFutures);
					co_await tr->commit();
					dispatchedInThisIteration = true;
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}
		}

		// Only finish if all shards are done AND we didn't dispatch any tasks this iteration.
		// This prevents the bug where we mark snapshot finished immediately after dispatching
		// the last batch of tasks, before they actually complete.
		if (countShardsNotDone == 0 && !dispatchedInThisIteration) {
			TraceEvent("FileBackupSnapshotDispatchFinished")
			    .detail("BackupUID", config.getUid())
			    .detail("AllShards", countAllShards)
			    .detail("ShardsDone", countShardsDone)
			    .detail("ShardsNotDone", countShardsNotDone)
			    .detail("SnapshotBeginVersion", snapshotBeginVersion)
			    .detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
			    .detail("CurrentVersion", recentReadVersion)
			    .detail("SnapshotIntervalSeconds", snapshotIntervalSeconds)
			    .detail("DispatchTimeSeconds", timer() - startTime);
			Params.snapshotFinished().set(task, true);
		}

		co_return;
	}

	// This function is just a wrapper for BackupSnapshotManifest::addTask() which is defined below.
	// The BackupSnapshotDispatchTask and BackupSnapshotManifest tasks reference each other so in order to keep
	// their execute and finish phases defined together inside their class definitions this wrapper is declared here
	// but defined after BackupSnapshotManifest is defined.
	static Future<Key> addSnapshotManifestTask(Reference<ReadYourWritesTransaction> tr,
	                                           Reference<TaskBucket> taskBucket,
	                                           Reference<Task> parentTask,
	                                           TaskCompletionKey completionKey,
	                                           Reference<TaskFuture> waitFor = Reference<TaskFuture>());

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		BackupConfig config(task);

		// Get the batch future and dispatch done keys, then clear them.
		Key snapshotBatchFutureKey;
		Key snapshotBatchDispatchDoneKey;

		co_await (store(snapshotBatchFutureKey, config.snapshotBatchFuture().getOrThrow(tr)) &&
		          store(snapshotBatchDispatchDoneKey, config.snapshotBatchDispatchDoneKey().getOrThrow(tr)));

		Reference<TaskFuture> snapshotBatchFuture = futureBucket->unpack(snapshotBatchFutureKey);
		Reference<TaskFuture> snapshotBatchDispatchDoneFuture = futureBucket->unpack(snapshotBatchDispatchDoneKey);
		config.snapshotBatchFuture().clear(tr);
		config.snapshotBatchDispatchDoneKey().clear(tr);
		config.snapshotBatchSize().clear(tr);

		// Update shardsBehind here again in case the execute phase did not actually have to create any shard tasks
		config.snapshotDispatchLastShardsBehind().set(tr, Params.shardsBehind().getOrDefault(task, 0));
		config.snapshotDispatchLastVersion().set(tr, tr->getReadVersion().get());

		Reference<TaskFuture> snapshotFinishedFuture = task->getDoneFuture(futureBucket);

		bool snapshotFinished = Params.snapshotFinished().getOrDefault(task, false);
		TraceEvent("FileBackupSnapshotDispatchFinish")
		    .detail("BackupUID", config.getUid())
		    .detail("SnapshotFinished", snapshotFinished)
		    .detail("ShardsBehind", Params.shardsBehind().getOrDefault(task, 0))
		    .detail("NextDispatchVersion", Params.nextDispatchVersion().getOrDefault(task, -1));

		// If the snapshot is finished, the next task is to write a snapshot manifest, otherwise it's another
		// snapshot dispatch task. In either case, the task should wait for snapshotBatchFuture. The snapshot done
		// key, passed to the current task, is also passed on.
		if (snapshotFinished) {
			TraceEvent("FileBackupSnapshotDispatchAddingManifestTask").detail("BackupUID", config.getUid());
			co_await addSnapshotManifestTask(
			    tr, taskBucket, task, TaskCompletionKey::signal(snapshotFinishedFuture), snapshotBatchFuture);
		} else {
			co_await addTask(tr,
			                 taskBucket,
			                 task,
			                 1,
			                 TaskCompletionKey::signal(snapshotFinishedFuture),
			                 snapshotBatchFuture,
			                 Params.nextDispatchVersion().get(task));
		}

		// This snapshot batch is finished, so set the batch done future.
		co_await snapshotBatchDispatchDoneFuture->set(tr, taskBucket);

		co_await taskBucket->finish(tr, task);

		co_return;
	}
};
StringRef BackupSnapshotDispatchTask::name = "file_backup_dispatch_ranges_5.2"_sr;
REGISTER_TASKFUNC(BackupSnapshotDispatchTask);

struct BackupLogRangeTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<bool> addBackupLogRangeTasks() { return __FUNCTION__sr; }
		static TaskParam<int64_t> fileSize() { return __FUNCTION__sr; }
		static TaskParam<Version> beginVersion() { return __FUNCTION__sr; }
		static TaskParam<Version> endVersion() { return __FUNCTION__sr; }
	} Params;

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

		co_await checkTaskVersion(cx, task, BackupLogRangeTaskFunc::name, BackupLogRangeTaskFunc::version);

		Version beginVersion = Params.beginVersion().get(task);
		Version endVersion = Params.endVersion().get(task);

		BackupConfig config(task);
		Reference<IBackupContainer> bc;

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		while (true) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			// Wait for the read version to pass endVersion
			{
				Error err;
				try {
					co_await taskBucket->keepRunning(tr, task);

					if (!bc) {
						// Backup container must be present if we're still here
						Reference<IBackupContainer> _bc = co_await config.backupContainer().getOrThrow(tr);
						bc = getBackupContainerWithProxy(_bc);
					}

					Version currentVersion = tr->getReadVersion().get();
					if (endVersion < currentVersion)
						break;

					co_await delay(
					    std::max(CLIENT_KNOBS->BACKUP_RANGE_MINWAIT,
					             (double)(endVersion - currentVersion) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
					tr->reset();
					continue;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}
		}

		Key destUidValue = co_await config.destUidValue().getOrThrow(tr);

		// Get the set of key ranges that hold mutations for (beginVersion, endVersion).  They will be queried in
		// parallel below and there is a limit on how many we want to process in a single BackupLogRangeTask so if
		// that limit is exceeded then set the addBackupLogRangeTasks boolean in Params and stop, signalling the
		// finish() step to break up the (beginVersion, endVersion) range into smaller intervals which are then
		// processed by individual BackupLogRangeTasks.
		Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(beginVersion, endVersion, destUidValue);
		if (ranges.size() > CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
			Params.addBackupLogRangeTasks().set(task, true);
			co_return;
		}

		// Block size must be at least large enough for 1 max size key, 1 max size value, and overhead, so
		// conservatively 125k.
		int blockSize =
		    BUGGIFY ? deterministicRandom()->randomInt(125e3, 4e6) : CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE;
		Reference<IBackupFile> outFile = co_await bc->writeLogFile(beginVersion, endVersion, blockSize);
		LogFileWriter logFile(outFile, blockSize);

		// Query all key ranges covering (beginVersion, endVersion) in parallel, writing their results to the
		// results promise stream as they are received.  Note that this means the records read from the results
		// stream are not likely to be in increasing Version order.
		PromiseStream<RangeResultWithVersion> results;
		std::vector<Future<Void>> rc;

		for (auto& range : ranges) {
			rc.push_back(readCommitted(cx,
			                           results,
			                           lock,
			                           range,
			                           Terminator::False,
			                           AccessSystemKeys::True,
			                           LockAware::True,
			                           ReadLowPriority(CLIENT_KNOBS->BACKUP_READS_USE_LOW_PRIORITY)));
		}

		Future<Void> sendEOS = map(errorOr(waitForAll(rc)), [=](ErrorOr<Void> const& result) mutable {
			if (result.isError())
				results.sendError(result.getError());
			else
				results.sendError(end_of_stream());
			return Void();
		});

		Version lastVersion{ 0 };
		{
			Error caughtErr;
			bool hasCaughtErr = false;
			try {
				while (true) {
					RangeResultWithVersion r = co_await results.getFuture();
					lock->release(r.first.expectedSize());

					for (int i = 0; i < r.first.size(); ++i) {
						// Remove the backupLogPrefix + UID bytes from the key
						co_await logFile.writeKV(r.first[i].key.substr(backupLogPrefixBytes + 16), r.first[i].value);
						lastVersion = r.second;
					}
				}
			} catch (Error& e) {
				caughtErr = e;
				hasCaughtErr = true;
			}
			if (hasCaughtErr) {
				if (caughtErr.code() == error_code_actor_cancelled)
					throw caughtErr;

				if (caughtErr.code() != error_code_end_of_stream) {
					Error err = caughtErr;
					co_await config.logError(
					    cx, err, format("Failed to write to file `%s'", outFile->getFileName().c_str()));
					throw err;
				}
			}
		}

		// Make sure this task is still alive, if it's not then the data read above could be incomplete.
		co_await taskBucket->keepRunning(cx, task);

		co_await outFile->finish();

		TraceEvent("FileBackupWroteLogFile")
		    .suppressFor(60)
		    .detail("BackupUID", config.getUid())
		    .detail("Size", outFile->size())
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion)
		    .detail("LastReadVersion", lastVersion);

		Params.fileSize().set(task, outFile->size());

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           int priority,
	                           Version beginVersion,
	                           Version endVersion,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = co_await addBackupTask(
		    BackupLogRangeTaskFunc::name,
		    BackupLogRangeTaskFunc::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.beginVersion().set(task, beginVersion);
			    Params.endVersion().set(task, endVersion);
			    Params.addBackupLogRangeTasks().set(task, false);
		    },
		    priority);
		co_return key;
	}

	static Future<Void> startBackupLogRangeInternal(Reference<ReadYourWritesTransaction> tr,
	                                                Reference<TaskBucket> taskBucket,
	                                                Reference<FutureBucket> futureBucket,
	                                                Reference<Task> task,
	                                                Reference<TaskFuture> taskFuture,
	                                                Version beginVersion,
	                                                Version endVersion) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		std::vector<Future<Key>> addTaskVector;
		int tasks = 0;
		for (int64_t vblock = beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		     vblock < (endVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		     vblock += CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
			Version bv = std::max(beginVersion, vblock * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

			if (tasks >= CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT) {
				addTaskVector.push_back(addTask(tr,
				                                taskBucket,
				                                task,
				                                task->getPriority(),
				                                bv,
				                                endVersion,
				                                TaskCompletionKey::joinWith(taskFuture)));
				break;
			}

			Version ev = std::min(endVersion,
			                      (vblock + CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
			addTaskVector.push_back(
			    addTask(tr, taskBucket, task, task->getPriority(), bv, ev, TaskCompletionKey::joinWith(taskFuture)));
			tasks++;
		}

		co_await waitForAll(addTaskVector);

		co_return;
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		Version beginVersion = Params.beginVersion().get(task);
		Version endVersion = Params.endVersion().get(task);
		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		BackupConfig config(task);

		if (Params.fileSize().exists(task)) {
			config.logBytesWritten().atomicOp(tr, Params.fileSize().get(task), MutationRef::AddValue);
		}

		if (Params.addBackupLogRangeTasks().get(task)) {
			co_await startBackupLogRangeInternal(
			    tr, taskBucket, futureBucket, task, taskFuture, beginVersion, endVersion);
		} else {
			co_await taskFuture->set(tr, taskBucket);
		}

		co_await taskBucket->finish(tr, task);
		co_return;
	}
};

StringRef BackupLogRangeTaskFunc::name = "file_backup_write_logs_5.2"_sr;
REGISTER_TASKFUNC(BackupLogRangeTaskFunc);

// This task stopped being used in 6.2, however the code remains here to handle upgrades.
struct EraseLogRangeTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct {
		static TaskParam<Version> beginVersion() { return __FUNCTION__sr; }
		static TaskParam<Version> endVersion() { return __FUNCTION__sr; }
		static TaskParam<Key> destUidValue() { return __FUNCTION__sr; }
	} Params;

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           UID logUid,
	                           TaskCompletionKey completionKey,
	                           Key destUidValue,
	                           Version endVersion = 0,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = co_await addBackupTask(
		    EraseLogRangeTaskFunc::name,
		    EraseLogRangeTaskFunc::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(logUid),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.beginVersion().set(task,
			                              1); // FIXME: remove in 6.X, only needed for 5.2 backward compatibility
			    Params.endVersion().set(task, endVersion);
			    Params.destUidValue().set(task, destUidValue);
		    },
		    0,
		    SetValidation::False);

		co_return key;
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		co_await checkTaskVersion(tr->getDatabase(), task, EraseLogRangeTaskFunc::name, EraseLogRangeTaskFunc::version);

		Version endVersion = Params.endVersion().get(task);
		Key destUidValue = Params.destUidValue().get(task);

		BackupConfig config(task);
		Key logUidValue = config.getUidAsKey();

		co_await (
		    taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task) &&
		    eraseLogData(
		        tr, logUidValue, destUidValue, endVersion != 0 ? Optional<Version>(endVersion) : Optional<Version>()));

		co_return;
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef EraseLogRangeTaskFunc::name = "file_backup_erase_logs_5.2"_sr;
REGISTER_TASKFUNC(EraseLogRangeTaskFunc);

struct BackupLogsDispatchTask : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> prevBeginVersion() { return __FUNCTION__sr; }
		static TaskParam<Version> beginVersion() { return __FUNCTION__sr; }
	} Params;

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		co_await checkTaskVersion(
		    tr->getDatabase(), task, BackupLogsDispatchTask::name, BackupLogsDispatchTask::version);

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		Reference<TaskFuture> onDone = task->getDoneFuture(futureBucket);
		Version prevBeginVersion = Params.prevBeginVersion().get(task);
		Version beginVersion = Params.beginVersion().get(task);
		BackupConfig config(task);
		config.latestLogEndVersion().set(tr, beginVersion);

		bool stopWhenDone{ false };
		Optional<Version> restorableVersion;
		EBackupState backupState;
		Optional<std::string> tag;
		Optional<Version> latestSnapshotEndVersion;
		Optional<bool> partitionedLog;

		co_await (store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)) &&
		          store(restorableVersion, config.getLatestRestorableVersion(tr)) &&
		          store(backupState, config.stateEnum().getOrThrow(tr)) && store(tag, config.tag().get(tr)) &&
		          store(latestSnapshotEndVersion, config.latestSnapshotEndVersion().get(tr)) &&
		          store(partitionedLog, config.partitionedLogEnabled().get(tr)));

		// If restorable, update the last restorable version for this tag
		if (restorableVersion.present() && tag.present()) {
			FileBackupAgent().setLastRestorable(tr, StringRef(tag.get()), restorableVersion.get());
		}

		// If the backup is restorable but the state is not differential then set state to differential
		if (restorableVersion.present() && backupState != EBackupState::STATE_RUNNING_DIFFERENTIAL)
			config.stateEnum().set(tr, EBackupState::STATE_RUNNING_DIFFERENTIAL);

		// If stopWhenDone is set and there is a restorable version, set the done future and do not create further
		// tasks.
		if (stopWhenDone && restorableVersion.present()) {
			co_await (onDone->set(tr, taskBucket) && taskBucket->finish(tr, task));

			TraceEvent("FileBackupLogsDispatchDone")
			    .detail("BackupUID", config.getUid())
			    .detail("BeginVersion", beginVersion)
			    .detail("RestorableVersion", restorableVersion.orDefault(-1));

			co_return;
		}

		Version endVersion = std::max<Version>(tr->getReadVersion().get() + 1,
		                                       beginVersion + (CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES - 1) *
		                                                          CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

		TraceEvent("FileBackupLogDispatch")
		    .suppressFor(60)
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion)
		    .detail("RestorableVersion", restorableVersion.orDefault(-1));

		Reference<TaskFuture> logDispatchBatchFuture = futureBucket->future(tr);

		// If a snapshot has ended for this backup then mutations are higher priority to reduce backup lag
		int priority = latestSnapshotEndVersion.present() ? 1 : 0;

		if (!partitionedLog.present() || !partitionedLog.get()) {
			// Add the initial log range task to read/copy the mutations and the next logs dispatch task which will
			// run after this batch is done
			// read blog/ prefix and write those (param1, param2) into files
			co_await BackupLogRangeTaskFunc::addTask(tr,
			                                         taskBucket,
			                                         task,
			                                         priority,
			                                         beginVersion,
			                                         endVersion,
			                                         TaskCompletionKey::joinWith(logDispatchBatchFuture));
			// issue the next key range
			co_await BackupLogsDispatchTask::addTask(tr,
			                                         taskBucket,
			                                         task,
			                                         priority,
			                                         beginVersion,
			                                         endVersion,
			                                         TaskCompletionKey::signal(onDone),
			                                         logDispatchBatchFuture);

			// Do not erase at the first time
			if (prevBeginVersion > 0) {
				Key destUidValue = co_await config.destUidValue().getOrThrow(tr);
				co_await eraseLogData(tr, config.getUidAsKey(), destUidValue, Optional<Version>(beginVersion));
			}
		} else {
			// Skip mutation copy and erase backup mutations. Just check back periodically.
			Version scheduledVersion = tr->getReadVersion().get() +
			                           CLIENT_KNOBS->BACKUP_POLL_PROGRESS_SECONDS * CLIENT_KNOBS->VERSIONS_PER_SECOND;
			co_await BackupLogsDispatchTask::addTask(tr,
			                                         taskBucket,
			                                         task,
			                                         1,
			                                         beginVersion,
			                                         endVersion,
			                                         TaskCompletionKey::signal(onDone),
			                                         Reference<TaskFuture>(),
			                                         scheduledVersion);
		}

		co_await taskBucket->finish(tr, task);

		TraceEvent("FileBackupLogsDispatchContinuing")
		    .suppressFor(60)
		    .detail("BackupUID", config.getUid())
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion);

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           int priority,
	                           Version prevBeginVersion,
	                           Version beginVersion,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
	                           Version scheduledVersion = invalidVersion) {
		Key key = co_await addBackupTask(
		    BackupLogsDispatchTask::name,
		    BackupLogsDispatchTask::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.prevBeginVersion().set(task, prevBeginVersion);
			    Params.beginVersion().set(task, beginVersion);
			    if (scheduledVersion != invalidVersion) {
				    ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
			    }
		    },
		    priority);
		co_return key;
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef BackupLogsDispatchTask::name = "file_backup_dispatch_logs_5.2"_sr;
REGISTER_TASKFUNC(BackupLogsDispatchTask);

struct FileBackupFinishedTask : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	StringRef getName() const override { return name; };

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		co_await checkTaskVersion(
		    tr->getDatabase(), task, FileBackupFinishedTask::name, FileBackupFinishedTask::version);

		BackupConfig backup(task);
		UID uid = backup.getUid();

		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
		Key destUidValue = co_await backup.destUidValue().getOrThrow(tr);

		co_await (eraseLogData(tr, backup.getUidAsKey(), destUidValue) && clearBackupStartID(tr, uid));

		backup.stateEnum().set(tr, EBackupState::STATE_COMPLETED);

		co_await taskBucket->finish(tr, task);

		TraceEvent("FileBackupFinished").detail("BackupUID", uid);

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = co_await addBackupTask(FileBackupFinishedTask::name,
		                                 FileBackupFinishedTask::version,
		                                 tr,
		                                 taskBucket,
		                                 completionKey,
		                                 BackupConfig(parentTask),
		                                 waitFor);
		co_return key;
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef FileBackupFinishedTask::name = "file_backup_finished_5.2"_sr;
REGISTER_TASKFUNC(FileBackupFinishedTask);

struct BackupSnapshotManifest : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	static struct {
		static TaskParam<Version> endVersion() { return __FUNCTION__sr; }
		static TaskParam<int64_t> totalBytes() { return __FUNCTION__sr; }
	} Params;

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		BackupConfig config(task);
		Reference<IBackupContainer> bc;
		DatabaseConfiguration dbConfig;

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

		// Read the entire range file map into memory, then walk it backwards from its last entry to produce a list
		// of non overlapping key range files
		std::map<Key, BackupConfig::RangeSlice> localmap;
		Key startKey;
		int batchSize = BUGGIFY ? 1 : 1000000;

		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				co_await taskBucket->keepRunning(tr, task);
				dbConfig = co_await getDatabaseConfiguration(cx);

				if (!bc) {
					// Backup container must be present if we're still here
					Reference<IBackupContainer> _bc = co_await config.backupContainer().getOrThrow(tr);
					bc = getBackupContainerWithProxy(_bc);
				}

				BackupConfig::RangeFileMapT::RangeResultType rangeresults =
				    co_await config.snapshotRangeFileMap().getRange(tr, startKey, {}, batchSize);

				for (auto& p : rangeresults.results) {
					localmap.insert(p);
				}

				if (!rangeresults.more) {
					break;
				}

				startKey = keyAfter(rangeresults.results.back().first);
				tr->reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		std::vector<std::string> files;
		std::vector<std::pair<Key, Key>> beginEndKeys;
		Version maxVer = 0;
		Version minVer = std::numeric_limits<Version>::max();
		int64_t totalBytes = 0;

		if (!localmap.empty()) {
			// Get iterator that points to greatest key, start there.
			auto ri = localmap.rbegin();
			auto i = (++ri).base();

			while (1) {
				const BackupConfig::RangeSlice& r = i->second;

				// Add file to final file list
				files.push_back(r.fileName);

				// Add (beginKey, endKey) pairs to the list
				beginEndKeys.emplace_back(i->second.begin, i->first);

				// Update version range seen
				if (r.version < minVer)
					minVer = r.version;
				if (r.version > maxVer)
					maxVer = r.version;

				// Update total bytes counted.
				totalBytes += r.fileSize;

				// Jump to file that either ends where this file begins or has the greatest end that is less than
				// the begin of this file.  In other words find the map key that is <= begin of this file.  To do
				// this find the first end strictly greater than begin and then back up one.
				i = localmap.upper_bound(i->second.begin);
				// If we get begin then we're done, there are no more ranges that end at or before the last file's
				// begin
				if (i == localmap.begin())
					break;
				--i;
			}
		}

		// Log what range files were found for debugging mode=BOTH issues
		TraceEvent("BackupSnapshotManifestRangeFileSummary")
		    .detail("BackupUID", config.getUid())
		    .detail("LocalMapSize", localmap.size())
		    .detail("FilesFound", files.size())
		    .detail("TotalBytes", totalBytes)
		    .detail("MinVersion", minVer == std::numeric_limits<Version>::max() ? -1 : minVer)
		    .detail("MaxVersion", maxVer);

		Params.endVersion().set(task, maxVer);
		Params.totalBytes().set(task, totalBytes);

		// Avoid keyRange filtering optimization for 'manifest' files
		co_await bc->writeKeyspaceSnapshotFile(files, beginEndKeys, totalBytes, IncludeKeyRangeMap::True);

		TraceEvent(SevInfo, "FileBackupWroteSnapshotManifest")
		    .detail("BackupUID", config.getUid())
		    .detail("BeginVersion", minVer)
		    .detail("EndVersion", maxVer)
		    .detail("TotalBytes", totalBytes);

		co_return;
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		co_await checkTaskVersion(
		    tr->getDatabase(), task, BackupSnapshotManifest::name, BackupSnapshotManifest::version);

		BackupConfig config(task);

		// Set the latest snapshot end version, which was set during the execute phase
		config.latestSnapshotEndVersion().set(tr, Params.endVersion().get(task));

		bool stopWhenDone{ false };
		EBackupState backupState;
		Optional<Version> restorableVersion;
		Optional<Version> firstSnapshotEndVersion;
		Optional<std::string> tag;

		co_await (store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)) &&
		          store(backupState, config.stateEnum().getOrThrow(tr)) &&
		          store(restorableVersion, config.getLatestRestorableVersion(tr)) &&
		          store(firstSnapshotEndVersion, config.firstSnapshotEndVersion().get(tr)) &&
		          store(tag, config.tag().get(tr)));

		// If restorable, update the last restorable version for this tag
		if (restorableVersion.present() && tag.present()) {
			FileBackupAgent().setLastRestorable(tr, StringRef(tag.get()), restorableVersion.get());
		}

		// Always set firstSnapshotEndVersion if not already set
		// This is required for getLatestRestorableVersion() to work correctly
		if (!firstSnapshotEndVersion.present()) {
			config.firstSnapshotEndVersion().set(tr, Params.endVersion().get(task));
		}

		// If the backup is restorable and the state isn't differential the set state to differential
		if (restorableVersion.present() && backupState != EBackupState::STATE_RUNNING_DIFFERENTIAL)
			config.stateEnum().set(tr, EBackupState::STATE_RUNNING_DIFFERENTIAL);

		// Unless we are to stop, start the next snapshot using the default interval
		Reference<TaskFuture> snapshotDoneFuture = task->getDoneFuture(futureBucket);
		if (!stopWhenDone) {
			co_await (config.initNewSnapshot(tr) &&
			          success(BackupSnapshotDispatchTask::addTask(
			              tr, taskBucket, task, 1, TaskCompletionKey::signal(snapshotDoneFuture))));
		} else {
			// Set the done future as the snapshot is now complete.
			co_await snapshotDoneFuture->set(tr, taskBucket);
		}

		co_await taskBucket->finish(tr, task);
		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = co_await addBackupTask(BackupSnapshotManifest::name,
		                                 BackupSnapshotManifest::version,
		                                 tr,
		                                 taskBucket,
		                                 completionKey,
		                                 BackupConfig(parentTask),
		                                 waitFor,
		                                 NOP_SETUP_TASK_FN,
		                                 1);
		co_return key;
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef BackupSnapshotManifest::name = "file_backup_write_snapshot_manifest_5.2"_sr;
REGISTER_TASKFUNC(BackupSnapshotManifest);

Future<Key> BackupSnapshotDispatchTask::addSnapshotManifestTask(Reference<ReadYourWritesTransaction> tr,
                                                                Reference<TaskBucket> taskBucket,
                                                                Reference<Task> parentTask,
                                                                TaskCompletionKey completionKey,
                                                                Reference<TaskFuture> waitFor) {
	return BackupSnapshotManifest::addTask(tr, taskBucket, parentTask, completionKey, waitFor);
}

// BulkDumpTaskFunc: Creates BulkDump snapshots during backup
// Must be defined before StartFullBackupTaskFunc which references it
struct BulkDumpTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> snapshotVersion() { return __FUNCTION__sr; }
		static TaskParam<std::string> bulkDumpJobId() { return __FUNCTION__sr; }
		static TaskParam<bool> timeoutOccurred() { return __FUNCTION__sr; }
	} Params;

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		BackupConfig config(task);
		Version snapshotVersion = Params.snapshotVersion().get(task);
		std::string jobId = Params.bulkDumpJobId().getOrDefault(task, "");

		TraceEvent("BulkDumpTaskStart")
		    .detail("BackupUID", config.getUid())
		    .detail("SnapshotVersion", snapshotVersion)
		    .detail("BulkDumpJobId", jobId);

		// Declare state variables before try block so they're accessible in catch block
		std::vector<KeyRange> backupRanges;
		Reference<IBackupContainer> bc;
		int originalBulkDumpMode = 0;
		BulkLoadTransportMethod transportMethod = BulkLoadTransportMethod::CP;
		BulkDumpState bulkDumpJob;
		bool jobAlreadyRunning = false;

		{
			Error savedError;
			try {
				// Submit BulkDump job via ManagementAPI
				// This is a black box delegation to the existing BulkDump system

				// Get backup ranges from config
				std::vector<KeyRange> ranges = co_await config.backupRanges().getOrThrow(cx.getReference());
				backupRanges = ranges;
				Reference<IBackupContainer> container = co_await config.backupContainer().getOrThrow(cx.getReference());
				bc = container;

				// Read the original BulkDump mode from config (saved by StartFullBackupTaskFunc before task creation).
				// This is persisted in the database so we can restore the correct mode even after a crash.
				int mode = co_await config.originalBulkDumpMode().getD(cx.getReference(), Snapshot::False, 0);
				originalBulkDumpMode = mode;

				// Determine transport method from backup URL
				transportMethod =
				    isBlobstoreUrl(bc->getURL()) ? BulkLoadTransportMethod::BLOBSTORE : BulkLoadTransportMethod::CP;

				// Check if there's already a running BulkDump job (e.g., from a previous task attempt or another
				// agent). If so, we'll monitor that job instead of submitting a new one. This prevents the "Conflict to
				// a running BulkDump job" error that occurs when multiple backup agents try to run the same task.
				Transaction checkTr(cx);
				while (true) {
					Error err;
					try {
						checkTr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
						checkTr.setOption(FDBTransactionOptions::LOCK_AWARE);
						Optional<BulkDumpState> existingJob = co_await getSubmittedBulkDumpJob(&checkTr);
						if (existingJob.present()) {
							bulkDumpJob = existingJob.get();
							jobAlreadyRunning = true;
							TraceEvent("BulkDumpTaskUsingExistingJob")
							    .detail("BackupUID", config.getUid())
							    .detail("ExistingJobId", bulkDumpJob.getJobId());
						}
						break;
					} catch (Error& e) {
						err = e;
					}
					co_await checkTr.onError(err);
				}

				if (!jobAlreadyRunning) {
					// Enable BulkDump mode at the DD level before submitting the job
					co_await setBulkDumpMode(cx, 1);

					// Configure BulkDump job for the full keyspace
					// BulkDump/BulkLoad requires the load range to be a subset of the dump range.
					// Using normalKeys for both ensures compatibility regardless of user-specified ranges.
					// Store data under data/<container>/bulkdump_data/ to be consistent with backup container layout
					std::string bulkDumpRoot = getBackupDataPath(bc->getURL(), "bulkdump_data");
					bulkDumpJob = createBulkDumpJob(normalKeys, bulkDumpRoot, BulkLoadType::SST, transportMethod);

					// Submit the BulkDump job
					co_await submitBulkDumpJob(cx, bulkDumpJob);

					// Set ownership so bulkdump status shows it belongs to this backup
					std::string backupTag = co_await config.tag().getOrThrow(cx.getReference());
					BulkDumpOwnerInfo ownerInfo(config.getUid(), "backup", backupTag, now());
					co_await setBulkDumpOwner(cx, bulkDumpJob.getJobId(), ownerInfo);
				}

				// Store job ID for monitoring
				Params.bulkDumpJobId().set(task, bulkDumpJob.getJobId().toString());

				// Monitor BulkDump progress - timeout is configurable for large datasets
				bool completed = co_await monitorBulkDumpJobCompletion(cx,
				                                                       bulkDumpJob.getJobId(),
				                                                       CLIENT_KNOBS->BULKDUMP_JOB_TIMEOUT,
				                                                       5.0); // Poll every 5 seconds

				if (!completed) {
					TraceEvent(SevWarn, "BulkDumpTaskTimeout")
					    .detail("BackupUID", config.getUid())
					    .detail("BulkDumpJobId", bulkDumpJob.getJobId())
					    .detail("TimeoutDuration", 300.0);
					Params.timeoutOccurred().set(task, true);
				}

				TraceEvent("BulkDumpTaskComplete")
				    .detail("BackupUID", config.getUid())
				    .detail("BulkDumpJobId", bulkDumpJob.getJobId())
				    .detail("TimeoutOccurred", Params.timeoutOccurred().getOrDefault(task, false))
				    .detail("JobAlreadyRunning", jobAlreadyRunning);

				// Restore original BulkDump mode after job completes, but only if:
				// 1. We actually enabled the mode (not if we just monitored an existing job)
				// 2. The original mode was different from what we set
				if (!jobAlreadyRunning && originalBulkDumpMode != 1) {
					co_await setBulkDumpMode(cx, originalBulkDumpMode);
				}

				// Write the keyspace snapshot file to mark the backup as complete
				// This is essential for the restore process to find the snapshot
				if (completed) {
					// Verify that BulkDump data was actually written before writing snapshot
					bool datasetComplete =
					    co_await verifyBulkDumpDatasetCompleteness(bc, bulkDumpJob.getJobId().toString());
					if (!datasetComplete) {
						TraceEvent(SevError, "BulkDumpTaskDatasetIncomplete")
						    .detail("BackupUID", config.getUid())
						    .detail("BulkDumpJobId", bulkDumpJob.getJobId());
						throw backup_error();
					}

					// Build beginEndKeys from backup ranges
					std::vector<std::pair<Key, Key>> beginEndKeys;
					for (const auto& range : backupRanges) {
						beginEndKeys.emplace_back(range.begin, range.end);
					}

					// Create BulkDump snapshot metadata
					// Note: totalBytes and totalKeys are set to 0 here. Unlike traditional range file backups
					// which accumulate fileSize from each RangeSlice, BulkDump writes its data to manifest files
					// in the backup container (one per shard). To get accurate byte/key counts, we would need to:
					//   1. List all manifest files under bc->getURL()/bulkDumpJobId/
					//   2. Read each BulkLoadManifest and call getTotalBytes()/getKeyCount()
					//   3. Aggregate the totals
					// For now, zeros are acceptable since the critical field is bulkDumpJobId which BulkLoad
					// uses to locate the data. TODO: Fix. The byte counts are informational for status display.
					SnapshotMetadata metadata =
					    SnapshotMetadata::bulkDump(bulkDumpJob.getJobId().toString(), snapshotVersion, 0, 0);

					// Write the snapshot file - empty file list since BulkDump uses job manifests, not range files
					co_await bc->writeKeyspaceSnapshotFile({}, // No individual range files for BulkDump
					                                       beginEndKeys,
					                                       0, // See comment above about totalBytes
					                                       IncludeKeyRangeMap::False,
					                                       metadata);

					TraceEvent("BulkDumpTaskWroteSnapshot")
					    .detail("BackupUID", config.getUid())
					    .detail("BulkDumpJobId", bulkDumpJob.getJobId())
					    .detail("SnapshotVersion", snapshotVersion)
					    .detail("RangeCount", backupRanges.size());
				}

				// Increment counter for test assertions
				g_bulkDumpTaskCompleteCount.fetch_add(1);
				co_return;
			} catch (Error& e) {
				savedError = e;
			}
			TraceEvent(SevWarn, "BulkDumpTaskError")
			    .error(savedError)
			    .detail("BackupUID", config.getUid())
			    .detail("SnapshotVersion", snapshotVersion)
			    .detail("JobAlreadyRunning", jobAlreadyRunning);
			// Restore original BulkDump mode on error, but only if we were the ones who enabled it
			try {
				if (!jobAlreadyRunning && originalBulkDumpMode != 1) {
					co_await setBulkDumpMode(cx, originalBulkDumpMode);
				}
			} catch (Error& e2) {
				TraceEvent(SevWarn, "BulkDumpTaskRestoreModeError").error(e2);
			}
			throw savedError;
		}
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		BackupConfig config(task);
		Version snapshotVersion = Params.snapshotVersion().get(task);
		std::string jobId = Params.bulkDumpJobId().getOrDefault(task, "");

		TraceEvent("BulkDumpTaskFinishStart")
		    .detail("BackupUID", config.getUid())
		    .detail("SnapshotVersion", snapshotVersion)
		    .detail("BulkDumpJobId", jobId);

		// Set latestSnapshotEndVersion so BackupLogsDispatchTask knows we're restorable
		// This is critical for the backup to complete when using BulkDump
		config.latestSnapshotEndVersion().set(tr, snapshotVersion);

		// Set bulkDumpSnapshotEndVersion to track that BulkDump data is available
		// This is used by getLatestRestorableVersion() for mode=BOTH to ensure both
		// rangefile and bulkdump data exist before marking backup as restorable
		config.bulkDumpSnapshotEndVersion().set(tr, snapshotVersion);

		// CRITICAL: Set bulkDumpJobId on the backup CONFIG so that status checks
		// and restore operations can find it. Without this, the backup status shows
		// "BulkLoad Compatible: no" and bulkload restore fails with "Missing backup data".
		if (!jobId.empty()) {
			config.bulkDumpJobId().set(tr, jobId);
		}

		// Set firstSnapshotEndVersion if not already set, BUT only if we're NOT in
		// mode=BOTH. In mode=BOTH, firstSnapshotEndVersion tracks the rangefile snapshot
		// completion and must only be set by BackupSnapshotManifest::_finish().
		// Setting it here would make getLatestRestorableVersion() think both snapshots
		// are complete when only bulkdump is done.
		Optional<int> mode = co_await config.snapshotMode().get(tr);
		if (!mode.present() || mode.get() != 2) {
			Optional<Version> firstSnapshotEnd = co_await config.firstSnapshotEndVersion().get(tr);
			if (!firstSnapshotEnd.present()) {
				config.firstSnapshotEndVersion().set(tr, snapshotVersion);
			}
		}

		TraceEvent("BulkDumpTaskFinishSetVersions")
		    .detail("BackupUID", config.getUid())
		    .detail("SnapshotVersion", snapshotVersion)
		    .detail("Mode", mode.present() ? mode.get() : 0);

		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		co_await taskFuture->set(tr, taskBucket);
		co_await taskBucket->finish(tr, task);

		TraceEvent("BulkDumpTaskFinish")
		    .detail("BackupUID", config.getUid())
		    .detail("SnapshotVersion", snapshotVersion);

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           Version snapshotVersion,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = co_await addBackupTask(BulkDumpTaskFunc::name,
		                                 BulkDumpTaskFunc::version,
		                                 tr,
		                                 taskBucket,
		                                 completionKey,
		                                 BackupConfig(parentTask),
		                                 waitFor,
		                                 [=](Reference<Task> task) {
			                                 Params.snapshotVersion().set(task, snapshotVersion);
			                                 Params.timeoutOccurred().set(task, false);
		                                 });
		co_return key;
	}
};
StringRef BulkDumpTaskFunc::name = "bulk_dump_snapshot_5.2"_sr;
REGISTER_TASKFUNC(BulkDumpTaskFunc);

struct StartFullBackupTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> beginVersion() { return __FUNCTION__sr; }
	} Params;

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		co_await checkTaskVersion(cx, task, StartFullBackupTaskFunc::name, StartFullBackupTaskFunc::version);

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		BackupConfig config(task);
		Future<Optional<bool>> partitionedLog;
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				partitionedLog = config.partitionedLogEnabled().get(tr);
				co_await partitionedLog;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// Check if backup worker is enabled
		DatabaseConfiguration dbConfig = co_await getDatabaseConfiguration(cx);
		bool backupWorkerEnabled = dbConfig.backupWorkerEnabled;
		if (!backupWorkerEnabled && partitionedLog.get().present() && partitionedLog.get().get()) {
			// Change configuration only when we set to use partitioned logs and
			// the flag was not set before.
			co_await ManagementAPI::changeConfig(cx.getReference(), "backup_worker_enabled:=1", true);
			backupWorkerEnabled = true;
			// the user is responsible for manually disabling backup worker after the last backup is done
		}

		// Get start version after backup worker are enabled
		while (true) {
			Error err;
			try {
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Future<Version> startVersionFuture = tr->getReadVersion();
				co_await startVersionFuture;
				Params.beginVersion().set(task, startVersionFuture.get());
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// Set the "backupStartedKey" and wait for all backup worker started
		tr->reset();
		while (true) {
			Future<Void> watchFuture;
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Future<Void> keepRunning = taskBucket->keepRunning(tr, task);

				Future<Optional<Value>> started = tr->get(backupStartedKey);
				Future<Optional<Value>> taskStarted = tr->get(config.allWorkerStarted().key);
				partitionedLog = config.partitionedLogEnabled().get(tr);
				co_await (success(started) && success(taskStarted) && success(partitionedLog));

				if (!partitionedLog.get().present() || !partitionedLog.get().get()) {
					co_return; // Skip if not using partitioned logs
				}

				std::vector<std::pair<UID, Version>> ids;
				if (started.get().present()) {
					ids = decodeBackupStartedValue(started.get().get());
				}
				const UID uid = config.getUid();
				auto it = std::find_if(
				    ids.begin(), ids.end(), [uid](const std::pair<UID, Version>& p) { return p.first == uid; });
				if (it == ids.end()) {
					ids.emplace_back(uid, Params.beginVersion().get(task));
				} else {
					Params.beginVersion().set(task, it->second);
				}

				tr->set(backupStartedKey, encodeBackupStartedValue(ids));

				// The task may be restarted. Set the watch if started key has NOT been set.
				if (!taskStarted.get().present()) {
					watchFuture = tr->watch(config.allWorkerStarted().key);
				}

				co_await keepRunning;
				co_await tr->commit();
				if (!taskStarted.get().present()) {
					co_await watchFuture;
				}
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		BackupConfig config(task);
		Version beginVersion = Params.beginVersion().get(task);

		Future<std::vector<KeyRange>> backupRangesFuture = config.backupRanges().getOrThrow(tr);
		Future<Key> destUidValueFuture = config.destUidValue().getOrThrow(tr);
		Future<Optional<bool>> partitionedLog = config.partitionedLogEnabled().get(tr);
		Future<Optional<bool>> incrementalBackupOnly = config.incrementalBackupOnly().get(tr);
		co_await (success(backupRangesFuture) && success(destUidValueFuture) && success(partitionedLog) &&
		          success(incrementalBackupOnly));
		std::vector<KeyRange> backupRanges = backupRangesFuture.get();
		Key destUidValue = destUidValueFuture.get();

		// Start logging the mutations for the specified ranges of the tag if needed
		if (!partitionedLog.get().present() || !partitionedLog.get().get()) {
			for (auto& backupRange : backupRanges) {
				config.startMutationLogs(tr, backupRange, destUidValue);
			}
		}

		Reference<IBackupContainer> bc = co_await config.backupContainer().getOrThrow(tr);
		co_await bc->writeEncryptionMetadata();

		config.stateEnum().set(tr, EBackupState::STATE_RUNNING);

		Reference<TaskFuture> backupFinished = futureBucket->future(tr);

		// Initialize the initial snapshot and create tasks to continually write logs and snapshots.
		Optional<int64_t> initialSnapshotIntervalSeconds = co_await config.initialSnapshotIntervalSeconds().get(tr);
		co_await config.initNewSnapshot(tr, initialSnapshotIntervalSeconds.orDefault(0));

		// Using priority 1 for both of these to at least start both tasks soon
		// Do not add snapshot task if we only want the incremental backup
		if (!incrementalBackupOnly.get().present() || !incrementalBackupOnly.get().get()) {
			// Check snapshot mode: 0=RANGEFILE, 1=BULKDUMP, 2=BOTH
			int snapshotModeValue = co_await config.snapshotMode().getD(tr, Snapshot::False, 0);

			TraceEvent("StartFullBackupTaskSnapshotMode")
			    .detail("BackupUID", config.getUid())
			    .detail("SnapshotModeValue", snapshotModeValue)
			    .detail("IncrementalBackupOnly",
			            incrementalBackupOnly.get().present() && incrementalBackupOnly.get().get());

			// Add BulkDump task if mode is BULKDUMP(1) or BOTH(2)
			if (snapshotModeValue == 1 || snapshotModeValue == 2) {
				// Save original BulkDump mode BEFORE creating the task, so it's persisted in the database.
				// This allows crash recovery to restore the correct mode even if the task restarts.
				int currentBulkDumpMode = co_await getBulkDumpMode(tr->getDatabase());
				config.originalBulkDumpMode().set(tr, currentBulkDumpMode);

				co_await BulkDumpTaskFunc::addTask(
				    tr, taskBucket, task, beginVersion, TaskCompletionKey::joinWith(backupFinished));
			}

			// Add traditional range file snapshot if mode is RANGEFILE(0) or BOTH(2)
			if (snapshotModeValue == 0 || snapshotModeValue == 2) {
				co_await BackupSnapshotDispatchTask::addTask(
				    tr, taskBucket, task, 1, TaskCompletionKey::joinWith(backupFinished));
			}
		}

		co_await BackupLogsDispatchTask::addTask(
		    tr, taskBucket, task, 1, 0, beginVersion, TaskCompletionKey::joinWith(backupFinished));

		// If a clean stop is requested, the log and snapshot tasks will quit after the backup is restorable, then
		// the following task will clean up and set the completed state.
		co_await FileBackupFinishedTask::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal(), backupFinished);

		co_await taskBucket->finish(tr, task);

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           UID uid,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = co_await addBackupTask(StartFullBackupTaskFunc::name,
		                                 StartFullBackupTaskFunc::version,
		                                 tr,
		                                 taskBucket,
		                                 completionKey,
		                                 BackupConfig(uid),
		                                 waitFor);
		co_return key;
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef StartFullBackupTaskFunc::name = "file_backup_start_5.2"_sr;
REGISTER_TASKFUNC(StartFullBackupTaskFunc);

// BulkLoadRestoreTaskFunc: Restores data using BulkLoad instead of traditional range file restore
// This task is used when useRangeFileRestore=false is specified
struct BulkLoadRestoreTaskFunc : RestoreTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> restoreVersion() { return __FUNCTION__sr; }
		static TaskParam<std::string> backupUrl() { return __FUNCTION__sr; }
		static TaskParam<std::string> bulkDumpJobId() { return __FUNCTION__sr; }
	} Params;

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		RestoreConfig restore(task);
		Version restoreVersion = Params.restoreVersion().get(task);
		std::string backupUrl = Params.backupUrl().get(task);
		std::string bulkDumpJobId = Params.bulkDumpJobId().getOrDefault(task, "");

		TraceEvent("BulkLoadRestoreTaskStart")
		    .detail("RestoreUID", restore.getUid())
		    .detail("RestoreVersion", restoreVersion)
		    .detail("BackupUrl", backupUrl)
		    .detail("BulkDumpJobId", bulkDumpJobId);

		// Declare state variable before try block so it's accessible in catch block
		int originalBulkLoadMode = 0;

		{
			Error savedError;
			try {
				// Open backup container for metadata access
				Reference<IBackupContainer> bcRef = IBackupContainer::openContainer(backupUrl, {}, {});

				// Get restore ranges using a transaction
				Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				std::vector<KeyRange> restoreRanges = co_await restore.getRestoreRangesOrDefault(tr);

				// If bulkDumpJobId is empty, read it from the snapshot file metadata
				if (bulkDumpJobId.empty()) {
					// Use the backup container we already opened to find the snapshot file for our restore version
					auto* bcfsPtr = dynamic_cast<BackupContainerFileSystem*>(bcRef.getPtr());
					if (bcfsPtr != nullptr) {
						Reference<BackupContainerFileSystem> bcfs =
						    Reference<BackupContainerFileSystem>::addRef(bcfsPtr);
						std::vector<KeyspaceSnapshotFile> snapshots = co_await bcfs->listKeyspaceSnapshots();
						int snapIdx = snapshots.size() - 1;
						while (snapIdx >= 0 && bulkDumpJobId.empty()) {
							// For BulkDump snapshots: beginVersion == endVersion == snapshotVersion
							// Accept any snapshot that is at or before our restore version
							if (snapshots[snapIdx].endVersion <= restoreVersion) {
								// Read the snapshot file to get bulkDumpJobId
								std::string snapFileName = snapshots[snapIdx].fileName;
								Reference<IAsyncFile> snapFile = co_await bcfs->readFile(snapFileName);
								int64_t snapSize = co_await snapFile->size();
								Standalone<StringRef> snapBuf = makeString(snapSize);
								int bytesRead = co_await snapFile->read(mutateString(snapBuf), snapSize, 0);
								if (bytesRead == snapSize) {
									json_spirit::mValue json;
									if (json_spirit::read_string(snapBuf.toString(), json)) {
										JSONDoc doc(json);
										std::string jobId;
										if (doc.tryGet("bulkDumpJobId", jobId) && !jobId.empty()) {
											bulkDumpJobId = jobId;
											TraceEvent("BulkLoadRestoreFoundJobId")
											    .detail("RestoreUID", restore.getUid())
											    .detail("BulkDumpJobId", bulkDumpJobId)
											    .detail("SnapshotFile", snapFileName);
										}
									}
								}
							}
							snapIdx--;
						}
					}
				}

				// Create BulkLoad job from the BulkDump data
				// BulkDump stores data under data/<container>/bulkdump_data/ subdirectory.
				// DD appends the jobId via getBulkLoadJobRoot(jobRoot, jobId) when accessing files.
				std::string jobRoot = getBackupDataPath(backupUrl, "bulkdump_data");
				UID dumpJobUid;
				if (!bulkDumpJobId.empty()) {
					dumpJobUid = UID::fromString(bulkDumpJobId);
				} else {
					// No BulkDump job ID found - this is a permanent error for BulkLoad restore.
					// Abort the restore immediately instead of retrying forever.
					TraceEvent(SevError, "BulkLoadRestoreNoBulkDumpJobId")
					    .detail("RestoreUID", restore.getUid())
					    .detail("BackupUrl", backupUrl)
					    .detail("Action", "Aborting restore - backup is not BulkLoad compatible");
					co_await restore.logError(
					    cx, restore_missing_data(), "BulkLoad restore failed: backup has no bulkdump data", nullptr);
					// Abort the restore by setting state to ABORTED
					Reference<ReadYourWritesTransaction> abortTr(new ReadYourWritesTransaction(cx));
					abortTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					abortTr->setOption(FDBTransactionOptions::LOCK_AWARE);
					restore.stateEnum().set(abortTr, ERestoreState::ABORTED);
					co_await abortTr->commit();
					throw restore_missing_data();
				}

				// Verify BulkDump dataset completeness before proceeding
				bool datasetComplete = co_await verifyBulkDumpDatasetCompleteness(bcRef, bulkDumpJobId);
				if (!datasetComplete) {
					// Dataset is incomplete - abort the restore permanently
					TraceEvent(SevError, "BulkLoadRestoreDatasetIncomplete")
					    .detail("RestoreUID", restore.getUid())
					    .detail("BulkDumpJobId", bulkDumpJobId)
					    .detail("BackupUrl", backupUrl)
					    .detail("Action", "Aborting restore - bulkdump dataset is incomplete");
					co_await restore.logError(
					    cx, restore_missing_data(), "BulkLoad restore failed: bulkdump dataset incomplete", nullptr);
					// Abort the restore by setting state to ABORTED
					Reference<ReadYourWritesTransaction> abortTr(new ReadYourWritesTransaction(cx));
					abortTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					abortTr->setOption(FDBTransactionOptions::LOCK_AWARE);
					restore.stateEnum().set(abortTr, ERestoreState::ABORTED);
					co_await abortTr->commit();
					throw restore_missing_data();
				}
				TraceEvent("BulkLoadRestoreDatasetVerified")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BulkDumpJobId", bulkDumpJobId);

				// Determine transport method from backup URL
				BulkLoadTransportMethod loadTransportMethod =
				    isBlobstoreUrl(backupUrl) ? BulkLoadTransportMethod::BLOBSTORE : BulkLoadTransportMethod::CP;

				// BulkLoad range must match the BulkDump range (normalKeys)
				// The actual restore ranges will be applied via mutation log replay
				BulkLoadJobState bulkLoadJob = createBulkLoadJob(dumpJobUid, normalKeys, jobRoot, loadTransportMethod);

				TraceEvent("BulkLoadRestoreJobCreated")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BulkLoadJobId", bulkLoadJob.getJobId())
				    .detail("JobRoot", jobRoot);

				// Register the BulkLoad range lock owner (required for range locking)
				co_await registerRangeLockOwner(cx, "BulkLoad", "BulkLoad restore operation");

				TraceEvent("BulkLoadRestoreRegisteredLockOwner")
				    .detail("RestoreUID", restore.getUid())
				    .detail("Owner", "BulkLoad");

				// Note: BulkLoad configuration validation (shard_encode_location_metadata, enable_read_lock_on_range)
				// is performed by the BulkLoad system on the server side when the job is submitted.

				// Read the original BulkLoad mode from config (saved by StartFullRestoreTaskFunc before task creation).
				// This is persisted in the database so we can restore the correct mode even after a crash.
				int mode = co_await restore.originalBulkLoadMode().getD(cx.getReference(), Snapshot::False, 0);
				originalBulkLoadMode = mode;

				// Enable BulkLoad mode at DD level so the job will be processed
				co_await setBulkLoadMode(cx, 1);

				TraceEvent("BulkLoadRestoreEnabledMode")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BulkLoadJobId", bulkLoadJob.getJobId());

				// Submit BulkLoad job (must be lockAware since DB is locked during restore)
				co_await submitBulkLoadJob(cx, bulkLoadJob, true /* lockAware */);

				TraceEvent("BulkLoadRestoreJobSubmitted")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BulkLoadJobId", bulkLoadJob.getJobId());

				// Get total block count for progress tracking
				int64_t totalBlocks = co_await restore.fileBlockCount().getD(cx.getReference(), Snapshot::False, 0);

				// Monitor BulkLoad progress - timeout is configurable for large datasets
				// Must be lockAware since DB is locked during restore
				// Use the progress-tracking version to update restore counters
				bool completed = co_await monitorBulkLoadJobCompletionWithProgress(cx,
				                                                                   bulkLoadJob.getJobId(),
				                                                                   restore.getUid(),
				                                                                   totalBlocks,
				                                                                   CLIENT_KNOBS->BULKLOAD_JOB_TIMEOUT,
				                                                                   5.0, // Poll every 5 seconds
				                                                                   true); // lockAware

				if (!completed) {
					TraceEvent(SevWarn, "BulkLoadRestoreTimeout")
					    .detail("RestoreUID", restore.getUid())
					    .detail("BulkLoadJobId", bulkLoadJob.getJobId())
					    .detail("TimeoutDuration", CLIENT_KNOBS->BULKLOAD_JOB_TIMEOUT);
					// Restore original BulkLoad mode before throwing
					if (originalBulkLoadMode != 1) {
						co_await setBulkLoadMode(cx, originalBulkLoadMode);
					}
					throw timed_out();
				}

				// Restore original BulkLoad mode now that the job is complete
				if (originalBulkLoadMode != 1) {
					co_await setBulkLoadMode(cx, originalBulkLoadMode);
				}

				TraceEvent("BulkLoadRestoreTaskComplete")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BulkLoadJobId", bulkLoadJob.getJobId())
				    .detail("RestoreVersion", restoreVersion);

				// Increment counter for test assertions
				g_bulkLoadRestoreTaskCompleteCount.fetch_add(1);
				co_return;
			} catch (Error& e) {
				savedError = e;
			}
			TraceEvent(SevWarn, "BulkLoadRestoreTaskError")
			    .error(savedError)
			    .detail("RestoreUID", restore.getUid())
			    .detail("BackupUrl", backupUrl);
			// Restore original BulkLoad mode on error
			try {
				if (originalBulkLoadMode != 1) {
					co_await setBulkLoadMode(cx, originalBulkLoadMode);
				}
			} catch (Error& e2) {
				TraceEvent(SevWarn, "BulkLoadRestoreRestoreModeError").error(e2);
			}
			throw savedError;
		}
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		RestoreConfig restore(task);

		// Mark BulkLoad phase as complete
		restore.bulkLoadComplete().set(tr, true);

		// Update fileBlocksFinished and filesBlocksDispatched to match fileBlockCount
		// so the restore progress tracker knows all range data has been loaded.
		// BulkLoad doesn't use the traditional block dispatching mechanism.
		int64_t totalBlocks = co_await restore.fileBlockCount().getD(tr, Snapshot::False, 0);
		int64_t finishedBlocks = co_await restore.fileBlocksFinished().getD(tr, Snapshot::False, 0);
		if (finishedBlocks < totalBlocks) {
			// Set both dispatched and finished to match total to indicate all range data is done
			restore.filesBlocksDispatched().set(tr, totalBlocks);
			restore.fileBlocksFinished().set(tr, totalBlocks);
		}

		// Set firstConsistentVersion if not already set
		// For BulkLoad restore, this is the snapshot version from the BulkDump
		Version firstConsistentVer =
		    co_await restore.firstConsistentVersion().getD(tr, Snapshot::False, invalidVersion);
		if (firstConsistentVer == invalidVersion) {
			// Get the restore version as the first consistent version
			Version restoreVer = co_await restore.restoreVersion().getD(tr, Snapshot::False, invalidVersion);
			if (restoreVer != invalidVersion) {
				restore.firstConsistentVersion().set(tr, restoreVer);
			}
		}

		TraceEvent("BulkLoadRestoreTaskFinished")
		    .detail("RestoreUID", restore.getUid())
		    .detail("TotalBlocks", totalBlocks)
		    .detail("FinishedBlocks", finishedBlocks);

		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		co_await taskFuture->set(tr, taskBucket);
		co_await taskBucket->finish(tr, task);
		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           Version restoreVersion,
	                           std::string backupUrl,
	                           std::string bulkDumpJobId,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = co_await completionKey.get(tr, taskBucket);
		Reference<Task> task(new Task(BulkLoadRestoreTaskFunc::name, BulkLoadRestoreTaskFunc::version, doneKey));

		// Set task parameters
		Params.restoreVersion().set(task, restoreVersion);
		Params.backupUrl().set(task, backupUrl);
		Params.bulkDumpJobId().set(task, bulkDumpJobId);

		// Get restore config from parent task and bind it to new task
		co_await RestoreConfig(parentTask).toTask(tr, task);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}
};
StringRef BulkLoadRestoreTaskFunc::name = "bulk_load_restore_5.2"_sr;
REGISTER_TASKFUNC(BulkLoadRestoreTaskFunc);

struct RestoreCompleteTaskFunc : RestoreTaskFuncBase {
	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {

		co_await checkTaskVersion(tr->getDatabase(), task, name, version);

		RestoreConfig restore(task);
		restore.stateEnum().set(tr, ERestoreState::COMPLETED);
		bool unlockDB = co_await restore.unlockDBAfterRestore().getD(tr, Snapshot::False, true);

		tr->atomicOp(metadataVersionKey, metadataVersionRequiredValue, MutationRef::SetVersionstampedValue);
		// Clear the file map now since it could be huge.
		restore.fileSet().clear(tr);

		// TODO:  Validate that the range version map has exactly the restored ranges in it.  This means that for
		// any restore operation the ranges to restore must be within the backed up ranges, otherwise from the
		// restore perspective it will appear that some key ranges were missing and so the backup set is incomplete
		// and the restore has failed. This validation cannot be done currently because Restore only supports a
		// single restore range but backups can have many ranges.

		// Clear the applyMutations stuff, including any unapplied mutations from versions beyond the restored
		// version.
		restore.clearApplyMutationsKeys(tr);

		co_await taskBucket->finish(tr, task);

		if (unlockDB) {
			co_await unlockDatabase(tr, restore.getUid());
		}

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = co_await completionKey.get(tr, taskBucket);
		Reference<Task> task(new Task(RestoreCompleteTaskFunc::name, RestoreCompleteTaskFunc::version, doneKey));

		// Get restore config from parent task and bind it to new task
		co_await RestoreConfig(parentTask).toTask(tr, task);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}

	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreCompleteTaskFunc::name = "restore_complete"_sr;
REGISTER_TASKFUNC(RestoreCompleteTaskFunc);

struct RestoreFileTaskFuncBase : RestoreTaskFuncBase {
	struct InputParams {
		static TaskParam<RestoreFile> inputFile() { return __FUNCTION__sr; }
		static TaskParam<int64_t> readOffset() { return __FUNCTION__sr; }
		static TaskParam<int64_t> readLen() { return __FUNCTION__sr; }
	} Params;

	std::string toString(Reference<Task> task) const override {
		return format("fileName '%s' readLen %lld readOffset %lld",
		              Params.inputFile().get(task).fileName.c_str(),
		              Params.readLen().get(task),
		              Params.readOffset().get(task));
	}
};

struct RestoreRangeTaskFunc : RestoreFileTaskFuncBase {
	static struct : InputParams {
		// The range of data that the (possibly empty) data represented, which is set if it intersects the target
		// restore range
		static TaskParam<KeyRange> originalFileRange() { return __FUNCTION__sr; }
		static TaskParam<std::vector<KeyRange>> originalFileRanges() { return __FUNCTION__sr; }

		static std::vector<KeyRange> getOriginalFileRanges(Reference<Task> task) {
			if (originalFileRanges().exists(task)) {
				return Params.originalFileRanges().get(task);
			} else {
				std::vector<KeyRange> range;
				if (originalFileRange().exists(task))
					range.push_back(Params.originalFileRange().get(task));
				return range;
			}
		}
	} Params;

	std::string toString(Reference<Task> task) const override {
		std::string returnStr = RestoreFileTaskFuncBase::toString(task);
		for (auto& range : Params.getOriginalFileRanges(task))
			returnStr += format("  originalFileRange '%s'", printable(range).c_str());
		return returnStr;
	}

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		RestoreConfig restore(task);

		RestoreFile rangeFile = Params.inputFile().get(task);
		int64_t readOffset = Params.readOffset().get(task);
		int64_t readLen = Params.readLen().get(task);

		TraceEvent("FileRestoreRangeStart")
		    .suppressFor(60)
		    .detail("RestoreUID", restore.getUid())
		    .detail("FileName", rangeFile.fileName)
		    .detail("FileVersion", rangeFile.version)
		    .detail("FileSize", rangeFile.fileSize)
		    .detail("ReadOffset", readOffset)
		    .detail("ReadLen", readLen);

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		Future<Reference<IBackupContainer>> bc;
		Future<std::vector<KeyRange>> restoreRanges;
		Future<Key> addPrefix;
		Future<Key> removePrefix;

		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Reference<IBackupContainer> _bc = co_await restore.sourceContainer().getOrThrow(tr);
				bc = getBackupContainerWithProxy(_bc);
				restoreRanges = restore.getRestoreRangesOrDefault(tr);
				addPrefix = restore.addPrefix().getD(tr);
				removePrefix = restore.removePrefix().getD(tr);

				co_await taskBucket->keepRunning(tr, task);

				co_await (success(bc) && success(restoreRanges) && success(addPrefix) && success(removePrefix) &&
				          checkTaskVersion(tr->getDatabase(), task, name, version));
				break;

			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		Reference<IAsyncFile> inFile = co_await bc.get()->readFile(rangeFile.fileName);
		Standalone<VectorRef<KeyValueRef>> blockData;
		try {
			// data is each real KV, not encoded mutations
			Standalone<VectorRef<KeyValueRef>> data = co_await decodeRangeFileBlock(inFile, readOffset, readLen, cx);
			blockData = data;
		} catch (Error& e) {
			if (e.code() == error_code_encrypt_keys_fetch_failed || e.code() == error_code_encrypt_key_not_found) {
				co_return;
			}
			throw;
		}
		Arena arena;
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);

		// First and last key are the range for this file
		KeyRange fileRange;
		std::vector<KeyRange> originalFileRanges;
		// If fileRange doesn't intersect restore range then we're done.
		int index{ 0 };
		for (index = 0; index < restoreRanges.get().size(); index++) {
			auto& restoreRange = restoreRanges.get()[index];
			fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);
			if (!fileRange.intersects(restoreRange))
				continue;

			// We know the file range intersects the restore range but there could still be keys outside the restore
			// range. Find the subvector of kv pairs that intersect the restore range.  Note that the first and last
			// keys are just the range endpoints for this file
			int rangeStart = 1;
			int rangeEnd = blockData.size() - 1;
			// Slide start forward, stop if something in range is found
			while (rangeStart < rangeEnd && !restoreRange.contains(blockData[rangeStart].key))
				++rangeStart;
			// Side end backward, stop if something in range is found
			while (rangeEnd > rangeStart && !restoreRange.contains(blockData[rangeEnd - 1].key))
				--rangeEnd;

			VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);

			// Shrink file range to be entirely within restoreRange and translate it to the new prefix
			// First, use the untranslated file range to create the shrunk original file range which must be used in
			// the kv range version map for applying mutations
			KeyRange originalFileRange =
			    KeyRangeRef(std::max(fileRange.begin, restoreRange.begin), std::min(fileRange.end, restoreRange.end));
			originalFileRanges.push_back(originalFileRange);

			// Now shrink and translate fileRange
			Key fileEnd = std::min(fileRange.end, restoreRange.end);
			if (fileEnd == (removePrefix.get().empty() ? allKeys.end : strinc(removePrefix.get()))) {
				fileEnd = addPrefix.get().empty() ? allKeys.end : strinc(addPrefix.get());
			} else {
				fileEnd = fileEnd.removePrefix(removePrefix.get()).withPrefix(addPrefix.get());
			}
			fileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.begin)
			                            .removePrefix(removePrefix.get())
			                            .withPrefix(addPrefix.get()),
			                        fileEnd);

			int start = 0;
			int end = data.size();
			int dataSizeLimit =
			    BUGGIFY ? deterministicRandom()->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

			tr->reset();
			while (true) {
				Error err;
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					int i = start;
					int txBytes = 0;
					int iend = start;

					// find iend that results in the desired transaction size
					for (; iend < end && txBytes < dataSizeLimit; ++iend) {
						txBytes += data[iend].key.expectedSize();
						txBytes += data[iend].value.expectedSize();
					}

					// Clear the range we are about to set.
					// If start == 0 then use fileBegin for the start of the range, else data[start]
					// If iend == end then use fileEnd for the end of the range, else data[iend]
					// clear the raw key(without alog prefix)
					KeyRange trRange = KeyRangeRef(
					    (start == 0) ? fileRange.begin
					                 : data[start].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()),
					    (iend == end) ? fileRange.end
					                  : data[iend].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()));
					tr->clear(trRange);

					for (; i < iend; ++i) {
						tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
						tr->set(data[i].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()),
						        data[i].value);
					}

					// Add to bytes written count
					restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

					Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

					co_await taskBucket->keepRunning(tr, task);

					co_await checkLock;

					co_await tr->commit();

					TraceEvent("FileRestoreCommittedRange")
					    .suppressFor(60)
					    .detail("RestoreUID", restore.getUid())
					    .detail("FileName", rangeFile.fileName)
					    .detail("FileVersion", rangeFile.version)
					    .detail("FileSize", rangeFile.fileSize)
					    .detail("ReadOffset", readOffset)
					    .detail("ReadLen", readLen)
					    .detail("CommitVersion", tr->getCommittedVersion())
					    .detail("BeginRange", trRange.begin)
					    .detail("EndRange", trRange.end)
					    .detail("StartIndex", start)
					    .detail("EndIndex", i)
					    .detail("DataSize", data.size())
					    .detail("Bytes", txBytes)
					    .detail("OriginalFileRange", originalFileRange);

					// Commit succeeded, so advance starting point
					start = i;

					if (start == end)
						break;
					tr->reset();
					continue;
				} catch (Error& e) {
					err = e;
				}
				if (err.code() == error_code_transaction_too_large)
					dataSizeLimit /= 2;
				else
					co_await tr->onError(err);
			}
		}
		if (!originalFileRanges.empty()) {
			if (BUGGIFY && restoreRanges.get().size() == 1) {
				Params.originalFileRange().set(task, originalFileRanges[0]);
			} else {
				Params.originalFileRanges().set(task, originalFileRanges);
			}
		}
		co_return;
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		RestoreConfig restore(task);
		restore.fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

		// Update the KV range map if originalFileRange is set
		std::vector<Future<Void>> updateMap;
		std::vector<KeyRange> ranges = Params.getOriginalFileRanges(task);
		// if to restore((a, b), (e, f), (x, y)), then there are 3 ranges
		for (auto& range : ranges) {
			Value versionEncoded = BinaryWriter::toValue(Params.inputFile().get(task).version, Unversioned());
			updateMap.push_back(krmSetRange(tr, restore.applyMutationsMapPrefix(), range, versionEncoded));
		}

		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		co_await (taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task) && waitForAll(updateMap));

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           RestoreFile rf,
	                           int64_t offset,
	                           int64_t len,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = co_await completionKey.get(tr, taskBucket);
		Reference<Task> task(new Task(RestoreRangeTaskFunc::name, RestoreRangeTaskFunc::version, doneKey));

		// Create a restore config from the current task and bind it to the new task.
		co_await RestoreConfig(parentTask).toTask(tr, task);

		Params.inputFile().set(task, rf);
		Params.readOffset().set(task, offset);
		Params.readLen().set(task, len);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}

	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreRangeTaskFunc::name = "restore_range_data"_sr;
REGISTER_TASKFUNC(RestoreRangeTaskFunc);

// Decodes a mutation log key, which contains (hash, commitVersion, chunkNumber) and
// returns (commitVersion, chunkNumber)
std::pair<Version, int32_t> decodeMutationLogKey(const StringRef& key) {
	ASSERT(key.size() == sizeof(uint8_t) + sizeof(Version) + sizeof(int32_t));

	uint8_t hash;
	Version version;
	int32_t part;
	BinaryReader rd(key, Unversioned());
	rd >> hash >> version >> part;
	version = bigEndian64(version);
	part = bigEndian32(part);

	int32_t v = version / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	ASSERT(((uint8_t)hashlittle(&v, sizeof(v), 0)) == hash);

	return std::make_pair(version, part);
}

// Decodes an encoded list of mutations in the format of:
//   [includeVersion:uint64_t][val_length:uint32_t][mutation_1][mutation_2]...[mutation_k],
// where a mutation is encoded as:
//   [type:uint32_t][keyLength:uint32_t][valueLength:uint32_t][param1][param2]
// noted version needs to be included here(0x0FDB00A200090001)
std::vector<MutationRef> decodeMutationLogValue(const StringRef& value) {
	StringRefReader reader(value, restore_corrupted_data());

	Version protocolVersion = reader.consume<uint64_t>();
	if (protocolVersion <= 0x0FDB00A200090001) {
		throw incompatible_protocol_version();
	}

	uint32_t val_length = reader.consume<uint32_t>();
	if (val_length != value.size() - sizeof(uint64_t) - sizeof(uint32_t)) {
		TraceEvent(SevError, "FileRestoreLogValueError")
		    .detail("ValueLen", val_length)
		    .detail("ValueSize", value.size())
		    .detail("Value", printable(value));
	}

	std::vector<MutationRef> mutations;
	while (1) {
		if (reader.eof())
			break;

		// Deserialization of a MutationRef, which was packed by MutationListRef::push_back_deep()
		uint32_t type, p1len, p2len;
		type = reader.consume<uint32_t>();
		p1len = reader.consume<uint32_t>();
		p2len = reader.consume<uint32_t>();

		const uint8_t* key = reader.consume(p1len);
		const uint8_t* val = reader.consume(p2len);

		mutations.emplace_back((MutationRef::Type)type, StringRef(key, p1len), StringRef(val, p2len));
	}
	return mutations;
}

void AccumulatedMutations::addChunk(int chunkNumber, const KeyValueRef& kv) {
	// here it validates that partition(chunk) number has to be continuous
	if (chunkNumber == lastChunkNumber + 1) {
		lastChunkNumber = chunkNumber;
		serializedMutations += kv.value.toString();
	} else {
		lastChunkNumber = -2;
		serializedMutations.clear();
	}
	kvs.push_back(kv);
}

bool AccumulatedMutations::isComplete() const {
	if (lastChunkNumber >= 0) {
		StringRefReader reader(serializedMutations, restore_corrupted_data());

		Version protocolVersion = reader.consume<uint64_t>();
		if (protocolVersion <= 0x0FDB00A200090001) {
			throw incompatible_protocol_version();
		}

		uint32_t vLen = reader.consume<uint32_t>();
		return vLen == reader.remainder().size();
	}

	return false;
}

// Returns true if a complete chunk contains any MutationRefs which intersect with any
// range in ranges.
// It is undefined behavior to run this if isComplete() does not return true.
bool AccumulatedMutations::matchesAnyRange(const RangeMapFilters& filters) const {
	// decode param2, so that each actual mutations are in mutations variable
	std::vector<MutationRef> mutations = decodeMutationLogValue(serializedMutations);
	for (auto& m : mutations) {
		if (filters.match(m)) {
			return true;
		}
	}

	return false;
}

bool RangeMapFilters::match(const MutationRef& m) const {
	if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		if (match(singleKeyRange(m.param1))) {
			return true;
		}
	} else if (m.type == MutationRef::ClearRange) {
		if (match(KeyRangeRef(m.param1, m.param2))) {
			return true;
		}
	} else {
		ASSERT(false);
	}
	return false;
}

bool RangeMapFilters::match(const KeyValueRef& kv) const {
	return match(singleKeyRange(kv.key));
}

bool RangeMapFilters::match(const KeyRangeRef& range) const {
	auto ranges = rangeMap.intersectingRanges(range);
	for (const auto& r : ranges) {
		if (r.cvalue() == 1) {
			return true;
		}
	}
	return false;
}

// Returns a vector of filtered KV refs from data which are either part of incomplete mutation groups OR complete
// and have data relevant to one of the KV ranges in ranges
std::vector<KeyValueRef> filterLogMutationKVPairs(VectorRef<KeyValueRef> data, const RangeMapFilters& filters) {
	std::unordered_map<Version, AccumulatedMutations> mutationBlocksByVersion;

	// group mutations by version
	for (auto& kv : data) {
		// each kv is a [param1, param2]
		auto versionAndChunkNumber = decodeMutationLogKey(kv.key);
		mutationBlocksByVersion[versionAndChunkNumber.first].addChunk(versionAndChunkNumber.second, kv);
	}

	std::vector<KeyValueRef> output;

	// then add each version to the output, and now each K in output is also a KeyValueRef,
	// but mutations of the same versions stay together
	for (auto& vb : mutationBlocksByVersion) {
		AccumulatedMutations& m = vb.second;

		// If the mutations are incomplete or match one of the ranges, include in results.
		if (!m.isComplete() || m.matchesAnyRange(filters)) {
			output.insert(output.end(), m.kvs.begin(), m.kvs.end());
		}
	}

	return output;
}
struct RestoreLogDataTaskFunc : RestoreFileTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct : InputParams {
	} Params;

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		RestoreConfig restore(task);

		RestoreFile logFile = Params.inputFile().get(task);
		int64_t readOffset = Params.readOffset().get(task);
		int64_t readLen = Params.readLen().get(task);

		TraceEvent("FileRestoreLogStart")
		    .suppressFor(60)
		    .detail("RestoreUID", restore.getUid())
		    .detail("FileName", logFile.fileName)
		    .detail("FileBeginVersion", logFile.version)
		    .detail("FileEndVersion", logFile.endVersion)
		    .detail("FileSize", logFile.fileSize)
		    .detail("ReadOffset", readOffset)
		    .detail("ReadLen", readLen);

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		Reference<IBackupContainer> bc;
		std::vector<KeyRange> ranges;

		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Reference<IBackupContainer> _bc = co_await restore.sourceContainer().getOrThrow(tr);
				bc = getBackupContainerWithProxy(_bc);

				ranges = co_await restore.getRestoreRangesOrDefault(tr);

				co_await checkTaskVersion(tr->getDatabase(), task, name, version);
				co_await taskBucket->keepRunning(tr, task);

				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		Key mutationLogPrefix = restore.mutationLogPrefix();
		Reference<IAsyncFile> inFile = co_await bc->readFile(logFile.fileName);
		Standalone<VectorRef<KeyValueRef>> dataOriginal =
		    co_await decodeMutationLogFileBlock(inFile, readOffset, readLen);

		// Filter the KV pairs extracted from the log file block to remove any records known to not be needed for
		// this restore based on the restore range set.
		RangeMapFilters filters(ranges);
		std::vector<KeyValueRef> dataFiltered = filterLogMutationKVPairs(dataOriginal, filters);

		int start = 0;
		int end = dataFiltered.size();
		int dataSizeLimit =
		    BUGGIFY ? deterministicRandom()->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

		tr->reset();
		while (true) {
			Error err;
			try {
				if (start == end)
					co_return;
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				int i = start;
				int txBytes = 0;
				for (; i < end && txBytes < dataSizeLimit; ++i) {
					Key k = dataFiltered[i].key.withPrefix(mutationLogPrefix);
					ValueRef v = dataFiltered[i].value; // each KV is a [param1 with added prefix -> param2]
					tr->set(k, v);
					txBytes += k.expectedSize();
					txBytes += v.expectedSize();
				}

				Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

				co_await taskBucket->keepRunning(tr, task);
				co_await checkLock;

				// Add to bytes written count
				restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

				co_await tr->commit();

				TraceEvent("FileRestoreCommittedLog")
				    .suppressFor(60)
				    .detail("RestoreUID", restore.getUid())
				    .detail("FileName", logFile.fileName)
				    .detail("FileBeginVersion", logFile.version)
				    .detail("FileEndVersion", logFile.endVersion)
				    .detail("FileSize", logFile.fileSize)
				    .detail("ReadOffset", readOffset)
				    .detail("ReadLen", readLen)
				    .detail("CommitVersion", tr->getCommittedVersion())
				    .detail("StartIndex", start)
				    .detail("EndIndex", i)
				    .detail("RecordCountOriginal", dataOriginal.size())
				    .detail("RecordCountFiltered", dataFiltered.size())
				    .detail("Bytes", txBytes);

				// Commit succeeded, so advance starting point
				start = i;
				tr->reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_transaction_too_large)
				dataSizeLimit /= 2;
			else
				co_await tr->onError(err);
		}
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		RestoreConfig(task).fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		// TODO:  Check to see if there is a leak in the FutureBucket since an invalid task (validation key fails)
		// will never set its taskFuture.
		co_await (taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           RestoreFile lf,
	                           int64_t offset,
	                           int64_t len,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = co_await completionKey.get(tr, taskBucket);
		Reference<Task> task(new Task(RestoreLogDataTaskFunc::name, RestoreLogDataTaskFunc::version, doneKey));

		// Create a restore config from the current task and bind it to the new task.
		// RestoreConfig(parentTask) creates prefix of : fileRestorePrefixRange.begin/uid->config/[uid]
		co_await RestoreConfig(parentTask).toTask(tr, task);
		Params.inputFile().set(task, lf);
		Params.readOffset().set(task, offset);
		Params.readLen().set(task, len);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreLogDataTaskFunc::name = "restore_log_data"_sr;
REGISTER_TASKFUNC(RestoreLogDataTaskFunc);

// type|kLen|vLen|Key|Value
// ref: decodeBackupLogValue()
Standalone<StringRef> transformMutationToOldFormat(MutationRef m) {
	BinaryWriter bw(Unversioned());
	uint32_t len1, len2, type;
	type = m.type;
	len1 = m.param1.size();
	len2 = m.param2.size();
	bw << type;
	bw << len1;
	bw << len2;
	// do not use <<, it is overloaded for stringref to write its size first
	bw.serializeBytes(m.param1);
	bw.serializeBytes(m.param2);
	return bw.toValue();
}

// this method takes a version and a list of list of mutations of this verison,
// each list is returned from a iterator sorted by sub
// it will first add all mutations in subsequence order
// then combine them in old-format (param1, parma2) and return
// this method assumes that iterator can return a list of mutations
/*
    mutations are serialized in file as below format:
        `<BlockHeader>`
        `<Version_1><Subseq_1><Mutation1_len><Mutation1>`
        `<Version_2><Subseq_2><Mutation2_len><Mutation2>`
        `…`
        `<Padding>

   for now, assume each iterator returns a vector<pair<subsequence, mutation> >
   noted that the mutation's arena has to be valid during the execution

   according to BackupWorker::addMutation, version 64-bit, sub is 32-bit and mutation length is 32-bit    So iterator
   will combine all mutations in the same version and return a vector iterator should also return the subsequence
   together with each mutation as here we will do another mergeSort for subsequence again to decide the order and here
   we will decode the stringref
*/
Standalone<VectorRef<KeyValueRef>> generateOldFormatMutations(
    Version commitVersion,
    const std::vector<Standalone<VectorRef<VersionedMutation>>>& newFormatMutations) {
	Standalone<VectorRef<KeyValueRef>> results;
	std::vector<Standalone<VectorRef<KeyValueRef>>> oldFormatMutations;
	// mergeSort subversion here
	// just do a global sort for everyone
	int32_t totalBytes = 0;
	std::map<uint32_t, std::vector<Standalone<StringRef>>> mutationsBySub;
	std::map<uint32_t, std::vector<Standalone<MutationRef>>> tmpMap;
	for (auto& eachTagMutations : newFormatMutations) {
		for (auto& vm : eachTagMutations) {
			uint32_t sub = vm.subsequence;
			Standalone<StringRef> mutationOldFormat = transformMutationToOldFormat(vm.mutation);
			mutationsBySub[sub].push_back(mutationOldFormat);
			tmpMap[sub].push_back(vm.mutation);
			totalBytes += mutationOldFormat.size();
		}
	}
	// the list of param2 needs to have the first 64 bites as 0x0FDB00A200090001
	BinaryWriter param2Writer(IncludeVersion(ProtocolVersion::withBackupMutations()));
	param2Writer << totalBytes;

	for (auto& mutationsForSub : mutationsBySub) {
		// concatenate them to param2Str
		for (auto& m : mutationsForSub.second) {
			// refer to transformMutationToOldFormat
			param2Writer.serializeBytes(m);
		}
	}
	Key param2Concat = param2Writer.toValue();

	// deal with param1
	int32_t hashBase = commitVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;

	BinaryWriter wrParam1(Unversioned()); // hash/commitVersion/part
	wrParam1 << (uint8_t)hashlittle(&hashBase, sizeof(hashBase), 0);
	wrParam1 << bigEndian64(commitVersion);
	uint32_t* partBuffer = nullptr;

	// TODO: re-use the similar logic in CommitProxyServer::addBackupMutations
	// generate a list of (param1, param2)
	// param2 has format: length_of_the_mutation_group | encoded_mutation_1 | … | encoded_mutation_k
	// each mutation has format type|kLen|vLen|Key|Value
	for (int part = 0; part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE < param2Concat.size(); part++) {
		KeyValueRef backupKV;
		// Assign the second parameter as the part
		backupKV.value = getBackupValue(param2Concat, part);
		Key key = getBackupKey(wrParam1, &partBuffer, part); // holds the memory
		backupKV.key = key;
		results.push_back_deep(results.arena(), backupKV);
	}
	return results;
}

struct RestoreLogDataPartitionedTaskFunc : RestoreFileTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct {
		static TaskParam<int64_t> maxTagID() { return __FUNCTION__sr; }
		static TaskParam<Version> beginVersion() { return __FUNCTION__sr; }
		static TaskParam<Version> endVersion() { return __FUNCTION__sr; }
		static TaskParam<int64_t> bytesWritten() { return __FUNCTION__sr; }
		static TaskParam<std::vector<RestoreConfig::RestoreFile>> logs() { return __FUNCTION__sr; }
	} Params;

	// From all iterators, find the minimum version and return it together with if this version has data.
	static Future<std::pair<Version, bool>> findNextVersion(std::vector<Reference<PartitionedLogIterator>> iterators) {
		Version minVersion = std::numeric_limits<int64_t>::max();
		bool atLeastOneIteratorHasNext = false;

		for (int k = 0; k < iterators.size(); k++) {
			if (!iterators[k]->hasNext()) {
				continue;
			}
			atLeastOneIteratorHasNext = true;
			Version v = co_await iterators[k]->peekNextVersion();
			minVersion = std::min(minVersion, v);
		}
		co_return std::make_pair(minVersion, atLeastOneIteratorHasNext);
	}

	// Reads from all iterators and sends a list of mutations for the given version
	// to the mutationStream for consumption. This actor reads as fast as possible,
	// which is fine since the task is working on the version range of size
	// RESTORE_PARTITIONED_BATCH_VERSION_SIZE, thus limiting the amount of memory used.
	static Future<Void> readLogData(PromiseStream<Standalone<VectorRef<KeyValueRef>>> mutationStream,
	                                std::vector<Reference<PartitionedLogIterator>> iterators,
	                                Version begin,
	                                Version end) {
		// mergeSort all iterator until all are exhausted
		// it stores all mutations for the next min version, in new format
		try {
			bool atLeastOneIteratorHasNext = true;
			Version minVersion{ 0 };
			Version lastMinVersion = invalidVersion;
			while (atLeastOneIteratorHasNext) {
				std::pair<Version, bool> minVersionAndHasNext = co_await findNextVersion(iterators);
				minVersion = minVersionAndHasNext.first;
				atLeastOneIteratorHasNext = minVersionAndHasNext.second;
				ASSERT_LT(lastMinVersion, minVersion);
				lastMinVersion = minVersion;

				if (atLeastOneIteratorHasNext) {
					std::vector<Standalone<VectorRef<VersionedMutation>>> mutationsSingleVersion =
					    co_await getMutationsForVersion(iterators, minVersion);

					if (minVersion < begin) {
						// skip generating mutations, because this is not within desired range
						// this is already handled by the previous taskfunc
						continue;
					} else if (minVersion >= end) {
						// all valid data has been consumed
						break;
					}

					// transform from new format to old format(param1, param2) for this version.
					// This transformation has to be done version by version.
					Standalone<VectorRef<KeyValueRef>> oldFormatMutations =
					    generateOldFormatMutations(minVersion, mutationsSingleVersion);
					mutationStream.send(oldFormatMutations);
				}
			}
			mutationStream.sendError(end_of_stream());
		} catch (Error& e) {
			TraceEvent(SevWarn, "FileRestoreLogReadError").error(e);
			mutationStream.sendError(e);
		}
		co_return;
	}

	// Reads from all iterators and returns a list of mutations for the given version.
	static Future<std::vector<Standalone<VectorRef<VersionedMutation>>>> getMutationsForVersion(
	    std::vector<Reference<PartitionedLogIterator>> iterators,
	    Version minVersion) {
		std::vector<Standalone<VectorRef<VersionedMutation>>> mutationsSingleVersion;

		for (int k = 0; k < iterators.size(); k++) {
			if (!iterators[k]->hasNext()) {
				continue;
			}
			Version v = co_await iterators[k]->peekNextVersion();
			if (v == minVersion) {
				Standalone<VectorRef<VersionedMutation>> tmp = co_await iterators[k]->getNext();
				mutationsSingleVersion.push_back(tmp);
			}
		}
		co_return mutationsSingleVersion;
	}

	// Writes backup mutations to the database
	static Future<Void> writeMutations(Database cx,
	                                   std::vector<Standalone<VectorRef<KeyValueRef>>> mutations,
	                                   Key mutationLogPrefix,
	                                   Reference<Task> task,
	                                   Reference<TaskBucket> taskBucket) {
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		Standalone<VectorRef<KeyValueRef>> oldFormatMutations;
		int mutationIndex = 0;
		int mutationCount = 0;
		int txBytes = 0;
		int txBytesLimit = CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

		for (const auto& data : mutations) {
			oldFormatMutations.append(oldFormatMutations.arena(), data.begin(), data.size());
		}
		int totalMutation = oldFormatMutations.size();

		// multiple transactions are needed, so this has to be in a _execute method rather than
		// a _finish method
		// this transaction does blind writes, so they are idempotent operations even if multiple instances of
		// the same tasks are running these transactions.
		while (true) {
			Error err;
			try {
				if (mutationIndex == totalMutation) {
					break;
				}
				txBytes = 0;
				mutationCount = 0;
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				while (mutationIndex + mutationCount < totalMutation && txBytes < txBytesLimit) {
					Key k = oldFormatMutations[mutationIndex + mutationCount].key.withPrefix(mutationLogPrefix);
					ValueRef v = oldFormatMutations[mutationIndex + mutationCount]
					                 .value; // each KV is a [param1 with added prefix -> param2]
					tr->set(k, v);
					txBytes += k.expectedSize();
					txBytes += v.expectedSize();
					++mutationCount;
				}
				co_await taskBucket->keepRunning(tr, task);
				co_await tr->commit();

				int64_t oldBytes = Params.bytesWritten().get(task);
				Params.bytesWritten().set(task, oldBytes + txBytes);
				DisabledTraceEvent("FileRestorePartitionedLogCommittData")
				    .detail("MutationIndex", mutationIndex)
				    .detail("MutationCount", mutationCount)
				    .detail("TotalMutation", totalMutation)
				    .detail("Bytes", txBytes);
				mutationIndex += mutationCount; // update mutationIndex after the commit succeeds
				continue;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_transaction_too_large) {
				txBytesLimit /= 2;
			} else {
				co_await tr->onError(err);
			}
		}
		co_return;
	}

	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		RestoreConfig restore(task);

		int64_t maxTagID = Params.maxTagID().get(task);
		std::vector<RestoreConfig::RestoreFile> logs = Params.logs().get(task);
		Version begin = Params.beginVersion().get(task);
		Version end = Params.endVersion().get(task);

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		Reference<IBackupContainer> bc;
		std::vector<KeyRange> ranges; // this is the actual KV, not version
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Reference<IBackupContainer> _bc = co_await restore.sourceContainer().getOrThrow(tr);
				bc = getBackupContainerWithProxy(_bc);

				ranges = co_await restore.getRestoreRangesOrDefault(tr);

				co_await checkTaskVersion(tr->getDatabase(), task, name, version);
				co_await taskBucket->keepRunning(tr, task);

				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		std::vector<std::vector<RestoreConfig::RestoreFile>> filesByTag(maxTagID + 1);
		std::vector<std::vector<Version>> fileEndVersionByTag(maxTagID + 1);

		for (RestoreConfig::RestoreFile& f : logs) {
			// find the tag, aggregate files by tags
			if (f.tagId == -1) {
				// inconsistent data
				TraceEvent(SevError, "PartitionedLogFileNoTag")
				    .detail("FileName", f.fileName)
				    .detail("FileSize", f.fileSize)
				    .log();
			} else {
				filesByTag[f.tagId].push_back(f);
			}
		}
		for (int i = 0; i < maxTagID + 1; i++) {
			std::vector<RestoreConfig::RestoreFile>& files = filesByTag[i];
			int cnt = files.size();
			fileEndVersionByTag[i].resize(cnt);
			for (int j = 0; j < cnt; j++) {
				// [10, 20), [18, 30) -> endVersion is (18, 30) for 2 files so we have [10, 18) and [18, 30)
				// greedy algorithm, because sorted by beginVersion and to minimize duplicate reading
				if (j != cnt - 1 && files[j].endVersion < files[j + 1].version) {
					TraceEvent(SevError, "NonContinuousLog").detail("Tag", i).detail("Index", j).log();
				}
				fileEndVersionByTag[i][j] = (j == cnt - 1 ? end : files[j + 1].version);
			}
		}

		std::vector<Reference<PartitionedLogIterator>> iterators(maxTagID + 1);
		// for each tag, create an iterator
		for (int k = 0; k < filesByTag.size(); k++) {
			iterators[k] =
			    makeReference<PartitionedLogIteratorTwoBuffers>(bc, k, filesByTag[k], fileEndVersionByTag[k]);
		}

		DisabledTraceEvent("FileRestorePartitionedLogDataExeStart")
		    .detail("BeginVersion", begin)
		    .detail("EndVersion", end)
		    .detail("Files", logs.size());

		// Converted backup mutations
		PromiseStream<Standalone<VectorRef<KeyValueRef>>> mutationStream;
		Future<Void> reader = readLogData(mutationStream, iterators, begin, end);

		std::vector<Standalone<VectorRef<KeyValueRef>>> mutations;
		int64_t totalBytes = 0;
		Standalone<VectorRef<KeyValueRef>> oneVersionData;
		while (true) {
			Error err;
			try {
				Standalone<VectorRef<KeyValueRef>> _data = co_await mutationStream.getFuture();
				oneVersionData = _data;

				// batching mutations from multiple versions together before writing to the database
				int64_t bytes = oneVersionData.expectedSize();
				if (totalBytes + bytes > CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE) {
					co_await writeMutations(cx, mutations, restore.mutationLogPrefix(), task, taskBucket);
					mutations.clear();
					totalBytes = 0;
				}
				mutations.push_back(oneVersionData);
				totalBytes += bytes;
				continue;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_end_of_stream) {
				if (!mutations.empty()) {
					co_await writeMutations(cx, mutations, restore.mutationLogPrefix(), task, taskBucket);
				}
				break;
			} else {
				throw err;
			}
		}
		DisabledTraceEvent("FileRestorePartitionedLogDataExeDone")
		    .detail("BeginVersion", begin)
		    .detail("EndVersion", end)
		    .detail("Files", logs.size());

		co_return;
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		int64_t logBytesWritten = Params.bytesWritten().get(task);
		RestoreConfig(task).bytesWritten().atomicOp(tr, logBytesWritten, MutationRef::Type::AddValue);

		int64_t blocks =
		    (logBytesWritten + CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE - 1) / CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE;
		// When dispatching, we don't know how many blocks are there, so we have to do it here
		RestoreConfig(task).filesBlocksDispatched().atomicOp(tr, blocks, MutationRef::Type::AddValue);
		RestoreConfig(task).fileBlocksFinished().atomicOp(tr, blocks, MutationRef::Type::AddValue);

		DisabledTraceEvent("FileRestorePartitionedLogCommittedData")
		    .detail("Blocks", blocks)
		    .detail("LogBytes", logBytesWritten);

		Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		co_await (taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           int64_t maxTagID,
	                           std::vector<RestoreConfig::RestoreFile> logs,
	                           Version begin,
	                           Version end,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = co_await completionKey.get(tr, taskBucket);
		Reference<Task> task(
		    new Task(RestoreLogDataPartitionedTaskFunc::name, RestoreLogDataPartitionedTaskFunc::version, doneKey));

		// Create a restore config from the current task and bind it to the new task.
		// RestoreConfig(parentTask) createsa prefix of : fileRestorePrefixRange.begin/uid->config/[uid]
		co_await RestoreConfig(parentTask).toTask(tr, task);
		Params.maxTagID().set(task, maxTagID);
		Params.beginVersion().set(task, begin);
		Params.endVersion().set(task, end);
		Params.logs().set(task, logs);
		Params.bytesWritten().set(task, 0);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreLogDataPartitionedTaskFunc::name = "restore_log_data_partitioned"_sr;
REGISTER_TASKFUNC(RestoreLogDataPartitionedTaskFunc);

// each task can be partitioned to smaller ranges because commit proxy would
// only start to commit alog/ prefix mutations to original prefix when
// the final version is set, but do it in a single task for now for simplicity
struct RestoreDispatchPartitionedTaskFunc : RestoreTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct {
		static TaskParam<Version> beginVersion() { return __FUNCTION__sr; }
		static TaskParam<Version> firstVersion() { return __FUNCTION__sr; }
		static TaskParam<Version> endVersion() { return __FUNCTION__sr; }
	} Params;

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		RestoreConfig restore(task);

		Version beginVersion = Params.beginVersion().get(task);
		Version firstVersion = Params.firstVersion().get(task);
		Version endVersion = Params.endVersion().get(task);
		Reference<IBackupContainer> _bc = co_await restore.sourceContainer().getOrThrow(tr);
		Reference<IBackupContainer> bc = getBackupContainerWithProxy(_bc);
		Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		Version restoreVersion{ 0 };
		int fileLimit = 1000;

		co_await (store(restoreVersion, restore.restoreVersion().getOrThrow(tr)) &&
		          checkTaskVersion(tr->getDatabase(), task, name, version));

		// if current is [40, 50] and restore version is 50, we need another [50, 51] task to process data at version 50
		Version nextEndVersion =
		    std::min(restoreVersion + 1, endVersion + CLIENT_KNOBS->RESTORE_PARTITIONED_BATCH_VERSION_SIZE);
		// update the apply mutations end version so the mutations from the previous batch can be applied.
		// Only do this once beginVersion is > 0 (it will be 0 for the initial dispatch).
		if (beginVersion > firstVersion) {
			// if the last file is [80, 100] and the restoreVersion is 90, we should use 90 here
			// this is an additional taskFunc after last file
			restore.setApplyEndVersion(tr, std::min(beginVersion, restoreVersion + 1));
		}

		// The applyLag must be retrieved AFTER potentially updating the apply end version.
		int64_t applyLag = co_await restore.getApplyVersionLag(tr);
		// this is to guarantee commit proxy is catching up doing apply alog -> normal key
		// with this  backupFile -> alog process
		// If starting a new batch and the apply lag is too large then re-queue and wait
		if (applyLag > (BUGGIFY ? 1 : CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 300)) {
			// Wait a small amount of time and then re-add this same task.
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
			co_await RestoreDispatchPartitionedTaskFunc::addTask(
			    tr, taskBucket, task, firstVersion, beginVersion, endVersion);

			TraceEvent("FileRestorePartitionDispatch")
			    .detail("RestoreUID", restore.getUid())
			    .detail("BeginVersion", beginVersion)
			    .detail("ApplyLag", applyLag)
			    .detail("Decision", "too_far_behind");

			co_await taskBucket->finish(tr, task);
			co_return;
		}

		// Get a batch of files.  We're targeting batchSize blocks(30k) being dispatched so query for batchSize(150)
		// files (each of which is 0 or more blocks).
		// lets say files have [10, 20), [20, 30) then if our range is [15, 25], we need to include both files,
		// because [15, 20] is included in the first file, and [20, 25] is included in the second file
		// say we have b and e
		// as a result, the first file(inclusive): largest file whose begin <= b,
		// the last file(exclusive): smallest file whose begin > e
		// because of the encoding, version comes before the type of files(log or range),
		// begin needs to start from the beginning --
		// if we have a 3 files, (log,100,200), (range, 180), (log, 200, 300), then if we want to restore[190, 250]
		// it would stop at (range, 180) and not looking at (log, 100, 200)
		Optional<RestoreConfig::RestoreFile> beginLogInclude = Optional<RestoreConfig::RestoreFile>{};
		// because of the encoding of RestoreFile, we use greaterThanOrEqual(end + 1) instead of greaterThan(end)
		// because RestoreFile::pack has the version at the most significant position, and keyAfter(end) does not result
		// in a end+1
		Optional<RestoreConfig::RestoreFile> endLogExclude = co_await restore.logFileSet().seekGreaterOrEqual(
		    tr, RestoreConfig::RestoreFile({ endVersion + 1, "", false }));
		RestoreConfig::FileSetT::RangeResultType logFiles =
		    co_await restore.logFileSet().getRange(tr, beginLogInclude, endLogExclude, fileLimit);
		Optional<RestoreConfig::RestoreFile> beginRangeInclude = co_await restore.rangeFileSet().seekGreaterOrEqual(
		    tr, RestoreConfig::RestoreFile({ beginVersion, "", true }));
		// greaterThanOrEqual(end + 1) instead of greaterThan(end)
		// because RestoreFile::pack has the version at the most significant position, and keyAfter(end) does not result
		// in a end+1
		Optional<RestoreConfig::RestoreFile> endRangeExclude = co_await restore.rangeFileSet().seekGreaterOrEqual(
		    tr, RestoreConfig::RestoreFile({ endVersion + 1, "", true }));
		RestoreConfig::FileSetT::RangeResultType rangeFiles =
		    co_await restore.rangeFileSet().getRange(tr, beginRangeInclude, endRangeExclude, fileLimit);
		int64_t maxTagID = 0;
		std::vector<RestoreConfig::RestoreFile> logs;
		std::vector<RestoreConfig::RestoreFile> ranges;
		for (const auto& f : logFiles.results) {
			if (f.endVersion > beginVersion) {
				// skip all files whose endVersion is smaller or equal to beginVersion
				logs.push_back(f);
				maxTagID = std::max(maxTagID, f.tagId);
			}
		}
		for (const auto& f : rangeFiles.results) {
			// the getRange might get out-of-bound range file because log files need them to work
			if (f.version >= beginVersion && f.version < endVersion) {
				ranges.push_back(f);
			}
		}
		// allPartsDone will be set once all block tasks in the current batch are finished.
		// create a new future for the new batch
		Reference<TaskFuture> allPartsDone = futureBucket->future(tr);
		restore.batchFuture().set(tr, allPartsDone->pack());

		// if there are no files, if i am not the last batch, then on to the next batch
		// if there are no files and i am the last batch, then just wait for applying to finish
		// do we need this files.results.size() == 0 at all?
		if (beginVersion > restoreVersion) {
			if (applyLag == 0) {
				// i am the last batch
				// If apply lag is 0 then we are done so create the completion task
				co_await RestoreCompleteTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal());

				TraceEvent("FileRestorePartitionDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "restore_complete");
			} else {
				// i am the last batch, and applyLag is not zero, then I will create another dummy task to wait
				// for apply log to be zero, then it will go into the branch above.
				// Applying of mutations is not yet finished so wait a small amount of time and then re-add this
				// same task.
				// this is only to create a dummy one wait for it to finish
				co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
				co_await RestoreDispatchPartitionedTaskFunc::addTask(
				    tr, taskBucket, task, firstVersion, beginVersion, endVersion);

				TraceEvent("FileRestorePartitionDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "apply_still_behind");
			}
			co_await taskBucket->finish(tr, task);
			co_return;
		}

		// if we reach here, this batch is not empty(i.e. we have range and/or mutation files in this)
		// Start moving through the file list and queuing up blocks.  Only queue up to RESTORE_DISPATCH_ADDTASK_SIZE
		// blocks per Dispatch task and target batchSize total per batch but a batch must end on a complete version
		// boundary so exceed the limit if necessary to reach the end of a version of files.
		std::vector<Future<Key>> addTaskFutures;
		int i = 0;
		// need to process all range files, keep using the same RestoreRangeTaskFunc as non-partitioned restore.
		// this can be done first, because they are not overlap within a restore uid
		// each task will read the file, restore those key to their original keys after clear that range
		// also it will update the keyVersionMap[key -> versionFromRangeFile]
		// by this time, corresponding mutation files within the same version range has not been applied yet
		// because they are waiting for the singal of this RestoreDispatchPartitionedTaskFunc
		// when log are being applied, they will compare version of key to the keyVersionMap updated by range file
		// after each RestoreDispatchPartitionedTaskFunc, keyVersionMap will be clear if mutation version is larger.
		for (; i < ranges.size(); ++i) {
			RestoreConfig::RestoreFile& f = ranges[i];
			// For each block of the file
			for (int64_t j = 0; j < f.fileSize; j += f.blockSize) {
				addTaskFutures.push_back(RestoreRangeTaskFunc::addTask(tr,
				                                                       taskBucket,
				                                                       task,
				                                                       f,
				                                                       j,
				                                                       std::min<int64_t>(f.blockSize, f.fileSize - j),
				                                                       TaskCompletionKey::joinWith(allPartsDone)));
			}
		}
		addTaskFutures.push_back(RestoreLogDataPartitionedTaskFunc::addTask(
		    tr, taskBucket, task, maxTagID, logs, beginVersion, endVersion, TaskCompletionKey::joinWith(allPartsDone)));
		// even if file exsists, but they are empty, in this case just start the next batch

		addTaskFutures.push_back(RestoreDispatchPartitionedTaskFunc::addTask(tr,
		                                                                     taskBucket,
		                                                                     task,
		                                                                     firstVersion,
		                                                                     endVersion,
		                                                                     nextEndVersion,
		                                                                     TaskCompletionKey::noSignal(),
		                                                                     allPartsDone));

		co_await waitForAll(addTaskFutures);
		co_await taskBucket->finish(tr, task);

		TraceEvent("FileRestorePartitionDispatch")
		    .detail("RestoreUID", restore.getUid())
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion)
		    .detail("NextEndVersion", nextEndVersion)
		    .detail("ApplyLag", applyLag)
		    .detail("Decision", "dispatch_batch_complete");

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           Version firstVersion,
	                           Version beginVersion,
	                           Version endVersion,
	                           TaskCompletionKey completionKey = TaskCompletionKey::noSignal(),
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = co_await completionKey.get(tr, taskBucket);

		// Use high priority for dispatch tasks that have to queue more blocks for the current batch
		unsigned int priority = 0;
		Reference<Task> task(new Task(
		    RestoreDispatchPartitionedTaskFunc::name, RestoreDispatchPartitionedTaskFunc::version, doneKey, priority));

		// Create a config from the parent task and bind it to the new task
		co_await RestoreConfig(parentTask).toTask(tr, task);
		Params.firstVersion().set(task, firstVersion);
		Params.beginVersion().set(task, beginVersion);
		Params.endVersion().set(task, endVersion);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreDispatchPartitionedTaskFunc::name = "restore_dispatch_partitioned"_sr;
REGISTER_TASKFUNC(RestoreDispatchPartitionedTaskFunc);

struct RestoreDispatchTaskFunc : RestoreTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct {
		static TaskParam<Version> beginVersion() { return __FUNCTION__sr; }
		static TaskParam<std::string> beginFile() { return __FUNCTION__sr; }
		static TaskParam<int64_t> beginBlock() { return __FUNCTION__sr; }
		static TaskParam<int64_t> batchSize() { return __FUNCTION__sr; }
		static TaskParam<int64_t> remainingInBatch() { return __FUNCTION__sr; }
	} Params;

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		RestoreConfig restore(task);

		Version beginVersion = Params.beginVersion().get(task);
		Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		int64_t remainingInBatch = Params.remainingInBatch().get(task);
		bool addingToExistingBatch = remainingInBatch > 0;
		Version restoreVersion{ 0 };

		co_await (store(restoreVersion, restore.restoreVersion().getOrThrow(tr)) &&
		          checkTaskVersion(tr->getDatabase(), task, name, version));

		// If not adding to an existing batch then update the apply mutations end version so the mutations from the
		// previous batch can be applied.  Only do this once beginVersion is > 0 (it will be 0 for the initial
		// dispatch).
		if (!addingToExistingBatch && beginVersion > 0) {
			// unblock apply alog to normal key space
			// if the last file is [80, 100] and the restoreVersion is 90, we should use 90 here
			// this call an additional call after last file
			restore.setApplyEndVersion(tr, std::min(beginVersion, restoreVersion + 1));
		}

		// The applyLag must be retrieved AFTER potentially updating the apply end version.
		int64_t applyLag = co_await restore.getApplyVersionLag(tr);
		int64_t batchSize = Params.batchSize().get(task);

		// If starting a new batch and the apply lag is too large then re-queue and wait
		if (!addingToExistingBatch && applyLag > (BUGGIFY ? 1 : CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 300)) {
			// Wait a small amount of time and then re-add this same task.
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
			co_await RestoreDispatchTaskFunc::addTask(
			    tr, taskBucket, task, beginVersion, "", 0, batchSize, remainingInBatch);

			TraceEvent("FileRestoreDispatch")
			    .detail("RestoreUID", restore.getUid())
			    .detail("BeginVersion", beginVersion)
			    .detail("ApplyLag", applyLag)
			    .detail("BatchSize", batchSize)
			    .detail("Decision", "too_far_behind");

			co_await taskBucket->finish(tr, task);
			co_return;
		}

		// need beginFile to handle stop in the middle of version case
		std::string beginFile = Params.beginFile().getOrDefault(task);
		// Get a batch of files.  We're targeting batchSize blocks being dispatched so query for batchSize files
		// (each of which is 0 or more blocks).
		int taskBatchSize = BUGGIFY ? 1 : CLIENT_KNOBS->RESTORE_DISPATCH_ADDTASK_SIZE;
		RestoreConfig::FileSetT::RangeResultType files = co_await restore.fileSet().getRange(
		    tr, Optional<RestoreConfig::RestoreFile>({ beginVersion, beginFile }), {}, taskBatchSize);

		// allPartsDone will be set once all block tasks in the current batch are finished.
		Reference<TaskFuture> allPartsDone;

		// If adding to existing batch then join the new block tasks to the existing batch future
		if (addingToExistingBatch) {
			Key fKey = co_await restore.batchFuture().getD(tr);
			allPartsDone = makeReference<TaskFuture>(futureBucket, fKey);
		} else {
			// Otherwise create a new future for the new batch
			allPartsDone = futureBucket->future(tr);
			restore.batchFuture().set(tr, allPartsDone->pack());
			// Set batch quota remaining to batch size
			remainingInBatch = batchSize;
		}

		// If there were no files to load then this batch is done and restore is almost done.
		if (files.results.empty()) {
			// If adding to existing batch then blocks could be in progress so create a new Dispatch task that waits
			// for them to finish
			if (addingToExistingBatch) {
				// Setting next begin to restoreVersion + 1 so that any files in the file map at the restore version
				// won't be dispatched again.
				co_await RestoreDispatchTaskFunc::addTask(tr,
				                                          taskBucket,
				                                          task,
				                                          restoreVersion + 1,
				                                          "",
				                                          0,
				                                          batchSize,
				                                          0,
				                                          TaskCompletionKey::noSignal(),
				                                          allPartsDone);

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("BeginFile", Params.beginFile().get(task))
				    .detail("BeginBlock", Params.beginBlock().get(task))
				    .detail("RestoreVersion", restoreVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "end_of_final_batch");
			} else if (beginVersion < restoreVersion) {
				// If beginVersion is less than restoreVersion then do one more dispatch task to get there
				// there are no more files between beginVersion and restoreVersion
				co_await RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, restoreVersion, "", 0, batchSize);

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("BeginFile", Params.beginFile().get(task))
				    .detail("BeginBlock", Params.beginBlock().get(task))
				    .detail("RestoreVersion", restoreVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "apply_to_restore_version");
			} else if (applyLag == 0) {
				// If apply lag is 0 then we are done so create the completion task
				co_await RestoreCompleteTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal());

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("BeginFile", Params.beginFile().get(task))
				    .detail("BeginBlock", Params.beginBlock().get(task))
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "restore_complete");
			} else {
				// Applying of mutations is not yet finished so wait a small amount of time and then re-add this
				// same task.
				// this is only to create a dummy one wait for it to finish
				co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
				co_await RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, beginVersion, "", 0, batchSize);

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "apply_still_behind");
			}

			// If adding to existing batch then task is joined with a batch future so set done future
			// Note that this must be done after joining at least one task with the batch future in case all other
			// blockers already finished.
			Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

			co_await (taskBucket->finish(tr, task) && setDone);
			co_return;
		}

		// Start moving through the file list and queuing up blocks.  Only queue up to RESTORE_DISPATCH_ADDTASK_SIZE
		// blocks per Dispatch task and target batchSize total per batch but a batch must end on a complete version
		// boundary so exceed the limit if necessary to reach the end of a version of files.
		std::vector<Future<Key>> addTaskFutures;
		Version endVersion = files.results[0].version;
		int blocksDispatched = 0;
		int64_t beginBlock = Params.beginBlock().getOrDefault(task);
		int i = 0;

		// for each file
		// not creating a new task at this level because restore files are read back together -- both range and log
		// so i have to process range files anyway.
		for (; i < files.results.size(); ++i) {
			RestoreConfig::RestoreFile& f = files.results[i];

			// Here we are "between versions" (prior to adding the first block of the first file of a new version)
			// so this is an opportunity to end the current dispatch batch (which must end on a version boundary) if
			// the batch size has been reached or exceeded
			if (f.version != endVersion && remainingInBatch <= 0) {
				// Next start will be at the first version after endVersion at the first file first block
				++endVersion;
				// beginFile set to empty to indicate we are not in the middle of a range
				// by middle of a range, we mean that we have rangeFile v=80, and logFile v=[80, 100],
				// then we have to include this log file too in this batch
				beginFile = "";
				beginBlock = 0;
				break;
			}

			// Set the starting point for the next task in case we stop inside this file
			endVersion = f.version;
			beginFile = f.fileName;

			int64_t j = beginBlock * f.blockSize;
			// For each block of the file
			for (; j < f.fileSize; j += f.blockSize) {
				// Stop if we've reached the addtask limit
				if (blocksDispatched == taskBatchSize)
					break;

				if (f.isRange) {
					addTaskFutures.push_back(
					    RestoreRangeTaskFunc::addTask(tr,
					                                  taskBucket,
					                                  task,
					                                  f,
					                                  j,
					                                  std::min<int64_t>(f.blockSize, f.fileSize - j),
					                                  TaskCompletionKey::joinWith(allPartsDone)));
				} else {
					addTaskFutures.push_back(
					    RestoreLogDataTaskFunc::addTask(tr,
					                                    taskBucket,
					                                    task,
					                                    f,
					                                    j,
					                                    std::min<int64_t>(f.blockSize, f.fileSize - j),
					                                    TaskCompletionKey::joinWith(allPartsDone)));
				}

				// Increment beginBlock for the file and total blocks dispatched for this task
				++beginBlock;
				++blocksDispatched;
				--remainingInBatch;
			}

			// Stop if we've reached the addtask limit
			if (blocksDispatched == taskBatchSize)
				break;

			// We just completed an entire file so the next task should start at the file after this one within
			// endVersion (or later) if this iteration ends up being the last for this task
			beginFile = beginFile + '\x00';
			beginBlock = 0;

			TraceEvent("FileRestoreDispatchedFile")
			    .suppressFor(60)
			    .detail("RestoreUID", restore.getUid())
			    .detail("FileName", f.fileName);
		}

		// If no blocks were dispatched then the next dispatch task should run now and be joined with the
		// allPartsDone future
		if (blocksDispatched == 0) {
			std::string decision;

			// If no files were dispatched either then the batch size wasn't large enough to catch all of the files
			// at the next lowest non-dispatched version, so increase the batch size.
			if (i == 0) {
				batchSize *= 2;
				decision = "increased_batch_size";
			} else
				decision = "all_files_were_empty";

			TraceEvent("FileRestoreDispatch")
			    .detail("RestoreUID", restore.getUid())
			    .detail("BeginVersion", beginVersion)
			    .detail("BeginFile", Params.beginFile().get(task))
			    .detail("BeginBlock", Params.beginBlock().get(task))
			    .detail("EndVersion", endVersion)
			    .detail("ApplyLag", applyLag)
			    .detail("BatchSize", batchSize)
			    .detail("Decision", decision)
			    .detail("RemainingInBatch", remainingInBatch);

			co_await RestoreDispatchTaskFunc::addTask(tr,
			                                          taskBucket,
			                                          task,
			                                          endVersion,
			                                          beginFile,
			                                          beginBlock,
			                                          batchSize,
			                                          remainingInBatch,
			                                          TaskCompletionKey::joinWith((allPartsDone)));

			// If adding to existing batch then task is joined with a batch future so set done future.
			// Note that this must be done after joining at least one task with the batch future in case all other
			// blockers already finished.
			Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

			co_await (setDone && taskBucket->finish(tr, task));

			co_return;
		}

		// Increment the number of blocks dispatched in the restore config
		restore.filesBlocksDispatched().atomicOp(tr, blocksDispatched, MutationRef::Type::AddValue);

		// If beginFile is not empty then we had to stop in the middle of a version (possibly within a file) so we
		// cannot end the batch here because we do not know if we got all of the files and blocks from the last
		// version queued, so make sure remainingInBatch is at least 1.
		if (!beginFile.empty()) {
			// this is to make sure if we stop in the middle of a version, we do not end this batch
			// instead next RestoreDispatchTaskFunc should have addingToExistingBatch as true
			// thus they are considered the same batch and alog will be committed only when all of them succeed
			remainingInBatch = std::max<int64_t>(1, remainingInBatch);
		}

		// If more blocks need to be dispatched in this batch then add a follow-on task that is part of the
		// allPartsDone group which will won't wait to run and will add more block tasks.
		if (remainingInBatch > 0)
			addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr,
			                                                          taskBucket,
			                                                          task,
			                                                          endVersion,
			                                                          beginFile,
			                                                          beginBlock,
			                                                          batchSize,
			                                                          remainingInBatch,
			                                                          TaskCompletionKey::joinWith(allPartsDone)));
		else // Otherwise, add a follow-on task to continue after all previously dispatched blocks are done
			addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr,
			                                                          taskBucket,
			                                                          task,
			                                                          endVersion,
			                                                          beginFile,
			                                                          beginBlock,
			                                                          batchSize,
			                                                          0,
			                                                          TaskCompletionKey::noSignal(),
			                                                          allPartsDone));

		co_await waitForAll(addTaskFutures);

		// If adding to existing batch then task is joined with a batch future so set done future.
		Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

		co_await (setDone && taskBucket->finish(tr, task));

		TraceEvent("FileRestoreDispatch")
		    .detail("RestoreUID", restore.getUid())
		    .detail("BeginVersion", beginVersion)
		    .detail("BeginFile", Params.beginFile().get(task))
		    .detail("BeginBlock", Params.beginBlock().get(task))
		    .detail("EndVersion", endVersion)
		    .detail("ApplyLag", applyLag)
		    .detail("BatchSize", batchSize)
		    .detail("Decision", "dispatched_files")
		    .detail("FilesDispatched", i)
		    .detail("BlocksDispatched", blocksDispatched)
		    .detail("RemainingInBatch", remainingInBatch);

		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           Reference<Task> parentTask,
	                           Version beginVersion,
	                           std::string beginFile,
	                           int64_t beginBlock,
	                           int64_t batchSize,
	                           int64_t remainingInBatch = 0,
	                           TaskCompletionKey completionKey = TaskCompletionKey::noSignal(),
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = co_await completionKey.get(tr, taskBucket);

		// Use high priority for dispatch tasks that have to queue more blocks for the current batch
		auto priority = (remainingInBatch > 0) ? 1u : 0u;
		Reference<Task> task(
		    new Task(RestoreDispatchTaskFunc::name, RestoreDispatchTaskFunc::version, doneKey, priority));

		// Create a config from the parent task and bind it to the new task
		co_await RestoreConfig(parentTask).toTask(tr, task);
		Params.beginVersion().set(task, beginVersion);
		Params.batchSize().set(task, batchSize);
		Params.remainingInBatch().set(task, remainingInBatch);
		Params.beginBlock().set(task, beginBlock);
		Params.beginFile().set(task, beginFile);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreDispatchTaskFunc::name = "restore_dispatch"_sr;
REGISTER_TASKFUNC(RestoreDispatchTaskFunc);

Future<std::string> restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	std::vector<KeyBackedTag> tags;
	if (tagName.empty()) {
		std::vector<KeyBackedTag> t = co_await getAllRestoreTags(tr);
		tags = t;
	} else
		tags.push_back(makeRestoreTag(tagName.toString()));

	// If no tags found, return helpful message
	if (tags.empty()) {
		co_return "No restores found.\n\n"
		          "To start a restore:\n"
		          "  fdbrestore start -r <BACKUP_URL> [-t <TAG>]\n";
	}

	std::string result;
	for (int i = 0; i < tags.size(); ++i) {
		UidAndAbortedFlagT u = co_await tags[i].getD(tr);
		std::string s = co_await RestoreConfig(u.first).getFullStatus(tr);
		result.append(s);
		result.append("\n\n");
	}

	co_return result;
}

Future<ERestoreState> abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

	KeyBackedTag tag = makeRestoreTag(tagName.toString());
	Optional<UidAndAbortedFlagT> current = co_await tag.get(tr);
	if (!current.present())
		co_return ERestoreState::UNINITIALIZED;

	RestoreConfig restore(current.get().first);

	ERestoreState status = co_await restore.stateEnum().getD(tr);
	bool runnable = co_await restore.isRunnable(tr);

	if (!runnable)
		co_return status;

	restore.stateEnum().set(tr, ERestoreState::ABORTED);

	// Clear all of the ApplyMutations stuff
	restore.clearApplyMutationsKeys(tr);

	// Cancel the backup tasks on this tag
	co_await tag.cancel(tr);

	co_await unlockDatabase(tr, current.get().first);
	co_return ERestoreState::ABORTED;
}

Future<ERestoreState> abortRestore(Database cx, Key tagName) {
	auto tr = makeReference<ReadYourWritesTransaction>(cx);

	while (true) {
		Error err;
		try {
			ERestoreState estate = co_await abortRestore(tr, tagName);
			if (estate != ERestoreState::ABORTED) {
				co_return estate;
			}
			co_await tr->commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}

	tr = makeReference<ReadYourWritesTransaction>(cx);

	// Commit a dummy transaction before returning success, to ensure the mutation applier has stopped submitting
	// mutations
	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
			tr->addReadConflictRange(singleKeyRange(KeyRef()));
			tr->addWriteConflictRange(singleKeyRange(KeyRef()));
			co_await tr->commit();
			co_return ERestoreState::ABORTED;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

struct StartFullRestoreTaskFunc : RestoreTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> firstVersion() { return __FUNCTION__sr; }
	} Params;

	// Find all files needed for the restore and save them in the RestoreConfig for the task.
	// Update the total number of files and blocks and change state to starting.
	static Future<Void> _execute(Database cx,
	                             Reference<TaskBucket> taskBucket,
	                             Reference<FutureBucket> futureBucket,
	                             Reference<Task> task) {
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		RestoreConfig restore(task);
		Version restoreVersion{ 0 };
		Version beginVersion{ 0 };
		Reference<IBackupContainer> bc;
		std::vector<KeyRange> ranges;
		bool logsOnly{ false };
		bool inconsistentSnapshotOnly{ false };

		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				co_await checkTaskVersion(tr->getDatabase(), task, name, version);
				beginVersion = co_await restore.beginVersion().getD(tr, Snapshot::False, ::invalidVersion);

				restoreVersion = co_await restore.restoreVersion().getOrThrow(tr);
				ranges = co_await restore.getRestoreRangesOrDefault(tr);
				logsOnly = co_await restore.onlyApplyMutationLogs().getD(tr, Snapshot::False, false);
				inconsistentSnapshotOnly = co_await restore.inconsistentSnapshotOnly().getD(tr, Snapshot::False, false);
				co_await taskBucket->keepRunning(tr, task);

				ERestoreState oldState = co_await restore.stateEnum().getD(tr);
				if (oldState != ERestoreState::QUEUED && oldState != ERestoreState::STARTING) {
					co_await restore.logError(cx,
					                          restore_error(),
					                          format("StartFullRestore: Encountered unexpected state(%d)", oldState),
					                          nullptr);
					co_return;
				}
				restore.stateEnum().set(tr, ERestoreState::STARTING);
				restore.fileSet().clear(tr);
				restore.fileBlockCount().clear(tr);
				restore.fileCount().clear(tr);
				Reference<IBackupContainer> _bc = co_await restore.sourceContainer().getOrThrow(tr);
				bc = getBackupContainerWithProxy(_bc);

				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		tr->reset();
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Version destVersion = co_await tr->getReadVersion();
				TraceEvent("FileRestoreVersionUpgrade")
				    .detail("RestoreVersion", restoreVersion)
				    .detail("Dest", destVersion);
				if (destVersion <= restoreVersion) {
					CODE_PROBE(true, "Forcing restored cluster to higher version");
					tr->set(minRequiredCommitVersionKey, BinaryWriter::toValue(restoreVersion + 1, Unversioned()));
					co_await tr->commit();
					tr->reset();
					continue;
				} else {
					break;
				}
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		Version firstConsistentVersion = invalidVersion;
		if (beginVersion == invalidVersion) {
			beginVersion = 0;
		}
		Standalone<VectorRef<KeyRangeRef>> keyRangesFilter;
		for (auto const& r : ranges) {
			keyRangesFilter.push_back_deep(keyRangesFilter.arena(), KeyRangeRef(r));
		}
		Optional<RestorableFileSet> restorable =
		    co_await bc->getRestoreSet(restoreVersion, keyRangesFilter, logsOnly, beginVersion);
		if (!restorable.present())
			throw restore_missing_data();

		// Convert the two lists in restorable (logs and ranges) to a single list of RestoreFiles.
		// Order does not matter, they will be put in order when written to the restoreFileMap below.
		std::vector<RestoreConfig::RestoreFile> files;
		std::vector<RestoreConfig::RestoreFile> logFiles;
		std::vector<RestoreConfig::RestoreFile> rangeFiles;
		if (!logsOnly) {
			beginVersion = restorable.get().snapshot.beginVersion;

			if (!inconsistentSnapshotOnly) {
				for (const RangeFile& f : restorable.get().ranges) {
					files.push_back({ f.version, f.fileName, true, f.blockSize, f.fileSize });
					rangeFiles.push_back({ f.version, f.fileName, true, f.blockSize, f.fileSize });
					// In a restore with both snapshots and logs, the firstConsistentVersion is the highest version
					// of any range file.
					firstConsistentVersion = std::max(firstConsistentVersion, f.version);
				}
			} else {
				for (int i = 0; i < restorable.get().ranges.size(); ++i) {
					const RangeFile& f = restorable.get().ranges[i];
					files.push_back({ f.version, f.fileName, true, f.blockSize, f.fileSize });
					rangeFiles.push_back({ f.version, f.fileName, true, f.blockSize, f.fileSize });
					// In inconsistentSnapshotOnly mode, if all range files have the same version, then it is the
					// firstConsistentVersion, otherwise unknown (use -1).
					if (i != 0 && f.version != firstConsistentVersion) {
						firstConsistentVersion = invalidVersion;
					} else {
						firstConsistentVersion = f.version;
					}
				}
			}
		} else {
			// In logs-only (incremental) mode, the firstConsistentVersion should just be restore.beginVersion().
			firstConsistentVersion = beginVersion;
		}
		if (!inconsistentSnapshotOnly) {
			for (const LogFile& f : restorable.get().logs) {
				files.push_back(
				    { f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion, f.tagId, f.totalTags });
				logFiles.push_back(
				    { f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion, f.tagId, f.totalTags });
			}
		}
		// First version for which log data should be applied
		Params.firstVersion().set(task, beginVersion);

		tr->reset();
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				restore.firstConsistentVersion().set(tr, firstConsistentVersion);
				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// add log files
		auto logStart = logFiles.begin();
		auto logEnd = logFiles.end();
		int txBytes = 0;

		tr->reset();
		while (logStart != logEnd) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				co_await taskBucket->keepRunning(tr, task);

				auto logIt = logStart;

				txBytes = 0;
				int logFileCount = 0;
				auto fileSet = restore.logFileSet();
				// TODO: split files into multiple keys, because files can be in the order of 10k or 100k, which
				// probably can't fit due to size limit for a value in FDB. as a result, fileSet has everything,
				// including [beginVersion, endVersion] for each tag
				for (; logIt != logEnd && txBytes < 1e6; ++logIt) {
					txBytes += fileSet.insert(tr, *logIt);
					++logFileCount;
				}
				co_await tr->commit();

				TraceEvent("FileRestoreLoadedLogFiles")
				    .detail("RestoreUID", restore.getUid())
				    .detail("FileCount", logFileCount)
				    .detail("TransactionBytes", txBytes);

				logStart = logIt;
				tr->reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		auto rangeStart = rangeFiles.begin();
		auto rangeEnd = rangeFiles.end();

		tr->reset();
		while (rangeStart != rangeEnd) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				co_await taskBucket->keepRunning(tr, task);

				auto rangeIt = rangeStart;

				txBytes = 0;
				int rangeFileCount = 0;
				auto fileSet = restore.rangeFileSet();
				// as a result, fileSet has everything, including [beginVersion, endVersion] for each tag
				for (; rangeIt != rangeEnd && txBytes < 1e6; ++rangeIt) {
					txBytes += fileSet.insert(tr, *rangeIt);
					// handle the remaining
					++rangeFileCount;
				}
				co_await tr->commit();

				TraceEvent("FileRestoreLoadedRangeFiles")
				    .detail("RestoreUID", restore.getUid())
				    .detail("FileCount", rangeFileCount)
				    .detail("TransactionBytes", txBytes);

				rangeStart = rangeIt;
				tr->reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// add files
		auto start = files.begin();
		auto end = files.end();

		tr->reset();
		while (start != end) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				co_await taskBucket->keepRunning(tr, task);

				auto it = start;

				txBytes = 0;
				int nFileBlocks = 0;
				int nFiles = 0;
				auto fileSet = restore.fileSet();
				// as a result, fileSet has everything, including [beginVersion, endVersion] for each tag
				for (; it != end && txBytes < 1e6; ++it) {
					txBytes += fileSet.insert(tr, *it);
					// handle the remaining
					nFileBlocks += (it->fileSize + it->blockSize - 1) / it->blockSize;
					++nFiles;
				}

				restore.fileCount().atomicOp(tr, nFiles, MutationRef::Type::AddValue);
				restore.fileBlockCount().atomicOp(tr, nFileBlocks, MutationRef::Type::AddValue);

				co_await tr->commit();

				TraceEvent("FileRestoreLoadedFiles")
				    .detail("RestoreUID", restore.getUid())
				    .detail("FileCount", nFiles)
				    .detail("FileBlockCount", nFileBlocks)
				    .detail("TransactionBytes", txBytes);

				start = it;
				tr->reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		co_return;
	}

	static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                            Reference<TaskBucket> taskBucket,
	                            Reference<FutureBucket> futureBucket,
	                            Reference<Task> task) {
		RestoreConfig restore(task);
		bool transformPartitionedLog{ false };
		Version restoreVersion{ 0 };
		Version firstVersion = Params.firstVersion().getOrDefault(task, invalidVersion);
		bool useRangeFileRestore = false;

		if (firstVersion == invalidVersion) {
			co_await restore.logError(
			    tr->getDatabase(), restore_missing_data(), "StartFullRestore: The backup had no data.", nullptr);
			std::string tag = co_await restore.tag().getD(tr);
			co_await abortRestore(tr, StringRef(tag));
			co_return;
		}

		restore.stateEnum().set(tr, ERestoreState::RUNNING);

		// Check if using traditional rangefile restore instead of BulkLoad
		// Default to true (traditional rangefile restore) for backward compatibility
		Optional<bool> rangeFileRestore = co_await restore.useRangeFileRestore().get(tr);
		useRangeFileRestore = !rangeFileRestore.present() || rangeFileRestore.get();

		// Set applyMutation versions
		restore.setApplyBeginVersion(tr, firstVersion);
		restore.setApplyEndVersion(tr, firstVersion);

		// Apply range data using either BulkLoad or traditional range file restore
		transformPartitionedLog = co_await restore.transformPartitionedLog().getD(tr, Snapshot::False, false);
		restoreVersion = co_await restore.restoreVersion().getOrThrow(tr);

		if (!useRangeFileRestore) {
			// Use BulkLoad for range data restoration, then apply logs
			Reference<IBackupContainer> bc = co_await restore.sourceContainer().getOrThrow(tr);

			// Get the BulkDump job ID - this should have been stored by the backup
			// when it completed the BulkDump phase
			Optional<std::string> bulkDumpJobIdOpt = co_await restore.bulkDumpJobId().get(tr);
			std::string bulkDumpJobId = bulkDumpJobIdOpt.present() ? bulkDumpJobIdOpt.get() : "";

			// Save original BulkLoad mode BEFORE creating the task, so it's persisted in the database.
			// This allows crash recovery to restore the correct mode even if the task restarts.
			int currentBulkLoadMode = co_await getBulkLoadMode(tr->getDatabase());
			restore.originalBulkLoadMode().set(tr, currentBulkLoadMode);

			TraceEvent("StartFullRestoreUsingBulkLoad")
			    .detail("RestoreUID", restore.getUid())
			    .detail("BackupUrl", bc->getURL())
			    .detail("BulkDumpJobId", bulkDumpJobId)
			    .detail("RestoreVersion", restoreVersion);

			// Create a future that BulkLoad will signal when done
			Reference<TaskFuture> bulkLoadDone = futureBucket->future(tr);

			// Add BulkLoad task for range data
			co_await BulkLoadRestoreTaskFunc::addTask(tr,
			                                          taskBucket,
			                                          task,
			                                          restoreVersion,
			                                          bc->getURL(),
			                                          bulkDumpJobId,
			                                          TaskCompletionKey::signal(bulkLoadDone));

			// After BulkLoad completes, run RestoreDispatch to apply mutation logs
			// Set onlyApplyMutationLogs so it only processes logs, not range files
			restore.onlyApplyMutationLogs().set(tr, true);

			// Add RestoreDispatch task that waits for BulkLoad to complete
			co_await RestoreDispatchTaskFunc::addTask(tr,
			                                          taskBucket,
			                                          task,
			                                          0,
			                                          "",
			                                          0,
			                                          CLIENT_KNOBS->RESTORE_DISPATCH_BATCH_SIZE,
			                                          0,
			                                          TaskCompletionKey::noSignal(),
			                                          bulkLoadDone);
		} else if (transformPartitionedLog) {
			// Traditional restore with partitioned logs
			Version endVersion =
			    std::min(firstVersion + CLIENT_KNOBS->RESTORE_PARTITIONED_BATCH_VERSION_SIZE, restoreVersion);
			co_await RestoreDispatchPartitionedTaskFunc::addTask(
			    tr, taskBucket, task, firstVersion, firstVersion, endVersion);
		} else {
			// Traditional restore with non-partitioned logs
			co_await RestoreDispatchTaskFunc::addTask(
			    tr, taskBucket, task, 0, "", 0, CLIENT_KNOBS->RESTORE_DISPATCH_BATCH_SIZE);
		}

		// Initialize apply mutations map.
		Future<Optional<bool>> logsOnly = restore.onlyApplyMutationLogs().get(tr);
		co_await logsOnly;
		if (logsOnly.get().present() && logsOnly.get().get()) {
			//  If this is an incremental restore, we need to set the applyMutationsMapPrefix
			//  to the earliest log version so no mutations are missed
			Value versionEncoded = BinaryWriter::toValue(Params.firstVersion().get(task), Unversioned());
			co_await krmSetRange(tr, restore.applyMutationsMapPrefix(), normalKeys, versionEncoded);
		}

		co_await taskBucket->finish(tr, task);
		co_return;
	}

	static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                           Reference<TaskBucket> taskBucket,
	                           UID uid,
	                           TaskCompletionKey completionKey,
	                           Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		Key doneKey = co_await completionKey.get(tr, taskBucket);
		Reference<Task> task(new Task(StartFullRestoreTaskFunc::name, StartFullRestoreTaskFunc::version, doneKey));

		RestoreConfig restore(uid);
		// Bind the restore config to the new task
		co_await restore.toTask(tr, task);

		if (!waitFor) {
			co_return taskBucket->addTask(tr, task);
		}

		co_await waitFor->onSetAddTask(tr, taskBucket, task);
		co_return "OnSetAddTask"_sr;
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef StartFullRestoreTaskFunc::name = "restore_start"_sr;
REGISTER_TASKFUNC(StartFullRestoreTaskFunc);
} // namespace fileBackup

struct LogInfo : public ReferenceCounted<LogInfo> {
	std::string fileName;
	Reference<IAsyncFile> logFile;
	Version beginVersion;
	Version endVersion;
	int64_t offset;

	LogInfo() : offset(0) {}
};

class FileBackupAgentImpl {
public:
	// This method will return the final status of the backup at tag, and return the URL that was used on the tag
	// when that status value was read.
	static Future<EBackupState> waitBackup(FileBackupAgent* backupAgent,
	                                       Database cx,
	                                       std::string tagName,
	                                       StopWhenDone stopWhenDone,
	                                       Reference<IBackupContainer>* pContainer = nullptr,
	                                       UID* pUID = nullptr) {
		std::string backTrace;
		KeyBackedTag tag = makeBackupTag(tagName);

		while (true) {
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Error err;
			try {
				Optional<UidAndAbortedFlagT> oldUidAndAborted = co_await tag.get(tr);
				if (!oldUidAndAborted.present()) {
					co_return EBackupState::STATE_NEVERRAN;
				}

				BackupConfig config(oldUidAndAborted.get().first);
				EBackupState status =
				    co_await config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);

				// Break, if one of the following is true
				//  - no longer runnable
				//  - in differential mode (restorable) and stopWhenDone is not enabled
				if (!FileBackupAgent::isRunnable(status) ||
				    ((!stopWhenDone) && (EBackupState::STATE_RUNNING_DIFFERENTIAL == status))) {

					if (pContainer != nullptr) {
						Reference<IBackupContainer> c =
						    co_await config.backupContainer().getOrThrow(tr, Snapshot::False, backup_invalid_info());
						*pContainer = fileBackup::getBackupContainerWithProxy(c);
					}

					if (pUID != nullptr) {
						*pUID = oldUidAndAborted.get().first;
					}

					co_return status;
				}

				Future<Void> watchFuture = tr->watch(config.stateEnum().key);
				co_await tr->commit();
				co_await watchFuture;
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	}

	static Future<Void> submitBackup(FileBackupAgent* backupAgent,
	                                 Reference<ReadYourWritesTransaction> tr,
	                                 Key outContainer,
	                                 Optional<std::string> proxy,
	                                 int initialSnapshotIntervalSeconds,
	                                 int snapshotIntervalSeconds,
	                                 std::string tagName,
	                                 Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                 StopWhenDone stopWhenDone,
	                                 UsePartitionedLog partitionedLog,
	                                 IncrementalBackupOnly incrementalBackupOnly,
	                                 Optional<std::string> encryptionKeyFileName,
	                                 int snapshotMode) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

		TraceEvent(SevInfo, "FBA_SubmitBackup")
		    .detail("TagName", tagName.c_str())
		    .detail("StopWhenDone", stopWhenDone)
		    .detail("UsePartitionedLog", partitionedLog)
		    .detail("OutContainer", outContainer.toString());

		KeyBackedTag tag = makeBackupTag(tagName);
		Optional<UidAndAbortedFlagT> uidAndAbortedFlag = co_await tag.get(tr);
		if (uidAndAbortedFlag.present()) {
			BackupConfig prevConfig(uidAndAbortedFlag.get().first);
			EBackupState prevBackupStatus =
			    co_await prevConfig.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);
			if (FileBackupAgent::isRunnable(prevBackupStatus)) {
				throw backup_duplicate();
			}

			// Now is time to clear prev backup config space. We have no more use for it.
			prevConfig.clear(tr);
		}

		BackupConfig config(deterministicRandom()->randomUniqueID());
		UID uid = config.getUid();

		// This check will ensure that current backupUid is later than the last backup Uid
		Standalone<StringRef> nowStr = BackupAgentBase::getCurrentTime();
		std::string backupContainer = outContainer.toString();

		// To be consistent with directory handling behavior since FDB backup was first released, if the container
		// string describes a local directory then "/backup-<timestamp>" will be added to it.
		if (backupContainer.find("file://") == 0) {
			backupContainer = joinPath(backupContainer, std::string("backup-") + nowStr.toString());
		}

		Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupContainer, proxy, encryptionKeyFileName);
		try {
			// Use longer timeout for blobstore:// URLs in simulation to handle slow S3 mock operations
			double createTimeout = (g_network->isSimulated() && isBlobstoreUrl(backupContainer)) ? 300.0 : 30.0;
			co_await timeoutError(bc->create(), createTimeout);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			fprintf(stderr, "ERROR: Could not create backup container: %s\n", e.what());
			throw backup_error();
		}

		Optional<Value> lastBackupTimestamp = co_await backupAgent->lastBackupTimestamp().get(tr);

		if ((lastBackupTimestamp.present()) && (lastBackupTimestamp.get() >= nowStr)) {
			fprintf(stderr,
			        "ERROR: The last backup `%s' happened in the future.\n",
			        printable(lastBackupTimestamp.get()).c_str());
			throw backup_error();
		}

		KeyRangeMap<int> backupRangeSet;
		for (auto& backupRange : backupRanges) {
			backupRangeSet.insert(backupRange, 1);
		}

		backupRangeSet.coalesce(allKeys);
		std::vector<KeyRange> normalizedRanges;

		for (auto& backupRange : backupRangeSet.ranges()) {
			if (backupRange.value()) {
				normalizedRanges.push_back(KeyRange(KeyRangeRef(backupRange.range().begin, backupRange.range().end)));
			}
		}

		config.clear(tr);

		Key destUidValue(BinaryWriter::toValue(uid, Unversioned()));
		if (normalizedRanges.size() == 1 || isDefaultBackup(normalizedRanges)) {
			RangeResult existingDestUidValues = co_await tr->getRange(
			    KeyRangeRef(destUidLookupPrefix, strinc(destUidLookupPrefix)), CLIENT_KNOBS->TOO_MANY);
			bool found = false;
			KeyRangeRef targetRange =
			    normalizedRanges.size() == 1 ? normalizedRanges[0] : getDefaultBackupSharedRange();
			for (auto it : existingDestUidValues) {
				KeyRange uidRange =
				    BinaryReader::fromStringRef<KeyRange>(it.key.removePrefix(destUidLookupPrefix), IncludeVersion());
				if (uidRange == targetRange) {
					destUidValue = it.value;
					found = true;
					CODE_PROBE(targetRange == getDefaultBackupSharedRange(),
					           "Backup mutation sharing with default backup");
					break;
				}
			}
			if (!found) {
				destUidValue = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());
				tr->set(BinaryWriter::toValue(targetRange, IncludeVersion(ProtocolVersion::withSharedMutations()))
				            .withPrefix(destUidLookupPrefix),
				        destUidValue);
			}
		}

		tr->set(config.getUidAsKey().withPrefix(destUidValue).withPrefix(backupLatestVersionsPrefix),
		        BinaryWriter::toValue<Version>(tr->getReadVersion().get(), Unversioned()));
		config.destUidValue().set(tr, destUidValue);

		// Point the tag to this new uid
		tag.set(tr, { uid, false });

		backupAgent->lastBackupTimestamp().set(tr, nowStr);

		// Set the backup keys
		config.tag().set(tr, tagName);
		config.stateEnum().set(tr, EBackupState::STATE_SUBMITTED);
		config.backupContainer().set(tr, bc);
		config.stopWhenDone().set(tr, stopWhenDone);
		config.backupRanges().set(tr, normalizedRanges);
		config.initialSnapshotIntervalSeconds().set(tr, initialSnapshotIntervalSeconds);
		config.snapshotIntervalSeconds().set(tr, snapshotIntervalSeconds);
		config.partitionedLogEnabled().set(tr, partitionedLog);
		config.incrementalBackupOnly().set(tr, incrementalBackupOnly);
		config.snapshotMode().set(tr, snapshotMode);

		Key taskKey = co_await fileBackup::StartFullBackupTaskFunc::addTask(
		    tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal());

		co_return;
	}

	static Future<Void> submitRestore(FileBackupAgent* backupAgent,
	                                  Reference<ReadYourWritesTransaction> tr,
	                                  Key tagName,
	                                  Key backupURL,
	                                  Optional<std::string> proxy,
	                                  Standalone<VectorRef<KeyRangeRef>> ranges,
	                                  Version restoreVersion,
	                                  Key addPrefix,
	                                  Key removePrefix,
	                                  LockDB lockDB,
	                                  UnlockDB unlockDB,
	                                  OnlyApplyMutationLogs onlyApplyMutationLogs,
	                                  InconsistentSnapshotOnly inconsistentSnapshotOnly,
	                                  Version beginVersion,
	                                  UID uid,
	                                  TransformPartitionedLog transformPartitionedLog,
	                                  bool useRangeFileRestore = true) {
		KeyRangeMap<int> restoreRangeSet;
		for (auto& range : ranges) {
			restoreRangeSet.insert(range, 1);
		}
		restoreRangeSet.coalesce(allKeys);
		std::vector<KeyRange> restoreRanges;
		for (auto& restoreRange : restoreRangeSet.ranges()) {
			if (restoreRange.value()) {
				restoreRanges.push_back(KeyRange(KeyRangeRef(restoreRange.range().begin, restoreRange.range().end)));
			}
		}
		for (auto& restoreRange : restoreRanges) {
			ASSERT(restoreRange.begin.startsWith(removePrefix) && restoreRange.end.startsWith(removePrefix));
		}

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		// Get old restore config for this tag
		KeyBackedTag tag = makeRestoreTag(tagName.toString());
		Optional<UidAndAbortedFlagT> oldUidAndAborted = co_await tag.get(tr);
		if (oldUidAndAborted.present()) {
			if (oldUidAndAborted.get().first == uid) {
				if (oldUidAndAborted.get().second) {
					throw restore_duplicate_uid();
				} else {
					co_return;
				}
			}

			RestoreConfig oldRestore(oldUidAndAborted.get().first);

			// Make sure old restore for this tag is not runnable
			bool runnable = co_await oldRestore.isRunnable(tr);

			if (runnable) {
				throw restore_duplicate_tag();
			}

			// Clear the old restore config
			oldRestore.clear(tr);
		}

		if (!onlyApplyMutationLogs) {
			int index{ 0 };
			for (index = 0; index < restoreRanges.size(); index++) {
				KeyRange restoreIntoRange = KeyRangeRef(restoreRanges[index].begin, restoreRanges[index].end)
				                                .removePrefix(removePrefix)
				                                .withPrefix(addPrefix);
				RangeResult existingRows = co_await tr->getRange(restoreIntoRange, 1);
				// Allow restoring over existing data only when using the validation restore prefix.
				// validateRestoreLogKeys.begin (\xff\x02/rlog/) is the designated prefix for validation restores.
				// Using any other prefix with existing data could corrupt user data.
				if (!existingRows.empty() && addPrefix != validateRestoreLogKeys.begin) {
					throw restore_destination_not_empty();
				}
			}
		}
		// Make new restore config
		RestoreConfig restore(uid);

		// Point the tag to the new uid
		tag.set(tr, { uid, false });

		Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupURL.toString(), proxy, {});

		// Configure the new restore
		restore.tag().set(tr, tagName.toString());
		restore.sourceContainer().set(tr, bc);
		restore.stateEnum().set(tr, ERestoreState::QUEUED);
		restore.restoreVersion().set(tr, restoreVersion);
		restore.onlyApplyMutationLogs().set(tr, onlyApplyMutationLogs);
		restore.inconsistentSnapshotOnly().set(tr, inconsistentSnapshotOnly);
		restore.beginVersion().set(tr, beginVersion);
		restore.unlockDBAfterRestore().set(tr, unlockDB);
		restore.transformPartitionedLog().set(tr, transformPartitionedLog);
		restore.useRangeFileRestore().set(tr, useRangeFileRestore);
		if (BUGGIFY && restoreRanges.size() == 1) {
			restore.restoreRange().set(tr, restoreRanges[0]);
		} else {
			for (auto& range : restoreRanges) {
				restore.restoreRangeSet().insert(tr, range);
			}
		}
		// this also sets restore.add/removePrefix.
		restore.initApplyMutations(tr, addPrefix, removePrefix, onlyApplyMutationLogs);

		Key taskKey = co_await fileBackup::StartFullRestoreTaskFunc::addTask(
		    tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal());

		if (lockDB)
			co_await lockDatabase(tr, uid);
		else
			co_await checkDatabaseLock(tr, uid);

		co_return;
	}

	// This method will return the final status of the backup
	static Future<ERestoreState> waitRestore(Database cx, Key tagName, Verbose verbose) {
		ERestoreState status;
		while (true) {
			Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				KeyBackedTag tag = makeRestoreTag(tagName.toString());
				Optional<UidAndAbortedFlagT> current = co_await tag.get(tr);
				if (!current.present()) {
					if (verbose)
						printf("waitRestore: Tag: %s  State: %s\n",
						       tagName.toString().c_str(),
						       FileBackupAgent::restoreStateText(ERestoreState::UNINITIALIZED).toString().c_str());
					co_return ERestoreState::UNINITIALIZED;
				}

				RestoreConfig restore(current.get().first);

				if (verbose) {
					std::string details = co_await restore.getProgress(tr);
					printf("%s\n", details.c_str());
				}

				ERestoreState status_ = co_await restore.stateEnum().getD(tr);
				status = status_;
				bool runnable = co_await restore.isRunnable(tr);

				// State won't change from here
				if (!runnable)
					break;

				// Wait for a change
				Future<Void> watchFuture = tr->watch(restore.stateEnum().key);
				co_await tr->commit();
				if (verbose)
					co_await (watchFuture || delay(1));
				else
					co_await watchFuture;
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		co_return status;
	}

	static Future<Void> discontinueBackup(FileBackupAgent* backupAgent,
	                                      Reference<ReadYourWritesTransaction> tr,
	                                      Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		KeyBackedTag tag = makeBackupTag(tagName.toString());
		UidAndAbortedFlagT current = co_await tag.getOrThrow(tr, Snapshot::False, backup_unneeded());
		BackupConfig config(current.first);
		EBackupState status = co_await config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);

		if (!FileBackupAgent::isRunnable(status)) {
			throw backup_unneeded();
		}

		// If the backup is already restorable then 'mostly' abort it - cancel all tasks via the tag
		// and clear the mutation logging config and data - but set its state as COMPLETED instead of ABORTED.
		Optional<Version> latestRestorableVersion = co_await config.getLatestRestorableVersion(tr);

		TraceEvent(SevInfo, "FBA_DiscontinueBackup")
		    .detail("AlreadyRestorable", latestRestorableVersion.present() ? "Yes" : "No")
		    .detail("TagName", tag.tagName.c_str())
		    .detail("Status", BackupAgentBase::getStateText(status));

		if (latestRestorableVersion.present()) {
			// Cancel all backup tasks through tag
			co_await tag.cancel(tr);

			tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

			Key destUidValue = co_await config.destUidValue().getOrThrow(tr);
			co_await tr->getReadVersion();
			co_await (eraseLogData(tr, config.getUidAsKey(), destUidValue) &&
			          fileBackup::clearBackupStartID(tr, config.getUid()));

			config.stateEnum().set(tr, EBackupState::STATE_COMPLETED);

			co_return;
		}

		bool stopWhenDone = co_await config.stopWhenDone().getOrThrow(tr);

		if (stopWhenDone) {
			throw backup_duplicate();
		}

		config.stopWhenDone().set(tr, true);

		co_return;
	}

	static Future<Void> abortBackup(FileBackupAgent* backupAgent,
	                                Reference<ReadYourWritesTransaction> tr,
	                                std::string tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		KeyBackedTag tag = makeBackupTag(tagName);
		UidAndAbortedFlagT current = co_await tag.getOrThrow(tr, Snapshot::False, backup_unneeded());

		BackupConfig config(current.first);
		Key destUidValue = co_await config.destUidValue().getOrThrow(tr);
		EBackupState status = co_await config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);

		if (!backupAgent->isRunnable(status)) {
			throw backup_unneeded();
		}

		TraceEvent(SevInfo, "FBA_AbortBackup")
		    .detail("TagName", tagName.c_str())
		    .detail("Status", BackupAgentBase::getStateText(status));

		co_await tag.cancel(tr);

		co_await (eraseLogData(tr, config.getUidAsKey(), destUidValue) &&
		          fileBackup::clearBackupStartID(tr, config.getUid()));

		config.stateEnum().set(tr, EBackupState::STATE_ABORTED);

		co_return;
	}

	static Future<Void> checkAndDisableBackupWorkers(Database cx) {
		bool running = co_await runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return anyPartitionedBackupRunning(tr); });
		if (!running) {
			co_await disableBackupWorker(cx);
		}
		co_return;
	}

	static Future<Void> changePause(FileBackupAgent* backupAgent, Database db, bool pause) {
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(db));

		while (true) {
			Error err;
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			try {
				// This is the pause key is in the task bucket, for backup agents
				backupAgent->taskBucket->changePause(tr, pause);
				// This is backup workers' pause key.
				tr->set(backupPausedKey, pause ? "1"_sr : "0"_sr);
				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
		TraceEvent("FileBackupAgentChangePaused").detail("Action", pause ? "Paused" : "Resumed");
		co_return;
	}

	struct TimestampedVersion {
		Optional<Version> version;
		Optional<int64_t> epochs;

		bool present() const { return version.present(); }

		JsonBuilderObject toJSON() const {
			JsonBuilderObject doc;
			if (version.present()) {
				doc.setKey("Version", version.get());
				if (epochs.present()) {
					doc.setKey("EpochSeconds", epochs.get());
					doc.setKey("Timestamp", timeStampToString(epochs));
				}
			}
			return doc;
		}
	};

	// Helper actor for generating status
	// If f is present, lookup epochs using timekeeper and tr, return TimestampedVersion
	static Future<TimestampedVersion> getTimestampedVersion(Reference<ReadYourWritesTransaction> tr,
	                                                        Future<Optional<Version>> f) {
		TimestampedVersion tv;
		tv.version = co_await f;
		if (tv.version.present()) {
			tv.epochs = co_await timeKeeperEpochsFromVersion(tv.version.get(), tr);
		}
		co_return tv;
	}

	static Future<std::string> getStatusJSON(FileBackupAgent* backupAgent, Database cx, std::string tagName) {
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

		while (true) {
			Error err;
			try {
				JsonBuilderObject doc;
				doc.setKey("SchemaVersion", "1.0.0");

				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				KeyBackedTag tag = makeBackupTag(tagName);
				Optional<UidAndAbortedFlagT> uidAndAbortedFlag;
				Optional<Value> paused;
				Version recentReadVersion{ 0 };

				co_await (store(paused, tr->get(backupAgent->taskBucket->getPauseKey())) &&
				          store(uidAndAbortedFlag, tag.get(tr)) && store(recentReadVersion, tr->getReadVersion()));

				doc.setKey("BackupAgentsPaused", paused.present());
				doc.setKey("Tag", tag.tagName);

				if (uidAndAbortedFlag.present()) {
					doc.setKey("UID", uidAndAbortedFlag.get().first.toString());

					BackupConfig config(uidAndAbortedFlag.get().first);

					EBackupState backupState =
					    co_await config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);
					JsonBuilderObject statusDoc;
					statusDoc.setKey("Name", BackupAgentBase::getStateName(backupState));
					statusDoc.setKey("Description", BackupAgentBase::getStateText(backupState));
					statusDoc.setKey("Completed", backupState == EBackupState::STATE_COMPLETED);
					statusDoc.setKey("Running", BackupAgentBase::isRunnable(backupState));
					doc.setKey("Status", statusDoc);

					Future<Void> done = Void();

					if (backupState != EBackupState::STATE_NEVERRAN) {
						Reference<IBackupContainer> bc;
						TimestampedVersion latestRestorable;

						co_await (
						    store(latestRestorable, getTimestampedVersion(tr, config.getLatestRestorableVersion(tr))) &&
						    store(bc, config.backupContainer().getOrThrow(tr)));
						bc = fileBackup::getBackupContainerWithProxy(bc);

						doc.setKey("Restorable", latestRestorable.present());

						if (latestRestorable.present()) {
							JsonBuilderObject o = latestRestorable.toJSON();
							if (backupState != EBackupState::STATE_COMPLETED) {
								o.setKey("LagSeconds",
								         (recentReadVersion - latestRestorable.version.get()) /
								             CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
							}
							doc.setKey("LatestRestorablePoint", o);
						}
						doc.setKey("DestinationURL", bc->getURL());
					}

					if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL ||
					    backupState == EBackupState::STATE_RUNNING) {
						int64_t snapshotInterval{ 0 };
						int64_t logBytesWritten{ 0 };
						int64_t rangeBytesWritten{ 0 };
						bool stopWhenDone{ false };
						TimestampedVersion snapshotBegin;
						TimestampedVersion snapshotTargetEnd;
						TimestampedVersion latestLogEnd;
						TimestampedVersion latestSnapshotEnd;
						TimestampedVersion snapshotLastDispatch;
						Optional<int64_t> snapshotLastDispatchShardsBehind;

						co_await (
						    store(snapshotInterval, config.snapshotIntervalSeconds().getOrThrow(tr)) &&
						    store(logBytesWritten, config.logBytesWritten().getD(tr)) &&
						    store(rangeBytesWritten, config.rangeBytesWritten().getD(tr)) &&
						    store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)) &&
						    store(snapshotBegin, getTimestampedVersion(tr, config.snapshotBeginVersion().get(tr))) &&
						    store(snapshotTargetEnd,
						          getTimestampedVersion(tr, config.snapshotTargetEndVersion().get(tr))) &&
						    store(latestLogEnd, getTimestampedVersion(tr, config.latestLogEndVersion().get(tr))) &&
						    store(latestSnapshotEnd,
						          getTimestampedVersion(tr, config.latestSnapshotEndVersion().get(tr))) &&
						    store(snapshotLastDispatch,
						          getTimestampedVersion(tr, config.snapshotDispatchLastVersion().get(tr))) &&
						    store(snapshotLastDispatchShardsBehind, config.snapshotDispatchLastShardsBehind().get(tr)));

						doc.setKey("StopAfterSnapshot", stopWhenDone);
						doc.setKey("SnapshotIntervalSeconds", snapshotInterval);
						doc.setKey("LogBytesWritten", logBytesWritten);
						doc.setKey("RangeBytesWritten", rangeBytesWritten);

						if (latestLogEnd.present()) {
							doc.setKey("LatestLogEnd", latestLogEnd.toJSON());
						}

						if (latestSnapshotEnd.present()) {
							doc.setKey("LatestSnapshotEnd", latestSnapshotEnd.toJSON());
						}

						JsonBuilderObject snapshot;

						if (snapshotBegin.present()) {
							snapshot.setKey("Begin", snapshotBegin.toJSON());

							if (snapshotTargetEnd.present()) {
								snapshot.setKey("EndTarget", snapshotTargetEnd.toJSON());

								Version interval = snapshotTargetEnd.version.get() - snapshotBegin.version.get();
								snapshot.setKey("IntervalSeconds", interval / CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

								Version elapsed = recentReadVersion - snapshotBegin.version.get();
								double progress = (interval > 0) ? (100.0 * elapsed / interval) : 100;
								snapshot.setKey("ExpectedProgress", progress);
							}

							JsonBuilderObject dispatchDoc = snapshotLastDispatch.toJSON();
							if (snapshotLastDispatchShardsBehind.present()) {
								dispatchDoc.setKey("ShardsBehind", snapshotLastDispatchShardsBehind.get());
							}
							snapshot.setKey("LastDispatch", dispatchDoc);
						}

						doc.setKey("CurrentSnapshot", snapshot);
					}

					// Add snapshot mode information
					Optional<int> snapshotModeOpt = co_await config.snapshotMode().get(tr);
					int snapshotModeValue = snapshotModeOpt.present() ? snapshotModeOpt.get() : 0;
					std::string snapshotModeText;
					switch (snapshotModeValue) {
					case 0:
						snapshotModeText = "rangefile";
						break;
					case 1:
						snapshotModeText = "bulkdump";
						break;
					case 2:
						snapshotModeText = "both";
						break;
					default:
						TraceEvent(SevError, "BackupInvalidSnapshotMode")
						    .detail("BackupUID", config.getUid())
						    .detail("SnapshotModeValue", snapshotModeValue)
						    .detail("ValidValues", "0=rangefile, 1=bulkdump, 2=both");
						throw backup_error();
					}
					doc.setKey("SnapshotMode", snapshotModeText);

					// Check if backup is BulkLoad compatible
					Optional<std::string> bulkDumpJobIdOpt = co_await config.bulkDumpJobId().get(tr);
					doc.setKey("BulkLoadCompatible", bulkDumpJobIdOpt.present() && !bulkDumpJobIdOpt.get().empty());

					// If using bulkdump mode, get bulkdump progress
					if (snapshotModeValue == 1 || snapshotModeValue == 2) {
						Optional<BulkDumpProgress> bulkDumpProgressOpt = co_await getBulkDumpProgress(cx);
						if (bulkDumpProgressOpt.present()) {
							BulkDumpProgress bdProgress = bulkDumpProgressOpt.get();
							JsonBuilderObject bdDoc;
							bdDoc.setKey("JobId", bdProgress.jobId.toString());
							bdDoc.setKey("TotalTasks", bdProgress.totalTasks);
							bdDoc.setKey("CompleteTasks", bdProgress.completeTasks);
							bdDoc.setKey("RunningTasks", bdProgress.runningTasks);
							bdDoc.setKey("ErrorTasks", bdProgress.errorTasks);
							bdDoc.setKey("ProgressPercent", bdProgress.progressPercent());
							bdDoc.setKey("TotalBytes", bdProgress.totalBytes);
							bdDoc.setKey("CompletedBytes", bdProgress.completedBytes);
							bdDoc.setKey("AvgBytesPerSecond", bdProgress.avgBytesPerSecond());
							if (bdProgress.etaSeconds().present()) {
								bdDoc.setKey("EstimatedSecondsRemaining", bdProgress.etaSeconds().get());
							}
							bdDoc.setKey("ElapsedSeconds", bdProgress.elapsedSeconds);

							// Add stalled tasks if any
							if (!bdProgress.stalledTasks.empty()) {
								JsonBuilderArray stalledArray;
								for (const auto& stalled : bdProgress.stalledTasks) {
									JsonBuilderObject stalledDoc;
									stalledDoc.setKey("TaskId", stalled.taskId.toString());
									stalledDoc.setKey("Range", stalled.range.toString());
									stalledDoc.setKey("StalledSeconds", stalled.stalledSeconds);
									stalledDoc.setKey("RestartCount", stalled.restartCount);
									if (!stalled.lastError.empty()) {
										stalledDoc.setKey("LastError", stalled.lastError);
									}
									stalledArray.push_back(stalledDoc);
								}
								bdDoc.setKey("StalledTasks", stalledArray);
							}

							doc.setKey("BulkDumpProgress", bdDoc);
						}
					}

					KeyBackedMap<int64_t, std::pair<std::string, Version>>::RangeResultType errors =
					    co_await config.lastErrorPerType().getRange(
					        tr, 0, std::numeric_limits<int>::max(), CLIENT_KNOBS->TOO_MANY);
					JsonBuilderArray errorList;
					for (auto& e : errors.results) {
						std::string msg = e.second.first;
						Version ver = e.second.second;

						JsonBuilderObject errDoc;
						errDoc.setKey("Message", msg.c_str());
						errDoc.setKey("RelativeSeconds",
						              (ver - recentReadVersion) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
					}
					doc.setKey("Errors", errorList);
				}

				co_return doc.getJson();
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	}

	static Future<std::string> getStatus(FileBackupAgent* backupAgent,
	                                     Database cx,
	                                     ShowErrors showErrors,
	                                     std::string tagName) {
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		std::string statusText;

		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				KeyBackedTag tag;
				BackupConfig config;
				EBackupState backupState;

				statusText = "";
				tag = makeBackupTag(tagName);
				Optional<UidAndAbortedFlagT> uidAndAbortedFlag = co_await tag.get(tr);
				Future<Optional<Value>> fPaused = tr->get(backupAgent->taskBucket->getPauseKey());
				if (uidAndAbortedFlag.present()) {
					config = BackupConfig(uidAndAbortedFlag.get().first);
					EBackupState status =
					    co_await config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);
					backupState = status;
				}

				if (!uidAndAbortedFlag.present() || backupState == EBackupState::STATE_NEVERRAN) {
					statusText += "No previous backups found on tag '" + tagName + "'.\n";
					statusText += "Use 'fdbbackup tags' to list all backup tags.\n";
				} else {
					std::string backupStatus(BackupAgentBase::getStateText(backupState));
					Reference<IBackupContainer> bc;
					Optional<Version> latestRestorableVersion;
					Version recentReadVersion{ 0 };

					co_await (store(latestRestorableVersion, config.getLatestRestorableVersion(tr)) &&
					          store(bc, config.backupContainer().getOrThrow(tr)) &&
					          store(recentReadVersion, tr->getReadVersion()));
					bc = fileBackup::getBackupContainerWithProxy(bc);

					bool snapshotProgress = false;

					switch (backupState) {
					case EBackupState::STATE_SUBMITTED:
						statusText += "The backup on tag `" + tagName + "' is in progress (just started) to " +
						              bc->getURL() + ".\n";
						break;
					case EBackupState::STATE_RUNNING:
						statusText += "The backup on tag `" + tagName + "' is in progress to " + bc->getURL() + ".\n";
						snapshotProgress = true;
						break;
					case EBackupState::STATE_RUNNING_DIFFERENTIAL:
						statusText += "The backup on tag `" + tagName + "' is restorable but continuing to " +
						              bc->getURL() + ".\n";
						snapshotProgress = true;
						break;
					case EBackupState::STATE_COMPLETED:
						statusText += "The previous backup on tag `" + tagName + "' at " + bc->getURL() +
						              " completed at version " + format("%lld", latestRestorableVersion.orDefault(-1)) +
						              ".\n";
						break;
					default:
						statusText += "The previous backup on tag `" + tagName + "' at " + bc->getURL() + " " +
						              backupStatus + ".\n";
						break;
					}
					statusText += format("BackupUID: %s\n", uidAndAbortedFlag.get().first.toString().c_str());
					statusText += format("BackupURL: %s\n", bc->getURL().c_str());

					// Add enhanced status fields for BulkLoad integration
					Optional<int> snapshotModeOpt = co_await config.snapshotMode().get(tr);
					int snapshotModeValue = snapshotModeOpt.present() ? snapshotModeOpt.get() : 0;
					std::string snapshotModeText;
					switch (snapshotModeValue) {
					case 0:
						snapshotModeText = "rangefile";
						break;
					case 1:
						snapshotModeText = "bulkdump";
						break;
					case 2:
						snapshotModeText = "both";
						break;
					default:
						TraceEvent(SevError, "BackupInvalidSnapshotMode")
						    .detail("BackupUID", config.getUid())
						    .detail("SnapshotModeValue", snapshotModeValue)
						    .detail("ValidValues", "0=rangefile, 1=bulkdump, 2=both");
						throw backup_error();
					}
					statusText += format("Snapshot Mode: %s\n", snapshotModeText.c_str());

					// Check if backup is BulkLoad compatible (has bulkdump_data/)
					Optional<std::string> bulkDumpJobIdOpt = co_await config.bulkDumpJobId().get(tr);
					bool bulkLoadCompatible = bulkDumpJobIdOpt.present() && !bulkDumpJobIdOpt.get().empty();
					statusText += format("BulkLoad Compatible: %s\n", bulkLoadCompatible ? "yes" : "no");

					bool showBulkDump = (snapshotModeValue == 1 || snapshotModeValue == 2);
					bool showRangeFile = (snapshotModeValue == 0 || snapshotModeValue == 2);

					if (snapshotProgress) {
						int64_t snapshotInterval{ 0 };
						Version snapshotBeginVersion{ 0 };
						Version snapshotTargetEndVersion{ 0 };
						Optional<Version> latestSnapshotEndVersion;
						Optional<Version> latestLogEndVersion;
						Optional<int64_t> logBytesWritten;
						Optional<int64_t> rangeBytesWritten;
						Optional<int64_t> latestSnapshotEndVersionTimestamp;
						Optional<int64_t> latestLogEndVersionTimestamp;
						Optional<int64_t> snapshotBeginVersionTimestamp;
						Optional<int64_t> snapshotTargetEndVersionTimestamp;
						bool stopWhenDone{ false };

						co_await (store(snapshotBeginVersion, config.snapshotBeginVersion().getOrThrow(tr)) &&
						          store(snapshotTargetEndVersion, config.snapshotTargetEndVersion().getOrThrow(tr)) &&
						          store(snapshotInterval, config.snapshotIntervalSeconds().getOrThrow(tr)) &&
						          store(logBytesWritten, config.logBytesWritten().get(tr)) &&
						          store(rangeBytesWritten, config.rangeBytesWritten().get(tr)) &&
						          store(latestLogEndVersion, config.latestLogEndVersion().get(tr)) &&
						          store(latestSnapshotEndVersion, config.latestSnapshotEndVersion().get(tr)) &&
						          store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)));

						co_await (
						    store(latestSnapshotEndVersionTimestamp,
						          getTimestampFromVersion(latestSnapshotEndVersion, tr)) &&
						    store(latestLogEndVersionTimestamp, getTimestampFromVersion(latestLogEndVersion, tr)) &&
						    store(snapshotBeginVersionTimestamp,
						          timeKeeperEpochsFromVersion(snapshotBeginVersion, tr)) &&
						    store(snapshotTargetEndVersionTimestamp,
						          timeKeeperEpochsFromVersion(snapshotTargetEndVersion, tr)));

						if (showBulkDump) {
							Optional<BulkDumpProgress> bulkDumpProgressOpt = co_await getBulkDumpProgress(cx);
							statusText += "\nBulkDump Snapshot:\n";
							if (bulkDumpProgressOpt.present()) {
								BulkDumpProgress bdProgress = bulkDumpProgressOpt.get();
								statusText += format(" Tasks: %d/%d complete (%.1f%%)\n",
								                     bdProgress.completeTasks,
								                     bdProgress.totalTasks,
								                     bdProgress.progressPercent());
								statusText += format(
								    " Bytes: %s\n",
								    formatBytesProgress(bdProgress.completedBytes, bdProgress.totalBytes).c_str());

								double throughput = bdProgress.avgBytesPerSecond();
								if (throughput > 0) {
									statusText += format(" Throughput: %.1f MB/s\n", throughput / 1048576.0);
								}

								if (bdProgress.elapsedSeconds > 0) {
									statusText +=
									    format(" Elapsed: %s\n",
									           formatDurationHumanReadable((int)bdProgress.elapsedSeconds).c_str());
								}

								if (bdProgress.etaSeconds().present()) {
									statusText +=
									    format(" ETA: %s\n",
									           formatDurationHumanReadable((int)bdProgress.etaSeconds().get()).c_str());
								}

								if (!bdProgress.stalledTasks.empty()) {
									statusText += format("\nWARNING: %zu stalled tasks (no progress > 60s):\n",
									                     bdProgress.stalledTasks.size());
									for (const auto& stalled : bdProgress.stalledTasks) {
										statusText += format(" Task %s: %s, stalled %.0fs, %d restarts\n",
										                     stalled.taskId.shortString().c_str(),
										                     stalled.range.toString().c_str(),
										                     stalled.stalledSeconds,
										                     stalled.restartCount);
										if (!stalled.lastError.empty()) {
											statusText +=
											    format("           Last error: %s\n", stalled.lastError.c_str());
										}
									}
								}
							} else {
								statusText += " Status: pending\n";
							}
						}

						if (showRangeFile) {
							statusText += "\nRangefile Snapshot:\n";
							statusText +=
							    format(" Bytes written: %s\n", formatBytesHumanReadable(rangeBytesWritten.orDefault(0)).c_str());

							std::string rangefileStatus;
							if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL) {
								double pct = 100.0 * (recentReadVersion - snapshotBeginVersion) /
								             (snapshotTargetEndVersion - snapshotBeginVersion);
								statusText += format(" Progress: %.2f%%\n", pct);
							} else {
								statusText += " Status: Initial snapshot still running\n";
							}
							statusText +=
							    format(" Started: %s\n", timeStampToString(snapshotBeginVersionTimestamp).c_str());
						}

						statusText += format("\nMutation Logs:\n");
						statusText += format(" Bytes written: %s\n", formatBytesHumanReadable(logBytesWritten.orDefault(0)).c_str());
						statusText += format(" Last complete version: %s (%s)\n",
						                     versionToString(latestLogEndVersion).c_str(),
						                     timeStampToString(latestLogEndVersionTimestamp).c_str());

						statusText += format("\nSnapshot interval is %lld seconds.\n", snapshotInterval);
					}

					// Append the errors, if requested
					if (showErrors) {
						KeyBackedMap<int64_t, std::pair<std::string, Version>>::RangeResultType errors =
						    co_await config.lastErrorPerType().getRange(
						        tr, 0, std::numeric_limits<int>::max(), CLIENT_KNOBS->TOO_MANY);
						std::string recentErrors;
						std::string pastErrors;

						for (auto& e : errors.results) {
							Version v = e.second.second;
							std::string msg = format(
							    "%s ago : %s\n",
							    secondsToTimeFormat((recentReadVersion - v) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND)
							        .c_str(),
							    e.second.first.c_str());

							// If error version is at or more recent than the latest restorable version then it
							// could be inhibiting progress
							if (v >= latestRestorableVersion.orDefault(0)) {
								recentErrors += msg;
							} else {
								pastErrors += msg;
							}
						}

						if (!recentErrors.empty()) {
							if (latestRestorableVersion.present())
								statusText +=
								    format("Recent Errors (since latest restorable point %s ago)\n",
								           secondsToTimeFormat((recentReadVersion - latestRestorableVersion.get()) /
								                               CLIENT_KNOBS->CORE_VERSIONSPERSECOND)
								               .c_str()) +
								    recentErrors;
							else
								statusText += "Recent Errors (since initialization)\n" + recentErrors;
						}
						if (!pastErrors.empty())
							statusText += "Older Errors\n" + pastErrors;
					}
				}

				Optional<Value> paused = co_await fPaused;
				if (paused.present()) {
					statusText += format("\nAll backup agents have been paused.\n");
				}

				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		co_return statusText;
	}

	static Future<Optional<Version>> getLastRestorable(FileBackupAgent* backupAgent,
	                                                   Reference<ReadYourWritesTransaction> tr,
	                                                   Key tagName,
	                                                   Snapshot snapshot) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		Optional<Value> version = co_await tr->get(backupAgent->lastRestorable.pack(tagName), snapshot);

		co_return (version.present())
		    ? Optional<Version>(BinaryReader::fromStringRef<Version>(version.get(), Unversioned()))
		    : Optional<Version>();
	}

	static StringRef read(StringRef& data, int bytes) {
		if (bytes > data.size())
			throw restore_error();
		StringRef r = data.substr(0, bytes);
		data = data.substr(bytes);
		return r;
	}

	// Submits the restore request to the database and throws "restore_invalid_version" error if
	// restore is not possible. Parameters:
	//   cx: the database to be restored to
	//   cxOrig: if present, is used to resolve the restore timestamp into a version.
	//   tagName: restore tag
	//   url: the backup container's URL that contains all backup files
	//   ranges: the restored key ranges; if empty, restore all key ranges in the backup
	//   waitForComplete: if set, wait until the restore is completed before returning; otherwise,
	//                    return when the request is submitted to the database.
	//   targetVersion: the version to be restored.
	//   verbose: print verbose information.
	//   addPrefix: each key is added this prefix during restore.
	//   removePrefix: for each key to be restored, remove this prefix first.
	//   lockDB: if set lock the database with randomUid before performing restore;
	//           otherwise, check database is locked with the randomUid
	//   onlyApplyMutationLogs: only perform incremental restore, by only applying mutation logs
	//   inconsistentSnapshotOnly: Ignore mutation log files during the restore to speedup the process.
	//                             When set to true, gives an inconsistent snapshot, thus not recommended
	//   beginVersions: restore's begin version for each range
	//   randomUid: the UID for lock the database
	static Future<Version> restore(FileBackupAgent* backupAgent,
	                               Database cx,
	                               Optional<Database> cxOrig,
	                               Key tagName,
	                               Key url,
	                               Optional<std::string> proxy,
	                               Standalone<VectorRef<KeyRangeRef>> ranges,
	                               Standalone<VectorRef<Version>> beginVersions,
	                               WaitForComplete waitForComplete,
	                               Version targetVersion,
	                               Verbose verbose,
	                               Key addPrefix,
	                               Key removePrefix,
	                               LockDB lockDB,
	                               UnlockDB unlockDB,
	                               OnlyApplyMutationLogs onlyApplyMutationLogs,
	                               InconsistentSnapshotOnly inconsistentSnapshotOnly,
	                               Optional<std::string> encryptionKeyFileName,
	                               UID randomUid,
	                               bool useRangeFileRestore = true) {
		// The restore command line tool won't allow ranges to be empty, but correctness workloads somehow might.
		if (ranges.empty()) {
			throw restore_error();
		}

		std::string urlStr = url.toString();
		Reference<IBackupContainer> bc = IBackupContainer::openContainer(urlStr, proxy, encryptionKeyFileName);

		// For blobstore:// URLs, use invalidVersion to allow describeBackup to write missing version properties
		// This is needed for S3 where metadata may not be immediately consistent
		BackupDescription desc = co_await bc->describeBackup(true, isBlobstoreUrl(urlStr) ? invalidVersion : 0);

		if (desc.fileLevelEncryption && !encryptionKeyFileName.present()) {
			fprintf(stderr, "ERROR: Backup is encrypted, please provide the encryption key file path.\n");
			throw restore_error();
		} else if (!desc.fileLevelEncryption && encryptionKeyFileName.present()) {
			fprintf(stderr, "ERROR: Backup is not encrypted, please remove the encryption key file path.\n");
			throw restore_error();
		}

		if (cxOrig.present()) {
			co_await desc.resolveVersionTimes(cxOrig.get());
		}

		printf("Backup Description\n%s", desc.toString().c_str());
		if (targetVersion == invalidVersion && desc.maxRestorableVersion.present())
			targetVersion = desc.maxRestorableVersion.get();

		if (targetVersion == invalidVersion && onlyApplyMutationLogs && desc.contiguousLogEnd.present()) {
			targetVersion = desc.contiguousLogEnd.get() - 1;
		}

		Version beginVersion = invalidVersion; // min begin version for all ranges
		if (!beginVersions.empty()) {
			beginVersion = *std::min_element(beginVersions.begin(), beginVersions.end());
		}
		Optional<RestorableFileSet> restoreSet =
		    co_await bc->getRestoreSet(targetVersion, ranges, onlyApplyMutationLogs, beginVersion);

		if (!restoreSet.present()) {
			TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
			    .detail("BackupContainer", bc->getURL())
			    .detail("BeginVersion", beginVersion)
			    .detail("TargetVersion", targetVersion);
			throw restore_invalid_version();
		}

		if (verbose) {
			printf("Restoring backup to version: %lld\n", (long long)targetVersion);
		}

		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				co_await submitRestore(backupAgent,
				                       tr,
				                       tagName,
				                       url,
				                       proxy,
				                       ranges,
				                       targetVersion,
				                       addPrefix,
				                       removePrefix,
				                       lockDB,
				                       unlockDB,
				                       onlyApplyMutationLogs,
				                       inconsistentSnapshotOnly,
				                       beginVersion,
				                       randomUid,
				                       TransformPartitionedLog(desc.partitioned),
				                       useRangeFileRestore);
				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_restore_duplicate_tag) {
				throw err;
			}
			co_await tr->onError(err);
		}

		if (waitForComplete) {
			ERestoreState finalState = co_await waitRestore(cx, tagName, verbose);
			if (finalState != ERestoreState::COMPLETED)
				throw restore_error();
		}

		co_return targetVersion;
	}

	// used for correctness only, locks the database before discontinuing the backup and that same lock is then used
	// while doing the restore. the tagname of the backup must be the same as the restore.
	static Future<Version> atomicRestore(FileBackupAgent* backupAgent,
	                                     Database cx,
	                                     Key tagName,
	                                     Standalone<VectorRef<KeyRangeRef>> ranges,
	                                     Key addPrefix,
	                                     Key removePrefix) {
		auto ryw_tr = makeReference<ReadYourWritesTransaction>(cx);
		BackupConfig backupConfig;
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);
		while (true) {
			Error err;
			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				KeyBackedTag tag = makeBackupTag(tagName.toString());
				UidAndAbortedFlagT uidFlag = co_await tag.getOrThrow(ryw_tr);
				backupConfig = BackupConfig(uidFlag.first);
				EBackupState status = co_await backupConfig.stateEnum().getOrThrow(ryw_tr);

				if (status != EBackupState::STATE_RUNNING_DIFFERENTIAL) {
					throw backup_duplicate();
				}

				break;
			} catch (Error& e) {
				err = e;
			}
			co_await ryw_tr->onError(err);
		}

		// Lock src, record commit version
		Transaction tr(cx);
		Version commitVersion{ 0 };
		UID randomUid = deterministicRandom()->randomUniqueID();
		while (true) {
			Error err;
			try {
				// We must get a commit version so add a conflict range that won't likely cause conflicts
				// but will ensure that the transaction is actually submitted.
				tr.addWriteConflictRange(backupConfig.snapshotRangeDispatchMap().subspace);
				co_await lockDatabase(&tr, randomUid);
				co_await tr.commit();
				commitVersion = tr.getCommittedVersion();
				TraceEvent("AS_Locked").detail("CommitVer", commitVersion);
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		ryw_tr->reset();
		while (true) {
			Error err;
			try {
				Optional<Version> restoreVersion = co_await backupConfig.getLatestRestorableVersion(ryw_tr);
				if (restoreVersion.present() && restoreVersion.get() >= commitVersion) {
					TraceEvent("AS_RestoreVersion").detail("RestoreVer", restoreVersion.get());
					break;
				} else {
					ryw_tr->reset();
					co_await delay(0.2);
					continue;
				}
			} catch (Error& e) {
				err = e;
			}
			co_await ryw_tr->onError(err);
		}

		ryw_tr->reset();
		while (true) {
			Error err;
			try {
				co_await discontinueBackup(backupAgent, ryw_tr, tagName);
				co_await ryw_tr->commit();
				TraceEvent("AS_DiscontinuedBackup").log();
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_backup_unneeded || err.code() == error_code_backup_duplicate) {
				break;
			}
			co_await ryw_tr->onError(err);
		}

		co_await waitBackup(backupAgent, cx, tagName.toString(), StopWhenDone::True);
		TraceEvent("AS_BackupStopped").log();

		ryw_tr->reset();
		while (true) {
			Error err;
			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				for (auto& range : ranges) {
					ryw_tr->addReadConflictRange(range);
					ryw_tr->clear(range);
				}
				co_await ryw_tr->commit();
				TraceEvent("AS_ClearedRange").log();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await ryw_tr->onError(err);
		}

		Reference<IBackupContainer> bc = co_await backupConfig.backupContainer().getOrThrow(cx.getReference());

		bc = fileBackup::getBackupContainerWithProxy(bc);

		TraceEvent("AS_StartRestore").log();
		Standalone<VectorRef<KeyRangeRef>> restoreRange;
		Standalone<VectorRef<KeyRangeRef>> systemRestoreRange;
		for (auto r : ranges) {
			restoreRange.push_back_deep(restoreRange.arena(), r);
		}
		if (!systemRestoreRange.empty()) {
			// restore system keys
			co_await restore(backupAgent,
			                 cx,
			                 cx,
			                 "system_restore"_sr,
			                 KeyRef(bc->getURL()),
			                 bc->getProxy(),
			                 systemRestoreRange,
			                 {},
			                 WaitForComplete::True,
			                 ::invalidVersion,
			                 Verbose::True,
			                 addPrefix,
			                 removePrefix,
			                 LockDB::True,
			                 UnlockDB::False,
			                 OnlyApplyMutationLogs::False,
			                 InconsistentSnapshotOnly::False,
			                 {},
			                 randomUid);
			auto rywTransaction = makeReference<ReadYourWritesTransaction>(cx);
			// clear old restore config associated with system keys
			while (true) {
				Error err;
				try {
					rywTransaction->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					rywTransaction->setOption(FDBTransactionOptions::LOCK_AWARE);
					RestoreConfig oldRestore(randomUid);
					oldRestore.clear(rywTransaction);
					co_await rywTransaction->commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await rywTransaction->onError(err);
			}
		}
		// restore user data
		Version ver = co_await restore(backupAgent,
		                               cx,
		                               cx,
		                               tagName,
		                               KeyRef(bc->getURL()),
		                               bc->getProxy(),
		                               restoreRange,
		                               {},
		                               WaitForComplete::True,
		                               ::invalidVersion,
		                               Verbose::True,
		                               addPrefix,
		                               removePrefix,
		                               LockDB::True,
		                               UnlockDB::True,
		                               OnlyApplyMutationLogs::False,
		                               InconsistentSnapshotOnly::False,
		                               {},
		                               randomUid);
		co_return ver;
	}
};

const int FileBackupAgent::dataFooterSize = 20;

Future<Version> FileBackupAgent::restore(Database cx,
                                         Optional<Database> cxOrig,
                                         Key tagName,
                                         Key url,
                                         Optional<std::string> proxy,
                                         Standalone<VectorRef<KeyRangeRef>> ranges,
                                         Standalone<VectorRef<Version>> versions,
                                         WaitForComplete waitForComplete,
                                         Version targetVersion,
                                         Verbose verbose,
                                         Key addPrefix,
                                         Key removePrefix,
                                         LockDB lockDB,
                                         UnlockDB unlockDB,
                                         OnlyApplyMutationLogs onlyApplyMutationLogs,
                                         InconsistentSnapshotOnly inconsistentSnapshotOnly,
                                         Optional<std::string> const& encryptionKeyFileName,
                                         Optional<UID> lockUID,
                                         bool useRangeFileRestore) {
	return FileBackupAgentImpl::restore(this,
	                                    cx,
	                                    cxOrig,
	                                    tagName,
	                                    url,
	                                    proxy,
	                                    ranges,
	                                    versions,
	                                    waitForComplete,
	                                    targetVersion,
	                                    verbose,
	                                    addPrefix,
	                                    removePrefix,
	                                    lockDB,
	                                    unlockDB,
	                                    onlyApplyMutationLogs,
	                                    inconsistentSnapshotOnly,
	                                    encryptionKeyFileName,
	                                    lockUID.present() ? lockUID.get() : deterministicRandom()->randomUniqueID(),
	                                    useRangeFileRestore);
}

Future<Version> FileBackupAgent::restore(Database cx,
                                         Optional<Database> cxOrig,
                                         Key tagName,
                                         Key url,
                                         Optional<std::string> proxy,
                                         Standalone<VectorRef<KeyRangeRef>> ranges,
                                         WaitForComplete waitForComplete,
                                         Version targetVersion,
                                         Verbose verbose,
                                         Key addPrefix,
                                         Key removePrefix,
                                         LockDB lockDB,
                                         UnlockDB unlockDB,
                                         OnlyApplyMutationLogs onlyApplyMutationLogs,
                                         InconsistentSnapshotOnly inconsistentSnapshotOnly,
                                         Version beginVersion,
                                         Optional<std::string> const& encryptionKeyFileName,
                                         Optional<UID> lockUID,
                                         bool useRangeFileRestore) {
	Standalone<VectorRef<Version>> beginVersions;
	for (auto i = 0; i < ranges.size(); ++i) {
		beginVersions.push_back(beginVersions.arena(), beginVersion);
	}
	return restore(cx,
	               cxOrig,
	               tagName,
	               url,
	               proxy,
	               ranges,
	               beginVersions,
	               waitForComplete,
	               targetVersion,
	               verbose,
	               addPrefix,
	               removePrefix,
	               lockDB,
	               unlockDB,
	               onlyApplyMutationLogs,
	               inconsistentSnapshotOnly,
	               encryptionKeyFileName,
	               lockUID,
	               useRangeFileRestore);
}

Future<Version> FileBackupAgent::restore(Database cx,
                                         Optional<Database> cxOrig,
                                         Key tagName,
                                         Key url,
                                         Optional<std::string> proxy,
                                         WaitForComplete waitForComplete,
                                         Version targetVersion,
                                         Verbose verbose,
                                         KeyRange range,
                                         Key addPrefix,
                                         Key removePrefix,
                                         LockDB lockDB,
                                         OnlyApplyMutationLogs onlyApplyMutationLogs,
                                         InconsistentSnapshotOnly inconsistentSnapshotOnly,
                                         Version beginVersion,
                                         Optional<std::string> const& encryptionKeyFileName,
                                         bool useRangeFileRestore) {
	Standalone<VectorRef<KeyRangeRef>> rangeRef;
	if (range.begin.empty() && range.end.empty()) {
		addDefaultBackupRanges(rangeRef);
	} else {
		rangeRef.push_back_deep(rangeRef.arena(), range);
	}
	Standalone<VectorRef<Version>> versionRef;
	versionRef.push_back(versionRef.arena(), beginVersion);

	return restore(cx,
	               cxOrig,
	               tagName,
	               url,
	               proxy,
	               rangeRef,
	               versionRef,
	               waitForComplete,
	               targetVersion,
	               verbose,
	               addPrefix,
	               removePrefix,
	               lockDB,
	               UnlockDB::True,
	               onlyApplyMutationLogs,
	               inconsistentSnapshotOnly,
	               encryptionKeyFileName,
	               {},
	               useRangeFileRestore);
}

Future<Version> FileBackupAgent::atomicRestore(Database cx,
                                               Key tagName,
                                               KeyRange range,
                                               Key addPrefix,
                                               Key removePrefix) {
	Standalone<VectorRef<KeyRangeRef>> rangeRef;
	if (range.begin.empty() && range.end.empty()) {
		addDefaultBackupRanges(rangeRef);
	} else {
		rangeRef.push_back_deep(rangeRef.arena(), range);
	}
	return atomicRestore(cx, tagName, rangeRef, addPrefix, removePrefix);
}

Future<Version> FileBackupAgent::atomicRestore(Database cx,
                                               Key tagName,
                                               Standalone<VectorRef<KeyRangeRef>> ranges,
                                               Key addPrefix,
                                               Key removePrefix) {
	return FileBackupAgentImpl::atomicRestore(this, cx, tagName, ranges, addPrefix, removePrefix);
}

Future<ERestoreState> FileBackupAgent::abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::abortRestore(tr, tagName);
}

Future<ERestoreState> FileBackupAgent::abortRestore(Database cx, Key tagName) {
	return fileBackup::abortRestore(cx, tagName);
}

Future<std::string> FileBackupAgent::restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::restoreStatus(tr, tagName);
}

Future<ERestoreState> FileBackupAgent::waitRestore(Database cx, Key tagName, Verbose verbose) {
	return FileBackupAgentImpl::waitRestore(cx, tagName, verbose);
};

Future<Void> FileBackupAgent::submitBackup(Reference<ReadYourWritesTransaction> tr,
                                           Key outContainer,
                                           Optional<std::string> proxy,
                                           int initialSnapshotIntervalSeconds,
                                           int snapshotIntervalSeconds,
                                           std::string const& tagName,
                                           Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                           StopWhenDone stopWhenDone,
                                           UsePartitionedLog partitionedLog,
                                           IncrementalBackupOnly incrementalBackupOnly,
                                           Optional<std::string> const& encryptionKeyFileName,
                                           int snapshotMode) {
	return FileBackupAgentImpl::submitBackup(this,
	                                         tr,
	                                         outContainer,
	                                         proxy,
	                                         initialSnapshotIntervalSeconds,
	                                         snapshotIntervalSeconds,
	                                         tagName,
	                                         backupRanges,
	                                         stopWhenDone,
	                                         partitionedLog,
	                                         incrementalBackupOnly,
	                                         encryptionKeyFileName,
	                                         snapshotMode);
}

Future<Void> FileBackupAgent::discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return FileBackupAgentImpl::discontinueBackup(this, tr, tagName);
}

Future<Void> FileBackupAgent::abortBackup(Reference<ReadYourWritesTransaction> tr, std::string tagName) {
	return FileBackupAgentImpl::abortBackup(this, tr, tagName);
}

Future<Void> FileBackupAgent::checkAndDisableBackupWorkers(Database cx) {
	return FileBackupAgentImpl::checkAndDisableBackupWorkers(cx);
}

Future<std::string> FileBackupAgent::getStatus(Database cx, ShowErrors showErrors, std::string tagName) {
	return FileBackupAgentImpl::getStatus(this, cx, showErrors, tagName);
}

Future<std::string> FileBackupAgent::getStatusJSON(Database cx, std::string tagName) {
	return FileBackupAgentImpl::getStatusJSON(this, cx, tagName);
}

Future<Optional<Version>> FileBackupAgent::getLastRestorable(Reference<ReadYourWritesTransaction> tr,
                                                             Key tagName,
                                                             Snapshot snapshot) {
	return FileBackupAgentImpl::getLastRestorable(this, tr, tagName, snapshot);
}

void FileBackupAgent::setLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName, Version version) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->set(lastRestorable.pack(tagName), BinaryWriter::toValue<Version>(version, Unversioned()));
}

Future<EBackupState> FileBackupAgent::waitBackup(Database cx,
                                                 std::string tagName,
                                                 StopWhenDone stopWhenDone,
                                                 Reference<IBackupContainer>* pContainer,
                                                 UID* pUID) {
	return FileBackupAgentImpl::waitBackup(this, cx, tagName, stopWhenDone, pContainer, pUID);
}

Future<Void> FileBackupAgent::changePause(Database db, bool pause) {
	return FileBackupAgentImpl::changePause(this, db, pause);
}

// Fast Restore addPrefix test helper functions
static std::pair<bool, bool> insideValidRange(KeyValueRef kv,
                                              Standalone<VectorRef<KeyRangeRef>> restoreRanges,
                                              Standalone<VectorRef<KeyRangeRef>> backupRanges) {
	bool insideRestoreRange = false;
	bool insideBackupRange = false;
	for (auto& range : restoreRanges) {
		TraceEvent(SevFRTestInfo, "InsideValidRestoreRange")
		    .detail("Key", kv.key)
		    .detail("Range", range)
		    .detail("Inside", (kv.key >= range.begin && kv.key < range.end));
		if (kv.key >= range.begin && kv.key < range.end) {
			insideRestoreRange = true;
			break;
		}
	}
	for (auto& range : backupRanges) {
		TraceEvent(SevFRTestInfo, "InsideValidBackupRange")
		    .detail("Key", kv.key)
		    .detail("Range", range)
		    .detail("Inside", (kv.key >= range.begin && kv.key < range.end));
		if (kv.key >= range.begin && kv.key < range.end) {
			insideBackupRange = true;
			break;
		}
	}
	return std::make_pair(insideBackupRange, insideRestoreRange);
}

// Write [begin, end) in kvs to DB
static Future<Void> writeKVs(Database cx, Standalone<VectorRef<KeyValueRef>> kvs, int begin, int end) {
	co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		int index = begin;
		while (index < end) {
			TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKV")
			    .detail("Index", index)
			    .detail("KVs", kvs.size())
			    .detail("Key", kvs[index].key)
			    .detail("Value", kvs[index].value);
			tr->set(kvs[index].key, kvs[index].value);
			++index;
		}
		return Void();
	});

	// Sanity check data has been written to DB
	ReadYourWritesTransaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			KeyRef k1 = kvs[begin].key;
			KeyRef k2 = end < kvs.size() ? kvs[end].key : allKeys.end;
			TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKVReadBack")
			    .detail("Range", KeyRangeRef(k1, k2))
			    .detail("Begin", begin)
			    .detail("End", end);
			RangeResult readKVs = co_await tr.getRange(KeyRangeRef(k1, k2), CLIENT_KNOBS->TOO_MANY);
			ASSERT(!readKVs.empty() || begin == end);
			break;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent("TransformDatabaseContentsWriteKVReadBackError").error(err);
		co_await tr.onError(err);
	}

	TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKVDone").detail("Begin", begin).detail("End", end);
}

void simulateBlobFailure() {
	if (BUGGIFY && deterministicRandom()->random01() < 0.01) { // Simulate blob failures
		double i = deterministicRandom()->random01();
		if (i < 0.5) {
			throw http_request_failed();
		} else if (i < 0.7) {
			throw connection_failed();
		} else if (i < 0.8) {
			throw timed_out();
		} else if (i < 0.9) {
			throw lookup_failed();
		}
	}
}
