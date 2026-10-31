/*
 * RocksDBCommon.cpp
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

#include "RocksDBCommon.h"

#ifdef WITH_ROCKSDB

#include "fdbserver/core/Knobs.h"
#include "flow/Platform.h"
#include "flow/Trace.h"

namespace RocksDBCommon {

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

std::string getErrorReason(rocksdb::BackgroundErrorReason reason) {
	switch (reason) {
	case rocksdb::BackgroundErrorReason::kFlush:
		return format("%d Flush", reason);
	case rocksdb::BackgroundErrorReason::kCompaction:
		return format("%d Compaction", reason);
	case rocksdb::BackgroundErrorReason::kWriteCallback:
		return format("%d WriteCallback", reason);
	case rocksdb::BackgroundErrorReason::kMemTable:
		return format("%d MemTable", reason);
	case rocksdb::BackgroundErrorReason::kManifestWrite:
		return format("%d ManifestWrite", reason);
	case rocksdb::BackgroundErrorReason::kFlushNoWAL:
		return format("%d FlushNoWAL", reason);
	case rocksdb::BackgroundErrorReason::kManifestWriteNoWAL:
		return format("%d ManifestWriteNoWAL", reason);
	default:
		return format("%d Unknown", reason);
	}
}

RocksDBBackgroundError getBackgroundError(rocksdb::BackgroundErrorReason reason, const rocksdb::Status& status) {
	int errorCode;
	if (status.IsIOError()) {
		errorCode = error_code_io_error;
	} else if (status.IsCorruption()) {
		errorCode = error_code_file_corrupt;
	} else {
		errorCode = error_code_unknown_error;
	}

	return RocksDBBackgroundError{ reason, static_cast<int>(status.severity()), status.ToString(), errorCode };
}

rocksdb::WALRecoveryMode getWalRecoveryModeFromKnob(int knobValue) {
	switch (knobValue) {
	case 0:
		return rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
	case 1:
		return rocksdb::WALRecoveryMode::kAbsoluteConsistency;
	case 2:
		return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
	case 3:
		return rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
	default:
		TraceEvent(SevWarn, "InvalidWalRecoveryMode").detail("KnobValue", knobValue);
		return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
	}
}

rocksdb::WALRecoveryMode getWalRecoveryMode() {
	return getWalRecoveryModeFromKnob(SERVER_KNOBS->ROCKSDB_WAL_RECOVERY_MODE);
}

rocksdb::CompactionPri getCompactionPriorityFromKnob(int knobValue) {
	switch (knobValue) {
	case 0:
		return rocksdb::CompactionPri::kByCompensatedSize;
	case 1:
		return rocksdb::CompactionPri::kOldestLargestSeqFirst;
	case 2:
		return rocksdb::CompactionPri::kOldestSmallestSeqFirst;
	case 3:
		return rocksdb::CompactionPri::kMinOverlappingRatio;
	case 4:
		return rocksdb::CompactionPri::kRoundRobin;
	default:
		TraceEvent(SevWarn, "InvalidCompactionPriority").detail("KnobValue", knobValue);
		return rocksdb::CompactionPri::kMinOverlappingRatio;
	}
}

rocksdb::BlockBasedTableOptions::IndexType getIndexTypeFromKnob(int knobValue) {
	switch (knobValue) {
	case 0:
		return rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch;
	case 1:
		return rocksdb::BlockBasedTableOptions::IndexType::kHashSearch;
	case 2:
		return rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
	case 3:
		return rocksdb::BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
	default:
		TraceEvent(SevWarn, "InvalidIndexType").detail("KnobValue", knobValue);
		return rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch;
	}
}

namespace {

// Forwards everything to its target Env, except that compaction pool threads set their nice value before running jobs.
class FdbRocksDBEnv : public rocksdb::EnvWrapper {
public:
	FdbRocksDBEnv(rocksdb::Env* target, int compactionThreadPriority)
	  : rocksdb::EnvWrapper(target), compactionThreadPriority(compactionThreadPriority) {
		// Use the target's FileSystem and SystemClock directly. By default, EnvWrapper routes them through RocksDB's
		// legacy Env API.
		file_system_ = target->GetFileSystem();
		system_clock_ = target->GetSystemClock();
	}

	const char* Name() const override { return "FdbRocksDBEnv"; }

	void Schedule(void (*function)(void*),
	              void* arg,
	              Priority pri,
	              void* tag = nullptr,
	              void (*unschedFunction)(void*) = nullptr) override {
		// RocksDB runs compactions on LOW and BOTTOM. It only puts flushes on LOW when HIGH has no threads, and
		// opening a DB always gives HIGH at least one.
		if (!compactionThreadPriority || (pri != Priority::LOW && pri != Priority::BOTTOM)) {
			target()->Schedule(function, arg, pri, tag, unschedFunction);
			return;
		}
		auto* const job = new Job{ function, arg, unschedFunction, compactionThreadPriority };
		target()->Schedule(&runJob, job, pri, tag, &unscheduleJob);
	}

private:
	struct Job {
		void (*function)(void*);
		void* arg;
		void (*unschedFunction)(void*);
		int threadPriority;
	};

	// Runs on a RocksDB pool thread: sets the thread's nice value once, then runs RocksDB's job.
	static void runJob(void* p) {
		Job* const owned = static_cast<Job*>(p);
		const Job job = *owned;
		delete owned;
		thread_local bool prioritySet = false;
		if (!prioritySet) {
			setThreadPriority(job.threadPriority);
			prioritySet = true;
		}
		job.function(job.arg);
	}

	// Runs instead of runJob if RocksDB cancels the job (e.g. on close). RocksDB's own callback must still run, or
	// DB::Close() waits forever for the job to finish.
	static void unscheduleJob(void* p) {
		Job* const owned = static_cast<Job*>(p);
		const Job job = *owned;
		delete owned;
		if (job.unschedFunction) {
			job.unschedFunction(job.arg);
		}
	}

	const int compactionThreadPriority;
};

} // namespace

rocksdb::Env* getFdbRocksDBEnv() {
	// Never destroyed: RocksDB's threads and DBs keep this pointer for the life of the process, like Env::Default().
	static FdbRocksDBEnv* const env = [] {
		const int priority = SERVER_KNOBS->ROCKSDB_COMPACTION_THREAD_PRIORITY;
		if (priority < 0) {
			TraceEvent(SevWarnAlways, "RocksDBCompactionThreadPriorityNegative")
			    .detail("Priority", priority)
			    .detail("Note", "Negative nice values need CAP_SYS_NICE; otherwise they are ignored");
		}
		return new FdbRocksDBEnv(rocksdb::Env::Default(), priority);
	}();
	return env;
}

} // namespace RocksDBCommon

#endif // WITH_ROCKSDB
