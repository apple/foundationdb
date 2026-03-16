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

#include "fdbserver/RocksDBCommon.h"

#ifdef WITH_ROCKSDB

#include "fdbserver/Knobs.h"
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

} // namespace RocksDBCommon

#endif // WITH_ROCKSDB
