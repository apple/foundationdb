#include "fdbclient/FDBTypes.h"
#ifdef WITH_ROCKSDB

#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "flow/flow.h"
#include "flow/serialize.h"
#include <rocksdb/c.h>
#include <rocksdb/cache.h>
#include <rocksdb/advanced_cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/listener.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/table_properties_collectors.h>
#include <rocksdb/version.h>
#if defined __has_include
#if __has_include(<liburing.h>)
#include <liburing.h>
#endif
#endif
#include "fdbclient/SystemData.h"
#include "fdbserver/CoroFlow.h"
#include "fdbserver/FDBRocksDBVersion.h"
#include "flow/flow.h"
#include "flow/IThreadPool.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Histogram.h"
#include "flow/UnitTest.h"

#include <memory>
#include <tuple>
#include <vector>

#endif // WITH_ROCKSDB

#include "fdbserver/Knobs.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "flow/actorcompiler.h" // has to be last include

#ifdef WITH_ROCKSDB

// Enforcing rocksdb version.
static_assert((ROCKSDB_MAJOR == FDB_ROCKSDB_MAJOR && ROCKSDB_MINOR == FDB_ROCKSDB_MINOR &&
               ROCKSDB_PATCH == FDB_ROCKSDB_PATCH),
              "Unsupported rocksdb version.");

const std::string METADATA_SHARD_ID = "kvs-metadata";
const std::string DEFAULT_CF_NAME = "default"; // `specialKeys` is stored in this culoumn family.
const std::string manifestFilePrefix = "MANIFEST-";
const KeyRef shardMappingPrefix("\xff\xff/ShardMapping/"_sr);
const KeyRef compactionTimestampPrefix("\xff\xff/CompactionTimestamp/"_sr);
// TODO: move constants to a header file.
const KeyRef persistVersion = "\xff\xffVersion"_sr;
const StringRef ROCKSDBSTORAGE_HISTOGRAM_GROUP = "RocksDBStorage"_sr;
const StringRef ROCKSDB_COMMIT_LATENCY_HISTOGRAM = "RocksDBCommitLatency"_sr;
const StringRef ROCKSDB_COMMIT_ACTION_HISTOGRAM = "RocksDBCommitAction"_sr;
const StringRef ROCKSDB_COMMIT_QUEUEWAIT_HISTOGRAM = "RocksDBCommitQueueWait"_sr;
const StringRef ROCKSDB_WRITE_HISTOGRAM = "RocksDBWrite"_sr;
const StringRef ROCKSDB_DELETE_COMPACTRANGE_HISTOGRAM = "RocksDBDeleteCompactRange"_sr;
const StringRef ROCKSDB_READRANGE_LATENCY_HISTOGRAM = "RocksDBReadRangeLatency"_sr;
const StringRef ROCKSDB_READVALUE_LATENCY_HISTOGRAM = "RocksDBReadValueLatency"_sr;
const StringRef ROCKSDB_READPREFIX_LATENCY_HISTOGRAM = "RocksDBReadPrefixLatency"_sr;
const StringRef ROCKSDB_READRANGE_ACTION_HISTOGRAM = "RocksDBReadRangeAction"_sr;
const StringRef ROCKSDB_READVALUE_ACTION_HISTOGRAM = "RocksDBReadValueAction"_sr;
const StringRef ROCKSDB_READPREFIX_ACTION_HISTOGRAM = "RocksDBReadPrefixAction"_sr;
const StringRef ROCKSDB_READRANGE_QUEUEWAIT_HISTOGRAM = "RocksDBReadRangeQueueWait"_sr;
const StringRef ROCKSDB_READVALUE_QUEUEWAIT_HISTOGRAM = "RocksDBReadValueQueueWait"_sr;
const StringRef ROCKSDB_READPREFIX_QUEUEWAIT_HISTOGRAM = "RocksDBReadPrefixQueueWait"_sr;
const StringRef ROCKSDB_READRANGE_NEWITERATOR_HISTOGRAM = "RocksDBReadRangeNewIterator"_sr;
const StringRef ROCKSDB_READVALUE_GET_HISTOGRAM = "RocksDBReadValueGet"_sr;
const StringRef ROCKSDB_READPREFIX_GET_HISTOGRAM = "RocksDBReadPrefixGet"_sr;
// Flush reason code:
// https://github.com/facebook/rocksdb/blob/63a5125a5220d953bf504daf33694f038403cc7c/include/rocksdb/listener.h#L164-L181
// This function needs to be updated when flush code changes.
const int ROCKSDB_NUM_FLUSH_REASONS = 14;

namespace {
struct PhysicalShard;
struct DataShard;
struct ReadIterator;
struct ShardedRocksDBKeyValueStore;

using rocksdb::BackgroundErrorReason;
using rocksdb::CompactionReason;
using rocksdb::FlushReason;

// Returns string representation of RocksDB background error reason.
// Error reason code:
// https://github.com/facebook/rocksdb/blob/12d798ac06bcce36be703b057d5f5f4dab3b270c/include/rocksdb/listener.h#L125
// This function needs to be updated when error code changes.
std::string getErrorReason(BackgroundErrorReason reason) {
	switch (reason) {
	case BackgroundErrorReason::kFlush:
		return format("%d Flush", reason);
	case BackgroundErrorReason::kCompaction:
		return format("%d Compaction", reason);
	case BackgroundErrorReason::kWriteCallback:
		return format("%d WriteCallback", reason);
	case BackgroundErrorReason::kMemTable:
		return format("%d MemTable", reason);
	case BackgroundErrorReason::kManifestWrite:
		return format("%d ManifestWrite", reason);
	case BackgroundErrorReason::kFlushNoWAL:
		return format("%d FlushNoWAL", reason);
	case BackgroundErrorReason::kManifestWriteNoWAL:
		return format("%d ManifestWriteNoWAL", reason);
	default:
		return format("%d Unknown", reason);
	}
}

ACTOR Future<Void> forwardError(Future<int> input) {
	int errorCode = wait(input);
	if (errorCode == error_code_success) {
		return Never();
	}
	throw Error::fromCode(errorCode);
}

int getWriteStallState(const rocksdb::WriteStallCondition& condition) {
	if (condition == rocksdb::WriteStallCondition::kDelayed)
		return 0;
	if (condition == rocksdb::WriteStallCondition::kStopped)
		return 1;
	if (condition == rocksdb::WriteStallCondition::kNormal)
		return 2;

	// unexpected.
	return 3;
}

// RocksDB's reason string contains spaces and will break trace events.
// Reference https://sourcegraph.com/github.com/facebook/rocksdb/-/blob/db/flush_job.cc?L52
const char* getFlushReasonString(FlushReason flush_reason) {
	switch (flush_reason) {
	case FlushReason::kOthers:
		return "ReasonOthers";
	case FlushReason::kGetLiveFiles:
		return "ReasonGetLiveFiles";
	case FlushReason::kShutDown:
		return "ReasonShutdown";
	case FlushReason::kExternalFileIngestion:
		return "ReasonExternalFileIngestion";
	case FlushReason::kManualCompaction:
		return "ReasonManualCompaction";
	case FlushReason::kWriteBufferManager:
		return "ReasonWriteBufferManager";
	case FlushReason::kWriteBufferFull:
		return "ReasonWriteBufferFull";
	case FlushReason::kTest:
		return "ReasonTest";
	case FlushReason::kDeleteFiles:
		return "ReasonDeleteFiles";
	case FlushReason::kAutoCompaction:
		return "ReasonAutoCompaction";
	case FlushReason::kManualFlush:
		return "ReasonManualFlush";
	case FlushReason::kErrorRecovery:
		return "ReasonErrorRecovery";
	case FlushReason::kErrorRecoveryRetryFlush:
		return "ReasonErrorRecoveryRetryFlush";
	case FlushReason::kWalFull:
		return "ReasonWALFull";
	case FlushReason::kCatchUpAfterErrorRecovery:
		return "ReasonCatchUpAfterErrorRecovery";
	default:
		return "ReasonInvalid";
	}
}

class RocksDBEventListener : public rocksdb::EventListener {
public:
	RocksDBEventListener(UID id)
	  : logId(id), compactionReasons((int)CompactionReason::kNumOfReasons), flushReasons(ROCKSDB_NUM_FLUSH_REASONS),
	    lastResetTime(now()) {}

	void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override {
		auto curState = getWriteStallState(info.condition.cur);
		auto prevState = getWriteStallState(info.condition.prev);
		if (curState == 1) {
			TraceEvent(SevWarn, "WriteStallInfo", logId)
			    .detail("CF", info.cf_name)
			    .detail("CurrentState", curState)
			    .detail("PrevState", prevState);
		}
	}

	void OnFlushBegin(rocksdb::DB* db, const rocksdb::FlushJobInfo& info) override {
		flushTotal++;
		auto index = (int)info.flush_reason;
		if (index >= ROCKSDB_NUM_FLUSH_REASONS) {
			TraceEvent(SevWarn, "UnknownRocksDBFlushReason", logId)
			    .suppressFor(5.0)
			    .detail("Reason", static_cast<int>(info.flush_reason));
			return;
		}

		flushReasons[index]++;
	}

	void OnCompactionBegin(rocksdb::DB* db, const rocksdb::CompactionJobInfo& info) override {
		compactionTotal++;
		auto index = (int)info.compaction_reason;
		if (index >= (int)CompactionReason::kNumOfReasons) {
			TraceEvent(SevWarn, "UnknownRocksDBCompactionReason", logId)
			    .suppressFor(5.0)
			    .detail("Reason", static_cast<int>(info.compaction_reason));
			return;
		}
		compactionReasons[index]++;
	}

	void logRecentRocksDBBackgroundWorkStats(UID ssId, std::string logReason = "PeriodicLog") {
		int flushCount = flushTotal.load(std::memory_order_relaxed);
		int compactionCount = compactionTotal.load(std::memory_order_relaxed);
		if (flushCount > 0) {
			TraceEvent e(SevInfo, "RocksDBFlushStats", logId);
			e.setMaxEventLength(20000);
			e.detail("LogReason", logReason);
			e.detail("StorageServerID", ssId);
			e.detail("DurationSeconds", now() - lastResetTime);
			e.detail("FlushCountTotal", flushCount);
			for (int i = 0; i < ROCKSDB_NUM_FLUSH_REASONS; ++i) {
				e.detail(getFlushReasonString((rocksdb::FlushReason)i), flushReasons[i]);
			}
		}
		if (compactionCount > 0) {
			TraceEvent e(SevInfo, "RocksDBCompactionStats", logId);
			e.setMaxEventLength(20000);
			e.detail("LogReason", logReason);
			e.detail("StorageServerID", ssId);
			e.detail("DurationSeconds", now() - lastResetTime);
			e.detail("CompactionTotal", compactionCount);
			for (int i = 0; i < (int)CompactionReason::kNumOfReasons; ++i) {
				e.detail(rocksdb::GetCompactionReasonString((rocksdb::CompactionReason)i), compactionReasons[i]);
			}
		}
		return;
	}

	void resetCounters() {
		flushTotal.store(0, std::memory_order_relaxed);
		compactionTotal.store(0, std::memory_order_relaxed);

		for (auto& flushCounter : flushReasons) {
			flushCounter.store(0, std::memory_order_relaxed);
		}

		for (auto& compactionCounter : compactionReasons) {
			compactionCounter.store(0, std::memory_order_relaxed);
		}
		lastResetTime = now();
	}

private:
	UID logId;

	std::vector<std::atomic_int> flushReasons;
	std::vector<std::atomic_int> compactionReasons;
	std::atomic_int flushTotal;
	std::atomic_int compactionTotal;

	double lastResetTime;
};

// Background error handling is tested with Chaos test.
// TODO: Test background error in simulation. RocksDB doesn't use flow IO in simulation, which limits our ability to
// inject IO errors. We could implement rocksdb::FileSystem using flow IO to unblock simulation. Also, trace event is
// not available on background threads because trace event requires setting up special thread locals. Using trace event
// could potentially cause segmentation fault.
class RocksDBErrorListener : public rocksdb::EventListener {
public:
	RocksDBErrorListener(){};
	void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status* bg_error) override {
		if (!bg_error)
			return;
		TraceEvent(SevError, "ShardedRocksDBBGError")
		    .detail("Reason", getErrorReason(reason))
		    .detail("ShardedRocksDBSeverity", bg_error->severity())
		    .detail("Status", bg_error->ToString());

		std::unique_lock<std::mutex> lock(mutex);
		if (!errorPromise.isValid())
			return;
		// RocksDB generates two types of background errors, IO Error and Corruption
		// Error type and severity map could be found at
		// https://github.com/facebook/rocksdb/blob/2e09a54c4fb82e88bcaa3e7cfa8ccbbbbf3635d5/db/error_handler.cc#L138.
		// All background errors will be treated as storage engine failure. Send the error to storage server.
		if (bg_error->IsIOError()) {
			errorPromise.send(error_code_io_error);
		} else if (bg_error->IsCorruption()) {
			errorPromise.send(error_code_file_corrupt);
		} else {
			errorPromise.send(error_code_unknown_error);
		}
	}
	Future<int> getFuture() {
		std::unique_lock<std::mutex> lock(mutex);
		return errorPromise.getFuture();
	}
	~RocksDBErrorListener() {
		std::unique_lock<std::mutex> lock(mutex);
		if (!errorPromise.isValid())
			return;
		errorPromise.send(error_code_success);
	}

private:
	ThreadReturnPromise<int> errorPromise;
	std::mutex mutex;
};

// Encapsulation of shared states.
struct ShardedRocksDBState {
	bool closing = false;
};

std::shared_ptr<rocksdb::Cache> rocksdb_block_cache = nullptr;

rocksdb::ExportImportFilesMetaData getMetaData(const CheckpointMetaData& checkpoint) {
	rocksdb::ExportImportFilesMetaData metaData;
	if (checkpoint.getFormat() != DataMoveRocksCF) {
		return metaData;
	}

	RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(checkpoint);
	metaData.db_comparator_name = rocksCF.dbComparatorName;

	for (const LiveFileMetaData& fileMetaData : rocksCF.sstFiles) {
		rocksdb::LiveFileMetaData liveFileMetaData;
		liveFileMetaData.relative_filename = fileMetaData.relative_filename;
		liveFileMetaData.directory = fileMetaData.directory;
		liveFileMetaData.file_number = fileMetaData.file_number;
		liveFileMetaData.file_type = static_cast<rocksdb::FileType>(fileMetaData.file_type);
		liveFileMetaData.size = fileMetaData.size;
		liveFileMetaData.temperature = static_cast<rocksdb::Temperature>(fileMetaData.temperature);
		liveFileMetaData.file_checksum = fileMetaData.file_checksum;
		liveFileMetaData.file_checksum_func_name = fileMetaData.file_checksum_func_name;
		liveFileMetaData.smallest_seqno = fileMetaData.smallest_seqno;
		liveFileMetaData.largest_seqno = fileMetaData.largest_seqno;
		liveFileMetaData.smallestkey = fileMetaData.smallestkey;
		liveFileMetaData.largestkey = fileMetaData.largestkey;
		liveFileMetaData.num_reads_sampled = fileMetaData.num_reads_sampled;
		liveFileMetaData.being_compacted = fileMetaData.being_compacted;
		liveFileMetaData.num_entries = fileMetaData.num_entries;
		liveFileMetaData.num_deletions = fileMetaData.num_deletions;
		liveFileMetaData.oldest_blob_file_number = fileMetaData.oldest_blob_file_number;
		liveFileMetaData.oldest_ancester_time = fileMetaData.oldest_ancester_time;
		liveFileMetaData.file_creation_time = fileMetaData.file_creation_time;
		liveFileMetaData.smallest = fileMetaData.smallest;
		liveFileMetaData.largest = fileMetaData.largest;
		liveFileMetaData.file_type = rocksdb::kTableFile;
		liveFileMetaData.epoch_number = fileMetaData.epoch_number;
		liveFileMetaData.name = fileMetaData.name;
		liveFileMetaData.db_path = fileMetaData.db_path;
		liveFileMetaData.column_family_name = fileMetaData.column_family_name;
		liveFileMetaData.level = fileMetaData.level;
		metaData.files.push_back(liveFileMetaData);
	}

	return metaData;
}

void populateMetaData(CheckpointMetaData* checkpoint, const rocksdb::ExportImportFilesMetaData* metaData) {
	RocksDBColumnFamilyCheckpoint rocksCF;
	if (metaData != nullptr) {
		rocksCF.dbComparatorName = metaData->db_comparator_name;
		for (const rocksdb::LiveFileMetaData& fileMetaData : metaData->files) {
			LiveFileMetaData liveFileMetaData;
			liveFileMetaData.relative_filename = fileMetaData.relative_filename;
			liveFileMetaData.directory = fileMetaData.directory;
			liveFileMetaData.file_number = fileMetaData.file_number;
			liveFileMetaData.file_type = static_cast<int>(fileMetaData.file_type);
			liveFileMetaData.size = fileMetaData.size;
			liveFileMetaData.temperature = static_cast<uint8_t>(fileMetaData.temperature);
			liveFileMetaData.file_checksum = fileMetaData.file_checksum;
			liveFileMetaData.file_checksum_func_name = fileMetaData.file_checksum_func_name;
			liveFileMetaData.smallest_seqno = fileMetaData.smallest_seqno;
			liveFileMetaData.largest_seqno = fileMetaData.largest_seqno;
			liveFileMetaData.smallestkey = fileMetaData.smallestkey;
			liveFileMetaData.largestkey = fileMetaData.largestkey;
			liveFileMetaData.num_reads_sampled = fileMetaData.num_reads_sampled;
			liveFileMetaData.being_compacted = fileMetaData.being_compacted;
			liveFileMetaData.num_entries = fileMetaData.num_entries;
			liveFileMetaData.num_deletions = fileMetaData.num_deletions;
			liveFileMetaData.oldest_blob_file_number = fileMetaData.oldest_blob_file_number;
			liveFileMetaData.oldest_ancester_time = fileMetaData.oldest_ancester_time;
			liveFileMetaData.file_creation_time = fileMetaData.file_creation_time;
			liveFileMetaData.smallest = fileMetaData.smallest;
			liveFileMetaData.largest = fileMetaData.largest;
			liveFileMetaData.epoch_number = fileMetaData.epoch_number;
			liveFileMetaData.name = fileMetaData.name;
			liveFileMetaData.db_path = fileMetaData.db_path;
			liveFileMetaData.column_family_name = fileMetaData.column_family_name;
			liveFileMetaData.level = fileMetaData.level;
			rocksCF.sstFiles.push_back(liveFileMetaData);
		}
	}
	checkpoint->setFormat(DataMoveRocksCF);
	checkpoint->serializedCheckpoint = ObjectWriter::toValue(rocksCF, IncludeVersion());
}

const rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

std::string getShardMappingKey(KeyRef key, StringRef prefix) {
	return prefix.toString() + key.toString();
}

void logRocksDBError(const rocksdb::Status& status, const std::string& method) {
	auto level = status.IsTimedOut() ? SevWarn : SevError;
	TraceEvent e(level, "ShardedRocksDBError");
	e.setMaxFieldLength(10000)
	    .detail("Error", status.ToString())
	    .detail("Method", method)
	    .detail("ShardedRocksDBSeverity", status.severity());
	if (status.IsIOError()) {
		e.detail("SubCode", status.subcode());
	}
}

// TODO: define shard ops.
enum class ShardOp {
	CREATE,
	OPEN,
	DESTROY,
	CLOSE,
	MODIFY_RANGE,
};

const char* ShardOpToString(ShardOp op) {
	switch (op) {
	case ShardOp::CREATE:
		return "CREATE";
	case ShardOp::OPEN:
		return "OPEN";
	case ShardOp::DESTROY:
		return "DESTROY";
	case ShardOp::CLOSE:
		return "CLOSE";
	case ShardOp::MODIFY_RANGE:
		return "MODIFY_RANGE";
	default:
		return "Unknown";
	}
}
void logShardEvent(StringRef name, ShardOp op, Severity severity = SevInfo, const std::string& message = "") {
	TraceEvent e(severity, "ShardedRocksDBKVSShardEvent");
	e.detail("ShardId", name).detail("Action", ShardOpToString(op));
	if (!message.empty()) {
		e.detail("Message", message);
	}
}
void logShardEvent(StringRef name,
                   KeyRangeRef range,
                   ShardOp op,
                   Severity severity = SevInfo,
                   const std::string& message = "") {
	TraceEvent e(severity, "ShardedRocksDBKVSShardEvent");
	e.detail("ShardId", name)
	    .detail("Action", ShardOpToString(op))
	    .detail("Begin", range.begin)
	    .detail("End", range.end);
	if (message != "") {
		e.detail("Message", message);
	}
}

Error statusToError(const rocksdb::Status& s) {
	if (s.IsIOError()) {
		return io_error();
	} else if (s.IsTimedOut()) {
		return key_value_store_deadline_exceeded();
	} else {
		return unknown_error();
	}
}

rocksdb::CompactionPri getCompactionPriority() {
	switch (SERVER_KNOBS->ROCKSDB_COMPACTION_PRI) {
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
		TraceEvent(SevWarn, "InvalidCompactionPriority").detail("KnobValue", SERVER_KNOBS->ROCKSDB_COMPACTION_PRI);
		return rocksdb::CompactionPri::kMinOverlappingRatio;
	}
}

rocksdb::WALRecoveryMode getWalRecoveryMode() {
	switch (SERVER_KNOBS->ROCKSDB_WAL_RECOVERY_MODE) {
	case 0:
		return rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
	case 1:
		return rocksdb::WALRecoveryMode::kAbsoluteConsistency;
	case 2:
		return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
	case 3:
		return rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
	default:
		TraceEvent(SevWarn, "InvalidWalRecoveryMode").detail("KnobValue", SERVER_KNOBS->ROCKSDB_WAL_RECOVERY_MODE);
		return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
	}
}

rocksdb::ColumnFamilyOptions getCFOptions() {
	rocksdb::ColumnFamilyOptions options;

	if (SERVER_KNOBS->ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES) {
		options.level_compaction_dynamic_level_bytes = SERVER_KNOBS->ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES;
		options.OptimizeLevelStyleCompaction(SERVER_KNOBS->SHARDED_ROCKSDB_MEMTABLE_BUDGET);
	}
	options.write_buffer_size = SERVER_KNOBS->SHARDED_ROCKSDB_WRITE_BUFFER_SIZE;
	options.max_write_buffer_number = SERVER_KNOBS->SHARDED_ROCKSDB_MAX_WRITE_BUFFER_NUMBER;
	options.target_file_size_base = SERVER_KNOBS->SHARDED_ROCKSDB_TARGET_FILE_SIZE_BASE;
	options.target_file_size_multiplier = SERVER_KNOBS->SHARDED_ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER;

	if (SERVER_KNOBS->ROCKSDB_PERIODIC_COMPACTION_SECONDS > 0) {
		options.periodic_compaction_seconds = SERVER_KNOBS->ROCKSDB_PERIODIC_COMPACTION_SECONDS;
	}
	options.memtable_protection_bytes_per_key = SERVER_KNOBS->ROCKSDB_MEMTABLE_PROTECTION_BYTES_PER_KEY;
	options.block_protection_bytes_per_key = SERVER_KNOBS->ROCKSDB_BLOCK_PROTECTION_BYTES_PER_KEY;
	options.paranoid_file_checks = SERVER_KNOBS->ROCKSDB_PARANOID_FILE_CHECKS;
	options.memtable_max_range_deletions = SERVER_KNOBS->SHARDED_ROCKSDB_MEMTABLE_MAX_RANGE_DELETIONS;
	options.disable_auto_compactions = SERVER_KNOBS->ROCKSDB_DISABLE_AUTO_COMPACTIONS;
	if (SERVER_KNOBS->SHARD_SOFT_PENDING_COMPACT_BYTES_LIMIT > 0) {
		options.soft_pending_compaction_bytes_limit = SERVER_KNOBS->SHARD_SOFT_PENDING_COMPACT_BYTES_LIMIT;
	}
	if (SERVER_KNOBS->SHARD_HARD_PENDING_COMPACT_BYTES_LIMIT > 0) {
		options.hard_pending_compaction_bytes_limit = SERVER_KNOBS->SHARD_HARD_PENDING_COMPACT_BYTES_LIMIT;
	}

	// Compact sstables when there's too much deleted stuff.
	if (SERVER_KNOBS->ROCKSDB_ENABLE_COMPACT_ON_DELETION) {
		// Creates a factory of a table property collector that marks a SST
		// file as need-compaction when it observe at least "D" deletion
		// entries in any "N" consecutive entries, or the ratio of tombstone
		// entries >= deletion_ratio.

		// @param sliding_window_size "N". Note that this number will be
		//     round up to the smallest multiple of 128 that is no less
		//     than the specified size.
		// @param deletion_trigger "D".  Note that even when "N" is changed,
		//     the specified number for "D" will not be changed.
		// @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
		//     based on deletion ratio. Disabled by default.
		options.table_properties_collector_factories = { rocksdb::NewCompactOnDeletionCollectorFactory(
			SERVER_KNOBS->ROCKSDB_CDCF_SLIDING_WINDOW_SIZE,
			SERVER_KNOBS->ROCKSDB_CDCF_DELETION_TRIGGER,
			SERVER_KNOBS->ROCKSDB_CDCF_DELETION_RATIO) };
	}

	rocksdb::BlockBasedTableOptions bbOpts;
	if (SERVER_KNOBS->SHARDED_ROCKSDB_PREFIX_LEN > 0) {
		// Prefix blooms are used during Seek.
		options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(SERVER_KNOBS->SHARDED_ROCKSDB_PREFIX_LEN));

		// Also turn on bloom filters in the memtable.
		// TODO: Make a knob for this as well.
		options.memtable_prefix_bloom_size_ratio = 0.1;

		// 5 -- Can be read by RocksDB's versions since 6.6.0. Full and partitioned
		// filters use a generally faster and more accurate Bloom filter
		// implementation, with a different schema.
		// https://github.com/facebook/rocksdb/blob/b77569f18bfc77fb1d8a0b3218f6ecf571bc4988/include/rocksdb/table.h#L391
		bbOpts.format_version = 5;

		// Create and apply a bloom filter using the 10 bits
		// which should yield a ~1% false positive rate:
		// https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#full-filters-new-format
		bbOpts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

		// The whole key blooms are only used for point lookups.
		// https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#prefix-vs-whole-key
		bbOpts.whole_key_filtering = false;
	}

	options.level0_file_num_compaction_trigger = SERVER_KNOBS->SHARDED_ROCKSDB_LEVEL0_FILENUM_COMPACTION_TRIGGER;
	options.level0_slowdown_writes_trigger = SERVER_KNOBS->SHARDED_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
	options.level0_stop_writes_trigger = SERVER_KNOBS->SHARDED_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER;

	if (rocksdb_block_cache == nullptr && SERVER_KNOBS->SHARDED_ROCKSDB_BLOCK_CACHE_SIZE > 0) {
		rocksdb_block_cache =
		    rocksdb::NewLRUCache(SERVER_KNOBS->SHARDED_ROCKSDB_BLOCK_CACHE_SIZE,
		                         -1, /* num_shard_bits, default value:-1*/
		                         false, /* strict_capacity_limit, default value:false */
		                         SERVER_KNOBS->SHARDED_ROCKSDB_CACHE_HIGH_PRI_POOL_RATIO /* high_pri_pool_ratio */);
		bbOpts.cache_index_and_filter_blocks = SERVER_KNOBS->SHARDED_ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS;
		bbOpts.pin_l0_filter_and_index_blocks_in_cache = SERVER_KNOBS->SHARDED_ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS;
		bbOpts.cache_index_and_filter_blocks_with_high_priority =
		    SERVER_KNOBS->SHARDED_ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS;
	}
	bbOpts.block_cache = rocksdb_block_cache;

	options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbOpts));

	options.compaction_pri = getCompactionPriority();

	return options;
}

rocksdb::ColumnFamilyOptions getCFOptionsForInactiveShard() {
	if (g_network->isSimulated()) {
		return getCFOptions();
	} else {
		ASSERT(false); // FIXME: remove when SHARDED_ROCKSDB_DELAY_COMPACTION_FOR_DATA_MOVE feature is well tuned
	}
	auto options = getCFOptions();
	// never slowdown ingest.
	options.level0_file_num_compaction_trigger = (1 << 30);
	options.level0_slowdown_writes_trigger = (1 << 30);
	options.level0_stop_writes_trigger = (1 << 30);
	options.soft_pending_compaction_bytes_limit = 0;
	options.hard_pending_compaction_bytes_limit = 0;

	// no auto compactions please. The application should issue a
	// manual compaction after all data is loaded into L0.
	options.disable_auto_compactions = true;
	// A manual compaction run should pick all files in L0 in
	// a single compaction run.
	options.max_compaction_bytes = (static_cast<uint64_t>(1) << 60);

	// It is better to have only 2 levels, otherwise a manual
	// compaction would compact at every possible level, thereby
	// increasing the total time needed for compactions.
	options.num_levels = 2;

	return options;
}

rocksdb::DBOptions getOptions() {
	rocksdb::DBOptions options;
	options.avoid_unnecessary_blocking_io = true;
	options.create_if_missing = true;
	options.atomic_flush = SERVER_KNOBS->ROCKSDB_ATOMIC_FLUSH;
	if (SERVER_KNOBS->SHARDED_ROCKSDB_BACKGROUND_PARALLELISM > 0) {
		options.IncreaseParallelism(SERVER_KNOBS->SHARDED_ROCKSDB_BACKGROUND_PARALLELISM);
	}

	options.wal_recovery_mode = getWalRecoveryMode();
	options.max_open_files = SERVER_KNOBS->SHARDED_ROCKSDB_MAX_OPEN_FILES;
	options.delete_obsolete_files_period_micros = SERVER_KNOBS->ROCKSDB_DELETE_OBSOLETE_FILE_PERIOD * 1000000;
	options.max_total_wal_size = SERVER_KNOBS->ROCKSDB_MAX_TOTAL_WAL_SIZE;
	options.max_subcompactions = SERVER_KNOBS->SHARDED_ROCKSDB_MAX_SUBCOMPACTIONS;
	options.max_background_jobs = SERVER_KNOBS->SHARDED_ROCKSDB_MAX_BACKGROUND_JOBS;

	// The following two fields affect how archived logs will be deleted.
	// 1. If both set to 0, logs will be deleted asap and will not get into
	//    the archive.
	// 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
	//    WAL files will be checked every 10 min and if total size is greater
	//    then WAL_size_limit_MB, they will be deleted starting with the
	//    earliest until size_limit is met. All empty files will be deleted.
	// 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
	//    WAL files will be checked every WAL_ttl_seconds / 2 and those that
	//    are older than WAL_ttl_seconds will be deleted.
	// 4. If both are not 0, WAL files will be checked every 10 min and both
	//    checks will be performed with ttl being first.
	options.WAL_ttl_seconds = SERVER_KNOBS->ROCKSDB_WAL_TTL_SECONDS;
	options.WAL_size_limit_MB = SERVER_KNOBS->ROCKSDB_WAL_SIZE_LIMIT_MB;

	options.db_write_buffer_size = SERVER_KNOBS->SHARDED_ROCKSDB_TOTAL_WRITE_BUFFER_SIZE;
	options.statistics = rocksdb::CreateDBStatistics();
	options.statistics->set_stats_level(rocksdb::kExceptHistogramOrTimers);
	options.db_log_dir = g_network->isSimulated() ? "" : SERVER_KNOBS->LOG_DIRECTORY;
	if (SERVER_KNOBS->ROCKSDB_LOG_LEVEL_DEBUG) {
		options.info_log_level = rocksdb::InfoLogLevel::DEBUG_LEVEL;
	}
	options.max_log_file_size = SERVER_KNOBS->ROCKSDB_MAX_LOG_FILE_SIZE;
	options.keep_log_file_num = SERVER_KNOBS->ROCKSDB_KEEP_LOG_FILE_NUM;

	options.skip_stats_update_on_db_open = SERVER_KNOBS->ROCKSDB_SKIP_STATS_UPDATE_ON_OPEN;
	options.skip_checking_sst_file_sizes_on_db_open = SERVER_KNOBS->ROCKSDB_SKIP_FILE_SIZE_CHECK_ON_OPEN;
	options.max_manifest_file_size = SERVER_KNOBS->ROCKSDB_MAX_MANIFEST_FILE_SIZE;

	if (SERVER_KNOBS->ROCKSDB_FULLFILE_CHECKSUM) {
		// We want this sst level checksum for many scenarios, such as compaction, backup, and physicalshardmove
		// https://github.com/facebook/rocksdb/wiki/Full-File-Checksum-and-Checksum-Handoff
		options.file_checksum_gen_factory = rocksdb::GetFileChecksumGenCrc32cFactory();
	}
	return options;
}

// Set some useful defaults desired for all reads.
rocksdb::ReadOptions getReadOptions() {
	rocksdb::ReadOptions options;
	options.background_purge_on_iterator_cleanup = true;
	options.auto_prefix_mode = (SERVER_KNOBS->SHARDED_ROCKSDB_PREFIX_LEN > 0);
	options.async_io = SERVER_KNOBS->SHARDED_ROCKSDB_READ_ASYNC_IO;
	return options;
}

struct ReadIterator {
	std::unique_ptr<rocksdb::Iterator> iter;
	double creationTime;
	KeyRange keyRange;
	std::unique_ptr<rocksdb::Slice> beginSlice, endSlice;

	ReadIterator(rocksdb::ColumnFamilyHandle* cf, rocksdb::DB* db)
	  : creationTime(now()), iter(db->NewIterator(getReadOptions(), cf)) {}

	ReadIterator(rocksdb::ColumnFamilyHandle* cf, rocksdb::DB* db, const KeyRange& range)
	  : creationTime(now()), keyRange(range) {
		auto options = getReadOptions();
		beginSlice = std::unique_ptr<rocksdb::Slice>(new rocksdb::Slice(toSlice(keyRange.begin)));
		options.iterate_lower_bound = beginSlice.get();
		endSlice = std::unique_ptr<rocksdb::Slice>(new rocksdb::Slice(toSlice(keyRange.end)));
		options.iterate_upper_bound = endSlice.get();
		iter = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options, cf));
	}
};

// Stores iterators for all shards for future reuse. One iterator is stored per shard.
class IteratorPool {
public:
	IteratorPool() {}

	std::shared_ptr<ReadIterator> getIterator(const std::string& id) {
		std::unique_lock<std::mutex> lock(mu);
		auto it = pool.find(id);
		if (it == pool.end()) {
			++numNewIterators;
			return nullptr;
		} else {
			auto ret = it->second;
			pool.erase(it);
			++numReusedIters;
			return ret;
		}
	}

	void returnIterator(const std::string& id, std::shared_ptr<ReadIterator> iterator) {
		ASSERT(iterator != nullptr);
		std::unique_lock<std::mutex> lock(mu);
		auto it = pool.find(id);
		if (it != pool.end()) {
			// An iterator already exist in the pool, replace it any way.
			++numReplacedIters;
		}
		pool[id] = iterator;
	}

	void refresh() {
		std::unique_lock<std::mutex> lock(mu);
		auto poolSize = pool.size();
		auto it = pool.begin();
		auto currTime = now();
		int refreshedIterCount = 0;
		while (it != pool.end()) {
			if (currTime - it->second->creationTime > SERVER_KNOBS->ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME) {
				it = pool.erase(it);
				++refreshedIterCount;
			} else {
				++it;
			}
		}
		TraceEvent("RefreshIterators")
		    .suppressFor(5.0)
		    .detail("NumReplacedIterators", numReplacedIters)
		    .detail("NumReusedIterators", numReusedIters)
		    .detail("NumNewIterators", numNewIterators)
		    .detail("PoolSize", poolSize)
		    .detail("RefreshedIterators", refreshedIterCount);
		numReplacedIters = 0;
		numReusedIters = 0;
		numNewIterators = 0;
	}

	void clear() {
		std::unique_lock<std::mutex> lock(mu);
		pool.clear();
	}

	void update(const std::string& id) {
		std::unique_lock<std::mutex> lock(mu);
		auto it = pool.find(id);
		if (it != pool.end()) {
			it->second->iter->Refresh();
		}
	}

	void erase(const std::string& id) {
		std::unique_lock<std::mutex> lock(mu);
		pool.erase(id);
	}

private:
	std::mutex mu;
	std::unordered_map<std::string, std::shared_ptr<ReadIterator>> pool;
	uint64_t numReplacedIters = 0;
	uint64_t numReusedIters = 0;
	uint64_t numNewIterators = 0;
};

ACTOR Future<Void> flowLockLogger(const FlowLock* readLock, const FlowLock* fetchLock) {
	loop {
		wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
		TraceEvent e("ShardedRocksDBFlowLock");
		e.detail("ReadAvailable", readLock->available());
		e.detail("ReadActivePermits", readLock->activePermits());
		e.detail("ReadWaiters", readLock->waiters());
		e.detail("FetchAvailable", fetchLock->available());
		e.detail("FetchActivePermits", fetchLock->activePermits());
		e.detail("FetchWaiters", fetchLock->waiters());
	}
}

// DataShard represents a key range (logical shard) in FDB. A DataShard is assigned to a specific physical shard.
struct DataShard {
	DataShard(KeyRange range, PhysicalShard* physicalShard) : range(range), physicalShard(physicalShard) {}

	bool initialized() const;

	KeyRange range;
	PhysicalShard* physicalShard;
};

// PhysicalShard represent a collection of logical shards. A PhysicalShard could have one or more DataShards. A
// PhysicalShard is stored as a column family in rocksdb. Each PhysicalShard has its own iterator pool.
struct PhysicalShard {
	PhysicalShard(rocksdb::DB* db, std::string id, const rocksdb::ColumnFamilyOptions& options)
	  : db(db), id(id), cfOptions(options), isInitialized(false) {}
	PhysicalShard(rocksdb::DB* db, std::string id, rocksdb::ColumnFamilyHandle* handle)
	  : db(db), id(id), cf(handle), isInitialized(true) {
		ASSERT(cf);
	}

	rocksdb::Status init() {
		if (cf) {
			return rocksdb::Status::OK();
		}
		auto status = db->CreateColumnFamily(cfOptions, id, &cf);
		if (!status.ok()) {
			logRocksDBError(status, "AddCF");
			return status;
		}
		logShardEvent(id, ShardOp::OPEN);
		this->isInitialized.store(true);
		return status;
	}

	// Restore from the checkpoint.
	rocksdb::Status restore(const CheckpointMetaData& checkpoint) {
		const CheckpointFormat format = checkpoint.getFormat();
		rocksdb::Status status;
		if (format == DataMoveRocksCF) {
			rocksdb::ExportImportFilesMetaData metaData = getMetaData(checkpoint);
			if (metaData.files.empty()) {
				TraceEvent(SevInfo, "RocksDBRestoreEmptyShard")
				    .detail("ShardId", id)
				    .detail("CheckpointID", checkpoint.checkpointID);
				status = db->CreateColumnFamily(getCFOptions(), id, &cf);
			} else {
				TraceEvent(SevInfo, "RocksDBRestoreCF");
				rocksdb::ImportColumnFamilyOptions importOptions;
				importOptions.move_files = SERVER_KNOBS->ROCKSDB_IMPORT_MOVE_FILES;
				status = db->CreateColumnFamilyWithImport(getCFOptions(), id, importOptions, metaData, &cf);
				TraceEvent(SevInfo, "RocksDBRestoreCFEnd").detail("Status", status.ToString());
			}
		} else if (format == RocksDBKeyValues) {
			ASSERT(cf != nullptr);
			std::vector<std::string> sstFiles;
			const RocksDBCheckpointKeyValues rcp = getRocksKeyValuesCheckpoint(checkpoint);
			for (const auto& file : rcp.fetchedFiles) {
				if (file.path != emptySstFilePath) {
					sstFiles.push_back(file.path);
				}
			}

			TraceEvent(SevDebug, "RocksDBRestoreFiles")
			    .detail("Shard", id)
			    .detail("CheckpointID", checkpoint.checkpointID)
			    .detail("Files", describe(sstFiles));

			if (!sstFiles.empty()) {
				rocksdb::IngestExternalFileOptions ingestOptions;
				ingestOptions.move_files = SERVER_KNOBS->ROCKSDB_IMPORT_MOVE_FILES;
				ingestOptions.verify_checksums_before_ingest = SERVER_KNOBS->ROCKSDB_VERIFY_CHECKSUM_BEFORE_RESTORE;
				status = db->IngestExternalFile(cf, sstFiles, ingestOptions);
			} else {
				TraceEvent(SevWarn, "RocksDBServeRestoreEmptyRange")
				    .detail("ShardId", id)
				    .detail("RocksKeyValuesCheckpoint", rcp.toString())
				    .detail("Checkpoint", checkpoint.toString());
			}
			TraceEvent(SevInfo, "PhysicalShardRestoredFiles")
			    .detail("ShardId", id)
			    .detail("CFName", cf->GetName())
			    .detail("Checkpoint", checkpoint.toString())
			    .detail("RestoredFiles", describe(sstFiles));
		} else {
			throw not_implemented();
		}

		TraceEvent(status.ok() ? SevInfo : SevWarnAlways, "PhysicalShardRestoreEnd")
		    .detail("Status", status.ToString())
		    .detail("Shard", id)
		    .detail("CFName", cf == nullptr ? "" : cf->GetName())
		    .detail("Checkpoint", checkpoint.toString());
		if (status.ok()) {
			if (!this->isInitialized) {
				this->isInitialized.store(true);
			}
		}

		return status;
	}

	bool initialized() { return this->isInitialized.load(); }

	std::vector<KeyRange> getAllRanges() const {
		std::vector<KeyRange> res;
		for (const auto& [key, shard] : dataShards) {
			if (shard != nullptr) {
				res.push_back(shard->range);
			}
		}
		return res;
	}

	bool shouldFlush() {
		return SERVER_KNOBS->ROCKSDB_CF_RANGE_DELETION_LIMIT > 0 &&
		       numRangeDeletions > SERVER_KNOBS->ROCKSDB_CF_RANGE_DELETION_LIMIT;
	}

	std::string toString() {
		std::string ret = "[ID]: " + this->id + ", [CF]: ";
		if (initialized()) {
			ret += std::to_string(this->cf->GetID());
		} else {
			ret += "Not initialized";
		}
		return ret;
	}

	~PhysicalShard() {
		logShardEvent(id, ShardOp::CLOSE);
		isInitialized.store(false);

		// Deleting default column family is not allowed.
		if (deletePending && id != DEFAULT_CF_NAME) {
			auto s = db->DropColumnFamily(cf);
			if (!s.ok()) {
				logRocksDBError(s, "DestroyShard");
				logShardEvent(id, ShardOp::DESTROY, SevError, s.ToString());
				return;
			}
		}
		auto s = db->DestroyColumnFamilyHandle(cf);
		if (!s.ok()) {
			logRocksDBError(s, "DestroyCFHandle");
			logShardEvent(id, ShardOp::DESTROY, SevError, s.ToString());
			return;
		}
		logShardEvent(id, ShardOp::DESTROY);
	}

	rocksdb::DB* db;
	std::string id;
	rocksdb::ColumnFamilyOptions cfOptions;
	rocksdb::ColumnFamilyHandle* cf = nullptr;
	std::unordered_map<std::string, std::unique_ptr<DataShard>> dataShards;
	bool deletePending = false;
	std::atomic<bool> isInitialized;
	uint64_t numRangeDeletions = 0;
	double deleteTimeSec = 0.0;
	double lastCompactionTime = 0.0;
};

int readRangeInDb(PhysicalShard* shard,
                  const KeyRangeRef range,
                  int rowLimit,
                  int byteLimit,
                  RangeResult* result,
                  std::shared_ptr<IteratorPool> iteratorPool) {
	if (rowLimit == 0 || byteLimit == 0) {
		return 0;
	}

	int accumulatedBytes = 0;
	rocksdb::Status s;
	std::shared_ptr<ReadIterator> readIter = nullptr;

	bool reuseIterator = SERVER_KNOBS->SHARDED_ROCKSDB_REUSE_ITERATORS && iteratorPool != nullptr;
	if (g_network->isSimulated() &&
	    deterministicRandom()->random01() > SERVER_KNOBS->ROCKSDB_PROBABILITY_REUSE_ITERATOR_SIM) {
		// Reduce probability of reusing iterators in simulation.
		reuseIterator = false;
	}

	if (reuseIterator) {

		readIter = iteratorPool->getIterator(shard->id);
		if (readIter == nullptr) {
			readIter = std::make_shared<ReadIterator>(shard->cf, shard->db);
		}
	} else {
		readIter = std::make_shared<ReadIterator>(shard->cf, shard->db, range);
	}
	// When using a prefix extractor, ensure that keys are returned in order even if they cross
	// a prefix boundary.
	if (rowLimit >= 0) {
		auto* cursor = readIter->iter.get();
		cursor->Seek(toSlice(range.begin));
		while (cursor->Valid() && toStringRef(cursor->key()) < range.end) {
			KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
			accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
			result->push_back_deep(result->arena(), kv);
			// Calling `cursor->Next()` is potentially expensive, so short-circut here just in case.
			if (result->size() >= rowLimit || accumulatedBytes >= byteLimit) {
				break;
			}
			cursor->Next();
		}
		s = cursor->status();
	} else {
		auto* cursor = readIter->iter.get();
		cursor->SeekForPrev(toSlice(range.end));
		if (cursor->Valid() && toStringRef(cursor->key()) == range.end) {
			cursor->Prev();
		}
		while (cursor->Valid() && toStringRef(cursor->key()) >= range.begin) {
			KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
			accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
			result->push_back_deep(result->arena(), kv);
			// Calling `cursor->Prev()` is potentially expensive, so short-circuit here just in case.
			if (result->size() >= -rowLimit || accumulatedBytes >= byteLimit) {
				break;
			}
			cursor->Prev();
		}
		s = cursor->status();
	}

	if (!s.ok()) {
		logRocksDBError(s, "ReadRange");
		// The data written to the arena is not erased, which will leave RangeResult in a dirty state. The RangeResult
		// should never be returned to user.
		return -1;
	}
	if (reuseIterator) {
		iteratorPool->returnIterator(shard->id, readIter);
	}
	return accumulatedBytes;
}

struct Counters {
	CounterCollection cc;
	Counter immediateThrottle;
	Counter failedToAcquire;
	Counter convertedRangeDeletions;

	Counters()
	  : cc("RocksDBCounters"), immediateThrottle("ImmediateThrottle", cc), failedToAcquire("FailedToAcquire", cc),
	    convertedRangeDeletions("ConvertedRangeDeletions", cc) {}
};

// Manages physical shards and maintains logical shard mapping.
class ShardManager {
public:
	ShardManager(std::string path,
	             UID logId,
	             const rocksdb::DBOptions& options,
	             std::shared_ptr<RocksDBErrorListener> errorListener,
	             std::shared_ptr<RocksDBEventListener> eventListener,
	             Counters* cc,
	             std::shared_ptr<IteratorPool> iteratorPool)
	  : path(path), logId(logId), dbOptions(options), cfOptions(getCFOptions()), dataShardMap(nullptr, specialKeys.end),
	    counters(cc), iteratorPool(iteratorPool) {
		if (!g_network->isSimulated()) {
			// Generating trace events in non-FDB thread will cause errors. The event listener is tested with local FDB
			// cluster.
			dbOptions.listeners.push_back(errorListener);
			if (SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_WHEN_IO_TIMEOUT ||
			    SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_PROBABILITY > 0) {
				dbOptions.listeners.push_back(eventListener);
			}
		}
	}

	ACTOR static Future<Void> shardMetricsLogger(std::shared_ptr<ShardedRocksDBState> rState,
	                                             Future<Void> openFuture,
	                                             ShardManager* shardManager) {
		state std::unordered_map<std::string, std::shared_ptr<PhysicalShard>>* physicalShards =
		    shardManager->getAllShards();

		try {
			wait(openFuture);
			loop {
				wait(delay(SERVER_KNOBS->ROCKSDB_CF_METRICS_DELAY));
				if (rState->closing) {
					break;
				}

				uint64_t numSstFiles = 0;
				for (auto& [id, shard] : *physicalShards) {
					if (!shard->initialized()) {
						continue;
					}
					uint64_t liveDataSize = 0;
					ASSERT(shard->db->GetIntProperty(
					    shard->cf, rocksdb::DB::Properties::kEstimateLiveDataSize, &liveDataSize));

					TraceEvent e(SevInfo, "PhysicalShardStats");
					e.detail("ShardId", id).detail("LiveDataSize", liveDataSize);

					// Get compression ratio for each level.
					rocksdb::ColumnFamilyMetaData cfMetadata;
					shard->db->GetColumnFamilyMetaData(shard->cf, &cfMetadata);
					e.detail("NumFiles", cfMetadata.file_count);
					numSstFiles += cfMetadata.file_count;
					std::string levelProp;
					int numLevels = 0;
					for (auto it = cfMetadata.levels.begin(); it != cfMetadata.levels.end(); ++it) {
						std::string propValue = "";
						ASSERT(shard->db->GetProperty(shard->cf,
						                              rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix +
						                                  std::to_string(it->level),
						                              &propValue));
						e.detail("Level" + std::to_string(it->level),
						         std::to_string(it->size) + " " + propValue + " " + std::to_string(it->files.size()));
						if (it->size > 0) {
							++numLevels;
						}
					}
					e.detail("NumLevels", numLevels);
				}
				TraceEvent(SevInfo, "KVSPhysialShardMetrics")
				    .detail("NumActiveShards", shardManager->numActiveShards())
				    .detail("TotalPhysicalShards", shardManager->numPhysicalShards())
				    .detail("NumSstFiles", numSstFiles);
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "ShardedRocksShardMetricsLoggerError").errorUnsuppressed(e);
			}
		}
		return Void();
	}

	rocksdb::Status init() {
		const double start = now();
		// Open instance.
		TraceEvent(SevInfo, "ShardedRocksDBInitBegin", this->logId).detail("DataPath", path);
		if (SERVER_KNOBS->SHARDED_ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC > 0) {
			rocksdb::RateLimiter::Mode mode;
			switch (SERVER_KNOBS->SHARDED_ROCKSDB_RATE_LIMITER_MODE) {
			case 0:
				mode = rocksdb::RateLimiter::Mode::kReadsOnly;
				break;
			case 1:
				mode = rocksdb::RateLimiter::Mode::kWritesOnly;
				break;
			case 2:
				mode = rocksdb::RateLimiter::Mode::kAllIo;
				break;
			default:
				TraceEvent(SevWarnAlways, "IncorrectRateLimiterMode")
				    .detail("Mode", SERVER_KNOBS->SHARDED_ROCKSDB_RATE_LIMITER_MODE);
				mode = rocksdb::RateLimiter::Mode::kWritesOnly;
			}

			auto rateLimiter =
			    rocksdb::NewGenericRateLimiter(SERVER_KNOBS->SHARDED_ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC,
			                                   100 * 1000, // refill_period_us
			                                   10, // fairness
			                                   mode,
			                                   SERVER_KNOBS->ROCKSDB_WRITE_RATE_LIMITER_AUTO_TUNE);
			dbOptions.rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(rateLimiter);
		}
		std::vector<std::string> columnFamilies;
		rocksdb::Status status = rocksdb::DB::ListColumnFamilies(dbOptions, path, &columnFamilies);

		std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
		bool foundMetadata = false;
		for (const auto& name : columnFamilies) {
			if (name == METADATA_SHARD_ID) {
				foundMetadata = true;
			}
			descriptors.push_back(rocksdb::ColumnFamilyDescriptor(name, cfOptions));
		}

		// Add default column family if it's a newly opened database.
		if (descriptors.size() == 0) {
			descriptors.push_back(rocksdb::ColumnFamilyDescriptor("default", cfOptions));
		}

		std::vector<rocksdb::ColumnFamilyHandle*> handles;
		status = rocksdb::DB::Open(dbOptions, path, descriptors, &handles, &db);
		if (!status.ok()) {
			logRocksDBError(status, "Open");
			return status;
		}

		TraceEvent("ShardedRocksDBOpen").detail("Duraton", now() - start).detail("NumCFs", descriptors.size());

		if (foundMetadata) {
			TraceEvent(SevInfo, "ShardedRocksInitLoadPhysicalShards", this->logId)
			    .detail("PhysicalShardCount", handles.size());

			std::shared_ptr<PhysicalShard> metadataShard = nullptr;
			for (auto handle : handles) {
				auto shard = std::make_shared<PhysicalShard>(db, handle->GetName(), handle);
				if (shard->id == METADATA_SHARD_ID) {
					metadataShard = shard;
				}
				physicalShards[shard->id] = shard;
				columnFamilyMap[handle->GetID()] = handle;
				TraceEvent(SevVerbose, "ShardedRocksInitPhysicalShard", this->logId).detail("ShardId", shard->id);
			}

			std::set<std::string> unusedShards(columnFamilies.begin(), columnFamilies.end());
			unusedShards.erase(METADATA_SHARD_ID);
			unusedShards.erase(DEFAULT_CF_NAME);

			KeyRange keyRange = prefixRange(shardMappingPrefix);
			while (true) {
				RangeResult metadata;
				const int bytes = readRangeInDb(metadataShard.get(),
				                                keyRange,
				                                std::max(2, SERVER_KNOBS->ROCKSDB_READ_RANGE_ROW_LIMIT),
				                                SERVER_KNOBS->SHARD_METADATA_SCAN_BYTES_LIMIT,
				                                &metadata,
				                                iteratorPool);
				if (bytes <= 0) {
					break;
				}

				ASSERT_GT(metadata.size(), 0);
				for (int i = 0; i < metadata.size() - 1; ++i) {
					const std::string name = metadata[i].value.toString();
					KeyRangeRef range(metadata[i].key.removePrefix(shardMappingPrefix),
					                  metadata[i + 1].key.removePrefix(shardMappingPrefix));
					TraceEvent(SevVerbose, "DecodeShardMapping", this->logId)
					    .detail("Range", range)
					    .detail("ShardId", name);

					// Empty name indicates the shard doesn't belong to the SS/KVS.
					if (name.empty()) {
						continue;
					}

					auto it = physicalShards.find(name);
					// Raise error if physical shard is missing.
					if (it == physicalShards.end()) {
						TraceEvent(SevError, "ShardedRocksDB", this->logId).detail("MissingShard", name);
						return rocksdb::Status::NotFound();
					}

					std::unique_ptr<DataShard> dataShard = std::make_unique<DataShard>(range, it->second.get());
					dataShardMap.insert(range, dataShard.get());
					it->second->dataShards[range.begin.toString()] = std::move(dataShard);
					activePhysicalShardIds.emplace(name);
					unusedShards.erase(name);
				}

				if (metadata.back().key.removePrefix(shardMappingPrefix) == specialKeys.end) {
					TraceEvent(SevVerbose, "ShardedRocksLoadShardMappingEnd", this->logId);
					break;
				}

				// Read from the current last key since the shard beginning with it hasn't been processed.
				if (metadata.size() == 1 && metadata.back().value.toString().empty()) {
					// Should not happen, just being paranoid.
					keyRange = KeyRangeRef(keyAfter(metadata.back().key), keyRange.end);
				} else {
					keyRange = KeyRangeRef(metadata.back().key, keyRange.end);
				}
			}

			for (const auto& name : unusedShards) {
				auto it = physicalShards.find(name);
				ASSERT(it != physicalShards.end());
				auto shard = it->second;
				if (shard->dataShards.size() == 0) {
					shard->deleteTimeSec = now();
					pendingDeletionShards.push_back(name);
					TraceEvent(SevInfo, "UnusedPhysicalShard", logId).detail("ShardId", name);
				}
			}
			if (unusedShards.size() > 0) {
				TraceEvent("ShardedRocksDB", logId).detail("CleanUpUnusedShards", unusedShards.size());
			}
		} else {
			// DB is opened with default shard.
			ASSERT(handles.size() == 1);
			// Clear the entire key space.
			rocksdb::WriteOptions options;
			db->DeleteRange(options, handles[0], toSlice(allKeys.begin), toSlice(specialKeys.end));

			// Add SpecialKeys range. This range should not be modified.
			std::shared_ptr<PhysicalShard> defaultShard =
			    std::make_shared<PhysicalShard>(db, DEFAULT_CF_NAME, handles[0]);
			columnFamilyMap[defaultShard->cf->GetID()] = defaultShard->cf;
			std::unique_ptr<DataShard> dataShard = std::make_unique<DataShard>(specialKeys, defaultShard.get());
			dataShardMap.insert(specialKeys, dataShard.get());
			defaultShard->dataShards[specialKeys.begin.toString()] = std::move(dataShard);
			physicalShards[defaultShard->id] = defaultShard;

			// Create metadata shard.
			auto metadataShard = std::make_shared<PhysicalShard>(db, METADATA_SHARD_ID, cfOptions);
			metadataShard->init();
			columnFamilyMap[metadataShard->cf->GetID()] = metadataShard->cf;
			physicalShards[METADATA_SHARD_ID] = metadataShard;

			// Write special key range metadata.
			writeBatch = std::make_unique<rocksdb::WriteBatch>(
			    0, // reserved_bytes default:0
			    0, // max_bytes default:0
			    SERVER_KNOBS->ROCKSDB_WRITEBATCH_PROTECTION_BYTES_PER_KEY, // protection_bytes_per_key
			    0 /* default_cf_ts_sz default:0 */);
			dirtyShards = std::make_unique<std::set<PhysicalShard*>>();
			persistRangeMapping(specialKeys, true);
			status = db->Write(options, writeBatch.get());
			if (!status.ok()) {
				return status;
			}
			TraceEvent(SevInfo, "ShardedRocksInitializeMetaDataShard", this->logId)
			    .detail("MetadataShardCF", metadataShard->cf->GetID());
		}

		writeBatch = std::make_unique<rocksdb::WriteBatch>(
		    0, // reserved_bytes default:0
		    0, // max_bytes default:0
		    SERVER_KNOBS->ROCKSDB_WRITEBATCH_PROTECTION_BYTES_PER_KEY, // protection_bytes_per_key
		    0 /* default_cf_ts_sz default:0 */);
		dirtyShards = std::make_unique<std::set<PhysicalShard*>>();
		iteratorPool->update(getMetaDataShard()->id);

		TraceEvent(SevInfo, "ShardedRocksDBInitEnd", this->logId)
		    .detail("DataPath", path)
		    .detail("Duration", now() - start);
		return status;
	}

	DataShard* getDataShard(KeyRef key) {
		DataShard* shard = dataShardMap[key];
		ASSERT(shard == nullptr || shard->range.contains(key));
		return shard;
	}

	PhysicalShard* getSpecialKeysShard() {
		auto it = physicalShards.find(DEFAULT_CF_NAME);
		ASSERT(it != physicalShards.end());
		return it->second.get();
	}

	PhysicalShard* getMetaDataShard() {
		auto it = physicalShards.find(METADATA_SHARD_ID);
		ASSERT(it != physicalShards.end());
		return it->second.get();
	}

	// Returns the physical shard that hosting all `ranges`; Returns nullptr if such a physical shard does not exists.
	PhysicalShard* getPhysicalShardForAllRanges(std::vector<KeyRange> ranges) {
		PhysicalShard* result = nullptr;
		for (const auto& range : ranges) {
			auto rangeIterator = this->dataShardMap.intersectingRanges(range);
			for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
				if (it.value() == nullptr) {
					TraceEvent(SevWarn, "ShardedRocksDBRangeNotInKVS", logId).detail("Range", range);
					return nullptr;
				} else {
					ASSERT(it.value()->physicalShard != nullptr);
					PhysicalShard* ps = it.value()->physicalShard;
					if (result == nullptr) {
						result = ps;
					} else if (ps != result) {
						TraceEvent(SevWarn, "ShardedRocksDBRangeOnMultipleShards", logId)
						    .detail("Ranges", describe(ranges))
						    .detail("PhysicalShard", result->id)
						    .detail("SecondPhysicalShard", ps->id);
						return nullptr;
					}
				}
			}
		}

		return result;
	}

	std::vector<DataShard*> getDataShardsByRange(KeyRangeRef range) {
		std::vector<DataShard*> result;
		auto rangeIterator = dataShardMap.intersectingRanges(range);

		for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
			if (it.value() == nullptr) {
				TraceEvent(SevVerbose, "ShardedRocksDB")
				    .detail("Info", "ShardNotFound")
				    .detail("BeginKey", range.begin)
				    .detail("EndKey", range.end);
				continue;
			}
			result.push_back(it.value());
		}
		return result;
	}

	PhysicalShard* addRange(KeyRange range, std::string id, bool active) {
		TraceEvent(SevVerbose, "ShardedRocksAddRangeBegin", this->logId).detail("Range", range).detail("ShardId", id);

		// Newly added range should not overlap with any existing range.
		auto ranges = dataShardMap.intersectingRanges(range);

		for (auto it = ranges.begin(); it != ranges.end(); ++it) {
			if (it.value()) {
				if (it.value()->physicalShard->id == id) {
					TraceEvent(SevError, "ShardedRocksDBAddRange")
					    .detail("ErrorType", "RangeAlreadyExist")
					    .detail("IntersectingRange", it->range())
					    .detail("DataShardRange", it->value()->range)
					    .detail("ExpectedShardId", id)
					    .detail("PhysicalShardID", it->value()->physicalShard->toString());
				} else {
					TraceEvent(SevError, "ShardedRocksDBAddRange")
					    .detail("ErrorType", "ConflictingRange")
					    .detail("IntersectingRange", it->range())
					    .detail("DataShardRange", it->value()->range)
					    .detail("ExpectedShardId", id)
					    .detail("PhysicalShardID", it->value()->physicalShard->toString());
				}
				return nullptr;
			}
		}

		auto cfOptions = active ? getCFOptions() : getCFOptionsForInactiveShard();
		auto [it, inserted] = physicalShards.emplace(id, std::make_shared<PhysicalShard>(db, id, cfOptions));
		std::shared_ptr<PhysicalShard>& shard = it->second;

		activePhysicalShardIds.emplace(id);

		auto dataShard = std::make_unique<DataShard>(range, shard.get());
		dataShardMap.insert(range, dataShard.get());
		shard->dataShards[range.begin.toString()] = std::move(dataShard);

		validate();

		TraceEvent(SevInfo, "ShardedRocksDBRangeAdded", this->logId)
		    .detail("Range", range)
		    .detail("ShardId", id)
		    .detail("Active", active);

		return shard.get();
	}

	std::vector<std::string> removeRange(KeyRange range) {
		TraceEvent(SevVerbose, "ShardedRocksRemoveRangeBegin", this->logId).detail("Range", range);
		std::vector<std::string> shardIds;

		std::vector<DataShard*> newShards;
		auto ranges = dataShardMap.intersectingRanges(range);

		for (auto it = ranges.begin(); it != ranges.end(); ++it) {
			if (!it.value()) {
				TraceEvent(SevDebug, "ShardedRocksDB")
				    .detail("Info", "RemoveNonExistentRange")
				    .detail("BeginKey", range.begin)
				    .detail("EndKey", range.end);
				continue;
			}
			auto existingShard = it.value()->physicalShard;
			auto shardRange = it.range();

			if (SERVER_KNOBS->ROCKSDB_EMPTY_RANGE_CHECK && existingShard->initialized()) {
				// Enable consistency validation.
				RangeResult rangeResult;
				auto bytesRead = readRangeInDb(existingShard, range, 1, UINT16_MAX, &rangeResult, iteratorPool);
				if (bytesRead > 0) {
					TraceEvent(SevError, "ShardedRocksDBRangeNotEmpty")
					    .detail("ShardId", existingShard->toString())
					    .detail("Range", range)
					    .detail("DataShardRange", shardRange);
					// Force clear range.
					writeBatch->DeleteRange(it.value()->physicalShard->cf, toSlice(range.begin), toSlice(range.end));
					dirtyShards->insert(it.value()->physicalShard);
				}
			}
			ASSERT(it.value()->range == shardRange); // Ranges should be consistent.

			if (range.contains(shardRange)) {
				existingShard->dataShards.erase(shardRange.begin.toString());
				TraceEvent(SevInfo, "ShardedRocksRemovedRange")
				    .detail("Range", range)
				    .detail("RemovedRange", shardRange)
				    .detail("ShardId", existingShard->toString());
				if (existingShard->dataShards.size() == 0) {
					TraceEvent(SevInfo, "ShardedRocksDBEmptyShard").detail("ShardId", existingShard->id);
					shardIds.push_back(existingShard->id);
					existingShard->deleteTimeSec = now();
					pendingDeletionShards.push_back(existingShard->id);
					activePhysicalShardIds.erase(existingShard->id);
				}
				continue;
			}

			// Range modification could result in more than one segments. Remove the original segment key here.
			existingShard->dataShards.erase(shardRange.begin.toString());
			if (shardRange.begin < range.begin) {
				auto dataShard =
				    std::make_unique<DataShard>(KeyRange(KeyRangeRef(shardRange.begin, range.begin)), existingShard);

				newShards.push_back(dataShard.get());
				const std::string msg = "Shrink shard from " + Traceable<KeyRangeRef>::toString(shardRange) + " to " +
				                        Traceable<KeyRangeRef>::toString(dataShard->range);

				existingShard->dataShards[shardRange.begin.toString()] = std::move(dataShard);
				logShardEvent(existingShard->id, shardRange, ShardOp::MODIFY_RANGE, SevInfo, msg);
			}

			if (shardRange.end > range.end) {
				auto dataShard =
				    std::make_unique<DataShard>(KeyRange(KeyRangeRef(range.end, shardRange.end)), existingShard);

				newShards.push_back(dataShard.get());
				const std::string msg = "Shrink shard from " + Traceable<KeyRangeRef>::toString(shardRange) + " to " +
				                        Traceable<KeyRangeRef>::toString(dataShard->range);

				existingShard->dataShards[range.end.toString()] = std::move(dataShard);
				logShardEvent(existingShard->id, shardRange, ShardOp::MODIFY_RANGE, SevInfo, msg);
			}
		}

		dataShardMap.insert(range, nullptr);
		for (DataShard* shard : newShards) {
			dataShardMap.insert(shard->range, shard);
		}

		validate();

		TraceEvent(SevInfo, "ShardedRocksRemoveRangeEnd", this->logId).detail("Range", range);

		return shardIds;
	}

	void markRangeAsActive(KeyRangeRef range) {
		auto ranges = dataShardMap.intersectingRanges(range);

		for (auto it = ranges.begin(); it != ranges.end(); ++it) {
			if (!it.value()) {
				continue;
			}

			auto beginSlice = toSlice(range.begin);
			auto endSlice = toSlice(range.end);
			db->SuggestCompactRange(it.value()->physicalShard->cf, &beginSlice, &endSlice);

			std::unordered_map<std::string, std::string> options = {
				{ "level0_file_num_compaction_trigger",
				  std::to_string(SERVER_KNOBS->SHARDED_ROCKSDB_LEVEL0_FILENUM_COMPACTION_TRIGGER) },
				{ "level0_slowdown_writes_trigger",
				  std::to_string(SERVER_KNOBS->SHARDED_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER) },
				{ "level0_stop_writes_trigger",
				  std::to_string(SERVER_KNOBS->SHARDED_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER) },
				{ "soft_pending_compaction_bytes_limit",
				  std::to_string(SERVER_KNOBS->SHARD_SOFT_PENDING_COMPACT_BYTES_LIMIT) },
				{ "hard_pending_compaction_bytes_limit",
				  std::to_string(SERVER_KNOBS->SHARD_HARD_PENDING_COMPACT_BYTES_LIMIT) },
				{ "disable_auto_compactions", "false" },
				{ "num_levels", "-1" }
			};
			db->SetOptions(it.value()->physicalShard->cf, options);
			TraceEvent("ShardedRocksDBRangeActive", logId).detail("ShardId", it.value()->physicalShard->id);
		}
	}

	std::vector<std::shared_ptr<PhysicalShard>> getPendingDeletionShards(double cleanUpDelay) {
		std::vector<std::shared_ptr<PhysicalShard>> emptyShards;
		double currentTime = now();

		TraceEvent(SevInfo, "ShardedRocksDB", logId)
		    .detail("PendingDeletionShardQueueSize", pendingDeletionShards.size());
		while (!pendingDeletionShards.empty()) {
			const auto id = pendingDeletionShards.front();
			auto it = physicalShards.find(id);
			if (it == physicalShards.end() || it->second->dataShards.size() != 0) {
				pendingDeletionShards.pop_front();
				continue;
			}
			if (currentTime - it->second->deleteTimeSec > cleanUpDelay) {
				pendingDeletionShards.pop_front();
				emptyShards.push_back(it->second);
				physicalShards.erase(id);
			} else {
				break;
			}
		}
		return emptyShards;
	}

	void put(KeyRef key, ValueRef value) {
		auto it = dataShardMap.rangeContaining(key);
		if (!it.value()) {
			TraceEvent(SevError, "ShardedRocksDB").detail("Error", "write to non-exist shard").detail("WriteKey", key);
			return;
		}
		TraceEvent(SevVerbose, "ShardedRocksShardManagerPut", this->logId)
		    .detail("WriteKey", key)
		    .detail("Value", value)
		    .detail("MapRange", it.range())
		    .detail("ShardRange", it.value()->range);
		ASSERT(it.value()->range == (KeyRangeRef)it.range());
		ASSERT(writeBatch != nullptr);
		ASSERT(dirtyShards != nullptr);
		writeBatch->Put(it.value()->physicalShard->cf, toSlice(key), toSlice(value));
		dirtyShards->insert(it.value()->physicalShard);
		TraceEvent(SevVerbose, "ShardedRocksShardManagerPutEnd", this->logId)
		    .detail("WriteKey", key)
		    .detail("Value", value);
	}

	void clear(KeyRef key) {
		auto it = dataShardMap.rangeContaining(key);
		if (!it.value()) {
			return;
		}
		writeBatch->Delete(it.value()->physicalShard->cf, toSlice(key));
		dirtyShards->insert(it.value()->physicalShard);
	}

	void clearRange(KeyRangeRef range, std::set<Key>* keysSet) {
		auto rangeIterator = dataShardMap.intersectingRanges(range);

		for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
			if (it.value() == nullptr) {
				TraceEvent(SevDebug, "ShardedRocksDB").detail("ClearNonExistentRange", it.range());
				continue;
			}

			auto physicalShard = it.value()->physicalShard;

			// TODO: Disable this once RocksDB is upgraded to a version with range delete improvement.
			if (SERVER_KNOBS->ROCKSDB_USE_POINT_DELETE_FOR_SYSTEM_KEYS && systemKeys.contains(range)) {
				auto scanRange = it.range() & range;
				auto beginSlice = toSlice(scanRange.begin);
				auto endSlice = toSlice(scanRange.end);

				rocksdb::ReadOptions options = getReadOptions();
				options.iterate_lower_bound = &beginSlice;
				options.iterate_upper_bound = &endSlice;
				auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options, physicalShard->cf));
				cursor->Seek(beginSlice);
				while (cursor->Valid() && toStringRef(cursor->key()) < toStringRef(endSlice)) {
					writeBatch->Delete(physicalShard->cf, cursor->key());
					cursor->Next();
				}
				if (!cursor->status().ok()) {
					// if readrange iteration fails, then do a deleteRange.
					writeBatch->DeleteRange(physicalShard->cf, beginSlice, endSlice);
				} else {

					auto key = keysSet->lower_bound(scanRange.begin);
					while (key != keysSet->end() && *key < scanRange.end) {
						writeBatch->Delete(physicalShard->cf, toSlice(*key));
						++key;
					}
				}
				++counters->convertedRangeDeletions;
			} else {
				writeBatch->DeleteRange(physicalShard->cf, toSlice(range.begin), toSlice(range.end));
				++physicalShard->numRangeDeletions;
			}

			// TODO: Skip clear range and compaction when entire CF is cleared.
			dirtyShards->insert(it.value()->physicalShard);
		}
	}

	void populateRangeMappingMutations(rocksdb::WriteBatch* writeBatch, KeyRangeRef range, bool isAdd) {
		TraceEvent(SevDebug, "ShardedRocksDB", this->logId)
		    .detail("Info", "RangeToPersist")
		    .detail("BeginKey", range.begin)
		    .detail("EndKey", range.end);
		PhysicalShard* metadataShard = getMetaDataShard();

		writeBatch->DeleteRange(metadataShard->cf,
		                        getShardMappingKey(range.begin, shardMappingPrefix),
		                        getShardMappingKey(range.end, shardMappingPrefix));

		KeyRef lastKey = range.end;
		if (isAdd) {
			auto ranges = dataShardMap.intersectingRanges(range);
			for (auto it = ranges.begin(); it != ranges.end(); ++it) {
				if (it.value()) {
					ASSERT(it.range() == it.value()->range);
					// Non-empty range.
					writeBatch->Put(metadataShard->cf,
					                getShardMappingKey(it.range().begin, shardMappingPrefix),
					                it.value()->physicalShard->id);
					TraceEvent(SevDebug, "ShardedRocksDB", this->logId)
					    .detail("Action", "PersistRangeMapping")
					    .detail("BeginKey", it.range().begin)
					    .detail("EndKey", it.range().end)
					    .detail("ShardId", it.value()->physicalShard->id);

				} else {
					// Empty range.
					writeBatch->Put(metadataShard->cf, getShardMappingKey(it.range().begin, shardMappingPrefix), "");
					TraceEvent(SevDebug, "ShardedRocksDB", this->logId)
					    .detail("Action", "PersistRangeMapping")
					    .detail("BeginKey", it.range().begin)
					    .detail("EndKey", it.range().end)
					    .detail("ShardId", "None");
				}
				lastKey = it.range().end;
			}
		} else {
			writeBatch->Put(metadataShard->cf, getShardMappingKey(range.begin, shardMappingPrefix), "");
			TraceEvent(SevDebug, "ShardedRocksDB", this->logId)
			    .detail("Action", "PersistRangeMapping")
			    .detail("RemoveRange", "True")
			    .detail("BeginKey", range.begin)
			    .detail("EndKey", range.end);
		}

		DataShard* nextShard = nullptr;
		if (lastKey <= allKeys.end) {
			nextShard = dataShardMap.rangeContaining(lastKey).value();
		}
		writeBatch->Put(metadataShard->cf,
		                getShardMappingKey(lastKey, shardMappingPrefix),
		                nextShard == nullptr ? "" : nextShard->physicalShard->id);
		TraceEvent(SevDebug, "ShardedRocksDB", this->logId)
		    .detail("Action", "PersistRangeMappingEnd")
		    .detail("NextShardKey", lastKey)
		    .detail("Value", nextShard == nullptr ? "" : nextShard->physicalShard->id);
	}

	void persistRangeMapping(KeyRangeRef range, bool isAdd) {
		populateRangeMappingMutations(writeBatch.get(), range, isAdd);
		dirtyShards->insert(getMetaDataShard());
	}

	std::unique_ptr<rocksdb::WriteBatch> getWriteBatch() {
		std::unique_ptr<rocksdb::WriteBatch> existingWriteBatch = std::move(writeBatch);
		writeBatch = std::make_unique<rocksdb::WriteBatch>(
		    0, // reserved_bytes default:0
		    0, // max_bytes default:0
		    SERVER_KNOBS->ROCKSDB_WRITEBATCH_PROTECTION_BYTES_PER_KEY, // protection_bytes_per_key
		    0 /* default_cf_ts_sz default:0 */);
		return existingWriteBatch;
	}

	std::unique_ptr<std::set<PhysicalShard*>> getDirtyShards() {
		std::unique_ptr<std::set<PhysicalShard*>> existingShards = std::move(dirtyShards);
		dirtyShards = std::make_unique<std::set<PhysicalShard*>>();
		return existingShards;
	}

	void flushShard(std::string shardId) {
		auto it = physicalShards.find(shardId);
		if (it == physicalShards.end()) {
			return;
		}
		rocksdb::FlushOptions fOptions;
		fOptions.wait = SERVER_KNOBS->ROCKSDB_WAIT_ON_CF_FLUSH;
		fOptions.allow_write_stall = SERVER_KNOBS->SHARDED_ROCKSDB_ALLOW_WRITE_STALL_ON_FLUSH;

		db->Flush(fOptions, it->second->cf);
	}

	void closeAllShards() {
		columnFamilyMap.clear();
		physicalShards.clear();
		// Close DB.
		auto s = db->Close();
		if (!s.ok()) {
			logRocksDBError(s, "Close");
			return;
		}
		TraceEvent("ShardedRocksDB", this->logId).detail("Info", "DBClosed");
	}

	void destroyAllShards() {
		auto metadataShard = getMetaDataShard();
		KeyRange metadataRange = prefixRange(shardMappingPrefix);
		rocksdb::WriteOptions options;
		db->DeleteRange(options, physicalShards["default"]->cf, toSlice(allKeys.begin), toSlice(specialKeys.end));
		db->DeleteRange(options, metadataShard->cf, toSlice(metadataRange.begin), toSlice(metadataRange.end));

		columnFamilyMap.clear();
		physicalShards.clear();
		// Close DB.
		auto s = db->Close();
		if (!s.ok()) {
			logRocksDBError(s, "Close");
			return;
		}
		s = rocksdb::DestroyDB(path, rocksdb::Options(dbOptions, getCFOptions()));
		if (!s.ok()) {
			logRocksDBError(s, "DestroyDB");
		}
		TraceEvent("ShardedRocksDB", this->logId).detail("Info", "DBDestroyed");
	}

	rocksdb::DB* getDb() const { return db; }

	std::unordered_map<std::string, std::shared_ptr<PhysicalShard>>* getAllShards() { return &physicalShards; }

	std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* getColumnFamilyMap() { return &columnFamilyMap; }

	std::vector<rocksdb::ColumnFamilyHandle*> getColumnFamilies() {
		std::vector<rocksdb::ColumnFamilyHandle*> res;
		for (auto& [id, cf] : columnFamilyMap) {
			res.push_back(cf);
		}
		return res;
	}

	size_t numPhysicalShards() const { return physicalShards.size(); };

	size_t numActiveShards() const { return activePhysicalShardIds.size(); };

	std::vector<std::pair<KeyRange, std::string>> getDataMapping() {
		std::vector<std::pair<KeyRange, std::string>> dataMap;
		for (auto it : dataShardMap.ranges()) {
			if (!it.value()) {
				continue;
			}
			dataMap.push_back(std::make_pair(it.range(), it.value()->physicalShard->id));
		}
		return dataMap;
	}

	CoalescedKeyRangeMap<std::string> getExistingRanges() {
		CoalescedKeyRangeMap<std::string> existingRanges;
		existingRanges.insert(allKeys, "");
		for (auto it : dataShardMap.intersectingRanges(allKeys)) {
			if (!it.value()) {
				continue;
			}

			if (it.value()->physicalShard->id == "kvs-metadata") {
				continue;
			}

			existingRanges.insert(it.range(), it.value()->physicalShard->id);
		}
		return existingRanges;
	}

	void validate() {
		if (SERVER_KNOBS->SHARDED_ROCKSDB_VALIDATE_MAPPING_RATIO <= 0 ||
		    deterministicRandom()->random01() > SERVER_KNOBS->SHARDED_ROCKSDB_VALIDATE_MAPPING_RATIO) {
			return;
		}

		TraceEvent(SevVerbose, "ShardedRocksValidateShardManager", this->logId);
		for (auto s = dataShardMap.ranges().begin(); s != dataShardMap.ranges().end(); ++s) {
			TraceEvent e(SevVerbose, "ShardedRocksValidateDataShardMap", this->logId);
			e.detail("Range", s->range());
			const DataShard* shard = s->value();
			e.detail("ShardAddress", reinterpret_cast<std::uintptr_t>(shard));
			if (shard != nullptr) {
				e.detail("PhysicalShard", shard->physicalShard->id);
			} else {
				e.detail("Shard", "Empty");
			}
			if (shard != nullptr) {
				if (shard->range != static_cast<KeyRangeRef>(s->range())) {
					TraceEvent(SevWarnAlways, "ShardRangeMismatch").detail("Range", s->range());
				}

				ASSERT(shard->range == static_cast<KeyRangeRef>(s->range()));
				ASSERT(shard->physicalShard != nullptr);
				auto it = shard->physicalShard->dataShards.find(shard->range.begin.toString());
				ASSERT(it != shard->physicalShard->dataShards.end());
				ASSERT(it->second.get() == shard);
			}
		}
	}

private:
	const std::string path;
	const UID logId;
	rocksdb::DBOptions dbOptions;
	rocksdb::ColumnFamilyOptions cfOptions;
	rocksdb::DB* db = nullptr;
	std::unordered_map<std::string, std::shared_ptr<PhysicalShard>> physicalShards;
	std::unordered_set<std::string> activePhysicalShardIds;
	// Stores mapping between cf id and cf handle, used during compaction.
	std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*> columnFamilyMap;
	std::unique_ptr<rocksdb::WriteBatch> writeBatch;
	std::unique_ptr<std::set<PhysicalShard*>> dirtyShards;
	KeyRangeMap<DataShard*> dataShardMap;
	std::deque<std::string> pendingDeletionShards;
	Counters* counters;
	std::shared_ptr<IteratorPool> iteratorPool;
};

class RocksDBMetrics {
public:
	RocksDBMetrics(UID debugID, std::shared_ptr<rocksdb::Statistics> stats);
	void logStats(rocksdb::DB* db, std::string manifestDirectory);
	// PerfContext
	void resetPerfContext();
	void collectPerfContext(int index);
	void logPerfContext(bool ignoreZeroMetric);
	// For Readers
	Reference<Histogram> getReadRangeLatencyHistogram(int index);
	Reference<Histogram> getReadValueLatencyHistogram(int index);
	Reference<Histogram> getReadPrefixLatencyHistogram(int index);
	Reference<Histogram> getReadRangeActionHistogram(int index);
	Reference<Histogram> getReadValueActionHistogram(int index);
	Reference<Histogram> getReadPrefixActionHistogram(int index);
	Reference<Histogram> getReadRangeQueueWaitHistogram(int index);
	Reference<Histogram> getReadValueQueueWaitHistogram(int index);
	Reference<Histogram> getReadPrefixQueueWaitHistogram(int index);
	Reference<Histogram> getReadRangeNewIteratorHistogram(int index);
	Reference<Histogram> getReadValueGetHistogram(int index);
	Reference<Histogram> getReadPrefixGetHistogram(int index);
	// For Writer
	Reference<Histogram> getCommitLatencyHistogram();
	Reference<Histogram> getCommitActionHistogram();
	Reference<Histogram> getCommitQueueWaitHistogram();
	Reference<Histogram> getWriteHistogram();
	Reference<Histogram> getDeleteCompactRangeHistogram();
	std::vector<std::pair<std::string, int64_t>> getManifestBytes(std::string manifestDirectory);

private:
	const UID debugID;
	// Global Statistic Input to RocksDB DB instance
	std::shared_ptr<rocksdb::Statistics> stats;
	// Statistic Output from RocksDB
	std::vector<std::tuple<const char*, uint32_t, uint64_t>> tickerStats;
	std::vector<std::pair<const char*, std::string>> intPropertyStats;
	// Iterator Pool Stats
	std::unordered_map<std::string, uint64_t> readIteratorPoolStats;
	// PerfContext
	std::vector<std::tuple<const char*, int, std::vector<uint64_t>>> perfContextMetrics;
	// Readers Histogram
	std::vector<Reference<Histogram>> readRangeLatencyHistograms;
	std::vector<Reference<Histogram>> readValueLatencyHistograms;
	std::vector<Reference<Histogram>> readPrefixLatencyHistograms;
	std::vector<Reference<Histogram>> readRangeActionHistograms;
	std::vector<Reference<Histogram>> readValueActionHistograms;
	std::vector<Reference<Histogram>> readPrefixActionHistograms;
	std::vector<Reference<Histogram>> readRangeQueueWaitHistograms;
	std::vector<Reference<Histogram>> readValueQueueWaitHistograms;
	std::vector<Reference<Histogram>> readPrefixQueueWaitHistograms;
	std::vector<Reference<Histogram>> readRangeNewIteratorHistograms;
	std::vector<Reference<Histogram>> readValueGetHistograms;
	std::vector<Reference<Histogram>> readPrefixGetHistograms;
	// Writer Histogram
	Reference<Histogram> commitLatencyHistogram;
	Reference<Histogram> commitActionHistogram;
	Reference<Histogram> commitQueueWaitHistogram;
	Reference<Histogram> writeHistogram;
	Reference<Histogram> deleteCompactRangeHistogram;

	uint64_t getRocksdbPerfcontextMetric(int metric);
};

std::vector<std::pair<std::string, int64_t>> RocksDBMetrics::getManifestBytes(std::string manifestDirectory) {
	std::vector<std::pair<std::string, int64_t>> res;
	std::vector<std::string> returnFiles = platform::listFiles(manifestDirectory, "");
	for (const auto& fileEntry : returnFiles) {
		if (fileEntry.find(manifestFilePrefix) != std::string::npos) {
			int64_t manifestSize = fileSize(manifestDirectory + "/" + fileEntry);
			res.push_back(std::make_pair(fileEntry, manifestSize));
		}
	}
	return res;
}

// We have 4 readers and 1 writer. Following input index denotes the
// id assigned to the reader thread when creating it
Reference<Histogram> RocksDBMetrics::getReadRangeLatencyHistogram(int index) {
	return readRangeLatencyHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadValueLatencyHistogram(int index) {
	return readValueLatencyHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadPrefixLatencyHistogram(int index) {
	return readPrefixLatencyHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadRangeActionHistogram(int index) {
	return readRangeActionHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadValueActionHistogram(int index) {
	return readValueActionHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadPrefixActionHistogram(int index) {
	return readPrefixActionHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadRangeQueueWaitHistogram(int index) {
	return readRangeQueueWaitHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadValueQueueWaitHistogram(int index) {
	return readValueQueueWaitHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadPrefixQueueWaitHistogram(int index) {
	return readPrefixQueueWaitHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadRangeNewIteratorHistogram(int index) {
	return readRangeNewIteratorHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadValueGetHistogram(int index) {
	return readValueGetHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getReadPrefixGetHistogram(int index) {
	return readPrefixGetHistograms[index];
}
Reference<Histogram> RocksDBMetrics::getCommitLatencyHistogram() {
	return commitLatencyHistogram;
}
Reference<Histogram> RocksDBMetrics::getCommitActionHistogram() {
	return commitActionHistogram;
}
Reference<Histogram> RocksDBMetrics::getCommitQueueWaitHistogram() {
	return commitQueueWaitHistogram;
}
Reference<Histogram> RocksDBMetrics::getWriteHistogram() {
	return writeHistogram;
}
Reference<Histogram> RocksDBMetrics::getDeleteCompactRangeHistogram() {
	return deleteCompactRangeHistogram;
}

RocksDBMetrics::RocksDBMetrics(UID debugID, std::shared_ptr<rocksdb::Statistics> stats)
  : debugID(debugID), stats(stats) {
	tickerStats = {
		{ "StallMicros", rocksdb::STALL_MICROS, 0 },
		{ "BytesRead", rocksdb::BYTES_READ, 0 },
		{ "IterBytesRead", rocksdb::ITER_BYTES_READ, 0 },
		{ "BytesWritten", rocksdb::BYTES_WRITTEN, 0 },
		{ "BlockCacheMisses", rocksdb::BLOCK_CACHE_MISS, 0 },
		{ "BlockCacheHits", rocksdb::BLOCK_CACHE_HIT, 0 },
		{ "BloomFilterUseful", rocksdb::BLOOM_FILTER_USEFUL, 0 },
		{ "BloomFilterFullPositive", rocksdb::BLOOM_FILTER_FULL_POSITIVE, 0 },
		{ "BloomFilterTruePositive", rocksdb::BLOOM_FILTER_FULL_TRUE_POSITIVE, 0 },
		// Deprecated in RocksDB 8.0
		// { "BloomFilterMicros", rocksdb::BLOOM_FILTER_MICROS, 0 },
		{ "MemtableHit", rocksdb::MEMTABLE_HIT, 0 },
		{ "MemtableMiss", rocksdb::MEMTABLE_MISS, 0 },
		{ "GetHitL0", rocksdb::GET_HIT_L0, 0 },
		{ "GetHitL1", rocksdb::GET_HIT_L1, 0 },
		{ "GetHitL2AndUp", rocksdb::GET_HIT_L2_AND_UP, 0 },
		{ "CountKeysWritten", rocksdb::NUMBER_KEYS_WRITTEN, 0 },
		{ "CountKeysRead", rocksdb::NUMBER_KEYS_READ, 0 },
		{ "CountDBSeek", rocksdb::NUMBER_DB_SEEK, 0 },
		{ "CountDBNext", rocksdb::NUMBER_DB_NEXT, 0 },
		{ "CountDBPrev", rocksdb::NUMBER_DB_PREV, 0 },
		{ "BloomFilterPrefixChecked", rocksdb::BLOOM_FILTER_PREFIX_CHECKED, 0 },
		{ "BloomFilterPrefixUseful", rocksdb::BLOOM_FILTER_PREFIX_USEFUL, 0 },
		// Deprecated in RocksDB 8.0
		// { "BlockCacheCompressedMiss", rocksdb::BLOCK_CACHE_COMPRESSED_MISS, 0 },
		// { "BlockCacheCompressedHit", rocksdb::BLOCK_CACHE_COMPRESSED_HIT, 0 },
		{ "CountWalFileSyncs", rocksdb::WAL_FILE_SYNCED, 0 },
		{ "CountWalFileBytes", rocksdb::WAL_FILE_BYTES, 0 },
		{ "CompactReadBytes", rocksdb::COMPACT_READ_BYTES, 0 },
		{ "CompactWriteBytes", rocksdb::COMPACT_WRITE_BYTES, 0 },
		{ "FlushWriteBytes", rocksdb::FLUSH_WRITE_BYTES, 0 },
		{ "CountBlocksCompressed", rocksdb::NUMBER_BLOCK_COMPRESSED, 0 },
		{ "CountBlocksDecompressed", rocksdb::NUMBER_BLOCK_DECOMPRESSED, 0 },
		{ "RowCacheHit", rocksdb::ROW_CACHE_HIT, 0 },
		{ "RowCacheMiss", rocksdb::ROW_CACHE_MISS, 0 },
		{ "CountIterSkippedKeys", rocksdb::NUMBER_ITER_SKIP, 0 },
		{ "NoIteratorCreated", rocksdb::NO_ITERATOR_CREATED, 0 },
		{ "NoIteratorDeleted", rocksdb::NO_ITERATOR_DELETED, 0 },

	};

	intPropertyStats = {
		{ "NumImmutableMemtables", rocksdb::DB::Properties::kNumImmutableMemTable },
		{ "NumImmutableMemtablesFlushed", rocksdb::DB::Properties::kNumImmutableMemTableFlushed },
		{ "IsMemtableFlushPending", rocksdb::DB::Properties::kMemTableFlushPending },
		{ "NumRunningFlushes", rocksdb::DB::Properties::kNumRunningFlushes },
		{ "IsCompactionPending", rocksdb::DB::Properties::kCompactionPending },
		{ "NumRunningCompactions", rocksdb::DB::Properties::kNumRunningCompactions },
		{ "CumulativeBackgroundErrors", rocksdb::DB::Properties::kBackgroundErrors },
		{ "CurrentSizeActiveMemtable", rocksdb::DB::Properties::kCurSizeActiveMemTable },
		{ "AllMemtablesBytes", rocksdb::DB::Properties::kCurSizeAllMemTables },
		{ "ActiveMemtableBytes", rocksdb::DB::Properties::kSizeAllMemTables },
		{ "CountEntriesActiveMemtable", rocksdb::DB::Properties::kNumEntriesActiveMemTable },
		{ "CountEntriesImmutMemtables", rocksdb::DB::Properties::kNumEntriesImmMemTables },
		{ "CountDeletesActiveMemtable", rocksdb::DB::Properties::kNumDeletesActiveMemTable },
		{ "CountDeletesImmutMemtables", rocksdb::DB::Properties::kNumDeletesImmMemTables },
		{ "EstimatedCountKeys", rocksdb::DB::Properties::kEstimateNumKeys },
		{ "EstimateSstReaderBytes", rocksdb::DB::Properties::kEstimateTableReadersMem },
		{ "CountActiveSnapshots", rocksdb::DB::Properties::kNumSnapshots },
		{ "OldestSnapshotTime", rocksdb::DB::Properties::kOldestSnapshotTime },
		{ "CountLiveVersions", rocksdb::DB::Properties::kNumLiveVersions },
		{ "EstimateLiveDataSize", rocksdb::DB::Properties::kEstimateLiveDataSize },
		{ "BaseLevel", rocksdb::DB::Properties::kBaseLevel },
		{ "EstPendCompactBytes", rocksdb::DB::Properties::kEstimatePendingCompactionBytes },
		{ "BlockCacheUsage", rocksdb::DB::Properties::kBlockCacheUsage },
		{ "BlockCachePinnedUsage", rocksdb::DB::Properties::kBlockCachePinnedUsage },
		{ "LiveSstFilesSize", rocksdb::DB::Properties::kLiveSstFilesSize },
	};

	perfContextMetrics = {
		{ "UserKeyComparisonCount", rocksdb_user_key_comparison_count, {} },
		{ "BlockCacheHitCount", rocksdb_block_cache_hit_count, {} },
		{ "BlockReadCount", rocksdb_block_read_count, {} },
		{ "BlockReadByte", rocksdb_block_read_byte, {} },
		{ "BlockReadTime", rocksdb_block_read_time, {} },
		{ "BlockChecksumTime", rocksdb_block_checksum_time, {} },
		{ "BlockDecompressTime", rocksdb_block_decompress_time, {} },
		{ "GetReadBytes", rocksdb_get_read_bytes, {} },
		{ "MultigetReadBytes", rocksdb_multiget_read_bytes, {} },
		{ "IterReadBytes", rocksdb_iter_read_bytes, {} },
		{ "InternalKeySkippedCount", rocksdb_internal_key_skipped_count, {} },
		{ "InternalDeleteSkippedCount", rocksdb_internal_delete_skipped_count, {} },
		{ "InternalRecentSkippedCount", rocksdb_internal_recent_skipped_count, {} },
		{ "InternalMergeCount", rocksdb_internal_merge_count, {} },
		{ "GetSnapshotTime", rocksdb_get_snapshot_time, {} },
		{ "GetFromMemtableTime", rocksdb_get_from_memtable_time, {} },
		{ "GetFromMemtableCount", rocksdb_get_from_memtable_count, {} },
		{ "GetPostProcessTime", rocksdb_get_post_process_time, {} },
		{ "GetFromOutputFilesTime", rocksdb_get_from_output_files_time, {} },
		{ "SeekOnMemtableTime", rocksdb_seek_on_memtable_time, {} },
		{ "SeekOnMemtableCount", rocksdb_seek_on_memtable_count, {} },
		{ "NextOnMemtableCount", rocksdb_next_on_memtable_count, {} },
		{ "PrevOnMemtableCount", rocksdb_prev_on_memtable_count, {} },
		{ "SeekChildSeekTime", rocksdb_seek_child_seek_time, {} },
		{ "SeekChildSeekCount", rocksdb_seek_child_seek_count, {} },
		{ "SeekMinHeapTime", rocksdb_seek_min_heap_time, {} },
		{ "SeekMaxHeapTime", rocksdb_seek_max_heap_time, {} },
		{ "SeekInternalSeekTime", rocksdb_seek_internal_seek_time, {} },
		{ "FindNextUserEntryTime", rocksdb_find_next_user_entry_time, {} },
		{ "WriteWalTime", rocksdb_write_wal_time, {} },
		{ "WriteMemtableTime", rocksdb_write_memtable_time, {} },
		{ "WriteDelayTime", rocksdb_write_delay_time, {} },
		{ "WritePreAndPostProcessTime", rocksdb_write_pre_and_post_process_time, {} },
		{ "DbMutexLockNanos", rocksdb_db_mutex_lock_nanos, {} },
		{ "DbConditionWaitNanos", rocksdb_db_condition_wait_nanos, {} },
		{ "MergeOperatorTimeNanos", rocksdb_merge_operator_time_nanos, {} },
		{ "ReadIndexBlockNanos", rocksdb_read_index_block_nanos, {} },
		{ "ReadFilterBlockNanos", rocksdb_read_filter_block_nanos, {} },
		{ "NewTableBlockIterNanos", rocksdb_new_table_block_iter_nanos, {} },
		{ "NewTableIteratorNanos", rocksdb_new_table_iterator_nanos, {} },
		{ "BlockSeekNanos", rocksdb_block_seek_nanos, {} },
		{ "FindTableNanos", rocksdb_find_table_nanos, {} },
		{ "BloomMemtableHitCount", rocksdb_bloom_memtable_hit_count, {} },
		{ "BloomMemtableMissCount", rocksdb_bloom_memtable_miss_count, {} },
		{ "BloomSstHitCount", rocksdb_bloom_sst_hit_count, {} },
		{ "BloomSstMissCount", rocksdb_bloom_sst_miss_count, {} },
		{ "KeyLockWaitTime", rocksdb_key_lock_wait_time, {} },
		{ "KeyLockWaitCount", rocksdb_key_lock_wait_count, {} },
		{ "EnvNewSequentialFileNanos", rocksdb_env_new_sequential_file_nanos, {} },
		{ "EnvNewRandomAccessFileNanos", rocksdb_env_new_random_access_file_nanos, {} },
		{ "EnvNewWritableFileNanos", rocksdb_env_new_writable_file_nanos, {} },
		{ "EnvReuseWritableFileNanos", rocksdb_env_reuse_writable_file_nanos, {} },
		{ "EnvNewRandomRwFileNanos", rocksdb_env_new_random_rw_file_nanos, {} },
		{ "EnvNewDirectoryNanos", rocksdb_env_new_directory_nanos, {} },
		{ "EnvFileExistsNanos", rocksdb_env_file_exists_nanos, {} },
		{ "EnvGetChildrenNanos", rocksdb_env_get_children_nanos, {} },
		{ "EnvGetChildrenFileAttributesNanos", rocksdb_env_get_children_file_attributes_nanos, {} },
		{ "EnvDeleteFileNanos", rocksdb_env_delete_file_nanos, {} },
		{ "EnvCreateDirNanos", rocksdb_env_create_dir_nanos, {} },
		{ "EnvCreateDirIfMissingNanos", rocksdb_env_create_dir_if_missing_nanos, {} },
		{ "EnvDeleteDirNanos", rocksdb_env_delete_dir_nanos, {} },
		{ "EnvGetFileSizeNanos", rocksdb_env_get_file_size_nanos, {} },
		{ "EnvGetFileModificationTimeNanos", rocksdb_env_get_file_modification_time_nanos, {} },
		{ "EnvRenameFileNanos", rocksdb_env_rename_file_nanos, {} },
		{ "EnvLinkFileNanos", rocksdb_env_link_file_nanos, {} },
		{ "EnvLockFileNanos", rocksdb_env_lock_file_nanos, {} },
		{ "EnvUnlockFileNanos", rocksdb_env_unlock_file_nanos, {} },
		{ "EnvNewLoggerNanos", rocksdb_env_new_logger_nanos, {} },
	};
	for (auto& [name, metric, vals] : perfContextMetrics) { // readers, then writer
		for (int i = 0; i < SERVER_KNOBS->ROCKSDB_READ_PARALLELISM; i++) {
			vals.push_back(0); // add reader
		}
		vals.push_back(0); // add writer
	}
	for (int i = 0; i < SERVER_KNOBS->ROCKSDB_READ_PARALLELISM; i++) {
		readRangeLatencyHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_LATENCY_HISTOGRAM, Histogram::Unit::milliseconds));
		readValueLatencyHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_LATENCY_HISTOGRAM, Histogram::Unit::milliseconds));
		readPrefixLatencyHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_LATENCY_HISTOGRAM, Histogram::Unit::milliseconds));
		readRangeActionHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_ACTION_HISTOGRAM, Histogram::Unit::milliseconds));
		readValueActionHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_ACTION_HISTOGRAM, Histogram::Unit::milliseconds));
		readPrefixActionHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_ACTION_HISTOGRAM, Histogram::Unit::milliseconds));
		readRangeQueueWaitHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_QUEUEWAIT_HISTOGRAM, Histogram::Unit::milliseconds));
		readValueQueueWaitHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_QUEUEWAIT_HISTOGRAM, Histogram::Unit::milliseconds));
		readPrefixQueueWaitHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_QUEUEWAIT_HISTOGRAM, Histogram::Unit::milliseconds));
		readRangeNewIteratorHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_NEWITERATOR_HISTOGRAM, Histogram::Unit::milliseconds));
		readValueGetHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_GET_HISTOGRAM, Histogram::Unit::milliseconds));
		readPrefixGetHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_GET_HISTOGRAM, Histogram::Unit::milliseconds));
	}
	commitLatencyHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_COMMIT_LATENCY_HISTOGRAM, Histogram::Unit::milliseconds);
	commitActionHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_COMMIT_ACTION_HISTOGRAM, Histogram::Unit::milliseconds);
	commitQueueWaitHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_COMMIT_QUEUEWAIT_HISTOGRAM, Histogram::Unit::milliseconds);
	writeHistogram =
	    Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_WRITE_HISTOGRAM, Histogram::Unit::milliseconds);
	deleteCompactRangeHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_DELETE_COMPACTRANGE_HISTOGRAM, Histogram::Unit::milliseconds);
}

void RocksDBMetrics::logStats(rocksdb::DB* db, std::string manifestDirectory) {
	TraceEvent e(SevInfo, "ShardedRocksDBMetrics", debugID);
	uint64_t stat;
	for (auto& [name, ticker, cumulation] : tickerStats) {
		stat = stats->getTickerCount(ticker);
		e.detail(name, stat - cumulation);
		cumulation = stat;
	}
	for (auto& [name, property] : intPropertyStats) {
		stat = 0;
		ASSERT(db->GetAggregatedIntProperty(property, &stat));
		e.detail(name, stat);
	}

	int64_t manifestBytesTotal = 0;
	auto manifests = getManifestBytes(manifestDirectory);
	int idx = 0;
	for (const auto& [fileName, fileBytes] : manifests) {
		e.detail(format("Manifest-%d", idx), format("%s-%lld", fileName.c_str(), fileBytes));
		manifestBytesTotal += fileBytes;
		idx++;
	}
	e.detail("ManifestBytes", manifestBytesTotal);

	std::string propValue = "";
	ASSERT(db->GetProperty(rocksdb::DB::Properties::kDBWriteStallStats, &propValue));
	TraceEvent(SevInfo, "DBWriteStallStats", debugID).detail("Stats", propValue);

	if (rocksdb_block_cache) {
		e.detail("CacheUsage", rocksdb_block_cache->GetUsage());
	}
}

void RocksDBMetrics::resetPerfContext() {
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableCount);
	rocksdb::get_perf_context()->Reset();
}

void RocksDBMetrics::collectPerfContext(int index) {
	for (auto& [name, metric, vals] : perfContextMetrics) {
		vals[index] = getRocksdbPerfcontextMetric(metric);
	}
}

void RocksDBMetrics::logPerfContext(bool ignoreZeroMetric) {
	TraceEvent e(SevInfo, "ShardedRocksDBPerfContextMetrics", debugID);
	e.setMaxEventLength(20000);
	for (auto& [name, metric, vals] : perfContextMetrics) {
		uint64_t s = 0;
		for (auto& v : vals) {
			s = s + v;
		}
		if (ignoreZeroMetric && s == 0)
			continue;
		e.detail(name, s);
	}
}

uint64_t RocksDBMetrics::getRocksdbPerfcontextMetric(int metric) {
	switch (metric) {
	case rocksdb_user_key_comparison_count:
		return rocksdb::get_perf_context()->user_key_comparison_count;
	case rocksdb_block_cache_hit_count:
		return rocksdb::get_perf_context()->block_cache_hit_count;
	case rocksdb_block_read_count:
		return rocksdb::get_perf_context()->block_read_count;
	case rocksdb_block_read_byte:
		return rocksdb::get_perf_context()->block_read_byte;
	case rocksdb_block_read_time:
		return rocksdb::get_perf_context()->block_read_time;
	case rocksdb_block_checksum_time:
		return rocksdb::get_perf_context()->block_checksum_time;
	case rocksdb_block_decompress_time:
		return rocksdb::get_perf_context()->block_decompress_time;
	case rocksdb_get_read_bytes:
		return rocksdb::get_perf_context()->get_read_bytes;
	case rocksdb_multiget_read_bytes:
		return rocksdb::get_perf_context()->multiget_read_bytes;
	case rocksdb_iter_read_bytes:
		return rocksdb::get_perf_context()->iter_read_bytes;
	case rocksdb_internal_key_skipped_count:
		return rocksdb::get_perf_context()->internal_key_skipped_count;
	case rocksdb_internal_delete_skipped_count:
		return rocksdb::get_perf_context()->internal_delete_skipped_count;
	case rocksdb_internal_recent_skipped_count:
		return rocksdb::get_perf_context()->internal_recent_skipped_count;
	case rocksdb_internal_merge_count:
		return rocksdb::get_perf_context()->internal_merge_count;
	case rocksdb_get_snapshot_time:
		return rocksdb::get_perf_context()->get_snapshot_time;
	case rocksdb_get_from_memtable_time:
		return rocksdb::get_perf_context()->get_from_memtable_time;
	case rocksdb_get_from_memtable_count:
		return rocksdb::get_perf_context()->get_from_memtable_count;
	case rocksdb_get_post_process_time:
		return rocksdb::get_perf_context()->get_post_process_time;
	case rocksdb_get_from_output_files_time:
		return rocksdb::get_perf_context()->get_from_output_files_time;
	case rocksdb_seek_on_memtable_time:
		return rocksdb::get_perf_context()->seek_on_memtable_time;
	case rocksdb_seek_on_memtable_count:
		return rocksdb::get_perf_context()->seek_on_memtable_count;
	case rocksdb_next_on_memtable_count:
		return rocksdb::get_perf_context()->next_on_memtable_count;
	case rocksdb_prev_on_memtable_count:
		return rocksdb::get_perf_context()->prev_on_memtable_count;
	case rocksdb_seek_child_seek_time:
		return rocksdb::get_perf_context()->seek_child_seek_time;
	case rocksdb_seek_child_seek_count:
		return rocksdb::get_perf_context()->seek_child_seek_count;
	case rocksdb_seek_min_heap_time:
		return rocksdb::get_perf_context()->seek_min_heap_time;
	case rocksdb_seek_max_heap_time:
		return rocksdb::get_perf_context()->seek_max_heap_time;
	case rocksdb_seek_internal_seek_time:
		return rocksdb::get_perf_context()->seek_internal_seek_time;
	case rocksdb_find_next_user_entry_time:
		return rocksdb::get_perf_context()->find_next_user_entry_time;
	case rocksdb_write_wal_time:
		return rocksdb::get_perf_context()->write_wal_time;
	case rocksdb_write_memtable_time:
		return rocksdb::get_perf_context()->write_memtable_time;
	case rocksdb_write_delay_time:
		return rocksdb::get_perf_context()->write_delay_time;
	case rocksdb_write_pre_and_post_process_time:
		return rocksdb::get_perf_context()->write_pre_and_post_process_time;
	case rocksdb_db_mutex_lock_nanos:
		return rocksdb::get_perf_context()->db_mutex_lock_nanos;
	case rocksdb_db_condition_wait_nanos:
		return rocksdb::get_perf_context()->db_condition_wait_nanos;
	case rocksdb_merge_operator_time_nanos:
		return rocksdb::get_perf_context()->merge_operator_time_nanos;
	case rocksdb_read_index_block_nanos:
		return rocksdb::get_perf_context()->read_index_block_nanos;
	case rocksdb_read_filter_block_nanos:
		return rocksdb::get_perf_context()->read_filter_block_nanos;
	case rocksdb_new_table_block_iter_nanos:
		return rocksdb::get_perf_context()->new_table_block_iter_nanos;
	case rocksdb_new_table_iterator_nanos:
		return rocksdb::get_perf_context()->new_table_iterator_nanos;
	case rocksdb_block_seek_nanos:
		return rocksdb::get_perf_context()->block_seek_nanos;
	case rocksdb_find_table_nanos:
		return rocksdb::get_perf_context()->find_table_nanos;
	case rocksdb_bloom_memtable_hit_count:
		return rocksdb::get_perf_context()->bloom_memtable_hit_count;
	case rocksdb_bloom_memtable_miss_count:
		return rocksdb::get_perf_context()->bloom_memtable_miss_count;
	case rocksdb_bloom_sst_hit_count:
		return rocksdb::get_perf_context()->bloom_sst_hit_count;
	case rocksdb_bloom_sst_miss_count:
		return rocksdb::get_perf_context()->bloom_sst_miss_count;
	case rocksdb_key_lock_wait_time:
		return rocksdb::get_perf_context()->key_lock_wait_time;
	case rocksdb_key_lock_wait_count:
		return rocksdb::get_perf_context()->key_lock_wait_count;
	case rocksdb_env_new_sequential_file_nanos:
		return rocksdb::get_perf_context()->env_new_sequential_file_nanos;
	case rocksdb_env_new_random_access_file_nanos:
		return rocksdb::get_perf_context()->env_new_random_access_file_nanos;
	case rocksdb_env_new_writable_file_nanos:
		return rocksdb::get_perf_context()->env_new_writable_file_nanos;
	case rocksdb_env_reuse_writable_file_nanos:
		return rocksdb::get_perf_context()->env_reuse_writable_file_nanos;
	case rocksdb_env_new_random_rw_file_nanos:
		return rocksdb::get_perf_context()->env_new_random_rw_file_nanos;
	case rocksdb_env_new_directory_nanos:
		return rocksdb::get_perf_context()->env_new_directory_nanos;
	case rocksdb_env_file_exists_nanos:
		return rocksdb::get_perf_context()->env_file_exists_nanos;
	case rocksdb_env_get_children_nanos:
		return rocksdb::get_perf_context()->env_get_children_nanos;
	case rocksdb_env_get_children_file_attributes_nanos:
		return rocksdb::get_perf_context()->env_get_children_file_attributes_nanos;
	case rocksdb_env_delete_file_nanos:
		return rocksdb::get_perf_context()->env_delete_file_nanos;
	case rocksdb_env_create_dir_nanos:
		return rocksdb::get_perf_context()->env_create_dir_nanos;
	case rocksdb_env_create_dir_if_missing_nanos:
		return rocksdb::get_perf_context()->env_create_dir_if_missing_nanos;
	case rocksdb_env_delete_dir_nanos:
		return rocksdb::get_perf_context()->env_delete_dir_nanos;
	case rocksdb_env_get_file_size_nanos:
		return rocksdb::get_perf_context()->env_get_file_size_nanos;
	case rocksdb_env_get_file_modification_time_nanos:
		return rocksdb::get_perf_context()->env_get_file_modification_time_nanos;
	case rocksdb_env_rename_file_nanos:
		return rocksdb::get_perf_context()->env_rename_file_nanos;
	case rocksdb_env_link_file_nanos:
		return rocksdb::get_perf_context()->env_link_file_nanos;
	case rocksdb_env_lock_file_nanos:
		return rocksdb::get_perf_context()->env_lock_file_nanos;
	case rocksdb_env_unlock_file_nanos:
		return rocksdb::get_perf_context()->env_unlock_file_nanos;
	case rocksdb_env_new_logger_nanos:
		return rocksdb::get_perf_context()->env_new_logger_nanos;
	default:
		break;
	}
	return 0;
}

ACTOR Future<Void> rocksDBAggregatedMetricsLogger(std::shared_ptr<ShardedRocksDBState> rState,
                                                  Future<Void> openFuture,
                                                  std::shared_ptr<RocksDBMetrics> rocksDBMetrics,
                                                  ShardManager* shardManager,
                                                  std::string manifestDirectory) {
	try {
		wait(openFuture);
		state rocksdb::DB* db = shardManager->getDb();
		loop {
			wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
			if (rState->closing) {
				break;
			}
			rocksDBMetrics->logStats(db, manifestDirectory);
			if (SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE != 0) {
				rocksDBMetrics->logPerfContext(true);
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			TraceEvent(SevError, "ShardedRocksDBMetricsError").errorUnsuppressed(e);
		}
	}
	return Void();
}

struct ShardedRocksDBKeyValueStore : IKeyValueStore {
	using CF = rocksdb::ColumnFamilyHandle*;

	ACTOR static Future<Void> refreshIteratorPool(std::shared_ptr<ShardedRocksDBState> rState,
	                                              std::shared_ptr<IteratorPool> iteratorPool,
	                                              Future<Void> readyToStart) {
		if (!SERVER_KNOBS->SHARDED_ROCKSDB_REUSE_ITERATORS) {
			return Void();
		}
		state Reference<Histogram> histogram = Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, "TimeSpentRefreshIterators"_sr, Histogram::Unit::milliseconds);

		try {
			wait(readyToStart);
			loop {
				wait(delay(SERVER_KNOBS->ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME));
				if (rState->closing) {
					break;
				}
				double startTime = timer_monotonic();

				iteratorPool->refresh();
				histogram->sample(timer_monotonic() - startTime);
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "RefreshReadIteratorPoolError").errorUnsuppressed(e);
			}
		}

		return Void();
	}

	ACTOR static Future<Void> refreshRocksDBBackgroundEventCounter(
	    UID id,
	    std::shared_ptr<RocksDBEventListener> eventListener) {
		if (!SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_WHEN_IO_TIMEOUT &&
		    SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_PROBABILITY <= 0.0) {
			return Void();
		}
		try {
			loop {
				wait(delay(SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_PERIOD_SEC));
				eventListener->logRecentRocksDBBackgroundWorkStats(id);
				eventListener->resetCounters();
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "RefreshRocksDBBackgroundEventCounter").errorUnsuppressed(e);
			}
		}
		return Void();
	}

	ACTOR static Future<Void> doRestore(ShardedRocksDBKeyValueStore* self,
	                                    std::string shardId,
	                                    std::vector<KeyRange> ranges,
	                                    std::vector<CheckpointMetaData> checkpoints) {
		for (const KeyRange& range : ranges) {
			std::vector<DataShard*> shards = self->shardManager.getDataShardsByRange(range);
			if (!shards.empty()) {
				TraceEvent te(SevWarnAlways, "RestoreRangesNotEmpty", self->id);
				te.detail("Range", range);
				te.detail("RestoreShardID", shardId);
				for (int i = 0; i < shards.size(); ++i) {
					te.detail("DataShard-" + std::to_string(i), shards[i]->range);
				}
				te.log();
				throw failed_to_restore_checkpoint();
			}
		}
		for (const KeyRange& range : ranges) {
			self->shardManager.addRange(range, shardId, true);
		}
		const Severity sevDm = static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY);
		TraceEvent(sevDm, "ShardedRocksRestoreAddRange", self->id)
		    .detail("Ranges", describe(ranges))
		    .detail("ShardID", shardId)
		    .detail("Checkpoints", describe(checkpoints));
		PhysicalShard* ps = self->shardManager.getPhysicalShardForAllRanges(ranges);
		ASSERT(ps != nullptr);
		auto a = new Writer::RestoreAction(&self->shardManager, ps, self->path, shardId, ranges, checkpoints);
		auto res = a->done.getFuture();
		self->writeThread->post(a);

		try {
			wait(res);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			for (const KeyRange& range : ranges) {
				self->shardManager.removeRange(range);
			}
			throw;
		}

		return Void();
	}

	struct CompactionWorker : IThreadPoolReceiver {
		const UID logId;
		explicit CompactionWorker(UID logId) : logId(logId) {}

		void init() override {}
		~CompactionWorker() override {}

		struct CompactShardsAction : TypedAction<CompactionWorker, CompactShardsAction> {
			std::vector<std::shared_ptr<PhysicalShard>> shards;
			std::shared_ptr<PhysicalShard> metadataShard;
			ThreadReturnPromise<Void> done;
			CompactShardsAction(std::vector<std::shared_ptr<PhysicalShard>> shards, PhysicalShard* metadataShard)
			  : shards(shards), metadataShard(metadataShard) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(CompactShardsAction& a) {
			auto start = now();
			ASSERT(a.metadataShard);
			auto db = a.metadataShard->db;
			int skipped = 0;
			for (auto& shard : a.shards) {
				if (shard->deletePending) {
					++skipped;
					continue;
				}
				std::string value;
				// TODO: Consider load last compaction time during shard init once rocksdb's start time is reduced.
				if (shard->lastCompactionTime <= 0.0) {
					auto s = db->Get(rocksdb::ReadOptions(),
					                 a.metadataShard->cf,
					                 compactionTimestampPrefix.toString() + shard->id,
					                 &value);
					if (s.ok()) {
						auto lastComapction = std::stod(value);
						if (start - lastComapction < SERVER_KNOBS->SHARDED_ROCKSDB_COMPACTION_PERIOD) {
							shard->lastCompactionTime = lastComapction;
							++skipped;
							continue;
						}
					} else if (!s.IsNotFound()) {
						TraceEvent(SevError, "ShardedRocksDBReadValueError", logId).detail("Description", s.ToString());
						a.done.sendError(internal_error());
						return;
					}
				}

				rocksdb::CompactRangeOptions compactOptions;
				// Force RocksDB to rewrite file to last level.
				compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
				shard->db->CompactRange(compactOptions, shard->cf, /*begin=*/nullptr, /*end=*/nullptr);
				shard->db->Put(rocksdb::WriteOptions(),
				               a.metadataShard->cf,
				               compactionTimestampPrefix.toString() + shard->id,
				               std::to_string(start));
				shard->lastCompactionTime = start;
				TraceEvent("ManualCompaction", logId).detail("ShardId", shard->id);
			}

			TraceEvent("CompactionCompleted", logId)
			    .detail("NumShards", a.shards.size())
			    .detail("Skipped", skipped)
			    .detail("Duration", now() - start);
			a.done.send(Void());
		}
	};

	struct Writer : IThreadPoolReceiver {
		const UID logId;
		int threadIndex;
		std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* columnFamilyMap;
		std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
		std::shared_ptr<IteratorPool> iteratorPool;
		double sampleStartTime;

		explicit Writer(UID logId,
		                int threadIndex,
		                std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* columnFamilyMap,
		                std::shared_ptr<RocksDBMetrics> rocksDBMetrics,
		                std::shared_ptr<IteratorPool> iteratorPool)
		  : logId(logId), threadIndex(threadIndex), columnFamilyMap(columnFamilyMap), rocksDBMetrics(rocksDBMetrics),
		    iteratorPool(iteratorPool), sampleStartTime(now()) {}

		~Writer() override {}

		void init() override {}

		void sample() {
			if (SERVER_KNOBS->ROCKSDB_METRICS_SAMPLE_INTERVAL > 0 &&
			    now() - sampleStartTime >= SERVER_KNOBS->ROCKSDB_METRICS_SAMPLE_INTERVAL) {
				rocksDBMetrics->collectPerfContext(threadIndex);
				rocksDBMetrics->resetPerfContext();
				sampleStartTime = now();
			}
		}

		struct OpenAction : TypedAction<Writer, OpenAction> {
			ShardManager* shardManager;
			ThreadReturnPromise<Void> done;
			Optional<Future<Void>>& metrics;
			const FlowLock* readLock;
			const FlowLock* fetchLock;

			OpenAction(ShardManager* shardManager,
			           Optional<Future<Void>>& metrics,
			           const FlowLock* readLock,
			           const FlowLock* fetchLock)
			  : shardManager(shardManager), metrics(metrics), readLock(readLock), fetchLock(fetchLock) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(OpenAction& a) {
			auto status = a.shardManager->init();

			if (!status.ok()) {
				logRocksDBError(status, "Open");
				a.done.sendError(statusToError(status));
				return;
			}

			TraceEvent(SevInfo, "ShardedRocksDB").detail("Method", "Open");
			a.done.send(Void());
		}

		struct AddShardAction : TypedAction<Writer, AddShardAction> {
			PhysicalShard* shard;
			ThreadReturnPromise<Void> done;

			AddShardAction(PhysicalShard* shard) : shard(shard) { ASSERT(shard); }
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(AddShardAction& a) {
			auto s = a.shard->init();
			if (!s.ok()) {
				TraceEvent(SevError, "AddShardError").detail("Status", s.ToString()).detail("ShardId", a.shard->id);
				a.done.sendError(statusToError(s));
				return;
			}
			ASSERT(a.shard->cf);
			(*columnFamilyMap)[a.shard->cf->GetID()] = a.shard->cf;
			a.done.send(Void());
		}

		struct RemoveShardAction : TypedAction<Writer, RemoveShardAction> {
			std::vector<std::shared_ptr<PhysicalShard>> shards;
			PhysicalShard* metadataShard;
			ThreadReturnPromise<Void> done;

			RemoveShardAction(std::vector<std::shared_ptr<PhysicalShard>>& shards, PhysicalShard* metadataShard)
			  : shards(shards), metadataShard(metadataShard) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(RemoveShardAction& a) {
			auto start = now();
			for (auto& shard : a.shards) {
				shard->deletePending = true;
				columnFamilyMap->erase(shard->cf->GetID());
				a.metadataShard->db->Delete(
				    rocksdb::WriteOptions(), a.metadataShard->cf, compactionTimestampPrefix.toString() + shard->id);
				iteratorPool->erase(shard->id);
			}
			TraceEvent("RemoveShardTime").detail("Duration", now() - start).detail("Size", a.shards.size());
			a.shards.clear();
			a.done.send(Void());
		}

		struct CommitAction : TypedAction<Writer, CommitAction> {
			rocksdb::DB* db;
			std::unique_ptr<rocksdb::WriteBatch> writeBatch;
			std::unique_ptr<std::set<PhysicalShard*>> dirtyShards;
			const std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* columnFamilyMap;
			ThreadReturnPromise<Void> done;
			double startTime;
			bool getHistograms;
			bool logShardMemUsage;
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
			CommitAction(rocksdb::DB* db,
			             std::unique_ptr<rocksdb::WriteBatch> writeBatch,
			             std::unique_ptr<std::set<PhysicalShard*>> dirtyShards,
			             std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* columnFamilyMap)
			  : db(db), writeBatch(std::move(writeBatch)), dirtyShards(std::move(dirtyShards)),
			    columnFamilyMap(columnFamilyMap) {
				if (deterministicRandom()->random01() < SERVER_KNOBS->SHARDED_ROCKSDB_HISTOGRAMS_SAMPLE_RATE) {
					getHistograms = true;
					startTime = timer_monotonic();
				} else {
					getHistograms = false;
				}
			}
		};

		struct DeleteVisitor : public rocksdb::WriteBatch::Handler {
			std::vector<std::pair<uint32_t, KeyRange>>* deletes;

			DeleteVisitor(std::vector<std::pair<uint32_t, KeyRange>>* deletes) : deletes(deletes) { ASSERT(deletes); }

			rocksdb::Status DeleteRangeCF(uint32_t column_family_id,
			                              const rocksdb::Slice& begin,
			                              const rocksdb::Slice& end) override {
				deletes->push_back(
				    std::make_pair(column_family_id, KeyRange(KeyRangeRef(toStringRef(begin), toStringRef(end)))));
				return rocksdb::Status::OK();
			}

			rocksdb::Status PutCF(uint32_t column_family_id,
			                      const rocksdb::Slice& key,
			                      const rocksdb::Slice& value) override {
				return rocksdb::Status::OK();
			}

			rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
				return rocksdb::Status::OK();
			}

			rocksdb::Status SingleDeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
				return rocksdb::Status::OK();
			}

			rocksdb::Status MergeCF(uint32_t column_family_id,
			                        const rocksdb::Slice& key,
			                        const rocksdb::Slice& value) override {
				return rocksdb::Status::OK();
			}
		};

		rocksdb::Status doCommit(rocksdb::WriteBatch* batch,
		                         rocksdb::DB* db,
		                         std::vector<std::pair<uint32_t, KeyRange>>* deletes,
		                         bool sample) {
			if (SERVER_KNOBS->SHARDED_ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE) {
				DeleteVisitor dv(deletes);
				rocksdb::Status s = batch->Iterate(&dv);
				if (!s.ok()) {
					logRocksDBError(s, "CommitDeleteVisitor");
					return s;
				}

				// If there are any range deletes, we should have added them to be deleted.
				ASSERT(!deletes->empty() || !batch->HasDeleteRange());
			}

			rocksdb::WriteOptions options;
			options.sync = !SERVER_KNOBS->ROCKSDB_UNSAFE_AUTO_FSYNC;

			double writeBeginTime = sample ? timer_monotonic() : 0;
			rocksdb::Status s = db->Write(options, batch);
			if (sample) {
				rocksDBMetrics->getWriteHistogram()->sampleSeconds(timer_monotonic() - writeBeginTime);
			}
			if (!s.ok()) {
				logRocksDBError(s, "Commit");
				return s;
			}

			return s;
		}

		void action(CommitAction& a) {
			double commitBeginTime;
			if (a.getHistograms) {
				commitBeginTime = timer_monotonic();
				rocksDBMetrics->getCommitQueueWaitHistogram()->sampleSeconds(commitBeginTime - a.startTime);
			}
			std::vector<std::pair<uint32_t, KeyRange>> deletes;
			auto s = doCommit(a.writeBatch.get(), a.db, &deletes, a.getHistograms);
			if (!s.ok()) {
				TraceEvent(SevError, "CommitError").detail("Status", s.ToString());
				a.done.sendError(statusToError(s));
				return;
			}

			if (SERVER_KNOBS->SHARDED_ROCKSDB_REUSE_ITERATORS) {
				for (auto shard : *(a.dirtyShards)) {
					iteratorPool->update(shard->id);
				}
			}

			if (SERVER_KNOBS->SHARDED_ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE) {
				for (const auto& [id, range] : deletes) {
					auto cf = columnFamilyMap->find(id);
					ASSERT(cf != columnFamilyMap->end());
					auto begin = toSlice(range.begin);
					auto end = toSlice(range.end);

					ASSERT(a.db->SuggestCompactRange(cf->second, &begin, &end).ok());
				}
			}

			// Check for number of range deletes in shards.
			// TODO: Disable this once RocksDB is upgraded to a version with range delete improvement.
			if (SERVER_KNOBS->ROCKSDB_CF_RANGE_DELETION_LIMIT > 0) {
				rocksdb::FlushOptions fOptions;
				fOptions.wait = SERVER_KNOBS->ROCKSDB_WAIT_ON_CF_FLUSH;
				fOptions.allow_write_stall = SERVER_KNOBS->SHARDED_ROCKSDB_ALLOW_WRITE_STALL_ON_FLUSH;

				for (auto shard : (*a.dirtyShards)) {
					if (shard->shouldFlush()) {
						TraceEvent("FlushCF")
						    .detail("PhysicalShardId", shard->id)
						    .detail("NumRangeDeletions", shard->numRangeDeletions);
						a.db->Flush(fOptions, shard->cf);
						shard->numRangeDeletions = 0;
					}
				}
			}

			if (a.getHistograms) {
				double currTime = timer_monotonic();
				rocksDBMetrics->getCommitActionHistogram()->sampleSeconds(currTime - commitBeginTime);
				rocksDBMetrics->getCommitLatencyHistogram()->sampleSeconds(currTime - a.startTime);
			}

			sample();

			a.done.send(Void());
		}

		struct CloseAction : TypedAction<Writer, CloseAction> {
			ShardManager* shardManager;
			ThreadReturnPromise<Void> done;
			bool deleteOnClose;
			CloseAction(ShardManager* shardManager, bool deleteOnClose)
			  : shardManager(shardManager), deleteOnClose(deleteOnClose) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(CloseAction& a) {
			if (a.deleteOnClose) {
				a.shardManager->destroyAllShards();
			} else {
				a.shardManager->closeAllShards();
			}
			TraceEvent(SevInfo, "ShardedRocksDB").detail("Method", "Close");
			a.done.send(Void());
		}

		struct CheckpointAction : TypedAction<Writer, CheckpointAction> {
			CheckpointAction(ShardManager* shardManager, const CheckpointRequest& request)
			  : shardManager(shardManager), request(request) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }

			ShardManager* shardManager;
			const CheckpointRequest request;
			ThreadReturnPromise<CheckpointMetaData> reply;
		};

		void action(CheckpointAction& a) {
			TraceEvent(SevInfo, "ShardedRocksCheckpointBegin", logId)
			    .detail("CheckpointID", a.request.checkpointID)
			    .detail("Version", a.request.version)
			    .detail("Ranges", describe(a.request.ranges))
			    .detail("Format", static_cast<int>(a.request.format))
			    .detail("CheckpointDir", a.request.checkpointDir);

			rocksdb::Status s;
			rocksdb::Checkpoint* checkpoint = nullptr;

			PhysicalShard* ps = a.shardManager->getPhysicalShardForAllRanges(a.request.ranges);
			if (ps == nullptr) {
				TraceEvent(SevInfo, "ShardedRocksCheckpointInvalidPhysicalShard", logId)
				    .detail("CheckpointID", a.request.checkpointID)
				    .detail("Version", a.request.version)
				    .detail("Ranges", describe(a.request.ranges))
				    .detail("Format", static_cast<int>(a.request.format))
				    .detail("CheckpointDir", a.request.checkpointDir);
				a.reply.sendError(failed_to_create_checkpoint());
				return;
			}

			TraceEvent(SevDebug, "ShardedRocksCheckpointCF", logId)
			    .detail("CFName", ps->cf->GetName())
			    .detail("CheckpointID", a.request.checkpointID)
			    .detail("Version", a.request.version)
			    .detail("Ranges", describe(a.request.ranges))
			    .detail("PhysicalShardRanges", describe(ps->getAllRanges()))
			    .detail("Format", static_cast<int>(a.request.format))
			    .detail("CheckpointDir", a.request.checkpointDir);

			rocksdb::PinnableSlice value;
			rocksdb::ReadOptions readOptions = getReadOptions();
			s = a.shardManager->getDb()->Get(
			    readOptions, a.shardManager->getSpecialKeysShard()->cf, toSlice(persistVersion), &value);

			if (!s.ok() && !s.IsNotFound()) {
				logRocksDBError(s, "ShardedRocksCheckpointReadPersistVersion");
				a.reply.sendError(failed_to_create_checkpoint());
				return;
			}

			const Version version = s.IsNotFound()
			                            ? latestVersion
			                            : BinaryReader::fromStringRef<Version>(toStringRef(value), Unversioned());

			TraceEvent(SevDebug, "ShardedRocksCheckpointVersion", logId)
			    .detail("CheckpointVersion", a.request.version)
			    .detail("PersistVersion", version);
			ASSERT(a.request.version == version || a.request.version == latestVersion);

			CheckpointMetaData res(a.request.ranges, version, a.request.format, a.request.checkpointID);
			s = rocksdb::Checkpoint::Create(a.shardManager->getDb(), &checkpoint);
			if (!s.ok()) {
				logRocksDBError(s, "CreateRocksDBCheckpoint");
				a.reply.sendError(failed_to_create_checkpoint());
				return;
			}

			const std::string& checkpointDir = abspath(a.request.checkpointDir);
			platform::eraseDirectoryRecursive(checkpointDir);
			if (a.request.format == DataMoveRocksCF) {
				rocksdb::ExportImportFilesMetaData* pMetadata = nullptr;
				s = checkpoint->ExportColumnFamily(ps->cf, checkpointDir, &pMetadata);
				if (!s.ok()) {
					logRocksDBError(s, "CheckpointExportColumnFamily");
					a.reply.sendError(failed_to_create_checkpoint());
					return;
				}

				populateMetaData(&res, pMetadata);
				rocksdb::ExportImportFilesMetaData metadata = *pMetadata;
				delete pMetadata;
				if (!metadata.files.empty() && SERVER_KNOBS->ROCKSDB_ENABLE_CHECKPOINT_VALIDATION) {
					rocksdb::ImportColumnFamilyOptions importOptions;
					importOptions.move_files = false;
					rocksdb::ColumnFamilyHandle* handle;
					const std::string cfName = deterministicRandom()->randomAlphaNumeric(8);
					s = a.shardManager->getDb()->CreateColumnFamilyWithImport(
					    getCFOptions(), cfName, importOptions, metadata, &handle);
					if (!s.ok()) {
						TraceEvent(SevError, "ShardedRocksCheckpointValidateImportError", logId)
						    .detail("Status", s.ToString())
						    .detail("CheckpointID", a.request.checkpointID)
						    .detail("Ranges", describe(a.request.ranges))
						    .detail("CheckDir", a.request.checkpointDir)
						    .detail("CheckpointVersion", a.request.version)
						    .detail("PersistVersion", version);
						a.reply.sendError(failed_to_create_checkpoint());
						return;
					}

					auto options = getReadOptions();
					rocksdb::Iterator* oriIter = a.shardManager->getDb()->NewIterator(options, ps->cf);
					rocksdb::Iterator* resIter = a.shardManager->getDb()->NewIterator(options, handle);
					oriIter->SeekToFirst();
					resIter->SeekToFirst();
					while (oriIter->Valid() && resIter->Valid()) {
						if (oriIter->key().compare(resIter->key()) != 0 ||
						    oriIter->value().compare(resIter->value()) != 0) {
							TraceEvent(SevError, "ShardedRocksCheckpointValidateError", logId)
							    .detail("KeyInDB", toStringRef(oriIter->key()))
							    .detail("ValueInDB", toStringRef(oriIter->value()))
							    .detail("KeyInCheckpoint", toStringRef(resIter->key()))
							    .detail("ValueInCheckpoint", toStringRef(resIter->value()));

							a.reply.sendError(failed_to_create_checkpoint());
							return;
						}
						oriIter->Next();
						resIter->Next();
					}
					if (oriIter->Valid() || resIter->Valid()) {
						a.reply.sendError(failed_to_create_checkpoint());
						return;
					}
					delete oriIter;
					delete resIter;

					s = a.shardManager->getDb()->DropColumnFamily(handle);
					if (!s.ok()) {
						logRocksDBError(s, "CheckpointDropColumnFamily");
						a.reply.sendError(failed_to_create_checkpoint());
						return;
					}
					s = a.shardManager->getDb()->DestroyColumnFamilyHandle(handle);
					if (!s.ok()) {
						logRocksDBError(s, "CheckpointDestroyColumnFamily");
						a.reply.sendError(failed_to_create_checkpoint());
						return;
					}
					TraceEvent(SevDebug, "ShardedRocksCheckpointValidateSuccess", logId)
					    .detail("CheckpointVersion", a.request.version)
					    .detail("PersistVersion", version);
				}
			} else {
				if (checkpoint != nullptr) {
					delete checkpoint;
				}
				a.reply.sendError(not_implemented());
				return;
			}

			res.setState(CheckpointMetaData::Complete);
			res.dir = a.request.checkpointDir;
			a.reply.send(res);

			if (checkpoint != nullptr) {
				delete checkpoint;
			}
			TraceEvent(SevInfo, "ShardedRocksCheckpointEnd", logId).detail("Checkpoint", res.toString());
		}

		struct RestoreAction : TypedAction<Writer, RestoreAction> {
			RestoreAction(ShardManager* shardManager,
			              PhysicalShard* ps,
			              const std::string& path,
			              const std::string& shardId,
			              const std::vector<KeyRange>& ranges,
			              const std::vector<CheckpointMetaData>& checkpoints)
			  : shardManager(shardManager), ps(ps), path(path), shardId(shardId), ranges(ranges),
			    checkpoints(checkpoints) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }

			ShardManager* shardManager;
			PhysicalShard* ps;
			const std::string path;
			const std::string shardId;
			std::vector<KeyRange> ranges;
			std::vector<CheckpointMetaData> checkpoints;
			ThreadReturnPromise<Void> done;
		};

		void action(RestoreAction& a) {
			TraceEvent(SevInfo, "ShardedRocksDBRestoreBegin", logId)
			    .detail("Path", a.path)
			    .detail("Checkpoints", describe(a.checkpoints));

			ASSERT(!a.checkpoints.empty());

			// PhysicalShard* ps = a.shardManager->getPhysicalShardForAllRanges(a.ranges);
			PhysicalShard* ps = a.ps;
			ASSERT(ps != nullptr);

			const CheckpointFormat format = a.checkpoints[0].getFormat();
			for (int i = 1; i < a.checkpoints.size(); ++i) {
				if (a.checkpoints[i].getFormat() != format) {
					throw invalid_checkpoint_format();
				}
			}

			rocksdb::Status status;
			rocksdb::WriteBatch writeBatch(
			    0, // reserved_bytes default:0
			    0, // max_bytes default:0
			    SERVER_KNOBS->ROCKSDB_WRITEBATCH_PROTECTION_BYTES_PER_KEY, // protection_bytes_per_key
			    0 /* default_cf_ts_sz default:0 */);
			rocksdb::WriteOptions options;
			options.sync = !SERVER_KNOBS->ROCKSDB_UNSAFE_AUTO_FSYNC;

			if (format == DataMoveRocksCF) {
				if (a.checkpoints.size() > 1) {
					TraceEvent(SevError, "ShardedRocksDBRestoreMultipleCFs", logId)
					    .detail("Path", a.path)
					    .detail("Checkpoints", describe(a.checkpoints));
					a.done.sendError(failed_to_restore_checkpoint());
					return;
				}
				CheckpointMetaData& checkpoint = a.checkpoints.front();
				std::sort(a.ranges.begin(), a.ranges.end(), KeyRangeRef::ArbitraryOrder());
				std::sort(checkpoint.ranges.begin(), checkpoint.ranges.end(), KeyRangeRef::ArbitraryOrder());
				if (a.ranges.empty() || checkpoint.ranges.empty() || a.ranges.size() > checkpoint.ranges.size() ||
				    a.ranges.front().begin != checkpoint.ranges.front().begin) {
					TraceEvent(SevError, "ShardedRocksDBRestoreFailed", logId)
					    .detail("Path", a.path)
					    .detail("Ranges", describe(a.ranges))
					    .detail("Checkpoints", describe(a.checkpoints));
					a.done.sendError(failed_to_restore_checkpoint());
					return;
				}

				for (int i = 0; i < a.ranges.size(); ++i) {
					if (a.ranges[i] != checkpoint.ranges[i] && i != a.ranges.size() - 1) {
						TraceEvent(SevError, "ShardedRocksDBRestoreFailed", logId)
						    .detail("Path", a.path)
						    .detail("Ranges", describe(a.ranges))
						    .detail("Checkpoints", describe(a.checkpoints));
						a.done.sendError(failed_to_restore_checkpoint());
						return;
					}
				}

				ASSERT(!ps->initialized());
				TraceEvent(SevDebug, "ShardedRocksRestoringCF", logId)
				    .detail("Path", a.path)
				    .detail("Checkpoints", describe(a.checkpoints))
				    .detail("RocksDBCF", getRocksCF(a.checkpoints[0]).toString());

				status = ps->restore(a.checkpoints[0]);

				if (!status.ok()) {
					a.done.sendError(failed_to_restore_checkpoint());
					return;
				} else {
					ASSERT(ps->initialized());
					(*columnFamilyMap)[ps->cf->GetID()] = ps->cf;
					TraceEvent(SevInfo, "RocksDBRestoreCFSuccess", logId)
					    .detail("Path", a.path)
					    .detail("ColumnFaminly", ps->cf->GetName())
					    .detail("Checkpoints", describe(a.checkpoints));

					// Remove the extra data.
					KeyRangeRef cRange(a.ranges.back().end, checkpoint.ranges.back().end);
					if (!cRange.empty()) {
						TraceEvent(SevInfo, "RocksDBRestoreCFRemoveExtraRange", logId)
						    .detail("Path", a.path)
						    .detail("Checkpoints", describe(a.checkpoints))
						    .detail("RestoreRanges", describe(a.ranges))
						    .detail("ClearExtraRange", cRange);
						writeBatch.DeleteRange(ps->cf, toSlice(cRange.begin), toSlice(cRange.end));
						status = a.shardManager->getDb()->Write(options, &writeBatch);
						if (!status.ok()) {
							logRocksDBError(status, "RestoreClearnExtraRanges");
							a.done.sendError(statusToError(status));
							return;
						}
					}
				}
			} else if (format == RocksDBKeyValues) {
				// Make sure the files are complete for the desired ranges.
				std::vector<KeyRange> fetchedRanges;
				std::vector<KeyRange> intendedRanges(a.ranges.begin(), a.ranges.end());
				std::vector<RocksDBCheckpointKeyValues> rkvs;
				for (const auto& checkpoint : a.checkpoints) {
					rkvs.push_back(getRocksKeyValuesCheckpoint(checkpoint));
					ASSERT(!rkvs.back().fetchedFiles.empty());
					for (const auto& file : rkvs.back().fetchedFiles) {
						fetchedRanges.push_back(file.range);
					}
				}
				// Verify that the collective fetchedRanges is the same as the collective intendedRanges.
				std::sort(fetchedRanges.begin(), fetchedRanges.end(), KeyRangeRef::ArbitraryOrder());
				std::sort(intendedRanges.begin(), intendedRanges.end(), KeyRangeRef::ArbitraryOrder());
				int i = 0, j = 0;
				while (i < fetchedRanges.size() && j < intendedRanges.size()) {
					if (fetchedRanges[i].begin != intendedRanges[j].begin) {
						break;
					} else if (fetchedRanges[i] == intendedRanges[j]) {
						++i;
						++j;
					} else if (fetchedRanges[i].contains(intendedRanges[j])) {
						fetchedRanges[i] = KeyRangeRef(intendedRanges[j].end, fetchedRanges[i].end);
						++j;
					} else if (intendedRanges[j].contains(fetchedRanges[i])) {
						intendedRanges[j] = KeyRangeRef(fetchedRanges[i].end, intendedRanges[j].end);
						++i;
					} else {
						break;
					}
				}
				if (i != fetchedRanges.size() || j != intendedRanges.size()) {
					TraceEvent(SevError, "ShardedRocksDBRestoreFailed", logId)
					    .detail("Reason", "RestoreFilesRangesMismatch")
					    .detail("Ranges", describe(a.ranges))
					    .detail("FetchedRanges", describe(fetchedRanges))
					    .detail("IntendedRanges", describe(intendedRanges))
					    .setMaxFieldLength(1000)
					    .detail("FetchedFiles", describe(rkvs));
					a.done.sendError(failed_to_restore_checkpoint());
					return;
				}

				if (!ps->initialized()) {
					TraceEvent(SevDebug, "ShardedRocksRestoreInitPS", logId)
					    .detail("Path", a.path)
					    .detail("Checkpoints", describe(a.checkpoints))
					    .detail("PhysicalShard", ps->toString());
					status = ps->init();
					if (!status.ok()) {
						logRocksDBError(status, "RestoreInitPhysicalShard");
						a.done.sendError(statusToError(status));
						return;
					}
					(*columnFamilyMap)[ps->cf->GetID()] = ps->cf;
				}
				if (!status.ok()) {
					logRocksDBError(status, "RestoreInitPhysicalShard");
					a.done.sendError(statusToError(status));
					return;
				}

				for (const auto& checkpoint : a.checkpoints) {
					status = ps->restore(checkpoint);
					if (!status.ok()) {
						TraceEvent(SevWarnAlways, "ShardedRocksIngestFileError", logId)
						    .detail("Error", status.ToString())
						    .detail("Path", a.path)
						    .detail("Checkpoint", checkpoint.toString())
						    .detail("PhysicalShard", ps->toString());
						break;
					}
				}

				if (!status.ok()) {
					TraceEvent(SevWarnAlways, "ShardedRocksRestoreError", logId).detail("Status", status.ToString());
					for (const auto& range : a.ranges) {
						writeBatch.DeleteRange(ps->cf, toSlice(range.begin), toSlice(range.end));
					}
					TraceEvent(SevInfo, "ShardedRocksRevertRestore", logId)
					    .detail("Path", a.path)
					    .detail("Checkpoints", describe(a.checkpoints))
					    .detail("PhysicalShard", ps->toString())
					    .detail("RestoreRanges", describe(a.ranges));

					rocksdb::Status s = a.shardManager->getDb()->Write(options, &writeBatch);
					if (!s.ok()) {
						TraceEvent(SevError, "ShardedRocksRevertRestoreError", logId)
						    .detail("Error", s.ToString())
						    .detail("Path", a.path)
						    .detail("PhysicalShard", ps->toString())
						    .detail("RestoreRanges", describe(a.ranges));
					}
					a.done.sendError(failed_to_restore_checkpoint());
					return;
				}
			} else if (format == RocksDB) {
				a.done.sendError(not_implemented());
				return;
			}

			TraceEvent(SevInfo, "ShardedRocksDBRestoreEnd", logId)
			    .detail("Path", a.path)
			    .detail("Checkpoints", describe(a.checkpoints));
			a.done.send(Void());
		}
	};

	struct Reader : IThreadPoolReceiver {
		const UID logId;
		double readValueTimeout;
		double readValuePrefixTimeout;
		double readRangeTimeout;
		int threadIndex;
		std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
		std::shared_ptr<IteratorPool> iteratorPool;
		double sampleStartTime;

		explicit Reader(UID logId,
		                int threadIndex,
		                std::shared_ptr<RocksDBMetrics> rocksDBMetrics,
		                std::shared_ptr<IteratorPool> iteratorPool)
		  : logId(logId), threadIndex(threadIndex), rocksDBMetrics(rocksDBMetrics), iteratorPool(iteratorPool),
		    sampleStartTime(now()) {
			readValueTimeout = SERVER_KNOBS->ROCKSDB_READ_VALUE_TIMEOUT;
			readValuePrefixTimeout = SERVER_KNOBS->ROCKSDB_READ_VALUE_PREFIX_TIMEOUT;
			readRangeTimeout = SERVER_KNOBS->ROCKSDB_READ_RANGE_TIMEOUT;
		}

		void init() override {}

		void sample() {
			if (SERVER_KNOBS->ROCKSDB_METRICS_SAMPLE_INTERVAL > 0 &&
			    now() - sampleStartTime >= SERVER_KNOBS->ROCKSDB_METRICS_SAMPLE_INTERVAL) {
				rocksDBMetrics->collectPerfContext(threadIndex);
				rocksDBMetrics->resetPerfContext();
				sampleStartTime = now();
			}
		}
		struct ReadValueAction : TypedAction<Reader, ReadValueAction> {
			Key key;
			PhysicalShard* shard;
			ReadType type;
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			bool logShardMemUsage;
			ThreadReturnPromise<Optional<Value>> result;

			ReadValueAction(KeyRef key, PhysicalShard* shard, ReadType type, Optional<UID> debugID)
			  : key(key), shard(shard), type(type), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms((deterministicRandom()->random01() < SERVER_KNOBS->SHARDED_ROCKSDB_HISTOGRAMS_SAMPLE_RATE)
			                      ? true
			                      : false) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};

		void action(ReadValueAction& a) {
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				rocksDBMetrics->getReadValueQueueWaitHistogram(threadIndex)->sampleSeconds(readBeginTime - a.startTime);
			}
			Optional<TraceBatch> traceBatch;
			if (a.debugID.present()) {
				traceBatch = { TraceBatch{} };
				traceBatch.get().addEvent("GetValueDebug", a.debugID.get().first(), "Reader.Before");
			}
			if (shouldThrottle(a.type, a.key) && SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT &&
			    readBeginTime - a.startTime > readValueTimeout) {
				TraceEvent(SevWarn, "ShardedRocksDBError")
				    .detail("Error", "Read value request timedout")
				    .detail("Method", "ReadValueAction")
				    .detail("Timeout value", readValueTimeout);
				if (SERVER_KNOBS->ROCKSDB_RETURN_OVERLOADED_ON_TIMEOUT) {
					a.result.sendError(server_overloaded());
				} else {
					a.result.sendError(key_value_store_deadline_exceeded());
				}
				return;
			}

			rocksdb::PinnableSlice value;
			auto options = getReadOptions();

			auto db = a.shard->db;
			if (shouldThrottle(a.type, a.key) && SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT) {
				uint64_t deadlineMircos =
				    db->GetEnv()->NowMicros() + (readValueTimeout - (timer_monotonic() - a.startTime)) * 1000000;
				std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
				options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);
			}
			double dbGetBeginTime = a.getHistograms ? timer_monotonic() : 0;
			auto s = db->Get(options, a.shard->cf, toSlice(a.key), &value);

			if (a.getHistograms) {
				rocksDBMetrics->getReadValueGetHistogram(threadIndex)
				    ->sampleSeconds(timer_monotonic() - dbGetBeginTime);
			}

			if (a.debugID.present()) {
				traceBatch.get().addEvent("GetValueDebug", a.debugID.get().first(), "Reader.After");
				traceBatch.get().dump();
			}
			if (s.ok()) {
				a.result.send(Value(toStringRef(value)));
			} else if (s.IsNotFound()) {
				a.result.send(Optional<Value>());
			} else {
				logRocksDBError(s, "ReadValue");
				a.result.sendError(statusToError(s));
			}

			if (a.getHistograms) {
				double currTime = timer_monotonic();
				rocksDBMetrics->getReadValueActionHistogram(threadIndex)->sampleSeconds(currTime - readBeginTime);
				rocksDBMetrics->getReadValueLatencyHistogram(threadIndex)->sampleSeconds(currTime - a.startTime);
			}

			sample();
		}

		struct ReadValuePrefixAction : TypedAction<Reader, ReadValuePrefixAction> {
			Key key;
			int maxLength;
			PhysicalShard* shard;
			ReadType type;
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			bool logShardMemUsage;
			ThreadReturnPromise<Optional<Value>> result;

			ReadValuePrefixAction(Key key, int maxLength, PhysicalShard* shard, ReadType type, Optional<UID> debugID)
			  : key(key), maxLength(maxLength), shard(shard), type(type), debugID(debugID),
			    startTime(timer_monotonic()),
			    getHistograms((deterministicRandom()->random01() < SERVER_KNOBS->SHARDED_ROCKSDB_HISTOGRAMS_SAMPLE_RATE)
			                      ? true
			                      : false){};
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};

		void action(ReadValuePrefixAction& a) {
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				rocksDBMetrics->getReadPrefixQueueWaitHistogram(threadIndex)
				    ->sampleSeconds(readBeginTime - a.startTime);
			}
			Optional<TraceBatch> traceBatch;
			if (a.debugID.present()) {
				traceBatch = { TraceBatch{} };
				traceBatch.get().addEvent("GetValuePrefixDebug",
				                          a.debugID.get().first(),
				                          "Reader.Before"); //.detail("TaskID", g_network->getCurrentTask());
			}
			if (shouldThrottle(a.type, a.key) && SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT &&
			    readBeginTime - a.startTime > readValuePrefixTimeout) {
				TraceEvent(SevWarn, "ShardedRocksDBError")
				    .detail("Error", "Read value prefix request timedout")
				    .detail("Method", "ReadValuePrefixAction")
				    .detail("Timeout value", readValuePrefixTimeout);

				if (SERVER_KNOBS->ROCKSDB_RETURN_OVERLOADED_ON_TIMEOUT) {
					a.result.sendError(server_overloaded());
				} else {
					a.result.sendError(key_value_store_deadline_exceeded());
				}
				return;
			}

			rocksdb::PinnableSlice value;
			auto options = getReadOptions();
			auto db = a.shard->db;
			if (shouldThrottle(a.type, a.key) && SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT) {
				uint64_t deadlineMircos =
				    db->GetEnv()->NowMicros() + (readValuePrefixTimeout - (timer_monotonic() - a.startTime)) * 1000000;
				std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
				options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);
			}

			double dbGetBeginTime = a.getHistograms ? timer_monotonic() : 0;
			auto s = db->Get(options, a.shard->cf, toSlice(a.key), &value);

			if (a.getHistograms) {
				rocksDBMetrics->getReadPrefixGetHistogram(threadIndex)
				    ->sampleSeconds(timer_monotonic() - dbGetBeginTime);
			}

			if (a.debugID.present()) {
				traceBatch.get().addEvent("GetValuePrefixDebug",
				                          a.debugID.get().first(),
				                          "Reader.After"); //.detail("TaskID", g_network->getCurrentTask());
				traceBatch.get().dump();
			}
			if (s.ok()) {
				a.result.send(Value(StringRef(reinterpret_cast<const uint8_t*>(value.data()),
				                              std::min(value.size(), size_t(a.maxLength)))));
			} else if (s.IsNotFound()) {
				a.result.send(Optional<Value>());
			} else {
				logRocksDBError(s, "ReadValuePrefix");
				a.result.sendError(statusToError(s));
			}
			if (a.getHistograms) {
				double currTime = timer_monotonic();
				rocksDBMetrics->getReadPrefixActionHistogram(threadIndex)->sampleSeconds(currTime - readBeginTime);
				rocksDBMetrics->getReadPrefixLatencyHistogram(threadIndex)->sampleSeconds(currTime - a.startTime);
			}

			sample();
		}

		struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			KeyRange keys;
			std::vector<std::pair<PhysicalShard*, KeyRange>> shardRanges;
			int rowLimit, byteLimit;
			ReadType type;
			double startTime;
			bool getHistograms;
			bool logShardMemUsage;
			ThreadReturnPromise<RangeResult> result;
			ReadRangeAction(KeyRange keys, std::vector<DataShard*> shards, int rowLimit, int byteLimit, ReadType type)
			  : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit), type(type), startTime(timer_monotonic()),
			    getHistograms((deterministicRandom()->random01() < SERVER_KNOBS->SHARDED_ROCKSDB_HISTOGRAMS_SAMPLE_RATE)
			                      ? true
			                      : false) {
				std::set<PhysicalShard*> usedShards;
				for (const DataShard* shard : shards) {
					ASSERT(shard);
					shardRanges.emplace_back(shard->physicalShard, keys & shard->range);
					usedShards.insert(shard->physicalShard);
				}
				if (usedShards.size() != shards.size()) {
					TraceEvent("ReadRangeMetrics")
					    .detail("NumPhysicalShards", usedShards.size())
					    .detail("NumDataShards", shards.size());
				}
			}
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
		};

		void action(ReadRangeAction& a) {
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				rocksDBMetrics->getReadRangeQueueWaitHistogram(threadIndex)->sampleSeconds(readBeginTime - a.startTime);
			}
			if (shouldThrottle(a.type, a.keys.begin) && SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT &&
			    readBeginTime - a.startTime > readRangeTimeout) {
				TraceEvent(SevWarn, "ShardedRocksKVSReadTimeout")
				    .detail("Error", "Read range request timedout")
				    .detail("Method", "ReadRangeAction")
				    .detail("Timeout value", readRangeTimeout);

				if (SERVER_KNOBS->ROCKSDB_RETURN_OVERLOADED_ON_TIMEOUT) {
					a.result.sendError(server_overloaded());
				} else {
					a.result.sendError(key_value_store_deadline_exceeded());
				}
				return;
			}

			int rowLimit = a.rowLimit;
			int byteLimit = a.byteLimit;
			RangeResult result;

			if (rowLimit == 0 || byteLimit == 0) {
				a.result.send(result);
			}
			if (rowLimit < 0) {
				// Reverses the shard order so we could read range in reverse direction.
				std::reverse(a.shardRanges.begin(), a.shardRanges.end());
			}

			// TODO: consider multi-thread reads. It's possible to read multiple shards in parallel. However, the
			// number of rows to read needs to be calculated based on the previous read result. We may read more
			// than we expected when parallel read is used when the previous result is not available. It's unlikely
			// to get to performance improvement when the actual number of rows to read is very small.
			int accumulatedBytes = 0;
			int numShards = 0;
			for (auto& [shard, range] : a.shardRanges) {
				if (shard == nullptr || !shard->initialized()) {
					TraceEvent(SevWarn, "ShardedRocksReadRangeShardNotReady", logId)
					    .detail("Range", range)
					    .detail("Reason", shard == nullptr ? "Not Exist" : "Not Initialized");
					continue;
				}
				auto bytesRead = readRangeInDb(shard, range, rowLimit, byteLimit, &result, iteratorPool);
				if (bytesRead < 0) {
					// Error reading an instance.
					a.result.sendError(internal_error());
					return;
				}
				byteLimit -= bytesRead;
				accumulatedBytes += bytesRead;
				++numShards;
				if (result.size() >= abs(a.rowLimit) || accumulatedBytes >= a.byteLimit) {
					break;
				}

				if (shouldThrottle(a.type, a.keys.begin) && SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT &&
				    timer_monotonic() - a.startTime > readRangeTimeout) {
					TraceEvent(SevInfo, "ShardedRocksDBTimeout")
					    .detail("Action", "ReadRange")
					    .detail("ShardsRead", numShards)
					    .detail("BytesRead", accumulatedBytes);

					if (SERVER_KNOBS->ROCKSDB_RETURN_OVERLOADED_ON_TIMEOUT) {
						a.result.sendError(server_overloaded());
					} else {
						a.result.sendError(key_value_store_deadline_exceeded());
					}
					return;
				}
			}

			result.more =
			    (result.size() == a.rowLimit) || (result.size() == -a.rowLimit) || (accumulatedBytes >= a.byteLimit);
			a.result.send(result);
			if (a.getHistograms) {
				double currTime = timer_monotonic();
				rocksDBMetrics->getReadRangeActionHistogram(threadIndex)->sampleSeconds(currTime - readBeginTime);
				rocksDBMetrics->getReadRangeLatencyHistogram(threadIndex)->sampleSeconds(currTime - a.startTime);
				if (a.shardRanges.size() > 1) {
					TraceEvent(SevInfo, "ShardedRocksDB").detail("ReadRangeShards", a.shardRanges.size());
				}
			}

			sample();
		}
	};

	// Persist shard mappinng key range should not be in shardMap.
	explicit ShardedRocksDBKeyValueStore(const std::string& path, UID id)
	  : rState(std::make_shared<ShardedRocksDBState>()), path(path), id(id),
	    readSemaphore(SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    fetchSemaphore(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX),
	    numReadWaiters(SERVER_KNOBS->ROCKSDB_READ_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    numFetchWaiters(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX),
	    errorListener(std::make_shared<RocksDBErrorListener>()),
	    eventListener(std::make_shared<RocksDBEventListener>(id)),
	    errorFuture(forwardError(errorListener->getFuture())), dbOptions(getOptions()),
	    iteratorPool(std::make_shared<IteratorPool>()),
	    shardManager(path, id, dbOptions, errorListener, eventListener, &counters, iteratorPool),
	    rocksDBMetrics(std::make_shared<RocksDBMetrics>(id, dbOptions.statistics)) {
		// In simluation, run the reader/writer threads as Coro threads (i.e. in the network thread. The storage
		// engine is still multi-threaded as background compaction threads are still present. Reads/writes to disk
		// will also block the network thread in a way that would be unacceptable in production but is a necessary
		// evil here. When performing the reads in background threads in simulation, the event loop thinks there is
		// no work to do and advances time faster than 1 sec/sec. By the time the blocking read actually finishes,
		// simulation has advanced time by more than 5 seconds, so every read fails with a transaction_too_old
		// error. Doing blocking IO on the main thread solves this issue. There are almost certainly better fixes,
		// but my goal was to get a less invasive change merged first and work on a more realistic version if/when
		// we think that would provide substantially more confidence in the correctness.
		// TODO: Adapt the simulation framework to not advance time quickly when background reads/writes are
		// occurring.
		if (g_network->isSimulated()) {
			TraceEvent(SevDebug, "ShardedRocksDB").detail("Info", "Use Coro threads in simulation.");
			writeThread = CoroThreadPool::createThreadPool();
			compactionThread = CoroThreadPool::createThreadPool();
			readThreads = CoroThreadPool::createThreadPool();
		} else {
			writeThread = createGenericThreadPool(/*stackSize=*/0, SERVER_KNOBS->ROCKSDB_WRITER_THREAD_PRIORITY);
			compactionThread = createGenericThreadPool(0, SERVER_KNOBS->ROCKSDB_COMPACTION_THREAD_PRIORITY);
			readThreads = createGenericThreadPool(/*stackSize=*/0, SERVER_KNOBS->ROCKSDB_READER_THREAD_PRIORITY);
		}
		writeThread->addThread(new Writer(id, 0, shardManager.getColumnFamilyMap(), rocksDBMetrics, iteratorPool),
		                       "fdb-rocksdb-wr");
		compactionThread->addThread(new CompactionWorker(id), "fdb-rocksdb-cw");
		TraceEvent("ShardedRocksDBReadThreads", id)
		    .detail("KnobRocksDBReadParallelism", SERVER_KNOBS->ROCKSDB_READ_PARALLELISM);
		for (unsigned i = 0; i < SERVER_KNOBS->ROCKSDB_READ_PARALLELISM; ++i) {
			readThreads->addThread(new Reader(id, i, rocksDBMetrics, iteratorPool), "fdb-rocksdb-re");
		}
	}

	Future<Void> getError() const override { return errorFuture; }

	ACTOR static void doClose(ShardedRocksDBKeyValueStore* self, bool deleteOnClose) {
		self->rState->closing = true;
		// The metrics future retains a reference to the DB, so stop it before we delete it.
		self->metrics.reset();
		self->refreshHolder.cancel();
		self->refreshRocksDBBackgroundWorkHolder.cancel();
		self->cleanUpJob.cancel();
		self->counterLogger.cancel();

		try {
			wait(self->readThreads->stop());
		} catch (Error& e) {
			TraceEvent(SevError, "ShardedRocksCloseReadThreadError").errorUnsuppressed(e);
		}

		TraceEvent("CloseKeyValueStore").detail("DeleteKVS", deleteOnClose);
		self->iteratorPool->clear();
		auto a = new Writer::CloseAction(&self->shardManager, deleteOnClose);
		auto f = a->done.getFuture();
		self->writeThread->post(a);
		try {
			wait(f);
		} catch (Error& e) {
			TraceEvent(SevError, "ShardedRocksCloseActionError").errorUnsuppressed(e);
		}

		try {
			wait(self->writeThread->stop());
			wait(self->compactionThread->stop());
		} catch (Error& e) {
			TraceEvent(SevError, "ShardedRocksCloseWriteThreadError").errorUnsuppressed(e);
		}

		if (deleteOnClose && directoryExists(self->path)) {
			TraceEvent(SevWarn, "DirectoryNotEmpty", self->id).detail("Path", self->path);
			platform::eraseDirectoryRecursive(self->path);
		}

		if (self->closePromise.canBeSet()) {
			self->closePromise.send(Void());
		}
		delete self;
	}

	Future<Void> onClosed() const override { return closePromise.getFuture(); }

	void dispose() override { doClose(this, true); }

	void close() override { doClose(this, false); }

	KeyValueStoreType getType() const override { return KeyValueStoreType(KeyValueStoreType::SSD_SHARDED_ROCKSDB); }

	bool shardAware() const override { return true; }

	Future<Void> init() override {
		if (openFuture.isValid()) {
			return openFuture;
			// Restore durable state if KVS is open. KVS will be re-initialized during rollback. To avoid the cost
			// of opening and closing multiple rocksdb instances, we reconcile the shard map using persist shard
			// mapping data.
		} else {
			auto a = std::make_unique<Writer::OpenAction>(&shardManager, metrics, &readSemaphore, &fetchSemaphore);
			openFuture = a->done.getFuture();
			if (SERVER_KNOBS->ROCKSDB_ENABLE_NONDETERMINISM) {
				this->metrics =
				    ShardManager::shardMetricsLogger(this->rState, openFuture, &shardManager) &&
				    rocksDBAggregatedMetricsLogger(this->rState, openFuture, rocksDBMetrics, &shardManager, this->path);
			}
			this->compactionJob = compactShards(this->rState, openFuture, &shardManager, compactionThread);
			this->refreshHolder = refreshIteratorPool(this->rState, iteratorPool, openFuture);
			this->refreshRocksDBBackgroundWorkHolder =
			    refreshRocksDBBackgroundEventCounter(this->id, this->eventListener);
			this->cleanUpJob = emptyShardCleaner(this->rState, openFuture, &shardManager, writeThread);
			writeThread->post(a.release());
			counterLogger = counters.cc.traceCounters("RocksDBCounters", id, SERVER_KNOBS->ROCKSDB_METRICS_DELAY);
			return openFuture;
		}
	}

	Future<Void> addRange(KeyRangeRef range, std::string id, bool active) override {
		auto shard = shardManager.addRange(range, id, active);
		if (shard->initialized()) {
			return Void();
		}
		auto a = new Writer::AddShardAction(shard);
		Future<Void> res = a->done.getFuture();
		writeThread->post(a);
		return res;
	}

	void markRangeAsActive(KeyRangeRef range) override { shardManager.markRangeAsActive(range); }

	void set(KeyValueRef kv, const Arena*) override {
		shardManager.put(kv.key, kv.value);
		if (SERVER_KNOBS->ROCKSDB_USE_POINT_DELETE_FOR_SYSTEM_KEYS && systemKeys.contains(kv.key)) {
			keysSet.insert(kv.key);
		}
	}

	void clear(KeyRangeRef range, const Arena*) override {
		if (range.singleKeyRange()) {
			shardManager.clear(range.begin);
			keysSet.erase(range.begin);
		} else {
			shardManager.clearRange(range, &keysSet);
		}
	}

	static bool overloaded(const uint64_t estPendCompactBytes) {
		// Rocksdb metadata estPendCompactBytes is not deterministic so we don't use it in simulation. We still want to
		// exercise the overload functionality for test coverage, so we return overloaded = true 5% of the time.
		if (g_network->isSimulated()) {
			return deterministicRandom()->randomInt(0, 100) < 5;
		}
		return estPendCompactBytes > SERVER_KNOBS->ROCKSDB_CAN_COMMIT_COMPACT_BYTES_LIMIT;
	}

	// Checks and waits for few seconds if rocskdb is overloaded.
	ACTOR Future<Void> checkRocksdbState(rocksdb::DB* db) {
		state uint64_t estPendCompactBytes;
		state int count = SERVER_KNOBS->ROCKSDB_CAN_COMMIT_DELAY_TIMES_ON_OVERLOAD;
		db->GetAggregatedIntProperty(rocksdb::DB::Properties::kEstimatePendingCompactionBytes, &estPendCompactBytes);
		while (count && overloaded(estPendCompactBytes)) {
			wait(delay(SERVER_KNOBS->ROCKSDB_CAN_COMMIT_DELAY_ON_OVERLOAD));
			count--;
			db->GetAggregatedIntProperty(rocksdb::DB::Properties::kEstimatePendingCompactionBytes,
			                             &estPendCompactBytes);
		}

		return Void();
	}

	Future<Void> canCommit() override { return checkRocksdbState(shardManager.getDb()); }

	Future<Void> commit(bool) override {
		auto a = new Writer::CommitAction(shardManager.getDb(),
		                                  shardManager.getWriteBatch(),
		                                  shardManager.getDirtyShards(),
		                                  shardManager.getColumnFamilyMap());
		keysSet.clear();
		auto res = a->done.getFuture();
		writeThread->post(a);
		return res;
	}

	void flushShard(std::string shardId) { return shardManager.flushShard(shardId); }

	void checkWaiters(const FlowLock& semaphore, int maxWaiters) {
		if (semaphore.waiters() > maxWaiters) {
			++counters.immediateThrottle;
			throw server_overloaded();
		}
	}

	// We don't throttle eager reads and reads to the FF keyspace because FDB struggles when those reads fail.
	// Thus far, they have been low enough volume to not cause an issue.
	static bool shouldThrottle(ReadType type, KeyRef key) {
		return type != ReadType::EAGER && !(key.startsWith(systemKeys.begin));
	}

	ACTOR template <class Action>
	static Future<Optional<Value>> read(Action* action, FlowLock* semaphore, IThreadPool* pool, Counter* counter) {
		state std::unique_ptr<Action> a(action);
		state Optional<Void> slot = wait(timeout(semaphore->take(), SERVER_KNOBS->ROCKSDB_READ_QUEUE_WAIT));
		if (!slot.present()) {
			++(*counter);
			throw server_overloaded();
		}

		state FlowLock::Releaser release(*semaphore);

		auto fut = a->result.getFuture();
		pool->post(a.release());
		Optional<Value> result = wait(fut);

		return result;
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions> options) override {
		auto* shard = shardManager.getDataShard(key);
		if (shard == nullptr || !shard->physicalShard->initialized()) {
			// TODO: read non-exist system key range should not cause an error.
			TraceEvent(SevWarn, "ShardedRocksDB", this->id)
			    .detail("Detail", "Read non-exist key range")
			    .detail("ReadKey", key);
			return Optional<Value>();
		}

		ReadType type = ReadType::NORMAL;
		Optional<UID> debugID;

		if (options.present()) {
			type = options.get().type;
			debugID = options.get().debugID;
		}

		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValueAction(key, shard->physicalShard, type, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValueAction>(key, shard->physicalShard, type, debugID);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<ReadOptions> options) override {
		auto* shard = shardManager.getDataShard(key);
		if (shard == nullptr || !shard->physicalShard->initialized()) {
			// TODO: read non-exist system key range should not cause an error.
			TraceEvent(SevWarn, "ShardedRocksDB", this->id)
			    .detail("Detail", "Read non-exist key range")
			    .detail("ReadKey", key);
			return Optional<Value>();
		}

		ReadType type = ReadType::NORMAL;
		Optional<UID> debugID;

		if (options.present()) {
			type = options.get().type;
			debugID = options.get().debugID;
		}

		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValuePrefixAction(key, maxLength, shard->physicalShard, type, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValuePrefixAction>(key, maxLength, shard->physicalShard, type, debugID);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
	}

	ACTOR static Future<Standalone<RangeResultRef>> read(Reader::ReadRangeAction* action,
	                                                     FlowLock* semaphore,
	                                                     IThreadPool* pool,
	                                                     Counter* counter) {
		state std::unique_ptr<Reader::ReadRangeAction> a(action);
		state Optional<Void> slot = wait(timeout(semaphore->take(), SERVER_KNOBS->ROCKSDB_READ_QUEUE_WAIT));
		if (!slot.present()) {
			++(*counter);
			throw server_overloaded();
		}

		state FlowLock::Releaser release(*semaphore);

		auto fut = a->result.getFuture();
		pool->post(a.release());
		Standalone<RangeResultRef> result = wait(fut);

		return result;
	}

	Future<RangeResult> readRange(KeyRangeRef keys,
	                              int rowLimit,
	                              int byteLimit,
	                              Optional<ReadOptions> options = Optional<ReadOptions>()) override {
		TraceEvent(SevVerbose, "ShardedRocksReadRangeBegin", this->id).detail("Range", keys);
		auto shards = shardManager.getDataShardsByRange(keys);

		ReadType type = ReadType::NORMAL;
		if (options.present()) {
			type = options.get().type;
		}

		if (!shouldThrottle(type, keys.begin)) {
			auto a = new Reader::ReadRangeAction(keys, shards, rowLimit, byteLimit, type);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == ReadType::FETCH) ? numFetchWaiters : numReadWaiters;
		checkWaiters(semaphore, maxWaiters);

		auto a = std::make_unique<Reader::ReadRangeAction>(keys, shards, rowLimit, byteLimit, type);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
	}

	static bool shouldCompactShard(const std::shared_ptr<PhysicalShard> shard,
	                               const uint64_t liveDataSize,
	                               const size_t fileCount) {
		// Rocksdb metadata kEstimateLiveDataSize and cfMetadata.file_count is not deterministic so we don't
		// use it in simulation. We still want to exercise the compact shard functionality for test coverage, so we
		// return shouldCompactShard = true 25% of the time.
		if (g_network->isSimulated()) {
			return deterministicRandom()->randomInt(0, 100) < 25;
		}
		if (fileCount <= 5) {
			return false;
		}
		if (liveDataSize / fileCount >= SERVER_KNOBS->SHARDED_ROCKSDB_AVERAGE_FILE_SIZE) {
			return false;
		}
		return true;
	}

	ACTOR static Future<Void> compactShards(std::shared_ptr<ShardedRocksDBState> rState,
	                                        Future<Void> openFuture,
	                                        ShardManager* shardManager,
	                                        Reference<IThreadPool> thread) {
		try {
			wait(openFuture);
			state std::unordered_map<std::string, std::shared_ptr<PhysicalShard>>* physicalShards =
			    shardManager->getAllShards();
			loop {
				if (rState->closing) {
					break;
				}
				wait(delay(SERVER_KNOBS->SHARDED_ROCKSDB_COMPACTION_ACTOR_DELAY));
				int count = 0;
				double start = now();
				std::vector<std::shared_ptr<PhysicalShard>> shards;
				for (auto& [id, shard] : *physicalShards) {
					if (count > SERVER_KNOBS->SHARDED_ROCKSDB_COMPACTION_SHARD_LIMIT) {
						break;
					}
					if (!shard->initialized() || shard->deletePending) {
						continue;
					}
					if (shard->lastCompactionTime > 0.0 &&
					    start - shard->lastCompactionTime < SERVER_KNOBS->SHARDED_ROCKSDB_COMPACTION_PERIOD) {
						continue;
					}

					uint64_t liveDataSize = 0;
					ASSERT(shard->db->GetIntProperty(
					    shard->cf, rocksdb::DB::Properties::kEstimateLiveDataSize, &liveDataSize));
					rocksdb::ColumnFamilyMetaData cfMetadata;
					shard->db->GetColumnFamilyMetaData(shard->cf, &cfMetadata);

					if (!shouldCompactShard(shard, liveDataSize, cfMetadata.file_count)) {
						continue;
					}

					shards.push_back(shard);

					TraceEvent("CompactionScheduled")
					    .detail("ShardId", id)
					    .detail("NumFiles", cfMetadata.file_count)
					    .detail("ShardSize", liveDataSize);
					++count;
				}

				if (shards.size() > 0) {
					auto a = new CompactionWorker::CompactShardsAction(shards, shardManager->getMetaDataShard());
					auto res = a->done.getFuture();
					thread->post(a);
					wait(res);
				} else {
					TraceEvent("CompactionSkipped").detail("Reason", "NoCandidate");
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "ShardedRocksDBCompactionActorError").errorUnsuppressed(e);
			}
		}
		return Void();
	}
	ACTOR static Future<Void> emptyShardCleaner(std::shared_ptr<ShardedRocksDBState> rState,
	                                            Future<Void> openFuture,
	                                            ShardManager* shardManager,
	                                            Reference<IThreadPool> writeThread) {
		state double cleanUpDelay = SERVER_KNOBS->ROCKSDB_PHYSICAL_SHARD_CLEAN_UP_DELAY;
		state double cleanUpPeriod = cleanUpDelay * 2;
		try {
			wait(openFuture);
			loop {
				wait(delay(cleanUpPeriod));
				if (rState->closing) {
					break;
				}
				auto shards = shardManager->getPendingDeletionShards(cleanUpDelay);
				if (shards.size() > 0) {
					auto a = new Writer::RemoveShardAction(shards, shardManager->getMetaDataShard());
					Future<Void> f = a->done.getFuture();
					writeThread->post(a);
					TraceEvent(SevInfo, "ShardedRocksDB").detail("DeleteEmptyShards", shards.size());
					wait(f);
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "DeleteEmptyShardsError").errorUnsuppressed(e);
			}
		}
		return Void();
	}

	StorageBytes getStorageBytes() const override {
		uint64_t live = 0;
		ASSERT(shardManager.getDb()->GetAggregatedIntProperty(rocksdb::DB::Properties::kLiveSstFilesSize, &live));

		int64_t free;
		int64_t total;
		g_network->getDiskBytes(path, free, total);

		// Rocksdb metadata kLiveSstFilesSize is not deterministic so don't rely on it for simulation. Instead, we pick
		// a sane value that is deterministically random.
		if (g_network->isSimulated()) {
			live = (total - free) * deterministicRandom()->random01();
		}

		return StorageBytes(free, total, live, free);
	}

	Future<CheckpointMetaData> checkpoint(const CheckpointRequest& request) override {
		// ShardedRocks with checkpoint is known to be non-deterministic
		// so setting noUnseed=true. See https://github.com/apple/foundationdb/pull/11841
		// for more context.
		if (g_network->isSimulated() && !noUnseed) {
			noUnseed = true;
		}
		auto a = new Writer::CheckpointAction(&shardManager, request);

		auto res = a->reply.getFuture();
		writeThread->post(a);
		return res;
	}

	Future<Void> restore(const std::string& shardId,
	                     const std::vector<KeyRange>& ranges,
	                     const std::vector<CheckpointMetaData>& checkpoints) override {
		return doRestore(this, shardId, ranges, checkpoints);
	}

	std::vector<std::string> removeRange(KeyRangeRef range) override { return shardManager.removeRange(range); }

	void persistRangeMapping(KeyRangeRef range, bool isAdd) override {
		return shardManager.persistRangeMapping(range, isAdd);
	}

	// Used for debugging shard mapping issue.
	std::vector<std::pair<KeyRange, std::string>> getDataMapping() { return shardManager.getDataMapping(); }

	Future<EncryptionAtRestMode> encryptionMode() override {
		return EncryptionAtRestMode(EncryptionAtRestMode::DISABLED);
	}

	CoalescedKeyRangeMap<std::string> getExistingRanges() override { return shardManager.getExistingRanges(); }

	void logRecentRocksDBBackgroundWorkStats(UID ssId, std::string logReason) override {
		return eventListener->logRecentRocksDBBackgroundWorkStats(ssId, logReason);
	}

	std::shared_ptr<ShardedRocksDBState> rState;
	rocksdb::DBOptions dbOptions;
	std::shared_ptr<RocksDBErrorListener> errorListener;
	std::shared_ptr<RocksDBEventListener> eventListener;
	std::shared_ptr<IteratorPool> iteratorPool;
	ShardManager shardManager;
	std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
	std::string path;
	UID id;
	std::set<Key> keysSet;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> compactionThread;
	Reference<IThreadPool> readThreads;
	Future<Void> errorFuture;
	Promise<Void> closePromise;
	Future<Void> openFuture;
	Optional<Future<Void>> metrics;
	FlowLock readSemaphore;
	int numReadWaiters;
	FlowLock fetchSemaphore;
	int numFetchWaiters;
	Counters counters;
	Future<Void> compactionJob;
	Future<Void> refreshHolder;
	Future<Void> refreshRocksDBBackgroundWorkHolder;
	Future<Void> cleanUpJob;
	Future<Void> counterLogger;
};

ACTOR Future<Void> testCheckpointRestore(IKeyValueStore* kvStore, std::vector<KeyRange> ranges) {
	state std::string checkpointDir = "checkpoint" + deterministicRandom()->randomAlphaNumeric(5);
	platform::eraseDirectoryRecursive(checkpointDir);
	CheckpointRequest request(
	    latestVersion, ranges, DataMoveRocksCF, deterministicRandom()->randomUniqueID(), checkpointDir);
	state CheckpointMetaData checkpoint = wait(kvStore->checkpoint(request));
	RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(checkpoint);

	TraceEvent(SevDebug, "ShardedRocksCheckpointTest")
	    .detail("Checkpoint", checkpoint.toString())
	    .detail("ColumnFamily", rocksCF.toString());

	state std::string rocksDBRestoreDir = "sharded-rocks-restore" + deterministicRandom()->randomAlphaNumeric(5);
	platform::eraseDirectoryRecursive(rocksDBRestoreDir);
	state IKeyValueStore* restoreKv = keyValueStoreShardedRocksDB(
	    rocksDBRestoreDir, deterministicRandom()->randomUniqueID(), KeyValueStoreType::SSD_SHARDED_ROCKSDB);
	wait(restoreKv->init());
	try {
		const std::string shardId = "restoredShard";
		wait(restoreKv->restore(shardId, ranges, { checkpoint }));
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "TestRestoreCheckpointError")
		    .errorUnsuppressed(e)
		    .detail("Checkpoint", checkpoint.toString());
		throw;
	}

	state int i = 0;
	for (; i < ranges.size(); ++i) {
		state RangeResult restoreResult = wait(restoreKv->readRange(ranges[i]));
		RangeResult result = wait(kvStore->readRange(ranges[i]));
		ASSERT(!restoreResult.more && !result.more);
		ASSERT(restoreResult.size() == result.size());
		for (int i = 0; i < result.size(); ++i) {
			TraceEvent(SevDebug, "ReadKeyValueFromRestoredRocks")
			    .detail("Key", result[i].key)
			    .detail("Value", result[i].value)
			    .detail("RestoreKey", restoreResult[i].key)
			    .detail("RestoreValue", restoreResult[i].value);
			ASSERT(result[i].key == restoreResult[i].key);
			ASSERT(result[i].value == restoreResult[i].value);
		}
	}

	Future<Void> restoreKvClose = restoreKv->onClosed();
	restoreKv->close();
	wait(restoreKvClose);
	return Void();
}
} // namespace

#endif // WITH_ROCKSDB

IKeyValueStore* keyValueStoreShardedRocksDB(std::string const& path,
                                            UID logID,
                                            KeyValueStoreType storeType,
                                            bool checkChecksums,
                                            bool checkIntegrity) {
#ifdef WITH_ROCKSDB
	return new ShardedRocksDBKeyValueStore(path, logID);
#else
	TraceEvent(SevError, "ShardedRocksDBEngineInitFailure").detail("Reason", "Built without RocksDB");
	ASSERT(false);
	return nullptr;
#endif // WITH_ROCKSDB
}

#ifdef WITH_ROCKSDB
#include "flow/UnitTest.h"

namespace {
TEST_CASE("noSim/ShardedRocksDB/Initialization") {
	state const std::string rocksDBTestDir = "sharded-rocksdb-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	Future<Void> closed = kvStore->onClosed();
	kvStore->dispose();
	wait(closed);
	ASSERT(!directoryExists(rocksDBTestDir));
	return Void();
}

TEST_CASE("noSim/ShardedRocksDB/SingleShardRead") {
	state const std::string rocksDBTestDir = "sharded-rocksdb-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	KeyRangeRef range("a"_sr, "b"_sr);
	wait(kvStore->addRange(range, "shard-1"));

	kvStore->set({ "a"_sr, "foo"_sr });
	kvStore->set({ "ac"_sr, "bar"_sr });
	wait(kvStore->commit(false));

	Optional<Value> val = wait(kvStore->readValue("a"_sr));
	ASSERT(Optional<Value>("foo"_sr) == val);
	{
		Optional<Value> val = wait(kvStore->readValue("ac"_sr));
		ASSERT(Optional<Value>("bar"_sr) == val);
	}

	Future<Void> closed = kvStore->onClosed();
	kvStore->dispose();
	wait(closed);
	ASSERT(!directoryExists(rocksDBTestDir));
	return Void();
}

TEST_CASE("noSim/ShardedRocksDB/RangeOps") {
	state std::string rocksDBTestDir = "sharded-rocksdb-kvs-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	std::vector<Future<Void>> addRangeFutures;
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("0"_sr, "3"_sr), "shard-1"));
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("4"_sr, "7"_sr), "shard-2"));

	wait(waitForAll(addRangeFutures));

	kvStore->persistRangeMapping(KeyRangeRef("0"_sr, "7"_sr), true);

	// write to shard 1
	state RangeResult expectedRows;
	for (int i = 0; i < 30; ++i) {
		std::string key = format("%02d", i);
		std::string value = std::to_string(i);
		kvStore->set({ key, value });
		expectedRows.push_back_deep(expectedRows.arena(), { key, value });
	}

	// write to shard 2
	for (int i = 40; i < 70; ++i) {
		std::string key = format("%02d", i);
		std::string value = std::to_string(i);
		kvStore->set({ key, value });
		expectedRows.push_back_deep(expectedRows.arena(), { key, value });
	}

	wait(kvStore->commit(false));
	Future<Void> closed = kvStore->onClosed();
	kvStore->close();
	wait(closed);
	kvStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	// Point read
	state int i = 0;
	for (i = 0; i < expectedRows.size(); ++i) {
		Optional<Value> val = wait(kvStore->readValue(expectedRows[i].key));
		ASSERT(val == Optional<Value>(expectedRows[i].value));
	}

	// Range read
	// Read forward full range.
	RangeResult result = wait(kvStore->readRange(KeyRangeRef("0"_sr, ":"_sr), 1000, 10000));
	ASSERT_EQ(result.size(), expectedRows.size());
	for (int i = 0; i < expectedRows.size(); ++i) {
		ASSERT(result[i] == expectedRows[i]);
	}

	// Read backward full range.
	{
		RangeResult result = wait(kvStore->readRange(KeyRangeRef("0"_sr, ":"_sr), -1000, 10000));
		ASSERT_EQ(result.size(), expectedRows.size());
		for (int i = 0; i < expectedRows.size(); ++i) {
			ASSERT(result[i] == expectedRows[59 - i]);
		}
	}

	// Forward with row limit.
	{
		RangeResult result = wait(kvStore->readRange(KeyRangeRef("2"_sr, "6"_sr), 10, 10000));
		ASSERT_EQ(result.size(), 10);
		for (int i = 0; i < 10; ++i) {
			ASSERT(result[i] == expectedRows[20 + i]);
		}
	}

	// Add another range on shard-1.
	wait(kvStore->addRange(KeyRangeRef("7"_sr, "9"_sr), "shard-1"));
	kvStore->persistRangeMapping(KeyRangeRef("7"_sr, "9"_sr), true);

	for (i = 70; i < 90; ++i) {
		std::string key = format("%02d", i);
		std::string value = std::to_string(i);
		kvStore->set({ key, value });
		expectedRows.push_back_deep(expectedRows.arena(), { key, value });
	}

	wait(kvStore->commit(false));

	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->close();
		wait(closed);
	}
	kvStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	// Read all values.
	{
		RangeResult result = wait(kvStore->readRange(KeyRangeRef("0"_sr, ":"_sr), 1000, 10000));
		ASSERT_EQ(result.size(), expectedRows.size());
		for (int i = 0; i < expectedRows.size(); ++i) {
			ASSERT(result[i] == expectedRows[i]);
		}
	}

	// Read partial range with row limit
	{
		RangeResult result = wait(kvStore->readRange(KeyRangeRef("5"_sr, ":"_sr), 35, 10000));
		ASSERT_EQ(result.size(), 35);
		for (int i = 0; i < result.size(); ++i) {
			ASSERT(result[i] == expectedRows[40 + i]);
		}
	}

	// Clear a range on a single shard.
	kvStore->clear(KeyRangeRef("40"_sr, "45"_sr));
	wait(kvStore->commit(false));

	{
		RangeResult result = wait(kvStore->readRange(KeyRangeRef("4"_sr, "5"_sr), 20, 10000));
		ASSERT_EQ(result.size(), 5);
	}

	// Clear a single value.
	kvStore->clear(KeyRangeRef("01"_sr, keyAfter("01"_sr)));
	wait(kvStore->commit(false));

	Optional<Value> val = wait(kvStore->readValue("01"_sr));
	ASSERT(!val.present());

	// Clear a range spanning on multiple shards.
	kvStore->clear(KeyRangeRef("1"_sr, "8"_sr));
	wait(kvStore->commit(false));

	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->close();
		wait(closed);
	}
	kvStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	{
		RangeResult result = wait(kvStore->readRange(KeyRangeRef("1"_sr, "8"_sr), 1000, 10000));
		ASSERT_EQ(result.size(), 0);
	}

	{
		RangeResult result = wait(kvStore->readRange(KeyRangeRef("0"_sr, ":"_sr), 1000, 10000));
		ASSERT_EQ(result.size(), 19);
	}

	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->dispose();
		wait(closed);
	}
	ASSERT(!directoryExists(rocksDBTestDir));
	return Void();
}

TEST_CASE("noSim/ShardedRocksDB/ShardOps") {
	state std::string rocksDBTestDir = "sharded-rocksdb-kvs-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state ShardedRocksDBKeyValueStore* rocksdbStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	state IKeyValueStore* kvStore = rocksdbStore;
	wait(kvStore->init());

	// Add some ranges.
	{
		std::vector<Future<Void>> addRangeFutures;
		addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("a"_sr, "c"_sr), "shard-1"));
		addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("c"_sr, "f"_sr), "shard-2"));

		wait(waitForAll(addRangeFutures));
	}

	{
		std::vector<Future<Void>> addRangeFutures;
		addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("x"_sr, "z"_sr), "shard-1"));
		addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("l"_sr, "n"_sr), "shard-3"));

		wait(waitForAll(addRangeFutures));
	}

	// Remove single range.
	std::vector<std::string> shardsToCleanUp;
	auto shardIds = kvStore->removeRange(KeyRangeRef("b"_sr, "c"_sr));
	// Remove range didn't create empty shard.
	ASSERT_EQ(shardIds.size(), 0);

	// Remove range spanning on multiple shards.
	shardIds = kvStore->removeRange(KeyRangeRef("c"_sr, "m"_sr));
	sort(shardIds.begin(), shardIds.end());
	int count = std::unique(shardIds.begin(), shardIds.end()) - shardIds.begin();
	ASSERT_EQ(count, 1);
	ASSERT(shardIds[0] == "shard-2");

	// Add more ranges.
	std::vector<Future<Void>> addRangeFutures;
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("b"_sr, "g"_sr), "shard-1"));
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("l"_sr, "m"_sr), "shard-2"));
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("u"_sr, "v"_sr), "shard-3"));

	wait(waitForAll(addRangeFutures));

	auto dataMap = rocksdbStore->getDataMapping();
	state std::vector<std::pair<KeyRange, std::string>> mapping;
	mapping.push_back(std::make_pair(KeyRange(KeyRangeRef("a"_sr, "b"_sr)), "shard-1"));
	mapping.push_back(std::make_pair(KeyRange(KeyRangeRef("b"_sr, "g"_sr)), "shard-1"));
	mapping.push_back(std::make_pair(KeyRange(KeyRangeRef("l"_sr, "m"_sr)), "shard-2"));
	mapping.push_back(std::make_pair(KeyRange(KeyRangeRef("m"_sr, "n"_sr)), "shard-3"));
	mapping.push_back(std::make_pair(KeyRange(KeyRangeRef("u"_sr, "v"_sr)), "shard-3"));
	mapping.push_back(std::make_pair(KeyRange(KeyRangeRef("x"_sr, "z"_sr)), "shard-1"));
	mapping.push_back(std::make_pair(specialKeys, DEFAULT_CF_NAME));

	for (auto it = dataMap.begin(); it != dataMap.end(); ++it) {
		std::cout << "Begin " << it->first.begin.toString() << ", End " << it->first.end.toString() << ", id "
		          << it->second << "\n";
	}
	ASSERT(dataMap == mapping);

	kvStore->persistRangeMapping(KeyRangeRef("a"_sr, "z"_sr), true);
	wait(kvStore->commit(false));

	// Restart.
	Future<Void> closed = kvStore->onClosed();
	kvStore->close();
	wait(closed);

	rocksdbStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	kvStore = rocksdbStore;
	wait(kvStore->init());

	{
		auto dataMap = rocksdbStore->getDataMapping();
		for (auto it = dataMap.begin(); it != dataMap.end(); ++it) {
			std::cout << "Begin " << it->first.begin.toString() << ", End " << it->first.end.toString() << ", id "
			          << it->second << "\n";
		}
		ASSERT(dataMap == mapping);
	}

	// Remove all the ranges.
	{
		state std::vector<std::string> shardsToCleanUp = kvStore->removeRange(KeyRangeRef("a"_sr, "z"_sr));
		ASSERT_EQ(shardsToCleanUp.size(), 3);

		// Add another range to shard-2.
		wait(kvStore->addRange(KeyRangeRef("h"_sr, "i"_sr), "shard-2"));
	}
	{
		auto dataMap = rocksdbStore->getDataMapping();
		ASSERT_EQ(dataMap.size(), 2);
		ASSERT(dataMap[0].second == "shard-2");
	}

	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->dispose();
		wait(closed);
	}
	ASSERT(!directoryExists(rocksDBTestDir));
	return Void();
}

TEST_CASE("noSim/ShardedRocksDB/Metadata") {
	state std::string rocksDBTestDir = "sharded-rocksdb-kvs-test-db";
	state Key testSpecialKey = "\xff\xff/TestKey"_sr;
	state Value testSpecialValue = "\xff\xff/TestValue"_sr;
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state ShardedRocksDBKeyValueStore* rocksdbStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	state IKeyValueStore* kvStore = rocksdbStore;
	wait(kvStore->init());

	Optional<Value> val = wait(kvStore->readValue(testSpecialKey));
	ASSERT(!val.present());

	kvStore->set(KeyValueRef(testSpecialKey, testSpecialValue));
	wait(kvStore->commit(false));

	{
		Optional<Value> val = wait(kvStore->readValue(testSpecialKey));
		ASSERT(val.get() == testSpecialValue);
	}

	// Add some ranges.
	std::vector<Future<Void>> addRangeFutures;
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("a"_sr, "c"_sr), "shard-1"));
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("c"_sr, "f"_sr), "shard-2"));
	kvStore->persistRangeMapping(KeyRangeRef("a"_sr, "f"_sr), true);

	wait(waitForAll(addRangeFutures));
	kvStore->set(KeyValueRef("a1"_sr, "foo"_sr));
	kvStore->set(KeyValueRef("d1"_sr, "bar"_sr));
	wait(kvStore->commit(false));

	// Restart.
	Future<Void> closed = kvStore->onClosed();
	kvStore->close();
	wait(closed);
	rocksdbStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	kvStore = rocksdbStore;
	wait(kvStore->init());

	{
		Optional<Value> val = wait(kvStore->readValue(testSpecialKey));
		ASSERT(val.get() == testSpecialValue);
	}

	// Read value back.
	{
		Optional<Value> val = wait(kvStore->readValue("a1"_sr));
		ASSERT(val == Optional<Value>("foo"_sr));
	}
	{
		Optional<Value> val = wait(kvStore->readValue("d1"_sr));
		ASSERT(val == Optional<Value>("bar"_sr));
	}

	// Remove range containing a1.
	kvStore->persistRangeMapping(KeyRangeRef("a"_sr, "b"_sr), false);
	auto shardIds = kvStore->removeRange(KeyRangeRef("a"_sr, "b"_sr));
	wait(kvStore->commit(false));

	// Read a1.
	{
		Optional<Value> val = wait(kvStore->readValue("a1"_sr));
		ASSERT(!val.present());
	}

	// Restart.
	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->close();
		wait(closed);
	}
	rocksdbStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	kvStore = rocksdbStore;
	wait(kvStore->init());

	// Read again.
	{
		Optional<Value> val = wait(kvStore->readValue("a1"_sr));
		ASSERT(!val.present());
	}
	{
		Optional<Value> val = wait(kvStore->readValue("d1"_sr));
		ASSERT(val == Optional<Value>("bar"_sr));
	}

	auto mapping = rocksdbStore->getDataMapping();
	ASSERT(mapping.size() == 3);

	// Remove all the ranges.
	kvStore->removeRange(KeyRangeRef("a"_sr, "f"_sr));
	mapping = rocksdbStore->getDataMapping();
	ASSERT(mapping.size() == 1);

	// Restart.
	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->close();
		wait(closed);
	}
	rocksdbStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	kvStore = rocksdbStore;
	wait(kvStore->init());

	// Because range metadata was not committed, ranges should be restored.
	{
		auto mapping = rocksdbStore->getDataMapping();
		ASSERT(mapping.size() == 3);

		// Remove ranges again.
		kvStore->persistRangeMapping(KeyRangeRef("a"_sr, "f"_sr), false);
		kvStore->removeRange(KeyRangeRef("a"_sr, "f"_sr));

		mapping = rocksdbStore->getDataMapping();
		ASSERT(mapping.size() == 1);

		wait(kvStore->commit(false));
	}

	// Restart.
	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->close();
		wait(closed);
	}

	rocksdbStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	kvStore = rocksdbStore;
	wait(kvStore->init());

	// No range available.
	{
		auto mapping = rocksdbStore->getDataMapping();
		for (auto it = mapping.begin(); it != mapping.end(); ++it) {
			std::cout << "Begin " << it->first.begin.toString() << ", End " << it->first.end.toString() << ", id "
			          << it->second << "\n";
		}
		ASSERT(mapping.size() == 1);
	}

	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->dispose();
		wait(closed);
	}
	ASSERT(!directoryExists(rocksDBTestDir));
	return Void();
}

TEST_CASE("noSim/ShardedRocksDB/CheckpointBasic") {
	state std::string rocksDBTestDir = "sharded-rocks-checkpoint-restore";
	state std::map<Key, Value> kvs({ { "a"_sr, "TestValueA"_sr },
	                                 { "ab"_sr, "TestValueAB"_sr },
	                                 { "ad"_sr, "TestValueAD"_sr },
	                                 { "b"_sr, "TestValueB"_sr },
	                                 { "ba"_sr, "TestValueBA"_sr },
	                                 { "c"_sr, "TestValueC"_sr },
	                                 { "d"_sr, "TestValueD"_sr },
	                                 { "e"_sr, "TestValueE"_sr },
	                                 { "h"_sr, "TestValueH"_sr },
	                                 { "ha"_sr, "TestValueHA"_sr } });
	platform::eraseDirectoryRecursive(rocksDBTestDir);
	state IKeyValueStore* kvStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	// Add some ranges.
	std::vector<Future<Void>> addRangeFutures;
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("a"_sr, "c"_sr), "shard-1"));
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("c"_sr, "f"_sr), "shard-2"));
	addRangeFutures.push_back(kvStore->addRange(KeyRangeRef("h"_sr, "k"_sr), "shard-1"));
	kvStore->persistRangeMapping(KeyRangeRef("a"_sr, "f"_sr), true);
	wait(waitForAll(addRangeFutures) && kvStore->commit(false));

	for (const auto& [k, v] : kvs) {
		kvStore->set(KeyValueRef(k, v));
	}
	wait(kvStore->commit(false));

	state std::string checkpointDir = "checkpoint";
	platform::eraseDirectoryRecursive(checkpointDir);

	// Checkpoint iterator returns only the desired keyrange, i.e., ["ab", "b"].
	CheckpointRequest request(latestVersion,
	                          { KeyRangeRef("a"_sr, "c"_sr), KeyRangeRef("h"_sr, "k"_sr) },
	                          DataMoveRocksCF,
	                          deterministicRandom()->randomUniqueID(),
	                          checkpointDir);
	CheckpointMetaData metaData = wait(kvStore->checkpoint(request));

	state Standalone<StringRef> token = BinaryWriter::toValue(KeyRangeRef("a"_sr, "k"_sr), IncludeVersion());
	state ICheckpointReader* cpReader =
	    newCheckpointReader(metaData, CheckpointAsKeyValues::True, deterministicRandom()->randomUniqueID());
	ASSERT(cpReader != nullptr);
	wait(cpReader->init(token));
	state KeyRange testRange(KeyRangeRef("ab"_sr, "b"_sr));
	state std::unique_ptr<ICheckpointIterator> iter0 = cpReader->getIterator(testRange);
	state int numKeys = 0;
	try {
		loop {
			RangeResult res = wait(iter0->nextBatch(CLIENT_KNOBS->REPLY_BYTE_LIMIT, CLIENT_KNOBS->REPLY_BYTE_LIMIT));
			for (const auto& kv : res) {
				ASSERT(testRange.contains(kv.key));
				ASSERT(kvs[kv.key] == kv.value);
				++numKeys;
			}
		}
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream);
		ASSERT(numKeys == 2);
	}

	testRange = KeyRangeRef("a"_sr, "k"_sr);
	state std::unique_ptr<ICheckpointIterator> iter1 = cpReader->getIterator(testRange);
	try {
		numKeys = 0;
		loop {
			RangeResult res = wait(iter1->nextBatch(CLIENT_KNOBS->REPLY_BYTE_LIMIT, CLIENT_KNOBS->REPLY_BYTE_LIMIT));
			for (const auto& kv : res) {
				ASSERT(testRange.contains(kv.key));
				ASSERT(kvs[kv.key] == kv.value);
				++numKeys;
			}
		}
	} catch (Error& e) {
		ASSERT(e.code() == error_code_end_of_stream);
		ASSERT(numKeys == 7);
	}

	iter0.reset();
	iter1.reset();
	ASSERT(!cpReader->inUse());
	TraceEvent(SevDebug, "ShardedRocksCheckpointReaaderTested");
	std::vector<Future<Void>> closes;
	closes.push_back(cpReader->close());
	closes.push_back(kvStore->onClosed());
	kvStore->dispose();
	wait(waitForAll(closes));

	platform::eraseDirectoryRecursive(rocksDBTestDir);
	platform::eraseDirectoryRecursive(checkpointDir);

	return Void();
}

TEST_CASE("noSim/ShardedRocksDB/CheckpointRestore") {
	state std::string rocksDBTestDir = "sharded-rocks-checkpoint" + deterministicRandom()->randomAlphaNumeric(5);
	state std::map<Key, Value> kvs({ { "ab"_sr, "TestValueAB"_sr },
	                                 { "ad"_sr, "TestValueAD"_sr },
	                                 { "b"_sr, "TestValueB"_sr },
	                                 { "ba"_sr, "TestValueBA"_sr },
	                                 { "c"_sr, "TestValueC"_sr },
	                                 { "d"_sr, "TestValueD"_sr },
	                                 { "e"_sr, "TestValueE"_sr },
	                                 { "h"_sr, "TestValueH"_sr },
	                                 { "ha"_sr, "TestValueHA"_sr } });
	platform::eraseDirectoryRecursive(rocksDBTestDir);
	state IKeyValueStore* kvStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	state std::string shardId = "shard_1";
	state std::string emptyShardId = "shard_2";
	state KeyRangeRef rangeK(""_sr, "k"_sr);
	state KeyRangeRef rangeKz("z1"_sr, "z3"_sr);
	// // Add some ranges.
	std::vector<Future<Void>> addRangeFutures;
	addRangeFutures.push_back(kvStore->addRange(rangeK, shardId));
	addRangeFutures.push_back(kvStore->addRange(rangeKz, emptyShardId));
	kvStore->persistRangeMapping(rangeK, true);
	kvStore->persistRangeMapping(rangeKz, true);
	wait(waitForAll(addRangeFutures));
	wait(kvStore->commit(false));

	for (const auto& [k, v] : kvs) {
		kvStore->set(KeyValueRef(k, v));
	}
	wait(kvStore->commit(false));
	kvStore->clear(KeyRangeRef(""_sr, "z"_sr));
	wait(kvStore->commit(false));

	state Error err;
	try {
		wait(testCheckpointRestore(kvStore, { rangeK }));
	} catch (Error& e) {
		TraceEvent(SevError, "TestCheckpointRestoreError").errorUnsuppressed(e);
		err = e;
	}
	// This will fail once RocksDB is upgraded to 8.1.
	// ASSERT(err.code() == error_code_failed_to_restore_checkpoint);

	try {
		wait(testCheckpointRestore(kvStore, { rangeKz }));
	} catch (Error& e) {
		TraceEvent("TestCheckpointRestoreError").errorUnsuppressed(e);
		err = e;
	}

	std::vector<Future<Void>> closes;
	closes.push_back(kvStore->onClosed());
	kvStore->close();
	wait(waitForAll(closes));

	platform::eraseDirectoryRecursive(rocksDBTestDir);

	return Void();
}

TEST_CASE("noSim/ShardedRocksDB/RocksDBSstFileWriter") {
	state std::string localFile = "rocksdb-sst-file-dump.sst";
	state std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
	// Write nothing to sst file
	sstWriter->open(localFile);
	bool anyFileCreated = sstWriter->finish();
	ASSERT(!anyFileCreated);
	// Write kvs1 to sst file
	state std::map<Key, Value> kvs1({ { "a"_sr, "1"_sr },
	                                  { "ab"_sr, "12"_sr },
	                                  { "ad"_sr, "14"_sr },
	                                  { "b"_sr, "2"_sr },
	                                  { "ba"_sr, "21"_sr },
	                                  { "c"_sr, "3"_sr },
	                                  { "d"_sr, "4"_sr },
	                                  { "e"_sr, "5"_sr },
	                                  { "h"_sr, "8"_sr },
	                                  { "ha"_sr, "81"_sr } });
	sstWriter = newRocksDBSstFileWriter();
	sstWriter->open(localFile);
	for (const auto& [key, value] : kvs1) {
		sstWriter->write(key, value);
	}
	anyFileCreated = sstWriter->finish();
	ASSERT(anyFileCreated);
	// Write kvs2 to the same sst file where kvs2 keys are different from kvs1
	state std::map<Key, Value> kvs2({ { "fa"_sr, "61"_sr },
	                                  { "fab"_sr, "612"_sr },
	                                  { "fad"_sr, "614"_sr },
	                                  { "fb"_sr, "62"_sr },
	                                  { "fba"_sr, "621"_sr },
	                                  { "fc"_sr, "63"_sr },
	                                  { "fd"_sr, "64"_sr },
	                                  { "fe"_sr, "65"_sr },
	                                  { "fh"_sr, "68"_sr },
	                                  { "fha"_sr, "681"_sr } });
	sstWriter->open(localFile);
	for (const auto& [key, value] : kvs2) {
		sstWriter->write(key, value);
	}
	anyFileCreated = sstWriter->finish();
	ASSERT(anyFileCreated);
	// Write kvs3 to the same sst file where kvs3 modifies values of kvs2
	state std::map<Key, Value> kvs3({ { "fa"_sr, "1"_sr },
	                                  { "fab"_sr, "12"_sr },
	                                  { "fad"_sr, "14"_sr },
	                                  { "fb"_sr, "2"_sr },
	                                  { "fba"_sr, "21"_sr },
	                                  { "fc"_sr, "3"_sr },
	                                  { "fd"_sr, "4"_sr },
	                                  { "fe"_sr, "5"_sr },
	                                  { "fh"_sr, "8"_sr },
	                                  { "fha"_sr, "81"_sr } });
	sstWriter->open(localFile);
	for (const auto& [key, value] : kvs3) {
		sstWriter->write(key, value);
	}
	anyFileCreated = sstWriter->finish();
	ASSERT(anyFileCreated);
	// Check: sst only contains kv of kvs3
	rocksdb::Status status;
	rocksdb::IngestExternalFileOptions ingestOptions;
	rocksdb::DB* db;
	rocksdb::Options options;
	options.create_if_missing = true;
	status = rocksdb::DB::Open(options, "testdb", &db);
	ASSERT(status.ok());
	status = db->IngestExternalFile({ localFile }, ingestOptions);
	ASSERT(status.ok());
	std::string value;
	for (const auto& [key, targetValue] : kvs1) {
		status = db->Get(rocksdb::ReadOptions(), key.toString(), &value);
		ASSERT(status.IsNotFound());
	}
	for (const auto& [key, targetValue] : kvs2) {
		status = db->Get(rocksdb::ReadOptions(), key.toString(), &value);
		ASSERT(value != targetValue.toString());
	}
	for (const auto& [key, targetValue] : kvs3) {
		status = db->Get(rocksdb::ReadOptions(), key.toString(), &value);
		ASSERT(status.ok());
		ASSERT(value == targetValue.toString());
	}
	delete db;
	return Void();
}

TEST_CASE("perf/ShardedRocksDB/RangeClearSysKey") {
	state int deleteCount = params.getInt("deleteCount").orDefault(20000);
	std::cout << "delete count: " << deleteCount << "\n";

	state std::string rocksDBTestDir = "sharded-rocksdb-perf-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	state KeyRef shardPrefix = "\xffprefix/"_sr;
	wait(kvStore->addRange(prefixRange(shardPrefix), "shard-1"));
	kvStore->persistRangeMapping(prefixRange(shardPrefix), true);
	state int i = 0;
	state std::string key1;
	state std::string key2;
	for (; i < deleteCount; ++i) {
		key1 = format("\xffprefix/%d", i);
		key2 = format("\xffprefix/%d", i + 1);

		kvStore->set({ key2, std::to_string(i) });
		kvStore->clear({ KeyRangeRef(shardPrefix, key1) });
		wait(kvStore->commit(false));
	}

	std::cout << "start flush\n";
	auto rocksdb = (ShardedRocksDBKeyValueStore*)kvStore;
	rocksdb->flushShard("shard-1");
	std::cout << "flush complete\n";

	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->close();
		wait(closed);
	}

	kvStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	std::cout << "Restarted.\n";
	i = 0;

	for (; i < deleteCount; ++i) {
		key1 = format("\xffprefix/%d", i);
		key2 = format("\xffprefix/%d", i + 1);

		kvStore->set({ key2, std::to_string(i) });
		RangeResult result = wait(kvStore->readRange(KeyRangeRef(shardPrefix, key1), 10000, 10000));
		kvStore->clear({ KeyRangeRef(shardPrefix, key1) });
		wait(kvStore->commit(false));
		if (i % 100 == 0) {
			std::cout << "Commit: " << i << "\n";
		}
	}
	Future<Void> closed = kvStore->onClosed();
	kvStore->dispose();
	wait(closed);
	ASSERT(!directoryExists(rocksDBTestDir));
	return Void();
}

TEST_CASE("perf/ShardedRocksDB/RangeClearUserKey") {
	state int deleteCount = params.getInt("deleteCount").orDefault(20000);
	std::cout << "delete count: " << deleteCount << "\n";

	state std::string rocksDBTestDir = "sharded-rocksdb-perf-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore =
	    new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	state KeyRef shardPrefix = "prefix/"_sr;
	wait(kvStore->addRange(prefixRange(shardPrefix), "shard-1"));
	kvStore->persistRangeMapping(prefixRange(shardPrefix), true);
	state int i = 0;
	state std::string key1;
	state std::string key2;
	for (; i < deleteCount; ++i) {
		key1 = format("prefix/%d", i);
		key2 = format("prefix/%d", i + 1);

		kvStore->set({ key2, std::to_string(i) });
		kvStore->clear({ KeyRangeRef(shardPrefix, key1) });
		wait(kvStore->commit(false));
	}

	std::cout << "start flush\n";
	auto rocksdb = (ShardedRocksDBKeyValueStore*)kvStore;
	rocksdb->flushShard("shard-1");
	std::cout << "flush complete\n";

	{
		Future<Void> closed = kvStore->onClosed();
		kvStore->close();
		wait(closed);
	}

	kvStore = new ShardedRocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	std::cout << "Restarted.\n";
	i = 0;
	for (; i < deleteCount; ++i) {
		key1 = format("prefix/%d", i);
		key2 = format("prefix/%d", i + 1);

		kvStore->set({ key2, std::to_string(i) });
		RangeResult result = wait(kvStore->readRange(KeyRangeRef(shardPrefix, key1), 10000, 10000));
		kvStore->clear({ KeyRangeRef(shardPrefix, key1) });
		wait(kvStore->commit(false));
		if (i % 100 == 0) {
			std::cout << "Commit: " << i << "\n";
		}
	}
	Future<Void> closed = kvStore->onClosed();
	kvStore->dispose();
	wait(closed);
	ASSERT(!directoryExists(rocksDBTestDir));
	return Void();
}
} // namespace

#endif // WITH_ROCKSDB
