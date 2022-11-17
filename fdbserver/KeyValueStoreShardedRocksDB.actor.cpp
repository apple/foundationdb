#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "flow/flow.h"
#include "flow/serialize.h"
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/listener.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/table_properties_collectors.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/c.h>
#include <rocksdb/version.h>
#if defined __has_include
#if __has_include(<liburing.h>)
#include <liburing.h>
#endif
#endif
#include "fdbclient/SystemData.h"
#include "fdbserver/CoroFlow.h"
#include "flow/flow.h"
#include "flow/IThreadPool.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Histogram.h"

#include <memory>
#include <tuple>
#include <vector>

#endif // SSD_ROCKSDB_EXPERIMENTAL

#include "fdbserver/IKeyValueStore.h"
#include "flow/actorcompiler.h" // has to be last include

#ifdef SSD_ROCKSDB_EXPERIMENTAL

// Enforcing rocksdb version to be 6.27.3 or greater.
static_assert(ROCKSDB_MAJOR >= 6, "Unsupported rocksdb version. Update the rocksdb to 6.27.3 version");
static_assert(ROCKSDB_MAJOR == 6 ? ROCKSDB_MINOR >= 27 : true,
              "Unsupported rocksdb version. Update the rocksdb to 6.27.3 version");
static_assert((ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR == 27) ? ROCKSDB_PATCH >= 3 : true,
              "Unsupported rocksdb version. Update the rocksdb to 6.27.3 version");

const std::string rocksDataFolderSuffix = "-data";
const std::string METADATA_SHARD_ID = "kvs-metadata";
const KeyRef shardMappingPrefix("\xff\xff/ShardMapping/"_sr);
// TODO: move constants to a header file.
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

namespace {
struct PhysicalShard;
struct DataShard;
struct ReadIterator;
struct ShardedRocksDBKeyValueStore;

using rocksdb::BackgroundErrorReason;

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

// Background error handling is tested with Chaos test.
// TODO: Test background error in simulation. RocksDB doesn't use flow IO in simulation, which limits our ability to
// inject IO errors. We could implement rocksdb::FileSystem using flow IO to unblock simulation. Also, trace event is
// not available on background threads because trace event requires setting up special thread locals. Using trace event
// could potentially cause segmentation fault.
class RocksDBErrorListener : public rocksdb::EventListener {
public:
	RocksDBErrorListener(){};
	void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status* bg_error) override {
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
			errorPromise.sendError(io_error());
		} else if (bg_error->IsCorruption()) {
			errorPromise.sendError(file_corrupt());
		} else {
			errorPromise.sendError(unknown_error());
		}
	}
	Future<Void> getFuture() {
		std::unique_lock<std::mutex> lock(mutex);
		return errorPromise.getFuture();
	}
	~RocksDBErrorListener() {
		std::unique_lock<std::mutex> lock(mutex);
		if (!errorPromise.isValid())
			return;
		errorPromise.send(Never());
	}

private:
	ThreadReturnPromise<Void> errorPromise;
	std::mutex mutex;
};

// Encapsulation of shared states.
struct ShardedRocksDBState {
	bool closing = false;
};

std::shared_ptr<rocksdb::Cache> rocksdb_block_cache = nullptr;

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
	e.detail("Error", status.ToString()).detail("Method", method).detail("ShardedRocksDBSeverity", status.severity());
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
	e.detail("Name", name).detail("Action", ShardOpToString(op));
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
	e.detail("Name", name).detail("Action", ShardOpToString(op)).detail("Begin", range.begin).detail("End", range.end);
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

rocksdb::ColumnFamilyOptions getCFOptions() {
	rocksdb::ColumnFamilyOptions options;
	options.level_compaction_dynamic_level_bytes = SERVER_KNOBS->ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES;
	options.OptimizeLevelStyleCompaction(SERVER_KNOBS->ROCKSDB_MEMTABLE_BYTES);
	if (SERVER_KNOBS->ROCKSDB_PERIODIC_COMPACTION_SECONDS > 0) {
		options.periodic_compaction_seconds = SERVER_KNOBS->ROCKSDB_PERIODIC_COMPACTION_SECONDS;
	}
	// Compact sstables when there's too much deleted stuff.
	options.table_properties_collector_factories = { rocksdb::NewCompactOnDeletionCollectorFactory(128, 1) };

	rocksdb::BlockBasedTableOptions bbOpts;
	// TODO: Add a knob for the block cache size. (Default is 8 MB)
	if (SERVER_KNOBS->ROCKSDB_PREFIX_LEN > 0) {
		// Prefix blooms are used during Seek.
		options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(SERVER_KNOBS->ROCKSDB_PREFIX_LEN));

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

	if (rocksdb_block_cache == nullptr && SERVER_KNOBS->ROCKSDB_BLOCK_CACHE_SIZE > 0) {
		rocksdb_block_cache = rocksdb::NewLRUCache(SERVER_KNOBS->ROCKSDB_BLOCK_CACHE_SIZE);
	}
	bbOpts.block_cache = rocksdb_block_cache;

	options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbOpts));

	return options;
}

rocksdb::Options getOptions() {
	rocksdb::Options options({}, getCFOptions());
	options.avoid_unnecessary_blocking_io = true;
	options.create_if_missing = true;
	if (SERVER_KNOBS->ROCKSDB_BACKGROUND_PARALLELISM > 0) {
		options.IncreaseParallelism(SERVER_KNOBS->ROCKSDB_BACKGROUND_PARALLELISM);
	}

	options.delete_obsolete_files_period_micros = SERVER_KNOBS->ROCKSDB_DELETE_OBSOLETE_FILE_PERIOD * 1000000;
	options.max_total_wal_size = SERVER_KNOBS->ROCKSDB_MAX_TOTAL_WAL_SIZE;
	options.max_subcompactions = SERVER_KNOBS->ROCKSDB_MAX_SUBCOMPACTIONS;
	options.max_background_jobs = SERVER_KNOBS->ROCKSDB_MAX_BACKGROUND_JOBS;

	options.db_write_buffer_size = SERVER_KNOBS->ROCKSDB_WRITE_BUFFER_SIZE;
	options.write_buffer_size = SERVER_KNOBS->ROCKSDB_CF_WRITE_BUFFER_SIZE;
	options.statistics = rocksdb::CreateDBStatistics();
	options.statistics->set_stats_level(rocksdb::kExceptHistogramOrTimers);
	options.db_log_dir = SERVER_KNOBS->LOG_DIRECTORY;
	return options;
}

// Set some useful defaults desired for all reads.
rocksdb::ReadOptions getReadOptions() {
	rocksdb::ReadOptions options;
	options.background_purge_on_iterator_cleanup = true;
	return options;
}

struct ReadIterator {
	uint64_t index; // incrementing counter to uniquely identify read iterator.
	bool inUse;
	std::shared_ptr<rocksdb::Iterator> iter;
	double creationTime;
	KeyRange keyRange;
	std::shared_ptr<rocksdb::Slice> beginSlice, endSlice;

	ReadIterator(rocksdb::ColumnFamilyHandle* cf, uint64_t index, rocksdb::DB* db, const rocksdb::ReadOptions& options)
	  : index(index), inUse(true), creationTime(now()), iter(db->NewIterator(options, cf)) {}
	ReadIterator(rocksdb::ColumnFamilyHandle* cf, uint64_t index, rocksdb::DB* db, const KeyRange& range)
	  : index(index), inUse(true), creationTime(now()), keyRange(range) {
		auto options = getReadOptions();
		beginSlice = std::shared_ptr<rocksdb::Slice>(new rocksdb::Slice(toSlice(keyRange.begin)));
		options.iterate_lower_bound = beginSlice.get();
		endSlice = std::shared_ptr<rocksdb::Slice>(new rocksdb::Slice(toSlice(keyRange.end)));
		options.iterate_upper_bound = endSlice.get();
		iter = std::shared_ptr<rocksdb::Iterator>(db->NewIterator(options, cf));
	}
};

/*
ReadIteratorPool: Collection of iterators. Reuses iterators on non-concurrent multiple read operations,
instead of creating and deleting for every read.

Read: IteratorPool provides an unused iterator if exists or creates and gives a new iterator.
Returns back the iterator after the read is done.

Write: Iterators in the pool are deleted, forcing new iterator creation on next reads. The iterators
which are currently used by the reads can continue using the iterator as it is a shared_ptr. Once
the read is processed, shared_ptr goes out of scope and gets deleted. Eventually the iterator object
gets deleted as the ref count becomes 0.
*/
class ReadIteratorPool {
public:
	ReadIteratorPool(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, const std::string& path)
	  : db(db), cf(cf), index(0), iteratorsReuseCount(0), readRangeOptions(getReadOptions()) {
		ASSERT(db);
		ASSERT(cf);
		readRangeOptions.background_purge_on_iterator_cleanup = true;
		readRangeOptions.auto_prefix_mode = (SERVER_KNOBS->ROCKSDB_PREFIX_LEN > 0);
		TraceEvent(SevVerbose, "ShardedRocksReadIteratorPool")
		    .detail("Path", path)
		    .detail("KnobRocksDBReadRangeReuseIterators", SERVER_KNOBS->ROCKSDB_READ_RANGE_REUSE_ITERATORS)
		    .detail("KnobRocksDBPrefixLen", SERVER_KNOBS->ROCKSDB_PREFIX_LEN);
	}

	// Called on every db commit.
	void update() {
		if (SERVER_KNOBS->ROCKSDB_READ_RANGE_REUSE_ITERATORS) {
			std::lock_guard<std::mutex> lock(mutex);
			iteratorsMap.clear();
		}
	}

	// Called on every read operation.
	ReadIterator getIterator(const KeyRange& range) {
		// Shared iterators are not bounded.
		if (SERVER_KNOBS->ROCKSDB_READ_RANGE_REUSE_ITERATORS) {
			std::lock_guard<std::mutex> lock(mutex);
			for (it = iteratorsMap.begin(); it != iteratorsMap.end(); it++) {
				if (!it->second.inUse) {
					it->second.inUse = true;
					iteratorsReuseCount++;
					return it->second;
				}
			}
			index++;
			ReadIterator iter(cf, index, db, readRangeOptions);
			iteratorsMap.insert({ index, iter });
			return iter;
		} else {
			index++;
			ReadIterator iter(cf, index, db, range);
			return iter;
		}
	}

	// Called on every read operation, after the keys are collected.
	void returnIterator(ReadIterator& iter) {
		if (SERVER_KNOBS->ROCKSDB_READ_RANGE_REUSE_ITERATORS) {
			std::lock_guard<std::mutex> lock(mutex);
			it = iteratorsMap.find(iter.index);
			// iterator found: put the iterator back to the pool(inUse=false).
			// iterator not found: update would have removed the iterator from pool, so nothing to do.
			if (it != iteratorsMap.end()) {
				ASSERT(it->second.inUse);
				it->second.inUse = false;
			}
		}
	}

	// Called for every ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME seconds in a loop.
	void refreshIterators() {
		std::lock_guard<std::mutex> lock(mutex);
		it = iteratorsMap.begin();
		while (it != iteratorsMap.end()) {
			if (now() - it->second.creationTime > SERVER_KNOBS->ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME) {
				it = iteratorsMap.erase(it);
			} else {
				it++;
			}
		}
	}

	uint64_t numReadIteratorsCreated() { return index; }

	uint64_t numTimesReadIteratorsReused() { return iteratorsReuseCount; }

private:
	std::unordered_map<int, ReadIterator> iteratorsMap;
	std::unordered_map<int, ReadIterator>::iterator it;
	rocksdb::DB* db;
	rocksdb::ColumnFamilyHandle* cf;
	rocksdb::ReadOptions readRangeOptions;
	std::mutex mutex;
	// incrementing counter for every new iterator creation, to uniquely identify the iterator in returnIterator().
	uint64_t index;
	uint64_t iteratorsReuseCount;
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
		readIterPool = std::make_shared<ReadIteratorPool>(db, cf, id);
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
		readIterPool = std::make_shared<ReadIteratorPool>(db, cf, id);
		this->isInitialized.store(true);
		return status;
	}

	bool initialized() { return this->isInitialized.load(); }

	void refreshReadIteratorPool() {
		ASSERT(this->readIterPool != nullptr);
		this->readIterPool->refreshIterators();
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
		readIterPool.reset();

		// Deleting default column family is not allowed.
		if (id == "default") {
			return;
		}

		if (deletePending) {
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
	std::shared_ptr<ReadIteratorPool> readIterPool;
	bool deletePending = false;
	std::atomic<bool> isInitialized;
	double deleteTimeSec;
};

int readRangeInDb(PhysicalShard* shard, const KeyRangeRef range, int rowLimit, int byteLimit, RangeResult* result) {
	if (rowLimit == 0 || byteLimit == 0) {
		return 0;
	}

	int accumulatedRows = 0;
	int accumulatedBytes = 0;
	rocksdb::Status s;

	// When using a prefix extractor, ensure that keys are returned in order even if they cross
	// a prefix boundary.
	if (rowLimit >= 0) {
		ReadIterator readIter = shard->readIterPool->getIterator(range);
		auto cursor = readIter.iter;
		cursor->Seek(toSlice(range.begin));
		while (cursor->Valid() && toStringRef(cursor->key()) < range.end) {
			KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
			++accumulatedRows;
			accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
			result->push_back_deep(result->arena(), kv);
			// Calling `cursor->Next()` is potentially expensive, so short-circut here just in case.
			if (result->size() >= rowLimit || accumulatedBytes >= byteLimit) {
				break;
			}
			cursor->Next();
		}
		s = cursor->status();
		shard->readIterPool->returnIterator(readIter);
	} else {
		ReadIterator readIter = shard->readIterPool->getIterator(range);
		auto cursor = readIter.iter;
		cursor->SeekForPrev(toSlice(range.end));
		if (cursor->Valid() && toStringRef(cursor->key()) == range.end) {
			cursor->Prev();
		}
		while (cursor->Valid() && toStringRef(cursor->key()) >= range.begin) {
			KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
			++accumulatedRows;
			accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
			result->push_back_deep(result->arena(), kv);
			// Calling `cursor->Prev()` is potentially expensive, so short-circut here just in case.
			if (result->size() >= -rowLimit || accumulatedBytes >= byteLimit) {
				break;
			}
			cursor->Prev();
		}
		s = cursor->status();
		shard->readIterPool->returnIterator(readIter);
	}

	if (!s.ok()) {
		logRocksDBError(s, "ReadRange");
		// The data writen to the arena is not erased, which will leave RangeResult in a dirty state. The RangeResult
		// should never be returned to user.
		return -1;
	}
	return accumulatedBytes;
}

// Manages physical shards and maintains logical shard mapping.
class ShardManager {
public:
	ShardManager(std::string path, UID logId, const rocksdb::Options& options)
	  : path(path), logId(logId), dbOptions(options), dataShardMap(nullptr, specialKeys.end) {}

	ACTOR static Future<Void> shardMetricsLogger(std::shared_ptr<ShardedRocksDBState> rState,
	                                             Future<Void> openFuture,
	                                             ShardManager* shardManager) {
		state std::unordered_map<std::string, std::shared_ptr<PhysicalShard>>* physicalShards =
		    shardManager->getAllShards();

		try {
			wait(openFuture);
			loop {
				wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
				if (rState->closing) {
					break;
				}
				TraceEvent(SevInfo, "ShardedRocksKVSPhysialShardMetrics")
				    .detail("NumActiveShards", shardManager->numActiveShards())
				    .detail("TotalPhysicalShards", shardManager->numPhysicalShards());

				for (auto& [id, shard] : *physicalShards) {
					if (!shard->initialized()) {
						continue;
					}
					std::string propValue = "";
					ASSERT(shard->db->GetProperty(shard->cf, rocksdb::DB::Properties::kCFStats, &propValue));
					TraceEvent(SevInfo, "PhysicalShardCFStats").detail("ShardId", id).detail("Detail", propValue);

					// Get compression ratio for each level.
					rocksdb::ColumnFamilyMetaData cfMetadata;
					shard->db->GetColumnFamilyMetaData(shard->cf, &cfMetadata);
					TraceEvent e(SevInfo, "PhysicalShardLevelStats");
					e.detail("ShardId", id);
					std::string levelProp;
					for (auto it = cfMetadata.levels.begin(); it != cfMetadata.levels.end(); ++it) {
						std::string propValue = "";
						ASSERT(shard->db->GetProperty(shard->cf,
						                              rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix +
						                                  std::to_string(it->level),
						                              &propValue));
						e.detail("Level" + std::to_string(it->level), std::to_string(it->size) + " " + propValue);
					}
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "ShardedRocksShardMetricsLoggerError").errorUnsuppressed(e);
			}
		}
		return Void();
	}

	rocksdb::Status init() {
		// Open instance.
		TraceEvent(SevInfo, "ShardedRocksShardManagerInitBegin", this->logId).detail("DataPath", path);
		std::vector<std::string> columnFamilies;
		rocksdb::Status status = rocksdb::DB::ListColumnFamilies(dbOptions, path, &columnFamilies);

		std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
		bool foundMetadata = false;
		for (const auto& name : columnFamilies) {
			if (name == METADATA_SHARD_ID) {
				foundMetadata = true;
			}
			descriptors.push_back(rocksdb::ColumnFamilyDescriptor{ name, rocksdb::ColumnFamilyOptions(dbOptions) });
		}

		ASSERT(foundMetadata || descriptors.size() == 0);

		// Add default column family if it's a newly opened database.
		if (descriptors.size() == 0) {
			descriptors.push_back(
			    rocksdb::ColumnFamilyDescriptor{ "default", rocksdb::ColumnFamilyOptions(dbOptions) });
		}

		std::vector<rocksdb::ColumnFamilyHandle*> handles;
		status = rocksdb::DB::Open(dbOptions, path, descriptors, &handles, &db);
		if (!status.ok()) {
			logRocksDBError(status, "Open");
			return status;
		}

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
				TraceEvent(SevVerbose, "ShardedRocksInitPhysicalShard", this->logId).detail("PhysicalShard", shard->id);
			}

			std::set<std::string> unusedShards(columnFamilies.begin(), columnFamilies.end());
			unusedShards.erase(METADATA_SHARD_ID);
			unusedShards.erase("default");

			KeyRange keyRange = prefixRange(shardMappingPrefix);
			while (true) {
				RangeResult metadata;
				const int bytes = readRangeInDb(metadataShard.get(),
				                                keyRange,
				                                std::max(2, SERVER_KNOBS->ROCKSDB_READ_RANGE_ROW_LIMIT),
				                                UINT16_MAX,
				                                &metadata);
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
					    .detail("Name", name);

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

				// Read from the current last key since the shard begining with it hasn't been processed.
				if (metadata.size() == 1 && metadata.back().value.toString().empty()) {
					// Should not happen, just being paranoid.
					keyRange = KeyRangeRef(keyAfter(metadata.back().key), keyRange.end);
				} else {
					keyRange = KeyRangeRef(metadata.back().key, keyRange.end);
				}
			}

			for (const auto& name : unusedShards) {
				TraceEvent(SevDebug, "UnusedShardName", logId).detail("Name", name);
				auto it = physicalShards.find(name);
				ASSERT(it != physicalShards.end());
				auto shard = it->second;
				if (shard->dataShards.size() == 0) {
					shard->deleteTimeSec = now();
					pendingDeletionShards.push_back(name);
				}
			}
			if (unusedShards.size() > 0) {
				TraceEvent("ShardedRocksDB", logId).detail("CleanUpUnusedShards", unusedShards.size());
			}
		} else {
			// DB is opened with default shard.
			ASSERT(handles.size() == 1);

			// Add SpecialKeys range. This range should not be modified.
			std::shared_ptr<PhysicalShard> defaultShard = std::make_shared<PhysicalShard>(db, "default", handles[0]);
			columnFamilyMap[defaultShard->cf->GetID()] = defaultShard->cf;
			std::unique_ptr<DataShard> dataShard = std::make_unique<DataShard>(specialKeys, defaultShard.get());
			dataShardMap.insert(specialKeys, dataShard.get());
			defaultShard->dataShards[specialKeys.begin.toString()] = std::move(dataShard);
			physicalShards[defaultShard->id] = defaultShard;

			// Create metadata shard.
			auto metadataShard =
			    std::make_shared<PhysicalShard>(db, METADATA_SHARD_ID, rocksdb::ColumnFamilyOptions(dbOptions));
			metadataShard->init();
			columnFamilyMap[metadataShard->cf->GetID()] = metadataShard->cf;
			physicalShards[METADATA_SHARD_ID] = metadataShard;

			// Write special key range metadata.
			writeBatch = std::make_unique<rocksdb::WriteBatch>();
			dirtyShards = std::make_unique<std::set<PhysicalShard*>>();
			persistRangeMapping(specialKeys, true);
			rocksdb::WriteOptions options;
			status = db->Write(options, writeBatch.get());
			if (!status.ok()) {
				return status;
			}
			metadataShard->readIterPool->update();
			TraceEvent(SevInfo, "ShardedRocksInitializeMetaDataShard", this->logId)
			    .detail("MetadataShardCF", metadataShard->cf->GetID());
		}

		writeBatch = std::make_unique<rocksdb::WriteBatch>();
		dirtyShards = std::make_unique<std::set<PhysicalShard*>>();

		TraceEvent(SevInfo, "ShardedRocksShardManagerInitEnd", this->logId).detail("DataPath", path);
		return status;
	}

	DataShard* getDataShard(KeyRef key) {
		DataShard* shard = dataShardMap[key];
		ASSERT(shard == nullptr || shard->range.contains(key));
		return shard;
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

	PhysicalShard* addRange(KeyRange range, std::string id) {
		TraceEvent(SevInfo, "ShardedRocksAddRangeBegin", this->logId)
		    .detail("Range", range)
		    .detail("PhysicalShardID", id);

		// Newly added range should not overlap with any existing range.
		auto ranges = dataShardMap.intersectingRanges(range);

		for (auto it = ranges.begin(); it != ranges.end(); ++it) {
			if (it.value() != nullptr && it.value()->physicalShard->id != id) {
				TraceEvent(SevError, "ShardedRocksAddOverlappingRanges")
				    .detail("IntersectingRange", it->range())
				    .detail("DataShardRange", it->value()->range)
				    .detail("PhysicalShard", it->value()->physicalShard->toString());
			}
		}

		auto [it, inserted] = physicalShards.emplace(
		    id, std::make_shared<PhysicalShard>(db, id, rocksdb::ColumnFamilyOptions(dbOptions)));
		std::shared_ptr<PhysicalShard>& shard = it->second;

		activePhysicalShardIds.emplace(id);

		auto dataShard = std::make_unique<DataShard>(range, shard.get());
		dataShardMap.insert(range, dataShard.get());
		shard->dataShards[range.begin.toString()] = std::move(dataShard);

		validate();

		TraceEvent(SevInfo, "ShardedRocksAddRangeEnd", this->logId)
		    .detail("Range", range)
		    .detail("PhysicalShardID", id);

		return shard.get();
	}

	std::vector<std::string> removeRange(KeyRange range) {
		TraceEvent(SevInfo, "ShardedRocksRemoveRangeBegin", this->logId).detail("Range", range);

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

			TraceEvent(SevDebug, "ShardedRocksRemoveRange")
			    .detail("Range", range)
			    .detail("IntersectingRange", shardRange)
			    .detail("DataShardRange", it.value()->range)
			    .detail("PhysicalShard", existingShard->toString());

			ASSERT(it.value()->range == shardRange); // Ranges should be consistent.
			if (range.contains(shardRange)) {
				existingShard->dataShards.erase(shardRange.begin.toString());
				TraceEvent(SevInfo, "ShardedRocksRemovedRange")
				    .detail("Range", range)
				    .detail("RemovedRange", shardRange)
				    .detail("PhysicalShard", existingShard->toString());
				if (existingShard->dataShards.size() == 0) {
					TraceEvent(SevDebug, "ShardedRocksDB").detail("EmptyShardId", existingShard->id);
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

	std::vector<std::shared_ptr<PhysicalShard>> getPendingDeletionShards(double cleanUpDelay) {
		std::vector<std::shared_ptr<PhysicalShard>> emptyShards;
		double currentTime = now();

		TraceEvent(SevInfo, "ShardedRocksDB", logId)
		    .detail("PendingDeletionShardQueueSize", pendingDeletionShards.size());
		while (!pendingDeletionShards.empty()) {
			const auto& id = pendingDeletionShards.front();
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

	void clearRange(KeyRangeRef range) {
		auto rangeIterator = dataShardMap.intersectingRanges(range);

		for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
			if (it.value() == nullptr) {
				TraceEvent(SevDebug, "ShardedRocksDB").detail("ClearNonExistentRange", it.range());
				continue;
			}
			writeBatch->DeleteRange(it.value()->physicalShard->cf, toSlice(range.begin), toSlice(range.end));
			dirtyShards->insert(it.value()->physicalShard);
		}
	}

	void persistRangeMapping(KeyRangeRef range, bool isAdd) {
		TraceEvent(SevDebug, "ShardedRocksDB", this->logId)
		    .detail("Info", "RangeToPersist")
		    .detail("BeginKey", range.begin)
		    .detail("EndKey", range.end);
		auto it = physicalShards.find(METADATA_SHARD_ID);
		ASSERT(it != physicalShards.end());
		auto metadataShard = it->second;

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
		dirtyShards->insert(metadataShard.get());
	}

	std::unique_ptr<rocksdb::WriteBatch> getWriteBatch() {
		std::unique_ptr<rocksdb::WriteBatch> existingWriteBatch = std::move(writeBatch);
		writeBatch = std::make_unique<rocksdb::WriteBatch>();
		return existingWriteBatch;
	}

	std::unique_ptr<std::set<PhysicalShard*>> getDirtyShards() {
		std::unique_ptr<std::set<PhysicalShard*>> existingShards = std::move(dirtyShards);
		dirtyShards = std::make_unique<std::set<PhysicalShard*>>();
		return existingShards;
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
		columnFamilyMap.clear();
		for (auto& [_, shard] : physicalShards) {
			shard->deletePending = true;
		}
		physicalShards.clear();
		// Close DB.
		auto s = db->Close();
		if (!s.ok()) {
			logRocksDBError(s, "Close");
			return;
		}
		s = rocksdb::DestroyDB(path, getOptions());
		if (!s.ok()) {
			logRocksDBError(s, "DestroyDB");
		}
		TraceEvent("ShardedRocksDB", this->logId).detail("Info", "DBDestroyed");
	}

	rocksdb::DB* getDb() const { return db; }

	std::unordered_map<std::string, std::shared_ptr<PhysicalShard>>* getAllShards() { return &physicalShards; }

	std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* getColumnFamilyMap() { return &columnFamilyMap; }

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

	void validate() {
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
	rocksdb::Options dbOptions;
	rocksdb::DB* db = nullptr;
	std::unordered_map<std::string, std::shared_ptr<PhysicalShard>> physicalShards;
	std::unordered_set<std::string> activePhysicalShardIds;
	// Stores mapping between cf id and cf handle, used during compaction.
	std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*> columnFamilyMap;
	std::unique_ptr<rocksdb::WriteBatch> writeBatch;
	std::unique_ptr<std::set<PhysicalShard*>> dirtyShards;
	KeyRangeMap<DataShard*> dataShardMap;
	std::deque<std::string> pendingDeletionShards;
};

class RocksDBMetrics {
public:
	RocksDBMetrics(UID debugID, std::shared_ptr<rocksdb::Statistics> stats);
	void logStats(rocksdb::DB* db);
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
	// Stat for Memory Usage
	void logMemUsage(rocksdb::DB* db);

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
	std::vector<Reference<Histogram>> readRangeNewIteratorHistograms; // Zhe: haven't used?
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
		{ "BloomFilterMicros", rocksdb::BLOOM_FILTER_MICROS, 0 },
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
		{ "BlockCacheCompressedMiss", rocksdb::BLOCK_CACHE_COMPRESSED_MISS, 0 },
		{ "BlockCacheCompressedHit", rocksdb::BLOCK_CACHE_COMPRESSED_HIT, 0 },
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
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_LATENCY_HISTOGRAM, Histogram::Unit::microseconds));
		readValueLatencyHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_LATENCY_HISTOGRAM, Histogram::Unit::microseconds));
		readPrefixLatencyHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_LATENCY_HISTOGRAM, Histogram::Unit::microseconds));
		readRangeActionHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_ACTION_HISTOGRAM, Histogram::Unit::microseconds));
		readValueActionHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_ACTION_HISTOGRAM, Histogram::Unit::microseconds));
		readPrefixActionHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_ACTION_HISTOGRAM, Histogram::Unit::microseconds));
		readRangeQueueWaitHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_QUEUEWAIT_HISTOGRAM, Histogram::Unit::microseconds));
		readValueQueueWaitHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_QUEUEWAIT_HISTOGRAM, Histogram::Unit::microseconds));
		readPrefixQueueWaitHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_QUEUEWAIT_HISTOGRAM, Histogram::Unit::microseconds));
		readRangeNewIteratorHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READRANGE_NEWITERATOR_HISTOGRAM, Histogram::Unit::microseconds));
		readValueGetHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READVALUE_GET_HISTOGRAM, Histogram::Unit::microseconds));
		readPrefixGetHistograms.push_back(Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_READPREFIX_GET_HISTOGRAM, Histogram::Unit::microseconds));
	}
	commitLatencyHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_COMMIT_LATENCY_HISTOGRAM, Histogram::Unit::microseconds);
	commitActionHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_COMMIT_ACTION_HISTOGRAM, Histogram::Unit::microseconds);
	commitQueueWaitHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_COMMIT_QUEUEWAIT_HISTOGRAM, Histogram::Unit::microseconds);
	writeHistogram =
	    Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_WRITE_HISTOGRAM, Histogram::Unit::microseconds);
	deleteCompactRangeHistogram = Histogram::getHistogram(
	    ROCKSDBSTORAGE_HISTOGRAM_GROUP, ROCKSDB_DELETE_COMPACTRANGE_HISTOGRAM, Histogram::Unit::microseconds);
}

void RocksDBMetrics::logStats(rocksdb::DB* db) {
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
}

void RocksDBMetrics::logMemUsage(rocksdb::DB* db) {
	TraceEvent e(SevInfo, "ShardedRocksDBMemMetrics", debugID);
	uint64_t stat;
	ASSERT(db != nullptr);
	ASSERT(db->GetAggregatedIntProperty(rocksdb::DB::Properties::kBlockCacheUsage, &stat));
	e.detail("BlockCacheUsage", stat);
	ASSERT(db->GetAggregatedIntProperty(rocksdb::DB::Properties::kEstimateTableReadersMem, &stat));
	e.detail("EstimateSstReaderBytes", stat);
	ASSERT(db->GetAggregatedIntProperty(rocksdb::DB::Properties::kCurSizeAllMemTables, &stat));
	e.detail("AllMemtablesBytes", stat);
	ASSERT(db->GetAggregatedIntProperty(rocksdb::DB::Properties::kBlockCachePinnedUsage, &stat));
	e.detail("BlockCachePinnedUsage", stat);
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
                                                  ShardManager* shardManager) {
	try {
		wait(openFuture);
		state rocksdb::DB* db = shardManager->getDb();
		loop {
			wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
			if (rState->closing) {
				break;
			}
			rocksDBMetrics->logStats(db);
			rocksDBMetrics->logMemUsage(db);
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

	ACTOR static Future<Void> refreshReadIteratorPools(
	    std::shared_ptr<ShardedRocksDBState> rState,
	    Future<Void> readyToStart,
	    std::unordered_map<std::string, std::shared_ptr<PhysicalShard>>* physicalShards) {
		state Reference<Histogram> histogram = Histogram::getHistogram(
		    ROCKSDBSTORAGE_HISTOGRAM_GROUP, "TimeSpentRefreshIterators"_sr, Histogram::Unit::microseconds);

		if (SERVER_KNOBS->ROCKSDB_READ_RANGE_REUSE_ITERATORS) {
			try {
				wait(readyToStart);
				loop {
					wait(delay(SERVER_KNOBS->ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME));
					if (rState->closing) {
						break;
					}
					double startTime = timer_monotonic();
					for (auto& [_, shard] : *physicalShards) {
						if (shard->initialized()) {
							shard->refreshReadIteratorPool();
						}
					}
					histogram->sample(timer_monotonic() - startTime);
				}
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent(SevError, "RefreshReadIteratorPoolError").errorUnsuppressed(e);
				}
			}
		}

		return Void();
	}

	struct Writer : IThreadPoolReceiver {
		const UID logId;
		int threadIndex;
		std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* columnFamilyMap;
		std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
		std::shared_ptr<rocksdb::RateLimiter> rateLimiter;
		double sampleStartTime;

		explicit Writer(UID logId,
		                int threadIndex,
		                std::unordered_map<uint32_t, rocksdb::ColumnFamilyHandle*>* columnFamilyMap,
		                std::shared_ptr<RocksDBMetrics> rocksDBMetrics)
		  : logId(logId), threadIndex(threadIndex), columnFamilyMap(columnFamilyMap), rocksDBMetrics(rocksDBMetrics),
		    rateLimiter(SERVER_KNOBS->ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC > 0
		                    ? rocksdb::NewGenericRateLimiter(
		                          SERVER_KNOBS->ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC, // rate_bytes_per_sec
		                          100 * 1000, // refill_period_us
		                          10, // fairness
		                          rocksdb::RateLimiter::Mode::kWritesOnly,
		                          SERVER_KNOBS->ROCKSDB_WRITE_RATE_LIMITER_AUTO_TUNE)
		                    : nullptr),
		    sampleStartTime(now()) {}

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
			std::shared_ptr<RocksDBErrorListener> errorListener;

			OpenAction(ShardManager* shardManager,
			           Optional<Future<Void>>& metrics,
			           const FlowLock* readLock,
			           const FlowLock* fetchLock,
			           std::shared_ptr<RocksDBErrorListener> errorListener)
			  : shardManager(shardManager), metrics(metrics), readLock(readLock), fetchLock(fetchLock),
			    errorListener(errorListener) {}

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
				a.done.sendError(statusToError(s));
				return;
			}
			ASSERT(a.shard->cf);
			(*columnFamilyMap)[a.shard->cf->GetID()] = a.shard->cf;
			a.done.send(Void());
		}

		struct RemoveShardAction : TypedAction<Writer, RemoveShardAction> {
			std::vector<std::shared_ptr<PhysicalShard>> shards;
			ThreadReturnPromise<Void> done;

			RemoveShardAction(std::vector<std::shared_ptr<PhysicalShard>>& shards) : shards(shards) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(RemoveShardAction& a) {
			for (auto& shard : a.shards) {
				shard->deletePending = true;
				columnFamilyMap->erase(shard->cf->GetID());
			}
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
				if (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) {
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
			DeleteVisitor dv(deletes);
			rocksdb::Status s = batch->Iterate(&dv);
			if (!s.ok()) {
				logRocksDBError(s, "CommitDeleteVisitor");
				return s;
			}

			// If there are any range deletes, we should have added them to be deleted.
			ASSERT(!deletes->empty() || !batch->HasDeleteRange());

			rocksdb::WriteOptions options;
			options.sync = !SERVER_KNOBS->ROCKSDB_UNSAFE_AUTO_FSYNC;

			double writeBeginTime = sample ? timer_monotonic() : 0;
			s = db->Write(options, batch);
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
				a.done.sendError(statusToError(s));
				return;
			}

			for (auto shard : *(a.dirtyShards)) {
				shard->readIterPool->update();
			}

			a.done.send(Void());
			if (SERVER_KNOBS->ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE) {
				for (const auto& [id, range] : deletes) {
					auto cf = columnFamilyMap->find(id);
					ASSERT(cf != columnFamilyMap->end());
					auto begin = toSlice(range.begin);
					auto end = toSlice(range.end);

					ASSERT(a.db->SuggestCompactRange(cf->second, &begin, &end).ok());
				}
			}

			if (a.getHistograms) {
				double currTime = timer_monotonic();
				rocksDBMetrics->getCommitActionHistogram()->sampleSeconds(currTime - commitBeginTime);
				rocksDBMetrics->getCommitLatencyHistogram()->sampleSeconds(currTime - a.startTime);
			}

			sample();
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
	};

	struct Reader : IThreadPoolReceiver {
		const UID logId;
		double readValueTimeout;
		double readValuePrefixTimeout;
		double readRangeTimeout;
		int threadIndex;
		std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
		double sampleStartTime;

		explicit Reader(UID logId, int threadIndex, std::shared_ptr<RocksDBMetrics> rocksDBMetrics)
		  : logId(logId), threadIndex(threadIndex), rocksDBMetrics(rocksDBMetrics), sampleStartTime(now()) {
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
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			bool logShardMemUsage;
			ThreadReturnPromise<Optional<Value>> result;

			ReadValueAction(KeyRef key, PhysicalShard* shard, Optional<UID> debugID)
			  : key(key), shard(shard), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) ? true : false) {
			}

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
			if (SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT && readBeginTime - a.startTime > readValueTimeout) {
				TraceEvent(SevWarn, "ShardedRocksDBError")
				    .detail("Error", "Read value request timedout")
				    .detail("Method", "ReadValueAction")
				    .detail("Timeout value", readValueTimeout);
				a.result.sendError(key_value_store_deadline_exceeded());
				return;
			}

			rocksdb::PinnableSlice value;
			auto options = getReadOptions();

			auto db = a.shard->db;
			if (SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT) {
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
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			bool logShardMemUsage;
			ThreadReturnPromise<Optional<Value>> result;

			ReadValuePrefixAction(Key key, int maxLength, PhysicalShard* shard, Optional<UID> debugID)
			  : key(key), maxLength(maxLength), shard(shard), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms((deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE)
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
			if (SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT && readBeginTime - a.startTime > readValuePrefixTimeout) {
				TraceEvent(SevWarn, "ShardedRocksDBError")
				    .detail("Error", "Read value prefix request timedout")
				    .detail("Method", "ReadValuePrefixAction")
				    .detail("Timeout value", readValuePrefixTimeout);
				a.result.sendError(key_value_store_deadline_exceeded());
				return;
			}

			rocksdb::PinnableSlice value;
			auto options = getReadOptions();
			auto db = a.shard->db;
			if (SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT) {
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
			double startTime;
			bool getHistograms;
			bool logShardMemUsage;
			ThreadReturnPromise<RangeResult> result;
			ReadRangeAction(KeyRange keys, std::vector<DataShard*> shards, int rowLimit, int byteLimit)
			  : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit), startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) ? true : false) {
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
			if (SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT && readBeginTime - a.startTime > readRangeTimeout) {
				TraceEvent(SevWarn, "ShardedRocksKVSReadTimeout")
				    .detail("Error", "Read range request timedout")
				    .detail("Method", "ReadRangeAction")
				    .detail("Timeout value", readRangeTimeout);
				a.result.sendError(key_value_store_deadline_exceeded());
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
				auto bytesRead = readRangeInDb(shard, range, rowLimit, byteLimit, &result);
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

				if (SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT && timer_monotonic() - a.startTime > readRangeTimeout) {
					TraceEvent(SevInfo, "ShardedRocksDBTimeout")
					    .detail("Action", "ReadRange")
					    .detail("ShardsRead", numShards)
					    .detail("BytesRead", accumulatedBytes);
					a.result.sendError(key_value_store_deadline_exceeded());
					return;
				}
			}

			result.more =
			    (result.size() == a.rowLimit) || (result.size() == -a.rowLimit) || (accumulatedBytes >= a.byteLimit);
			if (result.more) {
				result.readThrough = result[result.size() - 1].key;
			}
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

	struct Counters {
		CounterCollection cc;
		Counter immediateThrottle;
		Counter failedToAcquire;

		Counters()
		  : cc("RocksDBThrottle"), immediateThrottle("ImmediateThrottle", cc), failedToAcquire("failedToAcquire", cc) {}
	};

	// Persist shard mappinng key range should not be in shardMap.
	explicit ShardedRocksDBKeyValueStore(const std::string& path, UID id)
	  : rState(std::make_shared<ShardedRocksDBState>()), path(path), id(id),
	    readSemaphore(SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    fetchSemaphore(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX),
	    numReadWaiters(SERVER_KNOBS->ROCKSDB_READ_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    numFetchWaiters(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX),
	    errorListener(std::make_shared<RocksDBErrorListener>()), errorFuture(errorListener->getFuture()),
	    dbOptions(getOptions()), shardManager(path, id, dbOptions),
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
			readThreads = CoroThreadPool::createThreadPool();
		} else {
			writeThread = createGenericThreadPool(/*stackSize=*/0, SERVER_KNOBS->ROCKSDB_WRITER_THREAD_PRIORITY);
			readThreads = createGenericThreadPool(/*stackSize=*/0, SERVER_KNOBS->ROCKSDB_READER_THREAD_PRIORITY);
		}
		writeThread->addThread(new Writer(id, 0, shardManager.getColumnFamilyMap(), rocksDBMetrics), "fdb-rocksdb-wr");
		TraceEvent("ShardedRocksDBReadThreads", id)
		    .detail("KnobRocksDBReadParallelism", SERVER_KNOBS->ROCKSDB_READ_PARALLELISM);
		for (unsigned i = 0; i < SERVER_KNOBS->ROCKSDB_READ_PARALLELISM; ++i) {
			readThreads->addThread(new Reader(id, i, rocksDBMetrics), "fdb-rocksdb-re");
		}
	}

	Future<Void> getError() const override { return errorFuture; }

	ACTOR static void doClose(ShardedRocksDBKeyValueStore* self, bool deleteOnClose) {
		self->rState->closing = true;
		// The metrics future retains a reference to the DB, so stop it before we delete it.
		self->metrics.reset();
		self->refreshHolder.cancel();
		self->cleanUpJob.cancel();

		wait(self->readThreads->stop());
		auto a = new Writer::CloseAction(&self->shardManager, deleteOnClose);
		auto f = a->done.getFuture();
		self->writeThread->post(a);
		wait(f);
		wait(self->writeThread->stop());
		if (self->closePromise.canBeSet())
			self->closePromise.send(Void());
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
			auto a = std::make_unique<Writer::OpenAction>(
			    &shardManager, metrics, &readSemaphore, &fetchSemaphore, errorListener);
			openFuture = a->done.getFuture();
			this->metrics = ShardManager::shardMetricsLogger(this->rState, openFuture, &shardManager) &&
			                rocksDBAggregatedMetricsLogger(this->rState, openFuture, rocksDBMetrics, &shardManager);
			this->refreshHolder = refreshReadIteratorPools(this->rState, openFuture, shardManager.getAllShards());
			this->cleanUpJob = emptyShardCleaner(this->rState, openFuture, &shardManager, writeThread);
			writeThread->post(a.release());
			return openFuture;
		}
	}

	Future<Void> addRange(KeyRangeRef range, std::string id) override {
		auto shard = shardManager.addRange(range, id);
		if (shard->initialized()) {
			return Void();
		}
		auto a = new Writer::AddShardAction(shard);
		Future<Void> res = a->done.getFuture();
		writeThread->post(a);
		return res;
	}

	void set(KeyValueRef kv, const Arena*) override { shardManager.put(kv.key, kv.value); }

	void clear(KeyRangeRef range, const StorageServerMetrics*, const Arena*) override {
		if (range.singleKeyRange()) {
			shardManager.clear(range.begin);
		} else {
			shardManager.clearRange(range);
		}
	}

	Future<Void> commit(bool) override {
		auto a = new Writer::CommitAction(shardManager.getDb(),
		                                  shardManager.getWriteBatch(),
		                                  shardManager.getDirtyShards(),
		                                  shardManager.getColumnFamilyMap());
		auto res = a->done.getFuture();
		writeThread->post(a);
		return res;
	}

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
			auto a = new Reader::ReadValueAction(key, shard->physicalShard, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValueAction>(key, shard->physicalShard, debugID);
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
			auto a = new Reader::ReadValuePrefixAction(key, maxLength, shard->physicalShard, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValuePrefixAction>(key, maxLength, shard->physicalShard, debugID);
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
			auto a = new Reader::ReadRangeAction(keys, shards, rowLimit, byteLimit);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == ReadType::FETCH) ? numFetchWaiters : numReadWaiters;
		checkWaiters(semaphore, maxWaiters);

		auto a = std::make_unique<Reader::ReadRangeAction>(keys, shards, rowLimit, byteLimit);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
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
				auto a = new Writer::RemoveShardAction(shards);
				Future<Void> f = a->done.getFuture();
				writeThread->post(a);
				TraceEvent(SevInfo, "ShardedRocksDB").detail("DeleteEmptyShards", shards.size());
				wait(f);
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
		return StorageBytes(free, total, live, free);
	}

	std::vector<std::string> removeRange(KeyRangeRef range) override { return shardManager.removeRange(range); }

	void persistRangeMapping(KeyRangeRef range, bool isAdd) override {
		return shardManager.persistRangeMapping(range, isAdd);
	}

	// Used for debugging shard mapping issue.
	std::vector<std::pair<KeyRange, std::string>> getDataMapping() { return shardManager.getDataMapping(); }

	std::shared_ptr<ShardedRocksDBState> rState;
	rocksdb::Options dbOptions;
	ShardManager shardManager;
	std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
	std::string path;
	UID id;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> readThreads;
	std::shared_ptr<RocksDBErrorListener> errorListener;
	Future<Void> errorFuture;
	Promise<Void> closePromise;
	Future<Void> openFuture;
	Optional<Future<Void>> metrics;
	FlowLock readSemaphore;
	int numReadWaiters;
	FlowLock fetchSemaphore;
	int numFetchWaiters;
	Counters counters;
	Future<Void> refreshHolder;
	Future<Void> cleanUpJob;
};

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL

IKeyValueStore* keyValueStoreShardedRocksDB(std::string const& path,
                                            UID logID,
                                            KeyValueStoreType storeType,
                                            bool checkChecksums,
                                            bool checkIntegrity) {
#ifdef SSD_ROCKSDB_EXPERIMENTAL
	return new ShardedRocksDBKeyValueStore(path, logID);
#else
	TraceEvent(SevError, "ShardedRocksDBEngineInitFailure").detail("Reason", "Built without RocksDB");
	ASSERT(false);
	return nullptr;
#endif // SSD_ROCKSDB_EXPERIMENTAL
}

#ifdef SSD_ROCKSDB_EXPERIMENTAL
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
	shardsToCleanUp.insert(shardsToCleanUp.end(), shardIds.begin(), shardIds.end());

	// Clean up empty shards. shard-2 is empty now.
	Future<Void> cleanUpFuture = kvStore->cleanUpShardsIfNeeded(shardsToCleanUp);
	wait(cleanUpFuture);

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
	mapping.push_back(std::make_pair(specialKeys, "default"));

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

		// Clean up shards. Shard-2 should not be removed.
		wait(kvStore->cleanUpShardsIfNeeded(shardsToCleanUp));
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

	return Void();
}

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL
