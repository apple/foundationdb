#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/CoroFlow.h"
#include "flow/IThreadPool.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"
#include "flow/serialize.h"
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/listener.h>
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
const KeyRef persistShardMappingPrefix(LiteralStringRef("\xff\xff/ShardMapping/"));
const KeyRangeRef defaultShardRange = KeyRangeRef(LiteralStringRef("\xff\xff"), LiteralStringRef("\xff\xff\xff"));
// TODO: move constants to a header file.

const StringRef ROCKSDBSTORAGE_HISTOGRAM_GROUP = LiteralStringRef("RocksDBStorage");
const StringRef ROCKSDB_COMMIT_LATENCY_HISTOGRAM = LiteralStringRef("RocksDBCommitLatency");
const StringRef ROCKSDB_COMMIT_ACTION_HISTOGRAM = LiteralStringRef("RocksDBCommitAction");
const StringRef ROCKSDB_COMMIT_QUEUEWAIT_HISTOGRAM = LiteralStringRef("RocksDBCommitQueueWait");
const StringRef ROCKSDB_WRITE_HISTOGRAM = LiteralStringRef("RocksDBWrite");
const StringRef ROCKSDB_DELETE_COMPACTRANGE_HISTOGRAM = LiteralStringRef("RocksDBDeleteCompactRange");
const StringRef ROCKSDB_READRANGE_LATENCY_HISTOGRAM = LiteralStringRef("RocksDBReadRangeLatency");
const StringRef ROCKSDB_READVALUE_LATENCY_HISTOGRAM = LiteralStringRef("RocksDBReadValueLatency");
const StringRef ROCKSDB_READPREFIX_LATENCY_HISTOGRAM = LiteralStringRef("RocksDBReadPrefixLatency");
const StringRef ROCKSDB_READRANGE_ACTION_HISTOGRAM = LiteralStringRef("RocksDBReadRangeAction");
const StringRef ROCKSDB_READVALUE_ACTION_HISTOGRAM = LiteralStringRef("RocksDBReadValueAction");
const StringRef ROCKSDB_READPREFIX_ACTION_HISTOGRAM = LiteralStringRef("RocksDBReadPrefixAction");
const StringRef ROCKSDB_READRANGE_QUEUEWAIT_HISTOGRAM = LiteralStringRef("RocksDBReadRangeQueueWait");
const StringRef ROCKSDB_READVALUE_QUEUEWAIT_HISTOGRAM = LiteralStringRef("RocksDBReadValueQueueWait");
const StringRef ROCKSDB_READPREFIX_QUEUEWAIT_HISTOGRAM = LiteralStringRef("RocksDBReadPrefixQueueWait");
const StringRef ROCKSDB_READRANGE_NEWITERATOR_HISTOGRAM = LiteralStringRef("RocksDBReadRangeNewIterator");
const StringRef ROCKSDB_READVALUE_GET_HISTOGRAM = LiteralStringRef("RocksDBReadValueGet");
const StringRef ROCKSDB_READPREFIX_GET_HISTOGRAM = LiteralStringRef("RocksDBReadPrefixGet");

namespace {
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
		TraceEvent(SevError, "RocksDBBGError")
		    .detail("Reason", getErrorReason(reason))
		    .detail("RocksDBSeverity", bg_error->severity())
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
using DB = rocksdb::DB*;

std::shared_ptr<rocksdb::Cache> rocksdb_block_cache = nullptr;

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

std::vector<std::pair<KeyRangeRef, std::string>> decodeShardMapping(const RangeResult& result, StringRef prefix) {
	std::vector<std::pair<KeyRangeRef, std::string>> shards;
	KeyRef endKey;
	std::string name;
	for (const auto& kv : result) {
		auto keyWithoutPrefix = kv.key.removePrefix(prefix);
		if (name.size() > 0) {
			shards.push_back({ KeyRangeRef(endKey, keyWithoutPrefix), name });
			std::cout << "Discover shard " << name << "\n";
		}
		endKey = keyWithoutPrefix;
		name = kv.value.toString();
	}
	return std::move(shards);
}

void logRocksDBError(const rocksdb::Status& status, const std::string& method) {
	auto level = status.IsTimedOut() ? SevWarn : SevError;
	TraceEvent e(level, "RocksDBError");
	e.detail("Error", status.ToString()).detail("Method", method).detail("RocksDBSeverity", status.severity());
	if (status.IsIOError()) {
		e.detail("SubCode", status.subcode());
	}
}

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
	TraceEvent e(severity, "KVSShardEvent");
	e.detail("Name", name).detail("Action", ShardOpToString(op));
	if (message != "") {
		e.detail("Message", message);
	}
}
void logShardEvent(StringRef name,
                   KeyRangeRef range,
                   ShardOp op,
                   Severity severity = SevInfo,
                   const std::string& message = "") {
	TraceEvent e(severity, "KVSShardEvent");
	e.detail("Name", name).detail("Action", ShardOpToString(op)).detail("Begin", range.begin).detail("End", range.end);
	if (message != "") {
		e.detail("Message", message);
	}
}

Error statusToError(const rocksdb::Status& s) {
	if (s.IsIOError()) {
		return io_error();
	} else if (s.IsTimedOut()) {
		return transaction_too_old();
	} else {
		return unknown_error();
	}
}

rocksdb::ColumnFamilyOptions getCFOptions() {
	rocksdb::ColumnFamilyOptions options;
	options.level_compaction_dynamic_level_bytes = true;
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

	if (rocksdb_block_cache == nullptr) {
		rocksdb_block_cache = rocksdb::NewLRUCache(128);
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
	ReadIterator(uint64_t index, DB& db, rocksdb::ReadOptions& options)
	  : index(index), inUse(true), creationTime(now()), iter(db->NewIterator(options)) {}
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
	ReadIteratorPool(DB& db, const std::string& path)
	  : db(db), index(0), iteratorsReuseCount(0), readRangeOptions(getReadOptions()) {
		readRangeOptions.background_purge_on_iterator_cleanup = true;
		readRangeOptions.auto_prefix_mode = (SERVER_KNOBS->ROCKSDB_PREFIX_LEN > 0);
		TraceEvent("ReadIteratorPool")
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
	ReadIterator getIterator() {
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
			ReadIterator iter(index, db, readRangeOptions);
			iteratorsMap.insert({ index, iter });
			return iter;
		} else {
			index++;
			ReadIterator iter(index, db, readRangeOptions);
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
	DB& db;
	rocksdb::ReadOptions readRangeOptions;
	std::mutex mutex;
	// incrementing counter for every new iterator creation, to uniquely identify the iterator in returnIterator().
	uint64_t index;
	uint64_t iteratorsReuseCount;
};

ACTOR Future<Void> flowLockLogger(const FlowLock* readLock, const FlowLock* fetchLock) {
	loop {
		wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
		TraceEvent e("RocksDBFlowLock");
		e.detail("ReadAvailable", readLock->available());
		e.detail("ReadActivePermits", readLock->activePermits());
		e.detail("ReadWaiters", readLock->waiters());
		e.detail("FetchAvailable", fetchLock->available());
		e.detail("FetchActivePermits", fetchLock->activePermits());
		e.detail("FetchWaiters", fetchLock->waiters());
	}
}

struct DataShard {
	DataShard(std::string name) : name(name), writeBatch(std::make_unique<rocksdb::WriteBatch>()) {}

	~DataShard() {
		if (db == nullptr)
			return;
		// Close DB
		readIterPool.reset();
		auto s = db->Close();
		if (!s.ok()) {
			logShardEvent(name, ShardOp::CLOSE, SevError, s.ToString());
			logRocksDBError(s, "CloseShard");
			return;
		}
		logShardEvent(name, ShardOp::CLOSE);

		if (!deletePending || segments.size() > 0)
			return;

		std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{ "default",
			                                                                                        getCFOptions() } };
		s = rocksdb::DestroyDB(name, getOptions(), defaultCF);
		if (!s.ok()) {
			logRocksDBError(s, "DestroyShard");
			logShardEvent(name, ShardOp::DESTROY, SevError, s.ToString());
			return;
		}
		logShardEvent(name, ShardOp::DESTROY);
	}

	void setDB(rocksdb::DB* db) {
		this->db = db;
		readIterPool = std::make_shared<ReadIteratorPool>(this->db, name);
	}

	rocksdb::DB* db = nullptr;
	std::unique_ptr<rocksdb::WriteBatch> writeBatch;
	std::string name;
	// Use range.begin to refcount the active segments on this shard.
	std::set<std::string> segments;
	bool deletePending = false;
	bool isSpecialKeysShard = false;
	std::shared_ptr<ReadIteratorPool> readIterPool = nullptr;
};

using ShardMap = KeyRangeMap<std::shared_ptr<DataShard>>;

ACTOR Future<Void> refreshReadIteratorPool(ShardMap* shardMap) {
	if (SERVER_KNOBS->ROCKSDB_READ_RANGE_REUSE_ITERATORS) {
		loop {
			wait(delay(SERVER_KNOBS->ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME));
			for (auto it : shardMap->ranges()) {
				if (it.value() && it.value()->readIterPool) {
					it.value()->readIterPool->refreshIterators();
				}
			}
		}
	}
	return Void();
}

int readRangeInDb(std::shared_ptr<DataShard> shard,
                  const KeyRangeRef& range,
                  int rowLimit,
                  int byteLimit,
                  RangeResult* result) {
	if (rowLimit == 0 || byteLimit == 0) {
		return 0;
	}

	int accumulatedRows = 0;
	int accumulatedBytes = 0;
	// TODO: Pass read timeout.
	int readRangeTimeout = SERVER_KNOBS->ROCKSDB_READ_RANGE_TIMEOUT;
	rocksdb::Status s;
	auto options = getReadOptions();
	uint64_t deadlineMircos = shard->db->GetEnv()->NowMicros() + readRangeTimeout * 1000000;
	std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
	options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);

	// When using a prefix extractor, ensure that keys are returned in order even if they cross
	// a prefix boundary.
	options.auto_prefix_mode = (SERVER_KNOBS->ROCKSDB_PREFIX_LEN > 0);
	if (rowLimit >= 0) {
		ReadIterator readIter = shard->readIterPool->getIterator();
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

			/*
			if (timer_monotonic() - a.startTime > readRangeTimeout) {
			    TraceEvent(SevWarn, "RocksDBError")
			        .detail("Error", "Read range request timedout")
			        .detail("Method", "ReadRangeAction")
			        .detail("Timeout value", readRangeTimeout);
			    a.result.sendError(transaction_too_old());
			    return;
			}*/
			cursor->Next();
		}
		s = cursor->status();
		shard->readIterPool->returnIterator(readIter);
	} else {
		ReadIterator readIter = shard->readIterPool->getIterator();
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
			/*
			if (timer_monotonic() - a.startTime > readRangeTimeout) {
			    TraceEvent(SevWarn, "RocksDBError")
			        .detail("Error", "Read range request timedout")
			        .detail("Method", "ReadRangeAction")
			        .detail("Timeout value", readRangeTimeout);
			    a.result.sendError(transaction_too_old());
			    return;
			}*/
			cursor->Prev();
		}
		s = cursor->status();
		shard->readIterPool->returnIterator(readIter);
	}

	if (!s.ok()) {
		logRocksDBError(s, "ReadRange");
		// The data wrriten to the arena is not erased, which will leave RangeResult in a dirty state. The RangeResult
		// should never be returned to user.
		return -1;
	}
	return accumulatedBytes;
}

class RocksDBMetrics {
public:
	RocksDBMetrics();
	// Statistics
	std::shared_ptr<rocksdb::Statistics> getStatsObjForRocksDB();
	void logStats(rocksdb::DB* db);
	// PerfContext
	void resetPerfContext();
	void setPerfContext(int index);
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
	void logMemUsagePerShard(std::string shardName, rocksdb::DB* db);

private:
	// Global Statistic Input to RocksDB DB instance
	std::shared_ptr<rocksdb::Statistics> stats;
	// Statistic Output from RocksDB
	std::vector<std::tuple<const char*, uint32_t, uint64_t>> tickerStats;
	std::vector<std::pair<const char*, std::string>> propertyStats;
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

RocksDBMetrics::RocksDBMetrics() {
	stats = rocksdb::CreateDBStatistics();
	stats->set_stats_level(rocksdb::kExceptHistogramOrTimers);
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
	propertyStats = {
		// Zhe: TODO Aggregation
		{ "NumCompactionsRunning", rocksdb::DB::Properties::kNumRunningCompactions },
		{ "NumImmutableMemtables", rocksdb::DB::Properties::kNumImmutableMemTable },
		{ "NumImmutableMemtablesFlushed", rocksdb::DB::Properties::kNumImmutableMemTableFlushed },
		{ "IsMemtableFlushPending", rocksdb::DB::Properties::kMemTableFlushPending },
		{ "NumRunningFlushes", rocksdb::DB::Properties::kNumRunningFlushes },
		{ "IsCompactionPending", rocksdb::DB::Properties::kCompactionPending },
		{ "NumRunningCompactions", rocksdb::DB::Properties::kNumRunningCompactions },
		{ "CumulativeBackgroundErrors", rocksdb::DB::Properties::kBackgroundErrors },
		{ "CurrentSizeActiveMemtable", rocksdb::DB::Properties::kCurSizeActiveMemTable },
		{ "AllMemtablesBytes", rocksdb::DB::Properties::kCurSizeAllMemTables }, // for mem usage
		{ "ActiveMemtableBytes", rocksdb::DB::Properties::kSizeAllMemTables },
		{ "CountEntriesActiveMemtable", rocksdb::DB::Properties::kNumEntriesActiveMemTable },
		{ "CountEntriesImmutMemtables", rocksdb::DB::Properties::kNumEntriesImmMemTables },
		{ "CountDeletesActiveMemtable", rocksdb::DB::Properties::kNumDeletesActiveMemTable },
		{ "CountDeletesImmutMemtables", rocksdb::DB::Properties::kNumDeletesImmMemTables },
		{ "EstimatedCountKeys", rocksdb::DB::Properties::kEstimateNumKeys },
		{ "EstimateSstReaderBytes", rocksdb::DB::Properties::kEstimateTableReadersMem }, // for mem usage
		{ "CountActiveSnapshots", rocksdb::DB::Properties::kNumSnapshots },
		{ "OldestSnapshotTime", rocksdb::DB::Properties::kOldestSnapshotTime },
		{ "CountLiveVersions", rocksdb::DB::Properties::kNumLiveVersions },
		{ "EstimateLiveDataSize", rocksdb::DB::Properties::kEstimateLiveDataSize },
		{ "BaseLevel", rocksdb::DB::Properties::kBaseLevel },
		{ "EstPendCompactBytes", rocksdb::DB::Properties::kEstimatePendingCompactionBytes },
		{ "BlockCacheUsage", rocksdb::DB::Properties::kBlockCacheUsage }, // for mem usage
		{ "BlockCachePinnedUsage", rocksdb::DB::Properties::kBlockCachePinnedUsage }, // for mem usage
	};
	std::unordered_map<std::string, uint64_t> readIteratorPoolStats = {
		{ "NumReadIteratorsCreated", 0 },
		{ "NumTimesReadIteratorsReused", 0 },
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

std::shared_ptr<rocksdb::Statistics> RocksDBMetrics::getStatsObjForRocksDB() {
	// Zhe: reserved for statistic of RocksDBMetrics per shard
	// ASSERT(shard != nullptr && shard->stats != nullptr);
	// return shard->stats;
	ASSERT(stats != nullptr);
	return stats;
}

void RocksDBMetrics::logStats(rocksdb::DB* db) {
	TraceEvent e("RocksDBMetrics");
	uint64_t stat;
	for (auto& [name, ticker, cum] : tickerStats) {
		stat = stats->getTickerCount(ticker);
		e.detail(name, stat - cum);
		cum = stat;
	}
	for (auto& [name, property] : propertyStats) { // Zhe: TODO aggregation
		stat = 0;
		ASSERT(db->GetIntProperty(property, &stat));
		e.detail(name, stat);
	}
	/*
	stat = readIterPool->numReadIteratorsCreated();
	e.detail("NumReadIteratorsCreated", stat - readIteratorPoolStats["NumReadIteratorsCreated"]);
	readIteratorPoolStats["NumReadIteratorsCreated"] = stat;
	stat = readIterPool->numTimesReadIteratorsReused();
	e.detail("NumTimesReadIteratorsReused", stat - readIteratorPoolStats["NumTimesReadIteratorsReused"]);
	readIteratorPoolStats["NumTimesReadIteratorsReused"] = stat;
	*/
}

void RocksDBMetrics::logMemUsagePerShard(std::string shardName, rocksdb::DB* db) {
	TraceEvent e("RocksDBShardMemMetrics");
	uint64_t stat;
	ASSERT(db != nullptr);
	ASSERT(db->GetIntProperty(rocksdb::DB::Properties::kBlockCacheUsage, &stat));
	e.detail("BlockCacheUsage", stat);
	ASSERT(db->GetIntProperty(rocksdb::DB::Properties::kEstimateTableReadersMem, &stat));
	e.detail("EstimateSstReaderBytes", stat);
	ASSERT(db->GetIntProperty(rocksdb::DB::Properties::kCurSizeAllMemTables, &stat));
	e.detail("AllMemtablesBytes", stat);
	ASSERT(db->GetIntProperty(rocksdb::DB::Properties::kBlockCachePinnedUsage, &stat));
	e.detail("BlockCachePinnedUsage", stat);
	e.detail("Name", shardName);
}

void RocksDBMetrics::resetPerfContext() {
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	rocksdb::get_perf_context()->Reset();
}

void RocksDBMetrics::setPerfContext(int index) {
	for (auto& [name, metric, vals] : perfContextMetrics) {
		vals[index] = getRocksdbPerfcontextMetric(metric);
	}
}

void RocksDBMetrics::logPerfContext(bool ignoreZeroMetric) {
	TraceEvent e("RocksDBPerfContextMetrics");
	e.setMaxEventLength(20000);
	for (auto& [name, metric, vals] : perfContextMetrics) {
		uint64_t s = 0;
		for (auto& v : vals) {
			s = s + v;
		}
		if (ignoreZeroMetric && s == 0)
			continue;
		for (int i = 0; i < SERVER_KNOBS->ROCKSDB_READ_PARALLELISM; i++) {
			if (vals[i] != 0)
				e.detail("RD" + std::to_string(i) + name, vals[i]);
		}
		if (vals[SERVER_KNOBS->ROCKSDB_READ_PARALLELISM] != 0)
			e.detail("WR" + (std::string)name, vals[SERVER_KNOBS->ROCKSDB_READ_PARALLELISM]);
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

ACTOR Future<Void> rocksDBAggregatedMetricsLogger(std::shared_ptr<RocksDBMetrics> rocksDBMetrics, rocksdb::DB* db) {
	loop {
		wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
		if (SERVER_KNOBS->ROCKSDB_ENABLE_STATISTIC) {
			rocksDBMetrics->logStats(db);
		}
		if (SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE != 0) {
			rocksDBMetrics->logPerfContext(true);
		}
	}
}

struct RocksDBKeyValueStore : IKeyValueStore {
	using CF = rocksdb::ColumnFamilyHandle*;
	using CommitBatchMap = std::unordered_map<std::shared_ptr<DataShard>, std::unique_ptr<rocksdb::WriteBatch>>;

	struct Writer : IThreadPoolReceiver {
		DB& db;
		std::shared_ptr<DataShard> defaultShard;

		UID id;
		std::shared_ptr<rocksdb::RateLimiter> rateLimiter;
		std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
		int threadIndex;

		explicit Writer(DB& db, UID id, int threadIndex, std::shared_ptr<RocksDBMetrics> rocksDBMetrics)
		  : db(db), id(id), threadIndex(threadIndex), rocksDBMetrics(rocksDBMetrics),
		    rateLimiter(SERVER_KNOBS->ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC > 0
		                    ? rocksdb::NewGenericRateLimiter(
		                          SERVER_KNOBS->ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC, // rate_bytes_per_sec
		                          100 * 1000, // refill_period_us
		                          10, // fairness
		                          rocksdb::RateLimiter::Mode::kWritesOnly,
		                          SERVER_KNOBS->ROCKSDB_WRITE_RATE_LIMITER_AUTO_TUNE)
		                    : nullptr) {}

		~Writer() override {
			defaultShard.reset();
			if (db) {
				delete db;
			}
		}

		void init() override {}

		struct OpenAction : TypedAction<Writer, OpenAction> {
			std::string path;
			std::string dataPath;
			ThreadReturnPromise<Void> done;
			Optional<Future<Void>>& metrics;
			const FlowLock* readLock;
			const FlowLock* fetchLock;
			std::shared_ptr<RocksDBErrorListener> errorListener;
			ShardMap* shardMap;

			OpenAction(std::string path,
			           std::string dataPath,
			           Optional<Future<Void>>& metrics,
			           const FlowLock* readLock,
			           const FlowLock* fetchLock,
			           std::shared_ptr<RocksDBErrorListener> errorListener,
			           ShardMap* shardMap)
			  : path(std::move(path)), dataPath(std::move(dataPath)), metrics(metrics), readLock(readLock),
			    fetchLock(fetchLock), errorListener(errorListener), shardMap(shardMap) {
				ASSERT(shardMap);
			}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(OpenAction& a) {
			std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
				"default", getCFOptions() } };
			std::vector<rocksdb::ColumnFamilyHandle*> handle;
			auto options = getOptions();
			options.listeners.push_back(a.errorListener);
			options.statistics = rocksDBMetrics->getStatsObjForRocksDB();
			rocksdb::Status status = rocksdb::DB::Open(options, a.path, defaultCF, &handle, &db);
			if (!status.ok()) {
				logRocksDBError(status, "Open");
				a.done.sendError(statusToError(status));
				return;
			}
			defaultShard = std::make_shared<DataShard>(a.path);
			defaultShard->setDB(db);
			defaultShard->isSpecialKeysShard = true;

			// The current thread and main thread are same when the code runs in simulation.
			// blockUntilReady() is getting the thread into deadlock state, so avoiding the
			// metric logger in simulation.
			if (!g_network->isSimulated()) {
				onMainThread([&] {
					a.metrics = rocksDBAggregatedMetricsLogger(rocksDBMetrics, db) &&
					            flowLockLogger(a.readLock, a.fetchLock) && refreshReadIteratorPool(a.shardMap);
					return Future<bool>(true);
				}).blockUntilReady();
			}

			TraceEvent(SevInfo, "RocksDB")
			    .detail("Path", a.path)
			    .detail("Method", "Open")
			    .detail("MetaRocks", uint64_t(db));

			if (!SERVER_KNOBS->ROCKSDB_ENABLE_SHARDING) {
				a.shardMap->insert(allKeys, defaultShard);
				TraceEvent(SevInfo, "RocksDB").detail("Method", "Open").detail("Info", "Open with single shard.");
				a.done.send(Void());
			}

			// Add KVS metadata key space to default shard.
			a.shardMap->insert(specialKeys, defaultShard);

			// a.shardMap->insert(systemKeys, defaultShard);

			auto mappingRange = prefixRange(persistShardMappingPrefix);
			RangeResult rangeResult;
			readRangeInDb(
			    defaultShard, mappingRange, /* rowLimit= */ UINT16_MAX, /* byteLimit = */ UINT16_MAX, &rangeResult);

			auto shards = decodeShardMapping(rangeResult, persistShardMappingPrefix);

			std::unordered_map<std::string, std::shared_ptr<DataShard>> openShards;

			for (const auto& [range, name] : shards) {
				auto it = openShards.find(name);
				if (it != openShards.end()) {
					// Reuses an existing shard and updates ref count.
					(*it).second->segments.insert(range.begin.toString());
					a.shardMap->insert(range, (*it).second);
					continue;
				}

				// Opens a new DB instance.
				auto shard = std::make_shared<DataShard>(name);
				rocksdb::DB* shardDb;
				status = rocksdb::DB::Open(options, name, defaultCF, &handle, &shardDb);
				if (!status.ok()) {
					logRocksDBError(status, "OpenShard");
					logShardEvent(name, range, ShardOp::OPEN, SevError, status.ToString());
					a.done.sendError(statusToError(status));
					return;
				}
				shard->setDB(shardDb);
				shard->segments.insert(range.begin.toString());
				a.shardMap->insert(range, shard);
				openShards[name] = shard;
			}

			TraceEvent(a.shardMap->size() == 0 ? SevWarn : SevInfo, "RocksDB")
			    .detail("Method", "Open")
			    .detail("NumShard", a.shardMap->size());
			a.done.send(Void());
		}

		struct RestoreDurableStateAction : TypedAction<Writer, RestoreDurableStateAction> {
			std::string path;
			std::string dataPath;
			ThreadReturnPromise<Void> done;
			ShardMap* shardMap;
			std::set<std::shared_ptr<DataShard>>* uncommittedShards;

			RestoreDurableStateAction(std::string path,
			                          std::string dataPath,
			                          ShardMap* shardMap,
			                          std::set<std::shared_ptr<DataShard>>* uncommittedShards)
			  : path(std::move(path)), dataPath(std::move(dataPath)), shardMap(shardMap),
			    uncommittedShards(uncommittedShards) {
				ASSERT(shardMap);
			}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(RestoreDurableStateAction& a) {
			ASSERT(db); // Database should have been created.

			TraceEvent(SevInfo, "RocksDB").detail("Method", "RestoreDurableState");
			if (a.uncommittedShards) {
				// Do not destroy shard
				for (auto shard : *a.uncommittedShards) {
					shard->deletePending = false;
				}
				a.uncommittedShards->clear(); // All the shards should have been closed.
			}

			// Read and reload metadata.
			auto mappingRange = prefixRange(persistShardMappingPrefix);
			RangeResult rangeResult;
			readRangeInDb(
			    defaultShard, mappingRange, /* rowLimit= */ UINT16_MAX, /* byteLimit = */ UINT16_MAX, &rangeResult);

			auto shards = decodeShardMapping(rangeResult, persistShardMappingPrefix);
			std::unordered_map<std::string, std::shared_ptr<DataShard>> openShards;
			for (const auto& [range, name] : shards) {
				openShards[name] = nullptr;
			}

			std::shared_ptr<DataShard> specialKeysShard;
			for (auto it : a.shardMap->ranges()) {
				if (!it.value() || !it.value()->db)
					continue;

				if (it.value()->isSpecialKeysShard) {
					specialKeysShard = it.value();
					continue;
				}

				auto name = it.value()->db->GetName();

				it.value()->segments.clear();
				if (openShards.find(name) != openShards.end()) {
					openShards[name] = it.value();
				} else {
					it.value()->deletePending = true;
				}
			}

			a.shardMap->insert(allKeys, nullptr);
			a.shardMap->insert(specialKeys, specialKeysShard);

			auto options = getOptions();
			options.statistics = rocksDBMetrics->getStatsObjForRocksDB();
			std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
				"default", getCFOptions() } };
			std::vector<rocksdb::ColumnFamilyHandle*> handle;
			for (const auto& [range, name] : shards) {
				auto shard = openShards[name];
				if (shard == nullptr) {
					shard = std::make_shared<DataShard>(name);
					rocksdb::DB* shardDb;
					auto status = rocksdb::DB::Open(options, name, defaultCF, &handle, &shardDb);
					shard->setDB(shardDb);
					if (!status.ok()) {
						logRocksDBError(status, "RestoreShard");
						a.done.sendError(statusToError(status));
						return;
					}
					openShards[name] = shard;
				}
				// Updates segment key.
				shard->segments.insert(range.begin.toString());
				a.shardMap->insert(range, shard);
			}

			a.done.send(Void());
		}

		struct DeleteVisitor : public rocksdb::WriteBatch::Handler {
			VectorRef<KeyRangeRef>& deletes;
			Arena& arena;

			DeleteVisitor(VectorRef<KeyRangeRef>& deletes, Arena& arena) : deletes(deletes), arena(arena) {}

			rocksdb::Status DeleteRangeCF(uint32_t /*column_family_id*/,
			                              const rocksdb::Slice& begin,
			                              const rocksdb::Slice& end) override {
				KeyRangeRef kr(toStringRef(begin), toStringRef(end));
				deletes.push_back_deep(arena, kr);
				return rocksdb::Status::OK();
			}
		};

		struct CommitAction : TypedAction<Writer, CommitAction> {
			std::unique_ptr<CommitBatchMap> shardsToCommit;
			ThreadReturnPromise<Void> done;
			double startTime;
			bool getHistograms;
			bool getPerfContext;
			bool logShardMemUsage;
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
			CommitAction() {
				if (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) {
					getHistograms = true;
					startTime = timer_monotonic();
				} else {
					getHistograms = false;
				}
				if ((SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE != 0) &&
				    (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE)) {
					getPerfContext = true;
				} else {
					getPerfContext = false;
				}
				if ((SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE != 0) &&
				    (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE)) {
					logShardMemUsage = true;
				} else {
					logShardMemUsage = false;
				}
			}
		};

		rocksdb::Status doCommit(rocksdb::WriteBatch* batch, std::shared_ptr<DataShard> shard, bool sample) {
			// std::cout << "Committing in db " << db->GetName() << std::endl;
			Standalone<VectorRef<KeyRangeRef>> deletes;
			DeleteVisitor dv(deletes, deletes.arena());
			ASSERT(batch->Iterate(&dv).ok());
			// If there are any range deletes, we should have added them to be deleted.
			ASSERT(!deletes.empty() || !batch->HasDeleteRange());
			rocksdb::WriteOptions options;
			options.sync = !SERVER_KNOBS->ROCKSDB_UNSAFE_AUTO_FSYNC;

			double writeBeginTime = sample ? timer_monotonic() : 0;
			auto s = shard->db->Write(options, batch);
			shard->readIterPool->update();
			if (sample) {
				rocksDBMetrics->getWriteHistogram()->sampleSeconds(timer_monotonic() - writeBeginTime);
			}
			if (!s.ok()) {
				logRocksDBError(s, "Commit");
				return s;
			}

			double compactRangeBeginTime = sample ? timer_monotonic() : 0;
			for (const auto& keyRange : deletes) {
				auto begin = toSlice(keyRange.begin);
				auto end = toSlice(keyRange.end);
				ASSERT(shard->db->SuggestCompactRange(shard->db->DefaultColumnFamily(), &begin, &end).ok());
			}
			if (sample) {
				rocksDBMetrics->getDeleteCompactRangeHistogram()->sampleSeconds(timer_monotonic() -
				                                                                compactRangeBeginTime);
			}
			return s;
		}

		void action(CommitAction& a) {
			if (a.getPerfContext) {
				rocksDBMetrics->resetPerfContext();
			}
			double commitBeginTime;
			if (a.getHistograms) {
				commitBeginTime = timer_monotonic();
				rocksDBMetrics->getCommitQueueWaitHistogram()->sampleSeconds(commitBeginTime - a.startTime);
			}

			rocksdb::Status s;
			std::shared_ptr<DataShard> specialKeysShard;
			rocksdb::WriteBatch* writeBatch;

			for (auto& [shard, batch] : *(a.shardsToCommit)) {
				if (shard->isSpecialKeysShard) {
					specialKeysShard = shard;
					writeBatch = batch.get();
					continue;
				}

				if (shard->segments.size() == 0) {
					// Destroy shard.
					shard->deletePending = true;
					continue;
				}

				if (!shard->db) {
					// create db
					std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
						"default", getCFOptions() } };
					std::vector<rocksdb::ColumnFamilyHandle*> handle;
					auto options = getOptions();
					options.statistics = rocksDBMetrics->getStatsObjForRocksDB();
					rocksdb::DB* shardDb;
					rocksdb::Status status = rocksdb::DB::Open(options, shard->name, defaultCF, &handle, &shardDb);
					shard->setDB(shardDb);
					if (!status.ok()) {
						logShardEvent(shard->name, ShardOp::CREATE, SevError, status.ToString());
						logRocksDBError(status, "AddShard");
						a.done.sendError(statusToError(status));
						return;
					}
					logShardEvent(shard->name, ShardOp::CREATE);
				}

				s = doCommit(batch.get(), shard, a.getHistograms);
				if (!s.ok()) {
					a.done.sendError(statusToError(s));
					return;
				}
			}

			if (a.getHistograms) {
				double currTime = timer_monotonic();
				rocksDBMetrics->getCommitActionHistogram()->sampleSeconds(currTime - commitBeginTime);
				rocksDBMetrics->getCommitLatencyHistogram()->sampleSeconds(currTime - a.startTime);
			}

			// System mutation needs to be committed after all other mutations.
			if (specialKeysShard) {
				s = doCommit(writeBatch, specialKeysShard, a.getHistograms);
				if (!s.ok()) {
					a.done.sendError(statusToError(s));
					return;
				}
			}
			if (a.getPerfContext) {
				rocksDBMetrics->setPerfContext(threadIndex);
			}
			if (a.logShardMemUsage) {
				for (auto& [shard, batch] : *(a.shardsToCommit)) {
					rocksDBMetrics->logMemUsagePerShard(shard->name, shard->db);
				}
			}
			// Destroy all the delete pending shards.
			a.shardsToCommit->clear();
			a.done.send(Void());
		}

		struct CloseAction : TypedAction<Writer, CloseAction> {
			ThreadReturnPromise<Void> done;
			std::string path;
			std::string dataPath;
			ShardMap* shardMap;
			std::set<std::shared_ptr<DataShard>>* uncommittedShards;
			bool deleteOnClose;
			CloseAction(std::string path,
			            std::string dataPath,
			            ShardMap* shardMap,
			            std::set<std::shared_ptr<DataShard>>* uncommittedShards,
			            bool deleteOnClose)
			  : path(path), dataPath(dataPath), shardMap(shardMap), uncommittedShards(uncommittedShards),
			    deleteOnClose(deleteOnClose) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(CloseAction& a) {
			if (db == nullptr) {
				a.done.send(Void());
				return;
			}

			if (a.deleteOnClose) {
				for (auto it : a.shardMap->ranges()) {
					if (it.value()) {
						it.value()->segments.clear();
						it.value()->deletePending = true;
					}
				}
				if (a.uncommittedShards) {
					for (auto shard : *(a.uncommittedShards)) {
						shard->segments.clear();
						shard->deletePending = true;
					}
				}
			}

			// Close and delete shard if needed.
			a.shardMap->clear();

			if (a.uncommittedShards) {
				// Delete unpersisted shards here.
				a.uncommittedShards->clear();
			}

			TraceEvent(SevInfo, "RocksDB").detail("Path", a.path).detail("Method", "Close");
			a.done.send(Void());
		}
	};

	struct Reader : IThreadPoolReceiver {
		DB& db;
		double readValueTimeout;
		double readValuePrefixTimeout;
		double readRangeTimeout;
		int threadIndex;
		std::shared_ptr<RocksDBMetrics> rocksDBMetrics;

		explicit Reader(DB& db, int threadIndex, std::shared_ptr<RocksDBMetrics> rocksDBMetrics)
		  : db(db), threadIndex(threadIndex), rocksDBMetrics(rocksDBMetrics) {
			if (g_network->isSimulated()) {
				// In simulation, increasing the read operation timeouts to 5 minutes, as some of the tests have
				// very high load and single read thread cannot process all the load within the timeouts.
				readValueTimeout = 5 * 60;
				readValuePrefixTimeout = 5 * 60;
				readRangeTimeout = 5 * 60;
			} else {
				readValueTimeout = SERVER_KNOBS->ROCKSDB_READ_VALUE_TIMEOUT;
				readValuePrefixTimeout = SERVER_KNOBS->ROCKSDB_READ_VALUE_PREFIX_TIMEOUT;
				readRangeTimeout = SERVER_KNOBS->ROCKSDB_READ_RANGE_TIMEOUT;
			}
		}

		void init() override {}

		struct ReadValueAction : TypedAction<Reader, ReadValueAction> {
			Key key;
			rocksdb::DB* instance;
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			bool getPerfContext;
			bool logShardMemUsage;
			std::string shardName;
			ThreadReturnPromise<Optional<Value>> result;

			ReadValueAction(KeyRef key, rocksdb::DB* instance, Optional<UID> debugID, std::string shardName)
			  : key(key), instance(instance), debugID(debugID), startTime(timer_monotonic()), shardName(shardName),
			    getHistograms(
			        (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) ? true : false),
			    getPerfContext(
			        (SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE != 0) &&
			                (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE)
			            ? true
			            : false),
			    logShardMemUsage(
			        (SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE != 0) &&
			                (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE)
			            ? true
			            : false) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};

		void action(ReadValueAction& a) {
			if (a.getPerfContext) {
				rocksDBMetrics->resetPerfContext();
			}
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				rocksDBMetrics->getReadValueQueueWaitHistogram(threadIndex)->sampleSeconds(readBeginTime - a.startTime);
			}
			Optional<TraceBatch> traceBatch;
			if (a.debugID.present()) {
				traceBatch = { TraceBatch{} };
				traceBatch.get().addEvent("GetValueDebug", a.debugID.get().first(), "Reader.Before");
			}
			if (readBeginTime - a.startTime > readValueTimeout) {
				TraceEvent(SevWarn, "RocksDBError")
				    .detail("Error", "Read value request timedout")
				    .detail("Method", "ReadValueAction")
				    .detail("Timeout value", readValueTimeout);
				a.result.sendError(transaction_too_old());
				return;
			}

			rocksdb::PinnableSlice value;
			auto options = getReadOptions();

			uint64_t deadlineMircos =
			    a.instance->GetEnv()->NowMicros() + (readValueTimeout - (timer_monotonic() - a.startTime)) * 1000000;
			std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
			options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);
			double dbGetBeginTime = a.getHistograms ? timer_monotonic() : 0;
			auto s = a.instance->Get(options, a.instance->DefaultColumnFamily(), toSlice(a.key), &value);

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
			if (a.getPerfContext) {
				rocksDBMetrics->setPerfContext(threadIndex);
			}
			if (a.logShardMemUsage) {
				rocksDBMetrics->logMemUsagePerShard(a.shardName, a.instance);
			}
		}

		struct ReadValuePrefixAction : TypedAction<Reader, ReadValuePrefixAction> {
			Key key;
			int maxLength;
			rocksdb::DB* instance;
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			bool getPerfContext;
			bool logShardMemUsage;
			std::string shardName;
			ThreadReturnPromise<Optional<Value>> result;
			ReadValuePrefixAction(Key key,
			                      int maxLength,
			                      rocksdb::DB* instance,
			                      Optional<UID> debugID,
			                      std::string shardName)
			  : key(key), maxLength(maxLength), instance(instance), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) ? true : false),
			    shardName(shardName), getPerfContext((SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE != 0) &&
			                                                 (deterministicRandom()->random01() <
			                                                  SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE)
			                                             ? true
			                                             : false),
			    logShardMemUsage(
			        (SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE != 0) &&
			                (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE)
			            ? true
			            : false){};
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};
		void action(ReadValuePrefixAction& a) {
			if (a.getPerfContext) {
				rocksDBMetrics->resetPerfContext();
			}
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
			if (readBeginTime - a.startTime > readValuePrefixTimeout) {
				TraceEvent(SevWarn, "RocksDBError")
				    .detail("Error", "Read value prefix request timedout")
				    .detail("Method", "ReadValuePrefixAction")
				    .detail("Timeout value", readValuePrefixTimeout);
				a.result.sendError(transaction_too_old());
				return;
			}

			rocksdb::PinnableSlice value;
			auto options = getReadOptions();
			uint64_t deadlineMircos = a.instance->GetEnv()->NowMicros() +
			                          (readValuePrefixTimeout - (timer_monotonic() - a.startTime)) * 1000000;
			std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
			options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);

			double dbGetBeginTime = a.getHistograms ? timer_monotonic() : 0;
			auto s = a.instance->Get(options, db->DefaultColumnFamily(), toSlice(a.key), &value);

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
			if (a.getPerfContext) {
				rocksDBMetrics->setPerfContext(threadIndex);
			}
			if (a.logShardMemUsage) {
				rocksDBMetrics->logMemUsagePerShard(a.shardName, a.instance);
			}
		}

		struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			KeyRange keys;
			// std::vector<rocksdb::DB*> instances;
			std::vector<std::pair<KeyRange, std::shared_ptr<DataShard>>> instances;
			int rowLimit, byteLimit;
			double startTime;
			bool getHistograms;
			bool getPerfContext;
			bool logShardMemUsage;
			std::vector<std::pair<std::string, rocksdb::DB*>> shardNames;
			ThreadReturnPromise<RangeResult> result;
			ReadRangeAction(KeyRange keys,
			                std::vector<std::pair<KeyRange, std::shared_ptr<DataShard>>> instances,
			                int rowLimit,
			                int byteLimit,
			                std::vector<std::pair<std::string, rocksdb::DB*>> shardNames)
			  : keys(keys), instances(instances), rowLimit(rowLimit), byteLimit(byteLimit),
			    startTime(timer_monotonic()), shardNames(shardNames),
			    getHistograms(
			        (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) ? true : false),
			    getPerfContext(
			        (SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE != 0) &&
			                (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_PERFCONTEXT_SAMPLE_RATE)
			            ? true
			            : false),
			    logShardMemUsage(
			        (SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE != 0) &&
			                (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_MEM_USAGE_METRIC_SAMPLE_RATE)
			            ? true
			            : false) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
		};
		void action(ReadRangeAction& a) {
			if (a.getPerfContext) {
				rocksDBMetrics->resetPerfContext();
			}
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				rocksDBMetrics->getReadRangeQueueWaitHistogram(threadIndex)->sampleSeconds(readBeginTime - a.startTime);
			}
			if (readBeginTime - a.startTime > readRangeTimeout) {
				TraceEvent(SevWarn, "RocksDBError")
				    .detail("Error", "Read range request timedout")
				    .detail("Method", "ReadRangeAction")
				    .detail("Timeout value", readRangeTimeout);
				a.result.sendError(transaction_too_old());
				return;
			}

			int rowLimit = a.rowLimit;
			int byteLimit = a.byteLimit;
			RangeResult result;

			if (rowLimit == 0 || byteLimit == 0) {
				a.result.send(result);
			}
			if (rowLimit < 0) {
				// Reverses the instances so we could read range in reverse direction.
				std::reverse(a.instances.begin(), a.instances.end());
			}

			// TODO: Do multi-thread read to improve performance
			// All the shards should have the same version. Does the version matter?? probably not.
			// Consider the following case
			// Shard [a, b) => s1, [b, d) => s2
			// Enqueue ClearRange(a, c)
			// Enqueue Read(a, d), one shard could have applied ClearRange, Another may not.

			int accumulatedBytes = 0;
			for (auto [range, shard] : a.instances) {
				KeyRange readRange = KeyRange(KeyRangeRef(a.keys.begin > range.begin ? a.keys.begin : range.begin,
				                                          a.keys.end < range.end ? a.keys.end : range.end));

				auto bytesRead = readRangeInDb(shard, readRange, rowLimit, byteLimit, &result);
				if (bytesRead < 0) {
					// Error reading an instance.
					a.result.sendError(internal_error());
					return;
				}
				byteLimit -= bytesRead;
				accumulatedBytes += bytesRead;
				if (result.size() >= abs(a.rowLimit) || accumulatedBytes >= a.byteLimit) {
					break;
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
			}
			if (a.getPerfContext) {
				rocksDBMetrics->setPerfContext(threadIndex);
			}
			if (a.logShardMemUsage) {
				for (auto [shardName, instance] : a.shardNames) {
					rocksDBMetrics->logMemUsagePerShard(shardName, instance);
				}
			}
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
	explicit RocksDBKeyValueStore(const std::string& path, UID id)
	  : path(path), dataPath(path + rocksDataFolderSuffix), id(id),
	    readSemaphore(SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    fetchSemaphore(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX),
	    numReadWaiters(SERVER_KNOBS->ROCKSDB_READ_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    numFetchWaiters(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX),
	    errorListener(std::make_shared<RocksDBErrorListener>()), errorFuture(errorListener->getFuture()),
	    shardMap(nullptr, specialKeys.end), rocksDBMetrics(new RocksDBMetrics()) {
		// In simluation, run the reader/writer threads as Coro threads (i.e. in the network thread. The storage engine
		// is still multi-threaded as background compaction threads are still present. Reads/writes to disk will also
		// block the network thread in a way that would be unacceptable in production but is a necessary evil here. When
		// performing the reads in background threads in simulation, the event loop thinks there is no work to do and
		// advances time faster than 1 sec/sec. By the time the blocking read actually finishes, simulation has advanced
		// time by more than 5 seconds, so every read fails with a transaction_too_old error. Doing blocking IO on the
		// main thread solves this issue. There are almost certainly better fixes, but my goal was to get a less
		// invasive change merged first and work on a more realistic version if/when we think that would provide
		// substantially more confidence in the correctness.
		// TODO: Adapt the simulation framework to not advance time quickly when background reads/writes are occurring.
		if (g_network->isSimulated()) {
			writeThread = CoroThreadPool::createThreadPool();
			readThreads = CoroThreadPool::createThreadPool();
		} else {
			writeThread = createGenericThreadPool();
			readThreads = createGenericThreadPool();
		}
		writeThread->addThread(new Writer(db, id, SERVER_KNOBS->ROCKSDB_READ_PARALLELISM, rocksDBMetrics),
		                       "fdb-rocksdb-wr");
		TraceEvent("RocksDBReadThreads").detail("KnobRocksDBReadParallelism", SERVER_KNOBS->ROCKSDB_READ_PARALLELISM);
		for (unsigned i = 0; i < SERVER_KNOBS->ROCKSDB_READ_PARALLELISM; ++i) {
			readThreads->addThread(new Reader(db, i, rocksDBMetrics), "fdb-rocksdb-re");
		}
	}

	Future<Void> getError() const override { return errorFuture; }

	ACTOR static void doClose(RocksDBKeyValueStore* self, bool deleteOnClose) {
		// The metrics future retains a reference to the DB, so stop it before we delete it.
		self->metrics.reset();

		wait(self->readThreads->stop());
		auto a = new Writer::CloseAction(
		    self->path, self->dataPath, &self->shardMap, self->dirtyShards.get(), deleteOnClose);
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

	KeyValueStoreType getType() const override { return KeyValueStoreType(KeyValueStoreType::SSD_ROCKSDB_V1); }

	Future<Void> init() override {
		if (openFuture.isValid()) {
			// return openFuture;
			// Restore durable state if KVS is open. KVS will be re-initialized during rollback. To avoid the cost of
			// opening and closing multiple rocksdb instances, we reconcile the shard map using persist shard mapping
			// data.
			auto a = std::make_unique<Writer::RestoreDurableStateAction>(path, dataPath, &shardMap, dirtyShards.get());

			Future<Void> future = a->done.getFuture();
			writeThread->post(a.release());
			return future;
		} else {
			auto a = std::make_unique<Writer::OpenAction>(
			    path, dataPath, metrics, &readSemaphore, &fetchSemaphore, errorListener, &shardMap);
			openFuture = a->done.getFuture();
			writeThread->post(a.release());
			return openFuture;
		}
	}

	void set(KeyValueRef kv, const Arena*) override {
		if (dirtyShards == nullptr) {
			dirtyShards = std::make_unique<std::set<std::shared_ptr<DataShard>>>();
		}

		auto it = shardMap.rangeContaining(kv.key);
		if (it.value() == nullptr) {
			std::cout << "Write to non exist shard " << kv.key.toString() << ", " << it.range().begin.toString()
			          << " : " << it.range().end.toString() << "\n";
			if (kv.key.startsWith(systemKeys.begin)) {
				std::cout << "System key range not found ";
			}
			// ASSERT(it.value()); // Non-exist shard.
			TraceEvent(SevError, "RocksDB")
			    .detail("Method", "Set")
			    .detail("Key", kv.key)
			    .detail("Begin", it.range().begin)
			    .detail("End", it.range().end);

			return;
		}

		it.value()->writeBatch->Put(toSlice(kv.key), toSlice(kv.value));

		dirtyShards->insert(it.value());
	}

	void clear(KeyRangeRef range, const Arena*) override {
		if (dirtyShards == nullptr) {
			dirtyShards = std::make_unique<std::set<std::shared_ptr<DataShard>>>();
		}

		auto rangeIterator = shardMap.intersectingRanges(range);

		// Get DB instances
		for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
			if (it.value() == nullptr)
				continue;

			if (range.singleKeyRange()) {
				it.value()->writeBatch->Delete(toSlice(range.begin));
			} else {
				it.value()->writeBatch->DeleteRange(toSlice(range.begin), toSlice(range.end));
			}
			dirtyShards->insert(it.value());
		}
	}

	Future<Void> commit(bool) override {
		// If there is nothing to write, don't write.
		if (dirtyShards == nullptr || dirtyShards->size() == 0) {
			return Void();
		}

		auto a = new Writer::CommitAction();
		a->shardsToCommit = std::make_unique<CommitBatchMap>();
		for (auto shard : *dirtyShards) {
			a->shardsToCommit->emplace(shard, std::move(shard->writeBatch));
			shard->writeBatch = std::make_unique<rocksdb::WriteBatch>();
		}
		dirtyShards = nullptr;

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
	static bool shouldThrottle(IKeyValueStore::ReadType type, KeyRef key) {
		return type != IKeyValueStore::ReadType::EAGER && !(key.startsWith(systemKeys.begin));
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

	Future<Optional<Value>> readValue(KeyRef key, IKeyValueStore::ReadType type, Optional<UID> debugID) override {
		auto it = shardMap.rangeContaining(key);
		if (it.value() == nullptr) {
			// TODO: Add new error code, e.g., shard not found.
			// a.result.sendError(internal_error());
			// return internal_error();
			std::cout << "Read non-exist shard " << key.toString() << "\n";
			return Optional<Value>();
		}

		if (it.value()->db == nullptr) {
			std::cout << "Read uncommitted change " << key.toString() << "\n";
			return Optional<Value>();
		}

		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValueAction(key, it.value()->db, debugID, it.value()->name);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValueAction>(key, it.value()->db, debugID, it.value()->name);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        IKeyValueStore::ReadType type,
	                                        Optional<UID> debugID) override {
		auto it = shardMap.rangeContaining(key);
		if (it.value() == nullptr || it.value()->db == nullptr) {
			// TODO: Add new error code, e.g., shard not found.
			// a.result.sendError(internal_error());
			return Optional<Value>();
		}

		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValuePrefixAction(key, maxLength, it.value()->db, debugID, it.value()->name);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);

		auto a =
		    std::make_unique<Reader::ReadValuePrefixAction>(key, maxLength, it.value()->db, debugID, it.value()->name);
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
	                              IKeyValueStore::ReadType type) override {

		auto rangeIterator = shardMap.intersectingRanges(keys);
		// Get DB instances
		std::vector<std::pair<KeyRange, std::shared_ptr<DataShard>>> instances;
		std::vector<std::pair<std::string, rocksdb::DB*>> shardNames;
		for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
			if (it.value() == nullptr || it.value()->db == nullptr)
				continue;

			instances.push_back(std::make_pair(KeyRange(it.range()), it.value()));
			shardNames.push_back(std::make_pair(it.value()->name, it.value()->db));
		}

		if (!shouldThrottle(type, keys.begin)) {
			auto a = new Reader::ReadRangeAction(keys, instances, rowLimit, byteLimit, shardNames);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;
		checkWaiters(semaphore, maxWaiters);

		auto a = std::make_unique<Reader::ReadRangeAction>(keys, instances, rowLimit, byteLimit, shardNames);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
	}

	StorageBytes getStorageBytes() const override {
		uint64_t total_live = 0;
		int64_t total_free = 0;
		int64_t total_space = 0;
		// ASSERT(db->GetIntProperty(rocksdb::DB::Properties::kLiveSstFilesSize, &live));
		for (auto& it : shardMap.ranges()) {
			if (!it.value() || it.value()->db == nullptr)
				continue;

			uint64_t live = 0;
			ASSERT(it.value()->db->GetIntProperty(rocksdb::DB::Properties::kLiveSstFilesSize, &live));
			total_live += live;

			int64_t free, space;
			g_network->getDiskBytes(it.value()->name, free, space);
			total_free += free;
			total_space += space;
		}

		return StorageBytes(total_free, total_space, total_live, total_free);
	}

	std::vector<rocksdb::DB*> getAllInstances() const {
		std::vector<DB> res;
		for (auto& it : shardMap.ranges()) {
			std::cout << it.begin().toString() << ", " << it.end().toString() << ": " << uint64_t(it.value()->db)
			          << std::endl;
			if (it.value() != nullptr && it.value()->db != nullptr) {
				res.push_back(it.value()->db);
			}
		}
		return res;
	}

	void addShard(KeyRangeRef range, UID id) override {
		if (!SERVER_KNOBS->ROCKSDB_ENABLE_SHARDING) {
			// TODO: add a new error code.
			return;
		}

		if (dirtyShards == nullptr) {
			dirtyShards.reset(new std::set<std::shared_ptr<DataShard>>());
		}

		auto shard = std::make_shared<DataShard>(dataPath + id.toString());
		shard->segments.insert(range.begin.toString());

		auto ranges = shardMap.intersectingRanges(range);

		for (auto it = ranges.begin(); it != ranges.end(); ++it) {
			if (!it.value())
				continue;
			auto existingShard = it.value();
			auto shardRange = it.range();
			if (range.contains(shardRange)) {
				// Replace shard
				existingShard->segments.erase(it.range().begin.toString());
				dirtyShards->insert(existingShard);
				continue;
			}

			if (shardRange.contains(range)) {
				TraceEvent(SevInfo, "RocksDB")
				    .detail("Action", "SplitShard")
				    .detail("Begin", range.begin)
				    .detail("End", range.end);
				logShardEvent(existingShard->name,
				              shardRange,
				              ShardOp::MODIFY_RANGE,
				              SevWarn,
				              "Split shard, inner range: " + range.begin.toString() + " : " + range.end.toString());
				// TODO: clear overlapping range in existing shard.
				existingShard->segments.insert(range.begin.toString());
				existingShard->segments.insert(range.end.toString());
				continue;
			}

			// Overlapping shard.
			if (shardRange.end > range.begin) {
				// existing shard       ******
				// new shard                *******
				// existingShard->range = KeyRange(KeyRangeRef(existingShard->range.begin, range.begin));
				existingShard->segments.insert(shardRange.begin.toString());
			} else {
				// existing shard          ******
				// new shard          *******
				// shardRange = KeyRange(KeyRangeRef(range.end, existingShard->range.end));
				existingShard->segments.erase(shardRange.begin.toString());
				existingShard->segments.insert(range.end.toString());
			}
			logShardEvent(existingShard->name, shardRange, ShardOp::MODIFY_RANGE);
		}

		shardMap.insert(range, shard);
		dirtyShards->insert(shard);

		TraceEvent(SevInfo, "RocksDB")
		    .detail("Method", "AddShard")
		    .detail("Name", shard->name)
		    .detail("Begin", range.begin.toString())
		    .detail("End", range.end.toString());
		return;
	}

	Standalone<VectorRef<MutationRef>> getPersistShardMutations(KeyRangeRef range) override {
		TraceEvent(SevDebug, "GetPersistShardMutations")
		    .detail("Begin", range.begin.toString())
		    .detail("End", range.end.toString());
		Standalone<VectorRef<MutationRef>> refs;

		auto shard = shardMap.rangeContaining(range.begin);
		if (shard.range() != range) {
			TraceEvent(SevWarn, "RocksDB")
			    .detail("Action", "PersistShard")
			    .detail("ExpectedRangeBegin", range.begin)
			    .detail("ExpectedRangeEnd", range.end)
			    .detail("ShardRangeBegin", shard.range().begin)
			    .detail("ShardRangeEnd", shard.range().end)
			    .detail("Error", "RangeDiscrepancy");
		}

		if (!shard.value()) {
			TraceEvent(SevError, "RocksDB")
			    .detail("Action", "PersistShard")
			    .detail("ExpectedRangeBegin", range.begin)
			    .detail("ExpectedRangeEnd", range.end)
			    .detail("Error", "ShardNotFound");
			return refs;
		}

		// Ref:
		// https://github.com/apple/foundationdb/blob/6613ec282da87fd17ffdb9538f6da2fe42d2fa4f/fdbserver/storageserver.actor.cpp#L5608
		std::string beginKey = persistShardMappingPrefix.toString() + range.begin.toString();

		refs.push_back_deep(refs.arena(),
		                    MutationRef(MutationRef::ClearRange,
		                                StringRef(beginKey),
		                                StringRef(persistShardMappingPrefix.toString() + range.end.toString())));
		refs.push_back_deep(refs.arena(),
		                    MutationRef(MutationRef::SetValue,
		                                StringRef(persistShardMappingPrefix.toString() + range.begin.toString()),
		                                StringRef(shard.value()->name)));

		if (range.end <= allKeys.end) {
			auto it = shardMap.rangeContaining(range.end);
			refs.push_back_deep(refs.arena(),
			                    MutationRef(MutationRef::SetValue,
			                                StringRef(persistShardMappingPrefix.toString() + range.end.toString()),
			                                StringRef(it.value() == nullptr ? "" : it.value()->name)));
		} else {
			refs.push_back_deep(refs.arena(),
			                    MutationRef(MutationRef::SetValue,
			                                StringRef(persistShardMappingPrefix.toString() + range.end.toString()),
			                                LiteralStringRef("")));
		}

		return refs;
	}

	Standalone<VectorRef<MutationRef>> getDisposeRangeMutations(KeyRangeRef range) override {
		TraceEvent(SevDebug, "GetDisposeRangeMutations")
		    .detail("Begin", range.begin.toString())
		    .detail("End", range.end.toString());
		Standalone<VectorRef<MutationRef>> refs;
		auto shards = shardMap.intersectingRanges(range);

		int numDroppedShards = 0;
		for (auto shard : shards) {
			// Ignore empty range.
			if (!shard.value())
				continue;

			++numDroppedShards;
		}

		// This may cause multiple fragments in metadata, but will not affect correctness.
		refs.push_back_deep(refs.arena(),
		                    MutationRef(MutationRef::ClearRange,
		                                persistShardMappingPrefix.toString() + range.begin.toString(),
		                                persistShardMappingPrefix.toString() + range.end.toString()));
		refs.push_back_deep(refs.arena(),
		                    MutationRef(MutationRef::SetValue,
		                                persistShardMappingPrefix.toString() + range.begin.toString(),
		                                LiteralStringRef("")));

		// TODO: should we include sys key range here?
		if (range.end < normalKeys.end) {
			auto it = shardMap.rangeContaining(range.end);
			refs.push_back_deep(
			    refs.arena(),
			    MutationRef(MutationRef::SetValue,
			                persistShardMappingPrefix.toString() + range.end.toString(),
			                it.value() == nullptr || it.value()->db == nullptr ? "" : it.value()->db->GetName()));
		}

		// The shard will stay in shardMap.
		return refs;
	}

	void disposeRange(KeyRangeRef range) override {
		if (!SERVER_KNOBS->ROCKSDB_ENABLE_SHARDING) {
			return;
		}

		if (dirtyShards == nullptr) {
			dirtyShards.reset(new std::set<std::shared_ptr<DataShard>>());
		}

		if (range.intersects(specialKeys)) {
			std::cout << "Invalid range to dispose " << range.begin.toString() << " : " << range.end.toString() << "\n";
			return;
		}

		if (range.intersects(systemKeys)) {
			std::cout << "Remove system keys range";
		}

		auto shards = shardMap.intersectingRanges(range);

		for (auto shard : shards) {
			if (!shard.value())
				continue;

			auto existingShard = shard.value();
			auto shardRange = shard.range();
			if (range.contains(shardRange)) {
				existingShard->segments.erase(shardRange.begin.toString());
				// shard.value()->deletePending = true;
				dirtyShards->insert(existingShard);
				continue;
			}

			// Range modification could result in more than one segments. Remove the original segment key here.
			existingShard->segments.erase(shardRange.begin.toString());
			if (shardRange.begin < range.begin) {
				// existingShard->range = KeyRange(KeyRangeRef(shardRange.begin, range.begin));
				existingShard->segments.insert(shardRange.begin.toString());
				logShardEvent(existingShard->name, shardRange, ShardOp::MODIFY_RANGE);
			}

			if (shardRange.end > range.end) {
				// existingShard->range = KeyRange(KeyRangeRef(range.end, existingShard->range.end));
				existingShard->segments.insert(range.end.toString());
				logShardEvent(existingShard->name, shardRange, ShardOp::MODIFY_RANGE);
			}
		}

		shardMap.insert(range, nullptr);
		TraceEvent(SevInfo, "RocksDB")
		    .detail("Method", "DisposeRange")
		    .detail("Begin", range.begin.toString())
		    .detail("End", range.end.toString());

		// raise(SIGSEGV);
	}

	DB db = nullptr;
	std::shared_ptr<RocksDBMetrics> rocksDBMetrics;
	std::string path;
	const std::string dataPath;
	UID id;
	ShardMap shardMap;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> readThreads;
	std::shared_ptr<RocksDBErrorListener> errorListener;
	Future<Void> errorFuture;
	Promise<Void> closePromise;
	Future<Void> openFuture;
	std::unique_ptr<std::set<std::shared_ptr<DataShard>>> dirtyShards;
	Optional<Future<Void>> metrics;
	FlowLock readSemaphore;
	int numReadWaiters;
	FlowLock fetchSemaphore;
	int numFetchWaiters;
	Counters counters;
};

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL

IKeyValueStore* keyValueStoreRocksDB(std::string const& path,
                                     UID logID,
                                     KeyValueStoreType storeType,
                                     bool checkChecksums,
                                     bool checkIntegrity) {
#ifdef SSD_ROCKSDB_EXPERIMENTAL
	return new RocksDBKeyValueStore(path, logID);
#else
	TraceEvent(SevError, "RocksDBEngineInitFailure").detail("Reason", "Built without RocksDB");
	ASSERT(false);
	return nullptr;
#endif // SSD_ROCKSDB_EXPERIMENTAL
}

#ifdef SSD_ROCKSDB_EXPERIMENTAL
#include "flow/UnitTest.h"

namespace {

/*
TEST_CASE("RocksDBKVS/SystemKeySpace") {
    state const std::string rocksDBTestDir = "rocksdb-kvstore";
    platform::eraseDirectoryRecursive(rocksDBTestDir);

    state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
    wait(kvStore->init());

    kvStore->set({ LiteralStringRef("\xff/foo"), LiteralStringRef("bxx") });
    kvStore->set({ LiteralStringRef("\xff/bar"), LiteralStringRef("b") });
    wait(kvStore->commit(false));

    RangeResult result = wait(kvStore->readRange(defaultShardRange, 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "Default shard " << result.toString() << "\n";

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    kvStore->clear(KeyRangeRef("\xff"_sr, "\xff/c"_sr));
    wait(kvStore->commit(false));

    RangeResult result = wait(kvStore->readRange(defaultShardRange, 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "After clear range " << result.toString() << "\n";

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    Future<Void> closed = kvStore->onClosed();
    kvStore->dispose();
    wait(closed);
    return Void();
}

TEST_CASE("RocksDBKVS/CrossShardOps") {
    state const std::string rocksDBTestDir = "rocksdb-kvstore";
    platform::eraseDirectoryRecursive(rocksDBTestDir);

    state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
    wait(kvStore->init());

    kvStore->addShard(KeyRangeRef("f"_sr, "g"_sr), deterministicRandom()->randomUniqueID());
    kvStore->set({ LiteralStringRef("foo"), LiteralStringRef("bar") });
    kvStore->set({ LiteralStringRef("fd"), LiteralStringRef("baf") });
    kvStore->set({ LiteralStringRef("fko"), LiteralStringRef("sr") });
    kvStore->set({ LiteralStringRef("fp"), LiteralStringRef("kddr") });
    wait(kvStore->commit(false));

    RangeResult result =
        wait(kvStore->readRange(KeyRangeRef("f"_sr, "g"_sr), 100, 500, IKeyValueStore::ReadType::NORMAL));

    std::cout << "Single Shard Read\n";
    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    kvStore->addShard(KeyRangeRef("g"_sr, "m"_sr), deterministicRandom()->randomUniqueID());
    kvStore->set({ LiteralStringRef("fc"), LiteralStringRef("bxx") });
    kvStore->set({ LiteralStringRef("g"), LiteralStringRef("b") });
    kvStore->set({ LiteralStringRef("if"), LiteralStringRef("fi") });
    kvStore->set({ LiteralStringRef("h"), LiteralStringRef("r") });
    wait(kvStore->commit(false));

    RangeResult result =
        wait(kvStore->readRange(KeyRangeRef("f"_sr, "m"_sr), 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "Cross Shard Read " << result.toString() << "\n";

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    kvStore->clear(KeyRangeRef("a"_sr, "fm"_sr));
    wait(kvStore->commit(false));

    RangeResult result =
        wait(kvStore->readRange(KeyRangeRef("f"_sr, "m"_sr), 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "After clear range " << result.toString() << "\n";

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    Future<Void> closed = kvStore->onClosed();
    kvStore->dispose();
    wait(closed);
    return Void();
}

TEST_CASE("RocksDBKVS/ClearRange") {
    state const std::string rocksDBTestDir = "rocksdb-kvstore";
    platform::eraseDirectoryRecursive(rocksDBTestDir);

    state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
    wait(kvStore->init());

    kvStore->addShard(KeyRangeRef("f"_sr, "g"_sr), deterministicRandom()->randomUniqueID());
    kvStore->set({ LiteralStringRef("foo"), LiteralStringRef("bar") });
    kvStore->set({ LiteralStringRef("fd"), LiteralStringRef("baf") });
    kvStore->set({ LiteralStringRef("fko"), LiteralStringRef("sr") });
    kvStore->set({ LiteralStringRef("fp"), LiteralStringRef("kddr") });
    wait(kvStore->commit(false));

    Optional<Value> val = wait(kvStore->readValue(LiteralStringRef("foo")));
    ASSERT(Optional<Value>(LiteralStringRef("bar")) == val);
    std::cout << "Finish read";

    RangeResult result =
        wait(kvStore->readRange(KeyRangeRef("f"_sr, "g"_sr), 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "Range result user " << result.toString();

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    kvStore->clear(KeyRangeRef("f"_sr, "fm"_sr), nullptr);
    wait(kvStore->commit(false));

    RangeResult result =
        wait(kvStore->readRange(KeyRangeRef("f"_sr, "g"_sr), 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "Range result after delete " << result.toString() << "\n";

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    Future<Void> closed = kvStore->onClosed();
    kvStore->dispose();
    wait(closed);
    return Void();
}

TEST_CASE("RocksDBKVS/Destroy") {
    state const std::string rocksDBTestDir = "rocksdb-kvstore-destroy";
    platform::eraseDirectoryRecursive(rocksDBTestDir);

    state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
    wait(kvStore->init());

    kvStore->addShard(KeyRangeRef("f"_sr, "g"_sr), deterministicRandom()->randomUniqueID());
    kvStore->set({ LiteralStringRef("foo"), LiteralStringRef("bar") });
    kvStore->set({ LiteralStringRef("fd"), LiteralStringRef("baf") });
    kvStore->set({ LiteralStringRef("fko"), LiteralStringRef("sr") });
    kvStore->set({ LiteralStringRef("fp"), LiteralStringRef("kddr") });
    wait(kvStore->commit(false));

    Optional<Value> val = wait(kvStore->readValue(LiteralStringRef("foo")));
    ASSERT(Optional<Value>(LiteralStringRef("bar")) == val);
    std::cout << "Finish read";

    RangeResult result =
        wait(kvStore->readRange(KeyRangeRef("f"_sr, "g"_sr), 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "Range result user " << result.toString();

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    Future<Void> closed = kvStore->onClosed();
    kvStore->dispose();
    wait(closed);
    return Void();
}

TEST_CASE("RocksDBKVS/Reopen") {
    state const std::string rocksDBTestDir = "rocksdb-kvstore-reopen-test-db";
    platform::eraseDirectoryRecursive(rocksDBTestDir);

    state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
    wait(kvStore->init());

    state KeyRangeRef shardRange = KeyRangeRef("f"_sr, "g"_sr);
    kvStore->addShard(shardRange, deterministicRandom()->randomUniqueID());
    kvStore->set({ LiteralStringRef("foo"), LiteralStringRef("bar") });
    kvStore->set({ LiteralStringRef("fd"), LiteralStringRef("baf") });
    kvStore->set({ LiteralStringRef("fko"), LiteralStringRef("sr") });
    kvStore->set({ LiteralStringRef("fp"), LiteralStringRef("kddr") });
    wait(kvStore->commit(false));

    kvStore->persistShard(shardRange);
    wait(kvStore->commit(false));

    std::cout << "Persist shard\n";

    Optional<Value> val = wait(kvStore->readValue(LiteralStringRef("foo")));
    ASSERT(Optional<Value>(LiteralStringRef("bar")) == val);
    std::cout << "Finish read";

    RangeResult result =
        wait(kvStore->readRange(KeyRangeRef("f"_sr, "g"_sr), 100, 500, IKeyValueStore::ReadType::NORMAL));
    std::cout << "Range result user " << result.toString();

    for (auto kv : result) {
        std::cout << "kv " << kv.key.toString() << " " << kv.value.toString() << "\n";
    }

    Future<Void> closed = kvStore->onClosed();
    kvStore->close();
    wait(closed);

    kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
    wait(kvStore->init());
    // Confirm that `init()` is idempotent.
    wait(kvStore->init());

    Optional<Value> val = wait(kvStore->readValue(LiteralStringRef("foo")));
    ASSERT(Optional<Value>(LiteralStringRef("bar")) == val);

    Future<Void> closed = kvStore->onClosed();
    kvStore->dispose();
    wait(closed);

    platform::eraseDirectoryRecursive(rocksDBTestDir);
    return Void();
}*/

TEST_CASE("RocksDBKVS/ReadRangeTest") {
	state std::string cwd = platform::getWorkingDirectory() + "/";
	std::cout << "Working directory: " << cwd << std::endl;
	state std::string rocksDBTestDir = "rocksdb-kvs-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	kvStore->addShard(KeyRangeRef("0"_sr, "100"_sr), deterministicRandom()->randomUniqueID());

	for (int i = 0; i < 100; ++i) {
		kvStore->set({ format("%03d", i), std::to_string(i) });
	}

	wait(kvStore->commit(false));

	// Point read
	state int i = 0;
	for (i = 0; i < 100; ++i) {
		Optional<Value> val = wait(kvStore->readValue(format("%03d", i)));
		ASSERT(val == Optional<Value>(std::to_string(i)));
	}

	// Range read
	// Read forward full range.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("0"_sr, "100"_sr), 1000, 10000, IKeyValueStore::ReadType::NORMAL));

	ASSERT_EQ(result.size(), 100);

	// Read backward full range.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("0"_sr, "100"_sr), -1000, 10000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 100);

	// Forward with row limit.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("005"_sr, "080"_sr), 20, 10000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 20);

	// Backward with row limit.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("005"_sr, "080"_sr), -20, 10000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 20);

	// Forward with partial range.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("005"_sr, "015"_sr), 20, 10000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 10);

	// Backward with partial range.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("005"_sr, "015"_sr), -20, 10000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 10);

	// Split shard.
	kvStore->addShard(KeyRangeRef("050"_sr, "060"_sr), deterministicRandom()->randomUniqueID());
	for (i = 50; i < 60; i += 2) {
		kvStore->set({ format("%03d", i), "Val: " + std::to_string(i) });
	}
	wait(kvStore->commit(false));

	// Forward with 2 shards.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("040"_sr, "060"_sr), 20, 1000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 15);

	// Backward with 2 shards.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("050"_sr, "080"_sr), -30, 1000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 25);

	// Forward, same shard, multiple range.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("005"_sr, "070"_sr), 100, 10000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 60);

	// Backward, same shard, multiple range.
	RangeResult result =
	    wait(kvStore->readRange(KeyRangeRef("020"_sr, "075"_sr), -100, 10000, IKeyValueStore::ReadType::NORMAL));
	ASSERT_EQ(result.size(), 50);

	Future<Void> closed = kvStore->onClosed();
	kvStore->dispose();
	wait(closed);

	return Void();
}

TEST_CASE("RocksDBKVS/multiRocks") {
	state std::string cwd = platform::getWorkingDirectory() + "/";
	std::cout << "Working directory: " << cwd << std::endl;
	state std::string rocksDBTestDir = "rocksdb-kvstore-reopen-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	state RocksDBKeyValueStore* rocksDB = dynamic_cast<RocksDBKeyValueStore*>(kvStore);
	wait(kvStore->init());

	rocksDB->addShard(KeyRangeRef("a"_sr, "b"_sr), deterministicRandom()->randomUniqueID());

	kvStore->set({ LiteralStringRef("a"), LiteralStringRef("bar") });
	wait(kvStore->commit(false));

	Optional<Value> val = wait(kvStore->readValue(LiteralStringRef("a")));
	ASSERT(Optional<Value>(LiteralStringRef("bar")) == val);

	Future<Void> closed = kvStore->onClosed();
	kvStore->dispose();
	wait(closed);

	return Void();
}

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL
