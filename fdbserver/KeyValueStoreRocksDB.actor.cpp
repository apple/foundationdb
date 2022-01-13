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
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/table_properties_collectors.h>
#include <rocksdb/version.h>
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

// Enforcing rocksdb version to be 6.22.1 or greater.
static_assert(ROCKSDB_MAJOR >= 6, "Unsupported rocksdb version. Update the rocksdb to 6.22.1 version");
static_assert(ROCKSDB_MAJOR == 6 ? ROCKSDB_MINOR >= 22 : true,
              "Unsupported rocksdb version. Update the rocksdb to 6.22.1 version");
static_assert((ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR == 22) ? ROCKSDB_PATCH >= 1 : true,
              "Unsupported rocksdb version. Update the rocksdb to 6.22.1 version");

namespace {
// Copied from SystemData.cpp
// TODO: Move all constants to a header file.
const KeyRef systemKeysPrefix = LiteralStringRef("\xff");
const KeyRangeRef normalKeys(KeyRef(), systemKeysPrefix);
const KeyRangeRef systemKeys(systemKeysPrefix, LiteralStringRef("\xff\xff"));
const KeyRangeRef allKeys = KeyRangeRef(normalKeys.begin, systemKeys.end);
const KeyRef afterAllKeys = LiteralStringRef("\xff\xff\x00");
const KeyRangeRef specialKeys = KeyRangeRef(LiteralStringRef("\xff\xff"), LiteralStringRef("\xff\xff\xff"));
// End System Data constants

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

	if (SERVER_KNOBS->ROCKSDB_BLOCK_CACHE_SIZE > 0) {
		bbOpts.block_cache = rocksdb::NewLRUCache(SERVER_KNOBS->ROCKSDB_BLOCK_CACHE_SIZE);
	}

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

int readRangeInDb(rocksdb::DB* db, const KeyRangeRef& range, int rowLimit, int byteLimit, RangeResult* result) {
	if (rowLimit == 0 || byteLimit == 0) {
		return 0;
	}

	int accumulatedRows = 0;
	int accumulatedBytes = 0;
	// TODO: Pass read timeout.
	int readRangeTimeout = SERVER_KNOBS->ROCKSDB_READ_RANGE_TIMEOUT;
	rocksdb::Status s;
	auto options = getReadOptions();
	uint64_t deadlineMircos = db->GetEnv()->NowMicros() + readRangeTimeout * 1000000;
	std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
	options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);

	// When using a prefix extractor, ensure that keys are returned in order even if they cross
	// a prefix boundary.
	options.auto_prefix_mode = (SERVER_KNOBS->ROCKSDB_PREFIX_LEN > 0);
	if (rowLimit >= 0) {
		auto endSlice = toSlice(range.end);
		options.iterate_upper_bound = &endSlice;
		auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
		cursor->Seek(toSlice(range.begin));
		while (cursor->Valid() && toStringRef(cursor->key()) < range.end) {
			KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
			++accumulatedRows;
			accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
			result->push_back_deep(result->arena(), kv);
			// Calling `cursor->Next()` is potentially expensive, so short-circut here just in case.
			if (accumulatedRows >= rowLimit || accumulatedBytes >= byteLimit) {
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
	} else {
		auto beginSlice = toSlice(range.begin);
		options.iterate_lower_bound = &beginSlice;
		auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
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
			if (accumulatedRows >= -rowLimit || accumulatedBytes >= byteLimit) {
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
	}

	if (!s.ok()) {
		logRocksDBError(s, "ReadRange");
		// The data wrriten to the arena is not erased, which will leave RangeResult in a dirty state. The RangeResult
		// should never be returned to user.
		return -1;
	}
	return accumulatedBytes;
}
ACTOR Future<Void> flowLockLogger(const FlowLock* readLock, const FlowLock* fetchLock) {
	loop {
		wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
		TraceEvent e(SevInfo, "RocksDBFlowLock");
		e.detail("ReadAvailable", readLock->available());
		e.detail("ReadActivePermits", readLock->activePermits());
		e.detail("ReadWaiters", readLock->waiters());
		e.detail("FetchAvailable", fetchLock->available());
		e.detail("FetchActivePermits", fetchLock->activePermits());
		e.detail("FetchWaiters", fetchLock->waiters());
	}
}

ACTOR Future<Void> rocksDBMetricLogger(std::shared_ptr<rocksdb::Statistics> statistics, rocksdb::DB* db) {
	state std::vector<std::tuple<const char*, uint32_t, uint64_t>> tickerStats = {
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
	state std::vector<std::pair<const char*, std::string>> propertyStats = {
		{ "NumCompactionsRunning", rocksdb::DB::Properties::kNumRunningCompactions },
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
	};
	loop {
		wait(delay(SERVER_KNOBS->ROCKSDB_METRICS_DELAY));
		TraceEvent e("RocksDBMetrics");
		for (auto& t : tickerStats) {
			auto& [name, ticker, cum] = t;
			uint64_t val = statistics->getTickerCount(ticker);
			e.detail(name, val - cum);
			cum = val;
		}

		for (auto& p : propertyStats) {
			auto& [name, property] = p;
			uint64_t stat = 0;
			ASSERT(db->GetIntProperty(property, &stat));
			e.detail(name, stat);
		}
	}
}

struct DataShard {
	DataShard(std::string name) : name(name) {}
	~DataShard() {
		if (db == nullptr)
			return;
		// Close DB
		auto s = db->Close();
		if (!s.ok()) {
			logRocksDBError(s, "CloseShard");
			return;
		}
		TraceEvent(SevInfo, "RocksDB").detail("Method", "CloseShard").detail("Name", name);

		if (!deletePending)
			return;

		std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{ "default",
			                                                                                        getCFOptions() } };
		s = rocksdb::DestroyDB(name, getOptions(), defaultCF);
		if (!s.ok()) {
			logRocksDBError(s, "DestroyShard");
		} else {
			TraceEvent(SevInfo, "RocksDB").detail("Name", name).detail("Method", "DestroyShard");
		}
	}

	rocksdb::DB* db = nullptr;
	rocksdb::WriteBatch writeBatch;
	std::string name;
	bool deletePending = false;
	bool isSpecialKeysShard = false;
};

struct RocksDBKeyValueStore : IKeyValueStore {
	using DB = rocksdb::DB*;
	using CF = rocksdb::ColumnFamilyHandle*;
	using ShardMap = KeyRangeMap<std::shared_ptr<DataShard>>;

	struct Writer : IThreadPoolReceiver {
		DB& db;
		UID id;
		Reference<Histogram> commitLatencyHistogram;
		Reference<Histogram> commitActionHistogram;
		Reference<Histogram> commitQueueWaitHistogram;
		Reference<Histogram> writeHistogram;
		Reference<Histogram> deleteCompactRangeHistogram;

		explicit Writer(DB& db, UID id)
		  : db(db), id(id), commitLatencyHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                                   ROCKSDB_COMMIT_LATENCY_HISTOGRAM,
		                                                                   Histogram::Unit::microseconds)),
		    commitActionHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                  ROCKSDB_COMMIT_ACTION_HISTOGRAM,
		                                                  Histogram::Unit::microseconds)),
		    commitQueueWaitHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                     ROCKSDB_COMMIT_QUEUEWAIT_HISTOGRAM,
		                                                     Histogram::Unit::microseconds)),
		    writeHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                           ROCKSDB_WRITE_HISTOGRAM,
		                                           Histogram::Unit::microseconds)),
		    deleteCompactRangeHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                        ROCKSDB_DELETE_COMPACTRANGE_HISTOGRAM,
		                                                        Histogram::Unit::microseconds)) {}

		~Writer() override {
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
			ShardMap* shardMap;

			OpenAction(std::string path,
			           std::string dataPath,
			           Optional<Future<Void>>& metrics,
			           const FlowLock* readLock,
			           const FlowLock* fetchLock,
			           ShardMap* shardMap)
			  : path(std::move(path)), dataPath(std::move(dataPath)), metrics(metrics), readLock(readLock),
			    fetchLock(fetchLock), shardMap(shardMap) {
				ASSERT(shardMap);
			}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(OpenAction& a) {
			std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
				"default", getCFOptions() } };
			std::vector<rocksdb::ColumnFamilyHandle*> handle;
			auto options = getOptions();
			rocksdb::Status status = rocksdb::DB::Open(options, a.path, defaultCF, &handle, &db);
			if (!status.ok()) {
				logRocksDBError(status, "Open");
				a.done.sendError(statusToError(status));
				return;
			}
			auto defaultShard = std::make_shared<DataShard>(a.path);
			defaultShard->db = db;
			defaultShard->isSpecialKeysShard = true;

			// The current thread and main thread are same when the code runs in simulation.
			// blockUntilReady() is getting the thread into deadlock state, so avoiding the
			// metric logger in simulation.
			if (!g_network->isSimulated()) {
				onMainThread([&] {
					a.metrics = rocksDBMetricLogger(options.statistics, db) && flowLockLogger(a.readLock, a.fetchLock);
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

			// Add server metadata key space to default shard.
			a.shardMap->insert(specialKeys, defaultShard);

			auto mappingRange = prefixRange(persistShardMappingPrefix);
			RangeResult rangeResult;
			readRangeInDb(db, mappingRange, /* rowLimit= */ UINT16_MAX, /* byteLimit = */ UINT16_MAX, &rangeResult);

			auto shards = decodeShardMapping(rangeResult, persistShardMappingPrefix);

			for (const auto& [range, name] : shards) {
				auto shard = std::make_shared<DataShard>(name);
				rocksdb::DB* shardDb;
				status = rocksdb::DB::Open(options, name, defaultCF, &handle, &shardDb);
				if (!status.ok()) {
					logRocksDBError(status, "OpenShard");
					a.done.sendError(statusToError(status));
					return;
				}
				shard->db = shardDb;
				a.shardMap->insert(range, shard);
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
			std::set<std::shared_ptr<DataShard>>* dirtyShards;

			RestoreDurableStateAction(std::string path,
			                          std::string dataPath,
			                          ShardMap* shardMap,
			                          std::set<std::shared_ptr<DataShard>>* dirtyShards)
			  : path(std::move(path)), dataPath(std::move(dataPath)), shardMap(shardMap), dirtyShards(dirtyShards) {
				ASSERT(shardMap);
			}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};

		void action(RestoreDurableStateAction& a) {
			ASSERT(db); // Database should have been created.

			std::cout << "Restore durable state\n";
			if (a.dirtyShards) {
				// Do not destroy shard
				for (auto shard : *a.dirtyShards) {
					shard->deletePending = false;
				}
				a.dirtyShards->clear(); // All the shards should have been closed.
			}

			// Read and reload metadata.
			auto mappingRange = prefixRange(persistShardMappingPrefix);
			RangeResult rangeResult;
			readRangeInDb(db, mappingRange, /* rowLimit= */ UINT16_MAX, /* byteLimit = */ UINT16_MAX, &rangeResult);

			auto shards = decodeShardMapping(rangeResult, persistShardMappingPrefix);
			std::unordered_map<std::string, std::shared_ptr<DataShard>> openShards;
			for (const auto& [range, name] : shards) {
				openShards[name] = nullptr;
			}

			std::cout << "Num persisted shards: " << openShards.size() << "\n";

			std::shared_ptr<DataShard> specialKeysShard;
			for (auto it : a.shardMap->ranges()) {
				if (!it.value() || !it.value()->db)
					continue;

				if (it.value()->isSpecialKeysShard) {
					specialKeysShard = it.value();
					continue;
				}

				auto name = it.value()->db->GetName();

				if (openShards.find(name) != openShards.end()) {
					openShards[name] = it.value();
				} else {
					it.value()->deletePending = true;
				}
			}

			a.shardMap->insert(allKeys, nullptr);
			a.shardMap->insert(specialKeys, specialKeysShard);

			auto options = getOptions();
			std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
				"default", getCFOptions() } };
			std::vector<rocksdb::ColumnFamilyHandle*> handle;
			for (const auto& [range, name] : shards) {
				auto shard = openShards[name];
				if (shard == nullptr) {
					shard = std::make_shared<DataShard>(name);
					auto status = rocksdb::DB::Open(options, name, defaultCF, &handle, &shard->db);
					if (!status.ok()) {
						logRocksDBError(status, "RestoreShard");
						a.done.sendError(statusToError(status));
						return;
					}
				}
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
			std::unique_ptr<std::set<std::shared_ptr<DataShard>>> dirtyShards;
			ThreadReturnPromise<Void> done;
			double startTime;
			bool getHistograms;
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
			CommitAction() {
				if (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) {
					getHistograms = true;
					startTime = timer_monotonic();
				} else {
					getHistograms = false;
				}
			}
		};

		rocksdb::Status doCommit(rocksdb::WriteBatch* batch, rocksdb::DB* db, bool sample) {
			// std::cout << "Committing in db " << db->GetName() << std::endl;
			Standalone<VectorRef<KeyRangeRef>> deletes;
			DeleteVisitor dv(deletes, deletes.arena());
			ASSERT(batch->Iterate(&dv).ok());
			// If there are any range deletes, we should have added them to be deleted.
			ASSERT(!deletes.empty() || !batch->HasDeleteRange());
			rocksdb::WriteOptions options;
			options.sync = !SERVER_KNOBS->ROCKSDB_UNSAFE_AUTO_FSYNC;

			double writeBeginTime = sample ? timer_monotonic() : 0;
			auto s = db->Write(options, batch);
			if (sample) {
				writeHistogram->sampleSeconds(timer_monotonic() - writeBeginTime);
			}
			if (!s.ok()) {
				logRocksDBError(s, "Commit");
				return s;
			}

			double compactRangeBeginTime = sample ? timer_monotonic() : 0;
			for (const auto& keyRange : deletes) {
				auto begin = toSlice(keyRange.begin);
				auto end = toSlice(keyRange.end);
				ASSERT(db->SuggestCompactRange(db->DefaultColumnFamily(), &begin, &end).ok());
			}
			if (sample) {
				deleteCompactRangeHistogram->sampleSeconds(timer_monotonic() - compactRangeBeginTime);
			}
			return s;
		}

		void action(CommitAction& a) {
			double commitBeginTime;
			if (a.getHistograms) {
				commitBeginTime = timer_monotonic();
				commitQueueWaitHistogram->sampleSeconds(commitBeginTime - a.startTime);
			}

			rocksdb::Status s;
			std::shared_ptr<DataShard> specialKeysShard;

			for (auto shard : *(a.dirtyShards)) {
				if (shard->isSpecialKeysShard) {
					specialKeysShard = shard;
					continue;
				}

				if (shard->deletePending) {
					// Destroy shard.
					continue;
				}
				if (!shard->db) {
					// create DB
					std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
						"default", getCFOptions() } };
					std::vector<rocksdb::ColumnFamilyHandle*> handle;
					auto options = getOptions();
					rocksdb::Status status = rocksdb::DB::Open(options, shard->name, defaultCF, &handle, &shard->db);
					if (!status.ok()) {
						logRocksDBError(status, "AddShard");
						a.done.sendError(statusToError(status));
						return;
					}
					TraceEvent(SevInfo, "RocksDB").detail("Method", "OpenShard").detail("Name", shard->name);
				}

				s = doCommit(&shard->writeBatch, shard->db, a.getHistograms);
				shard->writeBatch.Clear();
				if (!s.ok()) {
					a.done.sendError(statusToError(s));
					return;
				}
			}

			if (a.getHistograms) {
				double currTime = timer_monotonic();
				commitActionHistogram->sampleSeconds(currTime - commitBeginTime);
				commitLatencyHistogram->sampleSeconds(currTime - a.startTime);
			}

			// System mutation needs to be committed after all other mutations.
			if (specialKeysShard) {
				s = doCommit(&specialKeysShard->writeBatch, specialKeysShard->db, a.getHistograms);
				if (!s.ok()) {
					a.done.sendError(statusToError(s));
					return;
				}
				specialKeysShard->writeBatch.Clear();
			}

			// Destroy all the delete pending shards.
			a.dirtyShards->clear();

			// TODO: Destroy all delete pending shards.
			a.done.send(Void());
		}

		struct CloseAction : TypedAction<Writer, CloseAction> {
			ThreadReturnPromise<Void> done;
			std::string path;
			std::string dataPath;
			ShardMap* shardMap;
			bool deleteOnClose;
			CloseAction(std::string path, std::string dataPath, ShardMap* shardMap, bool deleteOnClose)
			  : path(path), dataPath(dataPath), shardMap(shardMap), deleteOnClose(deleteOnClose) {}
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
						it.value()->deletePending = true;
					}
				}
			}

			// Close and delete shard if needed.
			a.shardMap->clear();

			TraceEvent(SevInfo, "RocksDB").detail("Path", a.path).detail("Method", "Close");
			a.done.send(Void());
		}
	};

	struct Reader : IThreadPoolReceiver {
		DB& db;
		double readValueTimeout;
		double readValuePrefixTimeout;
		double readRangeTimeout;
		Reference<Histogram> readRangeLatencyHistogram;
		Reference<Histogram> readValueLatencyHistogram;
		Reference<Histogram> readPrefixLatencyHistogram;
		Reference<Histogram> readRangeActionHistogram;
		Reference<Histogram> readValueActionHistogram;
		Reference<Histogram> readPrefixActionHistogram;
		Reference<Histogram> readRangeQueueWaitHistogram;
		Reference<Histogram> readValueQueueWaitHistogram;
		Reference<Histogram> readPrefixQueueWaitHistogram;
		Reference<Histogram> readRangeNewIteratorHistogram;
		Reference<Histogram> readValueGetHistogram;
		Reference<Histogram> readPrefixGetHistogram;

		explicit Reader(DB& db)
		  : db(db), readRangeLatencyHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                              ROCKSDB_READRANGE_LATENCY_HISTOGRAM,
		                                                              Histogram::Unit::microseconds)),
		    readValueLatencyHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                      ROCKSDB_READVALUE_LATENCY_HISTOGRAM,
		                                                      Histogram::Unit::microseconds)),
		    readPrefixLatencyHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                       ROCKSDB_READPREFIX_LATENCY_HISTOGRAM,
		                                                       Histogram::Unit::microseconds)),
		    readRangeActionHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                     ROCKSDB_READRANGE_ACTION_HISTOGRAM,
		                                                     Histogram::Unit::microseconds)),
		    readValueActionHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                     ROCKSDB_READVALUE_ACTION_HISTOGRAM,
		                                                     Histogram::Unit::microseconds)),
		    readPrefixActionHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                      ROCKSDB_READPREFIX_ACTION_HISTOGRAM,
		                                                      Histogram::Unit::microseconds)),
		    readRangeQueueWaitHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                        ROCKSDB_READRANGE_QUEUEWAIT_HISTOGRAM,
		                                                        Histogram::Unit::microseconds)),
		    readValueQueueWaitHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                        ROCKSDB_READVALUE_QUEUEWAIT_HISTOGRAM,
		                                                        Histogram::Unit::microseconds)),
		    readPrefixQueueWaitHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                         ROCKSDB_READPREFIX_QUEUEWAIT_HISTOGRAM,
		                                                         Histogram::Unit::microseconds)),
		    readRangeNewIteratorHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                          ROCKSDB_READRANGE_NEWITERATOR_HISTOGRAM,
		                                                          Histogram::Unit::microseconds)),
		    readValueGetHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                  ROCKSDB_READVALUE_GET_HISTOGRAM,
		                                                  Histogram::Unit::microseconds)),
		    readPrefixGetHistogram(Histogram::getHistogram(ROCKSDBSTORAGE_HISTOGRAM_GROUP,
		                                                   ROCKSDB_READPREFIX_GET_HISTOGRAM,
		                                                   Histogram::Unit::microseconds)) {
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
			ThreadReturnPromise<Optional<Value>> result;

			ReadValueAction(KeyRef key, rocksdb::DB* instance, Optional<UID> debugID)
			  : key(key), instance(instance), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) ? true : false) {
			}

			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};

		void action(ReadValueAction& a) {
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				readValueQueueWaitHistogram->sampleSeconds(readBeginTime - a.startTime);
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
				readValueGetHistogram->sampleSeconds(timer_monotonic() - dbGetBeginTime);
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
				readValueActionHistogram->sampleSeconds(currTime - readBeginTime);
				readValueLatencyHistogram->sampleSeconds(currTime - a.startTime);
			}
		}

		struct ReadValuePrefixAction : TypedAction<Reader, ReadValuePrefixAction> {
			Key key;
			int maxLength;
			rocksdb::DB* instance;
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			ThreadReturnPromise<Optional<Value>> result;
			ReadValuePrefixAction(Key key, int maxLength, rocksdb::DB* instance, Optional<UID> debugID)
			  : key(key), maxLength(maxLength), instance(instance), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms((deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE)
			                      ? true
			                      : false){};
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};
		void action(ReadValuePrefixAction& a) {
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				readPrefixQueueWaitHistogram->sampleSeconds(readBeginTime - a.startTime);
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
				readPrefixGetHistogram->sampleSeconds(timer_monotonic() - dbGetBeginTime);
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
				readPrefixActionHistogram->sampleSeconds(currTime - readBeginTime);
				readPrefixLatencyHistogram->sampleSeconds(currTime - a.startTime);
			}
		}

		struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			KeyRange keys;
			std::vector<rocksdb::DB*> instances;
			int rowLimit, byteLimit;
			double startTime;
			bool getHistograms;
			ThreadReturnPromise<RangeResult> result;
			ReadRangeAction(KeyRange keys, std::vector<rocksdb::DB*> instances, int rowLimit, int byteLimit)
			  : keys(keys), instances(instances), rowLimit(rowLimit), byteLimit(byteLimit),
			    startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < SERVER_KNOBS->ROCKSDB_HISTOGRAMS_SAMPLE_RATE) ? true : false) {
			}
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
		};
		void action(ReadRangeAction& a) {
			double readBeginTime = timer_monotonic();
			if (a.getHistograms) {
				readRangeQueueWaitHistogram->sampleSeconds(readBeginTime - a.startTime);
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
			for (auto* instance : a.instances) {
				auto bytesRead = readRangeInDb(instance, a.keys, rowLimit, byteLimit, &result);
				if (bytesRead < 0) {
					// Error reading an instance.
					a.result.sendError(internal_error());
					return;
				}
				if (result.size() >= abs(a.rowLimit) || bytesRead >= byteLimit) {
					break;
				}
				byteLimit -= bytesRead;
			}
			result.more =
			    (result.size() == a.rowLimit) || (result.size() == -a.rowLimit) || (accumulatedBytes >= a.byteLimit);
			if (result.more) {
				result.readThrough = result[result.size() - 1].key;
			}
			a.result.send(result);
			if (a.getHistograms) {
				double currTime = timer_monotonic();
				readRangeActionHistogram->sampleSeconds(currTime - readBeginTime);
				readRangeLatencyHistogram->sampleSeconds(currTime - a.startTime);
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
	    shardMap(nullptr, specialKeys.end) {
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
		writeThread->addThread(new Writer(db, id), "fdb-rocksdb-wr");
		for (unsigned i = 0; i < SERVER_KNOBS->ROCKSDB_READ_PARALLELISM; ++i) {
			readThreads->addThread(new Reader(db), "fdb-rocksdb-re");
		}
	}

	Future<Void> getError() const override { return errorPromise.getFuture(); }

	ACTOR static void doClose(RocksDBKeyValueStore* self, bool deleteOnClose) {
		// The metrics future retains a reference to the DB, so stop it before we delete it.
		self->metrics.reset();

		wait(self->readThreads->stop());
		auto a = new Writer::CloseAction(self->path, self->dataPath, &self->shardMap, deleteOnClose);
		auto f = a->done.getFuture();
		self->writeThread->post(a);
		wait(f);
		wait(self->writeThread->stop());
		if (self->closePromise.canBeSet())
			self->closePromise.send(Void());
		if (self->errorPromise.canBeSet())
			self->errorPromise.send(Never());
		delete self;
	}

	Future<Void> onClosed() const override { return closePromise.getFuture(); }

	void dispose() override { doClose(this, true); }

	void close() override { doClose(this, false); }

	KeyValueStoreType getType() const override { return KeyValueStoreType(KeyValueStoreType::SSD_ROCKSDB_V1); }

	Future<Void> init() override {
		if (openFuture.isValid()) {
			// Restore durable state if KVS is open. KVS will be re-initialized during rollback. To avoid the cost of
			// opening and closing multiple rocksdb instances, we reconcile the shard map using persist shard mapping
			// data.
			auto a = std::make_unique<Writer::RestoreDurableStateAction>(path, dataPath, &shardMap, dirtyShards.get());

			Future<Void> future = a->done.getFuture();
			writeThread->post(a.release());
			return future;
		} else {
			auto a = std::make_unique<Writer::OpenAction>(
			    path, dataPath, metrics, &readSemaphore, &fetchSemaphore, &shardMap);
			openFuture = a->done.getFuture();
			writeThread->post(a.release());
			return openFuture;
		}
	}

	void set(KeyValueRef kv, const Arena*) override {
		if (dirtyShards == nullptr) {
			dirtyShards.reset(new std::set<std::shared_ptr<DataShard>>());
		}

		auto it = shardMap.rangeContaining(kv.key);
		if (it.value() == nullptr) {
			std::cout << "Write to non exist shard " << kv.key.toString() << ", " << it.range().begin.toString() << " : " << it.range().end.toString();
			ASSERT(it.value()); // Non-exist shard.
		}

		it.value()->writeBatch.Put(toSlice(kv.key), toSlice(kv.value));

		dirtyShards->insert(it.value());
	}

	void clear(KeyRangeRef range, const Arena*) override {
		if (dirtyShards == nullptr) {
			dirtyShards.reset(new std::set<std::shared_ptr<DataShard>>());
		}

		auto rangeIterator = shardMap.intersectingRanges(range);

		// Get DB instances
		for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
			if (it.value() == nullptr)
				continue;

			if (range.singleKeyRange()) {
				it.value()->writeBatch.Delete(toSlice(range.begin));
			} else {
				it.value()->writeBatch.DeleteRange(toSlice(range.begin), toSlice(range.end));
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
		a->dirtyShards = std::move(dirtyShards);
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
		if (it.value() == nullptr || it.value()->db == nullptr) {
			// TODO: Add new error code, e.g., shard not found.
			// a.result.sendError(internal_error());
			// return internal_error();
			std::cout << "Read non-exist shard " << key.toString() << "\n";
			return Optional<Value>();
		}

		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValueAction(key, it.value()->db, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValueAction>(key, it.value()->db, debugID);
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
			return internal_error();
		}

		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValuePrefixAction(key, maxLength, it.value()->db, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);

		auto a = std::make_unique<Reader::ReadValuePrefixAction>(key, maxLength, it.value()->db, debugID);
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
		std::vector<rocksdb::DB*> instances;
		for (auto it = rangeIterator.begin(); it != rangeIterator.end(); ++it) {
			if (it.value() == nullptr || it.value()->db == nullptr)
				continue;

			instances.push_back(it.value()->db);
		}

		if (!shouldThrottle(type, keys.begin)) {
			auto a = new Reader::ReadRangeAction(keys, instances, rowLimit, byteLimit);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;
		checkWaiters(semaphore, maxWaiters);

		auto a = std::make_unique<Reader::ReadRangeAction>(keys, instances, rowLimit, byteLimit);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
	}

	StorageBytes getStorageBytes() const override {
		uint64_t live = 0;
		ASSERT(db->GetIntProperty(rocksdb::DB::Properties::kLiveSstFilesSize, &live));

		int64_t free;
		int64_t total;
		g_network->getDiskBytes(path, free, total);

		return StorageBytes(free, total, live, free);
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
		Standalone<VectorRef<MutationRef>> refs;

		auto shard = shardMap.rangeContaining(range.begin);
		ASSERT(shard.range() == range);
		ASSERT(shard.value());

		std::cout << "get shard mutations " << range.begin.toString() << " " << range.end.toString() << "\n";
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

		// TODO: should we include sys key range here?
		if (range.end < allKeys.end) {
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

		for (auto& mutation : refs) {
			std::cout << "mutation ref " << mutation.toString() << "\n";
		}
		return refs;
	}

	Standalone<VectorRef<MutationRef>> getDisposeRangeMutations(KeyRangeRef range) override {
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

		auto shards = shardMap.intersectingRanges(range);

		for (auto shard : shards) {
			if (!shard.value())
				continue;

			if (range.contains(shard.range())) {
				shard.value()->deletePending = true;
				dirtyShards->insert(shard.value());
			} else {
				// truncate shard
			}
		}

		shardMap.insert(range, nullptr);

		TraceEvent(SevInfo, "RocksDB").detail("Method", "DisposeRange").detail("Begin", range.begin.toString()).detail("End", range.end.toString());
	}

	DB db = nullptr;
	std::string path;
	const std::string dataPath;
	UID id;
	ShardMap shardMap;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> readThreads;
	Promise<Void> errorPromise;
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
}

TEST_CASE("RocksDBKVS/multiRocks") {
	state std::string cwd = platform::getWorkingDirectory() + "/";
	std::cout << "Working directory: " << cwd << std::endl;
	state std::string rocksDBTestDir = "rocksdb-kvstore-reopen-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	state RocksDBKeyValueStore* rocksDB = dynamic_cast<RocksDBKeyValueStore*>(kvStore);
	wait(kvStore->init());

	for (auto* rocks : rocksDB->getAllInstances()) {
		std::cout << "Rocks: " << rocks->GetName() << std::endl;
	}

	rocksDB->addShard(KeyRangeRef("a"_sr, "b"_sr), deterministicRandom()->randomUniqueID());

	for (auto* rocks : rocksDB->getAllInstances()) {
		std::cout << "Rocks: " << rocks->GetName() << std::endl;
	}

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
