#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/version.h>
#include <rocksdb/utilities/table_properties_collectors.h>
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

struct RocksDBKeyValueStore : IKeyValueStore {
	using DB = rocksdb::DB*;
	using CF = rocksdb::ColumnFamilyHandle*;

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
			ThreadReturnPromise<Void> done;
			Optional<Future<Void>>& metrics;
			const FlowLock* readLock;
			const FlowLock* fetchLock;
			OpenAction(std::string path,
			           Optional<Future<Void>>& metrics,
			           const FlowLock* readLock,
			           const FlowLock* fetchLock)
			  : path(std::move(path)), metrics(metrics), readLock(readLock), fetchLock(fetchLock) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(OpenAction& a) {
			std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
				"default", getCFOptions() } };
			std::vector<rocksdb::ColumnFamilyHandle*> handle;
			auto options = getOptions();
			auto status = rocksdb::DB::Open(options, a.path, defaultCF, &handle, &db);
			if (!status.ok()) {
				logRocksDBError(status, "Open");
				a.done.sendError(statusToError(status));
			} else {
				TraceEvent(SevInfo, "RocksDB").detail("Path", a.path).detail("Method", "Open");
				if (g_network->isSimulated()) {
					// The current thread and main thread are same when the code runs in simulation.
					// blockUntilReady() is getting the thread into deadlock state, so directly calling
					// the metricsLogger.
					a.metrics = rocksDBMetricLogger(options.statistics, db) && flowLockLogger(a.readLock, a.fetchLock);
				} else {
					onMainThread([&] {
						a.metrics =
						    rocksDBMetricLogger(options.statistics, db) && flowLockLogger(a.readLock, a.fetchLock);
						return Future<bool>(true);
					}).blockUntilReady();
				}
				a.done.send(Void());
			}
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
			std::unique_ptr<rocksdb::WriteBatch> batchToCommit;
			ThreadReturnPromise<Void> done;
			double startTime;
			bool getHistograms;
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
			CommitAction() {
				if (deterministicRandom()->random01() < 0.001) {
					getHistograms = true;
					startTime = timer_monotonic();
				} else {
					getHistograms = false;
				}
			}
		};
		void action(CommitAction& a) {
			double commitBeginTime;
			if (a.getHistograms) {
				commitBeginTime = timer_monotonic();
				commitQueueWaitHistogram->sampleSeconds(commitBeginTime - a.startTime);
			}
			Standalone<VectorRef<KeyRangeRef>> deletes;
			DeleteVisitor dv(deletes, deletes.arena());
			ASSERT(a.batchToCommit->Iterate(&dv).ok());
			// If there are any range deletes, we should have added them to be deleted.
			ASSERT(!deletes.empty() || !a.batchToCommit->HasDeleteRange());
			rocksdb::WriteOptions options;
			options.sync = !SERVER_KNOBS->ROCKSDB_UNSAFE_AUTO_FSYNC;

			double writeBeginTime = a.getHistograms ? timer_monotonic() : 0;
			auto s = db->Write(options, a.batchToCommit.get());
			if (a.getHistograms) {
				writeHistogram->sampleSeconds(timer_monotonic() - writeBeginTime);
			}

			if (!s.ok()) {
				logRocksDBError(s, "Commit");
				a.done.sendError(statusToError(s));
			} else {
				a.done.send(Void());

				double compactRangeBeginTime = a.getHistograms ? timer_monotonic() : 0;
				for (const auto& keyRange : deletes) {
					auto begin = toSlice(keyRange.begin);
					auto end = toSlice(keyRange.end);
					ASSERT(db->SuggestCompactRange(db->DefaultColumnFamily(), &begin, &end).ok());
				}
				if (a.getHistograms) {
					deleteCompactRangeHistogram->sampleSeconds(timer_monotonic() - compactRangeBeginTime);
				}
			}
			if (a.getHistograms) {
				double currTime = timer_monotonic();
				commitActionHistogram->sampleSeconds(currTime - commitBeginTime);
				commitLatencyHistogram->sampleSeconds(currTime - a.startTime);
			}
		}

		struct CloseAction : TypedAction<Writer, CloseAction> {
			ThreadReturnPromise<Void> done;
			std::string path;
			bool deleteOnClose;
			CloseAction(std::string path, bool deleteOnClose) : path(path), deleteOnClose(deleteOnClose) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(CloseAction& a) {
			if (db == nullptr) {
				a.done.send(Void());
				return;
			}
			auto s = db->Close();
			if (!s.ok()) {
				logRocksDBError(s, "Close");
			}
			if (a.deleteOnClose) {
				std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
					"default", getCFOptions() } };
				s = rocksdb::DestroyDB(a.path, getOptions(), defaultCF);
				if (!s.ok()) {
					logRocksDBError(s, "Destroy");
				} else {
					TraceEvent(SevInfo, "RocksDB").detail("Path", a.path).detail("Method", "Destroy");
				}
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
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			ThreadReturnPromise<Optional<Value>> result;
			ReadValueAction(KeyRef key, Optional<UID> debugID)
			  : key(key), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < 0.001) ? true : false) {
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
			    db->GetEnv()->NowMicros() + (readValueTimeout - (readBeginTime - a.startTime)) * 1000000;
			std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
			options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);

			double dbGetBeginTime = a.getHistograms ? timer_monotonic() : 0;
			auto s = db->Get(options, db->DefaultColumnFamily(), toSlice(a.key), &value);
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
			Optional<UID> debugID;
			double startTime;
			bool getHistograms;
			ThreadReturnPromise<Optional<Value>> result;
			ReadValuePrefixAction(Key key, int maxLength, Optional<UID> debugID)
			  : key(key), maxLength(maxLength), debugID(debugID), startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < 0.001) ? true : false) {
			}
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
			uint64_t deadlineMircos =
			    db->GetEnv()->NowMicros() + (readValuePrefixTimeout - (readBeginTime - a.startTime)) * 1000000;
			std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
			options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);

			double dbGetBeginTime = a.getHistograms ? timer_monotonic() : 0;
			auto s = db->Get(options, db->DefaultColumnFamily(), toSlice(a.key), &value);
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
			int rowLimit, byteLimit;
			double startTime;
			bool getHistograms;
			ThreadReturnPromise<RangeResult> result;
			ReadRangeAction(KeyRange keys, int rowLimit, int byteLimit)
			  : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit), startTime(timer_monotonic()),
			    getHistograms(
			        (deterministicRandom()->random01() < 0.001) ? true : false) {
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

			RangeResult result;
			if (a.rowLimit == 0 || a.byteLimit == 0) {
				a.result.send(result);
			}
			int accumulatedBytes = 0;
			rocksdb::Status s;
			auto options = getReadOptions();
			uint64_t deadlineMircos =
			    db->GetEnv()->NowMicros() + (readRangeTimeout - (readBeginTime - a.startTime)) * 1000000;
			std::chrono::seconds deadlineSeconds(deadlineMircos / 1000000);
			options.deadline = std::chrono::duration_cast<std::chrono::microseconds>(deadlineSeconds);
			// When using a prefix extractor, ensure that keys are returned in order even if they cross
			// a prefix boundary.
			options.auto_prefix_mode = (SERVER_KNOBS->ROCKSDB_PREFIX_LEN > 0);
			if (a.rowLimit >= 0) {
				auto endSlice = toSlice(a.keys.end);
				options.iterate_upper_bound = &endSlice;

				double iterCreationBeginTime = a.getHistograms ? timer_monotonic() : 0;
				auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
				if (a.getHistograms) {
					readRangeNewIteratorHistogram->sampleSeconds(timer_monotonic() - iterCreationBeginTime);
				}

				cursor->Seek(toSlice(a.keys.begin));
				while (cursor->Valid() && toStringRef(cursor->key()) < a.keys.end) {
					KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back_deep(result.arena(), kv);
					// Calling `cursor->Next()` is potentially expensive, so short-circut here just in case.
					if (result.size() >= a.rowLimit || accumulatedBytes >= a.byteLimit) {
						break;
					}
					if (timer_monotonic() - a.startTime > readRangeTimeout) {
						TraceEvent(SevWarn, "RocksDBError")
						    .detail("Error", "Read range request timedout")
						    .detail("Method", "ReadRangeAction")
						    .detail("Timeout value", readRangeTimeout);
						a.result.sendError(transaction_too_old());
						return;
					}
					cursor->Next();
				}
				s = cursor->status();
			} else {
				auto beginSlice = toSlice(a.keys.begin);
				options.iterate_lower_bound = &beginSlice;

				double iterCreationBeginTime = a.getHistograms ? timer_monotonic() : 0;
				auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
				if (a.getHistograms) {
					readRangeNewIteratorHistogram->sampleSeconds(timer_monotonic() - iterCreationBeginTime);
				}

				cursor->SeekForPrev(toSlice(a.keys.end));
				if (cursor->Valid() && toStringRef(cursor->key()) == a.keys.end) {
					cursor->Prev();
				}
				while (cursor->Valid() && toStringRef(cursor->key()) >= a.keys.begin) {
					KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back_deep(result.arena(), kv);
					// Calling `cursor->Prev()` is potentially expensive, so short-circut here just in case.
					if (result.size() >= -a.rowLimit || accumulatedBytes >= a.byteLimit) {
						break;
					}
					if (timer_monotonic() - a.startTime > readRangeTimeout) {
						TraceEvent(SevWarn, "RocksDBError")
						    .detail("Error", "Read range request timedout")
						    .detail("Method", "ReadRangeAction")
						    .detail("Timeout value", readRangeTimeout);
						a.result.sendError(transaction_too_old());
						return;
					}
					cursor->Prev();
				}
				s = cursor->status();
			}

			if (!s.ok()) {
				logRocksDBError(s, "ReadRange");
				a.result.sendError(statusToError(s));
				return;
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

	DB db = nullptr;
	std::string path;
	UID id;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> readThreads;
	Promise<Void> errorPromise;
	Promise<Void> closePromise;
	Future<Void> openFuture;
	std::unique_ptr<rocksdb::WriteBatch> writeBatch;
	Optional<Future<Void>> metrics;
	FlowLock readSemaphore;
	int numReadWaiters;
	FlowLock fetchSemaphore;
	int numFetchWaiters;

	struct Counters {
		CounterCollection cc;
		Counter immediateThrottle;
		Counter failedToAcquire;

		Counters()
		  : cc("RocksDBThrottle"), immediateThrottle("ImmediateThrottle", cc), failedToAcquire("failedToAcquire", cc) {}
	};

	Counters counters;

	explicit RocksDBKeyValueStore(const std::string& path, UID id)
	  : path(path), id(id), readSemaphore(SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    fetchSemaphore(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX),
	    numReadWaiters(SERVER_KNOBS->ROCKSDB_READ_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_READ_QUEUE_SOFT_MAX),
	    numFetchWaiters(SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_HARD_MAX - SERVER_KNOBS->ROCKSDB_FETCH_QUEUE_SOFT_MAX) {
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
		auto a = new Writer::CloseAction(self->path, deleteOnClose);
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
			return openFuture;
		}
		auto a = std::make_unique<Writer::OpenAction>(path, metrics, &readSemaphore, &fetchSemaphore);
		openFuture = a->done.getFuture();
		writeThread->post(a.release());
		return openFuture;
	}

	void set(KeyValueRef kv, const Arena*) override {
		if (writeBatch == nullptr) {
			writeBatch.reset(new rocksdb::WriteBatch());
		}
		writeBatch->Put(toSlice(kv.key), toSlice(kv.value));
	}

	void clear(KeyRangeRef keyRange, const Arena*) override {
		if (writeBatch == nullptr) {
			writeBatch.reset(new rocksdb::WriteBatch());
		}

		writeBatch->DeleteRange(toSlice(keyRange.begin), toSlice(keyRange.end));
	}

	Future<Void> commit(bool) override {
		// If there is nothing to write, don't write.
		if (writeBatch == nullptr) {
			return Void();
		}
		auto a = new Writer::CommitAction();
		a->batchToCommit = std::move(writeBatch);
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
		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValueAction(key, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValueAction>(key, debugID);
		return read(a.release(), &semaphore, readThreads.getPtr(), &counters.failedToAcquire);
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        IKeyValueStore::ReadType type,
	                                        Optional<UID> debugID) override {
		if (!shouldThrottle(type, key)) {
			auto a = new Reader::ReadValuePrefixAction(key, maxLength, debugID);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadValuePrefixAction>(key, maxLength, debugID);
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
		if (!shouldThrottle(type, keys.begin)) {
			auto a = new Reader::ReadRangeAction(keys, rowLimit, byteLimit);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}

		auto& semaphore = (type == IKeyValueStore::ReadType::FETCH) ? fetchSemaphore : readSemaphore;
		int maxWaiters = (type == IKeyValueStore::ReadType::FETCH) ? numFetchWaiters : numReadWaiters;

		checkWaiters(semaphore, maxWaiters);
		auto a = std::make_unique<Reader::ReadRangeAction>(keys, rowLimit, byteLimit);
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

TEST_CASE("noSim/fdbserver/KeyValueStoreRocksDB/Reopen") {
	state const std::string rocksDBTestDir = "rocksdb-kvstore-reopen-test-db";
	platform::eraseDirectoryRecursive(rocksDBTestDir);

	state IKeyValueStore* kvStore = new RocksDBKeyValueStore(rocksDBTestDir, deterministicRandom()->randomUniqueID());
	wait(kvStore->init());

	kvStore->set({ LiteralStringRef("foo"), LiteralStringRef("bar") });
	wait(kvStore->commit(false));

	Optional<Value> val = wait(kvStore->readValue(LiteralStringRef("foo")));
	ASSERT(Optional<Value>(LiteralStringRef("bar")) == val);

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
	kvStore->close();
	wait(closed);

	platform::eraseDirectoryRecursive(rocksDBTestDir);
	return Void();
}

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL
