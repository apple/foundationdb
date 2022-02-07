#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/types.h>
#include <rocksdb/version.h>

#include "fdbclient/SystemData.h"
#include "fdbserver/CoroFlow.h"
#include "flow/flow.h"
#include "flow/IThreadPool.h"
#include "flow/ThreadHelper.actor.h"

#include <memory>
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
using DB = rocksdb::DB*;
using CF = rocksdb::ColumnFamilyHandle*;

#define PERSIST_PREFIX "\xff\xff"
const KeyRef persistVersion = LiteralStringRef(PERSIST_PREFIX "Version");

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

rocksdb::ColumnFamilyOptions getCFOptions() {
	rocksdb::ColumnFamilyOptions options;
	return options;
}

rocksdb::Options getOptions() {
	rocksdb::Options options({}, getCFOptions());

	// options.avoid_unnecessary_blocking_io = true;
	options.create_if_missing = false;
	options.db_log_dir = SERVER_KNOBS->LOG_DIRECTORY;
	return options;
}

// Set some useful defaults desired for all reads.
rocksdb::ReadOptions getReadOptions() {
	rocksdb::ReadOptions options;
	options.background_purge_on_iterator_cleanup = true;
	return options;
}

void logRocksDBError(const rocksdb::Status& status, const std::string& method) {
	auto level = status.IsTimedOut() ? SevWarn : SevError;
	TraceEvent e(level, "RocksDBCheckpointReaderError");
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

struct RocksDBCheckpointReader : ICheckpointReader {
	struct Reader : IThreadPoolReceiver {
		explicit Reader(DB& db) : db(db), cf(nullptr) {
			if (g_network->isSimulated()) {
				// In simulation, increasing the read operation timeouts to 5 minutes, as some of the tests have
				// very high load and single read thread cannot process all the load within the timeouts.
				readRangeTimeout = 5 * 60;
			} else {
				readRangeTimeout = SERVER_KNOBS->ROCKSDB_READ_RANGE_TIMEOUT;
			}
		}

		~Reader() override {
			if (db) {
				delete db;
			}
		}

		void init() override {}

		struct OpenAction : TypedAction<Reader, OpenAction> {
			OpenAction(std::string path, KeyRange range) : path(std::move(path)), range(range) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }

			const std::string path;
			const KeyRange range;
			ThreadReturnPromise<Void> done;
		};

		void action(OpenAction& a) {
			ASSERT(cf == nullptr);

			std::cout << "Open RocksDB Checkpoint." << std::endl;
			std::vector<std::string> columnFamilies;
			rocksdb::Options options = getOptions();
			rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, a.path, &columnFamilies);
			std::cout << "Open RocksDB Found Column Families: " << describe(columnFamilies) << std::endl;
			if (std::find(columnFamilies.begin(), columnFamilies.end(), "default") == columnFamilies.end()) {
				columnFamilies.push_back("default");
			}

			rocksdb::ColumnFamilyOptions cfOptions = getCFOptions();
			std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
			for (const std::string& name : columnFamilies) {
				descriptors.push_back(rocksdb::ColumnFamilyDescriptor{ name, cfOptions });
			}

			std::vector<rocksdb::ColumnFamilyHandle*> handles;
			status = rocksdb::DB::OpenForReadOnly(options, a.path, descriptors, &handles, &db);

			std::cout << "Open RocksDB Checkpoint Status." << status.ToString() << std::endl;
			if (!status.ok()) {
				logRocksDBError(status, "OpenForReadOnly");
				a.done.sendError(statusToError(status));
				return;
			}

			for (rocksdb::ColumnFamilyHandle* handle : handles) {
				if (handle->GetName() == SERVER_KNOBS->DEFAULT_FDB_ROCKSDB_COLUMN_FAMILY) {
					cf = handle;
					break;
				}
			}

			TraceEvent(SevInfo, "RocksDBCheckpointReader")
			    .detail("Path", a.path)
			    .detail("Method", "OpenForReadOnly")
			    .detail("ColumnFamily", cf->GetName());

			ASSERT(db != nullptr && cf != nullptr);

			std::cout << "Init Iterator." << std::endl;

			begin = toSlice(a.range.begin);
			end = toSlice(a.range.end);

			rocksdb::ReadOptions readOptions = getReadOptions();
			readOptions.iterate_upper_bound = &end;
			cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(readOptions, cf));
			cursor->Seek(begin);

			a.done.send(Void());
		}

		struct CloseAction : TypedAction<Reader, CloseAction> {
			CloseAction(std::string path, bool deleteOnClose) : path(path), deleteOnClose(deleteOnClose) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }

			std::string path;
			bool deleteOnClose;
			ThreadReturnPromise<Void> done;
		};

		void action(CloseAction& a) {
			if (db == nullptr) {
				a.done.send(Void());
				return;
			}

			rocksdb::Status s = db->Close();
			if (!s.ok()) {
				logRocksDBError(s, "Close");
			}

			if (a.deleteOnClose) {
				std::set<std::string> columnFamilies{ "default" };
				columnFamilies.insert(SERVER_KNOBS->DEFAULT_FDB_ROCKSDB_COLUMN_FAMILY);
				std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
				for (const std::string& name : columnFamilies) {
					descriptors.push_back(rocksdb::ColumnFamilyDescriptor{ name, getCFOptions() });
				}
				s = rocksdb::DestroyDB(a.path, getOptions(), descriptors);
				if (!s.ok()) {
					logRocksDBError(s, "Destroy");
				} else {
					TraceEvent("RocksDBCheckpointReader").detail("Path", a.path).detail("Method", "Destroy");
				}
			}

			TraceEvent("RocksDBCheckpointReader").detail("Path", a.path).detail("Method", "Close");
			a.done.send(Void());
		}

		struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			ReadRangeAction(int rowLimit, int byteLimit)
			  : rowLimit(rowLimit), byteLimit(byteLimit), startTime(timer_monotonic()) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }

			const int rowLimit, byteLimit;
			const double startTime;
			ThreadReturnPromise<RangeResult> result;
		};

		void action(ReadRangeAction& a) {
			const double readBeginTime = timer_monotonic();

			if (readBeginTime - a.startTime > readRangeTimeout) {
				TraceEvent(SevWarn, "RocksDBCheckpointReaderError")
				    .detail("Error", "Read range request timedout")
				    .detail("Method", "ReadRangeAction")
				    .detail("Timeout value", readRangeTimeout);
				a.result.sendError(transaction_too_old());
				return;
			}

            std::cout << "Reading batch" << std::endl;

			RangeResult result;
			if (a.rowLimit == 0 || a.byteLimit == 0) {
				a.result.send(result);
                return;
			}

			ASSERT(a.rowLimit > 0);

			int accumulatedBytes = 0;
			rocksdb::Status s;
			while (cursor->Valid()) {
				KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
                std::cout << "Getting key " << cursor->key().ToString() << std::endl;
				accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
				result.push_back_deep(result.arena(), kv);
				// Calling `cursor->Next()` is potentially expensive, so short-circut here just in case.
				if (result.size() >= a.rowLimit || accumulatedBytes >= a.byteLimit) {
					break;
				}
				if (timer_monotonic() - a.startTime > readRangeTimeout) {
					TraceEvent(SevWarn, "RocksDBCheckpointReaderError")
					    .detail("Error", "Read range request timedout")
					    .detail("Method", "ReadRangeAction")
					    .detail("Timeout value", readRangeTimeout);
					a.result.sendError(transaction_too_old());
					return;
				}
				cursor->Next();
			}

			s = cursor->status();

			if (!s.ok()) {
				logRocksDBError(s, "ReadRange");
				a.result.sendError(statusToError(s));
				return;
			}

            std::cout << "Read Done." << cursor->status().ToString() << std::endl;
			// throw end_of_stream();

			a.result.send(result);
		}

		DB& db;
		CF cf;
		rocksdb::Slice begin;
		rocksdb::Slice end;
		double readRangeTimeout;
		std::unique_ptr<rocksdb::Iterator> cursor;
	};

	explicit RocksDBCheckpointReader(const std::string& path, UID id) : path(path), id(id) {
		if (g_network->isSimulated()) {
			readThreads = CoroThreadPool::createThreadPool();
		} else {
			readThreads = createGenericThreadPool();
		}
		readThreads->addThread(new Reader(db), "fdb-rocksdb-checkpoint-reader");
	}

	Future<Void> getError() const override { return errorPromise.getFuture(); }

	ACTOR static void doClose(RocksDBCheckpointReader* self, bool deleteOnClose) {
		auto a = new Reader::CloseAction(self->path, deleteOnClose);
		auto f = a->done.getFuture();
		self->readThreads->post(a);
		wait(f);

		wait(self->readThreads->stop());

		if (self->closePromise.canBeSet()) {
			self->closePromise.send(Void());
		}
		if (self->errorPromise.canBeSet()) {
			self->errorPromise.send(Never());
		}

		delete self;
	}

	Future<Void> onClosed() const override { return closePromise.getFuture(); }

	void dispose() override { doClose(this, true); }

	void close() override { doClose(this, false); }

	Future<Void> init(KeyRange range) override {
		if (openFuture.isValid()) {
			return openFuture;
		}
		auto a = std::make_unique<Reader::OpenAction>(path, range);
		openFuture = a->done.getFuture();
		readThreads->post(a.release());
		return openFuture;
	}

	Future<RangeResult> next(int rowLimit, int byteLimit) override {
		auto a = std::make_unique<Reader::ReadRangeAction>(rowLimit, byteLimit);
		auto res = a->result.getFuture();
		readThreads->post(a.release());
		return res;
	}

	DB db = nullptr;
	std::string path;
	UID id;
	Reference<IThreadPool> readThreads;
	Promise<Void> errorPromise;
	Promise<Void> closePromise;
	Future<Void> openFuture;
};

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL

ICheckpointReader* checkpointReaderRocksDB(const std::string& path, UID logID) {
#ifdef SSD_ROCKSDB_EXPERIMENTAL
	return new RocksDBCheckpointReader(path, logID);
#else
	TraceEvent(SevError, "RocksDBEngineInitFailure").detail("Reason", "Built without RocksDB");
	ASSERT(false);
	return nullptr;
#endif // SSD_ROCKSDB_EXPERIMENTAL
}

#ifdef SSD_ROCKSDB_EXPERIMENTAL
#include "flow/UnitTest.h"

namespace {} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL
