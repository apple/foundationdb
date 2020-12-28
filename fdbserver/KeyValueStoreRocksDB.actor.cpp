#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/table_properties_collectors.h>
#include "flow/flow.h"
#include "flow/IThreadPool.h"

#endif // SSD_ROCKSDB_EXPERIMENTAL

#include "fdbserver/IKeyValueStore.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // has to be last include

#ifdef SSD_ROCKSDB_EXPERIMENTAL

namespace {

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
	return options;
}

rocksdb::Options getOptions() {
	rocksdb::Options options({}, getCFOptions());
	options.avoid_unnecessary_blocking_io = true;
	options.create_if_missing = true;
	if (SERVER_KNOBS->ROCKSDB_BACKGROUND_PARALLELISM > 0) {
		options.IncreaseParallelism(SERVER_KNOBS->ROCKSDB_BACKGROUND_PARALLELISM);
	}
	return options;
}

struct RocksDBKeyValueStore : IKeyValueStore {
	using CF = rocksdb::ColumnFamilyHandle*;

	rocksdb::ReadOptions readOptions;

	template<typename T>
	void post(Reference<IThreadPool> const & threadPool, T & future) {
		if (!g_network->isSimulated()) {
			threadPool->post(future);
		} else {
			action(*future);
			delete future;
		}
	}

	template<typename T>
	struct StoreReceiver : IThreadPoolReceiver {
		RocksDBKeyValueStore * const store;

		explicit StoreReceiver(RocksDBKeyValueStore * store) : store(store) {}

		void init() override {
			if (store->proc != nullptr) {
				ISimulator::currentProcess = store->proc;
			}
		}

		template<typename U>
		std::enable_if_t<std::is_same_v<T, typename U::Receiver>, void> action(U &a){ store->action(a); }
	};

	struct Writer : StoreReceiver<Writer> {
		explicit Writer(RocksDBKeyValueStore * store) : StoreReceiver<Writer>(store) { }
	};
	struct Reader : StoreReceiver<Reader> {
		explicit Reader(RocksDBKeyValueStore * store) : StoreReceiver<Reader>(store) { }
	};

	Error statusToError(const rocksdb::Status& s) {
		if (s == rocksdb::Status::IOError()) {
			return io_error();
		} else {
			return unknown_error();
		}
	}

	struct OpenAction : TypedAction<Writer, OpenAction> {
		std::string path;
		ThreadReturnPromise<Void> done;

		double getTimeEstimate() {
			return SERVER_KNOBS->COMMIT_TIME_ESTIMATE;
		}
	};

	void action(OpenAction& a) {
		std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
			"default", getCFOptions() } };
		std::vector<rocksdb::ColumnFamilyHandle*> handle;
		rocksdb::DB * newDB = nullptr;
		auto status = rocksdb::DB::Open(getOptions(), a.path, defaultCF, &handle, &newDB);
		db.reset(newDB);
		if (!status.ok()) {
			handle.clear();
			TraceEvent(SevWarn, "RocksDBError").detail("Warn", status.ToString()).detail("Method", "Open (will retry)");
			// TODO: Back the database up instead of just nuking it!
			rocksdb::DestroyDB(a.path, getOptions(), defaultCF);
			status = rocksdb::DB::Open(getOptions(), a.path, defaultCF, &handle, &newDB);
			db.reset(newDB);
		}
		if (!status.ok()) {
			TraceEvent(SevError, "RocksDBError").detail("Error", status.ToString()).detail("Method", "Open");
			a.done.sendError(statusToError(status));
		} else {
			a.done.send(Void());
		}
	}

  struct DeleteVisitor : public rocksdb::WriteBatch::Handler {
    VectorRef<KeyRangeRef>& deletes;
    Arena& arena;

    DeleteVisitor(VectorRef<KeyRangeRef>& deletes, Arena& arena) : deletes(deletes), arena(arena) {}

    rocksdb::Status DeleteRangeCF(uint32_t /*column_family_id*/, const rocksdb::Slice& begin,
			                              const rocksdb::Slice& end) override {
      KeyRangeRef kr(toStringRef(begin), toStringRef(end));
      deletes.push_back_deep(arena, kr);
      return rocksdb::Status::OK();
    }
  };

	struct CommitAction : TypedAction<Writer, CommitAction> {
		std::unique_ptr<rocksdb::WriteBatch> batchToCommit;
		ThreadReturnPromise<Void> done;
		double getTimeEstimate() override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
	};
	void action(CommitAction& a) {
		Standalone<VectorRef<KeyRangeRef>> deletes;
 		DeleteVisitor dv(deletes, deletes.arena());
 		ASSERT(a.batchToCommit->Iterate(&dv).ok());
 		// If there are any range deletes, we should have added them to be deleted.
 		ASSERT(!deletes.empty() || !a.batchToCommit->HasDeleteRange());
		rocksdb::WriteOptions options;
		options.sync = true;
		auto s = db->Write(options, a.batchToCommit.get());
		if (!s.ok()) {
			TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "Commit");
			a.done.sendError(statusToError(s));
		} else {
			a.done.send(Void());
			for (const auto& keyRange : deletes) {
				auto begin = toSlice(keyRange.begin);
				auto end = toSlice(keyRange.end);
				ASSERT(db->SuggestCompactRange(db->DefaultColumnFamily(), &begin, &end).ok());
			}
		}
	}

	struct CloseAction : TypedAction<Writer, CloseAction> {
		ThreadReturnPromise<Void> done;
		std::string path;
		bool deleteOnClose;
		CloseAction(std::string path, bool deleteOnClose) : path(path), deleteOnClose(deleteOnClose) {}
		double getTimeEstimate() override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
	};
	void action(CloseAction& a) {
		if (db) {
			auto s = db->Close();
			if (!s.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "Close");
			}
			db = nullptr;
			if (a.deleteOnClose) {
				std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
					"default", getCFOptions() } };
				rocksdb::DestroyDB(a.path, getOptions(), defaultCF);
			}
		}
		if (proc) {
			proc->machine->nonFlowStores.erase(a.path);
			proc = nullptr;
		}
		a.done.send(Void());
	}

	struct ReadValueAction : TypedAction<Reader, ReadValueAction> {
		Key key;
		Optional<UID> debugID;
		ThreadReturnPromise<Optional<Value>> result;
		ReadValueAction(KeyRef key, Optional<UID> debugID)
			: key(key), debugID(debugID)
		{}
		double getTimeEstimate() override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
	};
	void action(ReadValueAction& a) {
		Optional<TraceBatch> traceBatch;
		if (a.debugID.present()) {
			traceBatch = { TraceBatch{} };
			traceBatch.get().addEvent("GetValueDebug", a.debugID.get().first(), "Reader.Before");
		}
		rocksdb::PinnableSlice value;
		auto s = db->Get(readOptions, db->DefaultColumnFamily(), toSlice(a.key), &value);
		if (a.debugID.present()) {
			traceBatch.get().addEvent("GetValueDebug", a.debugID.get().first(), "Reader.After");
			traceBatch.get().dump();
		}
		if (s.ok()) {
			a.result.send(Value(toStringRef(value)));
		} else {
			if (!s.IsNotFound()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "ReadValue");
			}
			a.result.send(Optional<Value>());
		}
	}

	struct ReadValuePrefixAction : TypedAction<Reader, ReadValuePrefixAction> {
		Key key;
		int maxLength;
		Optional<UID> debugID;
		ThreadReturnPromise<Optional<Value>> result;
		ReadValuePrefixAction(Key key, int maxLength, Optional<UID> debugID)
		  : key(key), maxLength(maxLength), debugID(debugID){};
		virtual double getTimeEstimate() { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
	};
	void action(ReadValuePrefixAction& a) {
		rocksdb::PinnableSlice value;
		Optional<TraceBatch> traceBatch;
		if (a.debugID.present()) {
			traceBatch = { TraceBatch{} };
			traceBatch.get().addEvent("GetValuePrefixDebug", a.debugID.get().first(),
			                          "Reader.Before");
		}
		auto s = db->Get({}, db->DefaultColumnFamily(), toSlice(a.key), &value);
		if (a.debugID.present()) {
			traceBatch.get().addEvent("GetValuePrefixDebug", a.debugID.get().first(),
			                          "Reader.After");
			traceBatch.get().dump();
		}
		if (s.ok()) {
			a.result.send(Value(StringRef(reinterpret_cast<const uint8_t*>(value.data()),
			                              std::min(value.size(), size_t(a.maxLength)))));
		} else {
			if (!s.IsNotFound()) {
				TraceEvent(SevError, "RocksDBError")
				    .detail("Error", s.ToString())
				    .detail("Method", "ReadValuePrefix")
				    .detail("Key", a.key);
			}
			a.result.send(Optional<Value>());
		}
	}

	struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
		KeyRange keys;
		int rowLimit, byteLimit;
		ThreadReturnPromise<Standalone<RangeResultRef>> result;
		ReadRangeAction(KeyRange keys, int rowLimit, int byteLimit)
		  : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit) {}
		virtual double getTimeEstimate() { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
	};
	void action(ReadRangeAction& a) {
		Standalone<RangeResultRef> result;
		if (a.rowLimit == 0 || a.byteLimit == 0) {
			a.result.send(result);
		}
		int accumulatedBytes = 0;
		ASSERT(a.byteLimit > 0);
		if (a.rowLimit == 0) {
			a.result.send(result);
			return;
		}
		rocksdb::Status s;
		if (a.rowLimit > 0) {
			rocksdb::ReadOptions options;
			auto endSlice = toSlice(a.keys.end);
			options.iterate_upper_bound = &endSlice;
			auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
			cursor->Seek(toSlice(a.keys.begin));
			while (cursor->Valid() && toStringRef(cursor->key()) < a.keys.end) {
				KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
				accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
				result.push_back_deep(result.arena(), kv);
				// Calling `cursor->Next()` is potentially expensive, so short-circut here just in case.
				if (result.size() >= a.rowLimit || accumulatedBytes >= a.byteLimit) {
					break;
				}
				cursor->Next();
			}
			s = cursor->status();
		} else {
			rocksdb::ReadOptions options;
			auto beginSlice = toSlice(a.keys.begin);
			options.iterate_lower_bound = &beginSlice;
			auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
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
				cursor->Prev();
			}
			s = cursor->status();
		}
		if (!(s.ok() || s.IsNotFound())) {
			TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "ReadRange");
		}
		result.more =
		    (result.size() == a.rowLimit) || (result.size() == -a.rowLimit) || (accumulatedBytes >= a.byteLimit);
		if (result.more) {
			result.readThrough = result[result.size() - 1].key;
		}
		a.result.send(result);
	}

	std::unique_ptr<rocksdb::DB> db;
	ISimulator::ProcessInfo * proc = nullptr;

	std::string path;
	UID id;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> readThreads;

	Promise<Void> errorPromise;
	Promise<Void> closePromise;
	std::unique_ptr<rocksdb::WriteBatch> writeBatch;

	explicit RocksDBKeyValueStore(const std::string& path, UID id)
		: path(path)
		, id(id)
	{
		int nReaders = SERVER_KNOBS->ROCKSDB_READ_PARALLELISM;
		writeThread = createGenericThreadPool();
		readThreads = createGenericThreadPool();
		if (g_network->isSimulated()) {
			proc = g_simulator.getCurrentProcess();
			nReaders = 0;
			proc->machine->nonFlowStores[path] = (IKeyValueStore*)this;
		} else {
			writeThread->addThread(new Writer(this));
		}
		for (unsigned i = 0; i < nReaders; ++i) {
			readThreads->addThread(new Reader(this));
		}
	}

	Future<Void> getError() override {
		return errorPromise.getFuture();
	}

	ACTOR static void doClose(RocksDBKeyValueStore* self, bool deleteOnClose) {
		wait(self->readThreads->stop());
		auto a = new CloseAction(self->path, deleteOnClose);
		auto f = a->done.getFuture();
		self->post(self->writeThread, a);
		wait(f);
		wait(self->writeThread->stop());
		if (self->closePromise.canBeSet()) self->closePromise.send(Void());
		if (self->errorPromise.canBeSet()) self->errorPromise.send(Never());
		delete self;
	}

	void preSimulatedCrashHookForNonFlowStorageEngines() override {
		close();
	}


	Future<Void> onClosed() override {
		return closePromise.getFuture();
	}

	void dispose() override {
		doClose(this, true);
	}

	void close() override {
		doClose(this, false);
	}

	KeyValueStoreType getType() override {
		return KeyValueStoreType(KeyValueStoreType::SSD_ROCKSDB_V1);
	}

	Future<Void> init() override {
		auto * a = new OpenAction();
		a->path = path;
		auto res = a->done.getFuture();
		post(writeThread, a);
		return res;
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
		auto a = new CommitAction();
		a->batchToCommit = std::move(writeBatch);
		auto res = a->done.getFuture();
		post(writeThread, a);
		return res;
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID) override {
		auto a = new ReadValueAction(key, debugID);
		auto res = a->result.getFuture();
		post(readThreads, a);
		return res;
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<UID> debugID) override {
		auto a = new ReadValuePrefixAction(key, maxLength, debugID);
		auto res = a->result.getFuture();
		post(readThreads, a);
		return res;
	}

	Future<Standalone<RangeResultRef>> readRange(KeyRangeRef keys, int rowLimit, int byteLimit) override {
		auto a = new ReadRangeAction(keys, rowLimit, byteLimit);
		auto res = a->result.getFuture();
		post(readThreads, a);
		return res;
	}

	StorageBytes getStorageBytes() override {
		uint64_t live = 0;
		ASSERT(db->GetIntProperty(rocksdb::DB::Properties::kEstimateLiveDataSize, &live));

		int64_t free;
		int64_t total;
		g_network->getDiskBytes(path, free, total);

		return StorageBytes(free, total, live, free);
	}
};

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL

IKeyValueStore* keyValueStoreRocksDB(std::string const& path, UID logID, KeyValueStoreType storeType, bool checkChecksums, bool checkIntegrity) {
#ifdef SSD_ROCKSDB_EXPERIMENTAL
	return new RocksDBKeyValueStore(path, logID);
#else
	TraceEvent(SevError, "RocksDBEngineInitFailure").detail("Reason", "Built without RocksDB");
	ASSERT(false);
	return nullptr;
#endif // SSD_ROCKSDB_EXPERIMENTAL
}
