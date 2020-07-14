#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include "flow/flow.h"
#include "flow/IThreadPool.h"

#endif // SSD_ROCKSDB_EXPERIMENTAL

#include "fdbserver/IKeyValueStore.h"
#include "flow/actorcompiler.h" // has to be last include

#ifdef SSD_ROCKSDB_EXPERIMENTAL

namespace {

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

rocksdb::Options getOptions(const std::string& path) {
	rocksdb::Options options;
	options.create_if_missing = true;
	return options;
}

rocksdb::ColumnFamilyOptions getCFOptions() {
	return {};
}

const StringRef SPECIAL_KEYSPACE = LiteralStringRef("\xff");

class AssertingWriteBatch : public rocksdb::WriteBatch {
public:
	rocksdb::Status Put(KeyValueRef kv) {
		ASSERT(kv.key < SPECIAL_KEYSPACE);
		return WriteBatch::Put(toSlice(kv.key), toSlice(kv.value));
	}

	rocksdb::Status DeleteRange(KeyRangeRef keyRange) {
		ASSERT(keyRange.begin < SPECIAL_KEYSPACE);
		return WriteBatch::DeleteRange(toSlice(keyRange.begin), toSlice(keyRange.end));
	}
};

struct RocksDBKeyValueStore : IKeyValueStore {
	using DB = rocksdb::DB*;
	using CF = rocksdb::ColumnFamilyHandle*;

	struct Writer : IThreadPoolReceiver {
		DB& db;
		UID id;

		explicit Writer(DB& db, UID id) : db(db), id(id) {}

		~Writer() {
			if (db) {
				delete db;
			}
		}

		void init() override {}

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
			auto status = rocksdb::DB::Open(getOptions(a.path), a.path, defaultCF, &handle, &db);
			if (!status.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", status.ToString()).detail("Method", "Open");
				a.done.sendError(statusToError(status));
			} else {
				a.done.send(Void());
			}
		}

		struct CommitAction : TypedAction<Writer, CommitAction> {
			std::unique_ptr<AssertingWriteBatch> batchToCommit;
			ThreadReturnPromise<Void> done;
			double getTimeEstimate() override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(CommitAction& a) {
			rocksdb::WriteOptions options;
			options.sync = true;
			auto s = db->Write(options, a.batchToCommit.get());
			if (!s.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "Commit");
				a.done.sendError(statusToError(s));
			} else {
				a.done.send(Void());
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
			auto s = db->Close();
			if (!s.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "Close");
			}
			if (a.deleteOnClose) {
				std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
					"default", getCFOptions() } };
				rocksdb::DestroyDB(a.path, getOptions(a.path), defaultCF);
			}
			a.done.send(Void());
		}
	};

	struct Reader : IThreadPoolReceiver {
		DB& db;
		rocksdb::ReadOptions readOptions;
		std::unique_ptr<rocksdb::Iterator> cursor = nullptr;

		explicit Reader(DB& db) : db(db) {}

		void init() override {}

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
			ASSERT(a.key < SPECIAL_KEYSPACE);

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
			ReadValuePrefixAction(Key key, int maxLength, Optional<UID> debugID) : key(key), maxLength(maxLength), debugID(debugID) {};
			virtual double getTimeEstimate() { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};
		void action(ReadValuePrefixAction& a) {
			ASSERT(a.key < SPECIAL_KEYSPACE);

			rocksdb::PinnableSlice value;
			Optional<TraceBatch> traceBatch;
			if (a.debugID.present()) {
				traceBatch = { TraceBatch{} };
				traceBatch.get().addEvent("GetValuePrefixDebug", a.debugID.get().first(),
				                          "Reader.Before"); //.detail("TaskID", g_network->getCurrentTask());
			}
			auto s = db->Get(readOptions, db->DefaultColumnFamily(), toSlice(a.key), &value);
			if (a.debugID.present()) {
				traceBatch.get().addEvent("GetValuePrefixDebug", a.debugID.get().first(),
				                          "Reader.After"); //.detail("TaskID", g_network->getCurrentTask());
				traceBatch.get().dump();
			}
			if (s.ok()) {
				a.result.send(Value(StringRef(reinterpret_cast<const uint8_t*>(value.data()),
											  std::min(value.size(), size_t(a.maxLength)))));
			} else {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "ReadValuePrefix");
				a.result.send(Optional<Value>());
			}
		}

		struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			KeyRange keys;
			int rowLimit, byteLimit;
			ThreadReturnPromise<Standalone<RangeResultRef>> result;
			ReadRangeAction(KeyRange keys, int rowLimit, int byteLimit) : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit) {}
			virtual double getTimeEstimate() { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
		};
		void action(ReadRangeAction& a) {
			ASSERT(a.keys.begin < SPECIAL_KEYSPACE);
			ASSERT(a.keys.end <= SPECIAL_KEYSPACE);

			if (cursor == nullptr) {
				cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(readOptions));
			} else {
				cursor->Refresh();
			}
			Standalone<RangeResultRef> result;
			int accumulatedBytes = 0;
			if (a.rowLimit >= 0) {
				cursor->Seek(toSlice(a.keys.begin));
				while (cursor->Valid() && toStringRef(cursor->key()) < a.keys.end && result.size() < a.rowLimit &&
				       accumulatedBytes < a.byteLimit) {
					KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back_deep(result.arena(), kv);
					cursor->Next();
				}
			} else {
				cursor->SeekForPrev(toSlice(a.keys.end));
				if (cursor->Valid() && toStringRef(cursor->key()) == a.keys.end) {
					cursor->Prev();
				}

				while (cursor->Valid() && toStringRef(cursor->key()) >= a.keys.begin && result.size() < -a.rowLimit &&
				       accumulatedBytes < a.byteLimit) {
					KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back_deep(result.arena(), kv);
					cursor->Prev();
				}
			}
			auto s = cursor->status();
			if (!s.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "ReadRange");
			}
			result.more =
			    (result.size() == a.rowLimit) || (result.size() == -a.rowLimit) || (accumulatedBytes >= a.byteLimit);
			if (result.more) {
			  result.readThrough = result[result.size()-1].key;
			}
			a.result.send(result);
		}
	};

	DB db = nullptr;
	std::string path;
	UID id;
	size_t diskBytesUsed = 0;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> readThreads;
	unsigned nReaders = 16;
	Promise<Void> errorPromise;
	Promise<Void> closePromise;
	std::unique_ptr<AssertingWriteBatch> writeBatch;
	IKeyValueStore* sqlLite;

	explicit RocksDBKeyValueStore(const std::string& path, UID id, IKeyValueStore* sqlLite)
	  : path(path), id(id), sqlLite(sqlLite) {
		writeThread = createGenericThreadPool();
		readThreads = createGenericThreadPool();
		writeThread->addThread(new Writer(db, id));
		for (unsigned i = 0; i < nReaders; ++i) {
			readThreads->addThread(new Reader(db));
		}
	}

	Future<Void> getError() override { return errorPromise.getFuture() && sqlLite->getError(); }

	ACTOR static void doClose(RocksDBKeyValueStore* self, bool deleteOnClose) {
		wait(self->readThreads->stop());
		auto a = new Writer::CloseAction(self->path, deleteOnClose);
		auto f = a->done.getFuture();
		self->writeThread->post(a);
		wait(f);
		wait(self->writeThread->stop());
		if (self->closePromise.canBeSet()) self->closePromise.send(Void());
		if (self->errorPromise.canBeSet()) self->errorPromise.send(Never());
		delete self;
	}

	Future<Void> onClosed() override { return closePromise.getFuture() && sqlLite->onClosed(); }

	void dispose() override {
		sqlLite->dispose();
		doClose(this, true);
	}

	void close() override {
		sqlLite->close();
		doClose(this, false);
	}

	KeyValueStoreType getType() override {
		return KeyValueStoreType(KeyValueStoreType::SSD_ROCKSDB_V1);
	}

	Future<Void> init() override {
		std::unique_ptr<Writer::OpenAction> a(new Writer::OpenAction());
		a->path = path;
		auto res = a->done.getFuture();
		writeThread->post(a.release());
		return res && sqlLite->init();
	}

	void set(KeyValueRef kv, const Arena* a) override {
		// Writes to the special keyspace only go to SQLite.
		if (kv.key >= SPECIAL_KEYSPACE) {
			sqlLite->set(kv, a);
			return;
		}
		if (writeBatch == nullptr) {
			writeBatch.reset(new AssertingWriteBatch());
		}
		writeBatch->Put(kv);
	}

	void clear(KeyRangeRef keyRange, const Arena* a) override {
		// If the clear touches the special keyspace send the clear to SQLite.
		if (keyRange.end > SPECIAL_KEYSPACE) {
			sqlLite->clear(keyRange, a);
		}
		// If the clear doesn't touch user ranges at all, don't create extra tombstones in Rocks.
		if (keyRange.begin >= SPECIAL_KEYSPACE) {
			return;
		}

		if (writeBatch == nullptr) {
			writeBatch.reset(new AssertingWriteBatch());
		}

		writeBatch->DeleteRange(keyRange);
	}

	Future<Void> commit(bool b) override {
		// TODO: We probably should check if there is anything to commit to SQLite.
		auto sf = sqlLite->commit(b);

		// If there is nothing to write, don't write.
		if (writeBatch == nullptr) {
			return sf;
		}
		auto a = new Writer::CommitAction();
		a->batchToCommit = std::move(writeBatch);
		auto res = a->done.getFuture();
		writeThread->post(a);
		return sf && res;
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID) override {
		if (key >= SPECIAL_KEYSPACE) {
			return sqlLite->readValue(key, debugID);
		}
		auto a = new Reader::ReadValueAction(key, debugID);
		auto res = a->result.getFuture();
		readThreads->post(a);

		return res;
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<UID> debugID) override {
		if (key >= SPECIAL_KEYSPACE) {
			return sqlLite->readValuePrefix(key, maxLength, debugID);
		}
		auto a = new Reader::ReadValuePrefixAction(key, maxLength, debugID);
		auto res = a->result.getFuture();
		readThreads->post(a);
		return res;
	}

	ACTOR static Future<Standalone<RangeResultRef>> stitchRangeRead(RocksDBKeyValueStore* self, KeyRangeRef keys,
	                                                                int rowLimit, int byteLimit) {
		ASSERT(keys.begin < SPECIAL_KEYSPACE);
		ASSERT(keys.end > SPECIAL_KEYSPACE);
		state Standalone<RangeResultRef> result;
		if (rowLimit >= 0) {
			// Limit the first range to end at the special keyspace to prevent infinite recursion.
			Standalone<RangeResultRef> result =
			    wait(self->readRange({ keys.begin, SPECIAL_KEYSPACE }, rowLimit, byteLimit));
			// We should probably not be duplicating this work, but these range reads are rare.
			int accumulatedBytes = 0;
			for (const auto& kv : result) {
				accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
			}
			if (accumulatedBytes < byteLimit && result.size() < rowLimit) {
				ASSERT(!result.more);
				Standalone<RangeResultRef> sResult = wait(self->sqlLite->readRange(
				    { SPECIAL_KEYSPACE, keys.end }, rowLimit - result.size(), byteLimit - accumulatedBytes));
				for (const auto& kv : sResult) {
					result.push_back_deep(result.arena(), kv);
				}
			} else {
				ASSERT(result.more);
			}
		} else {
			Standalone<RangeResultRef> result =
			    wait(self->sqlLite->readRange({ SPECIAL_KEYSPACE, keys.end }, rowLimit, byteLimit));
			int accumulatedBytes = 0;
			for (const auto& kv : result) {
				accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
			}
			if (accumulatedBytes < byteLimit && result.size() < -rowLimit) {
				ASSERT(!result.more);
				Standalone<RangeResultRef> rResult = wait(self->readRange(
				    { keys.begin, SPECIAL_KEYSPACE }, rowLimit + result.size(), byteLimit - accumulatedBytes));
				for (const auto& kv : rResult) {
					result.push_back_deep(result.arena(), kv);
				}
			} else {
				ASSERT(result.more);
			}
		}
		return result;
	}

	Future<Standalone<RangeResultRef>> readRange(KeyRangeRef keys, int rowLimit, int byteLimit) override {
		if (keys.begin >= SPECIAL_KEYSPACE) {
			return sqlLite->readRange(keys, rowLimit, byteLimit);
		}
		if (keys.end <= SPECIAL_KEYSPACE) {
			auto a = new Reader::ReadRangeAction(keys, rowLimit, byteLimit);
			auto res = a->result.getFuture();
			readThreads->post(a);
			return res;
		}
		return stitchRangeRead(this, keys, rowLimit, byteLimit);
	}

	StorageBytes getStorageBytes() override {
		int64_t free;
		int64_t total;

		g_network->getDiskBytes(path, free, total);

		return StorageBytes(free, total, diskBytesUsed, free);
	}
};

} // namespace

#endif // SSD_ROCKSDB_EXPERIMENTAL

IKeyValueStore* keyValueStoreRocksDB(std::string const& path, UID logID, KeyValueStoreType storeType, bool checkChecksums, bool checkIntegrity) {
#ifdef SSD_ROCKSDB_EXPERIMENTAL
	std::cout << "We created the Rock!\n";
	return new RocksDBKeyValueStore(path, logID,
	                                keyValueStoreSQLite(std::string(path) + "/sqlite", logID,
	                                                    KeyValueStoreType::SSD_BTREE_V2, checkChecksums,
	                                                    checkIntegrity));
#else
	TraceEvent(SevError, "RocksDBEngineInitFailure").detail("Reason", "Built without RocksDB");
	ASSERT(false);
	return nullptr;
#endif // SSD_ROCKSDB_EXPERIMENTAL
}
