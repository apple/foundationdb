#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include <rocksdb/env.h>
#include <rocksdb/db.h>
#include "flow/flow.h"
#include "fdbrpc/AsyncFileCached.actor.h"
#include "fdbserver/CoroFlow.h"

#endif // SSD_ROCKSDB_EXPERIMENTAL

#include "fdbserver/IKeyValueStore.h"
#include "flow/actorcompiler.h" // has to be last include

#ifdef SSD_ROCKSDB_EXPERIMENTAL

namespace {

class FlowLogger : public rocksdb::Logger, public FastAllocated<FlowLogger> {
	UID id;
	std::string loggerName;
	size_t logSize = 0;
public:
	explicit FlowLogger(UID id, const std::string& loggerName, const rocksdb::InfoLogLevel log_level = rocksdb::InfoLogLevel::INFO_LEVEL)
		: rocksdb::Logger(log_level)
		, id(id)
		, loggerName(loggerName) {}

	rocksdb::Status Close() override { return rocksdb::Status::OK(); }

	void Logv(const char* fmtString, va_list ap) override {
		Logv(rocksdb::InfoLogLevel::INFO_LEVEL, fmtString, ap);
	}

	void Logv(const rocksdb::InfoLogLevel log_level, const char* fmtString, va_list ap) override {
		Severity sev;
		switch (log_level) {
			case rocksdb::InfoLogLevel::DEBUG_LEVEL:
				sev = SevDebug;
				break;
			case rocksdb::InfoLogLevel::INFO_LEVEL:
			case rocksdb::InfoLogLevel::HEADER_LEVEL:
			case rocksdb::InfoLogLevel::NUM_INFO_LOG_LEVELS:
				sev = SevInfo;
				break;
			case rocksdb::InfoLogLevel::WARN_LEVEL:
				sev = SevWarn;
				break;
			case rocksdb::InfoLogLevel::ERROR_LEVEL:
				sev = SevWarnAlways;
				break;
			case rocksdb::InfoLogLevel::FATAL_LEVEL:
				sev = SevError;
				break;
		}
		std::string outStr;
		auto sz = vsformat(outStr, fmtString, ap);
		if (sz < 0) {
			TraceEvent(SevError, "RocksDBLogFormatError", id)
				.detail("Logger", loggerName)
				.detail("FormatString", fmtString);
			return;
		}
		logSize += sz;
		TraceEvent(sev, "RocksDBLogMessage", id)
			.detail("Msg", outStr);
	}

	size_t GetLogFileSize() const override {
		return logSize;
	}
};

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

rocksdb::Options getOptions(const std::string& path) {
	rocksdb::Options options;
	bool exists = directoryExists(path);
	options.create_if_missing = !exists;
	return options;
}

rocksdb::ColumnFamilyOptions getCFOptions() {
	return {};
}

struct RocksDBKeyValueStore : IKeyValueStore {
	using DB = rocksdb::DB*;
	using CF = rocksdb::ColumnFamilyHandle*;

	struct Writer : IThreadPoolReceiver {
		DB& db;
		UID id;
		IKeyValueStore* sqlLite;

		explicit Writer(DB& db, UID id, IKeyValueStore* sqlLite) : db(db), id(id), sqlLite(sqlLite) {}

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
			std::unique_ptr<rocksdb::WriteBatch> batchToCommit;
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
			double getTimeEstimate() override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(CloseAction& a) {
			auto s = db->Close();
			if (!s.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "Close");
			}
			a.done.send(Void());
		}
	};

	struct Reader : IThreadPoolReceiver {
		DB& db;
		rocksdb::ReadOptions readOptions;
		std::unique_ptr<rocksdb::Iterator> cursor = nullptr;
		IKeyValueStore* sqlLite;

		explicit Reader(DB& db, IKeyValueStore* sqlLite) : db(db), sqlLite(sqlLite) {
			readOptions.total_order_seek = true;
		}

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
			auto cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(readOptions));
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
				cursor->Seek(toSlice(a.keys.end));
				if (!cursor->Valid()) {
					cursor->SeekToLast();
				} else {
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
			result.more = (result.size() == a.rowLimit);
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
	unsigned nReaders = 2;
	Promise<Void> errorPromise;
	Promise<Void> closePromise;
	std::unique_ptr<rocksdb::WriteBatch> writeBatch;
	IKeyValueStore* sqlLite;

	explicit RocksDBKeyValueStore(const std::string& path, UID id, IKeyValueStore* sqlLite)
	  : path(path), id(id), sqlLite(sqlLite) {
		writeThread = createGenericThreadPool();
		readThreads = createGenericThreadPool();
		writeThread->addThread(new Writer(db, id, sqlLite));
		for (unsigned i = 0; i < nReaders; ++i) {
			readThreads->addThread(new Reader(db, sqlLite));
		}
	}

	Future<Void> getError() override { return join(errorPromise.getFuture(), sqlLite->getError()); }

	ACTOR static void doClose(RocksDBKeyValueStore* self, bool deleteOnClose) {
		wait(self->readThreads->stop());
		auto a = new Writer::CloseAction{};
		auto f = a->done.getFuture();
		self->writeThread->post(a);
		wait(f);
		wait(self->writeThread->stop());
		// TODO: delete data on close
		if (self->closePromise.canBeSet()) self->closePromise.send(Void());
		if (self->errorPromise.canBeSet()) self->errorPromise.send(Never());
		if (deleteOnClose) {
			std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
				"default", getCFOptions() } };
			rocksdb::DestroyDB(self->path, getOptions(self->path), defaultCF);
		}
		delete self;
	}

	ACTOR static Future<Void> join(Future<Void> a, Future<Void> b) {
		wait(success(a) && success(b));
		return Void();
	}

	ACTOR static Future<Optional<Value>> compare_read(KeyRef key, Future<Optional<Value>> s,
	                                                  Future<Optional<Value>> r) {
		state Optional<Value> sValue = wait(s);
		Optional<Value> rValue = wait(r);
		compare(key, sValue, rValue);
		return sValue;
	}

	ACTOR static Future<Standalone<RangeResultRef>> compare_range(KeyRangeRef key, int maxLength,
	                                                              Future<Standalone<RangeResultRef>> s,
	                                                              Future<Standalone<RangeResultRef>> r) {
		state Standalone<RangeResultRef> sRange = wait(s);
		Standalone<RangeResultRef> rRange = wait(r);
		if (sRange.size() != rRange.size()) {
			std::cout << "Begin: " << key.begin.printable() << " End: " << key.end.printable() << " Len: " << maxLength
			          << "\n";
			std::cout << "Wrong number of elements. Rocks: " << rRange.size() << " SQLite: " << sRange.size() << "\n";
		} else {
			bool different = false;
			for (int ii; ii < sRange.size(); ++ii) {
				if (compare(key.begin, sRange[ii], rRange[ii])) {
					different = true;
					std::cout << "Diference in " << ii << "\n";
				}
			}
			if (different) {
				std::cout << "Begin: " << key.begin.printable() << " End: " << key.end.printable()
				          << " Len: " << maxLength << "\n";
			}
		}
		return sRange;
	}

	static bool compare(KeyRef key, const Optional<Value>& s, const Optional<Value>& r) {
		if (s == r) {
			return false;
		}
		std::cout << "Mismatch: " << key.printable() << "\n";
		if (s.present() && !r.present()) {
			std::cout << "RocksDB missing: " << s.get().printable() << "\n";
		} else if (!s.present() && r.present()) {
			std::cout << "RocksDB added: " << r.get().printable() << "\n";
		} else if (s.get() != r.get()) {
			std::cout << "RocksDB: " << r.get().printable() << " SQL: " << s.get().printable() << "\n";
		}
		return true;
	}

	static bool compare(KeyRef key, const KeyValueRef& s, const KeyValueRef& r) {
		if (s == r) {
			return false;
		}
		if (s.key != r.key) {
			std::cout << "Key mismatch. Rocks: " << r.key.printable() << " SQLite: " << s.key.printable() << "\n";
		}
		if (s.value != r.value) {
			if (s.key == r.key) {
				std::cout << "Key: " << s.key.printable() << "\n";
			}
			std::cout << "Value mismatch. Rocks: " << r.value.printable() << " SQLite: " << s.value.printable() << "\n";
		}
		return true;
	}

	Future<Void> onClosed() override { return join(closePromise.getFuture(), sqlLite->onClosed()); }

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
		return join(res, sqlLite->init());
	}

	void set(KeyValueRef kv, const Arena* a) override {
		sqlLite->set(kv, a);
		if (writeBatch == nullptr) {
			writeBatch.reset(new rocksdb::WriteBatch());
		}
		writeBatch->Put(toSlice(kv.key), toSlice(kv.value));
	}

	void clear(KeyRangeRef keyRange, const Arena* a) override {
		sqlLite->clear(keyRange, a);
		if (writeBatch == nullptr) {
			writeBatch.reset(new rocksdb::WriteBatch());
		}

		writeBatch->DeleteRange(toSlice(keyRange.begin), toSlice(keyRange.end));
	}

	Future<Void> commit(bool s) override {
		// If there is nothing to write, don't write.
		if (writeBatch == nullptr) {
			return sqlLite->commit(s);
		}
		auto a = new Writer::CommitAction();
		a->batchToCommit = std::move(writeBatch);
		auto res = a->done.getFuture();
		writeThread->post(a);
		return join(res, sqlLite->commit(s));
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID) override {
		auto a = new Reader::ReadValueAction(key, debugID);
		auto res = a->result.getFuture();
		readThreads->post(a);
		return compare_read(key, res, sqlLite->readValue(key, debugID));
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<UID> debugID) override {
		auto a = new Reader::ReadValuePrefixAction(key, maxLength, debugID);
		auto res = a->result.getFuture();
		readThreads->post(a);
		return res;
	}

	Future<Standalone<RangeResultRef>> readRange(KeyRangeRef keys, int rowLimit, int byteLimit) override {
		auto a = new Reader::ReadRangeAction(keys, rowLimit, byteLimit);
		auto res = a->result.getFuture();
		readThreads->post(a);
		return compare_range(keys, rowLimit, res, sqlLite->readRange(keys, rowLimit, byteLimit));
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
