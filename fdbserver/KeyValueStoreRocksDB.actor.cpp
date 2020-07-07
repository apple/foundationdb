#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include <string>
#include <unordered_map>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/trace_reader_writer.h>
#include "flow/flow.h"
#include "flow/IThreadPool.h"
#include "flow/Platform.h"

#include "absl/container/btree_map.h"

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

std::string_view toStringView(rocksdb::Slice slice) {
  return std::string_view(slice.data(), slice.size());
}

std::string_view toStringView(StringRef s) {
  return std::string_view(reinterpret_cast<const char*>(s.begin()), s.size());
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
		absl::btree_map<std::string, std::string> data;
    std::unique_ptr<rocksdb::Iterator> cursor = nullptr;

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
			auto opt = getOptions(a.path);
			/*auto* env = rocksdb::Env::Default();
			rocksdb::EnvOptions env_options(opt);
			    std::unique_ptr<rocksdb::TraceWriter> trace_writer;
			auto status =
			    rocksdb::NewFileTraceWriter(env, env_options, "/data/foundationdb/rocksdb_trace", &trace_writer);
			if (!status.ok()) {
			    TraceEvent(SevError, "RocksDBError")
			        .detail("Error", status.ToString())
			        .detail("Method", "NewFileTraceWriter");
			    a.done.sendError(statusToError(status));
			    return;
		}*/

			std::vector<rocksdb::ColumnFamilyDescriptor> defaultCF = { rocksdb::ColumnFamilyDescriptor{
				"default", getCFOptions() } };
			std::vector<rocksdb::ColumnFamilyHandle*> handle;
			auto status = rocksdb::DB::Open(opt, a.path, defaultCF, &handle, &db);

			if (!status.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", status.ToString()).detail("Method", "Open");
				a.done.sendError(statusToError(status));
			} else {
				/*rocksdb::TraceOptions trace_opt;
				status = db->StartTrace(trace_opt, std::move(trace_writer));
				if (!status.ok()) {
				    TraceEvent(SevError, "RocksDBError")
				        .detail("Error", status.ToString())
				        .detail("Method", "StartTrace");
				    a.done.sendError(statusToError(status));
				    return;
		  }*/

				a.done.send(Void());
			}
		}

		struct CommitAction : TypedAction<Writer, CommitAction> {
			std::unique_ptr<rocksdb::WriteBatch> batchToCommit;
			absl::btree_map<std::string, std::string> updates;
			ThreadReturnPromise<Void> done;
			double getTimeEstimate() override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(CommitAction& a) {
      data.merge(a.updates);
      for (auto& update : a.updates) {
        data[update.first] = std::move(update.second);
      }
			rocksdb::WriteOptions options;
			options.sync = true;
			// std::cout << "Begin commit.\n";
			auto s = db->Write(options, a.batchToCommit.get());
			if (!s.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "Commit");
				a.done.sendError(statusToError(s));
			} else {
				threadSleep(1);
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

		rocksdb::ReadOptions readOptions;

		struct ReadValueAction : TypedAction<Writer, ReadValueAction> {
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

		struct ReadValuePrefixAction : TypedAction<Writer, ReadValuePrefixAction> {
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

    static void compare(KeyValueRef ref, const std::pair<const std::string, std::string>& map) {
      if (map.first != ref.key) {
        std::cout << "Found key mismatch. Read: " << ref.key.printable()
                  << " Should be: " << StringRef(map.first).printable() << "\n";
      }
      if (map.second != ref.value) {
        std::cout << "Found value mismatch. Key: " << ref.key.printable() << " Read: " << ref.value.printable()
                  << " Should be: " << StringRef(map.second).printable() << "\n";
      }
    }

		struct ReadRangeAction : TypedAction<Writer, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			KeyRange keys;
			int rowLimit, byteLimit;
			ThreadReturnPromise<Standalone<RangeResultRef>> result;
			ReadRangeAction(KeyRange keys, int rowLimit, int byteLimit) : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit) {}
			virtual double getTimeEstimate() { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
		};
		void action(ReadRangeAction& a) {
			// std::cout << "Starting begin: " << a.keys.begin.printable() << "\n";
			rocksdb::ReadOptions options;
			// options.snapshot = db->GetSnapshot();
      if (cursor == nullptr) {
        cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
      } else {
        cursor->Refresh();
      }
			Standalone<RangeResultRef> result;
			int accumulatedBytes = 0;
			if (a.rowLimit >= 0) {
				cursor->Seek(toSlice(a.keys.begin));
        auto it = data.lower_bound(toStringView(a.keys.begin));
				while (cursor->Valid() && toStringRef(cursor->key()) < a.keys.end && result.size() < a.rowLimit &&
				       accumulatedBytes < a.byteLimit) {
					KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
          compare(kv, *it);
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back_deep(result.arena(), kv);
					cursor->Next();
          ++it;
				}
			} else {
				cursor->SeekForPrev(toSlice(a.keys.end));
				if (cursor->Valid() && toStringRef(cursor->key()) == a.keys.end) {
					cursor->Prev();
				}
        auto it = data.lower_bound(toStringView(a.keys.end));
        --it;
				while (cursor->Valid() && toStringRef(cursor->key()) >= a.keys.begin && result.size() < -a.rowLimit &&
				       accumulatedBytes < a.byteLimit) {
					KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
          compare(kv, *it);
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back_deep(result.arena(), kv);
					cursor->Prev();
          --it;
				}
			}
			auto s = cursor->status();
			if (!s.ok()) {
				TraceEvent(SevError, "RocksDBError").detail("Error", s.ToString()).detail("Method", "ReadRange");
			}

			for (const auto& kv : result) {
				auto it = data.find(toStringView(kv.key));
				if (it == data.end()) {
					std::cout << "Missing key: " << toStringView(kv.key) << "\n";
				} else {
				}
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
	//	Reference<IThreadPool> readThreads;
	unsigned nReaders = 16;
	Promise<Void> errorPromise;
	Promise<Void> closePromise;
	std::unique_ptr<rocksdb::WriteBatch> writeBatch;
	absl::btree_map<std::string, std::string> updates;

	explicit RocksDBKeyValueStore(const std::string& path, UID id) : path(path), id(id) {
		writeThread = createGenericThreadPool();
		// readThreads = createGenericThreadPool();
		writeThread->addThread(new Writer(db, id));
		/*for (unsigned i = 0; i < nReaders; ++i) {
		    readThreads->addThread(new Reader(db, sqlLite));
	  }*/
	}

	Future<Void> getError() override { return errorPromise.getFuture(); }

	ACTOR static void doClose(RocksDBKeyValueStore* self, bool deleteOnClose) {
		// self->db->EndTrace();
		// wait(self->readThreads->stop());
		auto a = new Writer::CloseAction(self->path, deleteOnClose);
		auto f = a->done.getFuture();
		self->writeThread->post(a);
		wait(f);
		wait(self->writeThread->stop());
		if (self->closePromise.canBeSet()) self->closePromise.send(Void());
		if (self->errorPromise.canBeSet()) self->errorPromise.send(Never());
		delete self;
	}

	ACTOR static Future<Void> join(Future<Void> a, Future<Void> b) {
		wait(success(a) && success(b));
		return Void();
	}

	ACTOR static Future<Optional<Value>> compare_read(KeyRef key, Future<Optional<Value>> r,
	                                                  Future<Optional<Value>> s) {
		state Optional<Value> sValue = wait(s);
		Optional<Value> rValue = wait(r);
		compare(key, sValue, rValue);
		return sValue;
	}

	ACTOR static Future<Standalone<RangeResultRef>> compare_range(KeyRangeRef key, int maxLength,
	                                                              Future<Standalone<RangeResultRef>> r,
	                                                              Future<Standalone<RangeResultRef>> s) {
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

	static bool compare(KeyRef key, const Optional<Value>& r, const Optional<Value>& s) {
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

	static bool compare(KeyRef key, const KeyValueRef& r, const KeyValueRef& s) {
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

	Future<Void> onClosed() override { return closePromise.getFuture(); }

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
		std::unique_ptr<Writer::OpenAction> a(new Writer::OpenAction());
		a->path = path;
		auto res = a->done.getFuture();
		writeThread->post(a.release());
		return res;
	}

	void set(KeyValueRef kv, const Arena* a) override {
		if (writeBatch == nullptr) {
			writeBatch.reset(new rocksdb::WriteBatch());
		}
		updates[kv.key.toString()] = kv.value.toString();
		// std::cout << "set Key: " << kv.key.printable() << " Value: " << kv.value.printable() << "\n";
		writeBatch->Put(toSlice(kv.key), toSlice(kv.value));
	}

	void clear(KeyRangeRef keyRange, const Arena* a) override {
		if (writeBatch == nullptr) {
			writeBatch.reset(new rocksdb::WriteBatch());
		}

		writeBatch->DeleteRange(toSlice(keyRange.begin), toSlice(keyRange.end));
	}

	Future<Void> commit(bool s) override {
		// If there is nothing to write, don't write.
		if (writeBatch == nullptr) {
			return { Void() };
		}
		/*for (const auto& key : keys) {
		    if (key.rfind("mako", 0) == 0) {
		        // std::cout << "About to commit: " << key << "\n";
		    }
	  }*/
		auto a = new Writer::CommitAction();
		a->batchToCommit = std::move(writeBatch);
		a->updates = std::move(updates);
		auto res = a->done.getFuture();
		writeThread->post(a);
		return res;
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID) override {
		auto a = new Writer::ReadValueAction(key, debugID);
		auto res = a->result.getFuture();
		writeThread->post(a);
		return res;
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<UID> debugID) override {
		auto a = new Writer::ReadValuePrefixAction(key, maxLength, debugID);
		auto res = a->result.getFuture();
		writeThread->post(a);
		return res;
	}

	Future<Standalone<RangeResultRef>> readRange(KeyRangeRef keys, int rowLimit, int byteLimit) override {
		auto a = new Writer::ReadRangeAction(keys, rowLimit, byteLimit);
		auto res = a->result.getFuture();
		writeThread->post(a);
		return res;
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
	return new RocksDBKeyValueStore(path, logID);
#else
	TraceEvent(SevError, "RocksDBEngineInitFailure").detail("Reason", "Built without RocksDB");
	ASSERT(false);
	return nullptr;
#endif // SSD_ROCKSDB_EXPERIMENTAL
}
