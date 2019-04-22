#include <rocksdb/env.h>
#include <rocksdb/db.h>
#include "flow/flow.h"
#include "fdbrpc/AsyncFileCached.actor.h"
#include "fdbserver/CoroFlow.h"
#include "fdbserver/IKeyValueStore.h"
#include "flow/actorcompiler.h" // has to be last include

namespace {

std::unordered_map<std::string, UID> rocksdbFileIDs;

class FlowDirectory : public rocksdb::Directory, public FastAllocated<FlowDirectory> {
	// TODO[mpilman]: this need to be implemented - probably with EIO.

	std::string name;
public:

	FlowDirectory(const std::string& name)
		: name(name) {}

	rocksdb::Status Fsync() override {
		return rocksdb::Status::OK();
	}
};

class FlowFile {
protected:
	Reference<AsyncFileCached> file;
	mutable Optional<Error> error;

	FlowFile(const std::string& name, int flags) {
		ErrorOr<Reference<IAsyncFile>> res = waitForAndGet(errorOr(IAsyncFileSystem::filesystem()->open(name, flags, 0)));
		if (res.isError()) {
			error = res.getError();
		} else {
			file = Reference<AsyncFileCached>(dynamic_cast<AsyncFileCached*>(res.get().getPtr()));
		}
	}


	Optional<Error> getError() const {
		return error;
	}
public:
	template<class E>
	rocksdb::Status status(const E& err) const {
		if (err.isError()) {
			error = err.getError();
			return status();
		}
		return rocksdb::Status::OK();
	}

	rocksdb::Status status() const {
		if (error.present()) {
			auto error = this->error.get();
			if (error.code() == error_code_file_not_found) {
				return rocksdb::Status::NotFound();
			} else if (error.code() == error_code_io_error) {
				return rocksdb::Status::IOError();
			} else {
				return rocksdb::Status::IOError();
			}
		} else {
			return rocksdb::Status{};
		}
	}
};

class FlowSequentialFile : public FlowFile, public rocksdb::SequentialFile, public FastAllocated<FlowSequentialFile> {
	size_t currentOffset = 0;

public:
	FlowSequentialFile(const std::string name)
		: FlowFile(name, IAsyncFile::OPEN_CACHED_READ_ONLY)
	{
	}

	rocksdb::Status Read(size_t n, rocksdb::Slice* result, char* scratch) override {
		ErrorOr<int> res = waitForAndGet(errorOr(file->read(scratch, n, currentOffset)));
		if (res.isError()) {
			error = res.getError();
			return status();
		}
		*result = rocksdb::Slice(scratch, res.get());
		return rocksdb::Status{};
	}

	rocksdb::Status Skip(uint64_t n) override {
		currentOffset += n;
		return rocksdb::Status{};
	}

	rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
		auto res = waitForAndGet(errorOr(file->invalidateCache(offset, length)));
		return status(res);
	}
};

class FlowRandomAccessFile : public FlowFile, public rocksdb::RandomAccessFile, public FastAllocated<FlowRandomAccessFile> {
public:
	FlowRandomAccessFile(const std::string& name)
		: FlowFile(name, IAsyncFile::OPEN_CACHED_READ_ONLY)
	{
	}

	rocksdb::Status Read(uint64_t offset, size_t n, rocksdb::Slice* result, char* scratch) const override {
		ErrorOr<int> sz = waitForAndGet(errorOr(file->read(scratch, n, offset)));
		if (sz.isError()) {
			error = sz.getError();
			return status();
		}
		*result = rocksdb::Slice(scratch, sz.get());
		return rocksdb::Status::OK();
	}

	rocksdb::Status Prefetch(uint64_t offset, size_t n) override {
		file->prefetch(offset, int(n));
		return rocksdb::Status::OK();
	}

	// Note: these IDs are only valid for the duration of the process.
	size_t GetUniqueId(char* id, size_t max_size) const override {
		auto& fileID = rocksdbFileIDs[file->getFilename()];
		if (fileID == UID()) {
			fileID = g_random->randomUniqueID();
		}
		if (max_size <= sizeof(UID)) {
			auto p = reinterpret_cast<uint64_t*>(id);
			p[0] = fileID.first();
			p[1] = fileID.second();
			return sizeof(UID);
		}
		return 0;
	};

	rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
		ErrorOr<Void> res = waitForAndGet(errorOr(file->invalidateCache(offset, length)));
		return status(res);
	}
};

class FlowWriteableFile : public FlowFile, public rocksdb::WritableFile, public FastAllocated<FlowWriteableFile> {
public:
	FlowWriteableFile(const std::string& filename, int flags)
		: FlowFile(filename, flags)
	{}

	rocksdb::Status Append(const rocksdb::Slice& data) override {
		return rocksdb::Status::NotSupported();
	}

	rocksdb::Status PositionedAppend(const rocksdb::Slice& data, uint64_t offset) override {
		ErrorOr<Void> res = waitForAndGet(errorOr(file->write(data.data(), data.size(), offset)));
		return status(res);
	}

	rocksdb::Status Truncate(uint64_t size) override {
		ErrorOr<Void> res = waitForAndGet(errorOr(file->truncate(size)));
		return status(res);
	}

	rocksdb::Status Close() override {
		file = Reference<AsyncFileCached>();
		return rocksdb::Status::OK();
	}

	rocksdb::Status Flush() override {
		ErrorOr<Void> res = waitForAndGet(errorOr(file->flush()));
		return status(res);
	}

	rocksdb::Status Sync() override {
		ErrorOr<Void> res = waitForAndGet(errorOr(file->sync()));
		return status(res);
	}
};

class FlowRandomRWFile : public FlowFile, public rocksdb::RandomRWFile, public FastAllocated<FlowRandomRWFile> {
public:
	FlowRandomRWFile(const std::string& fname, int flags)
		: FlowFile(fname, flags)
	{}

	rocksdb::Status Write(uint64_t offset, const rocksdb::Slice& data) override {
		ErrorOr<Void> res = waitForAndGet(errorOr(file->write(data.data(), data.size(), offset)));
		return status(res);
	}

	rocksdb::Status Read(uint64_t offset, size_t n, rocksdb::Slice* result,
						 char* scratch) const override {
		ErrorOr<int> res = waitForAndGet(errorOr(file->read(scratch, n, offset)));
		if (res.isError()) {
			return status(res);
		}
		*result = rocksdb::Slice(scratch, res.get());
		return rocksdb::Status::OK();
	}

	rocksdb::Status Flush() {
		auto res = waitForAndGet(errorOr(file->flush()));
		return status(res);
	}

	rocksdb::Status Sync() {
		auto res = waitForAndGet(errorOr(file->sync()));
		return status(res);
	}

	rocksdb::Status Close() {
		file = Reference<AsyncFileCached>();
		return rocksdb::Status::OK();
	}
};

struct NotifiedCounter {
	int count = 0;
	std::queue<Promise<Void>> waiting;

	Future<Void> whenDone() {
		Promise<Void> p;
		waiting.push(p);
		return p.getFuture();
	}

	NotifiedCounter& operator++() {
		++count;
		return *this;
	}

	NotifiedCounter& operator--() {
		if (--count == 0) {
			while (!waiting.empty()) {
				waiting.back().send(Void());
				waiting.pop();
			}
		}
		UNSTOPPABLE_ASSERT(count >= 0);
		return *this;
	}
};

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

class FlowEnv : public rocksdb::EnvWrapper, public FastAllocated<FlowEnv> {
	std::map<rocksdb::Env::Priority, Reference<IThreadPool>> threadPools;
	std::map<rocksdb::Env::Priority, unsigned> threadCounts;
	NotifiedCounter threadCounter;
	using CActions = std::map<void*, std::pair<unsigned, bool>>;
	CActions cancableActions;
	UID id;

	struct BGThread : IThreadPoolReceiver {
		NotifiedCounter& tCounter;
		CActions& cancableActions;

		explicit BGThread(NotifiedCounter& tCounter, CActions& cancableActions)
			: tCounter(tCounter)
			, cancableActions(cancableActions)
		{}

		void init() override {}

		template <class Iter>
		void running(Iter iter) {
			UNSTOPPABLE_ASSERT(iter != cancableActions.end());
			if (--(iter->second.first) == 0) {
				cancableActions.erase(iter);
			}
		}

		void done()  {
			--tCounter;
		}

		struct Action : TypedAction<BGThread, Action> {
			void (*function)(void*);
			void* arg;
			Action(void (*function)(void*), void* arg)
				: function(function)
				, arg(arg)
			{}
			double getTimeEstimate() { return 0.001; }
		};

		void action(Action& a) {
			auto iter = cancableActions.find(a.arg);
			UNSTOPPABLE_ASSERT(iter != cancableActions.end());
			if (iter->second.second) {
				done();
				return;
			}
			running(iter);
			a.function(a.arg);
			done();
		}
	};

public:
	explicit FlowEnv(UID id)
		: rocksdb::EnvWrapper(rocksdb::Env::Default())
		, id(id)
	{}
	rocksdb::Status NewSequentialFile(const std::string& name, std::unique_ptr<rocksdb::SequentialFile>* result,
									  const rocksdb::EnvOptions& options) override {
		std::unique_ptr<FlowSequentialFile> res(new FlowSequentialFile(name));
		auto ret = res->status();
		if (ret != rocksdb::Status::OK()) {
			result->reset();
			return ret;
		}
		result->reset(res.release());
		return ret;
	}

	rocksdb::Status NewRandomAccessFile(const std::string& fname,
										std::unique_ptr<rocksdb::RandomAccessFile>* result,
										const rocksdb::EnvOptions& options) override {
		std::unique_ptr<FlowRandomAccessFile> file(new FlowRandomAccessFile(fname));
		if (file->status() != rocksdb::Status::OK()) {
			result->reset();
			return file->status();
		}
		result->reset(file.release());
		return rocksdb::Status::OK();
	}

	rocksdb::Status OpenWritableFile(const std::string& fname,
									 std::unique_ptr<rocksdb::WritableFile>* result,
									 int flags) {
		std::unique_ptr<FlowWriteableFile> file(new FlowWriteableFile(fname, flags));
		if (file->status() != rocksdb::Status::OK()) {
			result->reset();
			return file->status();
		}
		result->reset(file.release());
		return rocksdb::Status::OK();
	}

	rocksdb::Status ReopenWritableFile(const std::string& fname,
									   std::unique_ptr<rocksdb::WritableFile>* result,
									   const rocksdb::EnvOptions& options) override {
		return OpenWritableFile(fname, result, IAsyncFile::OPEN_READWRITE);
	}

	rocksdb::Status NewWritableFile(const std::string& fname,
									std::unique_ptr<rocksdb::WritableFile>* result,
									const rocksdb::EnvOptions& options) override {
		if (fileExists(fname)) {
			ErrorOr<Void> res = waitForAndGet(errorOr(IAsyncFileSystem::filesystem()->deleteFile(fname, true)));
			if (res.isError()) {
				return rocksdb::Status::IOError();
			}
		}
		return OpenWritableFile(fname, result, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE);
	}

	rocksdb::Status ReuseWritableFile(const std::string& fname,
									   const std::string& old_fname,
									   std::unique_ptr<rocksdb::WritableFile>* result,
									   const rocksdb::EnvOptions& options) override {
		renameFile(old_fname, fname);
		return ReopenWritableFile(fname, result, options);
	}

	rocksdb::Status NewRandomRWFile(const std::string& fname,
									std::unique_ptr<rocksdb::RandomRWFile>* result,
									const rocksdb::EnvOptions& options) override {
		int flags = IAsyncFile::OPEN_READWRITE;
		if (!fileExists(fname)) {
			flags |= IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE;
		}
		std::unique_ptr<FlowRandomRWFile> file(new FlowRandomRWFile(fname, flags));
		if (file->status() != rocksdb::Status::OK()) {
			result->reset();
			return file->status();
		}
		result->reset(file.release());
		return rocksdb::Status::OK();
	}

	rocksdb::Status NewDirectory(const std::string& name,
								 std::unique_ptr<rocksdb::Directory>* result) override {
		if (!directoryExists(name)) {
			return rocksdb::Status::NotFound();
		}
		result->reset(new FlowDirectory(name));
		return rocksdb::Status::OK();
	}

	rocksdb::Status FileExists(const std::string& fname) override {
		if (fileExists(fname)) {
			return rocksdb::Status::OK();
		}
		return rocksdb::Status::NotFound();
	}

	rocksdb::Status GetChildren(const std::string& dir,
								std::vector<std::string>* result) override {
		*result = platform::listFiles(dir);
		auto dirs = platform::listDirectories(dir);
		result->reserve(result->size() + dirs.size());
		result->insert(result->end(), dirs.begin(), dirs.end());
		return rocksdb::Status::OK();
	}

	rocksdb::Status DeleteFile(const std::string& fname) override {
		auto res = waitForAndGet(errorOr(IAsyncFileSystem::filesystem()->deleteFile(fname, true)));
		if (res.isError()) {
			return rocksdb::Status::IOError(res.getError().what());
		}
		return rocksdb::Status::OK();
	}

	rocksdb::Status CreateDir(const std::string& dirname) override {
		try {
			if (platform::createDirectory(dirname)) {
				return rocksdb::Status::OK();
			}
			return rocksdb::Status::IOError();
		} catch (Error& e) {
			return rocksdb::Status::IOError(e.what());
		}
	}

	rocksdb::Status CreateDirIfMissing(const std::string& dirname) {
		try {
			platform::createDirectory(dirname);
		} catch (Error& e) {
			return rocksdb::Status::IOError(e.what());
		}
		return rocksdb::Status::OK();
	}

	void StartThread(void (*function)(void* arg), void* arg) override {
		Schedule(function, arg);
	}

	void Schedule(void (*function)(void* arg), void* arg,
				  rocksdb::Env::Priority pri = rocksdb::Env::LOW,
				  void* tag = nullptr,
				  void (*unschedFunction)(void* arg) = nullptr) override {
		++threadCounter;
		auto iter = cancableActions.find(arg);
		if (iter == cancableActions.end()) {
			cancableActions.emplace(arg, std::make_pair(1u, false));
		} else {
			++iter->second.first;
		}
		auto& pool = threadPools[pri];
		if (!pool) {
			pool = CoroThreadPool::createThreadPool();
			pool->addThread(new BGThread(threadCounter, cancableActions));
			++threadCounts[pri];
		}
		pool->post(new BGThread::Action(function, arg));
	}
	int UnSchedule(void* arg, rocksdb::Env::Priority pri) override {
		auto iter = cancableActions.find(arg);
		int res = 0;
		if (iter != cancableActions.end()) {
			iter->second.second = true;
			res = iter->second.first;
		}
		return res;
	}

	void WaitForJoin() override {
		waitForAndGet(errorOr(threadCounter.whenDone()));
	}

	rocksdb::Status GetTestDirectory(std::string* path) override {
		// TODO[mpilman] Implement
		*path = "/tmp";
		return rocksdb::Status::OK();
	}

	rocksdb::Status Newogger(const std::string& fname,
							 std::shared_ptr<rocksdb::Logger>* result) override {
		result->reset(new FlowLogger(id, fname));
		return rocksdb::Status::OK();
	}

	uint64_t NowMicros() override {
		return uint64_t(g_network->now() * 1e6);
	}

	uint64_t NowNanos() override {
		return uint64_t(g_network->now() * 1e3);
	}

	void SleepForMicroseconds(int micros) override {
		waitForAndGet(delay(double(micros) / double(1e6)));
	}

	rocksdb::Status GetHostName(char* name, uint64_t len) override {
		auto str = g_network->getLocalAddress().toString();
		auto length = std::min(size_t(len - 1), str.size());
		memcpy(name, str.data(), length);
		name[length] = '\0';
		return rocksdb::Status::OK();
	}

	rocksdb::Status GetCurrentTime(int64_t* unix_time) override {
		*unix_time = int64_t(g_network->now());
		return rocksdb::Status::OK();
	}
	rocksdb::Status GetAbsolutePath(const std::string& db_path,
									std::string* output_path) override {
		*output_path = abspath(db_path);
		return rocksdb::Status::OK();
	}

	void SetBackgroundThreads(int number, rocksdb::Env::Priority pri = LOW) override {
		auto& current = threadCounts[pri];
		auto& pool = threadPools[pri];
		if (!pool) {
			pool = CoroThreadPool::createThreadPool();
		}
		for (; current < number; ++current) {
			pool->addThread(new BGThread(threadCounter, cancableActions));
		}
	}
	int GetBackgroundThreads(rocksdb::Env::Priority pri = LOW) override {
		return threadCounts[pri];
	}
	void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
		SetBackgroundThreads(number, pri);
	}

	std::string GenerateUniqueId() override {
		return g_random->randomUniqueID().toString();
	}
};

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

struct RocksDBKeyValueStore : IKeyValueStore {
	using DB = rocksdb::DB*;
	using CF = rocksdb::ColumnFamilyHandle*;

	struct Writer : IThreadPoolReceiver {
		DB& db;
		CF& cf;
		UID id;
		rocksdb::WriteOptions writeOptions;

		explicit Writer(DB& db, CF& cf, UID id)
			: db(db), cf(cf), id(id)
		{}

		void init() override {}

		Error statusToError(const rocksdb::Status& s) {
			if (s == rocksdb::Status::IOError()) {
				return io_error();
			} else {
				return unknown_error();
			}
		}

		struct OpenAction : TypedAction<Writer, OpenAction> {
			rocksdb::Options options;
			rocksdb::ColumnFamilyOptions cfOptions;
			std::string path;
			ThreadReturnPromise<Void> done;
			
			double getTimeEstimate() {
				return SERVER_KNOBS->COMMIT_TIME_ESTIMATE;
			}
		};
		void action(OpenAction& a) {
			a.options.env = new FlowEnv(id);
			a.options.create_if_missing = true;
			auto status = rocksdb::DB::Open(a.options, a.path, &db);
			db->CreateColumnFamily(a.cfOptions, "fdb_cf", &cf);
			if (!status.ok()) {
				a.done.sendError(statusToError(status));
			} else {
				a.done.send(Void());
			}
		}

		struct SetAction : TypedAction<Writer, SetAction> {
			rocksdb::Slice key;
			rocksdb::Slice value;
			rocksdb::WriteOptions options;
			explicit SetAction(KeyValueRef kv)
				: key(toSlice(kv.key)), value(toSlice(kv.value))
			{}

			double getTimeEstimate() override { return SERVER_KNOBS->SET_TIME_ESTIMATE; }
		};
		void action(SetAction& a) {
			db->Put(a.options, cf, a.key, a.value);
		}

		struct ClearAction : TypedAction<Writer, ClearAction> {
			rocksdb::Slice begin, end;
			explicit ClearAction(KeyRangeRef range)
				: begin(toSlice(range.begin)), end(toSlice(range.end))
			{}
			double getTimeEstimate() override { return SERVER_KNOBS->CLEAR_TIME_ESTIMATE; }
		};
		void action(ClearAction& a) {
			db->DeleteRange(writeOptions, cf, a.begin, a.end);
		}

		struct CommitAction : TypedAction<Writer, CommitAction> {
			ThreadReturnPromise<Void> done;
			double getTimeEstimate() override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(CommitAction& a) {
			auto s = db->SyncWAL();
			if (!s.ok()) {
				a.done.sendError(statusToError(s));
			}
		}

		struct CloseAction : TypedAction<Writer, CloseAction> {
			ThreadReturnPromise<Void> done;
			double getTimeEstimate() override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(CloseAction& a) {
			db->Close();
			a.done.send(Void());
		}
	};

	struct Reader : IThreadPoolReceiver {
		DB& db;
		CF cf;
		rocksdb::ReadOptions readOptions;
		rocksdb::Iterator* cursor = nullptr;

		explicit Reader(DB& db, CF cf)
			: db(db)
			, cf(cf)
		{}

		~Reader() {
			if (cursor) {
				delete cursor;
			}
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
			if (a.debugID.present()) {
				g_traceBatch.addEvent("GetValueDebug", a.debugID.get().first(), "Reader.Before");
			}
			rocksdb::PinnableSlice value;
			auto s = db->Get(readOptions, cf, toSlice(a.key), &value);
			if (a.debugID.present()) {
				g_traceBatch.addEvent("GetValueDebug", a.debugID.get().first(), "Reader.After");
			}
			if (s.ok()) {
				a.result.send(Value(toStringRef(value)));
			} else {
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
			if (a.debugID.present()) {
				g_traceBatch.addEvent("GetValuePrefixDebug", a.debugID.get().first(),
									  "Reader.Before"); //.detail("TaskID", g_network->getCurrentTask());
			}
			auto s = db->Get(readOptions, cf, toSlice(a.key), &value);
			if (a.debugID.present()) {
				g_traceBatch.addEvent("GetValuePrefixDebug", a.debugID.get().first(),
									  "Reader.After"); //.detail("TaskID", g_network->getCurrentTask());
			}
			if (s.ok()) {
				a.result.send(Value(StringRef(reinterpret_cast<const uint8_t*>(value.data()),
											  std::min(value.size(), size_t(a.maxLength)))));
			} else {
				a.result.send(Optional<Value>());
			}
		}

		struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			KeyRange keys;
			int rowLimit, byteLimit;
			ThreadReturnPromise<Standalone<VectorRef<KeyValueRef>>> result;
			ReadRangeAction(KeyRange keys, int rowLimit, int byteLimit) : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit) {}
			virtual double getTimeEstimate() { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
		};
		void action(ReadRangeAction& a) {
			if (!cursor) {
				cursor = db->NewIterator(readOptions, cf);
			}
			cursor->Seek(toSlice(a.keys.begin));
			Standalone<VectorRef<KeyValueRef>> result;
			int accumulatedBytes = 0;
			while (cursor->Valid() &&
				   toStringRef(cursor->key()) < a.keys.end &&
				   result.size() < a.rowLimit &&
				   accumulatedBytes < a.byteLimit) {
				KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
				accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
				result.push_back_deep(result.arena(), kv);
			}
			a.result.send(result);
		}
	};

	DB db;
	CF cf;
	std::string path;
	UID id;
	size_t diskBytesUsed = 0;
	Reference<IThreadPool> writeThread;
	Reference<IThreadPool> readThreads;
	unsigned nReaders = 64;
	Promise<Void> errorPromise;
	Promise<Void> closePromise;

	explicit RocksDBKeyValueStore(const std::string& path, UID id)
		: path(path)
		, id(id)
	{
		writeThread = CoroThreadPool::createThreadPool();
		readThreads = CoroThreadPool::createThreadPool();
		writeThread->addThread(new Writer(db, cf, id));
		for (unsigned i = 0; i < nReaders; ++i) {
			readThreads->addThread(new Reader(db, cf));
		}
	}

	~RocksDBKeyValueStore() {
		delete cf;
		delete db;
	}

	Future<Void> getError() override {
		return errorPromise.getFuture();
	}

	ACTOR static void doClose(RocksDBKeyValueStore* self, bool deleteOnClose) {
		state Promise<Void> closePromise = self->closePromise;
		auto a = new Writer::CloseAction{};
		auto f = a->done.getFuture();
		self->writeThread->post(a);
		wait(f);
		delete self;
		// TODO: delete data on close
		closePromise.send(Void());
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
		std::unique_ptr<Writer::OpenAction> a(new Writer::OpenAction());
		a->options.env = new FlowEnv(id);
		a->path = path;
		auto res = a->done.getFuture();
		writeThread->post(a.release());
		return res;
	}

	void set(KeyValueRef kv, const Arena*) override {
		writeThread->post(new Writer::SetAction(kv));
	}

	void clear(KeyRangeRef keyRange, const Arena*) override {
		writeThread->post(new Writer::ClearAction(keyRange));
	}

	Future<Void> commit(bool) override {
		auto a = new Writer::CommitAction();
		auto res = a->done.getFuture();
		writeThread->post(a);
		return res;
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID) override {
		auto a = new Reader::ReadValueAction(key, debugID);
		auto res = a->result.getFuture();
		readThreads->post(a);
		return res;
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<UID> debugID) override {
		auto a = new Reader::ReadValuePrefixAction(key, maxLength, debugID);
		auto res = a->result.getFuture();
		readThreads->post(a);
		return res;
	}

	Future<Standalone<VectorRef<KeyValueRef>>> readRange(KeyRangeRef keys, int rowLimit, int byteLimit) override {
		auto a = new Reader::ReadRangeAction(keys, rowLimit, byteLimit);
		auto res = a->result.getFuture();
		readThreads->post(a);
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

IKeyValueStore* keyValueStoreRocksDB(std::string const& path, UID logID, KeyValueStoreType storeType, bool checkChecksums, bool checkIntegrity) {
	return new RocksDBKeyValueStore(path, logID);
}
