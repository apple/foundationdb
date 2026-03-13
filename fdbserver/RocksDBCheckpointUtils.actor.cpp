/*
 *RocksDBCheckpointUtils.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbserver/RocksDBCheckpointUtils.actor.h"

#ifdef WITH_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/types.h>
#include <rocksdb/version.h>
#endif // WITH_ROCKSDB

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/MutationTracking.h"
#include "fdbclient/StorageCheckpoint.h"
#include "fdbserver/CoroFlow.h"
#include "fdbserver/FDBRocksDBVersion.h"
#include "fdbserver/Knobs.h"
#include "flow/IThreadPool.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Trace.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

#ifdef WITH_ROCKSDB

// Enforcing rocksdb version.
static_assert((ROCKSDB_MAJOR == FDB_ROCKSDB_MAJOR && ROCKSDB_MINOR == FDB_ROCKSDB_MINOR &&
               ROCKSDB_PATCH == FDB_ROCKSDB_PATCH),
              "Unsupported rocksdb version.");

namespace {

using DB = rocksdb::DB*;
using CF = rocksdb::ColumnFamilyHandle*;

KeyRef const persistVersion = "\xff\xffVersion"_sr;
KeyRef const readerInitialized = "\xff\xff/ReaderInitialized"_sr;
std::string const checkpointCf = "RocksDBCheckpoint";
std::string const checkpointReaderSubDir = "/reader";
std::string const rocksDefaultCf = "default";

rocksdb::ExportImportFilesMetaData getMetaData(CheckpointMetaData const& checkpoint) {
	rocksdb::ExportImportFilesMetaData metaData;
	if (checkpoint.getFormat() != DataMoveRocksCF) {
		return metaData;
	}

	RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(checkpoint);
	metaData.db_comparator_name = rocksCF.dbComparatorName;

	for (LiveFileMetaData const& fileMetaData : rocksCF.sstFiles) {
		rocksdb::LiveFileMetaData liveFileMetaData;
		liveFileMetaData.relative_filename = fileMetaData.relative_filename;
		liveFileMetaData.directory = fileMetaData.directory;
		liveFileMetaData.file_number = fileMetaData.file_number;
		liveFileMetaData.file_type = static_cast<rocksdb::FileType>(fileMetaData.file_type);
		liveFileMetaData.size = fileMetaData.size;
		liveFileMetaData.temperature = static_cast<rocksdb::Temperature>(fileMetaData.temperature);
		liveFileMetaData.file_checksum = fileMetaData.file_checksum;
		liveFileMetaData.file_checksum_func_name = fileMetaData.file_checksum_func_name;
		liveFileMetaData.smallest_seqno = fileMetaData.smallest_seqno;
		liveFileMetaData.largest_seqno = fileMetaData.largest_seqno;
		liveFileMetaData.smallestkey = fileMetaData.smallestkey;
		liveFileMetaData.largestkey = fileMetaData.largestkey;
		liveFileMetaData.num_reads_sampled = fileMetaData.num_reads_sampled;
		liveFileMetaData.being_compacted = fileMetaData.being_compacted;
		liveFileMetaData.num_entries = fileMetaData.num_entries;
		liveFileMetaData.num_deletions = fileMetaData.num_deletions;
		liveFileMetaData.oldest_blob_file_number = fileMetaData.oldest_blob_file_number;
		liveFileMetaData.oldest_ancester_time = fileMetaData.oldest_ancester_time;
		liveFileMetaData.file_creation_time = fileMetaData.file_creation_time;
		liveFileMetaData.epoch_number = fileMetaData.epoch_number;
		liveFileMetaData.name = fileMetaData.name;
		liveFileMetaData.db_path = fileMetaData.db_path;
		liveFileMetaData.column_family_name = fileMetaData.column_family_name;
		liveFileMetaData.level = fileMetaData.level;
		liveFileMetaData.smallest = fileMetaData.smallest;
		liveFileMetaData.largest = fileMetaData.largest;
		liveFileMetaData.file_type = rocksdb::kTableFile;
		metaData.files.push_back(liveFileMetaData);
	}

	return metaData;
}

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<char const*>(s.begin()), s.size());
}

StringRef toStringRef(rocksdb::Slice s) {
	return StringRef(reinterpret_cast<uint8_t const*>(s.data()), s.size());
}

rocksdb::ColumnFamilyOptions getCFOptions() {
	rocksdb::ColumnFamilyOptions options;
	return options;
}

rocksdb::Options getOptions() {
	rocksdb::Options options({}, getCFOptions());
	options.create_if_missing = true;
	options.db_log_dir = g_network->isSimulated() ? "" : SERVER_KNOBS->LOG_DIRECTORY;
	return options;
}

// Set some useful defaults desired for all reads.
rocksdb::ReadOptions getReadOptions() {
	rocksdb::ReadOptions options;
	options.background_purge_on_iterator_cleanup = true;
	options.auto_prefix_mode = (SERVER_KNOBS->SHARDED_ROCKSDB_PREFIX_LEN > 0);
	return options;
}

void logRocksDBError(rocksdb::Status const& status, std::string const& method, UID const& logId) {
	auto level = status.IsTimedOut() ? SevWarn : SevError;
	TraceEvent e(level, "RocksDBCheckpointReaderError", logId);
	e.detail("Error", status.ToString()).detail("Method", method).detail("RocksDBSeverity", status.severity());
	if (status.IsIOError()) {
		e.detail("SubCode", status.subcode());
	}
}

Error statusToError(rocksdb::Status const& s) {
	if (s.IsIOError()) {
		return io_error();
	} else if (s.IsTimedOut()) {
		return transaction_too_old();
	} else {
		return unknown_error();
	}
}

// Fetch a single sst file from storage server. The progress is checkpointed via cFun.
ACTOR Future<int64_t> doFetchCheckpointFile(Database cx,
                                            std::string remoteFile,
                                            std::string localFile,
                                            UID ssId,
                                            UID checkpointId,
                                            int maxRetries = 3) {
	state Transaction tr(cx);
	state StorageServerInterface ssi;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> ss = wait(tr.get(serverListKeyFor(ssId)));
			if (!ss.present()) {
				throw checkpoint_not_found();
			}
			ssi = decodeServerListValue(ss.get());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	state int attempt = 0;
	state int64_t offset = 0;
	state Reference<IAsyncFile> asyncFile;
	loop {
		offset = 0;
		try {
			asyncFile = Reference<IAsyncFile>();
			++attempt;
			TraceEvent(SevDebug, "FetchCheckpointFileBegin")
			    .detail("RemoteFile", remoteFile)
			    .detail("LocalFile", localFile)
			    .detail("TargetUID", ssId)
			    .detail("CheckpointId", checkpointId)
			    .detail("Attempt", attempt);

			wait(IAsyncFileSystem::filesystem()->deleteFile(localFile, true));
			int64_t const flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE |
			                      IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
			wait(store(asyncFile, IAsyncFileSystem::filesystem()->open(localFile, flags, 0666)));

			state ReplyPromiseStream<FetchCheckpointReply> stream =
			    ssi.fetchCheckpoint.getReplyStream(FetchCheckpointRequest(checkpointId, remoteFile));
			TraceEvent(SevDebug, "FetchCheckpointFileReceivingData")
			    .detail("RemoteFile", remoteFile)
			    .detail("LocalFile", localFile)
			    .detail("TargetUID", ssId)
			    .detail("CheckpointId", checkpointId)
			    .detail("Attempt", attempt);
			loop {
				state FetchCheckpointReply rep = waitNext(stream.getFuture());
				wait(asyncFile->write(rep.data.begin(), rep.data.size(), offset));
				wait(asyncFile->flush());
				offset += rep.data.size();
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			} else if (e.code() != error_code_end_of_stream) {
				TraceEvent(SevWarnAlways, "FetchCheckpointFileError")
				    .errorUnsuppressed(e)
				    .detail("RemoteFile", remoteFile)
				    .detail("LocalFile", localFile)
				    .detail("TargetUID", ssId)
				    .detail("CheckpointId", checkpointId)
				    .detail("Attempt", attempt);
				if (attempt >= maxRetries) {
					throw e;
				}
			} else {
				wait(asyncFile->sync());
				int64_t fileSize = wait(asyncFile->size());
				TraceEvent(SevDebug, "FetchCheckpointFileEnd")
				    .detail("RemoteFile", remoteFile)
				    .detail("LocalFile", localFile)
				    .detail("TargetUID", ssId)
				    .detail("CheckpointId", checkpointId)
				    .detail("Attempt", attempt)
				    .detail("FileSize", fileSize);
				return fileSize;
			}
		}
	}
}

ACTOR Future<Void> fetchCheckpointBytesSampleFile(Database cx,
                                                  std::shared_ptr<CheckpointMetaData> metaData,
                                                  std::string dir,
                                                  std::function<Future<Void>(CheckpointMetaData const&)> cFun,
                                                  int maxRetries = 3) {
	ASSERT(metaData->bytesSampleFile.present());
	state std::string localFile = joinPath(dir, metaData->checkpointID.toString() + "_metadata_bytes.sst");
	TraceEvent(SevDebug, "FetchCheckpointByteSampleBegin")
	    .detail("Checkpoint", metaData->toString())
	    .detail("LocalFile", localFile);

	ASSERT(!metaData->src.empty());
	state UID ssId = metaData->src.front();

	wait(success(doFetchCheckpointFile(cx, metaData->bytesSampleFile.get(), localFile, ssId, metaData->checkpointID)));
	metaData->bytesSampleFile = localFile;
	if (cFun) {
		wait(cFun(*metaData));
	}
	return Void();
}

// RocksDBColumnFamilyReader reads a RocksDB checkpoint, and returns the key-value pairs via nextKeyValues.
class RocksDBColumnFamilyReader : public ICheckpointReader {
public:
	class RocksCfCheckpointIterator : public ICheckpointIterator {
	public:
		RocksCfCheckpointIterator(RocksDBColumnFamilyReader* reader, KeyRange const& range, UID const& logId)
		  : reader(reader), range(range), logId(logId) {
			ASSERT(reader != nullptr);
			ASSERT(reader->db != nullptr);
			ASSERT(reader->cf != nullptr);
			this->beginSlice = toSlice(this->range.begin);
			this->endSlice = toSlice(this->range.end);
			rocksdb::ReadOptions options = getReadOptions();
			options.iterate_lower_bound = &beginSlice;
			options.iterate_upper_bound = &endSlice;
			options.fill_cache = false; // Optimized for bulk scan.
			options.readahead_size = SERVER_KNOBS->ROCKSDB_CHECKPOINT_READ_AHEAD_SIZE;
			// Note: ROCKSDB_SET_READ_TIMEOUT is false in simulation. If turned on, this code could lead to
			// non-deterministic simulation because of db->GetEnv()->NowMicros().
			if (SERVER_KNOBS->ROCKSDB_SET_READ_TIMEOUT) {
				uint64_t const deadlineMicros =
				    reader->db->GetEnv()->NowMicros() + SERVER_KNOBS->ROCKSDB_READ_CHECKPOINT_TIMEOUT * 1000000;
				options.deadline = std::chrono::microseconds(deadlineMicros);
			}
			this->iterator = std::unique_ptr<rocksdb::Iterator>(reader->db->NewIterator(options, reader->cf));
			iterator->Seek(this->beginSlice);
		}

		~RocksCfCheckpointIterator() { this->reader->numIter--; }

		Future<RangeResult> nextBatch(int const rowLimit, int const ByteLimit) override;

		rocksdb::Iterator* getIterator() { return iterator.get(); }

		rocksdb::Slice const& end() const { return this->endSlice; }

	private:
		RocksDBColumnFamilyReader* const reader;
		KeyRange const range;
		rocksdb::Slice beginSlice;
		rocksdb::Slice endSlice;
		std::unique_ptr<rocksdb::Iterator> iterator;
		UID const logId;
	};

	class DummyRocksDBCheckpointIterator : public ICheckpointIterator {
	public:
		DummyRocksDBCheckpointIterator(UID const& logId) : logId(logId) {}

		~DummyRocksDBCheckpointIterator() {}

		Future<RangeResult> nextBatch(int const rowLimit, int const ByteLimit) override { throw end_of_stream(); }

	private:
		UID const logId;
	};

	RocksDBColumnFamilyReader(CheckpointMetaData const& checkpoint, UID logId);

	Future<Void> init(StringRef token) override;

	Future<RangeResult> nextKeyValues(int const rowLimit, int const byteLimit) override { throw not_implemented(); }

	Future<Standalone<StringRef>> nextChunk(int const byteLimit) override { throw not_implemented(); }

	Future<Void> close() override { return doClose(this); }

	std::unique_ptr<ICheckpointIterator> getIterator(KeyRange range) override;

	bool inUse() const override { return this->numIter > 0; }

private:
	struct Reader : IThreadPoolReceiver {
		struct OpenAction : TypedAction<Reader, OpenAction> {
			OpenAction(CheckpointMetaData checkpoint) : checkpoint(std::move(checkpoint)) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }

			CheckpointMetaData const checkpoint;
			ThreadReturnPromise<Void> done;
		};

		struct CloseAction : TypedAction<Reader, CloseAction> {
			CloseAction(std::string path, bool deleteOnClose) : path(path), deleteOnClose(deleteOnClose) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }

			std::string path;
			bool deleteOnClose;
			ThreadReturnPromise<Void> done;
		};

		struct ReadRangeAction : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			ReadRangeAction(int rowLimit, int byteLimit, RocksCfCheckpointIterator* iterator)
			  : rowLimit(rowLimit), byteLimit(byteLimit), iterator(iterator), startTime(timer_monotonic()) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }

			int const rowLimit, byteLimit;
			RocksCfCheckpointIterator* const iterator;
			double const startTime;
			ThreadReturnPromise<RangeResult> result;
		};

		explicit Reader(DB& db, CF& cf, std::vector<rocksdb::ColumnFamilyHandle*>& handles, UID const& logId);
		~Reader() override {}

		void init() override {}

		void action(OpenAction& a);

		void action(CloseAction& a);

		void action(ReadRangeAction& a);

		rocksdb::Status tryOpenForRead(std::string const& path);

		rocksdb::Status importCheckpoint(std::string const& path, CheckpointMetaData const& checkpoint);

		rocksdb::Status closeInternal(std::string const& path, bool const deleteOnClose);

		DB& db;
		CF& cf;
		std::vector<rocksdb::ColumnFamilyHandle*>& handles;
		double readRangeTimeout;
		UID const logId;
	};

	Future<RangeResult> nextBatch(int const rowLimit, int const byteLimit, RocksCfCheckpointIterator* iterator);

	ACTOR static Future<Void> doClose(RocksDBColumnFamilyReader* self);

	DB db = nullptr;
	CF cf = nullptr;
	std::vector<rocksdb::ColumnFamilyHandle*> handles;
	std::string path;
	UID const logId;
	Version version;
	CheckpointMetaData checkpoint;
	Reference<IThreadPool> threads;
	Future<Void> openFuture;
	int numIter;
	bool empty;
};

Future<RangeResult> RocksDBColumnFamilyReader::RocksCfCheckpointIterator::nextBatch(int const rowLimit,
                                                                                    int const ByteLimit) {
	return this->reader->nextBatch(rowLimit, ByteLimit, this);
}

RocksDBColumnFamilyReader::RocksDBColumnFamilyReader(CheckpointMetaData const& checkpoint, UID logId)
  : logId(logId), checkpoint(checkpoint), numIter(0) {
	if (g_network->isSimulated()) {
		threads = CoroThreadPool::createThreadPool();
	} else {
		threads = createGenericThreadPool();
	}
	for (int i = 0; i < SERVER_KNOBS->ROCKSDB_CHECKPOINT_READER_PARALLELISM; ++i) {
		threads->addThread(new Reader(db, cf, handles, logId), "fdb-rocks-cr");
	}
	if (checkpoint.getFormat() == DataMoveRocksCF) {
		RocksDBColumnFamilyCheckpoint const rocksCF = getRocksCF(checkpoint);
		this->empty = rocksCF.sstFiles.empty();
	} else {
		this->empty = false;
	}
}

Future<Void> RocksDBColumnFamilyReader::init(StringRef token) {
	if (openFuture.isValid()) {
		return openFuture;
	}

	if (this->empty) {
		return Void();
	}

	auto a = std::make_unique<Reader::OpenAction>(this->checkpoint);
	openFuture = a->done.getFuture();
	threads->post(a.release());
	return openFuture;
}

Future<RangeResult> RocksDBColumnFamilyReader::nextBatch(int const rowLimit,
                                                         int const byteLimit,
                                                         RocksCfCheckpointIterator* iterator) {
	auto a = std::make_unique<Reader::ReadRangeAction>(rowLimit, byteLimit, iterator);
	auto res = a->result.getFuture();
	threads->post(a.release());
	return res;
}

std::unique_ptr<ICheckpointIterator> RocksDBColumnFamilyReader::getIterator(KeyRange range) {
	++this->numIter;
	if (this->empty) {
		return std::unique_ptr<ICheckpointIterator>(new DummyRocksDBCheckpointIterator(logId));
	} else {
		return std::unique_ptr<ICheckpointIterator>(new RocksCfCheckpointIterator(this, range, logId));
	}
}

RocksDBColumnFamilyReader::Reader::Reader(DB& db,
                                          CF& cf,
                                          std::vector<rocksdb::ColumnFamilyHandle*>& handles,
                                          UID const& logId)
  : db(db), cf(cf), handles(handles), logId(logId) {}

void RocksDBColumnFamilyReader::Reader::action(RocksDBColumnFamilyReader::Reader::OpenAction& a) {
	TraceEvent(SevDebug, "RocksDBCheckpointReaderInitBegin", logId).detail("Checkpoint", a.checkpoint.toString());
	ASSERT(cf == nullptr);

	CheckpointMetaData const& checkpoint = a.checkpoint;
	CheckpointFormat const format = checkpoint.getFormat();
	if (format != DataMoveRocksCF) {
		TraceEvent(SevDebug, "RocksDBCheckpointReaderError", logId).detail("InvalidFormat", checkpoint.toString());
		a.done.sendError(not_implemented());
		return;
	}

	RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(checkpoint);
	ASSERT(!rocksCF.sstFiles.empty());
	std::vector<std::string> const files = platform::listFiles(rocksCF.sstFiles.front().db_path);
	TraceEvent(SevDebug, "RocksDBColumnFamilyReaderInputFiles", logId).detail("Files", describe(files));
	std::string const path = joinPath(rocksCF.sstFiles.front().db_path, checkpointReaderSubDir);

	// Try to read the existing DB, the checkpoint could have been imported already.
	rocksdb::Status status = tryOpenForRead(path);

	// Existing DB is not ready to use, redo checkpoint import.
	if (!status.ok()) {
		TraceEvent(SevDebug, "RocksDBCheckpointOpenForReadFailed", logId)
		    .detail("Status", status.ToString())
		    .detail("Path", path);
		platform::eraseDirectoryRecursive(path);
		status = importCheckpoint(path, checkpoint);
		if (status.ok()) {
			status = tryOpenForRead(path);
		}
	}

	if (!status.ok()) {
		a.done.sendError(checkpoint_not_found());
		return;
	}

	a.done.send(Void());
	TraceEvent(SevDebug, "RocksDBCheckpointReaderInitEnd", logId)
	    .detail("Path", path)
	    .detail("ColumnFamily", cf->GetName());
}

void RocksDBColumnFamilyReader::Reader::action(RocksDBColumnFamilyReader::Reader::CloseAction& a) {
	closeInternal(a.path, a.deleteOnClose);
	a.done.send(Void());
}

void RocksDBColumnFamilyReader::Reader::action(RocksDBColumnFamilyReader::Reader::ReadRangeAction& a) {
	TraceEvent(SevDebug, "RocksDBCheckpointReaderReadRangeBegin", logId);
	ASSERT(a.iterator != nullptr);

	RangeResult result;
	if (a.rowLimit == 0 || a.byteLimit == 0) {
		a.result.send(result);
		return;
	}

	// For now, only forward scan is supported.
	ASSERT(a.rowLimit > 0);

	rocksdb::Iterator* iter = a.iterator->getIterator();
	int accumulatedBytes = 0;
	rocksdb::Status s;
	while (iter->Valid() && iter->key().compare(a.iterator->end()) < 0) {
		KeyValueRef kv(toStringRef(iter->key()), toStringRef(iter->value()));
		accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
		result.push_back_deep(result.arena(), kv);
		iter->Next();
		if (result.size() >= a.rowLimit || accumulatedBytes >= a.byteLimit) {
			break;
		}
	}

	s = iter->status();

	if (!s.ok()) {
		logRocksDBError(s, "ReadRange", logId);
		a.result.sendError(statusToError(s));
		return;
	}

	if (result.empty()) {
		a.result.sendError(end_of_stream());
	} else {
		a.result.send(result);
	}
}

rocksdb::Status RocksDBColumnFamilyReader::Reader::tryOpenForRead(std::string const& path) {
	std::vector<std::string> columnFamilies;
	rocksdb::Options const options = getOptions();
	rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, path, &columnFamilies);
	if (std::find(columnFamilies.begin(), columnFamilies.end(), rocksDefaultCf) == columnFamilies.end() ||
	    std::find(columnFamilies.begin(), columnFamilies.end(), checkpointCf) == columnFamilies.end()) {
		return rocksdb::Status::Aborted();
	}

	rocksdb::ColumnFamilyOptions const cfOptions = getCFOptions();
	std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
	for (std::string const& name : columnFamilies) {
		descriptors.emplace_back(name, cfOptions);
	}
	status = rocksdb::DB::OpenForReadOnly(options, path, descriptors, &handles, &db);
	if (!status.ok()) {
		logRocksDBError(status, "OpenForReadOnly", logId);
		return status;
	}

	rocksdb::PinnableSlice value;
	rocksdb::ReadOptions readOptions = getReadOptions();
	status = db->Get(readOptions, db->DefaultColumnFamily(), toSlice(readerInitialized), &value);
	if (!status.ok() && !status.IsNotFound()) {
		logRocksDBError(status, "CheckpointCheckInitState", logId);
		auto closeStatus = closeInternal(path, /*deleteOnClose=*/true);
		if (!closeStatus.ok()) {
			logRocksDBError(closeStatus, "CheckpointCloseInternal", logId);
		}
		delete db;
		return status;
	}

	if (status.IsNotFound()) {
		status = closeInternal(path, /*deleteOnClose=*/true);
		if (!status.ok()) {
			logRocksDBError(status, "CheckpointCloseInternal", logId);
		}
		delete db;
		TraceEvent(SevDebug, "RocksDBCheckpointReaderTryOpenError", logId).detail("Path", path);
		return rocksdb::Status::Aborted();
	}

	ASSERT(handles.size() == 2);
	for (rocksdb::ColumnFamilyHandle* handle : handles) {
		if (handle->GetName() == checkpointCf) {
			TraceEvent(SevDebug, "RocksDBCheckpointCF", logId)
			    .detail("Path", path)
			    .detail("ColumnFamily", handle->GetName());
			cf = handle;
			break;
		}
	}

	ASSERT(db != nullptr && cf != nullptr);
	return rocksdb::Status::OK();
}

rocksdb::Status RocksDBColumnFamilyReader::Reader::importCheckpoint(std::string const& path,
                                                                    CheckpointMetaData const& checkpoint) {
	TraceEvent(SevInfo, "CheckpointReaderImportCheckpointBegin", logId)
	    .detail("Path", path)
	    .detail("Checkpoint", checkpoint.toString());
	std::vector<std::string> columnFamilies;
	rocksdb::Options const options = getOptions();
	rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, path, &columnFamilies);
	if (std::find(columnFamilies.begin(), columnFamilies.end(), rocksDefaultCf) == columnFamilies.end()) {
		columnFamilies.push_back(rocksDefaultCf);
	}
	ASSERT(std::find(columnFamilies.begin(), columnFamilies.end(), checkpointCf) == columnFamilies.end());

	rocksdb::ColumnFamilyOptions const cfOptions = getCFOptions();
	std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
	for (std::string const& name : columnFamilies) {
		descriptors.emplace_back(name, cfOptions);
	}

	status = rocksdb::DB::Open(options, path, descriptors, &handles, &db);
	if (!status.ok()) {
		TraceEvent(SevWarn, "CheckpointReaderOpenedFailed", logId)
		    .detail("Status", status.ToString())
		    .detail("Path", path)
		    .detail("Checkpoint", checkpoint.toString());
		return status;
	}

	TraceEvent(SevInfo, "CheckpointReaderOpenedForImport", logId)
	    .detail("Path", path)
	    .detail("Checkpoint", checkpoint.toString());

	rocksdb::ExportImportFilesMetaData metaData = getMetaData(checkpoint);
	rocksdb::ImportColumnFamilyOptions importOptions;
	importOptions.move_files = false;
	status = db->CreateColumnFamilyWithImport(cfOptions, checkpointCf, importOptions, metaData, &cf);
	if (!status.ok()) {
		logRocksDBError(status, "CheckpointReaderImportCheckpoint", logId);
		return status;
	}
	handles.push_back(cf);
	TraceEvent(SevDebug, "RocksDBCheckpointReaderImportedCF", logId);

	rocksdb::WriteOptions writeOptions;
	writeOptions.sync = !SERVER_KNOBS->ROCKSDB_UNSAFE_AUTO_FSYNC;
	status = db->Put(writeOptions, toSlice(readerInitialized), toSlice("1"_sr));
	if (!status.ok()) {
		logRocksDBError(status, "CheckpointReaderPersistInitKey", logId);
		return status;
	}
	ASSERT(db != nullptr && cf != nullptr);

	return closeInternal(path, /*deleteOnClose=*/false);
}

rocksdb::Status RocksDBColumnFamilyReader::Reader::closeInternal(std::string const& path, bool const deleteOnClose) {
	if (db == nullptr) {
		return rocksdb::Status::OK();
	}

	for (rocksdb::ColumnFamilyHandle* handle : handles) {
		if (handle != nullptr) {
			TraceEvent("RocksDBCheckpointReaderDestroyCF", logId).detail("Path", path).detail("CF", handle->GetName());
			db->DestroyColumnFamilyHandle(handle);
		}
	}
	handles.clear();

	rocksdb::Status s = db->Close();
	if (!s.ok()) {
		logRocksDBError(s, "Close", logId);
	}

	if (deleteOnClose) {
		rocksdb::ColumnFamilyOptions options;
		std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
		descriptors.emplace_back(rocksDefaultCf, options);
		descriptors.emplace_back(checkpointCf, options);
		s = rocksdb::DestroyDB(path, getOptions(), descriptors);
		if (!s.ok()) {
			logRocksDBError(s, "Destroy", logId);
		} else {
			TraceEvent(SevDebug, "RocksDBColumnFamilyReader", logId).detail("Path", path).detail("Method", "Destroy");
		}
	}

	TraceEvent(SevDebug, "RocksDBColumnFamilyReader", logId).detail("Path", path).detail("Method", "Close");
	return s;
}

ACTOR Future<Void> RocksDBColumnFamilyReader::doClose(RocksDBColumnFamilyReader* self) {
	if (self == nullptr)
		return Void();

	auto a = new RocksDBColumnFamilyReader::Reader::CloseAction(self->path, false);
	auto f = a->done.getFuture();
	self->threads->post(a);
	wait(f);

	if (self != nullptr) {
		wait(self->threads->stop());
	}

	if (self != nullptr) {
		if (self->db != nullptr) {
			delete self->db;
		}
		delete self;
	}

	return Void();
}

class RocksDBSstFileWriter : public IRocksDBSstFileWriter {
public:
	RocksDBSstFileWriter()
	  : writer(std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), rocksdb::Options())), hasData(false){};

	void open(std::string const localFile) override;

	void write(KeyRef const key, ValueRef const value) override;

	bool finish() override;

private:
	std::unique_ptr<rocksdb::SstFileWriter> writer;
	std::string localFile;
	bool hasData;
};

void RocksDBSstFileWriter::open(std::string const localFile) {
	this->localFile = abspath(localFile);
	rocksdb::Status status = this->writer->Open(this->localFile);
	if (!status.ok()) {
		TraceEvent(SevError, "RocksDBSstFileWriterWrapperOpenFileError")
		    .detail("LocalFile", this->localFile)
		    .detail("Status", status.ToString());
		throw failed_to_create_checkpoint_shard_metadata();
	}
}

void RocksDBSstFileWriter::write(KeyRef const key, ValueRef const value) {
	rocksdb::Status status = this->writer->Put(toSlice(key), toSlice(value));
	if (!status.ok()) {
		TraceEvent(SevError, "RocksDBSstFileWriterWrapperWriteError")
		    .detail("LocalFile", this->localFile)
		    .detail("Key", key)
		    .detail("Value", value)
		    .detail("Status", status.ToString());
		throw failed_to_create_checkpoint_shard_metadata();
	}
	this->hasData = true;
}

bool RocksDBSstFileWriter::finish() {
	if (!this->hasData) {
		// writer->finish() cannot create sst file with no entries
		// So, we have to check whether any data set to be written to sst file before writer->finish()
		return false;
	}
	rocksdb::Status status = this->writer->Finish();
	if (!status.ok()) {
		TraceEvent(SevError, "RocksDBSstFileWriterWrapperCloseError")
		    .detail("LocalFile", this->localFile)
		    .detail("Status", status.ToString());
		throw failed_to_create_checkpoint_shard_metadata();
	}
	return true;
}

class RocksDBSstFileReader : public IRocksDBSstFileReader {
public:
	RocksDBSstFileReader() : sstReader(std::make_unique<rocksdb::SstFileReader>(rocksdb::Options())){};

	RocksDBSstFileReader(KeyRange const& rangeBoundary, size_t rowLimit, size_t byteLimit)
	  : sstReader(std::make_unique<rocksdb::SstFileReader>(rocksdb::Options())), rowLimit(rowLimit),
	    byteLimit(byteLimit) {
		beginSlice = toSlice(rangeBoundary.begin);
		endSlice = toSlice(rangeBoundary.end);
	};

	~RocksDBSstFileReader() {}

	void open(std::string const localFile) override;

	KeyValue next() override;

	bool hasNext() const override;

	RangeResult getRange(KeyRange const& range) override;

private:
	std::unique_ptr<rocksdb::SstFileReader> sstReader;
	std::unique_ptr<rocksdb::Iterator> iter;
	std::string localFile;
	rocksdb::Slice beginSlice;
	rocksdb::Slice endSlice;
	size_t byteLimit = -1;
	size_t rowLimit = -1;
};

void RocksDBSstFileReader::open(std::string const localFile) {
	this->localFile = abspath(localFile);
	rocksdb::Status status = sstReader->Open(this->localFile);
	if (status.ok()) {
		rocksdb::ReadOptions readOptions = getReadOptions();
		if (!beginSlice.empty()) {
			readOptions.iterate_lower_bound = &beginSlice;
		}
		if (!endSlice.empty()) {
			readOptions.iterate_upper_bound = &endSlice;
			// Note that iterator can return the endSlice but this endSlice is out of the range.
			// So, we need to check whether the key from iterator is out of the range.
		}
		iter.reset(sstReader->NewIterator(readOptions));
		if (!beginSlice.empty()) {
			iter->Seek(beginSlice);
		} else {
			iter->SeekToFirst();
		}
	} else {
		TraceEvent(SevError, "RocksDBSstFileReaderWrapperOpenFileError")
		    .detail("LocalFile", this->localFile)
		    .detail("Status", status.ToString());
	}
}

bool RocksDBSstFileReader::hasNext() const {
	return iter != nullptr && this->iter->Valid();
}

KeyValue RocksDBSstFileReader::next() {
	KeyValue res(KeyValueRef(toStringRef(this->iter->key()), toStringRef(this->iter->value())));
	iter->Next();
	return res;
}

// TODO(BulkLoad): current the bulkload is the only place using the method.
// This is implemented as the sync call for the simplicity. In the future
// we will implement this as async call using action.
RangeResult RocksDBSstFileReader::getRange(KeyRange const& range) {
	RangeResult rep;
	size_t expectedSize = 0;
	size_t keyCount = 0;
	ASSERT(iter != nullptr);
	iter->Seek(toSlice(range.begin));
	if (!iter->Valid() || toStringRef(iter->key()) >= range.end) {
		rep.more = false;
		return rep;
	}
	while (true) {
		KeyValue kv = KeyValueRef(toStringRef(iter->key()), toStringRef(iter->value()));
		rep.push_back_deep(rep.arena(), kv);
		expectedSize = expectedSize + kv.expectedSize();
		keyCount++;
		if (g_network->isSimulated() && SERVER_KNOBS->BULKLOAD_SIM_FAILURE_INJECTION &&
		    deterministicRandom()->random01() < 0.1) {
			TraceEvent(SevWarnAlways, "TryGetRangeForBulkLoadInjectError");
			throw operation_failed();
		}
		if ((byteLimit > 0 && expectedSize >= byteLimit) || (rowLimit > 0 && keyCount >= rowLimit)) {
			break;
		}

		iter->Next(); // Go to next at first, then decide if the rep has more.
		if (!iter->Valid()) {
			break;
		}
		if (toStringRef(iter->key()) >= range.end) {
			break;
		}
	}
	rep.more = iter->Valid() && toStringRef(iter->key()) < range.end;
	return rep;
}

class RocksDBCheckpointByteSampleReader : public ICheckpointByteSampleReader {
public:
	RocksDBCheckpointByteSampleReader(CheckpointMetaData const& checkpoint);
	~RocksDBCheckpointByteSampleReader() {}

	KeyValue next() override;

	bool hasNext() const override;

private:
	std::unique_ptr<rocksdb::SstFileReader> sstReader;
	std::unique_ptr<rocksdb::Iterator> iter;
};

RocksDBCheckpointByteSampleReader::RocksDBCheckpointByteSampleReader(CheckpointMetaData const& checkpoint)
  : sstReader(std::make_unique<rocksdb::SstFileReader>(rocksdb::Options())) {
	ASSERT(checkpoint.bytesSampleFile.present());
	rocksdb::Status status = sstReader->Open(checkpoint.bytesSampleFile.get());
	TraceEvent(SevDebug, "RocksDBCheckpointByteSampleReaderInit")
	    .detail("Checkpoint", checkpoint.toString())
	    .detail("Status", status.ToString());
	if (status.ok()) {
		iter.reset(sstReader->NewIterator(getReadOptions()));
		iter->SeekToFirst();
	} else {
		TraceEvent(SevError, "RocksDBCheckpointByteSampleReaderInit")
		    .detail("Checkpoint", checkpoint.toString())
		    .detail("Status", status.ToString());
	}
}

bool RocksDBCheckpointByteSampleReader::hasNext() const {
	return iter != nullptr && this->iter->Valid();
}

KeyValue RocksDBCheckpointByteSampleReader::next() {
	KeyValue res(KeyValueRef(toStringRef(this->iter->key()), toStringRef(this->iter->value())));
	iter->Next();
	return res;
}

// RocksDBCFCheckpointReader reads an exported RocksDB Column Family checkpoint files, and returns the
// serialized checkpoint via nextChunk.
class RocksDBCFCheckpointReader : public ICheckpointReader {
public:
	RocksDBCFCheckpointReader(CheckpointMetaData const& checkpoint, UID logId)
	  : checkpoint_(checkpoint), id_(logId), file_(Reference<IAsyncFile>()), offset_(0) {}

	Future<Void> init(StringRef token) override;

	Future<RangeResult> nextKeyValues(int const rowLimit, int const byteLimit) override { throw not_implemented(); }

	Future<Standalone<StringRef>> nextChunk(int const byteLimit) override;

	Future<Void> close() override;

private:
	ACTOR static Future<Void> doInit(RocksDBCFCheckpointReader* self) {
		ASSERT(self != nullptr);
		try {
			state Reference<IAsyncFile> _file = wait(IAsyncFileSystem::filesystem()->open(
			    self->path_, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO, 0));
			self->file_ = _file;
			TraceEvent("RocksDBCheckpointReaderOpenFile").detail("File", self->path_);
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "RocksDBCFCheckpointReaderInitError")
			    .errorUnsuppressed(e)
			    .detail("File", self->path_);
			throw e;
		}

		return Void();
	}

	ACTOR static UNCANCELLABLE Future<Standalone<StringRef>> getNextChunk(RocksDBCFCheckpointReader* self,
	                                                                      int byteLimit) {
		int blockSize = std::min(64 * 1024, byteLimit); // Block size read from disk.
		state Standalone<StringRef> buf = makeAlignedString(_PAGE_SIZE, blockSize);
		int bytesRead = wait(self->file_->read(mutateString(buf), blockSize, self->offset_));
		if (bytesRead == 0) {
			throw end_of_stream();
		}

		self->offset_ += bytesRead;
		return buf.substr(0, bytesRead);
	}

	ACTOR static Future<Void> doClose(RocksDBCFCheckpointReader* self) {
		wait(delay(0, TaskPriority::FetchKeys));
		delete self;
		return Void();
	}

	CheckpointMetaData checkpoint_;
	UID id_;
	Reference<IAsyncFile> file_;
	int offset_;
	std::string path_;
};

Future<Void> RocksDBCFCheckpointReader::init(StringRef token) {
	ASSERT_EQ(this->checkpoint_.getFormat(), DataMoveRocksCF);
	std::string const name = token.toString();
	this->offset_ = 0;
	this->path_.clear();
	RocksDBColumnFamilyCheckpoint const rocksCF = getRocksCF(this->checkpoint_);
	for (auto const& sstFile : rocksCF.sstFiles) {
		if (sstFile.name == name) {
			this->path_ = sstFile.db_path + sstFile.name;
			break;
		}
	}

	if (this->path_.empty() && this->checkpoint_.bytesSampleFile.present() &&
	    this->checkpoint_.bytesSampleFile.get() == name) {
		this->path_ = name;
	}

	if (this->path_.empty()) {
		TraceEvent("RocksDBCheckpointReaderInitFileNotFound").detail("File", this->path_);
		return checkpoint_not_found();
	}

	return doInit(this);
}

Future<Standalone<StringRef>> RocksDBCFCheckpointReader::nextChunk(int const byteLimit) {
	return getNextChunk(this, byteLimit);
}

Future<Void> RocksDBCFCheckpointReader::close() {
	return doClose(this);
}

// Fetch a single sst file from storage server. The progress is checkpointed via cFun.
ACTOR Future<Void> fetchCheckpointFile(Database cx,
                                       std::shared_ptr<CheckpointMetaData> metaData,
                                       int idx,
                                       std::string dir,
                                       std::function<Future<Void>(CheckpointMetaData const&)> cFun,
                                       int maxRetries = 3) {
	state RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(*metaData);

	// Skip fetched file.
	if (rocksCF.sstFiles[idx].fetched && rocksCF.sstFiles[idx].db_path == dir) {
		return Void();
	}

	std::string const remoteFile = rocksCF.sstFiles[idx].name;
	std::string const localFile = joinPath(dir, rocksCF.sstFiles[idx].name);
	ASSERT_EQ(metaData->src.size(), 1);
	UID const ssId = metaData->src.front();

	wait(success(doFetchCheckpointFile(cx, remoteFile, localFile, ssId, metaData->checkpointID)));
	rocksCF.sstFiles[idx].db_path = dir;
	rocksCF.sstFiles[idx].fetched = true;
	metaData->setSerializedCheckpoint(ObjectWriter::toValue(rocksCF, IncludeVersion()));
	if (cFun) {
		wait(cFun(*metaData));
	}
	return Void();
}

// TODO: Return when a file exceeds a limit.
ACTOR Future<Void> fetchCheckpointRange(Database cx,
                                        std::shared_ptr<CheckpointMetaData> metaData,
                                        KeyRange range,
                                        std::string dir,
                                        std::shared_ptr<rocksdb::SstFileWriter> writer,
                                        std::function<Future<Void>(CheckpointMetaData const&)> cFun,
                                        int maxRetries = 3) {
	state std::string localFile =
	    joinPath(dir, UID(metaData->checkpointID.first(), deterministicRandom()->randomUInt64()).toString() + ".sst");
	RocksDBCheckpointKeyValues rkv = getRocksKeyValuesCheckpoint(*metaData);
	TraceEvent("FetchCheckpointRange", metaData->checkpointID)
	    .detail("InitialState", metaData->toString())
	    .detail("RocksCheckpointKeyValues", rkv.toString())
	    .detail("FilePath", localFile);

	for (auto const& file : rkv.fetchedFiles) {
		ASSERT(!file.range.intersects(range));
	}

	ASSERT(!metaData->src.empty());
	state UID ssID = metaData->src.front();
	state Transaction tr(cx);
	state StorageServerInterface ssi;
	loop {
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			Optional<Value> ss = wait(tr.get(serverListKeyFor(ssID)));
			if (!ss.present()) {
				TraceEvent(SevWarnAlways, "FetchCheckpointRangeStorageServerNotFound", metaData->checkpointID)
				    .detail("SSID", ssID)
				    .detail("InitialState", metaData->toString());
				throw checkpoint_not_found();
			}
			ssi = decodeServerListValue(ss.get());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	ASSERT(ssi.id() == ssID);

	state int attempt = 0;
	state int64_t totalBytes = 0;
	state int64_t totalKeys = 0;
	state rocksdb::Status status;
	state Optional<Error> error;
	loop {
		totalBytes = 0;
		totalKeys = 0;
		++attempt;
		try {
			TraceEvent(SevInfo, "FetchCheckpointRangeBegin", metaData->checkpointID)
			    .detail("Range", range)
			    .detail("CheckpointID", metaData->checkpointID)
			    .detail("TargetStorageServerUID", ssID)
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt)
			    .log();

			wait(IAsyncFileSystem::filesystem()->deleteFile(localFile, true));
			status = writer->Open(localFile);
			if (!status.ok()) {
				Error e = statusToError(status);
				TraceEvent(SevError, "FetchCheckpointRangeOpenFileError", metaData->checkpointID)
				    .detail("LocalFile", localFile)
				    .detail("Status", status.ToString());
				throw e;
			}

			state ReplyPromiseStream<FetchCheckpointKeyValuesStreamReply> stream =
			    ssi.fetchCheckpointKeyValues.getReplyStream(
			        FetchCheckpointKeyValuesRequest(metaData->checkpointID, range));
			TraceEvent(SevDebug, "FetchCheckpointKeyValuesReceivingData", metaData->checkpointID)
			    .detail("Range", range)
			    .detail("CheckpointID", metaData->checkpointID)
			    .detail("TargetStorageServerUID", ssID)
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt)
			    .log();

			loop {
				FetchCheckpointKeyValuesStreamReply rep = waitNext(stream.getFuture());
				for (int i = 0; i < rep.data.size(); ++i) {
					DEBUG_MUTATION("FetchCheckpointData",
					               metaData->version,
					               MutationRef(MutationRef::SetValue, rep.data[i].key, rep.data[i].value),
					               metaData->checkpointID);
					status = writer->Put(toSlice(rep.data[i].key), toSlice(rep.data[i].value));
					if (!status.ok()) {
						Error e = statusToError(status);
						TraceEvent(SevError, "FetchCheckpointRangeWriteError", metaData->checkpointID)
						    .detail("LocalFile", localFile)
						    .detail("Key", rep.data[i].key)
						    .detail("Value", rep.data[i].value)
						    .detail("Status", status.ToString());
						throw e;
					}
					totalBytes += rep.data[i].expectedSize();
					++totalKeys;
				}
			}
		} catch (Error& e) {
			Error err = e;
			if (totalBytes > 0) {
				status = writer->Finish();
				if (!status.ok()) {
					err = statusToError(status);
				}
			}
			if (err.code() != error_code_end_of_stream) {
				TraceEvent(SevWarn, "FetchCheckpointRangeError", metaData->checkpointID)
				    .errorUnsuppressed(err)
				    .detail("Range", range)
				    .detail("CheckpointID", metaData->checkpointID)
				    .detail("TargetStorageServerUID", ssID)
				    .detail("LocalFile", localFile)
				    .detail("Attempt", attempt);
				if (attempt >= maxRetries) {
					error = err;
					break;
				}
			} else {
				RocksDBCheckpointKeyValues rcp = getRocksKeyValuesCheckpoint(*metaData);
				if (totalBytes > 0) {
					rcp.fetchedFiles.emplace_back(localFile, range, totalBytes);
				} else {
					rcp.fetchedFiles.emplace_back(emptySstFilePath, range, totalBytes);
				}
				metaData->setSerializedCheckpoint(ObjectWriter::toValue(rcp, IncludeVersion()));
				if (!fileExists(localFile)) {
					TraceEvent(SevWarn, "FetchCheckpointRangeEndFileNotFound", metaData->checkpointID)
					    .detail("Range", range)
					    .detail("CheckpointID", metaData->checkpointID)
					    .detail("TargetStorageServerUID", ssID)
					    .detail("LocalFile", localFile)
					    .detail("Attempt", attempt)
					    .detail("TotalKeys", totalKeys)
					    .detail("TotalBytes", totalBytes);
				} else {
					TraceEvent(SevInfo, "FetchCheckpointRangeEnd", metaData->checkpointID)
					    .detail("Range", range)
					    .detail("CheckpointID", metaData->checkpointID)
					    .detail("TargetStorageServerUID", ssID)
					    .detail("LocalFile", localFile)
					    .detail("Attempt", attempt)
					    .detail("TotalKeys", totalKeys)
					    .detail("TotalBytes", totalBytes);
					break;
				}
			}
		}
	}

	if (error.present()) {
		throw error.get();
	}

	return Void();
}

ACTOR Future<Void> fetchCheckpointRanges(Database cx,
                                         std::shared_ptr<CheckpointMetaData> metaData,
                                         std::string dir,
                                         std::function<Future<Void>(CheckpointMetaData const&)> cFun) {
	RocksDBCheckpointKeyValues rkv = getRocksKeyValuesCheckpoint(*metaData);
	TraceEvent("FetchCheckpointRanges")
	    .detail("InitialState", metaData->toString())
	    .detail("RocksCheckpointKeyValues", rkv.toString());

	KeyRangeMap<CheckpointFile> fileMap;
	for (auto const& file : rkv.fetchedFiles) {
		fileMap.insert(file.range, file);
	}

	std::vector<Future<Void>> fs;
	for (auto const& range : rkv.ranges) {
		auto ranges = fileMap.intersectingRanges(range);
		for (auto r = ranges.begin(); r != ranges.end(); ++r) {
			CheckpointFile& file = r->value();
			KeyRangeRef currentRange = range & r->range();
			if (!file.isValid()) {
				std::shared_ptr<rocksdb::SstFileWriter> writer =
				    std::make_shared<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), rocksdb::Options());
				fs.push_back(fetchCheckpointRange(cx, metaData, currentRange, dir, writer, cFun));
			}
		}
	}
	wait(waitForAll(fs));
	if (cFun) {
		wait(cFun(*metaData));
	}

	return Void();
}
} // namespace

ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(CheckpointMetaData const&)> cFun) {
	TraceEvent(SevInfo, "FetchRocksCheckpointBegin")
	    .detail("InitialState", initialState.toString())
	    .detail("CheckpointDir", dir);

	ASSERT(!initialState.ranges.empty());

	state std::shared_ptr<CheckpointMetaData> metaData = std::make_shared<CheckpointMetaData>(initialState);
	state std::vector<Future<Void>> futures;

	if (metaData->getFormat() == DataMoveRocksCF) {
		state RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(initialState);
		TraceEvent(SevDebug, "RocksDBCheckpointMetaData").detail("RocksCF", rocksCF.toString());

		state int i = 0;
		for (; i < rocksCF.sstFiles.size(); ++i) {
			futures.push_back(fetchCheckpointFile(cx, metaData, i, dir, cFun));
			TraceEvent(SevDebug, "GetCheckpointFetchingFile")
			    .detail("FileName", rocksCF.sstFiles[i].name)
			    .detail("Server", describe(metaData->src));
		}
		if (metaData->bytesSampleFile.present()) {
			futures.push_back(fetchCheckpointBytesSampleFile(cx, metaData, dir, cFun));
		}
		wait(waitForAll(futures));
	} else if (metaData->getFormat() == RocksDBKeyValues) {
		futures.push_back(fetchCheckpointRanges(cx, metaData, dir, cFun));
		if (metaData->bytesSampleFile.present()) {
			futures.push_back(fetchCheckpointBytesSampleFile(cx, metaData, dir, cFun));
		}
		wait(waitForAll(futures));
	} else if (metaData->getFormat() == RocksDB) {
		throw not_implemented();
	}

	return *metaData;
}

ACTOR Future<Void> deleteRocksCheckpoint(CheckpointMetaData checkpoint) {
	TraceEvent(SevInfo, "DeleteRocksCheckpointBegin", checkpoint.checkpointID)
	    .detail("Checkpoint", checkpoint.toString());
	if (checkpoint.getState() == CheckpointMetaData::Fail) {
		return Void();
	}

	state CheckpointFormat format = checkpoint.getFormat();
	state std::unordered_set<std::string> dirs;
	if (format == DataMoveRocksCF) {
		RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(checkpoint);
		TraceEvent(SevInfo, "DeleteRocksColumnFamilyCheckpoint", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("RocksCF", rocksCF.toString());

		for (LiveFileMetaData const& file : rocksCF.sstFiles) {
			dirs.insert(file.db_path);
		}
	} else if (format == RocksDB) {
		RocksDBCheckpoint rocksCheckpoint = getRocksCheckpoint(checkpoint);
		TraceEvent(SevInfo, "DeleteRocksCheckpoint", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("RocksCheckpoint", rocksCheckpoint.toString());
		dirs.insert(rocksCheckpoint.checkpointDir);
	} else if (format == RocksDBKeyValues) {
		RocksDBCheckpointKeyValues rocksKv = getRocksKeyValuesCheckpoint(checkpoint);
		TraceEvent(SevInfo, "DeleteRocksKeyValuesCheckpoint", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("RocksCF", rocksKv.toString());

		for (CheckpointFile const& file : rocksKv.fetchedFiles) {
			if (file.path != emptySstFilePath) {
				dirs.insert(file.path);
			}
		}
	} else {
		ASSERT(false);
	}

	state std::unordered_set<std::string>::iterator it = dirs.begin();
	for (; it != dirs.end(); ++it) {
		std::string const dir = *it;
		platform::eraseDirectoryRecursive(dir);
		TraceEvent(SevInfo, "DeleteCheckpointRemovedDir", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("Dir", dir);
		wait(delay(0, TaskPriority::FetchKeys));
	}

	return Void();
}
#else
ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(CheckpointMetaData const&)> cFun) {
	wait(delay(0));
	return initialState;
}

ACTOR Future<Void> deleteRocksCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0));
	return Void();
}
#endif // WITH_ROCKSDB

int64_t getTotalFetchedBytes(std::vector<CheckpointMetaData> const& checkpoints) {
	int64_t totalBytes = 0;
	for (auto const& checkpoint : checkpoints) {
		CheckpointFormat const format = checkpoint.getFormat();
		if (format == DataMoveRocksCF) {
			RocksDBColumnFamilyCheckpoint const rocksCF = getRocksCF(checkpoint);
			for (auto const& file : rocksCF.sstFiles) {
				totalBytes += file.size;
			}
		} else if (format == RocksDB) {
			auto rcp = getRocksCheckpoint(checkpoint);
			for (auto const& file : rcp.fetchedFiles) {
				totalBytes += file.size;
			}
		} else if (format == RocksDBKeyValues) {
			RocksDBCheckpointKeyValues const rkv = getRocksKeyValuesCheckpoint(checkpoint);
			for (auto const& file : rkv.fetchedFiles) {
				totalBytes += file.size;
			}
		}
	}
	return totalBytes;
}

ICheckpointReader* newRocksDBCheckpointReader(CheckpointMetaData const& checkpoint,
                                              CheckpointAsKeyValues const checkpointAsKeyValues,
                                              UID logId) {
#ifdef WITH_ROCKSDB
	CheckpointFormat const format = checkpoint.getFormat();
	if (format == DataMoveRocksCF && !checkpointAsKeyValues) {
		return new RocksDBCFCheckpointReader(checkpoint, logId);
	} else {
		return new RocksDBColumnFamilyReader(checkpoint, logId);
	}
#endif // WITH_ROCKSDB
	return nullptr;
}

std::unique_ptr<IRocksDBSstFileWriter> newRocksDBSstFileWriter() {
#ifdef WITH_ROCKSDB
	std::unique_ptr<IRocksDBSstFileWriter> sstWriter = std::make_unique<RocksDBSstFileWriter>();
	return sstWriter;
#endif // WITH_ROCKSDB
	return nullptr;
}

std::unique_ptr<IRocksDBSstFileReader> newRocksDBSstFileReader() {
#ifdef WITH_ROCKSDB
	std::unique_ptr<IRocksDBSstFileReader> sstReader = std::make_unique<RocksDBSstFileReader>();
	return sstReader;
#endif // WITH_ROCKSDB
	return nullptr;
}

std::unique_ptr<IRocksDBSstFileReader> newRocksDBSstFileReader(KeyRange const& range,
                                                               size_t rowLimit,
                                                               size_t byteLimit) {
#ifdef WITH_ROCKSDB
	std::unique_ptr<IRocksDBSstFileReader> sstReader =
	    std::make_unique<RocksDBSstFileReader>(range, rowLimit, byteLimit);
	return sstReader;
#endif // WITH_ROCKSDB
	return nullptr;
}

std::unique_ptr<ICheckpointByteSampleReader> newCheckpointByteSampleReader(CheckpointMetaData const& checkpoint) {
#ifdef WITH_ROCKSDB
	return std::make_unique<RocksDBCheckpointByteSampleReader>(checkpoint);
#endif // WITH_ROCKSDB
	return nullptr;
}

RocksDBColumnFamilyCheckpoint getRocksCF(CheckpointMetaData const& checkpoint) {
	RocksDBColumnFamilyCheckpoint rocksCF;
	ObjectReader reader(checkpoint.getSerializedCheckpoint().begin(), IncludeVersion());
	reader.deserialize(rocksCF);
	return rocksCF;
}

RocksDBCheckpoint getRocksCheckpoint(CheckpointMetaData const& checkpoint) {
	RocksDBCheckpoint rocksCheckpoint;
	ObjectReader reader(checkpoint.getSerializedCheckpoint().begin(), IncludeVersion());
	reader.deserialize(rocksCheckpoint);
	return rocksCheckpoint;
}

RocksDBCheckpointKeyValues getRocksKeyValuesCheckpoint(CheckpointMetaData const& checkpoint) {
	RocksDBCheckpointKeyValues rocksCheckpoint;
	ObjectReader reader(checkpoint.getSerializedCheckpoint().begin(), IncludeVersion());
	reader.deserialize(rocksCheckpoint);
	return rocksCheckpoint;
}