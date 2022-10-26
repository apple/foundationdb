/*
 *RocksDBCheckpointUtils.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#ifdef SSD_ROCKSDB_EXPERIMENTAL
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/types.h>
#include <rocksdb/version.h>
#endif // SSD_ROCKSDB_EXPERIMENTAL

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageCheckpoint.h"
#include "fdbserver/CoroFlow.h"
#include "fdbserver/Knobs.h"
#include "flow/IThreadPool.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Trace.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

#ifdef SSD_ROCKSDB_EXPERIMENTAL
// Enforcing rocksdb version to be 6.22.1 or greater.
static_assert(ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 22 && ROCKSDB_PATCH >= 1,
              "Unsupported rocksdb version. Update the rocksdb to at least 6.22.1 version");

namespace {

using DB = rocksdb::DB*;
using CF = rocksdb::ColumnFamilyHandle*;

const KeyRef persistVersion = "\xff\xffVersion"_sr;

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

// RocksDBCheckpointReader reads a RocksDB checkpoint, and returns the key-value pairs via nextKeyValues.
class RocksDBCheckpointReader : public ICheckpointReader {
public:
	RocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID);

	Future<Void> init(StringRef token) override;

	Future<RangeResult> nextKeyValues(const int rowLimit, const int byteLimit) override;

	Future<Standalone<StringRef>> nextChunk(const int byteLimit) override { throw not_implemented(); }

	Future<Void> close() override { return doClose(this); }

private:
	struct Reader : IThreadPoolReceiver {
		struct OpenAction : TypedAction<Reader, OpenAction> {
			OpenAction(std::string path, KeyRange range, Version version)
			  : path(std::move(path)), range(range), version(version) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }

			const std::string path;
			const KeyRange range;
			const Version version;
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
			ReadRangeAction(int rowLimit, int byteLimit)
			  : rowLimit(rowLimit), byteLimit(byteLimit), startTime(timer_monotonic()) {}

			double getTimeEstimate() const override { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }

			const int rowLimit, byteLimit;
			const double startTime;
			ThreadReturnPromise<RangeResult> result;
		};

		explicit Reader(DB& db);
		~Reader() override {}

		void init() override {}

		void action(OpenAction& a);

		void action(CloseAction& a);

		void action(ReadRangeAction& a);

		DB& db;
		CF cf;
		Key begin;
		Key end;
		std::vector<rocksdb::ColumnFamilyHandle*> handles;
		double readRangeTimeout;
		std::unique_ptr<rocksdb::Iterator> cursor;
	};

	ACTOR static Future<Void> doClose(RocksDBCheckpointReader* self);

	DB db = nullptr;
	std::string path;
	const UID id;
	Version version;
	Reference<IThreadPool> readThreads;
	Future<Void> openFuture;
};

RocksDBCheckpointReader::RocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID)
  : id(logID), version(checkpoint.version) {
	RocksDBCheckpoint rocksCheckpoint = getRocksCheckpoint(checkpoint);
	this->path = rocksCheckpoint.checkpointDir;
	if (g_network->isSimulated()) {
		readThreads = CoroThreadPool::createThreadPool();
	} else {
		readThreads = createGenericThreadPool();
	}
	readThreads->addThread(new Reader(db), "fdb-rocks-rd");
}

Future<Void> RocksDBCheckpointReader::init(StringRef token) {
	if (openFuture.isValid()) {
		return openFuture;
	}

	KeyRange range = BinaryReader::fromStringRef<KeyRange>(token, IncludeVersion());
	auto a = std::make_unique<Reader::OpenAction>(this->path, range, this->version);
	openFuture = a->done.getFuture();
	readThreads->post(a.release());
	return openFuture;
}

Future<RangeResult> RocksDBCheckpointReader::nextKeyValues(const int rowLimit, const int byteLimit) {
	auto a = std::make_unique<Reader::ReadRangeAction>(rowLimit, byteLimit);
	auto res = a->result.getFuture();
	readThreads->post(a.release());
	return res;
}

RocksDBCheckpointReader::Reader::Reader(DB& db) : db(db), cf(nullptr) {
	if (g_network->isSimulated()) {
		// In simulation, increasing the read operation timeouts to 5 minutes, as some of the tests have
		// very high load and single read thread cannot process all the load within the timeouts.
		readRangeTimeout = 5 * 60;
	} else {
		readRangeTimeout = SERVER_KNOBS->ROCKSDB_READ_RANGE_TIMEOUT;
	}
}

void RocksDBCheckpointReader::Reader::action(RocksDBCheckpointReader::Reader::OpenAction& a) {
	ASSERT(cf == nullptr);

	std::vector<std::string> columnFamilies;
	rocksdb::Options options = getOptions();
	rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, a.path, &columnFamilies);
	if (std::find(columnFamilies.begin(), columnFamilies.end(), "default") == columnFamilies.end()) {
		columnFamilies.push_back("default");
	}

	rocksdb::ColumnFamilyOptions cfOptions = getCFOptions();
	std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
	for (const std::string& name : columnFamilies) {
		descriptors.push_back(rocksdb::ColumnFamilyDescriptor{ name, cfOptions });
	}

	status = rocksdb::DB::OpenForReadOnly(options, a.path, descriptors, &handles, &db);

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

	ASSERT(db != nullptr && cf != nullptr);

	begin = a.range.begin;
	end = a.range.end;

	TraceEvent(SevInfo, "RocksDBCheckpointReaderInit")
	    .detail("Path", a.path)
	    .detail("Method", "OpenForReadOnly")
	    .detail("ColumnFamily", cf->GetName())
	    .detail("Begin", begin)
	    .detail("End", end);

	rocksdb::PinnableSlice value;
	rocksdb::ReadOptions readOptions = getReadOptions();
	status = db->Get(readOptions, cf, toSlice(persistVersion), &value);

	if (!status.ok() && !status.IsNotFound()) {
		logRocksDBError(status, "Checkpoint");
		a.done.sendError(statusToError(status));
		return;
	}

	const Version version =
	    status.IsNotFound() ? latestVersion : BinaryReader::fromStringRef<Version>(toStringRef(value), Unversioned());

	ASSERT(version == a.version);

	cursor = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(readOptions, cf));
	cursor->Seek(toSlice(begin));

	a.done.send(Void());
}

void RocksDBCheckpointReader::Reader::action(RocksDBCheckpointReader::Reader::CloseAction& a) {
	if (db == nullptr) {
		a.done.send(Void());
		return;
	}

	for (rocksdb::ColumnFamilyHandle* handle : handles) {
		if (handle != nullptr) {
			TraceEvent("RocksDBCheckpointReaderDestroyCF").detail("Path", a.path).detail("CF", handle->GetName());
			db->DestroyColumnFamilyHandle(handle);
		}
	}
	handles.clear();

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

void RocksDBCheckpointReader::Reader::action(RocksDBCheckpointReader::Reader::ReadRangeAction& a) {
	const double readBeginTime = timer_monotonic();

	if (readBeginTime - a.startTime > readRangeTimeout) {
		TraceEvent(SevWarn, "RocksDBCheckpointReaderError")
		    .detail("Error", "Read range request timedout")
		    .detail("Method", "ReadRangeAction")
		    .detail("Timeout value", readRangeTimeout);
		a.result.sendError(timed_out());
		return;
	}

	RangeResult result;
	if (a.rowLimit == 0 || a.byteLimit == 0) {
		a.result.send(result);
		return;
	}

	// For now, only forward scan is supported.
	ASSERT(a.rowLimit > 0);

	int accumulatedBytes = 0;
	rocksdb::Status s;
	while (cursor->Valid() && toStringRef(cursor->key()) < end) {
		KeyValueRef kv(toStringRef(cursor->key()), toStringRef(cursor->value()));
		accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
		result.push_back_deep(result.arena(), kv);
		cursor->Next();
		if (result.size() >= a.rowLimit || accumulatedBytes >= a.byteLimit) {
			break;
		}
		if (timer_monotonic() - a.startTime > readRangeTimeout) {
			TraceEvent(SevWarn, "RocksDBCheckpointReaderError")
			    .detail("Error", "Read range request timedout")
			    .detail("Method", "ReadRangeAction")
			    .detail("Timeout value", readRangeTimeout);
			a.result.sendError(transaction_too_old());
			delete (cursor.release());
			return;
		}
	}

	s = cursor->status();

	if (!s.ok()) {
		logRocksDBError(s, "ReadRange");
		a.result.sendError(statusToError(s));
		delete (cursor.release());
		return;
	}

	if (result.empty()) {
		delete (cursor.release());
		a.result.sendError(end_of_stream());
	} else {
		a.result.send(result);
	}
}

ACTOR Future<Void> RocksDBCheckpointReader::doClose(RocksDBCheckpointReader* self) {
	if (self == nullptr)
		return Void();

	auto a = new RocksDBCheckpointReader::Reader::CloseAction(self->path, false);
	auto f = a->done.getFuture();
	self->readThreads->post(a);
	wait(f);

	if (self != nullptr) {
		wait(self->readThreads->stop());
	}

	if (self != nullptr) {
		if (self->db != nullptr) {
			delete self->db;
		}
		delete self;
	}

	return Void();
}

// RocksDBCFCheckpointReader reads an exported RocksDB Column Family checkpoint, and returns the serialized
// checkpoint via nextChunk.
class RocksDBCFCheckpointReader : public ICheckpointReader {
public:
	RocksDBCFCheckpointReader(const CheckpointMetaData& checkpoint, UID logID)
	  : checkpoint_(checkpoint), id_(logID), file_(Reference<IAsyncFile>()), offset_(0) {}

	Future<Void> init(StringRef token) override;

	Future<RangeResult> nextKeyValues(const int rowLimit, const int byteLimit) override { throw not_implemented(); }

	Future<Standalone<StringRef>> nextChunk(const int byteLimit) override;

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
			TraceEvent(SevWarnAlways, "ServerGetCheckpointFileFailure")
			    .errorUnsuppressed(e)
			    .detail("File", self->path_);
			throw e;
		}

		return Void();
	}

	ACTOR static Future<Standalone<StringRef>> getNextChunk(RocksDBCFCheckpointReader* self, int byteLimit) {
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
	ASSERT_EQ(this->checkpoint_.getFormat(), RocksDBColumnFamily);
	const std::string name = token.toString();
	this->offset_ = 0;
	this->path_.clear();
	const RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(this->checkpoint_);
	for (const auto& sstFile : rocksCF.sstFiles) {
		if (sstFile.name == name) {
			this->path_ = sstFile.db_path + sstFile.name;
			break;
		}
	}

	if (this->path_.empty()) {
		TraceEvent("RocksDBCheckpointReaderInitFileNotFound").detail("File", this->path_);
		return checkpoint_not_found();
	}

	return doInit(this);
}

Future<Standalone<StringRef>> RocksDBCFCheckpointReader::nextChunk(const int byteLimit) {
	return getNextChunk(this, byteLimit);
}

Future<Void> RocksDBCFCheckpointReader::close() {
	return doClose(this);
}

// Fetch a single sst file from storage server. If the file is fetch successfully, it will be recorded via cFun.
ACTOR Future<Void> fetchCheckpointFile(Database cx,
                                       std::shared_ptr<CheckpointMetaData> metaData,
                                       int idx,
                                       std::string dir,
                                       std::function<Future<Void>(const CheckpointMetaData&)> cFun,
                                       int maxRetries = 3) {
	state RocksDBColumnFamilyCheckpoint rocksCF;
	ObjectReader reader(metaData->serializedCheckpoint.begin(), IncludeVersion());
	reader.deserialize(rocksCF);

	// Skip fetched file.
	if (rocksCF.sstFiles[idx].fetched && rocksCF.sstFiles[idx].db_path == dir) {
		return Void();
	}

	state std::string remoteFile = rocksCF.sstFiles[idx].name;
	state std::string localFile = dir + rocksCF.sstFiles[idx].name;
	state UID ssID = metaData->ssID;

	state Transaction tr(cx);
	state StorageServerInterface ssi;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> ss = wait(tr.get(serverListKeyFor(ssID)));
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
		try {
			asyncFile = Reference<IAsyncFile>();
			++attempt;
			TraceEvent("FetchCheckpointFileBegin")
			    .detail("RemoteFile", remoteFile)
			    .detail("TargetUID", ssID.toString())
			    .detail("StorageServer", ssi.id().toString())
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt);

			wait(IAsyncFileSystem::filesystem()->deleteFile(localFile, true));
			const int64_t flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE |
			                      IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
			wait(store(asyncFile, IAsyncFileSystem::filesystem()->open(localFile, flags, 0666)));

			state ReplyPromiseStream<FetchCheckpointReply> stream =
			    ssi.fetchCheckpoint.getReplyStream(FetchCheckpointRequest(metaData->checkpointID, remoteFile));
			TraceEvent("FetchCheckpointFileReceivingData")
			    .detail("RemoteFile", remoteFile)
			    .detail("TargetUID", ssID.toString())
			    .detail("StorageServer", ssi.id().toString())
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt);
			loop {
				state FetchCheckpointReply rep = waitNext(stream.getFuture());
				wait(asyncFile->write(rep.data.begin(), rep.data.size(), offset));
				wait(asyncFile->flush());
				offset += rep.data.size();
			}
		} catch (Error& e) {
			if (e.code() != error_code_end_of_stream) {
				TraceEvent("FetchCheckpointFileError")
				    .errorUnsuppressed(e)
				    .detail("RemoteFile", remoteFile)
				    .detail("StorageServer", ssi.toString())
				    .detail("LocalFile", localFile)
				    .detail("Attempt", attempt);
				if (attempt >= maxRetries) {
					throw e;
				}
			} else {
				wait(asyncFile->sync());
				int64_t fileSize = wait(asyncFile->size());
				TraceEvent("FetchCheckpointFileEnd")
				    .detail("RemoteFile", remoteFile)
				    .detail("StorageServer", ssi.toString())
				    .detail("LocalFile", localFile)
				    .detail("Attempt", attempt)
				    .detail("DataSize", offset)
				    .detail("FileSize", fileSize);
				rocksCF.sstFiles[idx].db_path = dir;
				rocksCF.sstFiles[idx].fetched = true;
				metaData->serializedCheckpoint = ObjectWriter::toValue(rocksCF, IncludeVersion());
				if (cFun) {
					wait(cFun(*metaData));
				}
				return Void();
			}
		}
	}
}

// TODO: Return when a file exceeds a limit.
ACTOR Future<Void> fetchCheckpointRange(Database cx,
                                        std::shared_ptr<CheckpointMetaData> metaData,
                                        KeyRange range,
                                        std::string dir,
                                        std::shared_ptr<rocksdb::SstFileWriter> writer,
                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun,
                                        int maxRetries = 3) {
	state std::string localFile = dir + "/" + metaData->checkpointID.toString() + ".sst";
	RocksDBCheckpoint rcp = getRocksCheckpoint(*metaData);
	TraceEvent("FetchCheckpointRange")
	    .detail("InitialState", metaData->toString())
	    .detail("RocksCheckpoint", rcp.toString());

	for (const auto& file : rcp.fetchedFiles) {
		ASSERT(!file.range.intersects(range));
	}

	state UID ssID = metaData->ssID;
	state Transaction tr(cx);
	state StorageServerInterface ssi;
	loop {
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			Optional<Value> ss = wait(tr.get(serverListKeyFor(ssID)));
			if (!ss.present()) {
				TraceEvent(SevWarnAlways, "FetchCheckpointRangeStorageServerNotFound")
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
	state rocksdb::Status status;
	state Optional<Error> error;
	loop {
		totalBytes = 0;
		++attempt;
		try {
			TraceEvent(SevInfo, "FetchCheckpointRangeBegin")
			    .detail("CheckpointID", metaData->checkpointID)
			    .detail("Range", range.toString())
			    .detail("TargetStorageServerUID", ssID)
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt)
			    .log();

			wait(IAsyncFileSystem::filesystem()->deleteFile(localFile, true));
			status = writer->Open(localFile);
			if (!status.ok()) {
				Error e = statusToError(status);
				TraceEvent(SevError, "FetchCheckpointRangeOpenFileError")
				    .detail("LocalFile", localFile)
				    .detail("Status", status.ToString());
				throw e;
			}

			state ReplyPromiseStream<FetchCheckpointKeyValuesStreamReply> stream =
			    ssi.fetchCheckpointKeyValues.getReplyStream(
			        FetchCheckpointKeyValuesRequest(metaData->checkpointID, range));
			TraceEvent(SevDebug, "FetchCheckpointKeyValuesReceivingData")
			    .detail("CheckpointID", metaData->checkpointID)
			    .detail("Range", range.toString())
			    .detail("TargetStorageServerUID", ssID.toString())
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt)
			    .log();

			loop {
				FetchCheckpointKeyValuesStreamReply rep = waitNext(stream.getFuture());
				for (int i = 0; i < rep.data.size(); ++i) {
					status = writer->Put(toSlice(rep.data[i].key), toSlice(rep.data[i].value));
					if (!status.ok()) {
						Error e = statusToError(status);
						TraceEvent(SevError, "FetchCheckpointRangeWriteError")
						    .detail("LocalFile", localFile)
						    .detail("Key", rep.data[i].key.toString())
						    .detail("Value", rep.data[i].value.toString())
						    .detail("Status", status.ToString());
						throw e;
					}
					totalBytes += rep.data[i].expectedSize();
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
				TraceEvent(SevWarn, "FetchCheckpointFileError")
				    .errorUnsuppressed(err)
				    .detail("CheckpointID", metaData->checkpointID)
				    .detail("Range", range.toString())
				    .detail("TargetStorageServerUID", ssID.toString())
				    .detail("LocalFile", localFile)
				    .detail("Attempt", attempt);
				if (attempt >= maxRetries) {
					error = err;
					break;
				}
			} else {
				if (totalBytes > 0) {
					RocksDBCheckpoint rcp = getRocksCheckpoint(*metaData);
					rcp.fetchedFiles.emplace_back(localFile, range, totalBytes);
					rcp.checkpointDir = dir;
					metaData->serializedCheckpoint = ObjectWriter::toValue(rcp, IncludeVersion());
				}
				if (!fileExists(localFile)) {
					TraceEvent(SevWarn, "FetchCheckpointRangeEndFileNotFound")
					    .detail("CheckpointID", metaData->checkpointID)
					    .detail("Range", range.toString())
					    .detail("TargetStorageServerUID", ssID.toString())
					    .detail("LocalFile", localFile)
					    .detail("Attempt", attempt)
					    .detail("TotalBytes", totalBytes);
				} else {
					TraceEvent(SevInfo, "FetchCheckpointRangeEnd")
					    .detail("CheckpointID", metaData->checkpointID)
					    .detail("Range", range.toString())
					    .detail("TargetStorageServerUID", ssID.toString())
					    .detail("LocalFile", localFile)
					    .detail("Attempt", attempt)
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

} // namespace

ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	TraceEvent(SevInfo, "FetchRocksCheckpointBegin")
	    .detail("InitialState", initialState.toString())
	    .detail("CheckpointDir", dir);

	ASSERT(!initialState.ranges.empty());

	state std::shared_ptr<CheckpointMetaData> metaData = std::make_shared<CheckpointMetaData>(initialState);

	if (metaData->format == RocksDBColumnFamily) {
		state RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(initialState);
		TraceEvent(SevDebug, "RocksDBCheckpointMetaData").detail("RocksCF", rocksCF.toString());

		state int i = 0;
		state std::vector<Future<Void>> fs;
		for (; i < rocksCF.sstFiles.size(); ++i) {
			fs.push_back(fetchCheckpointFile(cx, metaData, i, dir, cFun));
			TraceEvent(SevDebug, "GetCheckpointFetchingFile")
			    .detail("FileName", rocksCF.sstFiles[i].name)
			    .detail("Server", metaData->ssID.toString());
		}
		wait(waitForAll(fs));
	} else if (metaData->format == RocksDB) {
		std::shared_ptr<rocksdb::SstFileWriter> writer =
		    std::make_shared<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), rocksdb::Options());
		wait(fetchCheckpointRange(cx, metaData, metaData->ranges.front(), dir, writer, cFun));
	}

	return *metaData;
}

ACTOR Future<Void> deleteRocksCheckpoint(CheckpointMetaData checkpoint) {
	state CheckpointFormat format = checkpoint.getFormat();
	state std::unordered_set<std::string> dirs;
	if (format == RocksDBColumnFamily) {
		RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(checkpoint);
		TraceEvent(SevInfo, "DeleteRocksColumnFamilyCheckpoint", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("RocksCF", rocksCF.toString());

		for (const LiveFileMetaData& file : rocksCF.sstFiles) {
			dirs.insert(file.db_path);
		}
	} else if (format == RocksDB) {
		RocksDBCheckpoint rocksCheckpoint = getRocksCheckpoint(checkpoint);
		TraceEvent(SevInfo, "DeleteRocksCheckpoint", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("RocksCheckpoint", rocksCheckpoint.toString());
		dirs.insert(rocksCheckpoint.checkpointDir);
	} else {
		ASSERT(false);
	}

	state std::unordered_set<std::string>::iterator it = dirs.begin();
	for (; it != dirs.end(); ++it) {
		const std::string dir = *it;
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
                                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	wait(delay(0));
	return initialState;
}

ACTOR Future<Void> deleteRocksCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0));
	return Void();
}
#endif // SSD_ROCKSDB_EXPERIMENTAL

int64_t getTotalFetchedBytes(const std::vector<CheckpointMetaData>& checkpoints) {
	int64_t totalBytes = 0;
	for (const auto& checkpoint : checkpoints) {
		const CheckpointFormat format = checkpoint.getFormat();
		if (format == RocksDBColumnFamily) {
			// TODO: Returns the checkpoint size of a RocksDB Column Family.
		} else if (format == RocksDB) {
			auto rcp = getRocksCheckpoint(checkpoint);
			for (const auto& file : rcp.fetchedFiles) {
				totalBytes += file.size;
			}
		}
	}
	return totalBytes;
}

ICheckpointReader* newRocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID) {
#ifdef SSD_ROCKSDB_EXPERIMENTAL
	const CheckpointFormat format = checkpoint.getFormat();
	if (format == RocksDBColumnFamily) {
		return new RocksDBCFCheckpointReader(checkpoint, logID);
	} else if (format == RocksDB) {
		return new RocksDBCheckpointReader(checkpoint, logID);
	}
#endif // SSD_ROCKSDB_EXPERIMENTAL
	return nullptr;
}

RocksDBColumnFamilyCheckpoint getRocksCF(const CheckpointMetaData& checkpoint) {
	RocksDBColumnFamilyCheckpoint rocksCF;
	ObjectReader reader(checkpoint.serializedCheckpoint.begin(), IncludeVersion());
	reader.deserialize(rocksCF);
	return rocksCF;
}

RocksDBCheckpoint getRocksCheckpoint(const CheckpointMetaData& checkpoint) {
	RocksDBCheckpoint rocksCheckpoint;
	ObjectReader reader(checkpoint.serializedCheckpoint.begin(), IncludeVersion());
	reader.deserialize(rocksCheckpoint);
	return rocksCheckpoint;
}