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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageCheckpoint.h"
#include "flow/Trace.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

namespace {

class RocksDBCheckpointReader : public ICheckpointReader {
public:
	RocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID)
	  : checkpoint_(checkpoint), id_(logID), file_(Reference<IAsyncFile>()), offset_(0) {}

	Future<Void> init(StringRef token) override;

	Future<RangeResult> nextKeyValues(const int rowLimit, const int byteLimit) override { throw not_implemented(); }

	// Returns the next chunk of serialized checkpoint.
	Future<Standalone<StringRef>> nextChunk(const int byteLimit) override;

	Future<Void> close() override;

private:
	ACTOR static Future<Void> doInit(RocksDBCheckpointReader* self) {
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

	ACTOR static Future<Standalone<StringRef>> getNextChunk(RocksDBCheckpointReader* self, int byteLimit) {
		int blockSize = std::min(64 * 1024, byteLimit); // Block size read from disk.
		state Standalone<StringRef> buf = makeAlignedString(_PAGE_SIZE, blockSize);
		int bytesRead = wait(self->file_->read(mutateString(buf), blockSize, self->offset_));
		if (bytesRead == 0) {
			throw end_of_stream();
		}

		self->offset_ += bytesRead;
		return buf.substr(0, bytesRead);
	}

	ACTOR static Future<Void> doClose(RocksDBCheckpointReader* self) {
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

Future<Void> RocksDBCheckpointReader::init(StringRef token) {
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

Future<Standalone<StringRef>> RocksDBCheckpointReader::nextChunk(const int byteLimit) {
	return getNextChunk(this, byteLimit);
}

Future<Void> RocksDBCheckpointReader::close() {
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
	loop {
		try {
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
			state int64_t offset = 0;
			state Reference<IAsyncFile> asyncFile = wait(IAsyncFileSystem::filesystem()->open(localFile, flags, 0666));

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

} // namespace

ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	TraceEvent("FetchRocksCheckpointBegin")
	    .detail("InitialState", initialState.toString())
	    .detail("CheckpointDir", dir);

	state std::shared_ptr<CheckpointMetaData> metaData = std::make_shared<CheckpointMetaData>(initialState);

	if (metaData->format == RocksDBColumnFamily) {
		state RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(initialState);
		TraceEvent("RocksDBCheckpointMetaData").detail("RocksCF", rocksCF.toString());

		state int i = 0;
		state std::vector<Future<Void>> fs;
		for (; i < rocksCF.sstFiles.size(); ++i) {
			fs.push_back(fetchCheckpointFile(cx, metaData, i, dir, cFun));
			TraceEvent("GetCheckpointFetchingFile")
			    .detail("FileName", rocksCF.sstFiles[i].name)
			    .detail("Server", metaData->ssID.toString());
		}
		wait(waitForAll(fs));
	} else {
		throw not_implemented();
	}

	return *metaData;
}

ACTOR Future<Void> deleteRocksCFCheckpoint(CheckpointMetaData checkpoint) {
	ASSERT_EQ(checkpoint.getFormat(), RocksDBColumnFamily);
	RocksDBColumnFamilyCheckpoint rocksCF = getRocksCF(checkpoint);
	TraceEvent("DeleteRocksColumnFamilyCheckpoint", checkpoint.checkpointID)
	    .detail("CheckpointID", checkpoint.checkpointID)
	    .detail("RocksCF", rocksCF.toString());

	state std::unordered_set<std::string> dirs;
	for (const LiveFileMetaData& file : rocksCF.sstFiles) {
		dirs.insert(file.db_path);
	}

	state std::unordered_set<std::string>::iterator it = dirs.begin();
	for (; it != dirs.end(); ++it) {
		const std::string dir = *it;
		platform::eraseDirectoryRecursive(dir);
		TraceEvent("DeleteCheckpointRemovedDir", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("Dir", dir);
		wait(delay(0, TaskPriority::FetchKeys));
	}
	return Void();
}

ICheckpointReader* newRocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID) {
	return new RocksDBCheckpointReader(checkpoint, logID);
}

RocksDBColumnFamilyCheckpoint getRocksCF(const CheckpointMetaData& checkpoint) {
	RocksDBColumnFamilyCheckpoint rocksCF;
	ObjectReader reader(checkpoint.serializedCheckpoint.begin(), IncludeVersion());
	reader.deserialize(rocksCF);
	return rocksCF;
}