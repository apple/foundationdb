#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageCheckpoint.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/ServerCheckpoint.actor.h"
#include "flow/Trace.h"
#include "flow/flow.h"

#include <memory>
#include <tuple>
#include <vector>

#include "flow/actorcompiler.h" // has to be last include

namespace {
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

	state std::string remoteFile = rocksCF.sstFiles[idx].db_path + rocksCF.sstFiles[idx].name;
	state std::string localFile = dir + rocksCF.sstFiles[idx].name;
	state UID ssID = metaData->ssID;

	state Transaction tr(cx);
	state StorageServerInterface ssi;
	loop {
		try {
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

			state ReplyPromiseStream<GetCheckpointFileReply> stream =
			    ssi.getCheckpointFile.getReplyStream(GetCheckpointFileRequest(metaData->checkpointID, remoteFile, 0));
			TraceEvent("FetchCheckpointFileReceivingData")
			    .detail("RemoteFile", remoteFile)
			    .detail("TargetUID", ssID.toString())
			    .detail("StorageServer", ssi.id().toString())
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt);
			loop {
				state GetCheckpointFileReply rep = waitNext(stream.getFuture());
				wait(asyncFile->write(rep.data.begin(), rep.size, offset));
				wait(asyncFile->flush());
				offset += rep.data.size();
			}
		} catch (Error& e) {
			if (e.code() != error_code_end_of_stream) {
				TraceEvent("FetchCheckpointFileError")
				    .detail("RemoteFile", remoteFile)
				    .detail("StorageServer", ssi.toString())
				    .detail("LocalFile", localFile)
				    .detail("Attempt", attempt)
				    .error(e, true);
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

class RocksDBCheckpointReader : public ICheckpointReader {
public:
	RocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID) : checkpoint_(checkpoint), id_(logID) {}

	Future<Void> init() override;

	Future<RangeResult> nextKeyValues(const int rowLimit, const int ByteLimit) override { throw not_implemented(); }

	// Returns the next chunk of serialized checkpoint.
	Future<Standalone<StringRef>> nextChunk(const int ByteLimit) override;

	Future<Void> getError() const override { return Never(); }
	Future<Void> onClosed() const override { return Void(); }

	void dispose() override {}
	void close() override;

private:
	CheckpointMetaData checkpoint_;
	UID id_;
};

Future<Void>
RocksDBCheckpointReader::init() {
	return Void();
}

Future<Standalone<StringRef>> RocksDBCheckpointReader::nextChunk(const int ByteLimit) {
	Standalone<StringRef> result;
	return result;
}

void RocksDBCheckpointReader::close() {}
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
		state RocksDBColumnFamilyCheckpoint rocksCF;
		ObjectReader reader(initialState.serializedCheckpoint.begin(), IncludeVersion());
		reader.deserialize(rocksCF);

		state int i = 0;
		TraceEvent("GetCheckpointMetaData").detail("Checkpoint", metaData->toString());
		for (; i < rocksCF.sstFiles.size(); ++i) {
			TraceEvent("GetCheckpointFetchingFile")
			    .detail("FileName", rocksCF.sstFiles[i].name)
			    .detail("Server", metaData->ssID.toString());
			wait(fetchCheckpointFile(cx, metaData, i, dir, cFun));
		}
	} else {
		throw not_implemented();
	}

	return *metaData;
}

ACTOR Future<Void> deleteRocksCFCheckpoint(CheckpointMetaData checkpoint) {
	ASSERT_EQ(checkpoint.getFormat(), RocksDBColumnFamily);
	ObjectReader reader(checkpoint.serializedCheckpoint.begin(), IncludeVersion());
	RocksDBColumnFamilyCheckpoint rocksCF;
	reader.deserialize(rocksCF);

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