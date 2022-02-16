#ifdef SSD_ROCKSDB_EXPERIMENTAL

#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/table_properties_collectors.h>
#include <rocksdb/version.h>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "flow/flow.h"
#include "flow/Trace.h"
#include "flow/IRandom.h"

#include <memory>
#include <tuple>
#include <vector>

#endif // SSD_ROCKSDB_EXPERIMENTAL

#include "flow/actorcompiler.h" // has to be last include

#ifdef SSD_ROCKSDB_EXPERIMENTAL

// Enforcing rocksdb version to be 6.22.1 or greater.
static_assert(ROCKSDB_MAJOR >= 6, "Unsupported rocksdb version. Update the rocksdb to 6.22.1 version");
static_assert(ROCKSDB_MAJOR == 6 ? ROCKSDB_MINOR >= 22 : true,
              "Unsupported rocksdb version. Update the rocksdb to 6.22.1 version");
static_assert((ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR == 22) ? ROCKSDB_PATCH >= 1 : true,
              "Unsupported rocksdb version. Update the rocksdb to 6.22.1 version");

namespace {

rocksdb::Slice toSlice(StringRef s) {
	return rocksdb::Slice(reinterpret_cast<const char*>(s.begin()), s.size());
}

} // namespace

// Fetch a single sst file from storage server. If the file is fetch successfully, it will be recorded via cFun.
ACTOR static Future<Void> fetchCheckpointFile(Database cx,
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
#else

ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	wait(delay(0));
	std::cout << "RocksDB not enabled." << std::endl;
	ASSERT(false);
	return CheckpointMetaData();
}
#endif // SSD_ROCKSDB_EXPERIMENTAL