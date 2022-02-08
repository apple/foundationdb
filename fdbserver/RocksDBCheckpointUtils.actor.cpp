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

// Fetch a keyrange from a checkpoint on a storage server.
// If the file is fetch successfully, it will be recorded via cFun.
ACTOR static Future<Void> fetchCheckpointRange(Database cx,
                                               std::shared_ptr<CheckpointMetaData> metaData,
                                               KeyRange range,
                                               std::string localFile,
                                               std::shared_ptr<rocksdb::SstFileWriter> writer,
                                               std::function<Future<Void>(const CheckpointMetaData&)> cFun,
                                               int maxRetries = 3) {
	ASSERT(metaData->rocksDBCheckpoint.present());
	TraceEvent("FetchCheckpointRange").detail("InitialState", metaData->toString());
	// Skip fetched file.
	// if (metaData->rocksCF.get().sstFiles[idx].fetched && metaData->rocksCF.get().sstFiles[idx].db_path == dir) {
	// 	return Void();
	// }

	// state std::string remoteFile =
	//     metaData->rocksCF.get().sstFiles[idx].db_path + metaData->rocksCF.get().sstFiles[idx].name;
	// state std::string localFile = dir + metaData->rocksCF.get().sstFiles[idx].name;
	state UID ssID = metaData->ssID;
	state std::string remoteDir = metaData->rocksDBCheckpoint.get().checkpointDir;

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

	std::cout << "FetchRocksCheckpointKeyValues found ss: " << ssi.toString() << std::endl;

	state int attempt = 0;
	state int64_t totalBytes = 0;
	// state rocksdb::SstFileWriter writer = rocksdb::SstFileWriter(rocksdb::EnvOptions(), rocksdb::Options());
	state rocksdb::Status status;
	loop {
		totalBytes = 0;
		try {
			++attempt;
			TraceEvent("FetchCheckpointRangeBegin")
			    .detail("RemoteDir", remoteDir)
			    .detail("TargetUID", ssID.toString())
			    .detail("StorageServer", ssi.id().toString())
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt)
			    .log();
			// status = writer.Finish();
			// if (!status.ok()) {
			// 	std::cout << "SstFileWriter close failure: " << status.ToString() << std::endl;
			// 	break;
			// }

			wait(IAsyncFileSystem::filesystem()->deleteFile(localFile, true));
			status = writer->Open(localFile);
			if (!status.ok()) {
				std::cout << "SstFileWriter open failure: " << status.ToString() << std::endl;
				break;
			}
			// const int64_t flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE |
			//                       IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
			// state Reference<IAsyncFile> asyncFile = wait(IAsyncFileSystem::filesystem()->open(localFile, flags,
			// 0666));

			state ReplyPromiseStream<GetCheckpointKeyValuesStreamReply> stream =
			    ssi.getCheckpointKeyValues.getReplyStream(GetCheckpointKeyValuesRequest(remoteDir, range));
			std::cout << "FetchRocksCheckpointKeyValues stream." << std::endl;
			TraceEvent("FetchCheckpointKeyValuesReceivingData")
			    .detail("RemoteDir", remoteDir)
			    .detail("TargetUID", ssID.toString())
			    .detail("StorageServer", ssi.id().toString())
			    .detail("LocalFile", localFile)
			    .detail("Attempt", attempt)
			    .log();
			loop {
				GetCheckpointKeyValuesStreamReply rep = waitNext(stream.getFuture());
				// wait(asyncFile->write(rep.data.begin(), rep.size, offset));
				// wait(asyncFile->flush());
				//  += rep.data.size();
				for (int i = 0; i < rep.data.size(); ++i) {
					std::cout << "Writing key: " << rep.data[i].key.toString()
					          << ", value: " << rep.data[i].value.toString() << std::endl;
					status = writer->Put(toSlice(rep.data[i].key), toSlice(rep.data[i].value));
					if (!status.ok()) {
						std::cout << "SstFileWriter put failure: " << status.ToString() << std::endl;
						break;
					}
				}
			}
		} catch (Error& e) {
			status = writer->Finish();
			if (!status.ok()) {
				std::cout << "SstFileWriter close failure: " << status.ToString() << std::endl;
			}
			if (e.code() != error_code_end_of_stream) {
				TraceEvent("FetchCheckpointFileError")
				    .detail("RemoteFile", remoteDir)
				    .detail("StorageServer", ssi.toString())
				    .detail("LocalFile", localFile)
				    .detail("Attempt", attempt)
				    .error(e, true);
				if (attempt >= maxRetries) {
					throw e;
				}
			} else {
                metaData->rocksDBCheckpoint.get().fetchedFiles.emplace_back(range, localFile);
				TraceEvent("FetchCheckpointFileEnd")
				    .detail("RemoteFile", remoteDir)
				    .detail("StorageServer", ssi.toString())
				    .detail("LocalFile", localFile)
				    .detail("Attempt", attempt)
				    .detail("TotalBytes", totalBytes);
				if (cFun) {
					wait(cFun(*metaData));
				}
				return Void();
			}
		}
	}

	rocksdb::Status s = writer->Finish();
    std::cout << "SstFileWriterFinish status: " << s.ToString() << std::endl;

	if (!status.ok()) {
		throw internal_error();
	}

	return Void();
}

ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	TraceEvent("FetchRocksCheckpointBegin")
	    .detail("InitialState", initialState.toString())
	    .detail("CheckpointDir", dir);

	state std::shared_ptr<CheckpointMetaData> metaData = std::make_shared<CheckpointMetaData>(initialState);

	if (metaData->format == SingleRocksDB) {
		if (!metaData->rocksDBCheckpoint.present()) {
			throw internal_error();
		}
		std::string localFile = dir + "/" + metaData->checkpointID.toString() + ".sst";
		std::shared_ptr<rocksdb::SstFileWriter> writer =
		    std::make_shared<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), rocksdb::Options());
		wait(fetchCheckpointRange(cx, metaData, metaData->range, localFile, writer, cFun));
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
