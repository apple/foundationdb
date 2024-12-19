/*
 * BulkLoadUtils.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include <fmt/format.h>
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<Optional<BulkLoadTaskState>> getBulkLoadTaskStateFromDataMove(Database cx, UID dataMoveId, UID logId) {
	loop {
		state Transaction tr(cx);
		try {
			Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
			if (!val.present()) {
				TraceEvent(SevWarn, "SSBulkLoadDataMoveIdNotExist", logId).detail("DataMoveID", dataMoveId);
				return Optional<BulkLoadTaskState>();
			}
			DataMoveMetaData dataMoveMetaData = decodeDataMoveValue(val.get());
			return dataMoveMetaData.bulkLoadTaskState;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

void bulkLoadFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	std::string content = readFileBytes(fromFile, fileBytesMax);
	writeFile(toFile, content);
	return;
}

ACTOR Future<SSBulkLoadFileSet> bulkLoadTransportCP_impl(std::string dir,
                                                         BulkLoadTaskState bulkLoadTaskState,
                                                         size_t fileBytesMax,
                                                         UID logId) {
	ASSERT(bulkLoadTaskState.getTransportMethod() == BulkLoadTransportMethod::CP);
	loop {
		state std::string fromDataFilePath;
		state std::string toDataFilePath;
		state SSBulkLoadFileSet fileSet; // TODO(BulkLoad): do not use SSBulkLoadFileSet, use BulkLoadFileSet instead
		try {
			fileSet.folder = abspath(joinPath(dir, bulkLoadTaskState.getFolder()));

			// Clear existing folder
			platform::eraseDirectoryRecursive(fileSet.folder);
			if (!platform::createDirectory(fileSet.folder)) {
				throw retry();
			}

			// Move bulk load files to loading folder
			std::string fromDataFilePath = abspath(bulkLoadTaskState.getDataFileFullPath());
			std::string toDataFilePath = abspath(joinPath(fileSet.folder, basename(fromDataFilePath)));

			bulkLoadFileCopy(fromDataFilePath, toDataFilePath, fileBytesMax);
			fileSet.dataFileList.insert(toDataFilePath);
			TraceEvent(SevInfo, "SSBulkLoadSSTFileCopied", logId)
			    .detail("BulkLoadTask", bulkLoadTaskState.toString())
			    .detail("FromFile", fromDataFilePath)
			    .detail("ToFile", toDataFilePath);

			std::string fromByteSampleFilePath = abspath(bulkLoadTaskState.getBytesSampleFileFullPath());
			if (!fromByteSampleFilePath.empty() && fileExists(fromByteSampleFilePath)) {
				std::string toByteSampleFilePath = abspath(joinPath(fileSet.folder, basename(fromByteSampleFilePath)));
				bulkLoadFileCopy(fromByteSampleFilePath, toByteSampleFilePath, fileBytesMax);
				fileSet.bytesSampleFile = toByteSampleFilePath;
				TraceEvent(SevInfo, "SSBulkLoadSSTFileCopied", logId)
				    .detail("BulkLoadTask", bulkLoadTaskState.toString())
				    .detail("FromFile", fromByteSampleFilePath)
				    .detail("ToFile", toByteSampleFilePath);
			}
			return fileSet;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "SSBulkLoadTaskFetchSSTFileCopyError", logId)
			    .errorUnsuppressed(e)
			    .detail("BulkLoadTask", bulkLoadTaskState.toString())
			    .detail("FromFile", fromDataFilePath)
			    .detail("ToFile", toDataFilePath);
			wait(delay(5.0));
		}
	}
}

ACTOR Future<Optional<std::string>> getBytesSamplingFromSSTFiles(std::string folderToGenerate,
                                                                 std::unordered_set<std::string> dataFiles,
                                                                 UID logId) {
	loop {
		try {
			std::string bytesSampleFile = abspath(
			    joinPath(folderToGenerate, deterministicRandom()->randomUniqueID().toString() + "-bytesample.sst"));
			std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
			sstWriter->open(bytesSampleFile);
			bool anySampled = false;
			for (const auto& filePath : dataFiles) {
				std::unique_ptr<IRocksDBSstFileReader> reader = newRocksDBSstFileReader();
				reader->open(filePath);
				while (reader->hasNext()) {
					KeyValue kv = reader->next();
					ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
					if (sampleInfo.inSample) {
						sstWriter->write(kv.key, kv.value);
						anySampled = true;
					}
				}
			}
			// It is possible that no key is sampled
			// This can happen when the data to sample is small
			// In this case, no SST sample byte file is generated
			if (anySampled) {
				ASSERT(sstWriter->finish());
				return bytesSampleFile;
			} else {
				return Optional<std::string>();
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "SSBulkLoadTaskSamplingError", logId).errorUnsuppressed(e);
			wait(delay(5.0));
		}
	}
}
