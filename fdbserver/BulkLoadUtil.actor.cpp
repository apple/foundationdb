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
#include "flow/actorcompiler.h" // has to be last include

std::string generateRandomBulkLoadDataFileName() {
	return deterministicRandom()->randomUniqueID().toString() + "-data.sst";
}

std::string generateRandomBulkLoadBytesSampleFileName() {
	return deterministicRandom()->randomUniqueID().toString() + "-bytesample.sst";
}

ACTOR Future<Optional<BulkLoadState>> getBulkLoadState(Database cx, UID dataMoveId, UID logId) {
	// TODO(Zhe): check if bulk load task is outdated
	loop {
		state Transaction tr(cx);
		try {
			Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
			if (!val.present()) {
				TraceEvent(SevWarn, "SSBulkLoadDataMoveIdNotExist", logId).detail("DataMoveID", dataMoveId);
				return Optional<BulkLoadState>(); // TODO(Zhe): is this case not expected?
			}
			DataMoveMetaData dataMoveMetaData = decodeDataMoveValue(val.get());
			return dataMoveMetaData.bulkLoadState;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<BulkLoadState> updateBulkLoadTaskPhase(Transaction* tr, KeyRange range, UID taskId, BulkLoadPhase phase) {
	state BulkLoadState bulkLoadState;
	RangeResult result = wait(krmGetRanges(tr, bulkLoadPrefix, range));
	if (result.size() > 2) {
		throw bulkload_task_outdated();
	} else if (result[0].value.empty()) {
		throw bulkload_task_outdated();
	}
	bulkLoadState = decodeBulkLoadState(result[0].value);
	ASSERT(bulkLoadState.taskId.isValid());
	ASSERT(bulkLoadState.range == KeyRangeRef(result[0].key, result[1].key));
	if (taskId != bulkLoadState.taskId || bulkLoadState.phase == BulkLoadPhase::Complete) {
		// The taskId persisted on disk can be only updated by users
		// We ignore any complete task
		throw bulkload_task_outdated();
	}
	bulkLoadState.phase = phase;
	return bulkLoadState;
}

void bulkLoadFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	std::string content = readFileBytes(fromFile, fileBytesMax);
	writeFile(toFile, content);
	// Do file checksum for toFile
	return;
}

ACTOR Future<SSBulkLoadFileSet> bulkLoadTransportCP_impl(std::string dir,
                                                         BulkLoadState bulkLoadState,
                                                         size_t fileBytesMax,
                                                         UID logId) {
	ASSERT(bulkLoadState.transportMethod == BulkLoadTransportMethod::CP);
	loop {
		state std::string toFile;
		state std::string fromFile;
		state SSBulkLoadFileSet fileSet;
		try {
			fileSet.folder = abspath(joinPath(dir, bulkLoadState.folder));

			// Clear existing folder
			platform::eraseDirectoryRecursive(fileSet.folder);
			if (!platform::createDirectory(fileSet.folder)) {
				throw retry();
			}

			// Move bulk load files to loading folder
			for (const auto& filePath : bulkLoadState.dataFiles) {
				fromFile = abspath(filePath);
				toFile = abspath(joinPath(fileSet.folder, generateRandomBulkLoadDataFileName()));
				if (fileSet.dataFileList.find(toFile) != fileSet.dataFileList.end()) {
					ASSERT_WE_THINK(false);
					throw retry();
				}
				bulkLoadFileCopy(fromFile, toFile, fileBytesMax);
				fileSet.dataFileList.insert(toFile);
				TraceEvent(SevInfo, "SSBulkLoadSSTFileCopied", logId)
				    .detail("BulkLoadTask", bulkLoadState.toString())
				    .detail("FromFile", fromFile)
				    .detail("ToFile", toFile);
			}
			if (bulkLoadState.bytesSampleFile.present()) {
				fromFile = abspath(bulkLoadState.bytesSampleFile.get());
				if (fileExists(fromFile)) {
					toFile = abspath(joinPath(fileSet.folder, generateRandomBulkLoadBytesSampleFileName()));
					bulkLoadFileCopy(fromFile, toFile, fileBytesMax);
					fileSet.bytesSampleFile = toFile;
					TraceEvent(SevInfo, "SSBulkLoadSSTFileCopied", logId)
					    .detail("BulkLoadTask", bulkLoadState.toString())
					    .detail("FromFile", fromFile)
					    .detail("ToFile", toFile);
				}
			}
			return fileSet;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "SSBulkLoadTaskFetchSSTFileCopyError", logId)
			    .errorUnsuppressed(e)
			    .detail("BulkLoadTask", bulkLoadState.toString())
			    .detail("FromFile", fromFile)
			    .detail("ToFile", toFile);
			wait(delay(5.0));
		}
	}
}

ACTOR Future<Optional<std::string>> getBytesSamplingFromSSTFiles(std::string folderToGenerate,
                                                                 std::unordered_set<std::string> dataFiles,
                                                                 UID logId) {
	loop {
		try {
			std::string bytesSampleFile =
			    abspath(joinPath(folderToGenerate, generateRandomBulkLoadBytesSampleFileName()));
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
						sstWriter->write(kv.key, kv.value); // TODO(Zhe): validate if kvs are sorted
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

void checkContent(std::unordered_set<std::string> dataFiles, UID logId) {
	for (const auto& filePath : dataFiles) {
		std::unique_ptr<IRocksDBSstFileReader> reader = newRocksDBSstFileReader();
		reader->open(filePath);
		while (reader->hasNext()) {
			KeyValue kv = reader->next();
			TraceEvent("CheckContent", logId).detail("Key", kv.key).detail("Value", kv.value);
		}
	}
	return;
}
