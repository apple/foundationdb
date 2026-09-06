/*
 * BulkSstFiles.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/checkpoint/BulkSstFiles.h"
#include "fdbserver/checkpoint/RocksDBCheckpointUtils.h"
#include "fdbserver/core/BulkLoadUtil.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/StorageMetrics.h"
#include "flow/genericactors.h"

// Generate SST file given the input sortedKVS to the input filePath.
// TODO(BulkDump): This copy of sortedKVS can be a slow task if data is large.
void writeKVSToSSTFile(std::string filePath, std::map<Key, Value>& sortedKVS, UID logId) {
	const std::string absFilePath = abspath(filePath);
	// Check file
	if (fileExists(absFilePath)) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old File when writeKVSToSSTFile")
		    .detail("DataFilePathLocal", absFilePath);
		ASSERT_WE_THINK(false);
		throw retry();
	}
	// Dump data to file
	std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
	sstWriter->open(absFilePath);
	for (const auto& [key, value] : sortedKVS) {
		sstWriter->write(key, value); // assuming sorted
	}
	if (!sstWriter->finish()) {
		// Unexpected: having data but failed to finish
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "failed to finish data sst writer when writeKVSToSSTFile")
		    .detail("DataFilePath", absFilePath);
		ASSERT_WE_THINK(false);
		throw retry();
	}
	return;
}

Future<BulkLoadManifest> dumpDataFileToLocalDirectory(UID logId,
                                                      std::shared_ptr<RangeDumpRawData> rangeDumpRawData,
                                                      BulkLoadFileSet localFileSet,
                                                      BulkLoadFileSet remoteFileSet,
                                                      BulkLoadByteSampleSetting byteSampleSetting,
                                                      Version dumpVersion,
                                                      KeyRange dumpRange,
                                                      BulkLoadType dumpType,
                                                      BulkLoadTransportMethod transportMethod) {
	// Step 1: Clean up local folder
	resetFileFolder((abspath(localFileSet.getFolder())));

	// Step 2: Dump data to file
	bool containDataFile = false;
	if (!rangeDumpRawData->kvs.empty()) {
		writeKVSToSSTFile(abspath(localFileSet.getDataFileFullPath()), rangeDumpRawData->kvs, logId);
		containDataFile = true;
	} else {
		ASSERT(rangeDumpRawData->sampled.empty());
		containDataFile = false;
	}

	// Step 3: Dump sample to file
	bool containByteSampleFile = false;
	if (!rangeDumpRawData->sampled.empty()) {
		writeKVSToSSTFile(abspath(localFileSet.getBytesSampleFileFullPath()), rangeDumpRawData->sampled, logId);
		containByteSampleFile = true;
	} else {
		containByteSampleFile = false;
	}

	// Step 4: Generate manifest file
	if (fileExists(abspath(localFileSet.getManifestFileFullPath()))) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old manifestFile")
		    .detail("ManifestFilePathLocal", abspath(localFileSet.getManifestFileFullPath()));
		ASSERT_WE_THINK(false);
		throw retry();
	}
	BulkLoadFileSet fileSetRemote(remoteFileSet.getRootPath(),
	                              remoteFileSet.getRelativePath(),
	                              remoteFileSet.getManifestFileName(),
	                              containDataFile ? remoteFileSet.getDataFileName() : std::string(),
	                              containByteSampleFile ? remoteFileSet.getByteSampleFileName() : std::string(),
	                              BulkLoadChecksum());
	BulkLoadManifest manifestMetadata(fileSetRemote,
	                                  dumpRange.begin,
	                                  dumpRange.end,
	                                  dumpVersion,
	                                  rangeDumpRawData->kvsBytes,
	                                  rangeDumpRawData->kvs.size(),
	                                  byteSampleSetting,
	                                  dumpType,
	                                  transportMethod);
	std::string manifestStr = manifestMetadata.toString();
	std::shared_ptr<std::string> manifest = std::make_shared<std::string>(std::move(manifestStr));
	co_await writeBulkFileBytes(abspath(localFileSet.getManifestFileFullPath()), manifest);
	co_return manifestMetadata;
}


// Return true if generated the byte sampling file. Otherwise, return false.
// TODO(BulkDump): directly read from special key space.
Future<bool> doBytesSamplingOnDataFile(std::string dataFileFullPath, // input file
                                       std::string byteSampleFileFullPath, // output file
                                       UID logId) {
	int counter = 0;
	bool res = false;
	int retryCount = 0;
	double startTime = now();
	while (true) {
		Error err;
		try {
			std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
			sstWriter->open(abspath(byteSampleFileFullPath));
			bool anySampled = false;
			std::unique_ptr<IRocksDBSstFileReader> reader = newRocksDBSstFileReader();
			reader->open(abspath(dataFileFullPath));
			while (reader->hasNext()) {
				KeyValue kv = reader->next();
				ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
				if (sampleInfo.inSample) {
					sstWriter->write(kv.key, BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned()));
					anySampled = true;
					counter++;
					if (counter > SERVER_KNOBS->BULKLOAD_BYTE_SAMPLE_BATCH_KEY_COUNT) {
						co_await yield();
						counter = 0;
					}
				}
			}
			// It is possible that no key is sampled
			// This can happen when the data to sample is small
			// In this case, no SST sample byte file is generated
			if (anySampled) {
				ASSERT(sstWriter->finish());
				res = true;
			} else {
				ASSERT(!sstWriter->finish());
				deleteFile(abspath(byteSampleFileFullPath));
			}
			break;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}
		TraceEvent(SevWarn, "SSBulkLoadTaskSamplingError", logId)
		    .errorUnsuppressed(err)
		    .detail("DataFileFullPath", dataFileFullPath)
		    .detail("ByteSampleFileFullPath", byteSampleFileFullPath)
		    .detail("Duration", now() - startTime)
		    .detail("RetryCount", retryCount);
		co_await delay(5.0);
		deleteFile(abspath(byteSampleFileFullPath));
		retryCount++;
	}
	TraceEvent(bulkLoadVerboseEventSev(), "SSBulkLoadTaskSamplingComplete", logId)
	    .detail("DataFileFullPath", dataFileFullPath)
	    .detail("ByteSampleFileFullPath", byteSampleFileFullPath)
	    .detail("Duration", now() - startTime)
	    .detail("RetryCount", retryCount);
	co_return res;
}
