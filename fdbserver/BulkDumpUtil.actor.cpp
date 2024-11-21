/*
 * BulkDumpUtils.actor.cpp
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

#include "fdbclient/BulkDumping.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbserver/BulkDumpUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "flow/Buggify.h"
#include "flow/Optional.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // has to be last include

SSBulkDumpTask getSSBulkDumpTask(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                 const BulkDumpState& bulkDumpState) {
	StorageServerInterface targetServer;
	std::vector<UID> checksumServers;
	int dcid = 0;
	for (const auto& [_, dcServers] : locations) {
		if (dcid == 0) {
			const int idx = deterministicRandom()->randomInt(0, dcServers.size());
			targetServer = dcServers[idx];
		}
		for (int i = 0; i < dcServers.size(); i++) {
			if (dcServers[i].id() == targetServer.id()) {
				ASSERT_WE_THINK(dcid == 0);
			} else {
				checksumServers.push_back(dcServers[i].id());
			}
		}
		dcid++;
	}
	return SSBulkDumpTask(targetServer, checksumServers, bulkDumpState);
}

std::string generateBulkDumpDataFileName(Version version) {
	return std::to_string(version) + "-data.sst";
}

std::string generateBulkDumpManifestFileName(Version version) {
	return std::to_string(version) + "-manifest.txt";
}

std::string generateBulkDumpByteSampleFileName(Version version) {
	return std::to_string(version) + "-sample.sst";
}

BulkDumpManifest dumpDataFileToLocalDirectory(const std::map<Key, Value>& sortedKVS,
                                              const std::string& rootFolder,
                                              const std::string& relativeFolder,
                                              Version dumpVersion,
                                              const KeyRange& dumpRange,
                                              int64_t dumpBytes) {
	const std::string dumpFolder = abspath(joinPath(rootFolder, relativeFolder));
	platform::eraseDirectoryRecursive(dumpFolder);
	if (!platform::createDirectory(dumpFolder)) {
		throw retry();
	}

	std::string dataFilePath = "";
	std::string byteSampleFilePath = "";
	if (sortedKVS.size() > 0) {
		std::string dataFileName = generateBulkDumpDataFileName(dumpVersion);
		dataFilePath = abspath(joinPath(dumpFolder, dataFileName));
		ASSERT(!fileExists(dataFilePath));
		std::string byteSampleFileName = generateBulkDumpByteSampleFileName(dumpVersion);
		byteSampleFilePath = abspath(joinPath(dumpFolder, byteSampleFileName));
		ASSERT(!fileExists(byteSampleFilePath));

		std::unique_ptr<IRocksDBSstFileWriter> sstWriterData = newRocksDBSstFileWriter();
		std::unique_ptr<IRocksDBSstFileWriter> sstWriterByteSample = newRocksDBSstFileWriter();
		sstWriterData->open(dataFilePath);
		sstWriterByteSample->open(byteSampleFilePath);
		bool anySampled = false;
		for (const auto& [key, value] : sortedKVS) {
			sstWriterData->write(key, value); // assuming sorted
			ByteSampleInfo sampleInfo = isKeyValueInSample(KeyValueRef(key, value));
			if (sampleInfo.inSample) {
				sstWriterByteSample->write(key, value);
				anySampled = true;
			}
		}
		ASSERT(sstWriterData->finish());
		if (anySampled) {
			ASSERT(sstWriterByteSample->finish());
		} else {
			ASSERT(deleteFile(byteSampleFilePath));
		}
	}

	std::string manifestFileName = generateBulkDumpManifestFileName(dumpVersion);
	std::string manifestFilePath = abspath(joinPath(dumpFolder, manifestFileName));
	ASSERT(!fileExists(manifestFilePath));

	BulkDumpManifest manifest(
	    dataFilePath, manifestFilePath, byteSampleFilePath, dumpRange.begin, dumpRange.end, dumpVersion, "", dumpBytes);
	writeFile(manifestFilePath, manifest.toString());

	return manifest;
}

void bulkDumpFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	std::string content = readFileBytes(fromFile, fileBytesMax);
	writeFile(toFile, content);
	return;
}

ACTOR Future<Void> bulkDumpTransportCP_impl(std::string fromRoot,
                                            std::string toRoot,
                                            std::string relativeFolder,
                                            size_t fileBytesMax,
                                            UID logId) {
	loop {
		state std::string toFolder = abspath(joinPath(toRoot, relativeFolder));
		state std::string fromFolder = abspath(joinPath(fromRoot, relativeFolder));
		state std::string fromFile;
		state std::string toFile;
		try {
			// Clear existing folder
			platform::eraseDirectoryRecursive(toFolder);
			if (!platform::createDirectory(toFolder)) {
				throw retry();
			}

			// Move bulk dump files to the target folder
			for (const auto& filePath : platform::listFiles(fromFolder)) {
				fromFile = abspath(joinPath(fromFolder, basename(filePath)));
				toFile = abspath(joinPath(toFolder, basename(filePath)));
				bulkDumpFileCopy(fromFile, toFile, fileBytesMax);
				TraceEvent(SevInfo, "SSBulkDumpSSTFileCopied", logId)
				    .detail("FromFile", fromFile)
				    .detail("ToFile", toFile);
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "SSBulkDumpSSTFileCopyError", logId)
			    .errorUnsuppressed(e)
			    .detail("FromRoot", fromRoot)
			    .detail("ToRoot", toRoot)
			    .detail("RelativeFolder", relativeFolder)
			    .detail("FromFile", fromFile)
			    .detail("ToFile", toFile);
			if (g_network->isSimulated()) {
				wait(delay(5.0));
			}
		}
	}
	return Void();
}

ACTOR Future<Void> uploadFiles(BulkDumpTransportMethod transportMethod,
                               std::string fromRoot,
                               std::string toRoot,
                               std::string relativeFolder,
                               UID logId) {
	// Upload to S3 or mock file copy
	if (transportMethod != BulkDumpTransportMethod::CP) {
		ASSERT(false);
		throw not_implemented();
	}
	wait(bulkDumpTransportCP_impl(fromRoot, toRoot, relativeFolder, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
	return Void();
}

ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState) {
	state Transaction tr(cx);
	state Key beginKey = bulkDumpState.getRange().begin;
	state Key endKey = bulkDumpState.getRange().end;
	state KeyRange rangeToPersist;
	state RangeResult result;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			rangeToPersist = Standalone(KeyRangeRef(beginKey, endKey));
			wait(store(result, krmGetRanges(&tr, bulkDumpPrefix, rangeToPersist)));
			bool anyNew = false;
			for (int i = 0; i < result.size() - 1; i++) {
				if (result[i].value.empty()) {
					throw bulkdump_task_outdated();
				}
				BulkDumpState currentBulkDumpState = decodeBulkDumpState(result[i].value);
				if (currentBulkDumpState.getPhase() != BulkDumpPhase::Complete) {
					ASSERT(!currentBulkDumpState.getParentId().present());
					ASSERT(bulkDumpState.getParentId().present());
					if (currentBulkDumpState.getTaskId() != bulkDumpState.getParentId().get()) {
						throw bulkdump_task_outdated();
					}
				} else {
					ASSERT(currentBulkDumpState.getParentId().present());
					ASSERT(bulkDumpState.getParentId().present());
					if (currentBulkDumpState.getParentId() != bulkDumpState.getParentId()) {
						throw bulkdump_task_outdated();
					}
				}
				if (!anyNew && currentBulkDumpState.getPhase() == BulkDumpPhase::Submitted) {
					anyNew = true;
				}
			}
			if (!anyNew) {
				throw bulkdump_task_outdated();
			}
			wait(krmSetRangeCoalescing(&tr,
			                           bulkDumpPrefix,
			                           Standalone(KeyRangeRef(beginKey, result[result.size() - 1].key)),
			                           bulkDumpState.getRange(),
			                           bulkDumpStateValue(bulkDumpState)));
			wait(tr.commit());
			beginKey = result[result.size() - 1].key;
			if (beginKey >= endKey) {
				break;
			} else {
				tr.reset();
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}
