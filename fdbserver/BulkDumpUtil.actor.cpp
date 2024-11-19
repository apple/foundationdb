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

#include "fdbserver/BulkDumpUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "flow/Platform.h"
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

std::string generateRandomBulkDumpDataFileName(Version version) {
	return std::to_string(version) + "-data.sst";
}

void dumpDataFileToLocalDirectory(const std::map<Key, Value>& sortedKVS,
                                  const std::string& rootFolder,
                                  const std::string& relativeFolder,
                                  Version dumpVersion) {
	if (sortedKVS.size() == 0) {
		return;
	}
	const std::string dumpFolder = abspath(joinPath(rootFolder, relativeFolder));
	platform::eraseDirectoryRecursive(dumpFolder);
	if (!platform::createDirectory(dumpFolder)) {
		throw retry();
	}
	std::string dataFileName = generateRandomBulkDumpDataFileName(dumpVersion);
	std::string dataFile = abspath(joinPath(dumpFolder, dataFileName));
	ASSERT(!fileExists(dataFile));
	// TODO: generate metadata file
	std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
	sstWriter->open(dataFile);
	for (const auto& [key, value] : sortedKVS) {
		sstWriter->write(key, value); // assuming sorted
	}
	ASSERT(sstWriter->finish());
	return;
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
			wait(delay(5.0));
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
