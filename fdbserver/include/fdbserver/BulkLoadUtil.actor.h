/*
 * BulkLoadUtil.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BULKLOADUTIL_ACTOR_G_H)
#define FDBSERVER_BULKLOADUTIL_ACTOR_G_H
#include "fdbserver/BulkLoadUtil.actor.g.h"
#elif !defined(FDBSERVER_BULKLOADUTIL_ACTOR_H)
#define FDBSERVER_BULKLOADUTIL_ACTOR_H
#pragma once

#include "fdbclient/BulkLoading.h"
#include "flow/actorcompiler.h" // has to be last include

struct SSBulkLoadFileSet {
	std::unordered_set<std::string> dataFileList;
	Optional<std::string> bytesSampleFile;
	std::string folder;
	SSBulkLoadFileSet() = default;
	std::string toString() {
		std::string res = "SSBulkLoadFileSet: [DataFiles]: " + describe(dataFileList);
		if (bytesSampleFile.present()) {
			res = res + ", [BytesSampleFile]: " + bytesSampleFile.get();
		}
		res = res + ", [Folder]: " + folder;
		return res;
	}
};

std::string generateRandomBulkLoadDataFileName();

std::string generateRandomBulkLoadBytesSampleFileName();

ACTOR Future<Optional<BulkLoadState>> getBulkLoadStateFromDataMove(Database cx, UID dataMoveId, UID logId);

void bulkLoadFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax);

ACTOR Future<SSBulkLoadFileSet> bulkLoadTransportCP_impl(std::string dir,
                                                         BulkLoadState bulkLoadState,
                                                         size_t fileBytesMax,
                                                         UID logId);

ACTOR Future<Optional<std::string>> getBytesSamplingFromSSTFiles(std::string folderToGenerate,
                                                                 std::unordered_set<std::string> dataFiles,
                                                                 UID logId);

void checkContent(std::unordered_set<std::string> dataFiles, UID logId);

#include "flow/unactorcompiler.h"
#endif
