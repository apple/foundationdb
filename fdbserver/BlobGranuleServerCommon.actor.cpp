/*
 * BlobGranuleCommon.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/SystemData.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // has to be last include

// Gets the latest granule history node for range that was persisted
ACTOR Future<Optional<GranuleHistory>> getLatestGranuleHistory(Transaction* tr, KeyRange range) {
	KeyRange historyRange = blobGranuleHistoryKeyRangeFor(range);
	state RangeResult result;

	loop {
		try {
			RangeResult _result = wait(tr->getRange(historyRange, 1, Snapshot::False, Reverse::True));
			result = _result;
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	ASSERT(result.size() <= 1);

	Optional<GranuleHistory> history;
	if (!result.empty()) {
		std::pair<KeyRange, Version> decodedKey = decodeBlobGranuleHistoryKey(result[0].key);
		ASSERT(range == decodedKey.first);
		history = GranuleHistory(range, decodedKey.second, decodeBlobGranuleHistoryValue(result[0].value));
	}
	return history;
}

// Gets the files based on the file key range [startKey, endKey)
// and populates the files object accordingly
ACTOR Future<Void> readGranuleFiles(Transaction* tr,
                                    Key* startKey,
                                    Key endKey,
                                    GranuleFiles* files,
                                    UID granuleID,
                                    bool debug) {

	loop {
		int lim = BUGGIFY ? 2 : 1000;
		RangeResult res = wait(tr->getRange(KeyRangeRef(*startKey, endKey), lim));
		for (auto& it : res) {
			UID gid;
			uint8_t fileType;
			Version version;

			Standalone<StringRef> filename;
			int64_t offset;
			int64_t length;

			std::tie(gid, fileType, version) = decodeBlobGranuleFileKey(it.key);
			ASSERT(gid == granuleID);

			std::tie(filename, offset, length) = decodeBlobGranuleFileValue(it.value);

			BlobFileIndex idx(version, filename.toString(), offset, length);
			if (fileType == 'S') {
				ASSERT(files->snapshotFiles.empty() || files->snapshotFiles.back().version < idx.version);
				files->snapshotFiles.push_back(idx);
			} else {
				ASSERT(fileType == 'D');
				ASSERT(files->deltaFiles.empty() || files->deltaFiles.back().version < idx.version);
				files->deltaFiles.push_back(idx);
			}
		}
		if (res.more) {
			*startKey = keyAfter(res.back().key);
		} else {
			break;
		}
	}
	if (debug) {
		printf("Loaded %d snapshot and %d delta files for %s\n",
		       files->snapshotFiles.size(),
		       files->deltaFiles.size(),
		       granuleID.toString().c_str());
	}
	return Void();
}

// Wrapper around readGranuleFiles
// Gets all files belonging to the granule with id granule ID
ACTOR Future<GranuleFiles> loadHistoryFiles(Transaction* tr, UID granuleID, bool debug) {
	state KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
	state Key startKey = range.begin;
	state GranuleFiles files;
	loop {
		try {
			wait(readGranuleFiles(tr, &startKey, range.end, &files, granuleID, debug));
			return files;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}
