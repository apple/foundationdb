/*
 * BlobGranuleServerCommon.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BLOBGRANULESERVERCOMMON_ACTOR_G_H)
#define FDBSERVER_BLOBGRANULESERVERCOMMON_ACTOR_G_H
#include "fdbserver/BlobGranuleServerCommon.actor.g.h"
#elif !defined(FDBSERVER_BLOBGRANULESERVERCOMMON_ACTOR_H)
#define FDBSERVER_BLOBGRANULESERVERCOMMON_ACTOR_H

#pragma once

#include "flow/flow.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "flow/actorcompiler.h" // has to be last include

struct GranuleHistory {
	KeyRange range;
	Version version;
	Standalone<BlobGranuleHistoryValue> value;

	GranuleHistory() {}

	GranuleHistory(KeyRange range, Version version, Standalone<BlobGranuleHistoryValue> value)
	  : range(range), version(version), value(value) {}
};

// Stores info about a file in blob storage
struct BlobFileIndex {
	Version version;
	std::string filename;
	int64_t offset;
	int64_t length;
	int64_t fullFileLength;

	BlobFileIndex() {}

	BlobFileIndex(Version version, std::string filename, int64_t offset, int64_t length, int64_t fullFileLength)
	  : version(version), filename(filename), offset(offset), length(length), fullFileLength(fullFileLength) {}

	// compare on version
	bool operator<(const BlobFileIndex& r) const { return version < r.version; }
};

// FIXME: initialize these to smaller default sizes to save a bit of memory, particularly snapshotFiles
// Stores the files that comprise a blob granule
struct GranuleFiles {
	std::vector<BlobFileIndex> snapshotFiles;
	std::vector<BlobFileIndex> deltaFiles;

	void getFiles(Version beginVersion,
	              Version readVersion,
	              bool canCollapse,
	              BlobGranuleChunkRef& chunk,
	              Arena& replyArena,
	              int64_t& deltaBytesCounter) const;
};

class Transaction;
ACTOR Future<Optional<GranuleHistory>> getLatestGranuleHistory(Transaction* tr, KeyRange range);
ACTOR Future<Void> readGranuleFiles(Transaction* tr, Key* startKey, Key endKey, GranuleFiles* files, UID granuleID);

ACTOR Future<GranuleFiles> loadHistoryFiles(Database cx, UID granuleID);
#endif
