/*
 * BlobGranuleReader.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include <map>
#include <vector>

#include "fmt/format.h"
#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleFiles.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/BlobWorkerCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/FDBTypes.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Standalone<StringRef>> readFile(Reference<BlobConnectionProvider> bstoreProvider, BlobFilePointerRef f) {
	try {
		state Arena arena;
		std::string fname = f.filename.toString();
		state Reference<BackupContainerFileSystem> bstore = bstoreProvider->getForRead(fname);
		// printf("Starting read of snapshot file %s\n", fname.c_str());
		state Reference<IAsyncFile> reader = wait(bstore->readFile(fname));
		// printf("Got snapshot file size %lld\n", size);
		state uint8_t* data = new (arena) uint8_t[f.length];
		// printf("Reading %lld bytes from snapshot file %s\n", size, filename.c_str());

		state int lengthRemaining = f.length;
		state int64_t blockOffset = f.offset;
		while (lengthRemaining > 0) {
			int blockSize = std::min(lengthRemaining, CLIENT_KNOBS->BGR_READ_BLOCK_SIZE);
			int readSize = wait(reader->read(data + (blockOffset - f.offset), blockSize, blockOffset));
			ASSERT(readSize <= lengthRemaining);
			lengthRemaining -= readSize;
			blockOffset += readSize;
		}

		StringRef dataRef(data, f.length);
		return Standalone<StringRef>(dataRef, arena);
	} catch (Error& e) {
		throw e;
	}
}

// TODO: improve the interface of this function so that it doesn't need
//       to be passed the entire BlobWorkerStats object

// FIXME: probably want to chunk this up with yields to avoid slow task for blob worker re-snapshotting by calling the
// sub-functions that BlobGranuleFiles actually exposes?
ACTOR Future<RangeResult> readBlobGranule(BlobGranuleChunkRef chunk,
                                          KeyRangeRef keyRange,
                                          Version beginVersion,
                                          Version readVersion,
                                          Reference<BlobConnectionProvider> bstore,
                                          Optional<BlobWorkerStats*> stats) {

	// TODO REMOVE with early replying
	ASSERT(readVersion == chunk.includedVersion);

	state Arena arena;

	try {
		Future<Standalone<StringRef>> readSnapshotFuture;
		if (chunk.snapshotFile.present()) {
			readSnapshotFuture = readFile(bstore, chunk.snapshotFile.get());
			if (stats.present()) {
				++stats.get()->s3GetReqs;
			}
		}
		state std::vector<Future<Standalone<StringRef>>> readDeltaFutures;

		readDeltaFutures.reserve(chunk.deltaFiles.size());
		for (BlobFilePointerRef deltaFile : chunk.deltaFiles) {
			readDeltaFutures.push_back(readFile(bstore, deltaFile));
			if (stats.present()) {
				++stats.get()->s3GetReqs;
			}
		}

		state Optional<StringRef> snapshotData; // not present if snapshotFile isn't present
		if (chunk.snapshotFile.present()) {
			state Standalone<StringRef> s = wait(readSnapshotFuture);
			arena.dependsOn(s.arena());
			snapshotData = s;
		}

		state int numDeltaFiles = chunk.deltaFiles.size();
		state StringRef* deltaData = new (arena) StringRef[numDeltaFiles];
		state int deltaIdx;

		// for (Future<Standalone<StringRef>> deltaFuture : readDeltaFutures) {
		for (deltaIdx = 0; deltaIdx < numDeltaFiles; deltaIdx++) {
			Standalone<StringRef> data = wait(readDeltaFutures[deltaIdx]);
			deltaData[deltaIdx] = data;
			arena.dependsOn(data.arena());
		}

		return materializeBlobGranule(chunk, keyRange, beginVersion, readVersion, snapshotData, deltaData);

	} catch (Error& e) {
		throw e;
	}
}

// TODO probably should add things like limit/bytelimit at some point?
ACTOR Future<Void> readBlobGranules(BlobGranuleFileRequest request,
                                    BlobGranuleFileReply reply,
                                    Reference<BlobConnectionProvider> bstore,
                                    PromiseStream<RangeResult> results) {
	// TODO for large amount of chunks, this should probably have some sort of buffer limit like ReplyPromiseStream.
	// Maybe just use ReplyPromiseStream instead of PromiseStream?
	try {
		state int i;
		for (i = 0; i < reply.chunks.size(); i++) {
			RangeResult chunkResult = wait(
			    readBlobGranule(reply.chunks[i], request.keyRange, request.beginVersion, request.readVersion, bstore));
			results.send(std::move(chunkResult));
		}
		results.sendError(end_of_stream());
	} catch (Error& e) {
		results.sendError(e);
	}

	return Void();
}
