/*
 * BlobGranuleReader.actor.cpp
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

#include <map>
#include <vector>

#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleFiles.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/BlobWorkerCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO more efficient data structure besides std::map? PTree is unecessary since this isn't versioned, but some other
// sorted thing could work. And if it used arenas it'd probably be more efficient with allocations, since everything
// else is in 1 arena and discarded at the end.

// TODO could refactor the file reading code from here and the delta file function into another actor,
// then this part would also be testable? but meh

ACTOR Future<Standalone<StringRef>> readFile(Reference<BackupContainerFileSystem> bstore, BlobFilePointerRef f) {
	try {
		state Arena arena;
		// printf("Starting read of snapshot file %s\n", filename.c_str());
		state Reference<IAsyncFile> reader = wait(bstore->readFile(f.filename.toString()));
		// printf("Got snapshot file size %lld\n", size);
		state uint8_t* data = new (arena) uint8_t[f.length];
		// printf("Reading %lld bytes from snapshot file %s\n", size, filename.c_str());
		int readSize = wait(reader->read(data, f.length, f.offset));
		// printf("Read %lld bytes from snapshot file %s\n", readSize, filename.c_str());
		ASSERT(f.length == readSize);

		StringRef dataRef(data, f.length);
		return Standalone<StringRef>(dataRef, arena);
	} catch (Error& e) {
		printf("Reading file %s got error %s\n", f.toString().c_str(), e.name());
		throw e;
	}
}

// TODO: improve the interface of this function so that it doesn't need
//       to be passed the entire BlobWorkerStats object

// FIXME: probably want to chunk this up with yields to avoid slow task for blob worker re-snapshotting by calling the
// sub-functions that BlobGranuleFiles actually exposes?
ACTOR Future<RangeResult> readBlobGranule(BlobGranuleChunkRef chunk,
                                          KeyRangeRef keyRange,
                                          Version readVersion,
                                          Reference<BackupContainerFileSystem> bstore,
                                          Optional<BlobWorkerStats*> stats) {

	// TODO REMOVE with V2 of protocol
	ASSERT(readVersion == chunk.includedVersion);
	ASSERT(chunk.snapshotFile.present());

	state Arena arena;

	try {
		Future<Standalone<StringRef>> readSnapshotFuture = readFile(bstore, chunk.snapshotFile.get());
		state std::vector<Future<Standalone<StringRef>>> readDeltaFutures;
		if (stats.present()) {
			++stats.get()->s3GetReqs;
		}

		readDeltaFutures.reserve(chunk.deltaFiles.size());
		for (BlobFilePointerRef deltaFile : chunk.deltaFiles) {
			readDeltaFutures.push_back(readFile(bstore, deltaFile));
			if (stats.present()) {
				++stats.get()->s3GetReqs;
			}
		}

		state Standalone<StringRef> snapshotData = wait(readSnapshotFuture);
		arena.dependsOn(snapshotData.arena());

		state int numDeltaFiles = chunk.deltaFiles.size();
		state StringRef* deltaData = new (arena) StringRef[numDeltaFiles];
		state int deltaIdx;

		// for (Future<Standalone<StringRef>> deltaFuture : readDeltaFutures) {
		for (deltaIdx = 0; deltaIdx < numDeltaFiles; deltaIdx++) {
			Standalone<StringRef> data = wait(readDeltaFutures[deltaIdx]);
			deltaData[deltaIdx] = data;
			arena.dependsOn(data.arena());
		}

		return materializeBlobGranule(chunk, keyRange, readVersion, snapshotData, deltaData);

	} catch (Error& e) {
		printf("Reading blob granule got error %s\n", e.name());
		throw e;
	}
}

// TODO probably should add things like limit/bytelimit at some point?
ACTOR Future<Void> readBlobGranules(BlobGranuleFileRequest request,
                                    BlobGranuleFileReply reply,
                                    Reference<BackupContainerFileSystem> bstore,
                                    PromiseStream<RangeResult> results) {
	// TODO for large amount of chunks, this should probably have some sort of buffer limit like ReplyPromiseStream.
	// Maybe just use ReplyPromiseStream instead of PromiseStream?
	try {
		state int i;
		for (i = 0; i < reply.chunks.size(); i++) {
			/*printf("ReadBlobGranules processing chunk %d [%s - %s)\n",
			       i,
			       reply.chunks[i].keyRange.begin.printable().c_str(),
			       reply.chunks[i].keyRange.end.printable().c_str());*/
			RangeResult chunkResult =
			    wait(readBlobGranule(reply.chunks[i], request.keyRange, request.readVersion, bstore));
			results.send(std::move(chunkResult));
		}
		// printf("ReadBlobGranules done, sending EOS\n");
		results.sendError(end_of_stream());
	} catch (Error& e) {
		printf("ReadBlobGranules got error %s\n", e.name());
		results.sendError(e);
	}

	return Void();
}
