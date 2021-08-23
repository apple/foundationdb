/*
 * BlobWorker.actor.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Tuple.h"
#include "fdbclient/S3BlobStore.h"
#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbserver/BlobWorker.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WaitFailure.h"
#include "flow/actorcompiler.h" // has to be last include

// TODO might need to use IBackupFile instead of blob store interface to support non-s3 things like azure?
struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	std::deque<std::pair<Version, std::string>> snapshotFiles;
	std::deque<std::pair<Version, std::string>> deltaFiles;
	GranuleDeltas currentDeltas;
	uint64_t bytesInNewDeltaFiles = 0;
	Version lastWriteVersion = 0;
	uint64_t currentDeltaBytes = 0;
	Arena deltaArena;

	KeyRange keyRange;
	Future<Void> fileUpdaterFuture;
	PromiseStream<Version> snapshotVersions;

	// FIXME: right now there is a dependency because this contains both the actual file/delta data as well as the
	// metadata (worker futures), so removing this reference from the map doesn't actually cancel the workers. It'd be
	// better to have this in 2 separate objects, where the granule metadata map has the futures, but the read
	// queries/file updater/range feed only copy the reference to the file/delta data.
	void cancel() {
		// rangeFeedFuture = Never();
		fileUpdaterFuture = Never();
	}

	~GranuleMetadata() {
		// only print for "active" metadata
		if (lastWriteVersion != 0) {
			printf("Destroying granule metadata for [%s - %s)\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str());
		}
	}
};

struct BlobWorkerData {
	UID id;
	Database db;

	LocalityData locality;

	Reference<S3BlobStoreEndpoint> bstore;
	std::string bucket;

	KeyRangeMap<Reference<GranuleMetadata>> granuleMetadata;

	BlobWorkerData(UID id, Database db) : id(id), db(db) {}
	~BlobWorkerData() { printf("Destroying blob worker data for %s\n", id.toString().c_str()); }
};

// TODO add granule locks
ACTOR Future<std::pair<Version, std::string>> writeDeltaFile(BlobWorkerData* bwData,
                                                             KeyRange keyRange,
                                                             GranuleDeltas const* deltasToWrite) {

	// TODO some sort of directory structure would be useful?
	state Version lastVersion = deltasToWrite->back().v;
	state std::string fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
	                          std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(lastVersion) +
	                          ".delta";

	state Value serialized = ObjectWriter::toValue(*deltasToWrite, Unversioned());

	// write to s3 using multi part upload
	state Reference<AsyncFileS3BlobStoreWrite> objectFile =
	    makeReference<AsyncFileS3BlobStoreWrite>(bwData->bstore, bwData->bucket, fname);
	wait(objectFile->write(serialized.begin(), serialized.size(), 0));
	wait(objectFile->sync());

	// update FDB with new file
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	loop {
		try {
			Tuple deltaFileKey;
			deltaFileKey.append(keyRange.begin).append(keyRange.end);
			deltaFileKey.append(LiteralStringRef("delta")).append(lastVersion);
			tr->set(deltaFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin), fname);

			wait(tr->commit());
			printf("blob worker updated fdb with delta file %s of size %d at version %lld\n",
			       fname.c_str(),
			       serialized.size(),
			       lastVersion);
			return std::pair<Version, std::string>(lastVersion, fname);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<std::pair<Version, std::string>> dumpSnapshotFromFDB(BlobWorkerData* bwData, KeyRange keyRange) {
	printf("Dumping snapshot from FDB for [%s - %s)\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str());
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	loop {
		state std::string fname = "";
		try {
			state Version readVersion = wait(tr->getReadVersion());
			fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
			        std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(readVersion) + ".snapshot";

			// TODO some sort of directory structure would be useful?
			state Arena arena;
			state GranuleSnapshot allRows;

			// TODO would be fairly easy to change this to a promise stream, and optionally build from blobGranuleReader
			// instead
			state Key beginKey = keyRange.begin;
			loop {
				// TODO knob for limit?
				RangeResult res = wait(tr->getRange(KeyRangeRef(beginKey, keyRange.end), 1000));
				/*printf("granule [%s - %s) read %d%s rows\n",
				       keyRange.begin.printable().c_str(),
				       keyRange.end.printable().c_str(),
				       res.size(),
				       res.more ? "+" : "");*/
				arena.dependsOn(res.arena());
				allRows.append(arena, res.begin(), res.size());
				if (res.more) {
					beginKey = keyAfter(res.back().key);
				} else {
					break;
				}
			}

			printf("Granule [%s - %s) read %d snapshot rows from fdb\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       allRows.size());
			if (allRows.size() < 10) {
				for (auto& row : allRows) {
					printf("  %s=%s\n", row.key.printable().c_str(), row.value.printable().c_str());
				}
			}
			// TODO REMOVE sanity check!

			for (int i = 0; i < allRows.size() - 1; i++) {
				if (allRows[i].key >= allRows[i + 1].key) {
					printf("SORT ORDER VIOLATION IN SNAPSHOT FILE: %s, %s\n",
					       allRows[i].key.printable().c_str(),
					       allRows[i + 1].key.printable().c_str());
				}
				ASSERT(allRows[i].key < allRows[i + 1].key);
			}

			// TODO is this easy to read as a flatbuffer from reader? Need to be sure about this data format
			state Value serialized = ObjectWriter::toValue(allRows, Unversioned());

			// write to s3 using multi part upload
			state Reference<AsyncFileS3BlobStoreWrite> objectFile =
			    makeReference<AsyncFileS3BlobStoreWrite>(bwData->bstore, bwData->bucket, fname);
			wait(objectFile->write(serialized.begin(), serialized.size(), 0));
			wait(objectFile->sync());

			// TODO could move this into separate txn to avoid the timeout, it'll need to be separate later anyway
			// object uploaded successfully, save it to system key space (TODO later - and memory file history)
			// TODO add conflict range for writes?
			Tuple snapshotFileKey;
			snapshotFileKey.append(keyRange.begin).append(keyRange.end);
			snapshotFileKey.append(LiteralStringRef("snapshot")).append(readVersion);
			tr->set(snapshotFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin), fname);
			wait(tr->commit());
			printf("Granule [%s - %s) committed new snapshot file %s with %d bytes\n\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       fname.c_str(),
			       serialized.size());
			return std::pair<Version, std::string>(readVersion, fname);
		} catch (Error& e) {
			// TODO REMOVE
			printf("dump range txn got error %s\n", e.name());
			if (fname != "") {
				// TODO delete unsuccessfully written file
				bwData->bstore->deleteObject(bwData->bucket, fname);
				printf("deleting s3 object %s\n", fname.c_str());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<std::pair<Key, Version>> createRangeFeed(BlobWorkerData* bwData, KeyRange keyRange) {
	state Key rangeFeedID = StringRef(deterministicRandom()->randomUniqueID().toString());
	state Transaction tr(bwData->db);
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	loop {
		try {
			wait(tr.registerRangeFeed(rangeFeedID, keyRange));
			wait(tr.commit());
			return std::pair<Key, Version>(rangeFeedID, tr.getCommittedVersion());
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// TODO a hack, eventually just have open end interval in range feed request?
// Or maybe we want to cycle and start a new range feed stream every X million versions?
static const Version maxVersion = std::numeric_limits<Version>::max();

// updater for a single granule
ACTOR Future<Void> blobGranuleUpdateFiles(BlobWorkerData* bwData, Reference<GranuleMetadata> metadata) {

	state PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> rangeFeedStream;
	state Future<Void> rangeFeedFuture;
	try {
		// create range feed first so the version the SS start recording mutations <= the snapshot version
		state std::pair<Key, Version> rangeFeedData = wait(createRangeFeed(bwData, metadata->keyRange));
		printf("Successfully created range feed %s for [%s - %s) @ %lld\n",
		       rangeFeedData.first.printable().c_str(),
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str(),
		       rangeFeedData.second);

		std::pair<Version, std::string> newSnapshotFile = wait(dumpSnapshotFromFDB(bwData, metadata->keyRange));
		ASSERT(rangeFeedData.second <= newSnapshotFile.first);
		metadata->snapshotFiles.push_back(newSnapshotFile);
		metadata->lastWriteVersion = newSnapshotFile.first;
		metadata->snapshotVersions.send(newSnapshotFile.first);
		rangeFeedFuture = bwData->db->getRangeFeedStream(
		    rangeFeedStream, rangeFeedData.first, newSnapshotFile.first + 1, maxVersion, metadata->keyRange);

		loop {
			state Standalone<VectorRef<MutationsAndVersionRef>> mutations = waitNext(rangeFeedStream.getFuture());
			// TODO should maybe change mutation buffer to MutationsAndVersionRef instead of MutationAndVersion
			for (auto& deltas : mutations) {
				for (auto& delta : deltas.mutations) {
					// TODO REMOVE!!! Just for initial debugging
					/*printf("BlobWorker [%s - %s) Got Mutation @ %lld: %s\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       deltas.version,
					       delta.toString().c_str());*/

					metadata->currentDeltas.emplace_back_deep(metadata->deltaArena, delta, deltas.version);
					// 8 for version, 1 for type, 4 for each param length then actual param size
					metadata->currentDeltaBytes += 17 + delta.param1.size() + delta.param2.size();
				}

				// TODO handle version batch barriers
				if (metadata->currentDeltaBytes >= SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES &&
				    metadata->currentDeltas.back().v > metadata->lastWriteVersion) {
					printf("Granule [%s - %s) flushing delta file after %d bytes\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       metadata->currentDeltaBytes);
					std::pair<Version, std::string> newDeltaFile =
					    wait(writeDeltaFile(bwData, metadata->keyRange, &metadata->currentDeltas));

					// add new delta file
					metadata->deltaFiles.push_back(newDeltaFile);
					metadata->lastWriteVersion = newDeltaFile.first;
					metadata->bytesInNewDeltaFiles += metadata->currentDeltaBytes;

					// reset current deltas
					metadata->deltaArena = Arena();
					metadata->currentDeltas = GranuleDeltas();
					metadata->currentDeltaBytes = 0;

					printf("Popping range feed %s at %lld\n\n",
					       rangeFeedData.first.printable().c_str(),
					       newDeltaFile.first);
					wait(bwData->db->popRangeFeedMutations(rangeFeedData.first, newDeltaFile.first));
				}

				if (metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
					printf("Granule [%s - %s) re-snapshotting after %d bytes\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       metadata->bytesInNewDeltaFiles);
					// FIXME: instead of just doing new snapshot, it should offer shard back to blob manager and get
					// reassigned
					// FIXME: this should read previous snapshot + delta files instead, unless it knows it's really
					// small or there was a huge clear or something
					std::pair<Version, std::string> newSnapshotFile =
					    wait(dumpSnapshotFromFDB(bwData, metadata->keyRange));

					// add new snapshot file
					metadata->snapshotFiles.push_back(newSnapshotFile);
					metadata->lastWriteVersion = newSnapshotFile.first;
					metadata->snapshotVersions.send(newSnapshotFile.first);

					// reset metadata
					metadata->bytesInNewDeltaFiles = 0;
				}
			}
		}
	} catch (Error& e) {
		printf("Granule file updater for [%s - %s) got error %s, exiting\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str(),
		       e.name());
		throw e;
	}
}

static void handleBlobGranuleFileRequest(BlobWorkerData* bwData, const BlobGranuleFileRequest& req) {
	BlobGranuleFileReply rep;

	auto checkRanges = bwData->granuleMetadata.intersectingRanges(req.keyRange);
	// check for gaps as errors before doing actual data copying
	KeyRef lastRangeEnd = req.keyRange.begin;
	for (auto& r : checkRanges) {
		if (lastRangeEnd < r.begin()) {
			printf("No blob data for [%s - %s) in request range [%s - %s), skipping request\n",
			       lastRangeEnd.printable().c_str(),
			       r.begin().printable().c_str(),
			       req.keyRange.begin.printable().c_str(),
			       req.keyRange.end.printable().c_str());
			req.reply.sendError(transaction_too_old());
			return;
		}
		if (!r.value().isValid()) {
			printf("No valid blob data for [%s - %s) in request range [%s - %s), skipping request\n",
			       lastRangeEnd.printable().c_str(),
			       r.begin().printable().c_str(),
			       req.keyRange.begin.printable().c_str(),
			       req.keyRange.end.printable().c_str());
			req.reply.sendError(transaction_too_old());
			return;
		}
		lastRangeEnd = r.end();
	}
	if (lastRangeEnd < req.keyRange.end) {
		printf("No blob data for [%s - %s) in request range [%s - %s), skipping request\n",
		       lastRangeEnd.printable().c_str(),
		       req.keyRange.end.printable().c_str(),
		       req.keyRange.begin.printable().c_str(),
		       req.keyRange.end.printable().c_str());
		req.reply.sendError(transaction_too_old());
		return;
	}

	/*printf("BW processing blob granule request for [%s - %s)\n",
	       req.keyRange.begin.printable().c_str(),
	       req.keyRange.end.printable().c_str());*/

	// do work for each range
	auto requestRanges = bwData->granuleMetadata.intersectingRanges(req.keyRange);
	for (auto& r : requestRanges) {
		Reference<GranuleMetadata> metadata = r.value();
		// FIXME: eventually need to handle waiting for granule's committed version to catch up to the request version
		// before copying mutations into reply's arena, to ensure read isn't stale
		BlobGranuleChunk chunk;
		chunk.keyRange =
		    KeyRangeRef(StringRef(rep.arena, metadata->keyRange.begin), StringRef(rep.arena, r.value()->keyRange.end));

		// handle snapshot files
		int i = metadata->snapshotFiles.size() - 1;
		while (i >= 0 && metadata->snapshotFiles[i].first > req.readVersion) {
			i--;
		}
		// if version is older than oldest snapshot file (or no snapshot files), throw too old
		// FIXME: probably want a dedicated exception like blob_range_too_old or something instead
		if (i < 0) {
			req.reply.sendError(transaction_too_old());
			return;
		}
		chunk.snapshotFileName = StringRef(rep.arena, metadata->snapshotFiles[i].second);
		Version snapshotVersion = metadata->snapshotFiles[i].first;

		// handle delta files
		i = metadata->deltaFiles.size() - 1;
		// skip delta files that are too new
		while (i >= 0 && metadata->deltaFiles[i].first > req.readVersion) {
			i--;
		}
		if (i < metadata->deltaFiles.size() - 1) {
			i++;
		}
		// only include delta files after the snapshot file
		int j = i;
		while (j >= 0 && metadata->deltaFiles[j].first > snapshotVersion) {
			j--;
		}
		j++;
		while (j <= i) {
			chunk.deltaFileNames.push_back_deep(rep.arena, metadata->deltaFiles[j].second);
			j++;
		}

		// new deltas (if version is larger than version of last delta file)
		// FIXME: do trivial key bounds here if key range is not fully contained in request key range
		if (!metadata->deltaFiles.size() || req.readVersion >= metadata->deltaFiles.back().first) {
			rep.arena.dependsOn(metadata->deltaArena);
			for (auto& delta : metadata->currentDeltas) {
				if (delta.v <= req.readVersion) {
					chunk.newDeltas.push_back_deep(rep.arena, delta);
				}
			}
		}

		rep.chunks.push_back(rep.arena, chunk);

		// TODO yield?
	}
	req.reply.send(rep);
}

static Reference<GranuleMetadata> constructNewBlobRange(BlobWorkerData* bwData, KeyRange keyRange) {
	/*printf("Creating new worker metadata for range [%s - %s)\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str());*/
	Reference<GranuleMetadata> newMetadata = makeReference<GranuleMetadata>();
	newMetadata->keyRange = keyRange;
	// newMetadata->rangeFeedFuture = fakeRangeFeed(newMetadata->rangeFeed, newMetadata->snapshotVersions, keyRange);
	newMetadata->fileUpdaterFuture = blobGranuleUpdateFiles(bwData, newMetadata);

	return newMetadata;
}

// Any ranges that were purely contained by this range are cancelled. If this intersects but does not fully contain
// any existing range(s), it will restart them at the new cutoff points
static void changeBlobRange(BlobWorkerData* bwData, KeyRange keyRange, Reference<GranuleMetadata> newMetadata) {
	/*printf("Changing range for [%s - %s): %s\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str(),
	       newMetadata.isValid() ? "T" : "F");*/

	// if any of these ranges are active, cancel them.
	// if the first or last range was set, and this key range insertion truncates but does not completely replace them,
	// restart the truncated ranges
	// tricker because this is an iterator, so we don't know size up front
	int i = 0;
	Key firstRangeStart;
	bool firstRangeActive = false;
	Key lastRangeEnd;
	bool lastRangeActive = false;

	auto ranges = bwData->granuleMetadata.intersectingRanges(keyRange);
	for (auto& r : ranges) {
		lastRangeEnd = r.end();
		lastRangeActive = r.value().isValid();
		if (i == 0) {
			firstRangeStart = r.begin();
			firstRangeActive = lastRangeActive;
		}
		if (lastRangeActive) {
			// cancel actors for old range and clear reference
			// printf("  [%s - %s): T (cancelling)\n", r.begin().printable().c_str(), r.end().printable().c_str());
			r.value()->cancel();
			r.value().clear();
		} /*else {
		    printf("  [%s - %s):F\n", r.begin().printable().c_str(), r.end().printable().c_str());
		}*/
		i++;
	}

	bwData->granuleMetadata.insert(keyRange, newMetadata);

	if (firstRangeActive && firstRangeStart < keyRange.begin) {
		/*printf("    Truncated first range [%s - %s)\n",
		       firstRangeStart.printable().c_str(),
		       keyRange.begin.printable().c_str());*/
		// this should modify exactly one range so it's not so bad. A bug here could lead to infinite recursion
		// though
		KeyRangeRef newRange = KeyRangeRef(firstRangeStart, keyRange.begin);
		changeBlobRange(bwData, newRange, constructNewBlobRange(bwData, newRange));
	}

	if (lastRangeActive && keyRange.end < lastRangeEnd) {
		/*printf(
		    "    Truncated last range [%s - %s)\n", keyRange.end.printable().c_str(),
		   lastRangeEnd.printable().c_str());*/
		// this should modify exactly one range so it's not so bad. A bug here could lead to infinite recursion
		// though
		KeyRangeRef newRange = KeyRangeRef(keyRange.end, lastRangeEnd);
		changeBlobRange(bwData, newRange, constructNewBlobRange(bwData, newRange));
	}
}

// TODO USE VERSION!
static void handleAssignedRange(BlobWorkerData* bwData, KeyRange keyRange, Version version) {
	changeBlobRange(bwData, keyRange, constructNewBlobRange(bwData, keyRange));
}

static void handleRevokedRange(BlobWorkerData* bwData, KeyRange keyRange, Version version) {
	changeBlobRange(bwData, keyRange, Reference<GranuleMetadata>());
}

ACTOR Future<Void> registerBlobWorker(BlobWorkerData* bwData, BlobWorkerInterface interf) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {
			Key blobWorkerListKey = blobWorkerListKeyFor(interf.id());
			tr->addReadConflictRange(singleKeyRange(blobWorkerListKey));
			tr->set(blobWorkerListKey, blobWorkerListValue(interf));

			wait(tr->commit());

			printf("Registered blob worker %s\n", interf.id().toString().c_str());
			return Void();
		} catch (Error& e) {
			printf("Registering blob worker %s got error %s\n", interf.id().toString().c_str(), e.name());
			wait(tr->onError(e));
		}
	}
}

// TODO list of key ranges in the future to batch
ACTOR Future<Void> persistAssignWorkerRange(BlobWorkerData* bwData, KeyRange keyRange, Version assignVersion) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {

			wait(krmSetRangeCoalescing(
			    tr, blobGranuleMappingKeys.begin, keyRange, KeyRange(allKeys), blobGranuleMappingValueFor(bwData->id)));

			wait(tr->commit());

			printf("Blob worker %s persisted key range [%s - %s)\n",
			       bwData->id.toString().c_str(),
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str());
			return Void();
		} catch (Error& e) {
			printf("Persisting key range [%s - %s) for blob worker %s got error %s\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       bwData->id.toString().c_str(),
			       e.name());
			wait(tr->onError(e));
		}
	}
}

// TODO need to version assigned ranges with read version of txn the range was read and use that in
// handleAssigned/Revoked from to prevent out-of-order assignments/revokes from the blob manager from getting ranges in
// an incorrect state
// ACTOR Future<Void> blobWorkerCore(BlobWorkerInterface interf, BlobWorkerData* bwData) {

ACTOR Future<Void> blobWorker(BlobWorkerInterface bwInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state BlobWorkerData self(bwInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True));
	self.id = bwInterf.id();
	self.locality = bwInterf.locality;
	self.bucket = SERVER_KNOBS->BG_BUCKET;

	printf("Initializing blob worker s3 stuff\n");
	try {
		printf("BW constructing s3blobstoreendpoint from %s\n", SERVER_KNOBS->BG_URL.c_str());
		self.bstore = S3BlobStoreEndpoint::fromString(SERVER_KNOBS->BG_URL);
		printf("BW checking if bucket %s exists\n", self.bucket.c_str());
		bool bExists = wait(self.bstore->bucketExists(self.bucket));
		if (!bExists) {
			printf("BW Bucket %s does not exist!\n", self.bucket.c_str());
			return Void();
		}
	} catch (Error& e) {
		printf("BW got s3 init error %s\n", e.name());
		return Void();
	}

	printf("BW starting for bucket %s\n", self.bucket.c_str());
	wait(registerBlobWorker(&self, bwInterf));

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());

	addActor.send(waitFailureServer(bwInterf.waitFailure.getFuture()));

	try {
		loop choose {
			when(BlobGranuleFileRequest req = waitNext(bwInterf.blobGranuleFileRequest.getFuture())) {
				/*printf("Got blob granule request [%s - %s)\n",
				       req.keyRange.begin.printable().c_str(),
				       req.keyRange.end.printable().c_str());*/
				handleBlobGranuleFileRequest(&self, req);
			}
			when(AssignBlobRangeRequest _req = waitNext(bwInterf.assignBlobRangeRequest.getFuture())) {
				state AssignBlobRangeRequest req = _req;
				printf("Worker %s %s range [%s - %s) @ %lld\n",
				       self.id.toString().c_str(),
				       req.isAssign ? "assigned" : "revoked",
				       req.keyRange.begin.printable().c_str(),
				       req.keyRange.end.printable().c_str(),
				       req.assignVersion);

				// TODO with range versioning, need to persist only after it's confirmed
				if (req.isAssign) {
					wait(persistAssignWorkerRange(&self, req.keyRange, req.assignVersion));
					handleAssignedRange(&self, req.keyRange, req.assignVersion);
				} else {
					handleRevokedRange(&self, req.keyRange, req.assignVersion);
				}
				req.reply.send(Void());
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		printf("Blob worker got error %s, exiting\n", e.name());
		TraceEvent("BlobWorkerDied", self.id).error(e, true);
	}

	return Void();
}

// TODO add unit tests for assign/revoke range, especially version ordering