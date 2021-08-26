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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/BlobWorker.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WaitFailure.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // has to be last include

// TODO add comments + documentation
struct BlobFileIndex {
	Version version;
	std::string filename;
	int64_t offset;
	int64_t length;

	BlobFileIndex(Version version, std::string filename, int64_t offset, int64_t length)
	  : version(version), filename(filename), offset(offset), length(length) {}
};

struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	std::deque<BlobFileIndex> snapshotFiles;
	std::deque<BlobFileIndex> deltaFiles;
	GranuleDeltas currentDeltas;
	uint64_t bytesInNewDeltaFiles = 0;
	Version lastWriteVersion = 0;
	Version currentDeltaVersion = 0;
	uint64_t currentDeltaBytes = 0;
	Arena deltaArena;

	KeyRange keyRange;
	Future<Void> fileUpdaterFuture;

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

	// FIXME: refactor out the parts of this that are just for interacting with blob stores from the backup business
	// logic
	Reference<BackupContainerFileSystem> bstore;
	// Reference<S3BlobStoreEndpoint> bstore;
	// std::string bucket;

	KeyRangeMap<Reference<GranuleMetadata>> granuleMetadata;

	BlobWorkerData(UID id, Database db) : id(id), db(db) {}
	~BlobWorkerData() { printf("Destroying blob worker data for %s\n", id.toString().c_str()); }
};

static Value getFileValue(std::string fname, int64_t offset, int64_t length) {
	Tuple fileValue;
	fileValue.append(fname).append(offset).append(length);
	return fileValue.getDataAsStandalone();
}

// TODO add granule locks
ACTOR Future<BlobFileIndex> writeDeltaFile(BlobWorkerData* bwData,
                                           KeyRange keyRange,
                                           GranuleDeltas const* deltasToWrite,
                                           Version currentDeltaVersion) {

	// TODO some sort of directory structure would be useful?
	state std::string fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
	                          std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(currentDeltaVersion) +
	                          ".delta";

	state Value serialized = ObjectWriter::toValue(*deltasToWrite, Unversioned());

	state Reference<IBackupFile> objectFile = wait(bwData->bstore->writeFile(fname));
	wait(objectFile->append(serialized.begin(), serialized.size()));
	wait(objectFile->finish());

	// update FDB with new file
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			Tuple deltaFileKey;
			deltaFileKey.append(keyRange.begin).append(keyRange.end);
			deltaFileKey.append(LiteralStringRef("delta")).append(currentDeltaVersion);

			tr->set(deltaFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin),
			        getFileValue(fname, 0, serialized.size()));

			wait(tr->commit());
			printf("blob worker updated fdb with delta file %s of size %d at version %lld\n",
			       fname.c_str(),
			       serialized.size(),
			       currentDeltaVersion);
			return BlobFileIndex(currentDeltaVersion, fname, 0, serialized.size());
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<BlobFileIndex> writeSnapshot(BlobWorkerData* bwData,
                                          KeyRange keyRange,
                                          Version version,
                                          PromiseStream<RangeResult> rows) {
	// TODO some sort of directory structure would be useful maybe?
	state std::string fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
	                          std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(version) + ".snapshot";
	state Arena arena;
	state GranuleSnapshot snapshot;

	loop {
		try {
			RangeResult res = waitNext(rows.getFuture());
			arena.dependsOn(res.arena());
			snapshot.append(arena, res.begin(), res.size());
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			throw e;
		}
	}

	printf("Granule [%s - %s) read %d snapshot rows\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str(),
	       snapshot.size());
	if (snapshot.size() < 10) {
		for (auto& row : snapshot) {
			printf("  %s=%s\n", row.key.printable().c_str(), row.value.printable().c_str());
		}
	}

	// TODO REMOVE sanity check!
	for (int i = 0; i < snapshot.size() - 1; i++) {
		if (snapshot[i].key >= snapshot[i + 1].key) {
			printf("SORT ORDER VIOLATION IN SNAPSHOT FILE: %s, %s\n",
			       snapshot[i].key.printable().c_str(),
			       snapshot[i + 1].key.printable().c_str());
		}
		ASSERT(snapshot[i].key < snapshot[i + 1].key);
	}

	// TODO is this easy to read as a flatbuffer from reader? Need to be sure about this data format
	state Value serialized = ObjectWriter::toValue(snapshot, Unversioned());

	// write to s3 using multi part upload
	state Reference<IBackupFile> objectFile = wait(bwData->bstore->writeFile(fname));
	wait(objectFile->append(serialized.begin(), serialized.size()));
	wait(objectFile->finish());

	// object uploaded successfully, save it to system key space
	// TODO add conflict range for writes?
	state Tuple snapshotFileKey;
	snapshotFileKey.append(keyRange.begin).append(keyRange.end);
	snapshotFileKey.append(LiteralStringRef("snapshot")).append(version);

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);

	try {
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				tr->set(snapshotFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin),
				        getFileValue(fname, 0, serialized.size()));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// if transaction throws non-retryable error, delete s3 file before exiting
		printf("deleting s3 object %s after error %s\n", fname.c_str(), e.name());
		bwData->bstore->deleteFile(fname);
		throw e;
	}

	printf("Granule [%s - %s) committed new snapshot file %s with %d bytes\n\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str(),
	       fname.c_str(),
	       serialized.size());

	return BlobFileIndex(version, fname, 0, serialized.size());
}

ACTOR Future<BlobFileIndex> dumpInitialSnapshotFromFDB(BlobWorkerData* bwData, KeyRange keyRange) {
	printf("Dumping snapshot from FDB for [%s - %s)\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str());
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			state Version readVersion = wait(tr->getReadVersion());
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(bwData, keyRange, readVersion, rowsStream);

			state Key beginKey = keyRange.begin;
			loop {
				// TODO knob for limit?
				RangeResult res = wait(tr->getRange(KeyRangeRef(beginKey, keyRange.end), 1000));
				rowsStream.send(res);
				if (res.more) {
					beginKey = keyAfter(res.back().key);
				} else {
					rowsStream.sendError(end_of_stream());
					break;
				}
			}
			BlobFileIndex f = wait(snapshotWriter);
			return f;
		} catch (Error& e) {
			printf("Dumping snapshot from FDB for [%s - %s) got error %s\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       e.name());
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<BlobFileIndex> compactFromBlob(BlobWorkerData* bwData, KeyRange keyRange) {
	printf("Compacting snapshot from blob for [%s - %s)\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str());

	Reference<GranuleMetadata> metadata = bwData->granuleMetadata.rangeContaining(keyRange.begin).value();
	ASSERT(metadata->keyRange == keyRange);

	ASSERT(!metadata->snapshotFiles.empty());
	ASSERT(!metadata->deltaFiles.empty());
	ASSERT(metadata->currentDeltas.empty());
	state Version version = metadata->deltaFiles.back().version;

	state Arena filenameArena;
	state BlobGranuleChunkRef chunk;

	state Version snapshotVersion = metadata->snapshotFiles.back().version;
	BlobFileIndex snapshotF = metadata->snapshotFiles.back();
	chunk.snapshotFile = BlobFilenameRef(filenameArena, snapshotF.filename, snapshotF.offset, snapshotF.length);
	int deltaIdx = metadata->deltaFiles.size() - 1;
	while (deltaIdx >= 0 && metadata->deltaFiles[deltaIdx].version > snapshotVersion) {
		deltaIdx--;
	}
	deltaIdx++;
	while (deltaIdx < metadata->deltaFiles.size()) {
		BlobFileIndex deltaF = metadata->deltaFiles[deltaIdx];
		chunk.deltaFiles.emplace_back_deep(filenameArena, deltaF.filename, deltaF.offset, deltaF.length);
		deltaIdx++;
	}
	chunk.includedVersion = version;

	printf("Re-snapshotting [%s - %s) @ %lld\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str(),
	       version);

	printf("  SnapshotFile:\n    %s\n", chunk.snapshotFile.get().toString().c_str());
	printf("  DeltaFiles:\n");
	for (auto& df : chunk.deltaFiles) {
		printf("    %s\n", df.toString().c_str());
	}

	loop {
		try {
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(bwData, keyRange, version, rowsStream);
			RangeResult newGranule = wait(readBlobGranule(chunk, keyRange, version, bwData->bstore));
			rowsStream.send(std::move(newGranule));
			rowsStream.sendError(end_of_stream());

			BlobFileIndex f = wait(snapshotWriter);
			return f;
		} catch (Error& e) {
			// TODO better error handling eventually - should retry unless the error is because another worker took over
			// the range
			printf("Compacting snapshot from blob for [%s - %s) got error %s\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       e.name());
			throw e;
		}
	}
}

ACTOR Future<std::pair<Key, Version>> createRangeFeed(BlobWorkerData* bwData, KeyRange keyRange) {
	state Key rangeFeedID = StringRef(deterministicRandom()->randomUniqueID().toString());
	state Transaction tr(bwData->db);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
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

		BlobFileIndex newSnapshotFile = wait(dumpInitialSnapshotFromFDB(bwData, metadata->keyRange));
		ASSERT(rangeFeedData.second <= newSnapshotFile.version);
		metadata->snapshotFiles.push_back(newSnapshotFile);
		metadata->lastWriteVersion = newSnapshotFile.version;
		metadata->currentDeltaVersion = metadata->lastWriteVersion;
		rangeFeedFuture = bwData->db->getRangeFeedStream(
		    rangeFeedStream, rangeFeedData.first, newSnapshotFile.version + 1, maxVersion, metadata->keyRange);

		loop {
			state Standalone<VectorRef<MutationsAndVersionRef>> mutations = waitNext(rangeFeedStream.getFuture());
			for (auto& deltas : mutations) {
				if (!deltas.mutations.empty()) {
					metadata->currentDeltas.push_back_deep(metadata->deltaArena, deltas);
					for (auto& delta : deltas.mutations) {
						// FIXME: add mutation tracking here
						// 8 for version, 1 for type, 4 for each param length then actual param size
						metadata->currentDeltaBytes += 17 + delta.param1.size() + delta.param2.size();
					}
				}

				ASSERT(metadata->currentDeltaVersion <= deltas.version);
				metadata->currentDeltaVersion = deltas.version;

				// TODO handle version batch barriers
				if (metadata->currentDeltaBytes >= SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES &&
				    metadata->currentDeltaVersion > metadata->lastWriteVersion) {
					printf("Granule [%s - %s) flushing delta file after %d bytes\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       metadata->currentDeltaBytes);
					BlobFileIndex newDeltaFile = wait(writeDeltaFile(
					    bwData, metadata->keyRange, &metadata->currentDeltas, metadata->currentDeltaVersion));

					// add new delta file
					metadata->deltaFiles.push_back(newDeltaFile);
					metadata->lastWriteVersion = metadata->currentDeltaVersion;
					metadata->bytesInNewDeltaFiles += metadata->currentDeltaBytes;

					// reset current deltas
					metadata->deltaArena = Arena();
					metadata->currentDeltas = GranuleDeltas();
					metadata->currentDeltaBytes = 0;

					printf("Popping range feed %s at %lld\n\n",
					       rangeFeedData.first.printable().c_str(),
					       metadata->lastWriteVersion);
					wait(bwData->db->popRangeFeedMutations(rangeFeedData.first, metadata->lastWriteVersion));
				}

				if (metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
					printf("Granule [%s - %s) re-snapshotting after %d bytes\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       metadata->bytesInNewDeltaFiles);
					// FIXME: instead of just doing new snapshot, it should offer shard back to blob manager and get
					// reassigned
					// TODO: this could read from FDB read previous snapshot + delta files instead if it knew there was
					// a large range clear at the end or it knew the granule was small, or something
					BlobFileIndex newSnapshotFile = wait(compactFromBlob(bwData, metadata->keyRange));

					// add new snapshot file
					metadata->snapshotFiles.push_back(newSnapshotFile);
					metadata->lastWriteVersion = newSnapshotFile.version;

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
	// TODO REMOVE in api V2
	ASSERT(req.beginVersion == 0);
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
		BlobGranuleChunkRef chunk;
		// TODO change in V2
		chunk.includedVersion = req.readVersion;
		chunk.keyRange =
		    KeyRangeRef(StringRef(rep.arena, metadata->keyRange.begin), StringRef(rep.arena, r.value()->keyRange.end));

		// handle snapshot files
		int i = metadata->snapshotFiles.size() - 1;
		while (i >= 0 && metadata->snapshotFiles[i].version > req.readVersion) {
			i--;
		}
		// if version is older than oldest snapshot file (or no snapshot files), throw too old
		// FIXME: probably want a dedicated exception like blob_range_too_old or something instead
		if (i < 0) {
			req.reply.sendError(transaction_too_old());
			return;
		}
		BlobFileIndex snapshotF = metadata->snapshotFiles[i];
		chunk.snapshotFile = BlobFilenameRef(rep.arena, snapshotF.filename, snapshotF.offset, snapshotF.length);
		Version snapshotVersion = metadata->snapshotFiles[i].version;

		// handle delta files
		i = metadata->deltaFiles.size() - 1;
		// skip delta files that are too new
		while (i >= 0 && metadata->deltaFiles[i].version > req.readVersion) {
			i--;
		}
		if (i < metadata->deltaFiles.size() - 1) {
			i++;
		}
		// only include delta files after the snapshot file
		int j = i;
		while (j >= 0 && metadata->deltaFiles[j].version > snapshotVersion) {
			j--;
		}
		j++;
		while (j <= i) {
			BlobFileIndex deltaF = metadata->deltaFiles[j];
			chunk.deltaFiles.emplace_back_deep(rep.arena, deltaF.filename, deltaF.offset, deltaF.length);
			j++;
		}

		// new deltas (if version is larger than version of last delta file)
		// FIXME: do trivial key bounds here if key range is not fully contained in request key range
		if (!metadata->deltaFiles.size() || req.readVersion >= metadata->deltaFiles.back().version) {
			rep.arena.dependsOn(metadata->deltaArena);
			for (auto& delta : metadata->currentDeltas) {
				if (delta.version <= req.readVersion) {
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
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
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

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
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

	printf("Initializing blob worker s3 stuff\n");
	try {
		if (g_network->isSimulated()) {
			printf("BW constructing simulated backup container\n");
			self.bstore = BackupContainerFileSystem::openContainerFS("file://fdbblob/");
		} else {
			printf("BW constructing backup container from %s\n", SERVER_KNOBS->BG_URL.c_str());
			self.bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL);
			printf("BW constructed backup container\n");
		}
	} catch (Error& e) {
		printf("BW got backup container init error %s\n", e.name());
		return Void();
	}

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