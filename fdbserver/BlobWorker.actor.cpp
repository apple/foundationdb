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

#include "fdbclient/SystemData.h"
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

#define BW_DEBUG true
#define BW_REQUEST_DEBUG false

// TODO add comments + documentation
struct BlobFileIndex {
	Version version;
	std::string filename;
	int64_t offset;
	int64_t length;

	BlobFileIndex(Version version, std::string filename, int64_t offset, int64_t length)
	  : version(version), filename(filename), offset(offset), length(length) {}
};

// for a range that is set
struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	std::deque<BlobFileIndex> snapshotFiles;
	std::deque<BlobFileIndex> deltaFiles;
	GranuleDeltas currentDeltas;
	uint64_t bytesInNewDeltaFiles = 0;
	Version lastWriteVersion = 0;
	Version currentDeltaVersion = 0;
	uint64_t currentDeltaBytes = 0;
	Arena deltaArena;

	int64_t lockEpoch;
	int64_t lockSeqno;

	KeyRange keyRange;
	Future<Void> assignFuture;
	Future<Void> fileUpdaterFuture;

	// FIXME: right now there is a dependency because this contains both the actual file/delta data as well as the
	// metadata (worker futures), so removing this reference from the map doesn't actually cancel the workers. It'd be
	// better to have this in 2 separate objects, where the granule metadata map has the futures, but the read
	// queries/file updater/range feed only copy the reference to the file/delta data.
	void cancel() {
		assignFuture = Never();
		fileUpdaterFuture = Never();
	}
};

// for a range that may or may not be set
struct GranuleRangeMetadata {
	int64_t lastEpoch;
	int64_t lastSeqno;
	Reference<GranuleMetadata> activeMetadata;

	GranuleRangeMetadata() : lastEpoch(0), lastSeqno(0) {}
	GranuleRangeMetadata(int64_t epoch, int64_t seqno, Reference<GranuleMetadata> activeMetadata)
	  : lastEpoch(epoch), lastSeqno(seqno), activeMetadata(activeMetadata) {}
};

struct BlobWorkerData {
	UID id;
	Database db;

	LocalityData locality;
	int64_t currentManagerEpoch = -1;

	// FIXME: refactor out the parts of this that are just for interacting with blob stores from the backup business
	// logic
	Reference<BackupContainerFileSystem> bstore;
	// Reference<S3BlobStoreEndpoint> bstore;
	// std::string bucket;

	KeyRangeMap<GranuleRangeMetadata> granuleMetadata;

	BlobWorkerData(UID id, Database db) : id(id), db(db) {}
	~BlobWorkerData() { printf("Destroying blob worker data for %s\n", id.toString().c_str()); }
};

// returns true if we can acquire it
static void acquireGranuleLock(int64_t epoch, int64_t seqno, std::pair<int64_t, int64_t> prevOwner) {
	// returns true if our lock (E, S) >= (Eprev, Sprev)
	if (epoch < prevOwner.first || (epoch == prevOwner.first && seqno < prevOwner.second)) {
		if (BW_DEBUG) {
			printf("Lock acquire check failed. Proposed (%lld, %lld) < previous (%lld, %lld)\n",
			       epoch,
			       seqno,
			       prevOwner.first,
			       prevOwner.second);
		}
		throw granule_assignment_conflict();
	}
}

static void checkGranuleLock(int64_t epoch, int64_t seqno, std::pair<int64_t, int64_t> currentOwner) {
	// sanity check - lock value should never go backwards because of acquireGranuleLock
	ASSERT(epoch <= currentOwner.first);
	ASSERT(epoch < currentOwner.first || (epoch == currentOwner.first && seqno <= currentOwner.second));

	// returns true if we still own the lock, false if someone else does
	if (epoch != currentOwner.first || seqno != currentOwner.second) {
		if (BW_DEBUG) {
			printf("Lock assignment check failed. Expected (%lld, %lld), got (%lld, %lld)\n",
			       epoch,
			       seqno,
			       currentOwner.first,
			       currentOwner.second);
		}
		throw granule_assignment_conflict();
	}
}

static Key granuleLockKey(KeyRange granuleRange) {
	Tuple k;
	k.append(granuleRange.begin).append(granuleRange.end);
	return k.getDataAsStandalone().withPrefix(blobGranuleLockKeys.begin);
}

ACTOR Future<Void> readAndCheckGranuleLock(Reference<ReadYourWritesTransaction> tr,
                                           KeyRange granuleRange,
                                           int64_t epoch,
                                           int64_t seqno) {
	state Key lockKey = granuleLockKey(granuleRange);
	Optional<Value> lockValue = wait(tr->get(lockKey));

	ASSERT(lockValue.present());
	std::pair<int64_t, int64_t> currentOwner = decodeBlobGranuleLockValue(lockValue.get());
	checkGranuleLock(epoch, seqno, currentOwner);

	// if we still own the lock, add a conflict range in case anybody else takes it over while we add this file
	tr->addReadConflictRange(singleKeyRange(lockKey));

	return Void();
}

static Value getFileValue(std::string fname, int64_t offset, int64_t length) {
	Tuple fileValue;
	fileValue.append(fname).append(offset).append(length);
	return fileValue.getDataAsStandalone();
}

// TODO add granule locks
ACTOR Future<BlobFileIndex> writeDeltaFile(BlobWorkerData* bwData,
                                           KeyRange keyRange,
                                           int64_t epoch,
                                           int64_t seqno,
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
	try {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));
				Tuple deltaFileKey;
				deltaFileKey.append(keyRange.begin).append(keyRange.end);
				deltaFileKey.append(LiteralStringRef("delta")).append(currentDeltaVersion);

				tr->set(deltaFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin),
				        getFileValue(fname, 0, serialized.size()));

				wait(tr->commit());
				if (BW_DEBUG) {
					printf("blob worker updated fdb with delta file %s of size %d at version %lld\n",
					       fname.c_str(),
					       serialized.size(),
					       currentDeltaVersion);
				}
				return BlobFileIndex(currentDeltaVersion, fname, 0, serialized.size());
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// FIXME: only delete if key doesn't exist
		// if transaction throws non-retryable error, delete s3 file before exiting
		if (BW_DEBUG) {
			printf("deleting s3 delta file %s after error %s\n", fname.c_str(), e.name());
		}
		state Error eState = e;
		wait(bwData->bstore->deleteFile(fname));
		throw eState;
	}
}

ACTOR Future<BlobFileIndex> writeSnapshot(BlobWorkerData* bwData,
                                          KeyRange keyRange,
                                          int64_t epoch,
                                          int64_t seqno,
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

	if (BW_DEBUG) {
		printf("Granule [%s - %s) read %d snapshot rows\n",
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str(),
		       snapshot.size());
		if (snapshot.size() < 10) {
			for (auto& row : snapshot) {
				printf("  %s=%s\n", row.key.printable().c_str(), row.value.printable().c_str());
			}
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
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));
				tr->set(snapshotFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin),
				        getFileValue(fname, 0, serialized.size()));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// FIXME: only delete if key doesn't exist
		// if transaction throws non-retryable error, delete s3 file before exiting
		if (BW_DEBUG) {
			printf("deleting s3 snapshot file %s after error %s\n", fname.c_str(), e.name());
		}
		state Error eState = e;
		wait(bwData->bstore->deleteFile(fname));
		throw eState;
	}

	if (BW_DEBUG) {
		printf("Granule [%s - %s) committed new snapshot file %s with %d bytes\n\n",
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str(),
		       fname.c_str(),
		       serialized.size());
	}

	return BlobFileIndex(version, fname, 0, serialized.size());
}

ACTOR Future<BlobFileIndex> dumpInitialSnapshotFromFDB(BlobWorkerData* bwData, Reference<GranuleMetadata> metadata) {
	if (BW_DEBUG) {
		printf("Dumping snapshot from FDB for [%s - %s)\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str());
	}
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);

	loop {
		state Key beginKey = metadata->keyRange.begin;
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			state Version readVersion = wait(tr->getReadVersion());
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(
			    bwData, metadata->keyRange, metadata->lockEpoch, metadata->lockSeqno, readVersion, rowsStream);

			loop {
				// TODO: use streaming range read
				// TODO knob for limit?
				RangeResult res = wait(tr->getRange(KeyRangeRef(beginKey, metadata->keyRange.end), 1000));
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
			if (BW_DEBUG) {
				printf("Dumping snapshot from FDB for [%s - %s) got error %s\n",
				       metadata->keyRange.begin.printable().c_str(),
				       metadata->keyRange.end.printable().c_str(),
				       e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<BlobFileIndex> compactFromBlob(BlobWorkerData* bwData, Reference<GranuleMetadata> metadata) {
	if (BW_DEBUG) {
		printf("Compacting snapshot from blob for [%s - %s)\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str());
	}

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

	if (BW_DEBUG) {
		printf("Re-snapshotting [%s - %s) @ %lld\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str(),
		       version);

		printf("  SnapshotFile:\n    %s\n", chunk.snapshotFile.get().toString().c_str());
		printf("  DeltaFiles:\n");
		for (auto& df : chunk.deltaFiles) {
			printf("    %s\n", df.toString().c_str());
		}
	}

	loop {
		try {
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(
			    bwData, metadata->keyRange, metadata->lockEpoch, metadata->lockSeqno, version, rowsStream);
			RangeResult newGranule = wait(readBlobGranule(chunk, metadata->keyRange, version, bwData->bstore));
			rowsStream.send(std::move(newGranule));
			rowsStream.sendError(end_of_stream());

			BlobFileIndex f = wait(snapshotWriter);
			return f;
		} catch (Error& e) {
			// TODO better error handling eventually - should retry unless the error is because another worker took over
			// the range
			if (BW_DEBUG) {
				printf("Compacting snapshot from blob for [%s - %s) got error %s\n",
				       metadata->keyRange.begin.printable().c_str(),
				       metadata->keyRange.end.printable().c_str(),
				       e.name());
			}
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
		// before starting, make sure worker persists range assignment and acquires the granule lock
		wait(metadata->assignFuture);
		// TODO refactor creating range feed into the same transaction as above?

		// create range feed first so the version the SS start recording mutations <= the snapshot version
		state std::pair<Key, Version> rangeFeedData = wait(createRangeFeed(bwData, metadata->keyRange));
		if (BW_DEBUG) {
			printf("Successfully created range feed %s for [%s - %s) @ %lld\n",
			       rangeFeedData.first.printable().c_str(),
			       metadata->keyRange.begin.printable().c_str(),
			       metadata->keyRange.end.printable().c_str(),
			       rangeFeedData.second);
		}

		BlobFileIndex newSnapshotFile = wait(dumpInitialSnapshotFromFDB(bwData, metadata));
		ASSERT(rangeFeedData.second <= newSnapshotFile.version);
		metadata->snapshotFiles.push_back(newSnapshotFile);
		metadata->lastWriteVersion = newSnapshotFile.version;
		metadata->currentDeltaVersion = metadata->lastWriteVersion;
		rangeFeedFuture = bwData->db->getRangeFeedStream(
		    rangeFeedStream, rangeFeedData.first, newSnapshotFile.version + 1, maxVersion, metadata->keyRange);

		loop {
			// TODO: Buggify delay in change feed stream
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
					if (BW_DEBUG) {
						printf("Granule [%s - %s) flushing delta file after %d bytes\n",
						       metadata->keyRange.begin.printable().c_str(),
						       metadata->keyRange.end.printable().c_str(),
						       metadata->currentDeltaBytes);
					}
					BlobFileIndex newDeltaFile = wait(writeDeltaFile(bwData,
					                                                 metadata->keyRange,
					                                                 metadata->lockEpoch,
					                                                 metadata->lockSeqno,
					                                                 &metadata->currentDeltas,
					                                                 metadata->currentDeltaVersion));

					// add new delta file
					metadata->deltaFiles.push_back(newDeltaFile);
					metadata->lastWriteVersion = metadata->currentDeltaVersion;
					metadata->bytesInNewDeltaFiles += metadata->currentDeltaBytes;

					// reset current deltas
					metadata->deltaArena = Arena();
					metadata->currentDeltas = GranuleDeltas();
					metadata->currentDeltaBytes = 0;

					if (BW_DEBUG) {
						printf("Popping range feed %s at %lld\n\n",
						       rangeFeedData.first.printable().c_str(),
						       metadata->lastWriteVersion);
					}
					wait(bwData->db->popRangeFeedMutations(rangeFeedData.first, metadata->lastWriteVersion));
				}

				if (metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
					if (BW_DEBUG) {
						printf("Granule [%s - %s) re-snapshotting after %d bytes\n",
						       metadata->keyRange.begin.printable().c_str(),
						       metadata->keyRange.end.printable().c_str(),
						       metadata->bytesInNewDeltaFiles);
					}
					// FIXME: instead of just doing new snapshot, it should offer shard back to blob manager and get
					// reassigned
					// TODO: this could read from FDB read previous snapshot + delta files instead if it knew there was
					// a large range clear at the end or it knew the granule was small, or something
					BlobFileIndex newSnapshotFile = wait(compactFromBlob(bwData, metadata));

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
		// TODO in this case, need to update range mapping that it doesn't have the range, and/or try to re-"open" the
		// range if someone else doesn't have it
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
			if (BW_REQUEST_DEBUG) {
				printf("No blob data for [%s - %s) in request range [%s - %s), skipping request\n",
				       lastRangeEnd.printable().c_str(),
				       r.begin().printable().c_str(),
				       req.keyRange.begin.printable().c_str(),
				       req.keyRange.end.printable().c_str());
			}
			req.reply.sendError(wrong_shard_server());
			return;
		}
		if (!r.value().activeMetadata.isValid()) {
			if (BW_REQUEST_DEBUG) {
				printf("No valid blob data for [%s - %s) in request range [%s - %s), skipping request\n",
				       lastRangeEnd.printable().c_str(),
				       r.begin().printable().c_str(),
				       req.keyRange.begin.printable().c_str(),
				       req.keyRange.end.printable().c_str());
			}
			req.reply.sendError(wrong_shard_server());
			return;
		}
		lastRangeEnd = r.end();
	}
	if (lastRangeEnd < req.keyRange.end) {
		if (BW_REQUEST_DEBUG) {
			printf("No blob data for [%s - %s) in request range [%s - %s), skipping request\n",
			       lastRangeEnd.printable().c_str(),
			       req.keyRange.end.printable().c_str(),
			       req.keyRange.begin.printable().c_str(),
			       req.keyRange.end.printable().c_str());
		}
		req.reply.sendError(wrong_shard_server());
		return;
	}

	/*printf("BW processing blob granule request for [%s - %s)\n",
	       req.keyRange.begin.printable().c_str(),
	       req.keyRange.end.printable().c_str());*/

	// do work for each range
	auto requestRanges = bwData->granuleMetadata.intersectingRanges(req.keyRange);
	for (auto& r : requestRanges) {
		Reference<GranuleMetadata> metadata = r.value().activeMetadata;
		// FIXME: eventually need to handle waiting for granule's committed version to catch up to the request version
		// before copying mutations into reply's arena, to ensure read isn't stale
		BlobGranuleChunkRef chunk;
		// TODO change in V2
		chunk.includedVersion = req.readVersion;
		chunk.keyRange = KeyRangeRef(StringRef(rep.arena, metadata->keyRange.begin),
		                             StringRef(rep.arena, r.value().activeMetadata->keyRange.end));

		// handle snapshot files
		int i = metadata->snapshotFiles.size() - 1;
		while (i >= 0 && metadata->snapshotFiles[i].version > req.readVersion) {
			i--;
		}
		// if version is older than oldest snapshot file (or no snapshot files), throw too old
		// FIXME: probably want a dedicated exception like blob_range_too_old or something instead
		if (i < 0) {
			if (BW_REQUEST_DEBUG) {
				printf("Oldest snapshot file for [%s - %s) is @ %lld, later than request version %lld\n",
				       req.keyRange.begin.printable().c_str(),
				       req.keyRange.end.printable().c_str(),
				       metadata->snapshotFiles.size() == 0 ? 0 : metadata->snapshotFiles[0].version,
				       req.readVersion);
			}
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

// TODO list of key ranges in the future to batch
ACTOR Future<Void> persistAssignWorkerRange(BlobWorkerData* bwData, KeyRange keyRange, int64_t epoch, int64_t seqno) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	state Key lockKey = granuleLockKey(keyRange);

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> prevLockValue = wait(tr->get(lockKey));
			if (prevLockValue.present()) {
				std::pair<int64_t, int64_t> prevOwner = decodeBlobGranuleLockValue(prevLockValue.get());
				acquireGranuleLock(epoch, seqno, prevOwner);
			} // else we are first, no need to check for owner conflict

			tr->set(lockKey, blobGranuleLockValueFor(epoch, seqno));

			wait(krmSetRangeCoalescing(
			    tr, blobGranuleMappingKeys.begin, keyRange, KeyRange(allKeys), blobGranuleMappingValueFor(bwData->id)));

			wait(tr->commit());

			if (BW_DEBUG) {
				printf("Blob worker %s persisted key range [%s - %s)\n",
				       bwData->id.toString().c_str(),
				       keyRange.begin.printable().c_str(),
				       keyRange.end.printable().c_str());
			}
			return Void();
		} catch (Error& e) {
			if (BW_DEBUG) {
				printf("Persisting key range [%s - %s) for blob worker %s got error %s\n",
				       keyRange.begin.printable().c_str(),
				       keyRange.end.printable().c_str(),
				       bwData->id.toString().c_str(),
				       e.name());
			}
			wait(tr->onError(e));
		}
	}
}

static GranuleRangeMetadata constructActiveBlobRange(BlobWorkerData* bwData,
                                                     KeyRange keyRange,
                                                     int64_t epoch,
                                                     int64_t seqno) {
	if (BW_DEBUG) {
		printf("Creating new worker metadata for range [%s - %s)\n",
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str());
	}

	Reference<GranuleMetadata> newMetadata = makeReference<GranuleMetadata>();
	newMetadata->keyRange = keyRange;
	newMetadata->lockEpoch = epoch;
	newMetadata->lockSeqno = seqno;
	newMetadata->assignFuture = persistAssignWorkerRange(bwData, keyRange, epoch, seqno);
	newMetadata->fileUpdaterFuture = blobGranuleUpdateFiles(bwData, newMetadata);

	return GranuleRangeMetadata(epoch, seqno, newMetadata);
}

static GranuleRangeMetadata constructInactiveBlobRange(int64_t epoch, int64_t seqno) {
	return GranuleRangeMetadata(epoch, seqno, Reference<GranuleMetadata>());
}

// ignore stale assignments and make repeating the same one idempotent
static bool newerRangeAssignment(GranuleRangeMetadata oldMetadata, int64_t epoch, int64_t seqno) {
	return epoch > oldMetadata.lastEpoch || (epoch == oldMetadata.lastEpoch && seqno > oldMetadata.lastSeqno);
}

// TODO unit test this assignment, particularly out-of-order insertions!

// The contract from the blob manager is:
// If a key range [A, B) was assigned to the worker at seqno S1, no part of the keyspace that intersects [A, B] may be
// re-assigned to the worker until the range has been revoked from this worker. This revoking can either happen by the
// blob manager willingly relinquishing the range, or by the blob manager reassigning it somewhere else. This means that
// if the worker gets an assignment for any range that intersects [A, B) at S3, there must have been a revoke message
// for [A, B) with seqno S3 where S1 < S2 < S3, that was delivered out of order. This means that if there are any
// intersecting but not fully overlapping ranges with a new range assignment, they had already been revoked. So the
// worker will mark them as revoked, but leave the sequence number as S1, so that when the actual revoke message comes
// in, it is a no-op, but updates the sequence number. Similarly, if a worker gets an assign message for any range that
// already has a higher sequence number, that range was either revoked, or revoked and then re-assigned. Either way,
// this assignment is no longer valid.
static void changeBlobRange(BlobWorkerData* bwData, KeyRange keyRange, int64_t epoch, int64_t seqno, bool active) {
	if (BW_DEBUG) {
		printf("Changing range for [%s - %s): %s @ (%lld, %lld)\n",
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str(),
		       active ? "T" : "F",
		       epoch,
		       seqno);
	}

	// For each range that intersects this update:
	// If the identical range already exists at the same assignment sequence nunmber, this is a noop
	// Otherwise, this will consist of a series of ranges that are either older, or newer.
	// For each older range, cancel it if it is active.
	// Insert the current range.
	// Re-insert all newer ranges over the current range.

	std::vector<std::pair<KeyRange, GranuleRangeMetadata>> newerRanges;

	auto ranges = bwData->granuleMetadata.intersectingRanges(keyRange);
	for (auto& r : ranges) {
		if (r.value().lastEpoch == epoch && r.value().lastSeqno == seqno) {
			// applied the same assignment twice, make idempotent
			ASSERT(r.begin() == keyRange.begin);
			ASSERT(r.end() == keyRange.end);
			return;
		}
		bool thisAssignmentNewer = newerRangeAssignment(r.value(), epoch, seqno);
		if (r.value().activeMetadata.isValid() && thisAssignmentNewer) {
			// cancel actors for old range and clear reference
			if (BW_DEBUG) {
				printf("  [%s - %s): @ (%lld, %lld) (cancelling)\n",
				       r.begin().printable().c_str(),
				       r.end().printable().c_str(),
				       r.value().lastEpoch,
				       r.value().lastSeqno);
			}
			r.value().activeMetadata->cancel();
			r.value().activeMetadata.clear();
		} else if (!thisAssignmentNewer) {
			// this assignment is outdated, re-insert it over the current range
			newerRanges.push_back(std::pair(r.range(), r.value()));
		}
	}

	// if range is active, and isn't surpassed by a newer range already, insert an active range
	GranuleRangeMetadata newMetadata = (active && newerRanges.empty())
	                                       ? constructActiveBlobRange(bwData, keyRange, epoch, seqno)
	                                       : constructInactiveBlobRange(epoch, seqno);
	bwData->granuleMetadata.insert(keyRange, newMetadata);
	if (BW_DEBUG) {
		printf("Inserting new range [%s - %s): %s @ (%lld, %lld)\n",
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str(),
		       newMetadata.activeMetadata.isValid() ? "T" : "F",
		       newMetadata.lastEpoch,
		       newMetadata.lastSeqno);
	}

	for (auto& it : newerRanges) {
		if (BW_DEBUG) {
			printf("Re-inserting newer range [%s - %s): %s @ (%lld, %lld)\n",
			       it.first.begin.printable().c_str(),
			       it.first.end.printable().c_str(),
			       it.second.activeMetadata.isValid() ? "T" : "F",
			       it.second.lastEpoch,
			       it.second.lastSeqno);
		}
		bwData->granuleMetadata.insert(it.first, it.second);
	}
}

static void handleAssignedRange(BlobWorkerData* bwData, KeyRange keyRange, int64_t epoch, int64_t seqno) {
	changeBlobRange(bwData, keyRange, epoch, seqno, true);
}

static void handleRevokedRange(BlobWorkerData* bwData, KeyRange keyRange, int64_t epoch, int64_t seqno) {
	changeBlobRange(bwData, keyRange, epoch, seqno, false);
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

			if (BW_DEBUG) {
				printf("Registered blob worker %s\n", interf.id().toString().c_str());
			}
			return Void();
		} catch (Error& e) {
			if (BW_DEBUG) {
				printf("Registering blob worker %s got error %s\n", interf.id().toString().c_str(), e.name());
			}
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

	if (BW_DEBUG) {
		printf("Initializing blob worker s3 stuff\n");
	}
	try {
		if (g_network->isSimulated()) {
			if (BW_DEBUG) {
				printf("BW constructing simulated backup container\n");
			}
			self.bstore = BackupContainerFileSystem::openContainerFS("file://fdbblob/");
		} else {
			if (BW_DEBUG) {
				printf("BW constructing backup container from %s\n", SERVER_KNOBS->BG_URL.c_str());
			}
			self.bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL);
			if (BW_DEBUG) {
				printf("BW constructed backup container\n");
			}
		}
	} catch (Error& e) {
		if (BW_DEBUG) {
			printf("BW got backup container init error %s\n", e.name());
		}
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
				if (BW_DEBUG) {
					printf("Worker %s %s range [%s - %s) @ (%lld, %lld)\n",
					       self.id.toString().c_str(),
					       req.isAssign ? "assigned" : "revoked",
					       req.keyRange.begin.printable().c_str(),
					       req.keyRange.end.printable().c_str(),
					       req.managerEpoch,
					       req.managerSeqno);
				}

				if (req.managerEpoch < self.currentManagerEpoch) {
					if (BW_DEBUG) {
						printf("BW %s got request from old epoch %lld, notifying manager it is out of date\n",
						       self.id.toString().c_str(),
						       req.managerEpoch);
					}
					req.reply.send(AssignBlobRangeReply(false));
				} else {
					if (req.managerEpoch > self.currentManagerEpoch) {
						self.currentManagerEpoch = req.managerEpoch;
						if (BW_DEBUG) {
							printf("BW %s found new manager epoch %lld\n",
							       self.id.toString().c_str(),
							       self.currentManagerEpoch);
						}
					}

					// TODO with range versioning, need to persist only after it's confirmed
					changeBlobRange(&self, req.keyRange, req.managerEpoch, req.managerSeqno, req.isAssign);
					req.reply.send(AssignBlobRangeReply(true));
				}
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		if (BW_DEBUG) {
			printf("Blob worker got error %s, exiting\n", e.name());
		}
		TraceEvent("BlobWorkerDied", self.id).error(e, true);
	}

	return Void();
}

// TODO add unit tests for assign/revoke range, especially version ordering